/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_first_byte_timeout.h"

#include <aws/common/clock.h>
#include <aws/common/math.h>

#include <math.h>

static uint64_t s_timeout_threshold_ns = 5000000000; /* 5 Secs */
const size_t g_expect_timeout_offset_ms =
    700; /* 0.7 Secs. From experiments on c5n.18xlarge machine for 30 GiB upload, it gave us best performance. */

/* The mean of the response to first byte time is an EWMA with alpha 1/64. */
static const uint64_t s_mean_ewma_divisor = 64;

/* The hedge budget, in 1/100 tokens. The constants are the same as in the hedging design of the AWS S3 Transfer
 * Manager for Rust. */
static const uint32_t s_budget_reward_centitokens = 5; /* 0.05 tokens */
static const uint32_t s_budget_cost_centitokens = 100; /* 1 token */

static int s_compare_uint64_min_heap(const void *a, const void *b) {
    uint64_t arg1 = *(const uint64_t *)a;
    uint64_t arg2 = *(const uint64_t *)b;
    /* Use a min-heap to get the P90, which will be the min of the largest 10% of data. */
    return arg1 > arg2;
}

/* Moves `value` toward `target` by 1/`divisor` of the difference. */
static uint64_t s_move_toward(uint64_t value, uint64_t target, uint64_t divisor) {
    if (target >= value) {
        return value + (target - value) / divisor;
    }
    return value - (value - target) / divisor;
}

void aws_s3_first_byte_timeout_controller_init(
    struct aws_s3_first_byte_timeout_controller *controller,
    struct aws_allocator *allocator,
    uint32_t num_warmup_samples) {
    AWS_PRECONDITION(controller);
    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(num_warmup_samples >= AWS_S3_FIRST_BYTE_TIMEOUT_MIN_WARMUP_SAMPLES);

    AWS_ZERO_STRUCT(*controller);
    controller->allocator = allocator;
    controller->num_warmup_samples = num_warmup_samples;
    controller->budget_centitokens = AWS_S3_FIRST_BYTE_TIMEOUT_BUDGET_CAPACITY_CENTITOKENS;
}

void aws_s3_first_byte_timeout_controller_clean_up(struct aws_s3_first_byte_timeout_controller *controller) {
    AWS_PRECONDITION(controller);

    if (controller->initial_request_time.collecting_p90) {
        aws_priority_queue_clean_up(&controller->initial_request_time.p90_samples);
        controller->initial_request_time.collecting_p90 = false;
    }
}

/* Applies a calculated timeout. Zero keeps the current timeout. A timeout of more than 5 s stops the timeout. */
static void s_apply_timeout(struct aws_s3_first_byte_timeout_controller *controller, uint64_t updated_timeout_ns) {
    if (updated_timeout_ns == 0) {
        return;
    }
    if (updated_timeout_ns > s_timeout_threshold_ns) {
        /* Stops timeout, as wait for server to response will be faster to set our own timeout */
        controller->stop_timeout = true;
        /* Unset the timeout */
        updated_timeout_ns = 0;
    }
    controller->timeout_ms =
        (size_t)aws_timestamp_convert(updated_timeout_ns, AWS_TIMESTAMP_NANOS, AWS_TIMESTAMP_MILLIS, NULL);
}

static void s_record_warmup_sample(struct aws_s3_first_byte_timeout_controller *controller, uint64_t request_time_ns) {
    controller->initial_request_time.sum_ns =
        aws_add_u64_saturating(controller->initial_request_time.sum_ns, request_time_ns);
    ++controller->initial_request_time.num_samples;
    if (!controller->initial_request_time.collecting_p90) {
        /* Initialize the priority queue to collect the p90 of the initial requests. The nearest-rank p90 of N samples
         * is the smallest of the largest floor(N / 10) + 1 samples. */
        controller->initial_request_time.collecting_p90 = true;
        size_t queue_size = controller->num_warmup_samples / 10 + 1;
        aws_priority_queue_init_dynamic(
            &controller->initial_request_time.p90_samples,
            controller->allocator,
            queue_size,
            sizeof(uint64_t),
            s_compare_uint64_min_heap);
    }

    /* check if the queue is full, if full pop and top and push the next. */
    size_t current_size = aws_priority_queue_size(&controller->initial_request_time.p90_samples);
    size_t current_capacity = aws_priority_queue_capacity(&controller->initial_request_time.p90_samples);
    if (current_size == current_capacity) {
        uint64_t *min = NULL;
        aws_priority_queue_top(&controller->initial_request_time.p90_samples, (void **)&min);
        if (request_time_ns > *min) {
            /* Push the data into the queue if it's larger than the min of the queue. */
            uint64_t pop_val = 0;
            aws_priority_queue_pop(&controller->initial_request_time.p90_samples, &pop_val);
            aws_priority_queue_push(&controller->initial_request_time.p90_samples, &request_time_ns);
        }
    } else {
        aws_priority_queue_push(&controller->initial_request_time.p90_samples, &request_time_ns);
    }
}

/* Decides at the end of the warmup if a timeout is necessary, and sets the first timeout. */
static void s_seed(struct aws_s3_first_byte_timeout_controller *controller) {
    uint64_t average_request_time_ns =
        controller->initial_request_time.sum_ns / controller->initial_request_time.num_samples;
    if (average_request_time_ns >= s_timeout_threshold_ns) {
        /* We don't need a timeout, as retry will be slower than just wait for the server to response */
        controller->stop_timeout = true;
        return;
    }

    /* Start the timeout at th p90 of the initial samples. */
    uint64_t *p90_ns = NULL;
    aws_priority_queue_top(&controller->initial_request_time.p90_samples, (void **)&p90_ns);
    uint64_t p90_ms = aws_timestamp_convert(*p90_ns, AWS_TIMESTAMP_NANOS, AWS_TIMESTAMP_MILLIS, NULL);
    controller->timeout_ms = (size_t)aws_max_u64(p90_ms, 1000 /*1sec*/);
    /* Clean up the queue now, as not needed anymore. */
    aws_priority_queue_clean_up(&controller->initial_request_time.p90_samples);
    controller->initial_request_time.collecting_p90 = false;
}

void aws_s3_first_byte_timeout_controller_record_success(
    struct aws_s3_first_byte_timeout_controller *controller,
    uint64_t request_time_ns,
    uint64_t response_to_first_byte_time_ns) {
    AWS_PRECONDITION(controller);

    controller->budget_centitokens = aws_min_u32(
        controller->budget_centitokens + s_budget_reward_centitokens,
        AWS_S3_FIRST_BYTE_TIMEOUT_BUDGET_CAPACITY_CENTITOKENS);

    if (controller->stop_timeout) {
        /* Timeout was disabled */
        return;
    }

    controller->num_successful_requests = aws_add_u64_saturating(controller->num_successful_requests, 1);
    if (controller->num_successful_requests <= controller->num_warmup_samples) {
        /* Gether the data */
        s_record_warmup_sample(controller, request_time_ns);
        if (controller->num_successful_requests == controller->num_warmup_samples) {
            /* Decide we need a timeout or not */
            s_seed(controller);
        }
        return;
    }

    /* Starts to update timeout on case of succeed */
    controller->timeout_rate_tracking.num_completed =
        aws_add_u64_saturating(controller->timeout_rate_tracking.num_completed, 1);
    controller->response_to_first_byte_time.num_samples =
        aws_add_u64_saturating(controller->response_to_first_byte_time.num_samples, 1);
    /* Until there are s_mean_ewma_divisor samples, the divisor is the number of samples. This gives the arithmetic mean
     * of the first samples, so that the first sample does not have a too large weight. */
    controller->response_to_first_byte_time.mean_ns = s_move_toward(
        controller->response_to_first_byte_time.mean_ns,
        response_to_first_byte_time_ns,
        aws_min_u64(controller->response_to_first_byte_time.num_samples, s_mean_ewma_divisor));

    uint64_t current_timeout_ns =
        aws_timestamp_convert(controller->timeout_ms, AWS_TIMESTAMP_MILLIS, AWS_TIMESTAMP_NANOS, NULL);
    uint64_t expect_timeout_offset_ns =
        aws_timestamp_convert(g_expect_timeout_offset_ms, AWS_TIMESTAMP_MILLIS, AWS_TIMESTAMP_NANOS, NULL);
    uint64_t expected_timeout_ns = controller->response_to_first_byte_time.mean_ns + expect_timeout_offset_ns;
    double timeout_ns_double = (double)current_timeout_ns * 0.99 + (double)expected_timeout_ns * 0.01;
    s_apply_timeout(controller, (uint64_t)timeout_ns_double);
}

bool aws_s3_first_byte_timeout_controller_record_timeout(
    struct aws_s3_first_byte_timeout_controller *controller,
    size_t request_timeout_ms) {
    AWS_PRECONDITION(controller);

    controller->budget_centitokens = controller->budget_centitokens >= s_budget_cost_centitokens
                                         ? controller->budget_centitokens - s_budget_cost_centitokens
                                         : 0;

    if (controller->stop_timeout) {
        /* Timeout was disabled */
        return false;
    }
    if (controller->num_successful_requests < AWS_S3_FIRST_BYTE_TIMEOUT_MIN_WARMUP_SAMPLES) {
        return false;
    }

    /* Starts to update timeout on case of timed out */
    controller->timeout_rate_tracking.num_completed =
        aws_add_u64_saturating(controller->timeout_rate_tracking.num_completed, 1);
    controller->timeout_rate_tracking.num_failed =
        aws_add_u64_saturating(controller->timeout_rate_tracking.num_failed, 1);

    uint64_t timeout_threshold = (uint64_t)ceil((double)controller->timeout_rate_tracking.num_completed / 100);
    uint64_t warning_threshold = (uint64_t)ceil((double)controller->timeout_rate_tracking.num_completed / 1000);

    uint64_t current_timeout_ns =
        aws_timestamp_convert(controller->timeout_ms, AWS_TIMESTAMP_MILLIS, AWS_TIMESTAMP_NANOS, NULL);
    uint64_t updated_timeout_ns = 0;
    bool rate_too_high = false;

    if (controller->timeout_rate_tracking.num_failed > timeout_threshold) {
        /**
         * Restore the rate track, as we are larger than 1%, it goes off the record.
         */
        rate_too_high = true;
        controller->timeout_rate_tracking.num_completed = 0;
        controller->timeout_rate_tracking.num_failed = 0;
        if (request_timeout_ms + 1000 > controller->timeout_ms) {
            /* Update the timeout by adding 1 secs only when it's worth to do so */
            updated_timeout_ns = aws_add_u64_saturating(
                current_timeout_ns, aws_timestamp_convert(1, AWS_TIMESTAMP_SECS, AWS_TIMESTAMP_NANOS, NULL));
        }
    } else if (controller->timeout_rate_tracking.num_failed > warning_threshold) {
        if (request_timeout_ms + 100 > controller->timeout_ms) {
            /* Only update the timeout by adding 100 ms if the request was made with a longer time out. */
            updated_timeout_ns = aws_add_u64_saturating(
                current_timeout_ns, aws_timestamp_convert(100, AWS_TIMESTAMP_MILLIS, AWS_TIMESTAMP_NANOS, NULL));
        }
    }

    s_apply_timeout(controller, updated_timeout_ns);
    return rate_too_high;
}

size_t aws_s3_first_byte_timeout_controller_get_timeout_ms(
    const struct aws_s3_first_byte_timeout_controller *controller) {
    AWS_PRECONDITION(controller);

    if (controller->stop_timeout || controller->budget_centitokens < s_budget_cost_centitokens) {
        return 0;
    }
    return controller->timeout_ms;
}
