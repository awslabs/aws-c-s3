/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_get_hedging.h"

#include <aws/common/clock.h>
#include <aws/common/math.h>

#include <stdlib.h>

/* The constants are the same as for the UploadPart timeout in s3_client.c and the hedging design of the AWS S3
 * Transfer Manager for Rust. */
static const uint64_t s_seed_floor_ns = 1000000000;     /* 1 s */
static const uint64_t s_escape_ns = 5000000000;         /* 5 s */
static const uint64_t s_expected_offset_ns = 700000000; /* 700 ms */
static const int64_t s_mean_ewma_divisor = 64;
static const int64_t s_deadline_ewma_divisor = 100;

static const uint32_t s_hedge_capacity_centitokens = 1000; /* 10 tokens */
static const uint32_t s_hedge_reward_centitokens = 5;      /* 0.05 tokens */
static const uint32_t s_hedge_cost_centitokens = 100;      /* 1 token */

static int s_compare_uint64(const void *a, const void *b) {
    uint64_t arg1 = *(const uint64_t *)a;
    uint64_t arg2 = *(const uint64_t *)b;
    return (arg1 > arg2) - (arg1 < arg2);
}

/* Moves `value` toward `target` by 1/`divisor` of the difference. */
static uint64_t s_ewma(uint64_t value, uint64_t target, int64_t divisor) {
    int64_t difference = (int64_t)target - (int64_t)value;
    return (uint64_t)((int64_t)value + difference / divisor);
}

static void s_seed(struct aws_s3_get_hedging_controller *controller) {
    uint64_t sorted_ns[AWS_S3_GET_HEDGING_WARMUP_SAMPLES];
    uint64_t sum_ns = 0;
    for (size_t i = 0; i < AWS_S3_GET_HEDGING_WARMUP_SAMPLES; ++i) {
        sorted_ns[i] = controller->warmup_samples_ns[i];
        sum_ns = aws_add_u64_saturating(sum_ns, sorted_ns[i]);
    }

    /* Use the arithmetic mean, not the EWMA. The EWMA gives the first sample a weight of approximately 0.87 over 10
     * samples, so one slow cold start would stop the hedges on a healthy link. */
    if (sum_ns / AWS_S3_GET_HEDGING_WARMUP_SAMPLES >= s_escape_ns) {
        controller->stopped = true;
        return;
    }

    qsort(sorted_ns, AWS_S3_GET_HEDGING_WARMUP_SAMPLES, sizeof(uint64_t), s_compare_uint64);
    size_t p90_index = (AWS_S3_GET_HEDGING_WARMUP_SAMPLES * 9 + 9) / 10 - 1;
    controller->deadline_ns = aws_max_u64(sorted_ns[p90_index], s_seed_floor_ns);
    controller->warm = true;
}

void aws_s3_get_hedging_controller_init(struct aws_s3_get_hedging_controller *controller) {
    AWS_PRECONDITION(controller);
    AWS_ZERO_STRUCT(*controller);
    controller->hedge_centitokens = s_hedge_capacity_centitokens;
}

void aws_s3_get_hedging_controller_record_success(struct aws_s3_get_hedging_controller *controller, uint64_t ttfb_ns) {
    AWS_PRECONDITION(controller);

    controller->num_successes = aws_add_u64_saturating(controller->num_successes, 1);
    controller->hedge_centitokens =
        aws_min_u32(controller->hedge_centitokens + s_hedge_reward_centitokens, s_hedge_capacity_centitokens);

    if (controller->stopped) {
        return;
    }

    controller->mean_ttfb_ns =
        controller->num_successes == 1 ? ttfb_ns : s_ewma(controller->mean_ttfb_ns, ttfb_ns, s_mean_ewma_divisor);

    if (!controller->warm) {
        controller->warmup_samples_ns[controller->num_warmup_samples++] = ttfb_ns;
        if (controller->num_warmup_samples == AWS_S3_GET_HEDGING_WARMUP_SAMPLES) {
            s_seed(controller);
        }
        return;
    }

    uint64_t target_ns = aws_add_u64_saturating(controller->mean_ttfb_ns, s_expected_offset_ns);
    controller->deadline_ns = s_ewma(controller->deadline_ns, target_ns, s_deadline_ewma_divisor);
}

void aws_s3_get_hedging_controller_record_hedge(struct aws_s3_get_hedging_controller *controller) {
    AWS_PRECONDITION(controller);

    controller->num_hedges = aws_add_u64_saturating(controller->num_hedges, 1);
    controller->hedge_centitokens = controller->hedge_centitokens >= s_hedge_cost_centitokens
                                        ? controller->hedge_centitokens - s_hedge_cost_centitokens
                                        : 0;
}

uint64_t aws_s3_get_hedging_controller_deadline_ms(const struct aws_s3_get_hedging_controller *controller) {
    AWS_PRECONDITION(controller);

    if (!controller->warm || controller->stopped || controller->hedge_centitokens < s_hedge_cost_centitokens) {
        return 0;
    }

    /* Round up, so that the deadline is never 0 ms or shorter than the calculated value. */
    uint64_t deadline_ms =
        aws_timestamp_convert(controller->deadline_ns, AWS_TIMESTAMP_NANOS, AWS_TIMESTAMP_MILLIS, NULL);
    if (aws_timestamp_convert(deadline_ms, AWS_TIMESTAMP_MILLIS, AWS_TIMESTAMP_NANOS, NULL) < controller->deadline_ns) {
        ++deadline_ms;
    }
    return aws_max_u64(deadline_ms, 1);
}
