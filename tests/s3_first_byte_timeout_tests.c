/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_first_byte_timeout.h"

#include <aws/common/clock.h>
#include <aws/testing/aws_test_harness.h>

#define TEST_CASE(NAME)                                                                                                \
    AWS_TEST_CASE(NAME, s_test_##NAME);                                                                                \
    static int s_test_##NAME(struct aws_allocator *allocator, void *ctx)

static uint64_t s_ms_to_ns(uint64_t ms) {
    return aws_timestamp_convert(ms, AWS_TIMESTAMP_MILLIS, AWS_TIMESTAMP_NANOS, NULL);
}

/* Records `count` successes. Each success has a request time and a response to first byte time of `time_ms`. */
static void s_record_successes(
    struct aws_s3_first_byte_timeout_controller *controller,
    size_t count,
    uint64_t time_ms) {
    for (size_t i = 0; i < count; ++i) {
        aws_s3_first_byte_timeout_controller_record_success(controller, s_ms_to_ns(time_ms), s_ms_to_ns(time_ms));
    }
}

/* Completes the warmup with fast requests, so that the timeout starts at the 1 s minimum. */
static int s_init_warm_controller(
    struct aws_s3_first_byte_timeout_controller *controller,
    struct aws_allocator *allocator) {
    aws_s3_first_byte_timeout_controller_init(controller, allocator, AWS_S3_FIRST_BYTE_TIMEOUT_MIN_WARMUP_SAMPLES);
    s_record_successes(controller, AWS_S3_FIRST_BYTE_TIMEOUT_MIN_WARMUP_SAMPLES, 100);
    ASSERT_UINT_EQUALS(1000, aws_s3_first_byte_timeout_controller_get_timeout_ms(controller));
    ASSERT_UINT_EQUALS(AWS_S3_FIRST_BYTE_TIMEOUT_BUDGET_CAPACITY_CENTITOKENS, controller->budget_centitokens);
    return AWS_OP_SUCCESS;
}

/* During the warmup and after a stop, the controller gives no timeout, also with a full budget. */
TEST_CASE(first_byte_timeout_controller_no_timeout_during_warmup_and_after_stop) {
    (void)ctx;
    struct aws_s3_first_byte_timeout_controller controller;
    aws_s3_first_byte_timeout_controller_init(&controller, allocator, AWS_S3_FIRST_BYTE_TIMEOUT_MIN_WARMUP_SAMPLES);

    s_record_successes(&controller, AWS_S3_FIRST_BYTE_TIMEOUT_MIN_WARMUP_SAMPLES - 1, 100);
    ASSERT_UINT_EQUALS(0, aws_s3_first_byte_timeout_controller_get_timeout_ms(&controller));
    s_record_successes(&controller, 1, 100);
    ASSERT_UINT_EQUALS(1000, aws_s3_first_byte_timeout_controller_get_timeout_ms(&controller));
    aws_s3_first_byte_timeout_controller_clean_up(&controller);

    /* A warmup mean of 5 s or more stops the timeout. */
    aws_s3_first_byte_timeout_controller_init(&controller, allocator, AWS_S3_FIRST_BYTE_TIMEOUT_MIN_WARMUP_SAMPLES);
    s_record_successes(&controller, AWS_S3_FIRST_BYTE_TIMEOUT_MIN_WARMUP_SAMPLES, 5000);
    ASSERT_TRUE(controller.stop_timeout);
    ASSERT_UINT_EQUALS(AWS_S3_FIRST_BYTE_TIMEOUT_BUDGET_CAPACITY_CENTITOKENS, controller.budget_centitokens);
    ASSERT_UINT_EQUALS(0, aws_s3_first_byte_timeout_controller_get_timeout_ms(&controller));

    /* The stop is permanent, also if the link becomes fast later. */
    s_record_successes(&controller, 1000, 10);
    ASSERT_TRUE(controller.stop_timeout);
    ASSERT_UINT_EQUALS(0, aws_s3_first_byte_timeout_controller_get_timeout_ms(&controller));
    aws_s3_first_byte_timeout_controller_clean_up(&controller);
    return AWS_OP_SUCCESS;
}

/* The seed is the nearest-rank p90 of all warmup samples, and the first sample is one of them. */
TEST_CASE(first_byte_timeout_controller_seeds_at_nearest_rank_p90) {
    (void)ctx;
    struct aws_s3_first_byte_timeout_controller controller;

    /* 1 slow sample in 10 is above the p90, which is the 9th value. */
    aws_s3_first_byte_timeout_controller_init(&controller, allocator, AWS_S3_FIRST_BYTE_TIMEOUT_MIN_WARMUP_SAMPLES);
    s_record_successes(&controller, 1, 100);
    s_record_successes(&controller, 1, 3000);
    s_record_successes(&controller, 8, 100);
    ASSERT_UINT_EQUALS(1000, aws_s3_first_byte_timeout_controller_get_timeout_ms(&controller));
    aws_s3_first_byte_timeout_controller_clean_up(&controller);

    /* 2 slow samples in 10 put the p90 at the slow value. The mean plus 700 ms would be 1.38 s, which is in the slow
     * tenth. */
    aws_s3_first_byte_timeout_controller_init(&controller, allocator, AWS_S3_FIRST_BYTE_TIMEOUT_MIN_WARMUP_SAMPLES);
    s_record_successes(&controller, 8, 100);
    s_record_successes(&controller, 2, 3000);
    ASSERT_UINT_EQUALS(3000, aws_s3_first_byte_timeout_controller_get_timeout_ms(&controller));
    aws_s3_first_byte_timeout_controller_clean_up(&controller);

    /* The first sample is a warmup sample also. The p90 of 20 samples is the 3rd largest value. Without the first
     * sample, it would be 100 ms. */
    aws_s3_first_byte_timeout_controller_init(&controller, allocator, 20);
    s_record_successes(&controller, 1, 4000);
    s_record_successes(&controller, 1, 3000);
    s_record_successes(&controller, 1, 2000);
    s_record_successes(&controller, 17, 100);
    ASSERT_UINT_EQUALS(2000, aws_s3_first_byte_timeout_controller_get_timeout_ms(&controller));
    aws_s3_first_byte_timeout_controller_clean_up(&controller);
    return AWS_OP_SUCCESS;
}

/**
 * Many requests that use the same timeout can time out at the same time. The timeout rate increases the timeout value
 * one time for the burst. The budget stops the burst after 10 timeouts, and successes refill it.
 */
TEST_CASE(first_byte_timeout_controller_budget_limits_a_burst_of_timeouts) {
    (void)ctx;
    struct aws_s3_first_byte_timeout_controller controller;
    ASSERT_SUCCESS(s_init_warm_controller(&controller, allocator));

    for (size_t i = 0; i < 10; ++i) {
        /* All requests of the burst used the 1 s timeout. */
        ASSERT_TRUE(aws_s3_first_byte_timeout_controller_get_timeout_ms(&controller) > 0);
        aws_s3_first_byte_timeout_controller_record_timeout(&controller, 1000);
    }

    ASSERT_UINT_EQUALS(0, controller.budget_centitokens);
    ASSERT_UINT_EQUALS(0, aws_s3_first_byte_timeout_controller_get_timeout_ms(&controller));
    /* The empty budget does not change the timeout value. */
    ASSERT_FALSE(controller.stop_timeout);
    ASSERT_UINT_EQUALS(2000, controller.timeout_ms);

    /* 1 token needs 20 successes of 0.05 tokens. */
    s_record_successes(&controller, 19, 100);
    ASSERT_UINT_EQUALS(0, aws_s3_first_byte_timeout_controller_get_timeout_ms(&controller));
    s_record_successes(&controller, 1, 100);
    size_t timeout_ms = aws_s3_first_byte_timeout_controller_get_timeout_ms(&controller);
    ASSERT_UINT_EQUALS(controller.timeout_ms, timeout_ms);
    ASSERT_TRUE(timeout_ms > 1000);
    ASSERT_TRUE(timeout_ms < 2000);

    aws_s3_first_byte_timeout_controller_clean_up(&controller);
    return AWS_OP_SUCCESS;
}

/**
 * The controller gives a timeout to each request that it sends while 1 token or more is available. Thus, more requests
 * than tokens can be in flight with a timeout, and reach it together, for example during a broad slowdown. Each of
 * these timeouts removes 1 token, and the budget becomes negative. Successes pay back all of the timeouts before the
 * next request gets a timeout. Thus, waves of timeouts stay at approximately 4.8% of the requests also with many
 * connections. With a floor at 0, the 25 hedges of the wave would refill 1 token, and the next wave would start.
 */
TEST_CASE(first_byte_timeout_controller_budget_pays_back_a_wave_of_timeouts) {
    (void)ctx;
    struct aws_s3_first_byte_timeout_controller controller;
    ASSERT_SUCCESS(s_init_warm_controller(&controller, allocator));

    /* 25 requests in flight got the 1 s timeout, and all of them reach it. */
    for (size_t i = 0; i < 25; ++i) {
        aws_s3_first_byte_timeout_controller_record_timeout(&controller, 1000);
    }
    ASSERT_INT_EQUALS(-1500, controller.budget_centitokens);
    ASSERT_FALSE(controller.stop_timeout);
    ASSERT_UINT_EQUALS(0, aws_s3_first_byte_timeout_controller_get_timeout_ms(&controller));

    /* 1 token needs (1500 + 100) / 5 = 320 successes. */
    s_record_successes(&controller, 319, 100);
    ASSERT_UINT_EQUALS(0, aws_s3_first_byte_timeout_controller_get_timeout_ms(&controller));
    s_record_successes(&controller, 1, 100);
    ASSERT_TRUE(aws_s3_first_byte_timeout_controller_get_timeout_ms(&controller) > 0);

    aws_s3_first_byte_timeout_controller_clean_up(&controller);
    return AWS_OP_SUCCESS;
}

/* The budget is full at the start, and successes do not fill it more than its capacity. */
TEST_CASE(first_byte_timeout_controller_budget_has_a_capacity) {
    (void)ctx;
    struct aws_s3_first_byte_timeout_controller controller;
    ASSERT_SUCCESS(s_init_warm_controller(&controller, allocator));

    s_record_successes(&controller, 1000, 100);
    ASSERT_UINT_EQUALS(AWS_S3_FIRST_BYTE_TIMEOUT_BUDGET_CAPACITY_CENTITOKENS, controller.budget_centitokens);

    aws_s3_first_byte_timeout_controller_record_timeout(&controller, 0);
    ASSERT_UINT_EQUALS(AWS_S3_FIRST_BYTE_TIMEOUT_BUDGET_CAPACITY_CENTITOKENS - 100, controller.budget_centitokens);

    aws_s3_first_byte_timeout_controller_clean_up(&controller);
    return AWS_OP_SUCCESS;
}

/* The mean of the first 64 samples is the arithmetic mean. Thus, one slow first sample does not have a large weight. */
TEST_CASE(first_byte_timeout_controller_mean_starts_as_arithmetic_mean) {
    (void)ctx;
    struct aws_s3_first_byte_timeout_controller controller;
    ASSERT_SUCCESS(s_init_warm_controller(&controller, allocator));

    s_record_successes(&controller, 1, 8000);
    s_record_successes(&controller, 63, 50);

    /* (8000 + 63 * 50) / 64 = 174.2 ms. An EWMA with alpha 1/64 from the first sample would give approximately 3 s. */
    ASSERT_TRUE(controller.response_to_first_byte_time.mean_ns > s_ms_to_ns(174));
    ASSERT_TRUE(controller.response_to_first_byte_time.mean_ns < s_ms_to_ns(175));

    aws_s3_first_byte_timeout_controller_clean_up(&controller);
    return AWS_OP_SUCCESS;
}

/* After 64 samples, the mean ages. Thus, the timeout follows a lasting change of the network. */
TEST_CASE(first_byte_timeout_controller_mean_ages) {
    (void)ctx;
    struct aws_s3_first_byte_timeout_controller controller;
    ASSERT_SUCCESS(s_init_warm_controller(&controller, allocator));

    s_record_successes(&controller, 1000, 100);
    s_record_successes(&controller, 1000, 1000);

    /* A mean of all samples would be 550 ms. */
    ASSERT_TRUE(controller.response_to_first_byte_time.mean_ns > s_ms_to_ns(999));
    ASSERT_TRUE(controller.response_to_first_byte_time.mean_ns <= s_ms_to_ns(1000));

    /* The timeout value moves toward the new mean plus 700 ms. With the mean of all samples (775 ms), the timeout value
     * would stay below 1475 ms. */
    s_record_successes(&controller, 2000, 1000);
    ASSERT_TRUE(controller.timeout_ms > 1500);
    ASSERT_TRUE(controller.timeout_ms <= 1700);

    aws_s3_first_byte_timeout_controller_clean_up(&controller);
    return AWS_OP_SUCCESS;
}

/* The arithmetic mean of the warmup decides the stop. Thus, one slow cold start does not stop the timeout. */
TEST_CASE(first_byte_timeout_controller_one_slow_cold_start_does_not_stop) {
    (void)ctx;
    struct aws_s3_first_byte_timeout_controller controller;
    aws_s3_first_byte_timeout_controller_init(&controller, allocator, AWS_S3_FIRST_BYTE_TIMEOUT_MIN_WARMUP_SAMPLES);

    /* The arithmetic mean is 845 ms. The p90 is the 9th value, 50 ms. */
    s_record_successes(&controller, 1, 8000);
    s_record_successes(&controller, AWS_S3_FIRST_BYTE_TIMEOUT_MIN_WARMUP_SAMPLES - 1, 50);

    ASSERT_FALSE(controller.stop_timeout);
    ASSERT_UINT_EQUALS(1000, aws_s3_first_byte_timeout_controller_get_timeout_ms(&controller));
    aws_s3_first_byte_timeout_controller_clean_up(&controller);
    return AWS_OP_SUCCESS;
}

/* After the seed, successes move the timeout slowly toward the mean plus 700 ms. */
TEST_CASE(first_byte_timeout_controller_timeout_moves_toward_mean_plus_offset) {
    (void)ctx;
    struct aws_s3_first_byte_timeout_controller controller;
    aws_s3_first_byte_timeout_controller_init(&controller, allocator, AWS_S3_FIRST_BYTE_TIMEOUT_MIN_WARMUP_SAMPLES);
    s_record_successes(&controller, 8, 100);
    s_record_successes(&controller, 2, 3000);
    ASSERT_UINT_EQUALS(3000, aws_s3_first_byte_timeout_controller_get_timeout_ms(&controller));

    size_t previous_timeout_ms = aws_s3_first_byte_timeout_controller_get_timeout_ms(&controller);
    for (size_t i = 0; i < 2000; ++i) {
        s_record_successes(&controller, 1, 100);
        size_t timeout_ms = aws_s3_first_byte_timeout_controller_get_timeout_ms(&controller);
        ASSERT_TRUE(timeout_ms <= previous_timeout_ms);
        previous_timeout_ms = timeout_ms;
    }

    /* The mean goes to 100 ms, so the timeout goes to 100 ms + 700 ms. */
    ASSERT_TRUE(previous_timeout_ms >= 800);
    ASSERT_TRUE(previous_timeout_ms <= 810);
    aws_s3_first_byte_timeout_controller_clean_up(&controller);
    return AWS_OP_SUCCESS;
}

/**
 * One timeout for each 6 requests is 16.7%. The timeout rate increases the timeout value until it is more than 5 s,
 * and then the controller stops. The budget, which can pay for 4.8%, does not stop the timeouts first.
 */
TEST_CASE(first_byte_timeout_controller_high_timeout_rate_stops_timeouts) {
    (void)ctx;
    struct aws_s3_first_byte_timeout_controller controller;
    ASSERT_SUCCESS(s_init_warm_controller(&controller, allocator));

    size_t num_rounds = 0;
    size_t timeout_ms = 0;
    while ((timeout_ms = aws_s3_first_byte_timeout_controller_get_timeout_ms(&controller)) > 0) {
        ASSERT_TRUE(num_rounds < 100);
        s_record_successes(&controller, 5, 100);
        aws_s3_first_byte_timeout_controller_record_timeout(&controller, timeout_ms);
        ++num_rounds;
    }

    ASSERT_TRUE(num_rounds > 1);
    ASSERT_TRUE(controller.stop_timeout);
    ASSERT_TRUE(controller.budget_centitokens >= 100);
    aws_s3_first_byte_timeout_controller_clean_up(&controller);
    return AWS_OP_SUCCESS;
}

/* One timeout for each 100 requests is 1%. The controller keeps the timeout, and the budget stays full. */
TEST_CASE(first_byte_timeout_controller_low_timeout_rate_keeps_timeouts) {
    (void)ctx;
    struct aws_s3_first_byte_timeout_controller controller;
    ASSERT_SUCCESS(s_init_warm_controller(&controller, allocator));

    for (size_t round = 0; round < 1000; ++round) {
        s_record_successes(&controller, 99, 100);
        size_t timeout_ms = aws_s3_first_byte_timeout_controller_get_timeout_ms(&controller);
        ASSERT_TRUE(timeout_ms > 0);
        aws_s3_first_byte_timeout_controller_record_timeout(&controller, timeout_ms);
    }

    ASSERT_FALSE(controller.stop_timeout);
    ASSERT_UINT_EQUALS(AWS_S3_FIRST_BYTE_TIMEOUT_BUDGET_CAPACITY_CENTITOKENS - 100, controller.budget_centitokens);
    ASSERT_TRUE(controller.timeout_ms < 5000);
    aws_s3_first_byte_timeout_controller_clean_up(&controller);
    return AWS_OP_SUCCESS;
}
