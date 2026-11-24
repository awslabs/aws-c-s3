/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_client_impl.h"
#include "aws/s3/private/s3_get_hedging.h"
#include "aws/s3/private/s3_request.h"
#include "aws/s3/private/s3_util.h"
#include "s3_tester.h"

#include <aws/common/clock.h>
#include <aws/common/environment.h>
#include <aws/common/string.h>
#include <aws/testing/aws_test_harness.h>

#define TEST_CASE(NAME)                                                                                                \
    AWS_TEST_CASE(NAME, s_test_##NAME);                                                                                \
    static int s_test_##NAME(struct aws_allocator *allocator, void *ctx)

static const char *s_get_object_hedging_env_var = "AWS_CRT_S3_GET_OBJECT_HEDGING";

static uint64_t s_ms_to_ns(uint64_t ms) {
    return aws_timestamp_convert(ms, AWS_TIMESTAMP_MILLIS, AWS_TIMESTAMP_NANOS, NULL);
}

static void s_record_successes(struct aws_s3_get_hedging_controller *controller, size_t count, uint64_t ttfb_ms) {
    for (size_t i = 0; i < count; ++i) {
        aws_s3_get_hedging_controller_record_success(controller, s_ms_to_ns(ttfb_ms));
    }
}

TEST_CASE(get_hedging_controller_arms_no_deadline_during_warmup) {
    (void)allocator;
    (void)ctx;
    struct aws_s3_get_hedging_controller controller;
    aws_s3_get_hedging_controller_init(&controller);

    s_record_successes(&controller, AWS_S3_GET_HEDGING_WARMUP_SAMPLES - 1, 100);
    ASSERT_FALSE(controller.warm);
    ASSERT_UINT_EQUALS(0, aws_s3_get_hedging_controller_deadline_ms(&controller));

    s_record_successes(&controller, 1, 100);
    ASSERT_TRUE(controller.warm);
    ASSERT_TRUE(aws_s3_get_hedging_controller_deadline_ms(&controller) > 0);
    return AWS_OP_SUCCESS;
}

TEST_CASE(get_hedging_controller_seeds_at_p90) {
    (void)allocator;
    (void)ctx;
    struct aws_s3_get_hedging_controller controller;
    aws_s3_get_hedging_controller_init(&controller);

    /* The mean plus 700 ms would put the deadline at 1.38 s, inside the slow tenth. The p90 is 3 s. */
    s_record_successes(&controller, 8, 100);
    s_record_successes(&controller, 2, 3000);

    ASSERT_UINT_EQUALS(3000, aws_s3_get_hedging_controller_deadline_ms(&controller));
    return AWS_OP_SUCCESS;
}

TEST_CASE(get_hedging_controller_seed_is_at_least_one_second) {
    (void)allocator;
    (void)ctx;
    struct aws_s3_get_hedging_controller controller;
    aws_s3_get_hedging_controller_init(&controller);

    s_record_successes(&controller, AWS_S3_GET_HEDGING_WARMUP_SAMPLES, 20);

    ASSERT_UINT_EQUALS(1000, aws_s3_get_hedging_controller_deadline_ms(&controller));
    return AWS_OP_SUCCESS;
}

TEST_CASE(get_hedging_controller_stops_on_uniformly_slow_link) {
    (void)allocator;
    (void)ctx;
    struct aws_s3_get_hedging_controller controller;
    aws_s3_get_hedging_controller_init(&controller);

    s_record_successes(&controller, AWS_S3_GET_HEDGING_WARMUP_SAMPLES, 5000);
    ASSERT_TRUE(controller.stopped);
    ASSERT_UINT_EQUALS(0, aws_s3_get_hedging_controller_deadline_ms(&controller));

    /* The stop is permanent, also if the link becomes fast later. */
    s_record_successes(&controller, 1000, 10);
    ASSERT_TRUE(controller.stopped);
    ASSERT_UINT_EQUALS(0, aws_s3_get_hedging_controller_deadline_ms(&controller));
    return AWS_OP_SUCCESS;
}

TEST_CASE(get_hedging_controller_one_slow_cold_start_does_not_stop) {
    (void)allocator;
    (void)ctx;
    struct aws_s3_get_hedging_controller controller;
    aws_s3_get_hedging_controller_init(&controller);

    /* The arithmetic mean is 845 ms. An EWMA would give approximately 7 s, which is above the 5 s stop limit. */
    s_record_successes(&controller, 1, 8000);
    s_record_successes(&controller, AWS_S3_GET_HEDGING_WARMUP_SAMPLES - 1, 50);

    ASSERT_FALSE(controller.stopped);
    ASSERT_TRUE(controller.mean_ttfb_ns > s_ms_to_ns(5000));
    ASSERT_UINT_EQUALS(1000, aws_s3_get_hedging_controller_deadline_ms(&controller));
    return AWS_OP_SUCCESS;
}

TEST_CASE(get_hedging_controller_hedge_does_not_move_deadline) {
    (void)allocator;
    (void)ctx;
    struct aws_s3_get_hedging_controller controller;
    aws_s3_get_hedging_controller_init(&controller);
    s_record_successes(&controller, AWS_S3_GET_HEDGING_WARMUP_SAMPLES, 100);

    uint64_t deadline_ns = controller.deadline_ns;
    uint64_t mean_ttfb_ns = controller.mean_ttfb_ns;
    for (size_t i = 0; i < 5; ++i) {
        aws_s3_get_hedging_controller_record_hedge(&controller);
    }

    ASSERT_UINT_EQUALS(deadline_ns, controller.deadline_ns);
    ASSERT_UINT_EQUALS(mean_ttfb_ns, controller.mean_ttfb_ns);
    ASSERT_UINT_EQUALS(5, controller.num_hedges);
    return AWS_OP_SUCCESS;
}

TEST_CASE(get_hedging_controller_deadline_moves_toward_mean_plus_offset) {
    (void)allocator;
    (void)ctx;
    struct aws_s3_get_hedging_controller controller;
    aws_s3_get_hedging_controller_init(&controller);
    s_record_successes(&controller, 8, 100);
    s_record_successes(&controller, 2, 3000);
    ASSERT_UINT_EQUALS(3000, aws_s3_get_hedging_controller_deadline_ms(&controller));

    uint64_t previous_deadline_ms = aws_s3_get_hedging_controller_deadline_ms(&controller);
    for (size_t i = 0; i < 2000; ++i) {
        aws_s3_get_hedging_controller_record_success(&controller, s_ms_to_ns(100));
        uint64_t deadline_ms = aws_s3_get_hedging_controller_deadline_ms(&controller);
        ASSERT_TRUE(deadline_ms <= previous_deadline_ms);
        previous_deadline_ms = deadline_ms;
    }

    /* The mean goes to 100 ms, so the deadline goes to 100 ms + 700 ms. */
    ASSERT_TRUE(previous_deadline_ms >= 800);
    ASSERT_TRUE(previous_deadline_ms <= 810);
    return AWS_OP_SUCCESS;
}

TEST_CASE(get_hedging_controller_budget_drains_and_successes_refill_it) {
    (void)allocator;
    (void)ctx;
    struct aws_s3_get_hedging_controller controller;
    aws_s3_get_hedging_controller_init(&controller);
    s_record_successes(&controller, AWS_S3_GET_HEDGING_WARMUP_SAMPLES, 100);

    /* The budget starts full with 10 tokens. */
    for (size_t i = 0; i < 10; ++i) {
        ASSERT_TRUE(aws_s3_get_hedging_controller_deadline_ms(&controller) > 0);
        aws_s3_get_hedging_controller_record_hedge(&controller);
    }
    ASSERT_UINT_EQUALS(0, aws_s3_get_hedging_controller_deadline_ms(&controller));

    /* 1 token needs 20 successes of 0.05 tokens. */
    s_record_successes(&controller, 19, 100);
    ASSERT_UINT_EQUALS(0, aws_s3_get_hedging_controller_deadline_ms(&controller));
    s_record_successes(&controller, 1, 100);
    ASSERT_TRUE(aws_s3_get_hedging_controller_deadline_ms(&controller) > 0);
    return AWS_OP_SUCCESS;
}

TEST_CASE(get_hedging_controller_high_hedge_rate_stops_hedging) {
    (void)allocator;
    (void)ctx;
    struct aws_s3_get_hedging_controller controller;
    aws_s3_get_hedging_controller_init(&controller);
    s_record_successes(&controller, AWS_S3_GET_HEDGING_WARMUP_SAMPLES, 100);

    /* One hedge for each 6 requests is 16.7%, which is above the 4.8% that the budget can pay for. */
    size_t num_rounds = 0;
    while (aws_s3_get_hedging_controller_deadline_ms(&controller) > 0) {
        ASSERT_TRUE(num_rounds < 100);
        s_record_successes(&controller, 5, 100);
        aws_s3_get_hedging_controller_record_hedge(&controller);
        ++num_rounds;
    }
    ASSERT_TRUE(num_rounds > 1);
    return AWS_OP_SUCCESS;
}

TEST_CASE(get_hedging_controller_low_hedge_rate_keeps_hedging) {
    (void)allocator;
    (void)ctx;
    struct aws_s3_get_hedging_controller controller;
    aws_s3_get_hedging_controller_init(&controller);
    s_record_successes(&controller, AWS_S3_GET_HEDGING_WARMUP_SAMPLES, 100);

    /* One hedge for each 100 requests is 1%, which the budget can pay for. */
    for (size_t round = 0; round < 1000; ++round) {
        s_record_successes(&controller, 99, 100);
        ASSERT_TRUE(aws_s3_get_hedging_controller_deadline_ms(&controller) > 0);
        aws_s3_get_hedging_controller_record_hedge(&controller);
    }
    return AWS_OP_SUCCESS;
}

static int s_set_env(struct aws_allocator *allocator, const char *value) {
    struct aws_string *name = aws_string_new_from_c_str(allocator, s_get_object_hedging_env_var);
    int result = AWS_OP_SUCCESS;
    if (value == NULL) {
        result = aws_unset_environment_value(name);
    } else {
        struct aws_string *value_str = aws_string_new_from_c_str(allocator, value);
        result = aws_set_environment_value(name, value_str);
        aws_string_destroy(value_str);
    }
    aws_string_destroy(name);
    return result;
}

static int s_client_hedging_enabled(
    struct aws_s3_tester *tester,
    enum aws_tribool setting,
    enum aws_s3_retry_mode retry_mode,
    bool *out_enabled) {

    struct aws_s3_client_config client_config = {
        .part_size = MB_TO_BYTES(8),
        .get_object_hedging = setting,
        .retry_config.retry_mode = retry_mode,
    };
    ASSERT_SUCCESS(aws_s3_tester_bind_client(
        tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION | AWS_S3_TESTER_BIND_CLIENT_SIGNING));

    struct aws_s3_client *client = aws_s3_client_new(tester->allocator, &client_config);
    ASSERT_NOT_NULL(client);
    *out_enabled = client->get_object_hedging_enabled;

    aws_s3_client_release(client);
    aws_s3_tester_wait_for_client_shutdown(tester);
    tester->bound_to_client = false;
    return AWS_OP_SUCCESS;
}

static int s_check_client_hedging(
    struct aws_s3_tester *tester,
    const char *env_value,
    enum aws_tribool setting,
    enum aws_s3_retry_mode retry_mode,
    bool expected_enabled) {

    ASSERT_SUCCESS(s_set_env(tester->allocator, env_value));
    bool enabled = !expected_enabled;
    ASSERT_SUCCESS(s_client_hedging_enabled(tester, setting, retry_mode, &enabled));
    ASSERT_TRUE(enabled == expected_enabled);
    return AWS_OP_SUCCESS;
}

TEST_CASE(client_get_object_hedging_config_and_env_var) {
    (void)ctx;
    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    /* Off by default. */
    ASSERT_SUCCESS(s_check_client_hedging(&tester, NULL, AWS_TRIBOOL_UNSET, AWS_S3_RETRY_MODE_STANDARD, false));

    /* The config field wins over the environment variable. */
    ASSERT_SUCCESS(s_check_client_hedging(&tester, NULL, AWS_TRIBOOL_TRUE, AWS_S3_RETRY_MODE_STANDARD, true));
    ASSERT_SUCCESS(s_check_client_hedging(&tester, "true", AWS_TRIBOOL_FALSE, AWS_S3_RETRY_MODE_STANDARD, false));
    ASSERT_SUCCESS(s_check_client_hedging(&tester, "false", AWS_TRIBOOL_TRUE, AWS_S3_RETRY_MODE_STANDARD, true));

    /* The environment variable applies if the config field is unset. */
    ASSERT_SUCCESS(s_check_client_hedging(&tester, "true", AWS_TRIBOOL_UNSET, AWS_S3_RETRY_MODE_STANDARD, true));
    ASSERT_SUCCESS(s_check_client_hedging(&tester, "TRUE", AWS_TRIBOOL_UNSET, AWS_S3_RETRY_MODE_STANDARD, true));
    ASSERT_SUCCESS(s_check_client_hedging(&tester, "1", AWS_TRIBOOL_UNSET, AWS_S3_RETRY_MODE_STANDARD, true));
    ASSERT_SUCCESS(s_check_client_hedging(&tester, "false", AWS_TRIBOOL_UNSET, AWS_S3_RETRY_MODE_STANDARD, false));
    ASSERT_SUCCESS(s_check_client_hedging(&tester, "0", AWS_TRIBOOL_UNSET, AWS_S3_RETRY_MODE_STANDARD, false));
    ASSERT_SUCCESS(s_check_client_hedging(&tester, "yes", AWS_TRIBOOL_UNSET, AWS_S3_RETRY_MODE_STANDARD, false));

    /* Hedging needs retries. */
    ASSERT_SUCCESS(s_check_client_hedging(&tester, NULL, AWS_TRIBOOL_TRUE, AWS_S3_RETRY_MODE_NONE, false));
    ASSERT_SUCCESS(s_check_client_hedging(&tester, "true", AWS_TRIBOOL_UNSET, AWS_S3_RETRY_MODE_NONE, false));

    ASSERT_SUCCESS(s_set_env(allocator, NULL));
    aws_s3_tester_clean_up(&tester);
    return AWS_OP_SUCCESS;
}

static void s_init_mock_get_request(
    struct aws_s3_request *mock_request,
    int response_status,
    uint64_t ttfb_ms,
    size_t hedge_timeout_ms) {

    mock_request->request_type = AWS_S3_REQUEST_TYPE_GET_OBJECT;
    mock_request->send_data.response_status = response_status;
    mock_request->hedge_timeout_ms = hedge_timeout_ms;
    struct aws_s3_request_metrics *metrics = mock_request->send_data.metrics;
    metrics->time_metrics.send_end_timestamp_ns = (int64_t)s_ms_to_ns(1000);
    metrics->time_metrics.receive_start_timestamp_ns = (int64_t)s_ms_to_ns(1000 + ttfb_ms);
}

static struct aws_s3_get_hedging_controller s_controller_of(struct aws_s3_client *client) {
    aws_s3_client_lock_synced_data(client);
    struct aws_s3_get_hedging_controller controller = client->synced_data.get_object_hedging;
    aws_s3_client_unlock_synced_data(client);
    return controller;
}

TEST_CASE(client_update_get_object_hedging) {
    (void)ctx;
    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_tester_client_options client_options = {
        .part_size = MB_TO_BYTES(8),
        .tls_usage = AWS_S3_TLS_DISABLED,
        .get_object_hedging = AWS_TRIBOOL_TRUE,
    };
    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_s3_request mock_request;
    struct aws_s3_request_metrics metrics;
    AWS_ZERO_STRUCT(mock_request);
    AWS_ZERO_STRUCT(metrics);
    mock_request.send_data.metrics = &metrics;

    /* Successes with a 2xx response warm the controller up. */
    s_init_mock_get_request(&mock_request, AWS_HTTP_STATUS_CODE_206_PARTIAL_CONTENT, 50, 0);
    for (size_t i = 0; i < AWS_S3_GET_HEDGING_WARMUP_SAMPLES - 1; ++i) {
        aws_s3_client_update_get_object_hedging(client, &mock_request, AWS_ERROR_SUCCESS);
    }
    ASSERT_UINT_EQUALS(0, aws_s3_client_get_object_hedging_timeout_ms(client));
    aws_s3_client_update_get_object_hedging(client, &mock_request, AWS_ERROR_SUCCESS);
    ASSERT_UINT_EQUALS(1000, aws_s3_client_get_object_hedging_timeout_ms(client));
    ASSERT_UINT_EQUALS(AWS_S3_GET_HEDGING_WARMUP_SAMPLES, s_controller_of(client).num_successes);

    /* An error response is not a sample. */
    s_init_mock_get_request(&mock_request, AWS_HTTP_STATUS_CODE_503_SERVICE_UNAVAILABLE, 50, 0);
    aws_s3_client_update_get_object_hedging(client, &mock_request, AWS_ERROR_SUCCESS);
    ASSERT_UINT_EQUALS(AWS_S3_GET_HEDGING_WARMUP_SAMPLES, s_controller_of(client).num_successes);

    /* A first-byte timeout without a hedging deadline is not a hedge. */
    s_init_mock_get_request(&mock_request, 0, 0, 0);
    aws_s3_client_update_get_object_hedging(client, &mock_request, AWS_ERROR_HTTP_RESPONSE_FIRST_BYTE_TIMEOUT);
    ASSERT_UINT_EQUALS(0, s_controller_of(client).num_hedges);

    /* After 10 hedges, the budget is empty and the client arms no deadline. */
    s_init_mock_get_request(&mock_request, 0, 0, 1000);
    for (size_t i = 0; i < 10; ++i) {
        ASSERT_UINT_EQUALS(1000, aws_s3_client_get_object_hedging_timeout_ms(client));
        aws_s3_client_update_get_object_hedging(client, &mock_request, AWS_ERROR_HTTP_RESPONSE_FIRST_BYTE_TIMEOUT);
    }
    ASSERT_UINT_EQUALS(10, s_controller_of(client).num_hedges);
    ASSERT_UINT_EQUALS(0, aws_s3_client_get_object_hedging_timeout_ms(client));

    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);
    return AWS_OP_SUCCESS;
}

TEST_CASE(client_update_get_object_hedging_does_nothing_when_off) {
    (void)ctx;
    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_tester_client_options client_options = {
        .part_size = MB_TO_BYTES(8),
        .tls_usage = AWS_S3_TLS_DISABLED,
        .get_object_hedging = AWS_TRIBOOL_FALSE,
    };
    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_s3_request mock_request;
    struct aws_s3_request_metrics metrics;
    AWS_ZERO_STRUCT(mock_request);
    AWS_ZERO_STRUCT(metrics);
    mock_request.send_data.metrics = &metrics;

    s_init_mock_get_request(&mock_request, AWS_HTTP_STATUS_CODE_200_OK, 50, 0);
    for (size_t i = 0; i < AWS_S3_GET_HEDGING_WARMUP_SAMPLES; ++i) {
        aws_s3_client_update_get_object_hedging(client, &mock_request, AWS_ERROR_SUCCESS);
    }
    ASSERT_UINT_EQUALS(0, s_controller_of(client).num_successes);
    ASSERT_UINT_EQUALS(0, aws_s3_client_get_object_hedging_timeout_ms(client));

    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);
    return AWS_OP_SUCCESS;
}

/* The mock server delays part 2 of these routes by 3 s. */
static const uint64_t s_mock_server_delay_ms = 3000;
static const uint64_t s_hedging_deadline_ms = 1000;

/* The fast parts move the deadline a little toward their mean plus 700 ms before part 2 is sent. */
static const uint64_t s_min_hedging_deadline_ms = 900;

/* Completes the warmup with fast samples, so that the deadline is the 1 s floor. */
static void s_warm_up_get_object_hedging(struct aws_s3_client *client) {
    struct aws_s3_request mock_request;
    struct aws_s3_request_metrics metrics;
    AWS_ZERO_STRUCT(mock_request);
    AWS_ZERO_STRUCT(metrics);
    mock_request.send_data.metrics = &metrics;
    s_init_mock_get_request(&mock_request, AWS_HTTP_STATUS_CODE_206_PARTIAL_CONTENT, 5, 0);
    for (size_t i = 0; i < AWS_S3_GET_HEDGING_WARMUP_SAMPLES; ++i) {
        aws_s3_client_update_get_object_hedging(client, &mock_request, AWS_ERROR_SUCCESS);
    }
}

struct hedging_attempt_info {
    uint32_t part_number;
    uint32_t retry_attempt;
    int error_code;
    uint64_t first_byte_timeout_ms;
};

/* Copies the metrics of all attempts, so that the checks can run without the tester lock. */
static int s_get_attempts(
    struct aws_s3_tester *tester,
    struct aws_s3_meta_request_test_results *results,
    struct hedging_attempt_info *out_attempts,
    size_t max_attempts,
    size_t *out_num_attempts) {

    aws_s3_tester_lock_synced_data(tester);
    size_t num_attempts = aws_array_list_length(&results->synced_data.metrics);
    for (size_t i = 0; i < num_attempts && i < max_attempts; ++i) {
        struct aws_s3_request_metrics *metrics = NULL;
        aws_array_list_get_at(&results->synced_data.metrics, (void **)&metrics, i);
        aws_s3_request_metrics_get_part_number(metrics, &out_attempts[i].part_number);
        out_attempts[i].retry_attempt = aws_s3_request_metrics_get_retry_attempt(metrics);
        out_attempts[i].error_code = aws_s3_request_metrics_get_error_code(metrics);
        out_attempts[i].first_byte_timeout_ms = aws_s3_request_metrics_get_first_byte_timeout_ms(metrics);
    }
    aws_s3_tester_unlock_synced_data(tester);

    ASSERT_TRUE(num_attempts <= max_attempts);
    *out_num_attempts = num_attempts;
    return AWS_OP_SUCCESS;
}

/* Checks the attempt metrics and counts the attempts that reached the first-byte timeout. Only part 2 can reach it. The
 * first attempt of each part has the hedging deadline. The attempts after a hedge have no deadline. */
static int s_check_attempts(
    struct aws_s3_tester *tester,
    struct aws_s3_meta_request_test_results *results,
    size_t *out_num_first_byte_timeouts) {

    struct hedging_attempt_info attempts[16];
    size_t num_attempts = 0;
    ASSERT_SUCCESS(s_get_attempts(tester, results, attempts, AWS_ARRAY_SIZE(attempts), &num_attempts));

    *out_num_first_byte_timeouts = 0;
    for (size_t i = 0; i < num_attempts; ++i) {
        const struct hedging_attempt_info *attempt = &attempts[i];
        if (attempt->error_code == AWS_ERROR_HTTP_RESPONSE_FIRST_BYTE_TIMEOUT) {
            ASSERT_UINT_EQUALS(2, attempt->part_number);
            ++*out_num_first_byte_timeouts;
        }
        if (attempt->retry_attempt == 0) {
            ASSERT_TRUE(attempt->first_byte_timeout_ms >= s_min_hedging_deadline_ms);
            ASSERT_TRUE(attempt->first_byte_timeout_ms <= s_hedging_deadline_ms);
        } else {
            ASSERT_UINT_EQUALS(2, attempt->part_number);
            ASSERT_UINT_EQUALS(0, attempt->first_byte_timeout_ms);
        }
    }
    return AWS_OP_SUCCESS;
}

static int s_get_object_with_hedging(
    struct aws_allocator *allocator,
    const char *object_path,
    uint64_t *out_duration_ms,
    size_t *out_num_hedges,
    size_t *out_num_first_byte_timeouts) {

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_tester_client_options client_options = {
        .part_size = 64 * 1024,
        .tls_usage = AWS_S3_TLS_DISABLED,
        .get_object_hedging = AWS_TRIBOOL_TRUE,
        /* A retry through the retry strategy would wait 10 s. A hedge must not wait. */
        .backoff_scale_factor_ms = 10000,
        .jitter_mode = AWS_EXPONENTIAL_BACKOFF_JITTER_NONE,
    };
    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));
    s_warm_up_get_object_hedging(client);
    ASSERT_UINT_EQUALS(s_hedging_deadline_ms, aws_s3_client_get_object_hedging_timeout_ms(client));

    struct aws_s3_tester_meta_request_options get_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .client = client,
        .get_options =
            {
                .object_path = aws_byte_cursor_from_c_str(object_path),
            },
        .mock_server = true,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_SUCCESS,
    };
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);

    uint64_t start_ns = 0;
    aws_high_res_clock_get_ticks(&start_ns);
    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &get_options, &out_results));
    uint64_t end_ns = 0;
    aws_high_res_clock_get_ticks(&end_ns);

    ASSERT_UINT_EQUALS(AWS_ERROR_SUCCESS, out_results.finished_error_code);
    ASSERT_UINT_EQUALS(262144, out_results.received_body_size);
    *out_duration_ms = aws_timestamp_convert(end_ns - start_ns, AWS_TIMESTAMP_NANOS, AWS_TIMESTAMP_MILLIS, NULL);
    *out_num_hedges = (size_t)s_controller_of(client).num_hedges;
    ASSERT_SUCCESS(s_check_attempts(&tester, &out_results, out_num_first_byte_timeouts));

    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);
    return AWS_OP_SUCCESS;
}

/* Part 2 answers its first attempt after 3 s. The client hedges it after 1 s, and the new attempt gets a fast
 * answer. */
TEST_CASE(get_object_hedging_mock_server) {
    (void)ctx;
    uint64_t duration_ms = 0;
    size_t num_hedges = 0;
    size_t num_first_byte_timeouts = 0;
    ASSERT_SUCCESS(s_get_object_with_hedging(
        allocator, "/get_object_hedging_slow_part", &duration_ms, &num_hedges, &num_first_byte_timeouts));

    ASSERT_UINT_EQUALS(1, num_hedges);
    ASSERT_UINT_EQUALS(1, num_first_byte_timeouts);
    ASSERT_TRUE(duration_ms >= s_min_hedging_deadline_ms);
    /* Shorter than the delay of the slow answer and than the 10 s backoff of the retry strategy. */
    ASSERT_TRUE(duration_ms < s_mock_server_delay_ms);
    return AWS_OP_SUCCESS;
}

/* Part 2 answers its first two attempts after 3 s. The client hedges the first attempt only, and waits for the slow
 * answer to the second attempt. */
TEST_CASE(get_object_hedging_once_per_part_mock_server) {
    (void)ctx;
    uint64_t duration_ms = 0;
    size_t num_hedges = 0;
    size_t num_first_byte_timeouts = 0;
    ASSERT_SUCCESS(s_get_object_with_hedging(
        allocator, "/get_object_hedging_slow_part_twice", &duration_ms, &num_hedges, &num_first_byte_timeouts));

    ASSERT_UINT_EQUALS(1, num_hedges);
    ASSERT_UINT_EQUALS(1, num_first_byte_timeouts);
    /* A second hedge would end the download after approximately 2 s. */
    ASSERT_TRUE(duration_ms >= s_min_hedging_deadline_ms + s_mock_server_delay_ms);
    return AWS_OP_SUCCESS;
}
