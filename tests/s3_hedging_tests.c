/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_client_impl.h"
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
    mock_request->first_byte_timeout_ms = hedge_timeout_ms;
    struct aws_s3_request_metrics *metrics = mock_request->send_data.metrics;
    metrics->time_metrics.send_end_timestamp_ns = (int64_t)s_ms_to_ns(1000);
    metrics->time_metrics.receive_start_timestamp_ns = (int64_t)s_ms_to_ns(1000 + ttfb_ms);
}

/* Copies the GET hedging controller. The copy is only for reading fields. */
static struct aws_s3_first_byte_timeout_controller s_controller_of(struct aws_s3_client *client) {
    aws_s3_client_lock_synced_data(client);
    struct aws_s3_first_byte_timeout_controller controller = client->synced_data.get_object_timeout;
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
    uint32_t num_warmup_samples = s_controller_of(client).num_warmup_samples;
    s_init_mock_get_request(&mock_request, AWS_HTTP_STATUS_CODE_206_PARTIAL_CONTENT, 50, 0);
    for (size_t i = 0; i < num_warmup_samples - 1; ++i) {
        aws_s3_client_update_get_object_hedging(client, &mock_request, AWS_ERROR_SUCCESS);
    }
    ASSERT_UINT_EQUALS(0, aws_s3_client_get_object_hedging_timeout_ms(client));
    aws_s3_client_update_get_object_hedging(client, &mock_request, AWS_ERROR_SUCCESS);
    ASSERT_UINT_EQUALS(1000, aws_s3_client_get_object_hedging_timeout_ms(client));
    ASSERT_UINT_EQUALS(num_warmup_samples, s_controller_of(client).num_successful_requests);

    /* An error response is not a sample. */
    s_init_mock_get_request(&mock_request, AWS_HTTP_STATUS_CODE_503_SERVICE_UNAVAILABLE, 50, 0);
    aws_s3_client_update_get_object_hedging(client, &mock_request, AWS_ERROR_SUCCESS);
    ASSERT_UINT_EQUALS(num_warmup_samples, s_controller_of(client).num_successful_requests);

    /* A first-byte timeout of an attempt without a first-byte timeout is not a hedge. */
    s_init_mock_get_request(&mock_request, 0, 0, 0);
    aws_s3_client_update_get_object_hedging(client, &mock_request, AWS_ERROR_HTTP_RESPONSE_FIRST_BYTE_TIMEOUT);
    ASSERT_UINT_EQUALS(0, s_controller_of(client).num_timeouts);

    /* After 10 hedges, the budget is empty and the client sets no timeout. */
    s_init_mock_get_request(&mock_request, 0, 0, 1000);
    for (size_t i = 0; i < 10; ++i) {
        ASSERT_TRUE(aws_s3_client_get_object_hedging_timeout_ms(client) > 0);
        aws_s3_client_update_get_object_hedging(client, &mock_request, AWS_ERROR_HTTP_RESPONSE_FIRST_BYTE_TIMEOUT);
    }
    ASSERT_UINT_EQUALS(10, s_controller_of(client).num_timeouts);
    ASSERT_UINT_EQUALS(0, aws_s3_client_get_object_hedging_timeout_ms(client));
    /* The timeout rate increased the timeout value one time for the burst. */
    ASSERT_UINT_EQUALS(2000, s_controller_of(client).timeout_ms);

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
    for (size_t i = 0; i < s_controller_of(client).num_warmup_samples; ++i) {
        aws_s3_client_update_get_object_hedging(client, &mock_request, AWS_ERROR_SUCCESS);
    }
    ASSERT_UINT_EQUALS(0, s_controller_of(client).num_successful_requests);
    ASSERT_UINT_EQUALS(0, aws_s3_client_get_object_hedging_timeout_ms(client));

    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);
    return AWS_OP_SUCCESS;
}

/**
 * aws-c-http records the end of the request send in a task after the socket write completes. A fast server can answer
 * before this task runs. Then the response starts before the recorded end of the send. Such a response is a success
 * with a time to first byte of 0.
 */
TEST_CASE(client_update_get_object_hedging_counts_a_response_before_the_send_end) {
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
    s_init_mock_get_request(&mock_request, AWS_HTTP_STATUS_CODE_206_PARTIAL_CONTENT, 0, 0);
    metrics.time_metrics.receive_start_timestamp_ns = metrics.time_metrics.send_end_timestamp_ns - 1;

    /* The responses complete the warmup. */
    uint32_t num_warmup_samples = s_controller_of(client).num_warmup_samples;
    for (size_t i = 0; i < num_warmup_samples; ++i) {
        aws_s3_client_update_get_object_hedging(client, &mock_request, AWS_ERROR_SUCCESS);
    }
    ASSERT_UINT_EQUALS(num_warmup_samples, s_controller_of(client).num_successful_requests);
    ASSERT_UINT_EQUALS(1000, aws_s3_client_get_object_hedging_timeout_ms(client));

    /* After the warmup, the timeout moves from 1 s toward the mean of 0 plus 700 ms. */
    aws_s3_client_update_get_object_hedging(client, &mock_request, AWS_ERROR_SUCCESS);
    ASSERT_UINT_EQUALS(0, s_controller_of(client).response_to_first_byte_time.mean_ns);
    size_t timeout_ms = aws_s3_client_get_object_hedging_timeout_ms(client);
    ASSERT_TRUE(timeout_ms < 1000);
    ASSERT_TRUE(timeout_ms > 990);

    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);
    return AWS_OP_SUCCESS;
}

/* The mock server delays part 2 of these routes by 3 s. */
static const uint64_t s_mock_server_delay_ms = 3000;
static const uint64_t s_hedging_timeout_ms = 1000;

/* The fast parts move the timeout a little toward their mean plus 700 ms before part 2 is sent. */
static const uint64_t s_min_hedging_timeout_ms = 900;

/* Completes the warmup with fast samples, so that the timeout is the 1 s minimum. */
static void s_warm_up_get_object_hedging(struct aws_s3_client *client) {
    struct aws_s3_request mock_request;
    struct aws_s3_request_metrics metrics;
    AWS_ZERO_STRUCT(mock_request);
    AWS_ZERO_STRUCT(metrics);
    mock_request.send_data.metrics = &metrics;
    s_init_mock_get_request(&mock_request, AWS_HTTP_STATUS_CODE_206_PARTIAL_CONTENT, 5, 0);
    for (size_t i = 0; i < s_controller_of(client).num_warmup_samples; ++i) {
        aws_s3_client_update_get_object_hedging(client, &mock_request, AWS_ERROR_SUCCESS);
    }
}

struct hedging_attempt_info {
    enum aws_s3_request_type request_type;
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
        aws_s3_request_metrics_get_request_type(metrics, &out_attempts[i].request_type);
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

/**
 * Checks the metrics of the attempts of `hedged_request_type`, and counts the attempts of part 2 that reached the
 * first-byte timeout. The first attempt of each part has the first-byte timeout. The attempts after a hedge have no
 * first-byte timeout. The other request types, for example CreateMultipartUpload, have no timeout. A fast part can
 * also reach the timeout while the shared mock server is busy with other tests, thus the check allows such timeouts.
 */
static int s_check_attempts(
    struct aws_s3_tester *tester,
    struct aws_s3_meta_request_test_results *results,
    enum aws_s3_request_type hedged_request_type,
    size_t *out_num_part_2_first_byte_timeouts) {

    struct hedging_attempt_info attempts[16];
    size_t num_attempts = 0;
    ASSERT_SUCCESS(s_get_attempts(tester, results, attempts, AWS_ARRAY_SIZE(attempts), &num_attempts));

    *out_num_part_2_first_byte_timeouts = 0;
    size_t num_hedged_type_attempts = 0;
    for (size_t i = 0; i < num_attempts; ++i) {
        const struct hedging_attempt_info *attempt = &attempts[i];
        if (attempt->request_type != hedged_request_type) {
            ASSERT_UINT_EQUALS(0, attempt->first_byte_timeout_ms);
            continue;
        }
        ++num_hedged_type_attempts;
        if (attempt->error_code == AWS_ERROR_HTTP_RESPONSE_FIRST_BYTE_TIMEOUT && attempt->part_number == 2) {
            ++*out_num_part_2_first_byte_timeouts;
        }
        if (attempt->retry_attempt == 0) {
            ASSERT_TRUE(attempt->first_byte_timeout_ms >= s_min_hedging_timeout_ms);
            ASSERT_TRUE(attempt->first_byte_timeout_ms <= s_hedging_timeout_ms);
        } else {
            ASSERT_UINT_EQUALS(0, attempt->first_byte_timeout_ms);
        }
    }
    ASSERT_TRUE(num_hedged_type_attempts > 0);
    return AWS_OP_SUCCESS;
}

static int s_get_object_with_hedging(
    struct aws_allocator *allocator,
    const char *object_path,
    uint64_t *out_duration_ms,
    size_t *out_num_hedges,
    size_t *out_num_part_2_first_byte_timeouts) {

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
    ASSERT_UINT_EQUALS(s_hedging_timeout_ms, aws_s3_client_get_object_hedging_timeout_ms(client));

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
    *out_num_hedges = (size_t)s_controller_of(client).num_timeouts;
    ASSERT_SUCCESS(
        s_check_attempts(&tester, &out_results, AWS_S3_REQUEST_TYPE_GET_OBJECT, out_num_part_2_first_byte_timeouts));

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
    size_t num_part_2_first_byte_timeouts = 0;
    ASSERT_SUCCESS(s_get_object_with_hedging(
        allocator, "/get_object_hedging_slow_part", &duration_ms, &num_hedges, &num_part_2_first_byte_timeouts));

    ASSERT_TRUE(num_hedges >= 1);
    ASSERT_UINT_EQUALS(1, num_part_2_first_byte_timeouts);
    ASSERT_TRUE(duration_ms >= s_min_hedging_timeout_ms);
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
    size_t num_part_2_first_byte_timeouts = 0;
    ASSERT_SUCCESS(s_get_object_with_hedging(
        allocator, "/get_object_hedging_slow_part_twice", &duration_ms, &num_hedges, &num_part_2_first_byte_timeouts));

    ASSERT_TRUE(num_hedges >= 1);
    ASSERT_UINT_EQUALS(1, num_part_2_first_byte_timeouts);
    /* A second hedge would end the download after approximately 2 s. */
    ASSERT_TRUE(duration_ms >= s_min_hedging_timeout_ms + s_mock_server_delay_ms);
    return AWS_OP_SUCCESS;
}

/* Completes the UploadPart warmup with fast requests, so that the timeout starts at the 1 s minimum. */
static void s_warm_up_upload_part_timeout(struct aws_s3_client *client) {
    struct aws_s3_request mock_request;
    struct aws_s3_request_metrics metrics;
    AWS_ZERO_STRUCT(mock_request);
    AWS_ZERO_STRUCT(metrics);
    mock_request.request_type = AWS_S3_REQUEST_TYPE_UPLOAD_PART;
    mock_request.send_data.metrics = &metrics;
    metrics.time_metrics.send_start_timestamp_ns = 0;
    metrics.time_metrics.send_end_timestamp_ns = (int64_t)s_ms_to_ns(5);
    metrics.time_metrics.receive_start_timestamp_ns = (int64_t)s_ms_to_ns(10);
    metrics.time_metrics.receive_end_timestamp_ns = (int64_t)s_ms_to_ns(10);
    for (size_t i = 0; i < client->synced_data.upload_part_timeout.num_warmup_samples; ++i) {
        aws_s3_client_update_upload_part_timeout(client, &mock_request, AWS_ERROR_SUCCESS);
    }
}

static int s_put_object_with_hedging(
    struct aws_allocator *allocator,
    const char *object_path,
    uint64_t *out_duration_ms,
    size_t *out_num_part_2_first_byte_timeouts) {

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_tester_client_options client_options = {
        .part_size = MB_TO_BYTES(5),
        .tls_usage = AWS_S3_TLS_DISABLED,
        /* A retry through the retry strategy would wait 10 s. A hedge must not wait. */
        .backoff_scale_factor_ms = 10000,
        .jitter_mode = AWS_EXPONENTIAL_BACKOFF_JITTER_NONE,
    };
    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));
    s_warm_up_upload_part_timeout(client);
    ASSERT_UINT_EQUALS(s_hedging_timeout_ms, aws_atomic_load_int(&client->upload_timeout_ms));

    struct aws_s3_tester_meta_request_options put_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .client = client,
        .put_options =
            {
                .object_size_mb = 20, /* 4 parts of 5 MiB. */
                .object_path_override = aws_byte_cursor_from_c_str(object_path),
            },
        .mock_server = true,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_SUCCESS,
    };
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);

    uint64_t start_ns = 0;
    aws_high_res_clock_get_ticks(&start_ns);
    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &put_options, &out_results));
    uint64_t end_ns = 0;
    aws_high_res_clock_get_ticks(&end_ns);

    ASSERT_UINT_EQUALS(AWS_ERROR_SUCCESS, out_results.finished_error_code);
    *out_duration_ms = aws_timestamp_convert(end_ns - start_ns, AWS_TIMESTAMP_NANOS, AWS_TIMESTAMP_MILLIS, NULL);
    ASSERT_SUCCESS(
        s_check_attempts(&tester, &out_results, AWS_S3_REQUEST_TYPE_UPLOAD_PART, out_num_part_2_first_byte_timeouts));

    /* The UploadPart controller counted the hedge. */
    aws_s3_client_lock_synced_data(client);
    uint64_t num_timeouts = client->synced_data.upload_part_timeout.num_timeouts;
    aws_s3_client_unlock_synced_data(client);
    ASSERT_TRUE(num_timeouts >= 1);

    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);
    return AWS_OP_SUCCESS;
}

/* Part 2 answers its first attempt after 3 s. The client hedges it after 1 s, and the new attempt gets a fast
 * answer. */
TEST_CASE(upload_part_hedging_mock_server) {
    (void)ctx;
    uint64_t duration_ms = 0;
    size_t num_part_2_first_byte_timeouts = 0;
    ASSERT_SUCCESS(s_put_object_with_hedging(
        allocator, "/upload_part_hedging_slow_part", &duration_ms, &num_part_2_first_byte_timeouts));

    ASSERT_UINT_EQUALS(1, num_part_2_first_byte_timeouts);
    ASSERT_TRUE(duration_ms >= s_min_hedging_timeout_ms);
    /* Shorter than the delay of the slow answer and than the 10 s backoff of the retry strategy. */
    ASSERT_TRUE(duration_ms < s_mock_server_delay_ms);
    return AWS_OP_SUCCESS;
}

/* Part 2 answers its first two attempts after 3 s. The client hedges the first attempt only, and waits for the slow
 * answer to the second attempt. */
TEST_CASE(upload_part_hedging_once_per_part_mock_server) {
    (void)ctx;
    uint64_t duration_ms = 0;
    size_t num_part_2_first_byte_timeouts = 0;
    ASSERT_SUCCESS(s_put_object_with_hedging(
        allocator, "/upload_part_hedging_slow_part_twice", &duration_ms, &num_part_2_first_byte_timeouts));

    ASSERT_UINT_EQUALS(1, num_part_2_first_byte_timeouts);
    /* A second hedge would end the upload after approximately 2 s. */
    ASSERT_TRUE(duration_ms >= s_min_hedging_timeout_ms + s_mock_server_delay_ms);
    return AWS_OP_SUCCESS;
}
