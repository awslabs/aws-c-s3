/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_util.h"
#include "aws/s3/s3_client.h"
#include "s3_tester.h"
#include <aws/io/stream.h>
#include <aws/io/uri.h>
#include <aws/testing/aws_test_harness.h>
#include <inttypes.h>

#define TEST_CASE(NAME)                                                                                                \
    AWS_TEST_CASE(NAME, s_test_##NAME);                                                                                \
    static int s_test_##NAME(struct aws_allocator *allocator, void *ctx)

#define DEFINE_HEADER(NAME, VALUE)                                                                                     \
    {                                                                                                                  \
        .name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL(NAME),                                                           \
        .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL(VALUE),                                                         \
    }

static int s_validate_time_metrics(struct aws_s3_request_metrics *metrics, bool is_last_attempt) {
    uint64_t start = 0, end = 0, duration = 0;
    int error_code = aws_s3_request_metrics_get_error_code(metrics);
    bool is_success = (error_code == AWS_ERROR_SUCCESS);

    /* Always available */
    aws_s3_request_metrics_get_s3_request_first_attempt_start_timestamp_ns(metrics, &start);
    ASSERT_TRUE(start > 0);
    /* Available on last attempt only */
    if (is_last_attempt) {
        ASSERT_SUCCESS(aws_s3_request_metrics_get_s3_request_last_attempt_end_timestamp_ns(metrics, &end));
        ASSERT_TRUE(end > 0);
        ASSERT_SUCCESS(aws_s3_request_metrics_get_s3_request_total_duration_ns(metrics, &duration));
        ASSERT_TRUE(duration > 0);
        ASSERT_UINT_EQUALS(end - start, duration);
    } else {
        ASSERT_FAILS(aws_s3_request_metrics_get_s3_request_last_attempt_end_timestamp_ns(metrics, &end));
        ASSERT_FAILS(aws_s3_request_metrics_get_s3_request_total_duration_ns(metrics, &end));
    }

    aws_s3_request_metrics_get_start_timestamp_ns(metrics, &start);
    ASSERT_TRUE(start > 0);
    aws_s3_request_metrics_get_end_timestamp_ns(metrics, &end);
    ASSERT_TRUE(end > 0);
    aws_s3_request_metrics_get_total_duration_ns(metrics, &duration);
    ASSERT_TRUE(duration > 0);
    ASSERT_UINT_EQUALS(end - start, duration);

    /* Available on last attempt with success only */
    if (is_last_attempt && is_success) {
        ASSERT_SUCCESS(aws_s3_request_metrics_get_send_start_timestamp_ns(metrics, &start));
        ASSERT_TRUE(start > 0);
        ASSERT_SUCCESS(aws_s3_request_metrics_get_send_end_timestamp_ns(metrics, &end));
        ASSERT_TRUE(end > 0);
        ASSERT_SUCCESS(aws_s3_request_metrics_get_sending_duration_ns(metrics, &duration));
        ASSERT_TRUE(duration > 0);
        ASSERT_UINT_EQUALS(end - start, duration);

        ASSERT_SUCCESS(aws_s3_request_metrics_get_receive_start_timestamp_ns(metrics, &start));
        ASSERT_TRUE(start > 0);
        ASSERT_SUCCESS(aws_s3_request_metrics_get_receive_end_timestamp_ns(metrics, &end));
        ASSERT_TRUE(end > 0);
        ASSERT_SUCCESS(aws_s3_request_metrics_get_receiving_duration_ns(metrics, &duration));
        ASSERT_TRUE(duration > 0);
        ASSERT_UINT_EQUALS(end - start, duration);

        ASSERT_SUCCESS(aws_s3_request_metrics_get_service_call_duration_ns(metrics, &duration));
        ASSERT_TRUE(duration > 0);

        ASSERT_SUCCESS(aws_s3_request_metrics_get_conn_acquire_start_timestamp_ns(metrics, &start));
        ASSERT_TRUE(start > 0);
        ASSERT_SUCCESS(aws_s3_request_metrics_get_conn_acquire_end_timestamp_ns(metrics, &end));
        ASSERT_TRUE(end > 0);
        ASSERT_SUCCESS(aws_s3_request_metrics_get_conn_acquire_duration_ns(metrics, &duration));
        ASSERT_TRUE(duration > 0);
        ASSERT_UINT_EQUALS(end - start, duration);

        ASSERT_SUCCESS(aws_s3_request_metrics_get_sign_start_timestamp_ns(metrics, &start));
        ASSERT_TRUE(start > 0);
        ASSERT_SUCCESS(aws_s3_request_metrics_get_sign_end_timestamp_ns(metrics, &end));
        ASSERT_TRUE(end > 0);
        ASSERT_SUCCESS(aws_s3_request_metrics_get_signing_duration_ns(metrics, &duration));
        ASSERT_TRUE(duration > 0);
        ASSERT_UINT_EQUALS(end - start, duration);

        if (metrics->req_resp_info_metrics.request_type == AWS_S3_REQUEST_TYPE_GET_OBJECT) {
            ASSERT_SUCCESS(aws_s3_request_metrics_get_delivery_start_timestamp_ns(metrics, &start));
            ASSERT_TRUE(start > 0);
            ASSERT_SUCCESS(aws_s3_request_metrics_get_delivery_end_timestamp_ns(metrics, &end));
            ASSERT_TRUE(end > 0);
            ASSERT_SUCCESS(aws_s3_request_metrics_get_delivery_duration_ns(metrics, &duration));
            ASSERT_TRUE(duration > 0);
            ASSERT_UINT_EQUALS(end - start, duration);
        }
    }

    if (metrics->crt_info_metrics.retry_attempt > 0) {
        ASSERT_SUCCESS(aws_s3_request_metrics_get_retry_delay_start_timestamp_ns(metrics, &start));
        ASSERT_TRUE(start > 0);
        ASSERT_SUCCESS(aws_s3_request_metrics_get_retry_delay_end_timestamp_ns(metrics, &end));
        ASSERT_TRUE(end > 0);
        ASSERT_SUCCESS(aws_s3_request_metrics_get_retry_delay_duration_ns(metrics, &duration));
        ASSERT_TRUE(duration > 0);
        ASSERT_UINT_EQUALS(end - start, duration);
    } else {
        ASSERT_FAILS(aws_s3_request_metrics_get_retry_delay_start_timestamp_ns(metrics, &start));
        ASSERT_FAILS(aws_s3_request_metrics_get_retry_delay_end_timestamp_ns(metrics, &end));
        ASSERT_FAILS(aws_s3_request_metrics_get_retry_delay_duration_ns(metrics, &duration));
    }

    return AWS_OP_SUCCESS;
}

static int s_validate_create_multipart_upload_metrics(struct aws_s3_request_metrics *metrics) {
    struct aws_http_headers *response_headers = NULL;
    ASSERT_SUCCESS(aws_s3_request_metrics_get_response_headers(metrics, &response_headers));
    const struct aws_string *request_id = NULL;
    ASSERT_SUCCESS(aws_s3_request_metrics_get_request_id(metrics, &request_id));
    ASSERT_TRUE(aws_string_eq_c_str(request_id, "12345"));
    const struct aws_string *ip_address = NULL;
    ASSERT_SUCCESS(aws_s3_request_metrics_get_ip_address(metrics, &ip_address));
    /* Should be default local ip for ipv6/ipv4 */
    ASSERT_TRUE(aws_string_eq_c_str(ip_address, "::1") || aws_string_eq_c_str(ip_address, "127.0.0.1"));
    int response_status = 0;
    ASSERT_SUCCESS(aws_s3_request_metrics_get_response_status_code(metrics, &response_status));
    ASSERT_UINT_EQUALS(200, response_status);
    uint32_t stream_id = 0;
    ASSERT_SUCCESS(aws_s3_request_metrics_get_request_stream_id(metrics, &stream_id));
    ASSERT_UINT_EQUALS(1, stream_id);
    const struct aws_string *request_path_query = NULL;
    aws_s3_request_metrics_get_request_path_query(metrics, &request_path_query);
    ASSERT_TRUE(request_path_query->len > 0);
    const struct aws_string *host_address = NULL;
    aws_s3_request_metrics_get_host_address(metrics, &host_address);
    ASSERT_TRUE(host_address->len > 0);
    aws_thread_id_t thread_id = 0;
    ASSERT_SUCCESS(aws_s3_request_metrics_get_thread_id(metrics, &thread_id));
    size_t connection_ptr = 0;
    ASSERT_SUCCESS(aws_s3_request_metrics_get_connection_id(metrics, &connection_ptr));
    ASSERT_UINT_EQUALS(AWS_ERROR_SUCCESS, aws_s3_request_metrics_get_error_code(metrics));

    /* Get all those time stamp */
    ASSERT_SUCCESS(s_validate_time_metrics(metrics, true));

    enum aws_s3_request_type request_type = 0;
    aws_s3_request_metrics_get_request_type(metrics, &request_type);
    ASSERT_UINT_EQUALS(AWS_S3_REQUEST_TYPE_CREATE_MULTIPART_UPLOAD, request_type);

    const struct aws_string *operation_name = NULL;
    ASSERT_SUCCESS(aws_s3_request_metrics_get_operation_name(metrics, &operation_name));
    ASSERT_STR_EQUALS("CreateMultipartUpload", aws_string_c_str(operation_name));

    return AWS_OP_SUCCESS;
}

static int s_validate_upload_part_metrics(struct aws_s3_request_metrics *metrics, bool is_last_attempt) {
    struct aws_http_headers *response_headers = NULL;
    struct aws_byte_cursor header_value;
    enum aws_s3_request_type request_type = 0;
    const struct aws_string *operation_name = NULL;

    AWS_ZERO_STRUCT(header_value);
    response_headers = NULL;
    if (metrics->req_resp_info_metrics.response_status != -1) {
        ASSERT_SUCCESS(aws_s3_request_metrics_get_response_headers(metrics, &response_headers));
        ASSERT_SUCCESS(aws_http_headers_get(response_headers, aws_byte_cursor_from_c_str("ETag"), &header_value));
        ASSERT_TRUE(aws_byte_cursor_eq_c_str(&header_value, "b54357faf0632cce46e942fa68356b38"));
        ASSERT_SUCCESS(aws_http_headers_get(response_headers, aws_byte_cursor_from_c_str("Connection"), &header_value));
        ASSERT_TRUE(aws_byte_cursor_eq_c_str(&header_value, "keep-alive"));
    }

    request_type = 0;
    aws_s3_request_metrics_get_request_type(metrics, &request_type);
    ASSERT_UINT_EQUALS(AWS_S3_REQUEST_TYPE_UPLOAD_PART, request_type);
    ASSERT_SUCCESS(aws_s3_request_metrics_get_operation_name(metrics, &operation_name));
    ASSERT_STR_EQUALS("UploadPart", aws_string_c_str(operation_name));

    ASSERT_SUCCESS(s_validate_time_metrics(metrics, is_last_attempt));

    return AWS_OP_SUCCESS;
}

static int s_validate_complete_multipart_upload_metrics(struct aws_s3_request_metrics *metrics) {
    enum aws_s3_request_type request_type = 0;
    const struct aws_string *operation_name = NULL;

    aws_s3_request_metrics_get_request_type(metrics, &request_type);
    ASSERT_UINT_EQUALS(AWS_S3_REQUEST_TYPE_COMPLETE_MULTIPART_UPLOAD, request_type);
    ASSERT_SUCCESS(aws_s3_request_metrics_get_operation_name(metrics, &operation_name));
    ASSERT_STR_EQUALS("CompleteMultipartUpload", aws_string_c_str(operation_name));

    ASSERT_SUCCESS(s_validate_time_metrics(metrics, true));

    return AWS_OP_SUCCESS;
}

static int s_validate_abort_multipart_upload_metrics(struct aws_s3_request_metrics *metrics) {
    enum aws_s3_request_type request_type = 0;
    const struct aws_string *operation_name = NULL;

    aws_s3_request_metrics_get_request_type(metrics, &request_type);
    ASSERT_UINT_EQUALS(AWS_S3_REQUEST_TYPE_ABORT_MULTIPART_UPLOAD, request_type);
    ASSERT_SUCCESS(aws_s3_request_metrics_get_operation_name(metrics, &operation_name));
    ASSERT_STR_EQUALS("AbortMultipartUpload", aws_string_c_str(operation_name));

    ASSERT_SUCCESS(s_validate_time_metrics(metrics, true));

    return AWS_OP_SUCCESS;
}

static int s_validate_mpu_mock_server_metrics(struct aws_array_list *metrics_list, uint32_t expected_length) {
    /* Check the size of the metrics should be the same as the number of requests, which should be create MPU, two
     * upload parts and one complete MPU */
    ASSERT_UINT_EQUALS(expected_length, aws_array_list_length(metrics_list));
    struct aws_s3_request_metrics *metrics = NULL;

    /* First metrics should be the CreateMPU */
    aws_array_list_get_at(metrics_list, (void **)&metrics, 0);
    ASSERT_SUCCESS(s_validate_create_multipart_upload_metrics(metrics));

    /* All of the middle should be Upload Parts*/
    for (size_t i = 1; i < aws_array_list_length(metrics_list) - 1; i++) {
        metrics = NULL;
        aws_array_list_get_at(metrics_list, (void **)&metrics, i);
        ASSERT_SUCCESS(s_validate_upload_part_metrics(metrics, true)); /* assuming all requests were success */
    }

    /* Last metrics should be CompleteMPU*/
    metrics = NULL;
    aws_array_list_get_at(metrics_list, (void **)&metrics, aws_array_list_length(metrics_list) - 1);
    ASSERT_SUCCESS(s_validate_complete_multipart_upload_metrics(metrics));

    return AWS_OP_SUCCESS;
}

static int s_validate_retry_metrics(struct aws_array_list *metrics_list, uint32_t expected_failures, uint32_t parts) {
    struct aws_s3_request_metrics *metrics = NULL, *metrics2 = NULL;

    /* First metrics should be the CreateMPU */
    aws_array_list_get_at(metrics_list, (void **)&metrics, 0);
    ASSERT_SUCCESS(s_validate_create_multipart_upload_metrics(metrics));

    /* All of the middle should be Upload Parts*/
    /* This check assumes each request fails 'expected_failures' number of times and then succeeds.
     * So for each part, we would have expected_failures + 1 attempts.
     * We make sure all of the attempts have the same request_first_attempt_start_time and the last attempt has an end
     * time. */
    size_t failed_count = 0;
    for (size_t i = 1; i < aws_array_list_length(metrics_list) - 1; i = i + expected_failures + 1) {
        aws_array_list_get_at(metrics_list, &metrics, i);
        for (size_t j = i; j < i + expected_failures; j++) {
            aws_array_list_get_at(metrics_list, &metrics2, j + 1);
            ASSERT_TRUE(metrics->crt_info_metrics.error_code != AWS_ERROR_SUCCESS);
            failed_count++;
            aws_array_list_get_at(metrics_list, (void **)&metrics, j);
            ASSERT_SUCCESS(s_validate_upload_part_metrics(metrics, false));
            ASSERT_INT_EQUALS(
                metrics->time_metrics.s3_request_first_attempt_start_timestamp_ns,
                metrics2->time_metrics.s3_request_first_attempt_start_timestamp_ns);
            ASSERT_INT_EQUALS(metrics->crt_info_metrics.retry_attempt + 1, metrics2->crt_info_metrics.retry_attempt);
            ASSERT_TRUE(metrics->crt_info_metrics.request_ptr == metrics2->crt_info_metrics.request_ptr);
            metrics = metrics2;
        }
        ASSERT_SUCCESS(s_validate_upload_part_metrics(metrics, true));
    }

    ASSERT_UINT_EQUALS(expected_failures * parts, failed_count);

    /* Last metrics should be CompleteMPU*/
    metrics = NULL;
    aws_array_list_get_at(metrics_list, (void **)&metrics, aws_array_list_length(metrics_list) - 1);
    ASSERT_SUCCESS(s_validate_complete_multipart_upload_metrics(metrics));

    return AWS_OP_SUCCESS;
}

static int s_validate_fail_metrics(struct aws_array_list *metrics_list, uint32_t parts) {
    struct aws_s3_request_metrics *metrics = NULL, *metrics2 = NULL;

    /* First metrics should be the CreateMPU */
    aws_array_list_get_at(metrics_list, (void **)&metrics, 0);
    ASSERT_SUCCESS(s_validate_create_multipart_upload_metrics(metrics));

    /* It is difficult to simulate forced failure and be precise about how a request fails if there are multiple
     * connections. Assuming, the first request itself is failing, all of the other parts are force cancelled. If there
     * are n parts, we would record n + 5 metrics. 5 retries for the first part alone. */

    /* First part fails 5 times */
    aws_array_list_get_at(metrics_list, &metrics, 1);
    ASSERT_TRUE(metrics->crt_info_metrics.error_code != AWS_ERROR_SUCCESS);
    size_t request_ptr = (size_t)metrics->crt_info_metrics.request_ptr;
    int64_t request_start_time = metrics->time_metrics.s3_request_first_attempt_start_timestamp_ns;
    for (size_t i = 1; i < 6; i++) {
        aws_array_list_get_at(metrics_list, &metrics2, i + 1);
        ASSERT_INT_EQUALS(
            metrics->time_metrics.s3_request_first_attempt_start_timestamp_ns,
            metrics2->time_metrics.s3_request_first_attempt_start_timestamp_ns);
        ASSERT_INT_EQUALS(metrics->crt_info_metrics.retry_attempt + 1, metrics2->crt_info_metrics.retry_attempt);
        ASSERT_TRUE(metrics2->crt_info_metrics.error_code != AWS_ERROR_SUCCESS);
        ASSERT_SUCCESS(s_validate_upload_part_metrics(metrics, false));
        ASSERT_INT_EQUALS(request_start_time, metrics2->time_metrics.s3_request_first_attempt_start_timestamp_ns);
        ASSERT_TRUE(request_ptr == (size_t)metrics2->crt_info_metrics.request_ptr);
        metrics = metrics2;
    }
    ASSERT_SUCCESS(s_validate_upload_part_metrics(metrics, true));

    /* Rest of the request should have been cancelled */
    for (size_t i = 7; i < parts + 6; i++) {
        aws_array_list_get_at(metrics_list, &metrics, i);
        ASSERT_TRUE(metrics->crt_info_metrics.error_code == AWS_ERROR_S3_CANCELED);
        ASSERT_SUCCESS(s_validate_upload_part_metrics(metrics, true));
    }

    /* Last metrics should be AbortMPU*/
    metrics = NULL;
    aws_array_list_get_at(metrics_list, (void **)&metrics, aws_array_list_length(metrics_list) - 1);
    ASSERT_SUCCESS(s_validate_abort_multipart_upload_metrics(metrics));

    return AWS_OP_SUCCESS;
}

TEST_CASE(multipart_upload_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_tester_client_options client_options = {
        .part_size = MB_TO_BYTES(5),
        .tls_usage = AWS_S3_TLS_DISABLED,
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/default");

    struct aws_s3_tester_meta_request_options put_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .client = client,
        .checksum_algorithm = AWS_SCA_CRC32,
        .validate_get_response_checksum = false,
        .put_options =
            {
                .object_size_mb = 10,
                .object_path_override = object_path,
            },
        .mock_server = true,
    };
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);
    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &put_options, &out_results));
    ASSERT_SUCCESS(
        s_validate_mpu_mock_server_metrics(&out_results.synced_data.metrics, 4 /*1 create, 1 complete, 2 parts*/));
    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

static void s_upload_part_with_n_retries(struct aws_s3_request *request, struct aws_http_message *message) {
    if (message == NULL) {
        return;
    }

    struct aws_s3_meta_request *meta_request = request->meta_request;
    struct aws_s3_client *client = meta_request->client;
    struct aws_s3_tester *tester = client->shutdown_callback_user_data; // or similar
    uint32_t n = (uint32_t)(uintptr_t)tester->user_data;

    if (request->num_times_prepared < n - 1) {
        struct aws_http_header throttle_header = {
            .name = aws_byte_cursor_from_c_str("force_throttle"),
            .value = aws_byte_cursor_from_c_str("true"),
        };
        aws_http_message_add_header(message, throttle_header);
    }
}

TEST_CASE(multipart_upload_with_n_retries_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    tester.user_data = (void *)(uintptr_t)2; /* Fail 3 (n-1) times, succeed on 4th (nth) */
    int part_size = 5;

    struct aws_s3_tester_client_options client_options = {
        .part_size = MB_TO_BYTES(part_size),
        .tls_usage = AWS_S3_TLS_DISABLED,
        .max_active_connections_override = 1,
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));
    struct aws_s3_client_vtable *patched_client_vtable = aws_s3_tester_patch_client_vtable(&tester, client, NULL);
    patched_client_vtable->after_prepare_upload_part_finish_stub = s_upload_part_with_n_retries;

    int object_size = 10;
    int parts = object_size / part_size;

    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/default");
    {
        /* 1. Trailer checksum */
        struct aws_s3_tester_meta_request_options put_options = {
            .allocator = allocator,
            .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
            .client = client,
            .checksum_algorithm = AWS_SCA_CRC32,
            .validate_get_response_checksum = false,
            .put_options =
                {
                    .object_size_mb = object_size,
                    .object_path_override = object_path,
                },
            .mock_server = true,
        };

        struct aws_s3_meta_request_test_results meta_request_test_results;

        // check if number of metrics received for each part is n
        aws_s3_meta_request_test_results_init(&meta_request_test_results, allocator);
        ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &put_options, &meta_request_test_results));
        uint32_t expected_failures = (uint32_t)(uintptr_t)tester.user_data - 1;
        ASSERT_SUCCESS(
            s_validate_retry_metrics(&meta_request_test_results.synced_data.metrics, expected_failures, parts));

        aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);
    }
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

static void s_upload_part_force_fail(struct aws_s3_request *request, struct aws_http_message *message) {
    (void)request;
    if (message == NULL) {
        return;
    }

    struct aws_http_header throttle_header = {
        .name = aws_byte_cursor_from_c_str("force_throttle"),
        .value = aws_byte_cursor_from_c_str("true"),
    };
    aws_http_message_add_header(message, throttle_header);
}

TEST_CASE(multipart_upload_failure_with_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    int part_size = 5;

    struct aws_s3_tester_client_options client_options = {
        .part_size = MB_TO_BYTES(part_size),
        .tls_usage = AWS_S3_TLS_DISABLED,
        .max_active_connections_override = 1,
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));
    struct aws_s3_client_vtable *patched_client_vtable = aws_s3_tester_patch_client_vtable(&tester, client, NULL);
    patched_client_vtable->after_prepare_upload_part_finish_stub = s_upload_part_force_fail;

    int object_size = 10;
    int parts = object_size / part_size;

    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/default");
    {
        struct aws_s3_tester_meta_request_options put_options = {
            .allocator = allocator,
            .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
            .client = client,
            .checksum_algorithm = AWS_SCA_CRC32,
            .validate_get_response_checksum = false,
            .put_options =
                {
                    .object_size_mb = object_size,
                    .object_path_override = object_path,
                },
            .mock_server = true,
            .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_NO_VALIDATE,
        };

        struct aws_s3_meta_request_test_results meta_request_test_results;

        // check if number of metrics received for each part is n
        aws_s3_meta_request_test_results_init(&meta_request_test_results, allocator);
        ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &put_options, &meta_request_test_results));
        ASSERT_SUCCESS(s_validate_fail_metrics(&meta_request_test_results.synced_data.metrics, parts));

        aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);
    }
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

/* Singleton used by tests in this file */
static struct get_requests_header_tester {
    struct aws_allocator *alloc;

    /* Store the requests headers in the array. Array of struct aws_http_headers * */
    struct aws_array_list headers_array;
    struct aws_mutex lock;
} s_get_requests_header_tester;

static int s_get_requests_header_tester_init(struct aws_allocator *alloc) {
    ASSERT_SUCCESS(aws_array_list_init_dynamic(
        &s_get_requests_header_tester.headers_array, alloc, 1, sizeof(struct aws_http_headers *)));
    ASSERT_SUCCESS(aws_mutex_init(&s_get_requests_header_tester.lock));
    return AWS_OP_SUCCESS;
}

static void s_get_requests_header_tester_clean_up(void) {
    /* iterate thought the headers array to clean up the headers */
    for (size_t i = 0; i < aws_array_list_length(&s_get_requests_header_tester.headers_array); ++i) {
        struct aws_http_headers *headers = NULL;
        aws_array_list_get_at(&s_get_requests_header_tester.headers_array, &headers, i);
        aws_http_headers_release(headers);
    }
    aws_mutex_clean_up(&s_get_requests_header_tester.lock);
    aws_array_list_clean_up(&s_get_requests_header_tester.headers_array);
}

struct aws_http_stream *s_get_requests_header_make_request(
    struct aws_http_connection *client_connection,
    const struct aws_http_make_request_options *options) {
    /**
     * Record the headers in the array.
     */
    aws_mutex_lock(&s_get_requests_header_tester.lock);
    struct aws_http_headers *headers = aws_http_message_get_headers(options->request);
    /* Keep the headers alive until we clean up the tester. */
    aws_http_headers_acquire(headers);
    aws_array_list_push_back(&s_get_requests_header_tester.headers_array, &headers);
    aws_mutex_unlock(&s_get_requests_header_tester.lock);

    struct aws_http_stream *stream = aws_http_connection_make_request(client_connection, options);
    return stream;
}

/**
 * Note: currently (Nov, 2024), S3 don't support create multipart upload anonymously.
 */
TEST_CASE(multipart_upload_unsigned_with_trailer_checksum_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    ASSERT_SUCCESS(s_get_requests_header_tester_init(allocator));

    struct aws_s3_client_config client_config = {
        .tls_mode = AWS_MR_TLS_DISABLED,
    };

    ASSERT_SUCCESS(aws_s3_tester_bind_client(&tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION));
    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);
    ASSERT_NOT_NULL(client);

    /* Patch the client vtable to record the request header */
    struct aws_s3_client_vtable *s3_client_get_requests_header_vtable = client->vtable;
    s3_client_get_requests_header_vtable->http_connection_make_request = s_get_requests_header_make_request;

    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/default");

    struct aws_s3_tester_meta_request_options put_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .client = client,
        .checksum_algorithm = AWS_SCA_CRC32,
        .validate_get_response_checksum = false,
        .put_options =
            {
                .object_size_mb = 10,
                .object_path_override = object_path,
            },
        .mock_server = true,
    };
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);
    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &put_options, &out_results));
    ASSERT_SUCCESS(
        s_validate_mpu_mock_server_metrics(&out_results.synced_data.metrics, 4 /*1 create, 1 complete, 2 parts*/));

    /**
     * Check the recorded headers.
     * 4 requests should be made:
     * - Create MPU
     * - 2 Upload Part
     * - Complete MPU
     */
    ASSERT_UINT_EQUALS(4, aws_array_list_length(&s_get_requests_header_tester.headers_array));
    struct aws_byte_cursor content_sha256_header = aws_byte_cursor_from_c_str("x-amz-content-sha256");
    struct aws_byte_cursor content_sha256_header_val;
    AWS_ZERO_STRUCT(content_sha256_header_val);
    struct aws_byte_cursor authorization_header = aws_byte_cursor_from_c_str("Authorization");
    struct aws_byte_cursor authorization_header_val;
    AWS_ZERO_STRUCT(authorization_header_val);
    /* The first request should be Create MPU, and it should not have x-amz-content-sha256 header. */
    struct aws_http_headers *headers = NULL;
    ASSERT_SUCCESS(aws_array_list_get_at(&s_get_requests_header_tester.headers_array, &headers, 0));
    /* No x-amz-content-sha256 header should be found. */
    ASSERT_FAILS(aws_http_headers_get(headers, content_sha256_header, &content_sha256_header_val));
    /* The second and third requests should be Upload Part, and it should have x-amz-content-sha256 header with
     * STREAMING-UNSIGNED-PAYLOAD-TRAILER. */
    ASSERT_SUCCESS(aws_array_list_get_at(&s_get_requests_header_tester.headers_array, &headers, 1));
    /* x-amz-content-sha256 header should be found. */
    ASSERT_SUCCESS(aws_http_headers_get(headers, content_sha256_header, &content_sha256_header_val));
    ASSERT_TRUE(
        aws_byte_cursor_eq(&content_sha256_header_val, &g_aws_signed_body_value_streaming_unsigned_payload_trailer));
    /* But the Authorization header should not be found, since we are not signing the request. */
    ASSERT_FAILS(aws_http_headers_get(headers, authorization_header, &authorization_header_val));
    ASSERT_SUCCESS(aws_array_list_get_at(&s_get_requests_header_tester.headers_array, &headers, 2));
    /* x-amz-content-sha256 header should be found. */
    ASSERT_SUCCESS(aws_http_headers_get(headers, content_sha256_header, &content_sha256_header_val));
    ASSERT_TRUE(
        aws_byte_cursor_eq(&content_sha256_header_val, &g_aws_signed_body_value_streaming_unsigned_payload_trailer));
    /* But the Authorization header should not be found, since we are not signing the request. */
    ASSERT_FAILS(aws_http_headers_get(headers, authorization_header, &authorization_header_val));
    /* The last request should be Complete MPU, and it should not have x-amz-content-sha256 header. */
    ASSERT_SUCCESS(aws_array_list_get_at(&s_get_requests_header_tester.headers_array, &headers, 3));
    /* No x-amz-content-sha256 header should be found. */
    ASSERT_FAILS(aws_http_headers_get(headers, content_sha256_header, &content_sha256_header_val));

    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);
    s_get_requests_header_tester_clean_up();

    return AWS_OP_SUCCESS;
}

TEST_CASE(single_upload_unsigned_with_trailer_checksum_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    ASSERT_SUCCESS(s_get_requests_header_tester_init(allocator));

    struct aws_s3_client_config client_config = {
        .tls_mode = AWS_MR_TLS_DISABLED,
        .part_size = 20 * 1024 * 1024,
    };

    ASSERT_SUCCESS(aws_s3_tester_bind_client(&tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION));
    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);
    ASSERT_NOT_NULL(client);

    /* Patch the client vtable to record the request header */
    struct aws_s3_client_vtable *s3_client_get_requests_header_vtable = client->vtable;
    s3_client_get_requests_header_vtable->http_connection_make_request = s_get_requests_header_make_request;

    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/default");

    struct aws_s3_tester_meta_request_options put_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .client = client,
        .checksum_algorithm = AWS_SCA_CRC32,
        .validate_get_response_checksum = false,
        .put_options =
            {
                .object_size_mb = 10,
                .object_path_override = object_path,
            },
        .mock_server = true,
    };
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);
    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &put_options, &out_results));

    /**
     * Check the recorded headers.
     * 1 request should be made:
     * - Put Object
     */
    ASSERT_UINT_EQUALS(1, aws_array_list_length(&s_get_requests_header_tester.headers_array));
    struct aws_byte_cursor content_sha256_header = aws_byte_cursor_from_c_str("x-amz-content-sha256");
    struct aws_byte_cursor content_sha256_header_val;
    AWS_ZERO_STRUCT(content_sha256_header_val);
    struct aws_byte_cursor authorization_header = aws_byte_cursor_from_c_str("Authorization");
    struct aws_byte_cursor authorization_header_val;
    AWS_ZERO_STRUCT(authorization_header_val);
    /* The request should be Put Object, and it should have x-amz-content-sha256 header and not Authorization header. */
    struct aws_http_headers *headers = NULL;
    ASSERT_SUCCESS(aws_array_list_get_at(&s_get_requests_header_tester.headers_array, &headers, 0));
    /* x-amz-content-sha256 header should be found. */
    ASSERT_SUCCESS(aws_http_headers_get(headers, content_sha256_header, &content_sha256_header_val));
    ASSERT_TRUE(
        aws_byte_cursor_eq(&content_sha256_header_val, &g_aws_signed_body_value_streaming_unsigned_payload_trailer));
    /* But the Authorization header should not be found, since we are not signing the request. */
    ASSERT_FAILS(aws_http_headers_get(headers, authorization_header, &authorization_header_val));

    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);
    s_get_requests_header_tester_clean_up();

    return AWS_OP_SUCCESS;
}

/* Tracks what a `request_body` test observed about the request body as it was being sent. */
struct request_body_tester {
    /* The caller-owned memory that was passed via `request_body`. */
    struct aws_byte_cursor caller_body;
    /* Set to true if, on any send attempt, the request body buffer did NOT point at the caller's exact buffer. */
    bool saw_different_buffer;
    /* Number of times the request was sent (initial attempt + retries). */
    size_t send_count;
    /* If non-zero, inject a `force_throttle` header until this many attempts have been made, to trigger retries. */
    size_t throttle_until_attempt;
    /* The Content-Length header value observed on the last outgoing request (SIZE_MAX if the header was absent). */
    size_t observed_content_length;
};

/* Patched http_connection_make_request that verifies the outgoing request body points at the caller's exact buffer,
 * and optionally injects a throttle header to force retries.
 * This fires on every send attempt, so it verifies that retries re-use the caller's same buffer. */
static struct aws_http_stream *s_request_body_make_request(
    struct aws_http_connection *client_connection,
    const struct aws_http_make_request_options *options) {

    struct aws_s3_connection *connection = options->user_data;
    struct aws_s3_request *request = connection->request;
    struct aws_s3_meta_request *meta_request = request->meta_request;
    /* The tester owns meta_request->user_data, so reach our body-tester via tester->user_data. */
    struct aws_s3_tester *tester = meta_request->client->shutdown_callback_user_data;
    struct request_body_tester *body_tester = tester->user_data;

    ++body_tester->send_count;

    /* The request body must point at the caller's exact buffer (same pointer and length). */
    if (request->request_body.buffer != body_tester->caller_body.ptr ||
        request->request_body.len != body_tester->caller_body.len) {
        body_tester->saw_different_buffer = true;
    }

    /* Record the Content-Length the client put on the wire, so the test can verify it was derived correctly. */
    struct aws_http_headers *headers = aws_http_message_get_headers(options->request);
    struct aws_byte_cursor content_length_value;
    if (aws_http_headers_get(headers, g_content_length_header_name, &content_length_value) == AWS_OP_SUCCESS) {
        uint64_t parsed = 0;
        if (aws_byte_cursor_utf8_parse_u64(content_length_value, &parsed) == AWS_OP_SUCCESS) {
            body_tester->observed_content_length = (size_t)parsed;
        }
    } else {
        body_tester->observed_content_length = SIZE_MAX;
    }

    /* Optionally force a throttle (503) so the client retries, re-sending the same caller-owned memory. */
    if (body_tester->send_count <= body_tester->throttle_until_attempt) {
        struct aws_http_header throttle_header = {
            .name = aws_byte_cursor_from_c_str("force_throttle"),
            .value = aws_byte_cursor_from_c_str("true"),
        };
        aws_http_message_add_header(options->request, throttle_header);
    }

    return aws_http_connection_make_request(client_connection, options);
}

/* Shared helper for `request_body` tests. */
static int s_request_body_test_helper(
    struct aws_allocator *allocator,
    struct aws_byte_cursor body,
    size_t throttle_until_attempt,
    bool omit_content_length) {
    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_tester_client_options client_options = {
        .tls_usage = AWS_S3_TLS_DISABLED,
        .max_active_connections_override = 1,
    };
    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    /* Patch the client vtable to inspect the body of each outgoing request (and inject throttles). */
    struct aws_s3_client_vtable *patched_client_vtable = aws_s3_tester_patch_client_vtable(&tester, client, NULL);
    patched_client_vtable->http_connection_make_request = s_request_body_make_request;

    struct request_body_tester body_tester = {
        .caller_body = body,
        .throttle_until_attempt = throttle_until_attempt,
        /* Sentinel: distinct from any real Content-Length (including 0) so the assertion only passes if the hook
         * actually observed the header on the wire. */
        .observed_content_length = SIZE_MAX,
    };
    tester.user_data = &body_tester;

    struct aws_uri mock_server;
    ASSERT_SUCCESS(aws_uri_init_parse(&mock_server, allocator, &g_mock_server_uri));
    struct aws_byte_cursor host_cursor = *aws_uri_authority(&mock_server);
    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/default");

    struct aws_http_message *message = aws_s3_test_put_object_request_new_without_body(
        allocator, &host_cursor, g_test_body_content_type, object_path, body.len, 0 /*flags*/);
    ASSERT_NOT_NULL(message);

    if (omit_content_length) {
        aws_http_headers_erase(aws_http_message_get_headers(message), g_content_length_header_name);
    }

    struct aws_s3_meta_request_options meta_request_options = {
        .type = AWS_S3_META_REQUEST_TYPE_DEFAULT,
        .operation_name = aws_byte_cursor_from_c_str("PutObject"),
        .message = message,
        .endpoint = &mock_server,
        .request_body = body,
    };

    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request(
        &tester, client, &meta_request_options, &out_results, AWS_S3_TESTER_SEND_META_REQUEST_EXPECT_SUCCESS));

    if (body.len > 0) {
        ASSERT_FALSE(body_tester.saw_different_buffer);
    }
    ASSERT_UINT_EQUALS(throttle_until_attempt + 1, body_tester.send_count);
    ASSERT_UINT_EQUALS(body.len, body_tester.observed_content_length);

    aws_http_message_release(message);
    aws_uri_clean_up(&mock_server);
    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

/* Happy path: a DEFAULT PutObject with `request_body` sends the body from the caller's exact buffer. */
TEST_CASE(request_body_mock_server) {
    (void)ctx;
    struct aws_byte_cursor body = aws_byte_cursor_from_c_str("test_body");
    return s_request_body_test_helper(allocator, body, 0 /*throttle_until_attempt*/, false /*omit_content_length*/);
}

/* On retry, the request body still points at the caller's same `request_body` buffer (re-used across attempts). */
TEST_CASE(request_body_with_retry_mock_server) {
    (void)ctx;
    struct aws_byte_cursor body = aws_byte_cursor_from_c_str("test_body");
    return s_request_body_test_helper(allocator, body, 2 /*throttle_until_attempt*/, false /*omit_content_length*/);
}

/* Happy path with no Content-Length header: the client must derive the length from `request_body.len`. */
TEST_CASE(request_body_no_content_length_mock_server) {
    (void)ctx;
    struct aws_byte_cursor body = aws_byte_cursor_from_c_str("test_body");
    return s_request_body_test_helper(allocator, body, 0 /*throttle_until_attempt*/, true /*omit_content_length*/);
}

/* Happy path: a zero-length `request_body` with a non-NULL pointer. */
TEST_CASE(request_body_empty_mock_server) {
    (void)ctx;
    uint8_t body_storage[1];
    struct aws_byte_cursor empty_body = {.ptr = body_storage, .len = 0};
    return s_request_body_test_helper(
        allocator, empty_body, 0 /*throttle_until_attempt*/, false /*omit_content_length*/);
}

TEST_CASE(multipart_upload_with_network_interface_names_mock_server) {
    (void)ctx;
#if defined(AWS_OS_WINDOWS)
    (void)allocator;
    return AWS_OP_SKIP;
#else
    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_byte_cursor *interface_names_array = aws_mem_calloc(allocator, 2, sizeof(struct aws_byte_cursor));
    char *localhost_interface = "\0";
#    if defined(AWS_OS_APPLE)
    localhost_interface = "lo0";
#    else
    localhost_interface = "lo";
#    endif
    interface_names_array[0] = aws_byte_cursor_from_c_str(localhost_interface);
    interface_names_array[1] = aws_byte_cursor_from_c_str(localhost_interface);

    struct aws_s3_tester_client_options client_options = {
        .part_size = MB_TO_BYTES(5),
        .tls_usage = AWS_S3_TLS_DISABLED,
        .network_interface_names_array = interface_names_array,
        .num_network_interface_names = 2,
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/default");

    struct aws_s3_tester_meta_request_options put_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .client = client,
        .checksum_algorithm = AWS_SCA_CRC32,
        .validate_get_response_checksum = false,
        .put_options =
            {
                .object_size_mb = 10,
                .object_path_override = object_path,
            },
        .mock_server = true,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_NO_VALIDATE,
    };
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);
    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &put_options, &out_results));
    if (out_results.finished_error_code != 0) {
#    if !defined(AWS_OS_APPLE) && !defined(AWS_OS_LINUX)
        if (out_results.finished_error_code == AWS_ERROR_PLATFORM_NOT_SUPPORTED) {
            return AWS_OP_SKIP;
        }
#    endif
        ASSERT_TRUE(false, "aws_s3_tester_send_meta_request_with_options(() failed");
    }
    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);
    aws_mem_release(allocator, interface_names_array);

    return AWS_OP_SUCCESS;
#endif
}

/* Total hack to flip the bytes. */
static void s_after_prepare_upload_part_finish_stub(struct aws_s3_request *request, struct aws_http_message *message) {
    (void)message;
    if (request->num_times_prepared == 0 && message != NULL) {
        struct aws_http_header throttle_header = {
            .name = aws_byte_cursor_from_c_str("force_throttle"),
            .value = aws_byte_cursor_from_c_str("true"),
        };
        aws_http_message_add_header(message, throttle_header);
    }
    if (request->num_times_prepared > 0) {
        /* mock that the body buffer was messed up in memory */
        request->request_body.buffer[1]++;
    }
}

static void s_after_prepare_upload_part_finish_stub_retry_before_finish_sending(
    struct aws_s3_request *request,
    struct aws_http_message *message) {
    if (request->num_times_prepared == 0 && message != NULL) {
        struct aws_http_header before_finish_header = {
            .name = aws_byte_cursor_from_c_str("before_finish"),
            .value = aws_byte_cursor_from_c_str("true"),
        };
        aws_http_message_add_header(message, before_finish_header);
        struct aws_http_header throttle_header = {
            .name = aws_byte_cursor_from_c_str("force_throttle"),
            .value = aws_byte_cursor_from_c_str("true"),
        };
        aws_http_message_add_header(message, throttle_header);
    }
    if (request->num_times_prepared > 0 && request->request_body.buffer != NULL) {
        /* mock that the body buffer was messed up in memory */
        request->request_body.buffer[1]++;
    }
}

/**
 * This test is built for
 * 1. The retry happens before the upload has finished.
 */
TEST_CASE(multipart_upload_checksum_with_retry_before_finish_mock_server) {
    (void)ctx;
    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_tester_client_options client_options = {
        .part_size = MB_TO_BYTES(5),
        .tls_usage = AWS_S3_TLS_DISABLED,
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));
    struct aws_s3_client_vtable *patched_client_vtable = aws_s3_tester_patch_client_vtable(&tester, client, NULL);
    patched_client_vtable->after_prepare_upload_part_finish_stub =
        s_after_prepare_upload_part_finish_stub_retry_before_finish_sending;

    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/default");
    {
        /* 1. Trailer checksum */
        struct aws_s3_tester_meta_request_options put_options = {
            .allocator = allocator,
            .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
            .client = client,
            .checksum_algorithm = AWS_SCA_CRC32,
            .validate_get_response_checksum = false,
            .put_options =
                {
                    .object_size_mb = 10,
                    .object_path_override = object_path,
                },
            .mock_server = true,
        };

        struct aws_s3_meta_request_test_results meta_request_test_results;
        aws_s3_meta_request_test_results_init(&meta_request_test_results, allocator);

        ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &put_options, &meta_request_test_results));

        ASSERT_INT_EQUALS(meta_request_test_results.upload_review.part_count, 2);
        /* Note: the data we currently generate is always the same,
         * The retry got the messed up data, while the first run never actually finish reading the bytes, so the messed
         * up data checksum got to be sent. */
        ASSERT_STR_EQUALS(
            "dKYRxA==", aws_string_c_str(meta_request_test_results.upload_review.part_checksums_array[0]));
        ASSERT_STR_EQUALS(
            "dxV2Sw==", aws_string_c_str(meta_request_test_results.upload_review.part_checksums_array[1]));
        aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);
    }
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

/**
 * This test is built for
 * 1. We had a memory leak when the retry was triggered and the checksum was calculated.
 *      The retry will initialize the checksum buffer again, but the previous one was not freed.
 * 2. We had a bug where the retry will mangle the data with the error response from server.
 * 3. Don't recalculate the checksum when retrying.
 */
TEST_CASE(multipart_upload_checksum_with_retry_mock_server) {
    (void)ctx;
    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_tester_client_options client_options = {
        .part_size = MB_TO_BYTES(5),
        .tls_usage = AWS_S3_TLS_DISABLED,
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));
    struct aws_s3_client_vtable *patched_client_vtable = aws_s3_tester_patch_client_vtable(&tester, client, NULL);
    patched_client_vtable->after_prepare_upload_part_finish_stub = s_after_prepare_upload_part_finish_stub;

    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/default");
    {
        /* 1. Trailer checksum */
        struct aws_s3_tester_meta_request_options put_options = {
            .allocator = allocator,
            .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
            .client = client,
            .checksum_algorithm = AWS_SCA_CRC32,
            .validate_get_response_checksum = false,
            .put_options =
                {
                    .object_size_mb = 10,
                    .object_path_override = object_path,
                },
            .mock_server = true,
        };

        struct aws_s3_meta_request_test_results meta_request_test_results;
        aws_s3_meta_request_test_results_init(&meta_request_test_results, allocator);

        ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &put_options, &meta_request_test_results));

        ASSERT_INT_EQUALS(meta_request_test_results.upload_review.part_count, 2);
        /* Note: the data we currently generate is always the same,
         * so make sure that retry does not mangle the data by checking the checksum value */
        ASSERT_STR_EQUALS(
            "7/xUXw==", aws_string_c_str(meta_request_test_results.upload_review.part_checksums_array[0]));
        ASSERT_STR_EQUALS(
            "PCOjcw==", aws_string_c_str(meta_request_test_results.upload_review.part_checksums_array[1]));
        aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);
    }
    {
        /* 2. header checksum */
        struct aws_s3_tester_meta_request_options put_options = {
            .allocator = allocator,
            .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
            .client = client,
            .checksum_algorithm = AWS_SCA_CRC32,
            .checksum_via_header = true,
            .validate_get_response_checksum = false,
            .put_options =
                {
                    .object_size_mb = 10,
                    .object_path_override = object_path,
                },
            .mock_server = true,
        };

        struct aws_s3_meta_request_test_results meta_request_test_results;
        aws_s3_meta_request_test_results_init(&meta_request_test_results, allocator);

        ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &put_options, &meta_request_test_results));

        ASSERT_INT_EQUALS(meta_request_test_results.upload_review.part_count, 2);
        /* Note: the data we currently generate is always the same,
         * so make sure that retry does not mangle the data by checking the checksum value */
        ASSERT_STR_EQUALS(
            "7/xUXw==", aws_string_c_str(meta_request_test_results.upload_review.part_checksums_array[0]));
        ASSERT_STR_EQUALS(
            "PCOjcw==", aws_string_c_str(meta_request_test_results.upload_review.part_checksums_array[1]));
        aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);
    }
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

TEST_CASE(multipart_upload_checksum_fio_with_retry_mock_server) {
    (void)ctx;
    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_tester_client_options client_options = {
        .part_size = MB_TO_BYTES(5),
        .tls_usage = AWS_S3_TLS_DISABLED,
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));
    struct aws_s3_client_vtable *patched_client_vtable = aws_s3_tester_patch_client_vtable(&tester, client, NULL);
    patched_client_vtable->after_prepare_upload_part_finish_stub =
        s_after_prepare_upload_part_finish_stub_retry_before_finish_sending;
    struct aws_s3_file_io_options fio_opts = {
        .should_stream = true,
        .direct_io = true,
    };
    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/default");

    /* retry with streaming upload. */
    struct aws_s3_tester_meta_request_options put_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .client = client,
        .checksum_algorithm = AWS_SCA_CRC32,
        .validate_get_response_checksum = false,
        .fio_opts = &fio_opts,
        .put_options =
            {
                .file_on_disk = true,
                .object_size_mb = 10,
                .object_path_override = object_path,
            },
        .mock_server = true,
    };

    struct aws_s3_meta_request_test_results meta_request_test_results;
    aws_s3_meta_request_test_results_init(&meta_request_test_results, allocator);

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &put_options, &meta_request_test_results));

    ASSERT_INT_EQUALS(meta_request_test_results.upload_review.part_count, 2);
    /* Note: the data we currently generate is always the same,
     * so make sure that retry does not mangle the data by checking the checksum value */
    ASSERT_STR_EQUALS("7/xUXw==", aws_string_c_str(meta_request_test_results.upload_review.part_checksums_array[0]));
    ASSERT_STR_EQUALS("PCOjcw==", aws_string_c_str(meta_request_test_results.upload_review.part_checksums_array[1]));
    aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

TEST_CASE(multipart_download_checksum_with_retry_mock_server) {
    (void)ctx;
    /**
     * We had a memory leak after the header of the request received successfully, the request failed.
     * We have allocated memory that never frees.
     */
    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_tester_client_options client_options = {
        .part_size = MB_TO_BYTES(5),
        .tls_usage = AWS_S3_TLS_DISABLED,
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));
    /* Mock server will response without fake checksum for the body */
    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/get_object_checksum_retry");

    struct aws_s3_tester_meta_request_options get_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .client = client,
        .expected_validate_checksum_alg = AWS_SCA_CRC32,
        .validate_get_response_checksum = true,
        .get_options =
            {
                .object_path = object_path,
            },
        .default_type_options =
            {
                .mode = AWS_S3_TESTER_DEFAULT_TYPE_MODE_GET,
                .operation_name = aws_byte_cursor_from_c_str("GetObject"),
            },
        .mock_server = true,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE,
    };

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &get_options, NULL));

    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

TEST_CASE(async_internal_error_from_complete_multipart_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_tester_client_options client_options = {
        .part_size = MB_TO_BYTES(5),
        .tls_usage = AWS_S3_TLS_DISABLED,
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    /* Checkout the ./mock_s3_server/CompleteMultipartUpload/async_internal_error.json for the response details */
    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/async_internal_error");

    struct aws_s3_tester_meta_request_options put_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .client = client,
        .checksum_algorithm = AWS_SCA_CRC32,
        .validate_get_response_checksum = false,
        .put_options =
            {
                .object_size_mb = 10,
                .object_path_override = object_path,
            },
        .mock_server = true,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE,
    };
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &put_options, &out_results));

    /* Internal error will be retried and failed with internal error. */
    ASSERT_UINT_EQUALS(AWS_ERROR_S3_INTERNAL_ERROR, out_results.finished_error_code);

    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

TEST_CASE(async_access_denied_from_complete_multipart_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_tester_client_options client_options = {
        .part_size = MB_TO_BYTES(5),
        .tls_usage = AWS_S3_TLS_DISABLED,
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    /* Checkout the ./mock_s3_server/CompleteMultipartUpload/async_access_denied_error.json for the response details */
    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/async_access_denied_error");

    struct aws_s3_tester_meta_request_options put_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .client = client,
        .checksum_algorithm = AWS_SCA_CRC32,
        .validate_get_response_checksum = false,
        .put_options =
            {
                .object_size_mb = 10,
                .object_path_override = object_path,
            },
        .mock_server = true,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE,
    };
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &put_options, &out_results));

    ASSERT_UINT_EQUALS(AWS_ERROR_S3_NON_RECOVERABLE_ASYNC_ERROR, out_results.finished_error_code);
    ASSERT_UINT_EQUALS(AWS_HTTP_STATUS_CODE_200_OK, out_results.finished_response_status);
    ASSERT_TRUE(out_results.error_response_body.len != 0);
    ASSERT_STR_EQUALS("CompleteMultipartUpload", aws_string_c_str(out_results.error_response_operation_name));

    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

TEST_CASE(get_object_modified_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_tester_client_options client_options = {
        .part_size = 64 * 1024,
        .tls_usage = AWS_S3_TLS_DISABLED,
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    /* Check the mock server README/GetObject Response for the response that will be received. */
    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/get_object_modified");

    struct aws_s3_tester_meta_request_options get_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .client = client,
        .get_options =
            {
                .object_path = object_path,
            },
        .mock_server = true,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE,
    };
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &get_options, &out_results));

    ASSERT_UINT_EQUALS(AWS_ERROR_S3_OBJECT_MODIFIED, out_results.finished_error_code);
    ASSERT_UINT_EQUALS(AWS_HTTP_STATUS_CODE_412_PRECONDITION_FAILED, out_results.finished_response_status);
    ASSERT_STR_EQUALS("GetObject", aws_string_c_str(out_results.error_response_operation_name));

    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

TEST_CASE(get_object_invalid_responses_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_tester_client_options client_options = {
        .part_size = 64 * 1024,
        .tls_usage = AWS_S3_TLS_DISABLED,
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    /* 1 - Mock server will response without Content-Range */
    struct aws_byte_cursor object_path =
        aws_byte_cursor_from_c_str("/get_object_invalid_response_missing_content_range");

    struct aws_s3_tester_meta_request_options get_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .client = client,
        .get_options =
            {
                .object_path = object_path,
            },
        .mock_server = true,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE,
    };
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &get_options, &out_results));

    ASSERT_UINT_EQUALS(AWS_ERROR_S3_MISSING_CONTENT_RANGE_HEADER, out_results.finished_error_code);

    /* 2 - Mock server will response without Etags */
    object_path = aws_byte_cursor_from_c_str("/get_object_invalid_response_missing_etags");
    get_options.get_options.object_path = object_path;
    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &get_options, &out_results));
    ASSERT_UINT_EQUALS(AWS_ERROR_S3_MISSING_ETAG, out_results.finished_error_code);

    /* 3 -  Mock server will response without Content-Range response for HEAD request */
    object_path = aws_byte_cursor_from_c_str("/get_object_invalid_response_missing_content_range");
    /* Put together a simple S3 Get Object request. */
    struct aws_uri mock_server;
    ASSERT_SUCCESS(aws_uri_init_parse(&mock_server, allocator, &g_mock_server_uri));
    struct aws_http_message *message =
        aws_s3_test_get_object_request_new(allocator, *aws_uri_authority(&mock_server), object_path);
    struct aws_http_header range_header = {
        .name = g_range_header_name,
        .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("bytes=-1"),
    };
    ASSERT_SUCCESS(aws_http_message_add_header(message, range_header));
    get_options.get_options.object_path = object_path;
    get_options.message = message;
    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &get_options, &out_results));
    ASSERT_UINT_EQUALS(AWS_ERROR_S3_MISSING_CONTENT_RANGE_HEADER, out_results.finished_error_code);
    aws_uri_clean_up(&mock_server);
    aws_http_message_destroy(message);

    /* Clean up */
    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

TEST_CASE(get_object_mismatch_checksum_responses_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_tester_client_options client_options = {
        .part_size = 64 * 1024,
        .tls_usage = AWS_S3_TLS_DISABLED,
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    /* Mock server will response without fake checksum for the body */
    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/get_object_unmatch_checksum_crc32");

    struct aws_s3_tester_meta_request_options get_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .client = client,
        .expected_validate_checksum_alg = AWS_SCA_CRC32,
        .validate_get_response_checksum = true,
        .get_options =
            {
                .object_path = object_path,
            },
        .default_type_options =
            {
                .mode = AWS_S3_TESTER_DEFAULT_TYPE_MODE_GET,
            },
        .mock_server = true,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE,
    };
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &get_options, &out_results));

    ASSERT_UINT_EQUALS(AWS_ERROR_S3_RESPONSE_CHECKSUM_MISMATCH, out_results.finished_error_code);
    ASSERT_UINT_EQUALS(AWS_SCA_CRC32, out_results.algorithm);

    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

/* Test that the HTTP throughput monitoring's default settings can detect dead (or absurdly slow) connections.
 * We trigger this by having the mock server delay 60 seconds before sending the response. */
TEST_CASE(get_object_throughput_failure_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_tester_client_options client_options = {
        .part_size = 64 * 1024,
        .tls_usage = AWS_S3_TLS_DISABLED,
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/get_object_delay_60s");

    struct aws_s3_tester_meta_request_options get_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .client = client,
        .get_options =
            {
                .object_path = object_path,
            },
        .default_type_options =
            {
                .mode = AWS_S3_TESTER_DEFAULT_TYPE_MODE_GET,
            },
        .mock_server = true,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE,
    };
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &get_options, &out_results));

    ASSERT_UINT_EQUALS(AWS_ERROR_HTTP_CHANNEL_THROUGHPUT_FAILURE, out_results.finished_error_code);

    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

TEST_CASE(get_object_long_error_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_tester_client_options client_options = {
        .part_size = 64 * 1024,
        .tls_usage = AWS_S3_TLS_DISABLED,
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/get_object_long_error");

    struct aws_s3_tester_meta_request_options get_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .client = client,
        .get_options =
            {
                .object_path = object_path,
            },
        .default_type_options =
            {
                .mode = AWS_S3_TESTER_DEFAULT_TYPE_MODE_GET,
            },
        .mock_server = true,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE,
    };
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &get_options, &out_results));

    ASSERT_UINT_EQUALS(AWS_ERROR_S3_INVALID_RESPONSE_STATUS, out_results.finished_error_code);

    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

static int s_test_upload_part_invalid_response_mock_server_ex(
    struct aws_allocator *allocator,
    bool async_input_stream) {

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_tester_client_options client_options = {
        .part_size = MB_TO_BYTES(5),
        .tls_usage = AWS_S3_TLS_DISABLED,
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/missing_etag");

    struct aws_s3_tester_meta_request_options put_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .client = client,
        .checksum_algorithm = AWS_SCA_CRC32,
        .validate_get_response_checksum = false,
        .put_options =
            {
                .object_size_mb = 1024, /* big, so it's likely we're still reading when failure happens */
                .object_path_override = object_path,
                .async_input_stream = async_input_stream,
            },
        .mock_server = true,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE,
    };
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &put_options, &out_results));
    ASSERT_UINT_EQUALS(AWS_ERROR_S3_MISSING_ETAG, out_results.finished_error_code);

    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

/* Test an UploadPart failing due to invalid response */
TEST_CASE(upload_part_invalid_response_mock_server) {
    (void)ctx;
    return s_test_upload_part_invalid_response_mock_server_ex(allocator, false /*async_input_stream*/);
}

/* Test an UploadPart failing due to invalid response, while uploading from an async-input-stream */
TEST_CASE(upload_part_async_invalid_response_mock_server) {
    (void)ctx;
    return s_test_upload_part_invalid_response_mock_server_ex(allocator, true /*async_input_stream*/);
}

/* Fake a MPU with 4 parts and the 2nd and 3rd have already completed and resume works fine */
TEST_CASE(resume_first_part_not_completed_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    size_t num_parts = 4;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_tester_client_options client_options = {
        .part_size = MB_TO_BYTES(8),
        .tls_usage = AWS_S3_TLS_DISABLED,
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    /* ListParts from mock server will return Etags for the 2nd and 3rd parts */
    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/resume_first_part_not_completed");
    struct aws_s3_upload_resume_token_options token_options = {
        .upload_id = aws_byte_cursor_from_c_str("upload_id"),
        .part_size = client_options.part_size,
        .total_num_parts = num_parts,
    };
    struct aws_s3_meta_request_resume_token *token =
        aws_s3_meta_request_resume_token_new_upload(allocator, &token_options);

    struct aws_s3_tester_meta_request_options put_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .client = client,
        .checksum_algorithm = AWS_SCA_CRC32,
        .validate_get_response_checksum = false,
        .put_options =
            {
                .object_size_mb = (uint32_t)num_parts * 8, /* Make sure we have exactly 4 parts */
                .object_path_override = object_path,
                .resume_token = token,
            },
        .mock_server = true,
    };
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &put_options, &out_results));
    /* Make Sure we only uploaded 2 parts. */
    /* TODO: monitor telemetry ensure this happened */

    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_meta_request_resume_token_release(token);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

/* Fake a MPU with 4 parts and the 2nd and 3rd have already completed and resume works fine with two response of
 * ListParts
 */
TEST_CASE(resume_multi_page_list_parts_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    size_t num_parts = 4;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_tester_client_options client_options = {
        .part_size = MB_TO_BYTES(8),
        .tls_usage = AWS_S3_TLS_DISABLED,
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    /* ListParts from mock server will return NextPartNumberMarker */
    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/multiple_list_parts");
    struct aws_s3_upload_resume_token_options token_options = {
        .upload_id = aws_byte_cursor_from_c_str("upload_id"),
        .part_size = client_options.part_size,
        .total_num_parts = num_parts,
    };
    struct aws_s3_meta_request_resume_token *token =
        aws_s3_meta_request_resume_token_new_upload(allocator, &token_options);

    struct aws_s3_tester_meta_request_options put_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .client = client,
        .checksum_algorithm = AWS_SCA_CRC32,
        .validate_get_response_checksum = false,
        .put_options =
            {
                .object_size_mb = (uint32_t)num_parts * 8, /* Make sure we have exactly 4 parts */
                .object_path_override = object_path,
                .resume_token = token,
            },
        .mock_server = true,
    };
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &put_options, &out_results));
    /* Make Sure we only uploaded 2 parts. */
    /* TODO: monitor telemetry ensure this happened */

    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_meta_request_resume_token_release(token);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

TEST_CASE(resume_list_parts_failed_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    size_t num_parts = 4;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_tester_client_options client_options = {
        .part_size = MB_TO_BYTES(8),
        .tls_usage = AWS_S3_TLS_DISABLED,
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/non-exist");
    struct aws_s3_upload_resume_token_options token_options = {
        .upload_id = aws_byte_cursor_from_c_str("upload_id"),
        .part_size = client_options.part_size,
        .total_num_parts = num_parts,
    };
    struct aws_s3_meta_request_resume_token *token =
        aws_s3_meta_request_resume_token_new_upload(allocator, &token_options);

    struct aws_s3_tester_meta_request_options put_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .client = client,
        .checksum_algorithm = AWS_SCA_CRC32,
        .validate_get_response_checksum = false,
        .put_options =
            {
                .object_size_mb = (uint32_t)num_parts * 8, /* Make sure we have exactly 4 parts */
                .object_path_override = object_path,
                .resume_token = token,
            },
        .mock_server = true,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE,
    };
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &put_options, &out_results));
    ASSERT_UINT_EQUALS(AWS_ERROR_S3_INVALID_RESPONSE_STATUS, out_results.finished_error_code);
    ASSERT_UINT_EQUALS(AWS_HTTP_STATUS_CODE_404_NOT_FOUND, out_results.finished_response_status);
    ASSERT_STR_EQUALS("ListParts", aws_string_c_str(out_results.error_response_operation_name));

    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_meta_request_resume_token_release(token);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

TEST_CASE(resume_after_finished_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    size_t num_parts = 4;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_tester_client_options client_options = {
        .part_size = MB_TO_BYTES(8),
        .tls_usage = AWS_S3_TLS_DISABLED,
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/non-exist");
    struct aws_s3_upload_resume_token_options token_options = {
        .upload_id = aws_byte_cursor_from_c_str("upload_id"),
        .part_size = client_options.part_size,
        .total_num_parts = num_parts,
        .num_parts_completed = num_parts,
    };
    struct aws_s3_meta_request_resume_token *token =
        aws_s3_meta_request_resume_token_new_upload(allocator, &token_options);

    struct aws_s3_tester_meta_request_options put_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .client = client,
        .checksum_algorithm = AWS_SCA_CRC32,
        .validate_get_response_checksum = false,
        .put_options =
            {
                .object_size_mb = (uint32_t)num_parts * 8, /* Make sure we have exactly 4 parts */
                .object_path_override = object_path,
                .resume_token = token,
            },
        .mock_server = true,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_NO_VALIDATE,
    };
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &put_options, &out_results));
    /* The error code should be success, but there are no headers and stuff as no request was made. */
    ASSERT_UINT_EQUALS(AWS_ERROR_SUCCESS, out_results.finished_error_code);
    /* TODO: monitor telemetry to ensure no actual data was sent */

    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_meta_request_resume_token_release(token);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

TEST_CASE(multipart_upload_proxy_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_tester_client_options client_options = {
        .part_size = MB_TO_BYTES(5),
        .tls_usage = AWS_S3_TLS_DISABLED,
        .use_proxy = true,
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/default");

    struct aws_s3_tester_meta_request_options put_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .client = client,
        .checksum_algorithm = AWS_SCA_CRC32,
        .validate_get_response_checksum = false,
        .put_options =
            {
                .object_size_mb = 10,
                .object_path_override = object_path,
            },
        .mock_server = true,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_NO_VALIDATE,
    };

    /* The request can fail if proxy is unavailable. */
    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &put_options, NULL));

    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

TEST_CASE(endpoint_override_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_tester_client_options client_options = {
        .part_size = MB_TO_BYTES(5),
        .tls_usage = AWS_S3_TLS_DISABLED,
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    /* 1 - Mock server will response without Content-Range */
    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/default");

    struct aws_s3_tester_meta_request_options put_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .client = client,
        .put_options =
            {
                .object_size_mb = 5,
                .object_path_override = object_path,
            },
        .mock_server = true,
    };

    /* Put together a simple S3 Put Object request. */
    struct aws_input_stream *input_stream =
        aws_s3_test_input_stream_new(allocator, put_options.put_options.object_size_mb);
    struct aws_http_message *message =
        aws_s3_test_put_object_request_new(allocator, NULL, object_path, g_test_body_content_type, input_stream, 0);
    ASSERT_NOT_NULL(message);

    /* 1. Create request without host and use endpoint override for the host info */
    put_options.message = message;
    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &put_options, NULL));

    /* 2. Create request with host info missmatch endpoint override */
    struct aws_http_header host_header = {
        .name = g_host_header_name,
        .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("bad_host"),
    };
    ASSERT_SUCCESS(aws_http_message_add_header(message, host_header));
    put_options.message = message;
    put_options.validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE;
    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &put_options, NULL));
    ASSERT_INT_EQUALS(2, tester.synced_data.meta_request_shutdown_count);

    /* Clean up */
    aws_http_message_destroy(message);
    aws_input_stream_release(input_stream);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

/* Test that `RequestTimeTooSkewed` will be retried */
TEST_CASE(request_time_too_skewed_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_tester_client_options client_options = {
        .part_size = MB_TO_BYTES(5),
        .tls_usage = AWS_S3_TLS_DISABLED,
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/request_time_too_skewed");
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);

    struct aws_s3_tester_meta_request_options put_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .client = client,
        .checksum_algorithm = AWS_SCA_CRC32,
        .validate_get_response_checksum = false,
        .put_options =
            {
                .object_size_mb = 10,
                .object_path_override = object_path,
            },
        .mock_server = true,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE,
    };

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &put_options, &out_results));

    ASSERT_UINT_EQUALS(AWS_ERROR_S3_REQUEST_TIME_TOO_SKEWED, out_results.finished_error_code);

    /* The default retry will max out after 5 times. So, in total, it will be 6 requests, first one and 5 retries. */
    size_t result_num = aws_array_list_length(&out_results.synced_data.metrics);
    ASSERT_UINT_EQUALS(6, result_num);

    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

TEST_CASE(request_timeout_error_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_tester_client_options client_options = {
        .part_size = MB_TO_BYTES(5),
        .tls_usage = AWS_S3_TLS_DISABLED,
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/request_timeout");
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);

    struct aws_s3_tester_meta_request_options put_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .client = client,
        .checksum_algorithm = AWS_SCA_CRC32,
        .validate_get_response_checksum = false,
        .put_options =
            {
                .object_size_mb = 10,
                .object_path_override = object_path,
            },
        .mock_server = true,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE,
    };

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &put_options, &out_results));

    ASSERT_UINT_EQUALS(AWS_ERROR_S3_REQUEST_TIMEOUT, out_results.finished_error_code);

    /* The default retry will max out after 5 times. So, in total, it will be 6 requests, first one and 5 retries. */
    size_t result_num = aws_array_list_length(&out_results.synced_data.metrics);
    ASSERT_UINT_EQUALS(6, result_num);

    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

/**
 * Pause/resume tests against the mock server: /get_object_pause_resume serves a 1MiB object
 * (Content-Range total). With 64KB part_size that's 16 range-GET parts. The mock generates
 * every body as 'a' bytes of the requested range length.
 */

#include "aws/s3/private/s3_bitmap.h"

#define PAUSE_RESUME_MOCK_OBJECT_SIZE (1024 * 1024)
#define PAUSE_RESUME_MOCK_PART_SIZE (64 * 1024)
#define PAUSE_RESUME_MOCK_TOTAL_PARTS (PAUSE_RESUME_MOCK_OBJECT_SIZE / PAUSE_RESUME_MOCK_PART_SIZE)

struct pause_resume_mock_test_data {
    struct aws_mutex mutex;
    struct aws_condition_variable c_var;
    bool execution_completed;
    bool pause_callback_invoked;
    struct aws_s3_meta_request_resume_token *resume_token;
    int pause_error_code;
    int meta_request_error_code;

    /* Delivery-side tracking */
    struct aws_atomic_var bytes_delivered;
    struct aws_atomic_var parts_delivered;
    struct aws_atomic_var num_telemetry_requests;
    size_t pause_after_n_parts;
    struct aws_atomic_var pause_initiated;
};

static void s_pause_resume_mock_test_data_init(struct pause_resume_mock_test_data *test_data) {
    AWS_ZERO_STRUCT(*test_data);
    test_data->c_var = (struct aws_condition_variable)AWS_CONDITION_VARIABLE_INIT;
    aws_mutex_init(&test_data->mutex);
    aws_atomic_init_int(&test_data->bytes_delivered, 0);
    aws_atomic_init_int(&test_data->parts_delivered, 0);
    aws_atomic_init_int(&test_data->num_telemetry_requests, 0);
    aws_atomic_init_int(&test_data->pause_initiated, false);
    test_data->pause_after_n_parts = SIZE_MAX;
}

static void s_pause_resume_mock_test_data_clean_up(struct pause_resume_mock_test_data *test_data) {
    aws_s3_meta_request_resume_token_release(test_data->resume_token);
    aws_mutex_clean_up(&test_data->mutex);
}

static void s_pause_resume_mock_finish(
    struct aws_s3_meta_request *meta_request,
    const struct aws_s3_meta_request_result *meta_request_result,
    void *user_data) {
    (void)meta_request;
    struct pause_resume_mock_test_data *test_data = user_data;
    aws_mutex_lock(&test_data->mutex);
    test_data->meta_request_error_code = meta_request_result->error_code;
    test_data->execution_completed = true;
    aws_mutex_unlock(&test_data->mutex);
    aws_condition_variable_notify_one(&test_data->c_var);
}

static bool s_pause_resume_mock_completion_predicate(void *arg) {
    struct pause_resume_mock_test_data *test_data = arg;
    return test_data->execution_completed;
}

static void s_pause_resume_mock_pause_complete(
    struct aws_s3_meta_request *meta_request,
    const struct aws_s3_meta_request_resume_token *resume_token,
    int error_code,
    void *user_data) {
    (void)meta_request;
    struct pause_resume_mock_test_data *test_data = user_data;
    aws_mutex_lock(&test_data->mutex);
    test_data->pause_callback_invoked = true;
    test_data->pause_error_code = error_code;
    if (resume_token != NULL) {
        test_data->resume_token =
            aws_s3_meta_request_resume_token_acquire((struct aws_s3_meta_request_resume_token *)resume_token);
    }
    aws_mutex_unlock(&test_data->mutex);
}

static int s_pause_resume_mock_body_callback(
    struct aws_s3_meta_request *meta_request,
    const struct aws_byte_cursor *body,
    uint64_t range_start,
    void *user_data) {
    (void)meta_request;
    (void)range_start;
    struct pause_resume_mock_test_data *test_data = user_data;
    aws_atomic_fetch_add(&test_data->bytes_delivered, body->len);
    return AWS_OP_SUCCESS;
}

/* Progress fires after a part's body was delivered: trigger pause once the threshold is reached. */
static void s_pause_resume_mock_progress_callback(
    struct aws_s3_meta_request *meta_request,
    const struct aws_s3_meta_request_progress *progress,
    void *user_data) {
    (void)progress;
    struct pause_resume_mock_test_data *test_data = user_data;

    size_t delivered = (size_t)aws_atomic_fetch_add(&test_data->parts_delivered, 1) + 1;
    if (delivered >= test_data->pause_after_n_parts) {
        size_t expected = false;
        if (aws_atomic_compare_exchange_int(&test_data->pause_initiated, &expected, true)) {
            aws_s3_meta_request_pause_async(meta_request, s_pause_resume_mock_pause_complete, test_data);
        }
    }
}

/* Pause a mock-server download after N delivered parts; assert the resume token marks EXACTLY
 * the delivered bytes (truthful token), then resume and assert the remaining bytes are delivered
 * exactly once (pause bytes + resume bytes == object size, no overlap or loss). */
TEST_CASE(get_pause_resume_exact_part_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/get_object_pause_resume");

    struct aws_uri mock_server;
    ASSERT_SUCCESS(aws_uri_init_parse(&mock_server, allocator, &g_mock_server_uri));

    /* --- Pause leg --- */
    struct pause_resume_mock_test_data pause_data;
    s_pause_resume_mock_test_data_init(&pause_data);
    pause_data.pause_after_n_parts = 3;

    struct aws_s3_tester_client_options client_options = {
        .part_size = PAUSE_RESUME_MOCK_PART_SIZE,
        .tls_usage = AWS_S3_TLS_DISABLED,
        .max_active_connections_override = 1, /* serialize parts for a deterministic pause point */
    };
    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_http_message *message =
        aws_s3_test_get_object_request_new(allocator, *aws_uri_authority(&mock_server), object_path);

    struct aws_s3_meta_request_options get_options = {
        .user_data = &pause_data,
        .finish_callback = s_pause_resume_mock_finish,
        .progress_callback = s_pause_resume_mock_progress_callback,
        .body_callback = s_pause_resume_mock_body_callback,
        .message = message,
        .type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .endpoint = &mock_server,
    };

    struct aws_s3_meta_request *meta_request = aws_s3_client_make_meta_request(client, &get_options);
    ASSERT_NOT_NULL(meta_request);

    aws_mutex_lock(&pause_data.mutex);
    aws_condition_variable_wait_pred(
        &pause_data.c_var, &pause_data.mutex, s_pause_resume_mock_completion_predicate, &pause_data);
    aws_mutex_unlock(&pause_data.mutex);

    ASSERT_INT_EQUALS(AWS_ERROR_S3_PAUSED, pause_data.meta_request_error_code);
    ASSERT_TRUE(pause_data.pause_callback_invoked);
    ASSERT_INT_EQUALS(AWS_ERROR_SUCCESS, pause_data.pause_error_code);
    ASSERT_NOT_NULL(pause_data.resume_token);

    uint64_t pause_leg_bytes = (uint64_t)aws_atomic_load_int(&pause_data.bytes_delivered);
    size_t num_completed = aws_s3_meta_request_resume_token_num_parts_completed(pause_data.resume_token);

    /* Paused at (or shortly after) the requested part */
    ASSERT_TRUE(num_completed >= pause_data.pause_after_n_parts);
    ASSERT_TRUE(num_completed < PAUSE_RESUME_MOCK_TOTAL_PARTS);

    /* Token truthfulness: marked parts must be a contiguous prefix that matches EXACTLY the bytes
     * delivered to the caller. A part whose network request finished but whose body was never
     * delivered must not be marked. */
    ASSERT_UINT_EQUALS(num_completed * (uint64_t)PAUSE_RESUME_MOCK_PART_SIZE, pause_leg_bytes);
    for (uint32_t part = 1; part <= PAUSE_RESUME_MOCK_TOTAL_PARTS; ++part) {
        bool expected_set = part <= num_completed;
        ASSERT_INT_EQUALS(expected_set, aws_s3_bitmap_get(&pause_data.resume_token->completed_parts_bitmap, part));
    }

    ASSERT_UINT_EQUALS(PAUSE_RESUME_MOCK_OBJECT_SIZE, aws_s3_meta_request_resume_token_object_size(pause_data.resume_token));

    aws_s3_meta_request_release(meta_request);
    aws_http_message_release(message);
    aws_s3_client_release(client);
    client = NULL;
    aws_s3_tester_wait_for_client_shutdown(&tester);
    tester.bound_to_client = false;

    /* --- Resume leg: exactly the missing bytes are delivered --- */
    struct pause_resume_mock_test_data resume_data;
    s_pause_resume_mock_test_data_init(&resume_data);

    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));
    message = aws_s3_test_get_object_request_new(allocator, *aws_uri_authority(&mock_server), object_path);

    struct aws_s3_meta_request_options resume_options = {
        .user_data = &resume_data,
        .finish_callback = s_pause_resume_mock_finish,
        .body_callback = s_pause_resume_mock_body_callback,
        .message = message,
        .type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .endpoint = &mock_server,
        .resume_token = pause_data.resume_token,
    };

    meta_request = aws_s3_client_make_meta_request(client, &resume_options);
    ASSERT_NOT_NULL(meta_request);

    aws_mutex_lock(&resume_data.mutex);
    aws_condition_variable_wait_pred(
        &resume_data.c_var, &resume_data.mutex, s_pause_resume_mock_completion_predicate, &resume_data);
    aws_mutex_unlock(&resume_data.mutex);

    ASSERT_INT_EQUALS(AWS_ERROR_SUCCESS, resume_data.meta_request_error_code);

    uint64_t resume_leg_bytes = (uint64_t)aws_atomic_load_int(&resume_data.bytes_delivered);
    ASSERT_UINT_EQUALS((uint64_t)PAUSE_RESUME_MOCK_OBJECT_SIZE - pause_leg_bytes, resume_leg_bytes);

    aws_s3_meta_request_release(meta_request);
    aws_http_message_release(message);
    aws_s3_client_release(client);
    aws_s3_tester_wait_for_client_shutdown(&tester);
    tester.bound_to_client = false;

    aws_uri_clean_up(&mock_server);
    s_pause_resume_mock_test_data_clean_up(&pause_data);
    s_pause_resume_mock_test_data_clean_up(&resume_data);
    aws_s3_tester_clean_up(&tester);
    return AWS_OP_SUCCESS;
}

/* Resume from a HAND-CRAFTED token with a gap in the middle: every part completed except part 2.
 * A local file is pre-created with correct content everywhere except part 2's region. The resume
 * must download ONLY part 2 and write it at the correct file offset.
 * TODO: general gap support (multiple scattered gaps interleaved with large completed runs) needs
 * broader coverage once officially supported; this exercises the single-gap case end to end. */
TEST_CASE(get_resume_gap_token_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/get_object_pause_resume");
    const char *download_filepath = "test_get_resume_gap_token_tmp";

    struct aws_uri mock_server;
    ASSERT_SUCCESS(aws_uri_init_parse(&mock_server, allocator, &g_mock_server_uri));

    /* Pre-create the local file: 'a' everywhere (matching the mock's generated bodies),
     * except part 2's region which is zeroed -- the "gap". */
    FILE *seed_file = aws_fopen(download_filepath, "wb");
    ASSERT_NOT_NULL(seed_file);
    for (uint32_t part = 1; part <= PAUSE_RESUME_MOCK_TOTAL_PARTS; ++part) {
        char fill = (part == 2) ? '\0' : 'a';
        for (size_t i = 0; i < PAUSE_RESUME_MOCK_PART_SIZE; ++i) {
            fputc(fill, seed_file);
        }
    }
    fclose(seed_file);

    /* Hand-craft the token: all parts completed except part 2. */
    uint32_t completed_parts[PAUSE_RESUME_MOCK_TOTAL_PARTS - 1];
    size_t num_completed = 0;
    for (uint32_t part = 1; part <= PAUSE_RESUME_MOCK_TOTAL_PARTS; ++part) {
        if (part != 2) {
            completed_parts[num_completed++] = part;
        }
    }
    struct aws_s3_download_resume_token_options token_options = {
        .etag = aws_byte_cursor_from_c_str("b54357faf0632cce46e942fa68356b30"),
        .part_size = PAUSE_RESUME_MOCK_PART_SIZE,
        .first_part_size = PAUSE_RESUME_MOCK_PART_SIZE,
        .object_size = PAUSE_RESUME_MOCK_OBJECT_SIZE,
        .total_num_parts = PAUSE_RESUME_MOCK_TOTAL_PARTS,
        .completed_parts = completed_parts,
        .num_completed_parts = num_completed,
        .file_last_modified_epoch_ns = 1, /* nonzero: mtime validation is not currently enforced */
    };
    struct aws_s3_meta_request_resume_token *token =
        aws_s3_meta_request_resume_token_new_download(allocator, &token_options);
    ASSERT_NOT_NULL(token);

    struct pause_resume_mock_test_data resume_data;
    s_pause_resume_mock_test_data_init(&resume_data);

    struct aws_s3_tester_client_options client_options = {
        .part_size = PAUSE_RESUME_MOCK_PART_SIZE,
        .tls_usage = AWS_S3_TLS_DISABLED,
    };
    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_http_message *message =
        aws_s3_test_get_object_request_new(allocator, *aws_uri_authority(&mock_server), object_path);

    struct aws_s3_meta_request_options resume_options = {
        .user_data = &resume_data,
        .finish_callback = s_pause_resume_mock_finish,
        .message = message,
        .type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .endpoint = &mock_server,
        .recv_filepath = aws_byte_cursor_from_c_str(download_filepath),
        .recv_file_option = AWS_S3_RECV_FILE_RESUME,
        .resume_token = token,
    };

    struct aws_s3_meta_request *meta_request = aws_s3_client_make_meta_request(client, &resume_options);
    ASSERT_NOT_NULL(meta_request);

    aws_mutex_lock(&resume_data.mutex);
    aws_condition_variable_wait_pred(
        &resume_data.c_var, &resume_data.mutex, s_pause_resume_mock_completion_predicate, &resume_data);
    aws_mutex_unlock(&resume_data.mutex);

    ASSERT_INT_EQUALS(AWS_ERROR_SUCCESS, resume_data.meta_request_error_code);

    /* Verify the gap is filled and everything else untouched: whole file must now be 'a'. */
    FILE *verify_file = aws_fopen(download_filepath, "rb");
    ASSERT_NOT_NULL(verify_file);
    fseek(verify_file, 0, SEEK_END);
    ASSERT_UINT_EQUALS(PAUSE_RESUME_MOCK_OBJECT_SIZE, (size_t)ftell(verify_file));
    fseek(verify_file, 0, SEEK_SET);
    uint8_t verify_buffer[64 * 1024];
    size_t total_read = 0;
    size_t read_len = 0;
    while ((read_len = fread(verify_buffer, 1, sizeof(verify_buffer), verify_file)) > 0) {
        for (size_t i = 0; i < read_len; ++i) {
            ASSERT_UINT_EQUALS('a', verify_buffer[i]);
        }
        total_read += read_len;
    }
    ASSERT_UINT_EQUALS(PAUSE_RESUME_MOCK_OBJECT_SIZE, total_read);
    fclose(verify_file);

    aws_s3_meta_request_release(meta_request);
    aws_http_message_release(message);
    aws_s3_client_release(client);
    aws_s3_tester_wait_for_client_shutdown(&tester);
    tester.bound_to_client = false;

    remove(download_filepath);
    aws_s3_meta_request_resume_token_release(token);
    aws_uri_clean_up(&mock_server);
    s_pause_resume_mock_test_data_clean_up(&resume_data);
    aws_s3_tester_clean_up(&tester);
    return AWS_OP_SUCCESS;
}

/* ---- Re-pause a resumed download: pause -> resume -> pause -> resume to completion ---- */
TEST_CASE(get_pause_resume_repause_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/get_object_pause_resume");
    struct aws_uri mock_server;
    ASSERT_SUCCESS(aws_uri_init_parse(&mock_server, allocator, &g_mock_server_uri));

    struct aws_s3_tester_client_options client_options = {
        .part_size = PAUSE_RESUME_MOCK_PART_SIZE,
        .tls_usage = AWS_S3_TLS_DISABLED,
        .max_active_connections_override = 1,
    };

    struct pause_resume_mock_test_data legs[3];
    struct aws_s3_meta_request_resume_token *tokens[2] = {NULL, NULL};
    uint64_t leg_bytes[3] = {0, 0, 0};
    size_t leg_pause_thresholds[3] = {3, 2, SIZE_MAX}; /* leg 3 runs to completion */

    for (size_t leg = 0; leg < 3; ++leg) {
        s_pause_resume_mock_test_data_init(&legs[leg]);
        legs[leg].pause_after_n_parts = leg_pause_thresholds[leg];

        struct aws_s3_client *client = NULL;
        ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));
        struct aws_http_message *message =
            aws_s3_test_get_object_request_new(allocator, *aws_uri_authority(&mock_server), object_path);

        struct aws_s3_meta_request_options options = {
            .user_data = &legs[leg],
            .finish_callback = s_pause_resume_mock_finish,
            .progress_callback = s_pause_resume_mock_progress_callback,
            .body_callback = s_pause_resume_mock_body_callback,
            .message = message,
            .type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
            .endpoint = &mock_server,
            .resume_token = leg > 0 ? tokens[leg - 1] : NULL,
        };

        struct aws_s3_meta_request *meta_request = aws_s3_client_make_meta_request(client, &options);
        ASSERT_NOT_NULL(meta_request);

        aws_mutex_lock(&legs[leg].mutex);
        aws_condition_variable_wait_pred(
            &legs[leg].c_var, &legs[leg].mutex, s_pause_resume_mock_completion_predicate, &legs[leg]);
        aws_mutex_unlock(&legs[leg].mutex);

        leg_bytes[leg] = (uint64_t)aws_atomic_load_int(&legs[leg].bytes_delivered);

        if (leg < 2) {
            ASSERT_INT_EQUALS(AWS_ERROR_S3_PAUSED, legs[leg].meta_request_error_code);
            ASSERT_NOT_NULL(legs[leg].resume_token);
            tokens[leg] = legs[leg].resume_token;
        } else {
            ASSERT_INT_EQUALS(AWS_ERROR_SUCCESS, legs[leg].meta_request_error_code);
        }

        aws_s3_meta_request_release(meta_request);
        aws_http_message_release(message);
        aws_s3_client_release(client);
        aws_s3_tester_wait_for_client_shutdown(&tester);
        tester.bound_to_client = false;
    }

    size_t completed_1 = aws_s3_meta_request_resume_token_num_parts_completed(tokens[0]);
    size_t completed_2 = aws_s3_meta_request_resume_token_num_parts_completed(tokens[1]);

    /* Each leg delivered exactly the parts its token claims. Token 2 must be the union of
     * token 1's restored parts and leg 2's newly streamed parts. */
    ASSERT_UINT_EQUALS(completed_1 * (uint64_t)PAUSE_RESUME_MOCK_PART_SIZE, leg_bytes[0]);
    ASSERT_UINT_EQUALS((completed_2 - completed_1) * (uint64_t)PAUSE_RESUME_MOCK_PART_SIZE, leg_bytes[1]);
    ASSERT_TRUE(completed_2 > completed_1);
    ASSERT_TRUE(completed_2 < PAUSE_RESUME_MOCK_TOTAL_PARTS);
    for (uint32_t part = 1; part <= PAUSE_RESUME_MOCK_TOTAL_PARTS; ++part) {
        ASSERT_INT_EQUALS(part <= completed_2, aws_s3_bitmap_get(&tokens[1]->completed_parts_bitmap, part));
    }

    /* Across all three legs, every byte was delivered exactly once. */
    ASSERT_UINT_EQUALS(PAUSE_RESUME_MOCK_OBJECT_SIZE, leg_bytes[0] + leg_bytes[1] + leg_bytes[2]);

    aws_uri_clean_up(&mock_server);
    for (size_t leg = 0; leg < 3; ++leg) {
        s_pause_resume_mock_test_data_clean_up(&legs[leg]);
    }
    aws_s3_tester_clean_up(&tester);
    return AWS_OP_SUCCESS;
}

/* Telemetry counter: counts every HTTP request the meta request makes. */
static void s_pause_resume_mock_count_telemetry(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request_metrics *metrics,
    void *user_data) {
    (void)meta_request;
    (void)metrics;
    struct pause_resume_mock_test_data *test_data = user_data;
    aws_atomic_fetch_add(&test_data->num_telemetry_requests, 1);
}

/* Resume from a token that says ALL parts are complete: the meta request must finish
 * successfully right away, delivering zero bytes and sending zero HTTP requests. */
TEST_CASE(get_resume_all_parts_complete_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/get_object_pause_resume");
    struct aws_uri mock_server;
    ASSERT_SUCCESS(aws_uri_init_parse(&mock_server, allocator, &g_mock_server_uri));

    uint32_t completed_parts[PAUSE_RESUME_MOCK_TOTAL_PARTS];
    for (uint32_t part = 1; part <= PAUSE_RESUME_MOCK_TOTAL_PARTS; ++part) {
        completed_parts[part - 1] = part;
    }
    struct aws_s3_download_resume_token_options token_options = {
        .etag = aws_byte_cursor_from_c_str("b54357faf0632cce46e942fa68356b30"),
        .part_size = PAUSE_RESUME_MOCK_PART_SIZE,
        .first_part_size = PAUSE_RESUME_MOCK_PART_SIZE,
        .object_size = PAUSE_RESUME_MOCK_OBJECT_SIZE,
        .total_num_parts = PAUSE_RESUME_MOCK_TOTAL_PARTS,
        .completed_parts = completed_parts,
        .num_completed_parts = PAUSE_RESUME_MOCK_TOTAL_PARTS,
    };
    struct aws_s3_meta_request_resume_token *token =
        aws_s3_meta_request_resume_token_new_download(allocator, &token_options);
    ASSERT_NOT_NULL(token);

    struct pause_resume_mock_test_data resume_data;
    s_pause_resume_mock_test_data_init(&resume_data);

    struct aws_s3_tester_client_options client_options = {
        .part_size = PAUSE_RESUME_MOCK_PART_SIZE,
        .tls_usage = AWS_S3_TLS_DISABLED,
    };
    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));
    struct aws_http_message *message =
        aws_s3_test_get_object_request_new(allocator, *aws_uri_authority(&mock_server), object_path);

    struct aws_s3_meta_request_options resume_options = {
        .user_data = &resume_data,
        .finish_callback = s_pause_resume_mock_finish,
        .body_callback = s_pause_resume_mock_body_callback,
        .telemetry_callback = s_pause_resume_mock_count_telemetry,
        .message = message,
        .type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .endpoint = &mock_server,
        .resume_token = token,
    };

    struct aws_s3_meta_request *meta_request = aws_s3_client_make_meta_request(client, &resume_options);
    ASSERT_NOT_NULL(meta_request);

    aws_mutex_lock(&resume_data.mutex);
    aws_condition_variable_wait_pred(
        &resume_data.c_var, &resume_data.mutex, s_pause_resume_mock_completion_predicate, &resume_data);
    aws_mutex_unlock(&resume_data.mutex);

    ASSERT_INT_EQUALS(AWS_ERROR_SUCCESS, resume_data.meta_request_error_code);
    ASSERT_UINT_EQUALS(0, (uint64_t)aws_atomic_load_int(&resume_data.bytes_delivered));
    /* No HTTP requests were needed: everything was already complete. */
    ASSERT_UINT_EQUALS(0, (uint64_t)aws_atomic_load_int(&resume_data.num_telemetry_requests));

    aws_s3_meta_request_release(meta_request);
    aws_http_message_release(message);
    aws_s3_client_release(client);
    aws_s3_tester_wait_for_client_shutdown(&tester);
    tester.bound_to_client = false;

    aws_s3_meta_request_resume_token_release(token);
    aws_uri_clean_up(&mock_server);
    s_pause_resume_mock_test_data_clean_up(&resume_data);
    aws_s3_tester_clean_up(&tester);
    return AWS_OP_SUCCESS;
}

/* Resume from a token with ZERO completed parts: full re-download of every byte. */
TEST_CASE(get_resume_zero_completed_token_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/get_object_pause_resume");
    struct aws_uri mock_server;
    ASSERT_SUCCESS(aws_uri_init_parse(&mock_server, allocator, &g_mock_server_uri));

    struct aws_s3_download_resume_token_options token_options = {
        .etag = aws_byte_cursor_from_c_str("b54357faf0632cce46e942fa68356b30"),
        .part_size = PAUSE_RESUME_MOCK_PART_SIZE,
        .first_part_size = PAUSE_RESUME_MOCK_PART_SIZE,
        .object_size = PAUSE_RESUME_MOCK_OBJECT_SIZE,
        .total_num_parts = PAUSE_RESUME_MOCK_TOTAL_PARTS,
        .completed_parts = NULL,
        .num_completed_parts = 0,
    };
    struct aws_s3_meta_request_resume_token *token =
        aws_s3_meta_request_resume_token_new_download(allocator, &token_options);
    ASSERT_NOT_NULL(token);

    struct pause_resume_mock_test_data resume_data;
    s_pause_resume_mock_test_data_init(&resume_data);

    struct aws_s3_tester_client_options client_options = {
        .part_size = PAUSE_RESUME_MOCK_PART_SIZE,
        .tls_usage = AWS_S3_TLS_DISABLED,
    };
    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));
    struct aws_http_message *message =
        aws_s3_test_get_object_request_new(allocator, *aws_uri_authority(&mock_server), object_path);

    struct aws_s3_meta_request_options resume_options = {
        .user_data = &resume_data,
        .finish_callback = s_pause_resume_mock_finish,
        .body_callback = s_pause_resume_mock_body_callback,
        .message = message,
        .type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .endpoint = &mock_server,
        .resume_token = token,
    };

    struct aws_s3_meta_request *meta_request = aws_s3_client_make_meta_request(client, &resume_options);
    ASSERT_NOT_NULL(meta_request);

    aws_mutex_lock(&resume_data.mutex);
    aws_condition_variable_wait_pred(
        &resume_data.c_var, &resume_data.mutex, s_pause_resume_mock_completion_predicate, &resume_data);
    aws_mutex_unlock(&resume_data.mutex);

    ASSERT_INT_EQUALS(AWS_ERROR_SUCCESS, resume_data.meta_request_error_code);
    ASSERT_UINT_EQUALS(PAUSE_RESUME_MOCK_OBJECT_SIZE, (uint64_t)aws_atomic_load_int(&resume_data.bytes_delivered));

    aws_s3_meta_request_release(meta_request);
    aws_http_message_release(message);
    aws_s3_client_release(client);
    aws_s3_tester_wait_for_client_shutdown(&tester);
    tester.bound_to_client = false;

    aws_s3_meta_request_resume_token_release(token);
    aws_uri_clean_up(&mock_server);
    s_pause_resume_mock_test_data_clean_up(&resume_data);
    aws_s3_tester_clean_up(&tester);
    return AWS_OP_SUCCESS;
}

/* Body callback used by the backpressure test: pause once the read window's worth of bytes
 * has been delivered (mid part 2). */
static int s_pause_backpressure_body_callback(
    struct aws_s3_meta_request *meta_request,
    const struct aws_byte_cursor *body,
    uint64_t range_start,
    void *user_data) {
    (void)range_start;
    struct pause_resume_mock_test_data *test_data = user_data;
    size_t total = aws_atomic_fetch_add(&test_data->bytes_delivered, body->len) + body->len;

    if (total >= test_data->pause_after_n_parts /* reused as byte threshold */) {
        size_t expected = false;
        if (aws_atomic_compare_exchange_int(&test_data->pause_initiated, &expected, true)) {
            aws_s3_meta_request_pause_async(meta_request, s_pause_resume_mock_pause_complete, test_data);
        }
    }
    return AWS_OP_SUCCESS;
}

/* Backpressure + pause: with a read window of 1.5 parts and no window increments, part 1 is
 * fully delivered and part 2 only half delivered when the pause fires. The token must mark
 * ONLY part 1 (a partially delivered part is not resumable), so the resume re-delivers part 2
 * in full: the half-part delivered before the pause is the expected, documented overlap. */
TEST_CASE(get_pause_backpressure_partial_part_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/get_object_pause_resume");
    struct aws_uri mock_server;
    ASSERT_SUCCESS(aws_uri_init_parse(&mock_server, allocator, &g_mock_server_uri));

    const size_t window_size = PAUSE_RESUME_MOCK_PART_SIZE + PAUSE_RESUME_MOCK_PART_SIZE / 2;

    /* --- Pause leg: backpressure client, window = 1.5 parts, never incremented --- */
    struct pause_resume_mock_test_data pause_data;
    s_pause_resume_mock_test_data_init(&pause_data);
    pause_data.pause_after_n_parts = window_size; /* byte threshold for the body-callback trigger */

    struct aws_s3_client_config client_config = {
        .part_size = PAUSE_RESUME_MOCK_PART_SIZE,
        .tls_mode = AWS_MR_TLS_DISABLED,
        .enable_read_backpressure = true,
        .initial_read_window = window_size,
        .max_active_connections_override = 1,
    };
    ASSERT_SUCCESS(aws_s3_tester_bind_client(
        &tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION | AWS_S3_TESTER_BIND_CLIENT_SIGNING));
    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);
    ASSERT_NOT_NULL(client);

    struct aws_http_message *message =
        aws_s3_test_get_object_request_new(allocator, *aws_uri_authority(&mock_server), object_path);

    struct aws_s3_meta_request_options get_options = {
        .user_data = &pause_data,
        .finish_callback = s_pause_resume_mock_finish,
        .body_callback = s_pause_backpressure_body_callback,
        .message = message,
        .type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .endpoint = &mock_server,
    };

    struct aws_s3_meta_request *meta_request = aws_s3_client_make_meta_request(client, &get_options);
    ASSERT_NOT_NULL(meta_request);

    aws_mutex_lock(&pause_data.mutex);
    aws_condition_variable_wait_pred(
        &pause_data.c_var, &pause_data.mutex, s_pause_resume_mock_completion_predicate, &pause_data);
    aws_mutex_unlock(&pause_data.mutex);

    ASSERT_INT_EQUALS(AWS_ERROR_S3_PAUSED, pause_data.meta_request_error_code);
    ASSERT_NOT_NULL(pause_data.resume_token);

    uint64_t pause_leg_bytes = (uint64_t)aws_atomic_load_int(&pause_data.bytes_delivered);
    ASSERT_UINT_EQUALS(window_size, pause_leg_bytes); /* 1.5 parts delivered */
    /* Only the FULLY delivered part is marked */
    ASSERT_UINT_EQUALS(1, aws_s3_meta_request_resume_token_num_parts_completed(pause_data.resume_token));

    aws_s3_meta_request_release(meta_request);
    aws_http_message_release(message);
    aws_s3_client_release(client);
    aws_s3_tester_wait_for_client_shutdown(&tester);
    tester.bound_to_client = false;

    /* --- Resume leg: normal client, no backpressure --- */
    struct pause_resume_mock_test_data resume_data;
    s_pause_resume_mock_test_data_init(&resume_data);

    struct aws_s3_tester_client_options client_options = {
        .part_size = PAUSE_RESUME_MOCK_PART_SIZE,
        .tls_usage = AWS_S3_TLS_DISABLED,
    };
    struct aws_s3_client *resume_client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &resume_client));
    message = aws_s3_test_get_object_request_new(allocator, *aws_uri_authority(&mock_server), object_path);

    struct aws_s3_meta_request_options resume_options = {
        .user_data = &resume_data,
        .finish_callback = s_pause_resume_mock_finish,
        .body_callback = s_pause_resume_mock_body_callback,
        .message = message,
        .type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .endpoint = &mock_server,
        .resume_token = pause_data.resume_token,
    };

    meta_request = aws_s3_client_make_meta_request(resume_client, &resume_options);
    ASSERT_NOT_NULL(meta_request);

    aws_mutex_lock(&resume_data.mutex);
    aws_condition_variable_wait_pred(
        &resume_data.c_var, &resume_data.mutex, s_pause_resume_mock_completion_predicate, &resume_data);
    aws_mutex_unlock(&resume_data.mutex);

    ASSERT_INT_EQUALS(AWS_ERROR_SUCCESS, resume_data.meta_request_error_code);

    /* Resume re-delivers all 15 unmarked parts in full. The half of part 2 that was delivered
     * before the pause is delivered again: overlap == pause_bytes - 1 full part. */
    uint64_t resume_leg_bytes = (uint64_t)aws_atomic_load_int(&resume_data.bytes_delivered);
    ASSERT_UINT_EQUALS(
        (uint64_t)(PAUSE_RESUME_MOCK_TOTAL_PARTS - 1) * PAUSE_RESUME_MOCK_PART_SIZE, resume_leg_bytes);

    aws_s3_meta_request_release(meta_request);
    aws_http_message_release(message);
    aws_s3_client_release(resume_client);
    aws_s3_tester_wait_for_client_shutdown(&tester);
    tester.bound_to_client = false;

    aws_uri_clean_up(&mock_server);
    s_pause_resume_mock_test_data_clean_up(&pause_data);
    s_pause_resume_mock_test_data_clean_up(&resume_data);
    aws_s3_tester_clean_up(&tester);
    return AWS_OP_SUCCESS;
}
