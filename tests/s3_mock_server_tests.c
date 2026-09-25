/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_auto_ranged_get.h"
#include "aws/s3/private/s3_client_impl.h"
#include "aws/s3/private/s3_meta_request_impl.h"
#include "aws/s3/private/s3_request.h"
#include "aws/s3/private/s3_util.h"
#include "aws/s3/s3_client.h"
#include "s3_tester.h"
#include <aws/common/environment.h>
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

/* Matches the object size the mock server reports for /get_object_parallel_write: three 64 KiB parts
 * plus a 3392 byte unaligned tail. Deliberately NOT the shared geometry below -- this is the one route
 * whose last part is unaligned, which is what exercises the O_DIRECT tail path. */
#define S_PARALLEL_WRITE_OBJECT_SIZE 200000

/* The geometry every other GET route in this file serves: a 256 KiB object in four aligned 64 KiB
 * parts. Shared rather than redeclared per test because it is one shape, not several -- these routes
 * are all backed by the same two fixtures (get_object_parallel_write_normal_part.json and
 * get_object_parallel_write_delayed_part.json), so a change to the geometry has to move together:
 *
 *   /get_object_parallel_write_aligned          all parts served immediately
 *   /get_object_parallel_write_delay_part       part 2 delayed, runs to completion
 *   /get_object_parallel_write_empty_part       part 2 returns an empty body
 *   /get_object_pause_delay_part_positional     part 2 delayed long enough to pause mid-download
 *
 * Every part's file offset and length is page-aligned on a 4 KiB page, so a transfer stays on O_DIRECT
 * and recv_file_direct_io_fallback_count stays 0 -- which is what makes that counter usable as proof
 * that direct I/O was really used. */
#define S_PART_SIZE (64 * 1024)
#define S_PART_COUNT 4
#define S_OBJECT_SIZE ((uint64_t)S_PART_COUNT * S_PART_SIZE)

/* The part the mock server delays, on the routes that delay one. 1-based, matching
 * aws_s3_request_metrics_get_part_number. */
#define S_DELAYED_PART_NUMBER 2

/* Byte the mock server serves at object offset `offset`, matching its position-derived generator. */
static uint8_t s_positional_byte(uint64_t offset) {
    return (uint8_t)(32 + (offset % 90));
}

/* aws_s3_tester's pre_exist_file_length fills the pre-existing file with this. */
#define S_PRE_EXIST_FILL 'a'

/* Assert the received file is `expected_len` bytes, that its first `prefix_len` bytes are the untouched
 * pre-existing fill, and that every byte after that is the object byte for `object_offset_origin + i`.
 *
 * The prefix check is what catches a part written at the wrong file offset: the fill is a constant run
 * while the object bytes vary every byte, so an object part landing inside the prefix shows up
 * immediately. Reports the first wrong offset, since which offset is wrong identifies the bad part. */
static int s_check_recv_file_content(
    struct aws_s3_meta_request_test_results *out_results,
    uint64_t expected_len,
    uint64_t prefix_len,
    uint64_t object_offset_origin) {

    ASSERT_UINT_EQUALS(expected_len, out_results->received_file_size);
    ASSERT_UINT_EQUALS(expected_len, out_results->received_file_content.len);

    for (uint64_t i = 0; i < out_results->received_file_content.len; ++i) {
        uint8_t expected =
            i < prefix_len ? (uint8_t)S_PRE_EXIST_FILL : s_positional_byte(object_offset_origin + (i - prefix_len));
        if (out_results->received_file_content.buffer[i] != expected) {
            AWS_LOGF_ERROR(
                AWS_LS_S3_GENERAL,
                "First wrong byte at file offset %" PRIu64 " (prefix_len %" PRIu64 ", object origin %" PRIu64
                "): expected %u, got %u",
                i,
                prefix_len,
                object_offset_origin,
                (unsigned)expected,
                (unsigned)out_results->received_file_content.buffer[i]);
            ASSERT_UINT_EQUALS(expected, out_results->received_file_content.buffer[i]);
        }
    }

    return AWS_OP_SUCCESS;
}

/* Assert the transfer really ran on the parallel out-of-order path with O_DIRECT, so a test that means
 * to cover those cannot quietly pass having used the ordered or buffered path instead. The direct-I/O
 * expectation follows the platform, matching how the other direct-I/O tests phrase it. */
static int s_check_direct_io_out_of_order(
    struct aws_s3_meta_request_test_results *out_results,
    size_t expected_part_count) {

    ASSERT_TRUE(out_results->out_of_order_delivery);

    if (aws_file_direct_io_is_supported()) {
        /* Both, not just the counter: a path that abandoned direct I/O without recording a fallback
         * would leave the count at 0, so the count alone cannot tell "used O_DIRECT throughout" from
         * "never used O_DIRECT at all". The flag is what the writers consult when opening. */
        ASSERT_TRUE(out_results->recv_file_direct_io);
        ASSERT_UINT_EQUALS(0, out_results->recv_file_direct_io_fallback_count);
    } else {
        ASSERT_FALSE(out_results->recv_file_direct_io);
        ASSERT_UINT_EQUALS(1, out_results->recv_file_direct_io_fallback_count);
    }

    ASSERT_UINT_EQUALS(expected_part_count, aws_array_list_length(&out_results->synced_data.succeed_metrics));

    return AWS_OP_SUCCESS;
}

/* Ranged-GET geometry over /get_object_parallel_write_aligned. Both ranges below span exactly three
 * whole parts, so every file offset AND every write length stays page-aligned and the direct-I/O
 * fallback counter stays a clean signal. A range running to the object's end would instead leave a
 * short final part, whose unaligned length takes the per-write buffered fallback and would bump that
 * same counter for a reason unrelated to what these tests check. */
#define S_RANGED_PART_COUNT 3
#define S_RANGED_LENGTH ((uint64_t)S_RANGED_PART_COUNT * S_PART_SIZE)

/* Starts at part 2's boundary and runs to the object's last byte: 262143 - 65536 + 1 == S_RANGED_LENGTH. */
#define S_RANGED_ALIGNED_START ((uint64_t)65536)
#define S_RANGED_ALIGNED_RANGE "bytes=65536-262143"

/* Deliberately not page-aligned: 197607 - 1000 + 1 == S_RANGED_LENGTH. The file offsets stay aligned
 * anyway because they are measured from the range start, which is the property under test. */
#define S_RANGED_UNALIGNED_START ((uint64_t)1000)
#define S_RANGED_UNALIGNED_RANGE "bytes=1000-197607"

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

TEST_CASE(multipart_upload_meta_request_part_size_over_memory_limit_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    /* Client keeps the default part size; only the memory limit is constrained. */
    struct aws_s3_tester_client_options client_options = {
        .tls_usage = AWS_S3_TLS_DISABLED,
        .memory_limit_in_bytes = MB_TO_BYTES(256),
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/default");
    struct aws_s3_file_io_options fio_options = {
        .should_stream = true,
    };

    struct aws_s3_tester_meta_request_options put_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .client = client,
        .fio_opts = &fio_options,
        /* Meta-request-level part size override: this is what ends up as
         * meta_request->part_size, and therefore as the buffer pool reserve size. */
        .part_size = MB_TO_BYTES(256),
        .put_options =
            {
                .file_on_disk = true,
                .object_size_mb = 300,
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

TEST_CASE(multipart_upload_meta_request_part_size_over_memory_limit_no_stream_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_tester_client_options client_options = {
        .tls_usage = AWS_S3_TLS_DISABLED,
        .memory_limit_in_bytes = MB_TO_BYTES(256),
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/default");

    struct aws_s3_tester_meta_request_options put_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .client = client,
        /* No .fio_opts: streaming is off, so the full part size is reserved. */
        .part_size = MB_TO_BYTES(256),
        .put_options =
            {
                .file_on_disk = true,
                .object_size_mb = 300,
                .object_path_override = object_path,
            },
        .mock_server = true,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE,
    };
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);
    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &put_options, &out_results));
    ASSERT_INT_EQUALS(AWS_ERROR_S3_PART_SIZE_EXCEEDS_MEMORY_LIMIT, out_results.finished_error_code);
    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

TEST_CASE(multipart_upload_meta_request_part_size_over_memory_limit_not_on_disk_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_tester_client_options client_options = {
        .tls_usage = AWS_S3_TLS_DISABLED,
        .memory_limit_in_bytes = MB_TO_BYTES(256),
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/default");
    struct aws_s3_file_io_options fio_options = {
        .should_stream = true,
    };

    struct aws_s3_tester_meta_request_options put_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .client = client,
        .fio_opts = &fio_options,
        .part_size = MB_TO_BYTES(256),
        .put_options =
            {
                /* No .file_on_disk: no parallel read stream, so streaming cannot engage. */
                .object_size_mb = 300,
                .object_path_override = object_path,
            },
        .mock_server = true,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE,
    };
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);
    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &put_options, &out_results));
    ASSERT_INT_EQUALS(AWS_ERROR_S3_PART_SIZE_EXCEEDS_MEMORY_LIMIT, out_results.finished_error_code);
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

/* http_manager_metrics is a snapshot of the endpoint's connection manager taken right before this request
 * asks for a connection - it must reflect state from *other* requests, never this request's own acquire.
 *
 * Force the client down to a single connection, so a 2-part multipart upload's later requests
 * (UploadPart #2, CompleteMultipartUpload) can only proceed once the sole connection has been released back
 * to the idle pool by whichever request held it before - never by acquiring a second connection. */
TEST_CASE(request_metrics_http_manager_metrics_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_tester_client_options client_options = {
        /* g_s3_min_upload_part_size clamps any smaller override up to 5MiB, so use that as the part size. */
        .part_size = MB_TO_BYTES(5),
        .tls_usage = AWS_S3_TLS_DISABLED,
        .max_active_connections_override = 1,
    };
    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/default");
    struct aws_s3_tester_meta_request_options put_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .client = client,
        .put_options =
            {
                .object_size_mb = 10, /* 2 parts of 5 MiB. */
                .object_path_override = object_path,
            },
        .mock_server = true,
    };

    struct aws_s3_meta_request_test_results results;
    aws_s3_meta_request_test_results_init(&results, allocator);
    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &put_options, &results));

    /* CreateMultipartUpload, UploadPart x2, CompleteMultipartUpload */
    size_t num_requests = aws_array_list_length(&results.synced_data.metrics);
    ASSERT_UINT_EQUALS(4, num_requests);

    /* CreateMultipartUpload: the very first request ever made on this client, so the connection manager has
     * never leased or idled a connection yet. If the snapshot were (incorrectly) taken after this request's
     * own connection was acquired instead of before, leased_concurrency would be 1 here instead of 0. */
    struct aws_s3_request_metrics *first_metrics = NULL;
    aws_array_list_get_at(&results.synced_data.metrics, &first_metrics, 0);

    struct aws_http_manager_metrics first_manager_metrics;
    aws_s3_request_metrics_get_http_manager_metrics(first_metrics, &first_manager_metrics);
    ASSERT_UINT_EQUALS(0, first_manager_metrics.leased_concurrency);
    ASSERT_UINT_EQUALS(0, first_manager_metrics.available_concurrency);
    ASSERT_UINT_EQUALS(0, first_manager_metrics.pending_concurrency_acquires);

    /* UploadPart #1, UploadPart #2, CompleteMultipartUpload: with only 1 connection allowed, each of these
     * had to wait for the prior request's connection to be released back to the idle pool before it could
     * proceed, so each should see that single connection sitting idle and available, not leased. */
    for (size_t i = 1; i < num_requests; i++) {
        struct aws_s3_request_metrics *metrics = NULL;
        aws_array_list_get_at(&results.synced_data.metrics, &metrics, i);

        struct aws_http_manager_metrics manager_metrics;
        aws_s3_request_metrics_get_http_manager_metrics(metrics, &manager_metrics);
        ASSERT_UINT_EQUALS(0, manager_metrics.leased_concurrency);
        ASSERT_UINT_EQUALS(1, manager_metrics.available_concurrency);
    }

    aws_s3_meta_request_test_results_clean_up(&results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

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

TEST_CASE(get_object_opaque_etag_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_tester_client_options client_options = {
        .part_size = 64 * 1024,
        .tls_usage = AWS_S3_TLS_DISABLED,
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    /* Mock server returns a 256 KiB object whose ETag is opaque and cannot be
     * parsed into a part count. The download must still succeed using the fallback part size. */
    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/get_object_opaque_etag");

    struct aws_s3_tester_meta_request_options get_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .client = client,
        .get_options =
            {
                .object_path = object_path,
            },
        .mock_server = true,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_SUCCESS,
    };
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &get_options, &out_results));
    ASSERT_UINT_EQUALS(AWS_ERROR_SUCCESS, out_results.finished_error_code);
    ASSERT_UINT_EQUALS(262144, out_results.received_body_size);

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

/* Downloads a 256 KiB object in four 64 KiB parts, where the whole-object CRC32 is only advertised on
 * the HEAD response. Each part's CRC is computed on its own connection and the four are combined, so a
 * correct result proves the combined checksum equals the checksum of the concatenated object.
 *
 * `object_path` selects whether parts complete in order or with part 2 delayed. */
static int s_test_multipart_download_checksum_combine(
    struct aws_allocator *allocator,
    struct aws_byte_cursor object_path) {

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_tester_client_options client_options = {
        .part_size = 64 * 1024,
        .tls_usage = AWS_S3_TLS_DISABLED,
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

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
        .mock_server = true,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_SUCCESS,
    };
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &get_options, &out_results));

    ASSERT_UINT_EQUALS(AWS_ERROR_SUCCESS, out_results.finished_error_code);
    ASSERT_UINT_EQUALS(AWS_SCA_CRC32, out_results.algorithm);
    /* All 262144 bytes must have been delivered, otherwise a passing checksum would be meaningless. */
    ASSERT_UINT_EQUALS(262144, out_results.received_body_size);

    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

TEST_CASE(multipart_download_checksum_combine_mock_server) {
    (void)ctx;
    return s_test_multipart_download_checksum_combine(
        allocator, aws_byte_cursor_from_c_str("/get_object_checksum_combine"));
}

TEST_CASE(multipart_download_checksum_combine_out_of_order_mock_server) {
    (void)ctx;
    /* Part 2 is served slowly, so parts 3 and 4 finish first and must wait in the combine queue.
     * Combining out of arrival order has to produce the same digest as combining in order. */
    return s_test_multipart_download_checksum_combine(
        allocator, aws_byte_cursor_from_c_str("/get_object_checksum_combine_out_of_order"));
}

/* A combinable whole-object checksum does NOT force ordered delivery: each part folds its own digest in
 * at finish, so the body never has to be hashed in object order. Downloads to a file, the sink that
 * delivers out of order by default, and asserts both that the combined checksum still validated and that
 * delivery really was out of order.
 *
 * Pins where the delivery order is resolved. Whether a whole-object checksum can be folded per part is
 * settled by aws_s3_meta_request_setup_checksum_combine_synced, under the meta request lock; resolving the
 * delivery order any earlier reads "not combinable" and quietly demotes this download to ordered delivery
 * while still producing a correct file, which nothing else here would notice. */
TEST_CASE(download_checksum_combine_delivers_out_of_order_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_tester_client_options client_options = {
        .part_size = 64 * 1024,
        .tls_usage = AWS_S3_TLS_DISABLED,
    };
    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_s3_tester_meta_request_options get_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .client = client,
        .expected_validate_checksum_alg = AWS_SCA_CRC32,
        .validate_get_response_checksum = true,
        .get_options =
            {
                .object_path = aws_byte_cursor_from_c_str("/get_object_checksum_combine"),
                .file_on_disk = true,
                .recv_file_option = AWS_S3_RECV_FILE_CREATE_OR_REPLACE,
            },
        .mock_server = true,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_SUCCESS,
    };
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &get_options, &out_results));

    ASSERT_UINT_EQUALS(AWS_ERROR_SUCCESS, out_results.finished_error_code);
    /* The whole-object checksum was folded from the parts and matched. */
    ASSERT_UINT_EQUALS(AWS_SCA_CRC32, out_results.algorithm);
    ASSERT_TRUE(out_results.did_validate);
    ASSERT_UINT_EQUALS(262144, out_results.received_file_size);

    /* And it got there without giving up out-of-order delivery. */
    ASSERT_TRUE(out_results.out_of_order_delivery);

    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

/* A 64 KiB object that downloads as a single part, where the part response carries the whole-object
 * CRC32 as its own checksum header with the correct value. That part is therefore both validated
 * against its own header and folded into the whole-object checksum, and both must succeed. */
TEST_CASE(download_checksum_single_part_with_part_header_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_tester_client_options client_options = {
        .part_size = 64 * 1024,
        .tls_usage = AWS_S3_TLS_DISABLED,
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_s3_tester_meta_request_options get_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .client = client,
        .expected_validate_checksum_alg = AWS_SCA_CRC32,
        .validate_get_response_checksum = true,
        .get_options =
            {
                .object_path = aws_byte_cursor_from_c_str("/get_object_checksum_single_part"),
            },
        .mock_server = true,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_SUCCESS,
    };
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &get_options, &out_results));

    ASSERT_UINT_EQUALS(AWS_ERROR_SUCCESS, out_results.finished_error_code);
    ASSERT_UINT_EQUALS(AWS_SCA_CRC32, out_results.algorithm);
    ASSERT_UINT_EQUALS(65536, out_results.received_body_size);

    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

/* Download to a file with parts written out of object order, and check every byte landed where it
 * belongs.
 *
 * Mock object: 200000 bytes served in 64 KiB parts -- three full parts plus a 3392 byte tail, so the
 * last write is unaligned and takes the buffered path even when the others use O_DIRECT.
 *
 * The mock's body bytes encode their own object offset (byte at offset o is 32 + o % 90). That is what
 * makes this a positional test: with filler bytes a part written to the wrong offset would still
 * compare equal, and only a wrong total size would be caught. The modulus is not a divisor of the part
 * size, so consecutive parts start at different phases. */
TEST_CASE(parallel_write_to_file_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_tester_client_options client_options = {
        .part_size = 64 * 1024,
        .tls_usage = AWS_S3_TLS_DISABLED,
    };
    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_s3_tester_meta_request_options get_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .client = client,
        .get_options =
            {
                .object_path = aws_byte_cursor_from_c_str("/get_object_parallel_write"),
                .file_on_disk = true,
                .recv_file_option = AWS_S3_RECV_FILE_CREATE_OR_REPLACE,
                .capture_file_content = true,
            },
        .mock_server = true,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_SUCCESS,
    };
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &get_options, &out_results));
    ASSERT_UINT_EQUALS(AWS_ERROR_SUCCESS, out_results.finished_error_code);

    /* Without this the rest of the test would still pass on the ordered path, proving nothing about
     * out-of-order writes. */
    ASSERT_TRUE(out_results.out_of_order_delivery);

    ASSERT_UINT_EQUALS(S_PARALLEL_WRITE_OBJECT_SIZE, out_results.received_file_size);
    ASSERT_UINT_EQUALS(S_PARALLEL_WRITE_OBJECT_SIZE, out_results.received_file_content.len);

    /* Four ranged GETs, so the tail really was a short fourth part rather than the object having been
     * fetched in one piece -- which would make the unaligned-tail coverage claim untrue. */
    ASSERT_UINT_EQUALS(4, aws_array_list_length(&out_results.synced_data.succeed_metrics));

    /* Report the first wrong offset rather than just that the file differs, since which offset is
     * wrong is what identifies the misplaced part. */
    for (size_t i = 0; i < out_results.received_file_content.len; ++i) {
        uint8_t expected = (uint8_t)(32 + (i % 90));
        if (out_results.received_file_content.buffer[i] != expected) {
            AWS_LOGF_ERROR(
                AWS_LS_S3_GENERAL,
                "First wrong byte at object offset %zu: expected %u, got %u",
                i,
                (unsigned)expected,
                (unsigned)out_results.received_file_content.buffer[i]);
            ASSERT_UINT_EQUALS(expected, out_results.received_file_content.buffer[i]);
        }
    }

    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

/* Test that parts are written to the file as they arrive rather than in object order, by making the
 * mock server hold back part 2 while parts 3 and 4 are served immediately.
 *
 * Mock object: 256 KiB served in four aligned 64 KiB parts, with the part at offset 65536 delayed by
 * 2 seconds. Body bytes encode their own object offset (byte at offset o is 32 + o % 90), so a part
 * written to the wrong offset is caught rather than only a wrong total size.
 *
 * The ordering claim rests on the per-part write window the metrics already record:
 * deliver_start/deliver_end bracket the pwrite into the recv file. Parts 3 and 4 finish writing
 * before part 2's write even begins, which the ordered path cannot produce -- there parts 3 and 4
 * queue behind part 2 and cannot reach the file until it has been written. So this is the assertion
 * that distinguishes the two paths, and asserting only the file's final contents would not: the
 * bytes end up identical either way. */
TEST_CASE(parallel_write_delayed_part_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_tester_client_options client_options = {
        .part_size = S_PART_SIZE,
        .tls_usage = AWS_S3_TLS_DISABLED,
    };
    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_s3_tester_meta_request_options get_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .client = client,
        .get_options =
            {
                .object_path = aws_byte_cursor_from_c_str("/get_object_parallel_write_delay_part"),
                .file_on_disk = true,
                .recv_file_option = AWS_S3_RECV_FILE_CREATE_OR_REPLACE,
                .capture_file_content = true,
            },
        .mock_server = true,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_SUCCESS,
    };
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &get_options, &out_results));
    ASSERT_UINT_EQUALS(AWS_ERROR_SUCCESS, out_results.finished_error_code);

    /* The ordering assertion below only means anything on the parallel path. */
    ASSERT_TRUE(out_results.out_of_order_delivery);

    size_t expected_size = (size_t)S_PART_COUNT * S_PART_SIZE;
    ASSERT_UINT_EQUALS(expected_size, out_results.received_file_size);
    ASSERT_UINT_EQUALS(expected_size, out_results.received_file_content.len);

    /* The delay must not have collapsed the transfer into fewer, larger parts, or there would be no
     * out-of-order write to observe. */
    ASSERT_UINT_EQUALS(S_PART_COUNT, aws_array_list_length(&out_results.synced_data.succeed_metrics));

    /* Report the first wrong offset rather than just that the file differs, since which offset is
     * wrong is what identifies the misplaced part. */
    for (size_t i = 0; i < out_results.received_file_content.len; ++i) {
        uint8_t expected = (uint8_t)(32 + (i % 90));
        if (out_results.received_file_content.buffer[i] != expected) {
            AWS_LOGF_ERROR(
                AWS_LS_S3_GENERAL,
                "First wrong byte at object offset %zu: expected %u, got %u",
                i,
                (unsigned)expected,
                (unsigned)out_results.received_file_content.buffer[i]);
            ASSERT_UINT_EQUALS(expected, out_results.received_file_content.buffer[i]);
        }
    }

    /* Collect each part's write window, indexed by part number so the delayed part can be picked out
     * regardless of the order the metrics were appended in. */
    uint64_t write_start_ns[S_PART_COUNT + 1] = {0};
    uint64_t write_end_ns[S_PART_COUNT + 1] = {0};
    bool seen[S_PART_COUNT + 1] = {false};

    for (size_t i = 0; i < aws_array_list_length(&out_results.synced_data.succeed_metrics); ++i) {
        struct aws_s3_request_metrics *metrics = NULL;
        ASSERT_SUCCESS(aws_array_list_get_at(&out_results.synced_data.succeed_metrics, (void **)&metrics, i));

        uint32_t part_number = 0;
        aws_s3_request_metrics_get_part_number(metrics, &part_number);
        ASSERT_TRUE(part_number >= 1 && part_number <= S_PART_COUNT);
        ASSERT_FALSE(seen[part_number]);
        seen[part_number] = true;

        /* A part that never reached the write path would leave these unset, and comparing unset
         * timestamps would make the ordering assertion vacuous. */
        ASSERT_SUCCESS(aws_s3_request_metrics_get_delivery_start_timestamp_ns(metrics, &write_start_ns[part_number]));
        ASSERT_SUCCESS(aws_s3_request_metrics_get_delivery_end_timestamp_ns(metrics, &write_end_ns[part_number]));

        /* Confirms part numbers map to offsets the way the assertion assumes, so the delayed part
         * really is the one the mock server held back. */
        uint64_t range_start = 0;
        aws_s3_request_metrics_get_part_range_start(metrics, &range_start);
        ASSERT_UINT_EQUALS((uint64_t)(part_number - 1) * S_PART_SIZE, range_start);
    }

    /* Every other part was written to the file, start to finish, before the delayed part's write
     * began: it is the last portion written, and the writes did not overlap at all. */
    for (uint32_t part_number = 1; part_number <= S_PART_COUNT; ++part_number) {
        if (part_number == S_DELAYED_PART_NUMBER) {
            continue;
        }
        if (write_end_ns[part_number] >= write_start_ns[S_DELAYED_PART_NUMBER]) {
            AWS_LOGF_ERROR(
                AWS_LS_S3_GENERAL,
                "Part %" PRIu32 " finished writing at %" PRIu64
                " ns, which is not before part %d began writing at %" PRIu64
                " ns. The delayed part was not the last portion written.",
                part_number,
                write_end_ns[part_number],
                S_DELAYED_PART_NUMBER,
                write_start_ns[S_DELAYED_PART_NUMBER]);
            ASSERT_TRUE(write_end_ns[part_number] < write_start_ns[S_DELAYED_PART_NUMBER]);
        }
    }

    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

/* Test that out-of-order delivery reaches the body callback, not just a file, and that each range the
 * callback hands over carries the correct offset AND the correct bytes for that offset.
 *
 * The mock server holds part 2 back while parts 3 and 4 are served immediately, so with the ordering
 * wait removed the callback must see part 1, then parts 3 and 4, then part 2 -- a range_start that
 * goes backwards. The tester assembles each range into `received_body_content` at `range_start`, so a
 * fully intact object at the end proves every delivery reported the offset its bytes actually belong
 * at; a part delivered with the wrong range_start would land on top of another part and corrupt it.
 *
 * The callback is still invoked from one thread, one part at a time. This test covers the order
 * change only, not concurrency. */
TEST_CASE(out_of_order_body_callback_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_tester_client_options client_options = {
        .part_size = S_PART_SIZE,
        .tls_usage = AWS_S3_TLS_DISABLED,
        /* A body callback needs the explicit opt-in; UNSET would leave delivery in object order. */
        .out_of_order_delivery = AWS_TRIBOOL_TRUE,
    };
    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_s3_tester_meta_request_options get_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .client = client,
        .get_options =
            {
                .object_path = aws_byte_cursor_from_c_str("/get_object_parallel_write_delay_part"),
                /* No file_on_disk: the point is that this works with the callback as the sink. */
                .allow_out_of_order_body = true,
            },
        .mock_server = true,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_SUCCESS,
    };
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &get_options, &out_results));
    ASSERT_UINT_EQUALS(AWS_ERROR_SUCCESS, out_results.finished_error_code);

    /* Without this the assertions below could pass on a run that quietly took the ordered path. */
    ASSERT_TRUE(out_results.out_of_order_delivery);

    /* And this is what proves the ordering wait was actually skipped: some range arrived behind one
     * already delivered. An ordered run would never satisfy it. */
    ASSERT_TRUE(out_results.body_arrived_out_of_order);

    size_t expected_size = (size_t)S_PART_COUNT * S_PART_SIZE;
    ASSERT_UINT_EQUALS(expected_size, out_results.received_body_size);
    ASSERT_UINT_EQUALS(expected_size, out_results.received_body_content.len);

    /* Parts 3 and 4 finished ahead of part 2, so they had to park in the completed-deliveries heap and
     * then drain once part 2 closed the gap. Both counters reaching the full object size is what shows
     * the drain happened -- a prefix that stopped at part 1 would leave a resume token from this
     * transfer understating what the caller already has. */
    ASSERT_UINT_EQUALS(expected_size, out_results.num_bytes_delivered);
    ASSERT_UINT_EQUALS(expected_size, out_results.num_bytes_delivered_total);

    /* The delay must not have collapsed the transfer into fewer, larger parts, or there would be no
     * out-of-order delivery to observe. */
    ASSERT_UINT_EQUALS(S_PART_COUNT, aws_array_list_length(&out_results.synced_data.succeed_metrics));

    /* Report the first wrong offset rather than just that the object differs, since which offset is
     * wrong is what identifies the misdelivered range. */
    for (size_t i = 0; i < out_results.received_body_content.len; ++i) {
        uint8_t expected = (uint8_t)(32 + (i % 90));
        if (out_results.received_body_content.buffer[i] != expected) {
            AWS_LOGF_ERROR(
                AWS_LS_S3_GENERAL,
                "First wrong byte at object offset %zu: expected %u, got %u",
                i,
                (unsigned)expected,
                (unsigned)out_results.received_body_content.buffer[i]);
            ASSERT_UINT_EQUALS(expected, out_results.received_body_content.buffer[i]);
        }
    }

    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

/* Test that a request's out-of-order override reaches the descriptor allocation in init, not just the
 * delivery-order latch.
 *
 * What only real transfer can show is that init consulted the override at all: it allocates the per-worker
 * descriptors up front, and skips them when out-of-order delivery has been ruled out. An allocation
 * site that read only the client setting would leave a request overriding client-FALSE to TRUE with no
 * descriptors, and the latch would then quietly fall back to ordered writes -- the exact bug this
 * guards. So the client says FALSE and the request says TRUE, and the transfer has to come out
 * out-of-order with the file intact, which it can only do through descriptors that were allocated. */
TEST_CASE(out_of_order_override_allocates_write_slots_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_tester_client_options client_options = {
        .part_size = S_PART_SIZE,
        .tls_usage = AWS_S3_TLS_DISABLED,
        .out_of_order_delivery = AWS_TRIBOOL_FALSE,
    };
    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_s3_tester_meta_request_options get_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .client = client,
        /* The override, pulling in the opposite direction to the client. */
        .out_of_order_delivery = AWS_TRIBOOL_TRUE,
        .get_options =
            {
                /* The undelayed route: nothing here asserts on timing, so there is no reason to pay
                 * for the delayed part. */
                .object_path = aws_byte_cursor_from_c_str("/get_object_parallel_write_aligned"),
                .file_on_disk = true,
                .recv_file_option = AWS_S3_RECV_FILE_CREATE_OR_REPLACE,
                .capture_file_content = true,
            },
        .mock_server = true,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_SUCCESS,
    };
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &get_options, &out_results));
    ASSERT_UINT_EQUALS(AWS_ERROR_SUCCESS, out_results.finished_error_code);

    /* The request's TRUE won, despite the client's FALSE -- which means init allocated the descriptors
     * the out-of-order path needs. */
    ASSERT_TRUE(out_results.out_of_order_delivery);

    size_t expected_size = (size_t)S_PART_COUNT * S_PART_SIZE;
    ASSERT_UINT_EQUALS(expected_size, out_results.received_file_size);
    ASSERT_UINT_EQUALS(expected_size, out_results.received_file_content.len);
    for (size_t i = 0; i < out_results.received_file_content.len; ++i) {
        uint8_t expected = (uint8_t)(32 + (i % 90));
        if (out_results.received_file_content.buffer[i] != expected) {
            AWS_LOGF_ERROR(
                AWS_LS_S3_GENERAL,
                "First wrong byte at object offset %zu: expected %u, got %u",
                i,
                (unsigned)expected,
                (unsigned)out_results.received_file_content.buffer[i]);
            ASSERT_UINT_EQUALS(expected, out_results.received_file_content.buffer[i]);
        }
    }

    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

/* ============================ spread requests ============================ */

TEST_CASE(spread_requests_mock_server) {
    (void)ctx;
    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    /* Spreading is declined unless there are more parts left to request than there are regions, and the
     * region count comes from the connection count -- which floors at g_min_num_connections (10), well
     * above the three parts this object has left once discovery has taken part 1. Holding the client to
     * two connections gives two regions over those three parts, so the spread engages: region 0 holds
     * parts 2 and 3, region 1 holds part 4, and the rotation issues them 2, 4, 3. */
    struct aws_s3_tester_client_options client_options = {
        .part_size = S_PART_SIZE,
        .tls_usage = AWS_S3_TLS_DISABLED,
        .max_active_connections_override = 2,
    };
    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));
    struct aws_s3_tester_meta_request_options get_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .client = client,
        .get_options =
            {
                .object_path = aws_byte_cursor_from_c_str("/get_object_parallel_write_aligned"),
                .file_on_disk = true,
                .recv_file_option = AWS_S3_RECV_FILE_CREATE_OR_REPLACE,
                .capture_file_content = true,
            },
        .mock_server = true,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_SUCCESS,
    };
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);
    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &get_options, &out_results));
    ASSERT_UINT_EQUALS(AWS_ERROR_SUCCESS, out_results.finished_error_code);
    ASSERT_TRUE(out_results.out_of_order_delivery);
    ASSERT_TRUE(out_results.spread_num_regions > 1);
    ASSERT_UINT_EQUALS(S_PART_COUNT, aws_array_list_length(&out_results.synced_data.succeed_metrics));
    size_t expected_size = (size_t)S_PART_COUNT * S_PART_SIZE;
    ASSERT_UINT_EQUALS(expected_size, out_results.received_file_size);
    ASSERT_UINT_EQUALS(expected_size, out_results.received_file_content.len);
    for (size_t i = 0; i < out_results.received_file_content.len; ++i) {
        uint8_t expected = s_positional_byte(i);
        if (out_results.received_file_content.buffer[i] != expected) {
            ASSERT_UINT_EQUALS(expected, out_results.received_file_content.buffer[i]);
        }
    }
    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);
    return AWS_OP_SUCCESS;
}

TEST_CASE(spread_requests_declined_when_delivery_ordered_mock_server) {
    (void)ctx;
    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_tester_client_options client_options = {
        .part_size = S_PART_SIZE,
        .tls_usage = AWS_S3_TLS_DISABLED,
        .out_of_order_delivery = AWS_TRIBOOL_FALSE,
        /* Two connections give two regions over the three parts left after discovery, which is what
         * makes a spread possible here at all -- see spread_requests_mock_server. Without it the spread
         * would be declined for want of parts, and the assertion below would hold even if the mechanism
         * under test had stopped working. */
        .max_active_connections_override = 2,
    };
    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));
    struct aws_s3_tester_meta_request_options get_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .client = client,
        .get_options =
            {
                .object_path = aws_byte_cursor_from_c_str("/get_object_parallel_write_aligned"),
                .file_on_disk = true,
                .recv_file_option = AWS_S3_RECV_FILE_CREATE_OR_REPLACE,
            },
        .mock_server = true,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_SUCCESS,
    };
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);
    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &get_options, &out_results));
    ASSERT_UINT_EQUALS(AWS_ERROR_SUCCESS, out_results.finished_error_code);
    ASSERT_FALSE(out_results.out_of_order_delivery);
    ASSERT_UINT_EQUALS(0, out_results.spread_num_regions);
    ASSERT_UINT_EQUALS((size_t)S_PART_COUNT * S_PART_SIZE, out_results.received_file_size);
    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);
    return AWS_OP_SUCCESS;
}

TEST_CASE(spread_requests_forced_sequential_mock_server) {
    (void)ctx;
    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_string *env_name = aws_string_new_from_c_str(allocator, "AWS_CRT_S3_FORCE_SEQUENTIAL_REQUESTS");
    struct aws_string *env_value = aws_string_new_from_c_str(allocator, "1");
    ASSERT_SUCCESS(aws_set_environment_value(env_name, env_value));
    aws_string_destroy(env_value);
    struct aws_s3_tester_client_options client_options = {
        .part_size = S_PART_SIZE,
        .tls_usage = AWS_S3_TLS_DISABLED,
        /* Two connections give two regions over the three parts left after discovery, which is what
         * makes a spread possible here at all -- see spread_requests_mock_server. Without it the spread
         * would be declined for want of parts, and the assertion below would hold even if the mechanism
         * under test had stopped working. */
        .max_active_connections_override = 2,
    };
    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));
    struct aws_s3_tester_meta_request_options get_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .client = client,
        .get_options =
            {
                .object_path = aws_byte_cursor_from_c_str("/get_object_parallel_write_aligned"),
                .file_on_disk = true,
                .recv_file_option = AWS_S3_RECV_FILE_CREATE_OR_REPLACE,
            },
        .mock_server = true,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_SUCCESS,
    };
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);
    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &get_options, &out_results));
    ASSERT_UINT_EQUALS(AWS_ERROR_SUCCESS, out_results.finished_error_code);
    ASSERT_UINT_EQUALS(0, out_results.spread_num_regions);
    ASSERT_TRUE(out_results.out_of_order_delivery);
    ASSERT_UINT_EQUALS((size_t)S_PART_COUNT * S_PART_SIZE, out_results.received_file_size);
    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);
    ASSERT_SUCCESS(aws_unset_environment_value(env_name));
    aws_string_destroy(env_name);
    return AWS_OP_SUCCESS;
}

/* ============================ env var ordered delivery ============================ */

TEST_CASE(env_ordered_delivery_changes_default_mock_server) {
    (void)ctx;
    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_string *env_name = aws_string_new_from_c_str(allocator, "AWS_CRT_S3_ORDERED_DELIVERY");
    struct aws_string *env_value = aws_string_new_from_c_str(allocator, "1");
    ASSERT_SUCCESS(aws_set_environment_value(env_name, env_value));
    aws_string_destroy(env_value);
    struct aws_s3_tester_client_options client_options = {
        .part_size = S_PART_SIZE,
        .tls_usage = AWS_S3_TLS_DISABLED,
    };
    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));
    struct aws_s3_tester_meta_request_options get_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .client = client,
        .get_options =
            {
                .object_path = aws_byte_cursor_from_c_str("/get_object_parallel_write_aligned"),
                .file_on_disk = true,
                .recv_file_option = AWS_S3_RECV_FILE_CREATE_OR_REPLACE,
                .capture_file_content = true,
            },
        .mock_server = true,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_SUCCESS,
    };
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);
    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &get_options, &out_results));
    ASSERT_UINT_EQUALS(AWS_ERROR_SUCCESS, out_results.finished_error_code);
    ASSERT_FALSE(out_results.out_of_order_delivery);
    ASSERT_UINT_EQUALS((size_t)S_PART_COUNT * S_PART_SIZE, out_results.received_file_size);
    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);
    ASSERT_SUCCESS(aws_unset_environment_value(env_name));
    aws_string_destroy(env_name);
    return AWS_OP_SUCCESS;
}

TEST_CASE(env_ordered_delivery_yields_to_explicit_request_mock_server) {
    (void)ctx;
    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_string *env_name = aws_string_new_from_c_str(allocator, "AWS_CRT_S3_ORDERED_DELIVERY");
    struct aws_string *env_value = aws_string_new_from_c_str(allocator, "1");
    ASSERT_SUCCESS(aws_set_environment_value(env_name, env_value));
    aws_string_destroy(env_value);
    struct aws_s3_tester_client_options client_options = {
        .part_size = S_PART_SIZE,
        .tls_usage = AWS_S3_TLS_DISABLED,
    };
    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));
    struct aws_s3_tester_meta_request_options get_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .client = client,
        .out_of_order_delivery = AWS_TRIBOOL_TRUE,
        .get_options =
            {
                .object_path = aws_byte_cursor_from_c_str("/get_object_parallel_write_aligned"),
                .file_on_disk = true,
                .recv_file_option = AWS_S3_RECV_FILE_CREATE_OR_REPLACE,
            },
        .mock_server = true,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_SUCCESS,
    };
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);
    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &get_options, &out_results));
    ASSERT_UINT_EQUALS(AWS_ERROR_SUCCESS, out_results.finished_error_code);
    ASSERT_TRUE(out_results.out_of_order_delivery);
    ASSERT_UINT_EQUALS((size_t)S_PART_COUNT * S_PART_SIZE, out_results.received_file_size);
    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);
    ASSERT_SUCCESS(aws_unset_environment_value(env_name));
    aws_string_destroy(env_name);
    return AWS_OP_SUCCESS;
}

/* ============================ write-failure propagation ============================
 *
 * A write that fails must fail the meta request. If it did not, the caller would be handed a file
 * that is silently missing whatever the failed write carried -- a success return over corrupt data,
 * which is the worst failure mode this path has.
 *
 * A mock server cannot provoke it: the failure lives below the response, in the file I/O. So the write
 * is replaced by `recv_file_write_stub`, a TEST ONLY STUB on the meta request vtable that stands in for
 * the positional write and fails the one covering `fail_at_file_offset`. Every other write reports
 * success without touching the disk, so the test does not depend on the file's contents -- only on
 * where the error ends up. */

/* The part whose write is made to fail. Same 1-based numbering as S_DELAYED_PART_NUMBER, and
 * deliberately not part 1, so the failure lands after at least one write has already been reported as
 * succeeding. */
#define S_WRITE_FAIL_PART_NUMBER 2
#define S_WRITE_FAIL_OFFSET ((uint64_t)(S_WRITE_FAIL_PART_NUMBER - 1) * S_PART_SIZE)

struct write_fail_mock_test_data {
    struct aws_atomic_var writes_attempted;
    struct aws_atomic_var writes_failed;
};
static struct write_fail_mock_test_data s_write_fail_test_data;

static int s_recv_file_write_fail_stub(
    struct aws_s3_meta_request *meta_request,
    uint64_t file_offset,
    const struct aws_byte_cursor *body) {

    (void)meta_request;
    (void)body;
    struct write_fail_mock_test_data *test_data = &s_write_fail_test_data;
    aws_atomic_fetch_add(&test_data->writes_attempted, 1);

    if (file_offset == S_WRITE_FAIL_OFFSET) {
        aws_atomic_fetch_add(&test_data->writes_failed, 1);
        return aws_raise_error(AWS_ERROR_FILE_WRITE_FAILURE);
    }

    /* Stand in for a completed write. Nothing reaches the disk, which is fine: this test is about
     * where the error surfaces, not about file contents. */
    return AWS_OP_SUCCESS;
}

static struct aws_s3_meta_request *s_write_fail_meta_request_factory(
    struct aws_s3_client *client,
    const struct aws_s3_meta_request_options *options) {

    AWS_ASSERT(client != NULL);
    struct aws_s3_tester *tester = client->shutdown_callback_user_data;
    AWS_ASSERT(tester != NULL);

    struct aws_s3_client_vtable *original_client_vtable =
        aws_s3_tester_get_client_vtable_patch(tester, 0)->original_vtable;
    struct aws_s3_meta_request *meta_request = original_client_vtable->meta_request_factory(client, options);

    struct aws_s3_meta_request_vtable *patched_meta_request_vtable =
        aws_s3_tester_patch_meta_request_vtable(tester, meta_request, NULL);
    patched_meta_request_vtable->recv_file_write_stub = s_recv_file_write_fail_stub;

    return meta_request;
}

/* `out_of_order` selects which write path carries the failure: a parallel worker, or the ordered
 * delivery thread. Both funnel through the same write function, so both must propagate. */
static int s_test_parallel_write_failure_helper(struct aws_allocator *allocator, bool out_of_order) {
    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    AWS_ZERO_STRUCT(s_write_fail_test_data);
    aws_atomic_init_int(&s_write_fail_test_data.writes_attempted, 0);
    aws_atomic_init_int(&s_write_fail_test_data.writes_failed, 0);

    struct aws_s3_tester_client_options client_options = {
        .part_size = S_PART_SIZE,
        .tls_usage = AWS_S3_TLS_DISABLED,
        .out_of_order_delivery = out_of_order ? AWS_TRIBOOL_TRUE : AWS_TRIBOOL_FALSE,
    };
    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_s3_client_vtable *patched_client_vtable = aws_s3_tester_patch_client_vtable(&tester, client, NULL);
    patched_client_vtable->meta_request_factory = s_write_fail_meta_request_factory;

    struct aws_s3_tester_meta_request_options get_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .client = client,
        .get_options =
            {
                .object_path = aws_byte_cursor_from_c_str("/get_object_parallel_write_aligned"),
                .file_on_disk = true,
                .recv_file_option = AWS_S3_RECV_FILE_CREATE_OR_REPLACE,
            },
        .mock_server = true,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE,
    };
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &get_options, &out_results));

    /* The whole point: the injected write error is what the caller is told, rather than a success over
     * a file missing part 2. */
    ASSERT_UINT_EQUALS(AWS_ERROR_FILE_WRITE_FAILURE, out_results.finished_error_code);

    /* Confirms the failure came from where the test intended, not from the request never reaching the
     * write path at all -- which would make the assertion above pass for the wrong reason. */
    ASSERT_UINT_EQUALS(1, aws_atomic_load_int(&s_write_fail_test_data.writes_failed));
    ASSERT_TRUE(aws_atomic_load_int(&s_write_fail_test_data.writes_attempted) >= 2);

    /* And confirms the path under test was the one selected. */
    ASSERT_UINT_EQUALS(out_of_order, out_results.out_of_order_delivery);

    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

TEST_CASE(parallel_write_failure_propagates_mock_server) {
    (void)ctx;
    return s_test_parallel_write_failure_helper(allocator, true /*out_of_order*/);
}

TEST_CASE(ordered_write_failure_propagates_mock_server) {
    (void)ctx;
    return s_test_parallel_write_failure_helper(allocator, false /*out_of_order*/);
}

/* ==================== whole-object checksum forces ordered delivery ====================
 *
 * A whole-object SHA256 can only be verified by hashing the body in object order, so it is a
 * correctness constraint that has to outrank the caller's preference for out-of-order delivery.
 *
 * The client asks for out-of-order explicitly, and the mock's HEAD advertises a whole-object SHA256
 * (non-combinable, unlike the CRC32 every other checksum fixture uses). Delivery must come back
 * ordered, and the checksum must still validate -- a run that went out of order would either fail
 * validation or produce a digest over the wrong byte order. */
TEST_CASE(noncombinable_checksum_forces_ordered_delivery_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_tester_client_options client_options = {
        .part_size = 64 * 1024,
        .tls_usage = AWS_S3_TLS_DISABLED,
        /* Asked for, and must lose to the checksum's ordering demand. */
        .out_of_order_delivery = AWS_TRIBOOL_TRUE,
    };
    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_s3_tester_meta_request_options get_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .client = client,
        .expected_validate_checksum_alg = AWS_SCA_SHA256,
        .validate_get_response_checksum = true,
        .get_options =
            {
                .object_path = aws_byte_cursor_from_c_str("/get_object_checksum_noncombinable"),
                .file_on_disk = true,
                .recv_file_option = AWS_S3_RECV_FILE_CREATE_OR_REPLACE,
            },
        .mock_server = true,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_SUCCESS,
    };
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &get_options, &out_results));

    ASSERT_UINT_EQUALS(AWS_ERROR_SUCCESS, out_results.finished_error_code);

    /* The constraint won over the preference. */
    ASSERT_FALSE(out_results.out_of_order_delivery);

    /* And the checksum was actually verified, so the ordering the constraint demanded was delivered.
     * Without this a passing test could just as well have skipped validation entirely. */
    ASSERT_UINT_EQUALS(AWS_SCA_SHA256, out_results.algorithm);
    ASSERT_TRUE(out_results.did_validate);

    ASSERT_UINT_EQUALS(262144, out_results.received_file_size);

    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

/* ======================== zero-length part in the middle ========================
 *
 * A part that delivers no bytes still has to close its slot in the contiguous prefix. If it does not,
 * every later part parks in the completed-deliveries heap waiting for a gap that never closes, and the
 * download resume token reports a prefix frozen before the empty part forever.
 *
 * Part 2 comes back with an empty body while its Content-Range still claims the full 64 KiB. A
 * zero-byte OBJECT cannot cover this: with only one part and no bytes, recording it and not recording
 * it produce identical counters, so nothing distinguishes the two. It takes an empty part with later
 * parts behind it for the prefix to have anywhere to get stuck. */
TEST_CASE(parallel_write_empty_part_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_tester_client_options client_options = {
        .part_size = S_PART_SIZE,
        .tls_usage = AWS_S3_TLS_DISABLED,
        .out_of_order_delivery = AWS_TRIBOOL_TRUE,
    };
    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_s3_tester_meta_request_options get_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .client = client,
        .get_options =
            {
                /* Callback sink: the zero-length branch lives in the delivery loop, which a file
                 * destination bypasses in favour of the write workers. */
                .object_path = aws_byte_cursor_from_c_str("/get_object_parallel_write_empty_part"),
                .allow_out_of_order_body = true,
            },
        .mock_server = true,
        /* NO_VALIDATE rather than EXPECT_SUCCESS: the generic get-object validation cross-checks the
         * advertised Content-Length against the bytes delivered, and this route deliberately breaks
         * that by under-delivering. The assertions below check success directly instead. */
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_NO_VALIDATE,
    };
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);

    /* Reaching this at all is half the assertion: a prefix that never advanced past the empty part
     * must still let the meta request finish rather than waiting on a gap that cannot close. */
    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &get_options, &out_results));
    ASSERT_UINT_EQUALS(AWS_ERROR_SUCCESS, out_results.finished_error_code);

    ASSERT_TRUE(out_results.out_of_order_delivery);

    /* Every part but the empty one carried bytes. */
    size_t expected_bytes = (size_t)(S_PART_COUNT - 1) * S_PART_SIZE;
    ASSERT_UINT_EQUALS(expected_bytes, out_results.received_body_size);

    /* The prefix ran all the way to the end, which it can only do by stepping over the empty part.
     * Left unrecorded, part 2 would hold the prefix at 64 KiB while the total reached 192 KiB. */
    ASSERT_UINT_EQUALS(expected_bytes, out_results.num_bytes_delivered);
    ASSERT_UINT_EQUALS(expected_bytes, out_results.num_bytes_delivered_total);

    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

/* Test that a download over an existing file replaces it rather than leaving any of it behind.
 *
 * The pre-existing file is deliberately LONGER than the object (384 KiB vs 256 KiB), so a path that
 * wrote the parts without truncating would leave the tail of the old file in place and be caught by
 * the size assertion. The pre-existing length is page-aligned so it cannot be the reason O_DIRECT
 * falls back; CREATE_OR_REPLACE truncates to empty anyway, leaving base_offset 0. */
TEST_CASE(parallel_write_create_or_replace_existing_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_tester_client_options client_options = {
        .part_size = S_PART_SIZE,
        .tls_usage = AWS_S3_TLS_DISABLED,
    };
    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_s3_file_io_options fio_opts = {
        .direct_io = true,
    };

    struct aws_s3_tester_meta_request_options get_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .client = client,
        .fio_opts = &fio_opts,
        .get_options =
            {
                .object_path = aws_byte_cursor_from_c_str("/get_object_parallel_write_aligned"),
                .file_on_disk = true,
                .recv_file_option = AWS_S3_RECV_FILE_CREATE_OR_REPLACE,
                .pre_exist_file_length = S_OBJECT_SIZE + (128 * 1024),
                .capture_file_content = true,
            },
        .mock_server = true,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_SUCCESS,
    };
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &get_options, &out_results));
    ASSERT_UINT_EQUALS(AWS_ERROR_SUCCESS, out_results.finished_error_code);

    ASSERT_SUCCESS(s_check_direct_io_out_of_order(&out_results, S_PART_COUNT));

    /* No prefix: the whole file is object bytes from offset 0, and the old tail is gone. */
    ASSERT_SUCCESS(s_check_recv_file_content(&out_results, S_OBJECT_SIZE, 0 /*prefix_len*/, 0 /*origin*/));

    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

/* Test that appending to an existing file writes every part past the existing bytes without disturbing
 * them, which is the `recv_file_base_offset` half of the file offset calculation.
 *
 * The pre-existing length is page-aligned on purpose: an unaligned one makes the init-time check give
 * up on O_DIRECT and fall back to buffered, so the test would still pass while covering neither the
 * direct-I/O path nor the alignment requirement. s_check_direct_io_out_of_order asserts the fallback
 * did not happen, so that substitution cannot go unnoticed. */
TEST_CASE(parallel_write_create_or_append_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_tester_client_options client_options = {
        .part_size = S_PART_SIZE,
        .tls_usage = AWS_S3_TLS_DISABLED,
    };
    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_s3_file_io_options fio_opts = {
        .direct_io = true,
    };

    /* Page-aligned, and not a multiple of the part size, so a part placed at the wrong multiple of the
     * part size cannot coincidentally land where the correct offset is. */
    uint64_t prefix_len = 3 * 4096;

    struct aws_s3_tester_meta_request_options get_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .client = client,
        .fio_opts = &fio_opts,
        .get_options =
            {
                .object_path = aws_byte_cursor_from_c_str("/get_object_parallel_write_aligned"),
                .file_on_disk = true,
                .recv_file_option = AWS_S3_RECV_FILE_CREATE_OR_APPEND,
                .pre_exist_file_length = prefix_len,
                .capture_file_content = true,
            },
        .mock_server = true,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_SUCCESS,
    };
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &get_options, &out_results));
    ASSERT_UINT_EQUALS(AWS_ERROR_SUCCESS, out_results.finished_error_code);

    ASSERT_SUCCESS(s_check_direct_io_out_of_order(&out_results, S_PART_COUNT));

    /* The file grew by exactly the object, the existing bytes are untouched, and object byte 0 sits at
     * file offset prefix_len rather than at 0. */
    ASSERT_SUCCESS(s_check_recv_file_content(&out_results, prefix_len + S_OBJECT_SIZE, prefix_len, 0 /*origin*/));

    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

/* Test that WRITE_TO_POSITION shifts every part by exactly the requested offset, which is the other
 * caller-supplied half of `recv_file_base_offset`.
 *
 * The offset is page-aligned so O_DIRECT survives, and is deliberately NOT a multiple of the part size:
 * with a part-size offset, a part written at the wrong multiple of the part size could still land on a
 * byte pattern that looks correct. WRITE_TO_POSITION also requires the file to already exist, so this
 * covers writing into a pre-existing file without truncating it -- the mode is "r+", not "wb". */
TEST_CASE(parallel_write_write_to_position_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_tester_client_options client_options = {
        .part_size = S_PART_SIZE,
        .tls_usage = AWS_S3_TLS_DISABLED,
    };
    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_s3_file_io_options fio_opts = {
        .direct_io = true,
    };

    /* One page in, so the first page of the existing file must survive untouched. */
    uint64_t write_position = 4096;

    struct aws_s3_tester_meta_request_options get_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .client = client,
        .fio_opts = &fio_opts,
        .get_options =
            {
                .object_path = aws_byte_cursor_from_c_str("/get_object_parallel_write_aligned"),
                .file_on_disk = true,
                .recv_file_option = AWS_S3_RECV_FILE_WRITE_TO_POSITION,
                .recv_file_position = write_position,
                /* Must exist, or the meta request fails with AWS_ERROR_S3_RECV_FILE_NOT_FOUND. Larger
                 * than the write position so the object really is written into existing bytes. */
                .pre_exist_file_length = 2 * 4096,
                .capture_file_content = true,
            },
        .mock_server = true,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_SUCCESS,
    };
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &get_options, &out_results));
    ASSERT_UINT_EQUALS(AWS_ERROR_SUCCESS, out_results.finished_error_code);

    ASSERT_SUCCESS(s_check_direct_io_out_of_order(&out_results, S_PART_COUNT));

    /* Object byte 0 sits at `write_position`, the bytes before it are the untouched existing fill, and
     * the object's tail extended the file past its original length. */
    ASSERT_SUCCESS(
        s_check_recv_file_content(&out_results, write_position + S_OBJECT_SIZE, write_position, 0 /*origin*/));

    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

/* Shared body for the two ranged-GET tests: download `range` of the object to a fresh file and assert
 * the file holds exactly that range starting at file offset 0.
 *
 * This is the `recv_file_object_range_origin` half of the file offset calculation. Getting it wrong
 * writes part 1 at the range start instead of at 0 -- the exact bug this path carried before the origin
 * was introduced -- which leaves a hole at the front of the file and shows up as both a wrong length
 * and a wrong first byte. */
static int s_test_parallel_write_ranged_get(struct aws_allocator *allocator, const char *range, uint64_t range_start) {

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_tester_client_options client_options = {
        .part_size = S_PART_SIZE,
        .tls_usage = AWS_S3_TLS_DISABLED,
    };
    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_s3_file_io_options fio_opts = {
        .direct_io = true,
    };

    struct aws_s3_tester_meta_request_options get_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .client = client,
        .fio_opts = &fio_opts,
        .get_options =
            {
                .object_path = aws_byte_cursor_from_c_str("/get_object_parallel_write_aligned"),
                .object_range = aws_byte_cursor_from_c_str(range),
                .file_on_disk = true,
                .recv_file_option = AWS_S3_RECV_FILE_CREATE_OR_REPLACE,
                .capture_file_content = true,
            },
        .mock_server = true,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_SUCCESS,
    };
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &get_options, &out_results));
    ASSERT_UINT_EQUALS(AWS_ERROR_SUCCESS, out_results.finished_error_code);

    /* Three parts, not four: the range covers three of the object's four parts. Asserting the count
     * also confirms the range really was applied rather than the whole object being fetched. */
    ASSERT_SUCCESS(s_check_direct_io_out_of_order(&out_results, S_RANGED_PART_COUNT));

    /* The file is exactly the range's length -- no leading hole -- and file offset 0 holds the object
     * byte at `range_start`. */
    ASSERT_SUCCESS(s_check_recv_file_content(&out_results, S_RANGED_LENGTH, 0 /*prefix_len*/, range_start));

    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

/* Ranged GET whose range start is page-aligned. */
TEST_CASE(parallel_write_ranged_get_aligned_start_mock_server) {
    (void)ctx;
    return s_test_parallel_write_ranged_get(allocator, S_RANGED_ALIGNED_RANGE, S_RANGED_ALIGNED_START);
}

/* Ranged GET whose range start is NOT page-aligned, which must not cost the transfer its O_DIRECT
 * descriptor: file offsets are measured from the range start, so they stay aligned even though the
 * range start is not. The direct-I/O assertion inside the shared body is what pins that down. */
TEST_CASE(parallel_write_ranged_get_unaligned_start_mock_server) {
    (void)ctx;
    return s_test_parallel_write_ranged_get(allocator, S_RANGED_UNALIGNED_RANGE, S_RANGED_UNALIGNED_START);
}

/* A checksum header only describes the bytes of the response that carried it. Two downloads below
 * receive a checksum on their size-discovery response that covers fewer bytes (or more) than the
 * meta request goes on to deliver, so that value must not be used as the whole-download checksum. The
 * bodies are intact in both cases, so a checksum mismatch here would be the client's own doing. */

/* A ranged download whose discovery request is a ranged GET (a Range header with a start range skips the
 * HEAD). The response covers only the first 64 KiB part and carries that part's CRC32, while the download
 * delivers 128 KiB. Real S3 answers a range that lines up with an uploaded part exactly this way, and the
 * part-level value is indistinguishable from a whole-object one by value alone: no "-N" suffix to make it
 * the wrong length, and no x-amz-mp-parts-count, which only comes back for partNumber requests. */
TEST_CASE(download_ranged_part_level_checksum_not_whole_object_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_tester_client_options client_options = {
        .part_size = 64 * 1024,
        .tls_usage = AWS_S3_TLS_DISABLED,
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_s3_tester_meta_request_options get_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .client = client,
        .expected_validate_checksum_alg = AWS_SCA_CRC32,
        .validate_get_response_checksum = true,
        .get_options =
            {
                .object_path = aws_byte_cursor_from_c_str("/get_object_checksum_per_part_header"),
                /* Two 64 KiB parts of the 256 KiB object. The start range keeps discovery on the ranged
                 * GET path instead of a HEAD. */
                .object_range = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("bytes=0-131071"),
            },
        .mock_server = true,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_SUCCESS,
    };
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &get_options, &out_results));

    ASSERT_UINT_EQUALS(AWS_ERROR_SUCCESS, out_results.finished_error_code);
    ASSERT_UINT_EQUALS(131072, out_results.received_body_size);
    /* Each part still validates against its own header, which is all these headers can support. */
    ASSERT_TRUE(out_results.did_validate);
    ASSERT_UINT_EQUALS(AWS_SCA_CRC32, out_results.validation_algorithm);

    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

/* A suffix range ("bytes=-1024") has no start range, so discovery is a HEAD. The HEAD copies the original
 * request's headers, including the Range, so it comes back 206 with the whole 64 KiB object's CRC32 while
 * the download delivers only the last 1 KiB. Nothing is left to validate against, so the download should
 * simply finish unvalidated. */
TEST_CASE(download_suffix_range_whole_object_checksum_out_of_scope_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_tester_client_options client_options = {
        .part_size = 64 * 1024,
        .tls_usage = AWS_S3_TLS_DISABLED,
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_s3_tester_meta_request_options get_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .client = client,
        .expected_validate_checksum_alg = AWS_SCA_CRC32,
        .validate_get_response_checksum = true,
        .get_options =
            {
                .object_path = aws_byte_cursor_from_c_str("/get_object_checksum_suffix_range"),
                .object_range = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("bytes=-1024"),
            },
        .mock_server = true,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_SUCCESS,
    };
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &get_options, &out_results));

    ASSERT_UINT_EQUALS(AWS_ERROR_SUCCESS, out_results.finished_error_code);
    ASSERT_UINT_EQUALS(1024, out_results.received_body_size);
    /* The part responses carry no checksum of their own, and the object's checksum covers bytes this
     * download never asked for. */
    ASSERT_FALSE(out_results.did_validate);

    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

/* A default meta request is a single HTTP request, so whatever checksum its response carries describes exactly
 * the bytes that request returned, which is everything the meta request delivers. That holds for an object
 * stored as a multipart upload too: this response carries the object's full object CRC32 next to
 * x-amz-mp-parts-count, and the whole object is what comes back. */
TEST_CASE(default_get_checksum_with_mp_parts_count_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_tester_client_options client_options = {
        .part_size = 64 * 1024,
        .tls_usage = AWS_S3_TLS_DISABLED,
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_s3_tester_meta_request_options get_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_DEFAULT,
        .client = client,
        .expected_validate_checksum_alg = AWS_SCA_CRC32,
        .validate_get_response_checksum = true,
        .get_options =
            {
                .object_path = aws_byte_cursor_from_c_str("/get_object_checksum_mp_parts_count"),
            },
        .default_type_options =
            {
                .mode = AWS_S3_TESTER_DEFAULT_TYPE_MODE_GET,
                .operation_name = aws_byte_cursor_from_c_str("GetObject"),
            },
        .mock_server = true,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_SUCCESS,
    };
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &get_options, &out_results));

    ASSERT_UINT_EQUALS(AWS_ERROR_SUCCESS, out_results.finished_error_code);
    ASSERT_UINT_EQUALS(64 * 1024, out_results.received_body_size);
    ASSERT_TRUE(out_results.did_validate);
    ASSERT_UINT_EQUALS(AWS_SCA_CRC32, out_results.validation_algorithm);

    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

/* HeadObject through a default meta request, with validation asked for. The response carries the object's
 * checksum but no body, so there is nothing for that checksum to describe. The request must finish
 * successfully and unvalidated, not as a mismatch against the empty body. */
TEST_CASE(default_head_object_with_checksum_header_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_tester_client_options client_options = {
        .part_size = 64 * 1024,
        .tls_usage = AWS_S3_TLS_DISABLED,
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_uri mock_server;
    ASSERT_SUCCESS(aws_uri_init_parse(&mock_server, allocator, &g_mock_server_uri));
    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/get_object_checksum_single_part");
    struct aws_http_message *message =
        aws_s3_test_get_object_request_new(allocator, *aws_uri_authority(&mock_server), object_path);
    ASSERT_SUCCESS(aws_http_message_set_request_method(message, g_head_method));

    struct aws_s3_tester_meta_request_options head_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_DEFAULT,
        .client = client,
        .message = message,
        .validate_get_response_checksum = true,
        .default_type_options =
            {
                .operation_name = aws_byte_cursor_from_c_str("HeadObject"),
            },
        .mock_server = true,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_SUCCESS,
    };
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &head_options, &out_results));

    ASSERT_UINT_EQUALS(AWS_ERROR_SUCCESS, out_results.finished_error_code);
    ASSERT_UINT_EQUALS(0, out_results.received_body_size);
    ASSERT_FALSE(out_results.did_validate);

    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_http_message_destroy(message);
    aws_uri_clean_up(&mock_server);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

/* The caller can hand the client the checksum of the data a GET returns instead of having the client learn one from
 * the service. Downloads below use /get_object_opaque_etag, a 256 KiB object of repeated 'a' whose responses carry
 * no checksum header of any kind, so the caller's value is the only thing validation can run against. That path also
 * has no HEAD response to serve, so a download that still tried to discover a checksum would fail outright. */
static int s_test_get_object_expected_checksum(
    struct aws_allocator *allocator,
    enum aws_s3_checksum_algorithm algorithm,
    struct aws_byte_cursor expected_checksum,
    const char *object_range,
    uint64_t expected_body_size,
    int expected_error_code) {

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_tester_client_options client_options = {
        .part_size = 64 * 1024,
        .tls_usage = AWS_S3_TLS_DISABLED,
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_s3_tester_meta_request_options get_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .client = client,
        /* validate_get_response_checksum is deliberately left unset: supplying a checksum is by itself a request
         * to validate against it. */
        .expected_checksum = expected_checksum,
        .expected_checksum_algorithm = algorithm,
        .get_options =
            {
                .object_path = aws_byte_cursor_from_c_str("/get_object_opaque_etag"),
            },
        .mock_server = true,
        .validate_type = expected_error_code == AWS_ERROR_SUCCESS ? AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_SUCCESS
                                                                  : AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE,
    };
    if (object_range != NULL) {
        get_options.get_options.object_range = aws_byte_cursor_from_c_str(object_range);
    }
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &get_options, &out_results));

    ASSERT_UINT_EQUALS(expected_error_code, out_results.finished_error_code);
    /* Whether the value matched or not, the data was compared against it. */
    ASSERT_TRUE(out_results.did_validate);
    ASSERT_UINT_EQUALS(algorithm, out_results.validation_algorithm);
    /* A mismatch is only found once the last part has been checksummed, so every requested byte is delivered
     * either way. */
    ASSERT_UINT_EQUALS(expected_body_size, out_results.received_body_size);

    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

/* The whole object, in four parts. CRC32 combines, so the parts checksum themselves as they stream and the digests
 * are folded into the value the caller gave. */
TEST_CASE(get_object_expected_checksum_crc32_mock_server) {
    (void)ctx;
    return s_test_get_object_expected_checksum(
        allocator,
        AWS_SCA_CRC32,
        aws_byte_cursor_from_c_str("uo2NxA=="),
        NULL /*object_range*/,
        262144 /*expected_body_size*/,
        AWS_ERROR_SUCCESS);
}

/* Same download with SHA256, which does not combine: the body is fed to a single running sum in object order as it
 * is delivered. */
TEST_CASE(get_object_expected_checksum_sha256_mock_server) {
    (void)ctx;
    return s_test_get_object_expected_checksum(
        allocator,
        AWS_SCA_SHA256,
        aws_byte_cursor_from_c_str("3T3eh2I9mms1TGjJQ9GJyJxjZS2UXnu98JhsrpGklSE="),
        NULL /*object_range*/,
        262144 /*expected_body_size*/,
        AWS_ERROR_SUCCESS);
}

/* The value covers the requested bytes, not the object: here 128 KiB out of the middle of the object, which the
 * service has no checksum of to report. */
TEST_CASE(get_object_expected_checksum_range_mock_server) {
    (void)ctx;
    return s_test_get_object_expected_checksum(
        allocator,
        AWS_SCA_CRC32,
        aws_byte_cursor_from_c_str("ypdRMA=="),
        "bytes=65536-196607" /*object_range*/,
        131072 /*expected_body_size*/,
        AWS_ERROR_SUCCESS);
}

/* The CRC32 of the first 64 KiB, offered as the checksum of all 256 KiB. */
TEST_CASE(get_object_expected_checksum_mismatch_mock_server) {
    (void)ctx;
    return s_test_get_object_expected_checksum(
        allocator,
        AWS_SCA_CRC32,
        aws_byte_cursor_from_c_str("wyCR/w=="),
        NULL /*object_range*/,
        262144 /*expected_body_size*/,
        AWS_ERROR_S3_RESPONSE_CHECKSUM_MISMATCH);
}

/* What the caller supplies is what the download is validated against, even where the service reports a checksum of
 * its own. Every part response here carries the correct CRC32 of its own 64 KiB, so per-part validation passes, and
 * the object's CRC32 is discoverable; the caller's value is wrong for the 256 KiB delivered, and that is what
 * decides the outcome. */
TEST_CASE(get_object_expected_checksum_takes_precedence_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_tester_client_options client_options = {
        .part_size = 64 * 1024,
        .tls_usage = AWS_S3_TLS_DISABLED,
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_s3_tester_meta_request_options get_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .client = client,
        .validate_get_response_checksum = true,
        /* The CRC32 of 64 KiB of 'a', which is one part rather than the whole download. */
        .expected_checksum = aws_byte_cursor_from_c_str("wyCR/w=="),
        .expected_checksum_algorithm = AWS_SCA_CRC32,
        .get_options =
            {
                .object_path = aws_byte_cursor_from_c_str("/get_object_checksum_per_part_header"),
            },
        .mock_server = true,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE,
    };
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &get_options, &out_results));

    ASSERT_UINT_EQUALS(AWS_ERROR_S3_RESPONSE_CHECKSUM_MISMATCH, out_results.finished_error_code);
    ASSERT_TRUE(out_results.did_validate);
    ASSERT_UINT_EQUALS(AWS_SCA_CRC32, out_results.validation_algorithm);
    ASSERT_UINT_EQUALS(262144, out_results.received_body_size);

    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

/* A download the caller asked not to be split is a default meta request, and the checksum it supplies covers the
 * response body the single request returns. /get_object_default_no_checksum is a 64 KiB object of repeated 'a'
 * answered whole, with no checksum header of any kind, so the caller's value is the only thing validation runs
 * against. */
static int s_test_default_get_expected_checksum(
    struct aws_allocator *allocator,
    struct aws_byte_cursor object_path,
    struct aws_byte_cursor expected_checksum,
    int expected_error_code) {

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_tester_client_options client_options = {
        .part_size = 64 * 1024,
        .tls_usage = AWS_S3_TLS_DISABLED,
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_s3_tester_meta_request_options get_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_DEFAULT,
        .client = client,
        .expected_checksum = expected_checksum,
        .expected_checksum_algorithm = AWS_SCA_CRC32,
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
        .validate_type = expected_error_code == AWS_ERROR_SUCCESS ? AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_SUCCESS
                                                                  : AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE,
    };
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &get_options, &out_results));

    ASSERT_UINT_EQUALS(expected_error_code, out_results.finished_error_code);
    ASSERT_TRUE(out_results.did_validate);
    ASSERT_UINT_EQUALS(AWS_SCA_CRC32, out_results.validation_algorithm);
    ASSERT_UINT_EQUALS(64 * 1024, out_results.received_body_size);

    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

/* The CRC32 of the 64 KiB the request returns. */
TEST_CASE(default_get_expected_checksum_mock_server) {
    (void)ctx;
    return s_test_default_get_expected_checksum(
        allocator,
        aws_byte_cursor_from_c_str("/get_object_default_no_checksum"),
        aws_byte_cursor_from_c_str("wyCR/w=="),
        AWS_ERROR_SUCCESS);
}

/* The CRC32 of 128 KiB of 'a', offered as the checksum of the 64 KiB that came back. */
TEST_CASE(default_get_expected_checksum_mismatch_mock_server) {
    (void)ctx;
    return s_test_default_get_expected_checksum(
        allocator,
        aws_byte_cursor_from_c_str("/get_object_default_no_checksum"),
        aws_byte_cursor_from_c_str("ypdRMA=="),
        AWS_ERROR_S3_RESPONSE_CHECKSUM_MISMATCH);
}

/* The caller's value decides the outcome even where the response reports a checksum of its own:
 * /get_object_checksum_mp_parts_count answers with the correct CRC32 of its 64 KiB, so the response is validated
 * against its own header and passes, and against the caller's wrong value and does not. */
TEST_CASE(default_get_expected_checksum_takes_precedence_mock_server) {
    (void)ctx;
    return s_test_default_get_expected_checksum(
        allocator,
        aws_byte_cursor_from_c_str("/get_object_checksum_mp_parts_count"),
        aws_byte_cursor_from_c_str("ypdRMA=="),
        AWS_ERROR_S3_RESPONSE_CHECKSUM_MISMATCH);
}

/* response_checksum_validation_mode picks which checksums a download is checked against. The downloads below use the
 * same objects as the tests above, so what changes with the mode is visible in the result: whether the whole download
 * was validated against a single checksum on top of each part being validated against its own. */
static int s_test_get_object_checksum_validation_mode(
    struct aws_allocator *allocator,
    struct aws_byte_cursor object_path,
    enum aws_s3_checksum_validation_mode mode,
    bool expected_did_validate) {

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_tester_client_options client_options = {
        .part_size = 64 * 1024,
        .tls_usage = AWS_S3_TLS_DISABLED,
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_s3_tester_meta_request_options get_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .client = client,
        .validate_get_response_checksum = true,
        .response_checksum_validation_mode = mode,
        .get_options =
            {
                .object_path = object_path,
            },
        .mock_server = true,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_SUCCESS,
    };
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &get_options, &out_results));

    ASSERT_UINT_EQUALS(AWS_ERROR_SUCCESS, out_results.finished_error_code);
    ASSERT_UINT_EQUALS(262144, out_results.received_body_size);
    ASSERT_UINT_EQUALS(expected_did_validate, out_results.did_validate);
    if (expected_did_validate) {
        ASSERT_UINT_EQUALS(AWS_SCA_CRC32, out_results.validation_algorithm);
    }

    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

/* Every part response of this object carries the CRC32 of its own body and nothing describes the whole object, so
 * validating each response against its own checksum is all these headers can support, and it still reports the
 * download as validated. */
TEST_CASE(get_object_checksum_validation_request_only_mock_server) {
    (void)ctx;
    return s_test_get_object_checksum_validation_mode(
        allocator,
        aws_byte_cursor_from_c_str("/get_object_checksum_per_part_header"),
        AWS_SCVM_REQUEST_ONLY,
        true /*expected_did_validate*/);
}

/* Here the object's CRC32 is only advertised on the HEAD response and the part responses carry no checksum of their
 * own, so validation is only possible by combining the parts against the discovered value -- which spans more than
 * one response. AWS_SCVM_REQUEST_ONLY neither makes that HEAD request nor uses its checksum, leaving nothing to
 * validate: the same download that multipart_download_checksum_combine_mock_server reports as validated finishes
 * unvalidated here. */
TEST_CASE(get_object_checksum_validation_request_only_skips_whole_object_mock_server) {
    (void)ctx;
    return s_test_get_object_checksum_validation_mode(
        allocator,
        aws_byte_cursor_from_c_str("/get_object_checksum_combine"),
        AWS_SCVM_REQUEST_ONLY,
        false /*expected_did_validate*/);
}

/* AWS_SCVM_FULL_OBJECT asks for the whole download to be validated, discovering the checksum since the caller
 * supplied none: the same result the default mode gives. */
TEST_CASE(get_object_checksum_validation_full_object_mock_server) {
    (void)ctx;
    return s_test_get_object_checksum_validation_mode(
        allocator,
        aws_byte_cursor_from_c_str("/get_object_checksum_combine"),
        AWS_SCVM_FULL_OBJECT,
        true /*expected_did_validate*/);
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

/* GET pause with one part blocked: verify the resume token's byte counters.
 * Mock object: 256 KiB served in 64 KiB parts (4 parts). The mock server delays part 2
 * (offset 65536), so parts 1, 3 and 4 complete on the network while part 2 is stalled.
 * Only part 1 can be delivered (in-order delivery is blocked by the part 2 gap), so pausing
 * must produce a token with continuous_downloaded_bytes == total_downloaded_bytes == 64 KiB:
 * parts 3 and 4 were cancelled undelivered and their bytes must not be counted. */

struct get_pause_token_mock_test_data {
    struct aws_s3_tester *tester;
    struct aws_atomic_var parts_completed;
    struct aws_atomic_var pause_initiated;
    struct aws_mutex mutex;
    struct aws_s3_meta_request_resume_token *resume_token;
    int pause_error_code;
    bool pause_callback_invoked;
};

static struct get_pause_token_mock_test_data s_get_pause_token_test_data;

static void s_get_pause_token_mock_pause_complete(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_meta_request_resume_token *resume_token,
    int error_code,
    void *user_data) {
    (void)meta_request;
    struct get_pause_token_mock_test_data *test_data = user_data;

    aws_mutex_lock(&test_data->mutex);
    test_data->pause_callback_invoked = true;
    test_data->pause_error_code = error_code;
    if (resume_token != NULL) {
        test_data->resume_token =
            aws_s3_meta_request_resume_token_acquire((struct aws_s3_meta_request_resume_token *)resume_token);
    }
    aws_mutex_unlock(&test_data->mutex);
}

static void s_get_pause_token_mock_finished_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    int error_code) {
    struct get_pause_token_mock_test_data *test_data = &s_get_pause_token_test_data;

    /* Let the real handler run first so the delivered-parts state is updated before pausing. */
    struct aws_s3_meta_request_vtable *original =
        aws_s3_tester_get_meta_request_vtable_patch(test_data->tester, 0)->original_vtable;
    original->finished_request(meta_request, request, error_code);

    if ((error_code == AWS_ERROR_SUCCESS) &&
        (request->request_tag == AWS_S3_AUTO_RANGE_GET_REQUEST_TYPE_GET_OBJECT_WITH_RANGE)) {
        size_t completed = (size_t)aws_atomic_fetch_add(&test_data->parts_completed, 1) + 1;
        if (completed >= 3) {
            size_t expected = false;
            if (aws_atomic_compare_exchange_int(&test_data->pause_initiated, &expected, true)) {
                aws_s3_meta_request_pause_async(meta_request, s_get_pause_token_mock_pause_complete, test_data);
            }
        }
    }
}

static struct aws_s3_meta_request *s_get_pause_token_mock_meta_request_factory(
    struct aws_s3_client *client,
    const struct aws_s3_meta_request_options *options) {
    struct aws_s3_tester *tester = client->shutdown_callback_user_data;
    struct aws_s3_client_vtable *original_client_vtable =
        aws_s3_tester_get_client_vtable_patch(tester, 0)->original_vtable;
    struct aws_s3_meta_request *meta_request = original_client_vtable->meta_request_factory(client, options);
    struct aws_s3_meta_request_vtable *patched = aws_s3_tester_patch_meta_request_vtable(tester, meta_request, NULL);
    patched->finished_request = s_get_pause_token_mock_finished_request;
    return meta_request;
}

TEST_CASE(get_pause_token_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    AWS_ZERO_STRUCT(s_get_pause_token_test_data);
    struct get_pause_token_mock_test_data *test_data = &s_get_pause_token_test_data;
    test_data->tester = &tester;
    aws_atomic_init_int(&test_data->parts_completed, 0);
    aws_atomic_init_int(&test_data->pause_initiated, false);
    aws_mutex_init(&test_data->mutex);

    struct aws_s3_tester_client_options client_options = {
        .part_size = 64 * 1024,
        .tls_usage = AWS_S3_TLS_DISABLED,
    };
    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_s3_client_vtable *patched_client_vtable = aws_s3_tester_patch_client_vtable(&tester, client, NULL);
    patched_client_vtable->meta_request_factory = s_get_pause_token_mock_meta_request_factory;

    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/get_object_pause_delay_part");
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

    ASSERT_UINT_EQUALS(AWS_ERROR_S3_PAUSED, out_results.finished_error_code);

    ASSERT_TRUE(test_data->pause_callback_invoked);
    ASSERT_INT_EQUALS(AWS_ERROR_SUCCESS, test_data->pause_error_code);
    ASSERT_NOT_NULL(test_data->resume_token);

    struct aws_s3_meta_request_resume_token *token = test_data->resume_token;
    ASSERT_INT_EQUALS(AWS_S3_META_REQUEST_TYPE_GET_OBJECT, aws_s3_meta_request_resume_token_type(token));
    ASSERT_UINT_EQUALS(4, aws_s3_meta_request_resume_token_total_num_parts(token));
    /* Only part 1 was delivered; parts 3, 4 completed on the network but were cancelled
     * undelivered at pause, so they must not be counted. In-order delivery keeps the two
     * counters equal. */
    ASSERT_UINT_EQUALS(64 * 1024, aws_s3_meta_request_resume_token_continuous_downloaded_bytes(token));
    ASSERT_UINT_EQUALS(64 * 1024, aws_s3_meta_request_resume_token_total_downloaded_bytes(token));
    ASSERT_UINT_EQUALS(0, aws_s3_meta_request_resume_token_object_range_start(token));
    ASSERT_UINT_EQUALS(256 * 1024 - 1, aws_s3_meta_request_resume_token_object_range_end(token));
    ASSERT_UINT_EQUALS(256 * 1024, aws_s3_meta_request_resume_token_object_size(token));

    struct aws_byte_cursor etag = aws_s3_meta_request_resume_token_etag(token);
    ASSERT_TRUE(aws_byte_cursor_eq_c_str(&etag, "pausetokenmocketag"));

    aws_s3_meta_request_resume_token_release(test_data->resume_token);
    aws_mutex_clean_up(&test_data->mutex);
    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

/* GET pause while writing to a file out of object order: the token's two byte counters must diverge.
 *
 * Same 256 KiB / 4 part object with part 2 delayed, but delivered to a `recv_filepath`, so parts reach
 * disk as they arrive instead of queueing behind the part 2 gap. Parts 1, 3 and 4 are therefore all
 * written before the pause takes effect, which is exactly the case the ordered path cannot produce:
 *
 *   continuous_downloaded_bytes = 64 KiB  -- the gap-free prefix stops at the missing part 2
 *   total_downloaded_bytes      = 192 KiB -- parts 1, 3 and 4 all landed
 *
 * A caller must resume from the former; resuming from the latter would skip the hole. The on-disk bytes
 * are checked against that claim, since a token saying "64 KiB contiguous" is only meaningful if the
 * first 64 KiB really are intact and the next 64 KiB really are absent. */

struct get_pause_parallel_mock_test_data {
    struct aws_s3_tester *tester;
    struct aws_atomic_var parts_completed;
    struct aws_atomic_var pause_initiated;
    struct aws_mutex mutex;
    struct aws_s3_meta_request_resume_token *resume_token;
    int pause_error_code;
    bool pause_callback_invoked;
};

static struct get_pause_parallel_mock_test_data s_get_pause_parallel_test_data;

static void s_get_pause_parallel_mock_pause_complete(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_meta_request_resume_token *resume_token,
    int error_code,
    void *user_data) {
    (void)meta_request;
    struct get_pause_parallel_mock_test_data *test_data = user_data;

    aws_mutex_lock(&test_data->mutex);
    test_data->pause_callback_invoked = true;
    test_data->pause_error_code = error_code;
    if (resume_token != NULL) {
        test_data->resume_token =
            aws_s3_meta_request_resume_token_acquire((struct aws_s3_meta_request_resume_token *)resume_token);
    }
    aws_mutex_unlock(&test_data->mutex);
}

static void s_get_pause_parallel_mock_finished_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    int error_code) {
    struct get_pause_parallel_mock_test_data *test_data = &s_get_pause_parallel_test_data;

    /* Let the real handler run first, so this part has been dispatched for writing before we pause. */
    struct aws_s3_meta_request_vtable *original =
        aws_s3_tester_get_meta_request_vtable_patch(test_data->tester, 0)->original_vtable;
    original->finished_request(meta_request, request, error_code);

    if ((error_code == AWS_ERROR_SUCCESS) &&
        (request->request_tag == AWS_S3_AUTO_RANGE_GET_REQUEST_TYPE_GET_OBJECT_WITH_RANGE)) {
        size_t completed = (size_t)aws_atomic_fetch_add(&test_data->parts_completed, 1) + 1;
        if (completed >= 3) {
            size_t expected = false;
            if (aws_atomic_compare_exchange_int(&test_data->pause_initiated, &expected, true)) {
                aws_s3_meta_request_pause_async(meta_request, s_get_pause_parallel_mock_pause_complete, test_data);
            }
        }
    }
}

static struct aws_s3_meta_request *s_get_pause_parallel_mock_meta_request_factory(
    struct aws_s3_client *client,
    const struct aws_s3_meta_request_options *options) {
    struct aws_s3_tester *tester = client->shutdown_callback_user_data;
    struct aws_s3_client_vtable *original_client_vtable =
        aws_s3_tester_get_client_vtable_patch(tester, 0)->original_vtable;
    struct aws_s3_meta_request *meta_request = original_client_vtable->meta_request_factory(client, options);
    struct aws_s3_meta_request_vtable *patched = aws_s3_tester_patch_meta_request_vtable(tester, meta_request, NULL);
    patched->finished_request = s_get_pause_parallel_mock_finished_request;
    return meta_request;
}

TEST_CASE(get_pause_token_parallel_write_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    AWS_ZERO_STRUCT(s_get_pause_parallel_test_data);
    struct get_pause_parallel_mock_test_data *test_data = &s_get_pause_parallel_test_data;
    test_data->tester = &tester;
    aws_atomic_init_int(&test_data->parts_completed, 0);
    aws_atomic_init_int(&test_data->pause_initiated, false);
    aws_mutex_init(&test_data->mutex);

    struct aws_s3_tester_client_options client_options = {
        .part_size = 64 * 1024,
        .tls_usage = AWS_S3_TLS_DISABLED,
    };
    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_s3_client_vtable *patched_client_vtable = aws_s3_tester_patch_client_vtable(&tester, client, NULL);
    patched_client_vtable->meta_request_factory = s_get_pause_parallel_mock_meta_request_factory;

    struct aws_s3_tester_meta_request_options get_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .client = client,
        .get_options =
            {
                .object_path = aws_byte_cursor_from_c_str("/get_object_pause_delay_part_positional"),
                .file_on_disk = true,
                .recv_file_option = AWS_S3_RECV_FILE_CREATE_OR_REPLACE,
                .capture_file_content = true,
            },
        .mock_server = true,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE,
    };
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);
    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &get_options, &out_results));

    ASSERT_UINT_EQUALS(AWS_ERROR_S3_PAUSED, out_results.finished_error_code);

    /* Without this the counters below could match for the boring reason that delivery was ordered. */
    ASSERT_TRUE(out_results.out_of_order_delivery);

    ASSERT_TRUE(test_data->pause_callback_invoked);
    ASSERT_INT_EQUALS(AWS_ERROR_SUCCESS, test_data->pause_error_code);
    ASSERT_NOT_NULL(test_data->resume_token);

    struct aws_s3_meta_request_resume_token *token = test_data->resume_token;
    ASSERT_INT_EQUALS(AWS_S3_META_REQUEST_TYPE_GET_OBJECT, aws_s3_meta_request_resume_token_type(token));
    ASSERT_UINT_EQUALS(4, aws_s3_meta_request_resume_token_total_num_parts(token));

    /* The divergence this test exists for. */
    ASSERT_UINT_EQUALS(S_PART_SIZE, aws_s3_meta_request_resume_token_continuous_downloaded_bytes(token));
    ASSERT_UINT_EQUALS(3 * S_PART_SIZE, aws_s3_meta_request_resume_token_total_downloaded_bytes(token));

    /* Part 4 extends the file to its full length even though part 2 never arrived, so the file is
     * longer than the bytes actually written -- the hole is inside it. */
    ASSERT_UINT_EQUALS(4 * S_PART_SIZE, out_results.received_file_size);
    ASSERT_UINT_EQUALS(4 * S_PART_SIZE, out_results.received_file_content.len);

    const uint8_t *bytes = out_results.received_file_content.buffer;

    /* The prefix the token calls contiguous must actually be intact, byte for byte. */
    for (size_t i = 0; i < S_PART_SIZE; ++i) {
        ASSERT_UINT_EQUALS((uint8_t)(32 + (i % 90)), bytes[i]);
    }

    /* Part 2's range must be a hole. Reading as zeros is what makes resuming from
     * total_downloaded_bytes wrong: those bytes were never written. */
    for (size_t i = S_PART_SIZE; i < 2 * S_PART_SIZE; ++i) {
        ASSERT_UINT_EQUALS(0, bytes[i]);
    }

    /* Parts 3 and 4 landed at their own offsets rather than being packed in behind the gap, which is
     * what the total counter is claiming. */
    for (size_t i = 2 * S_PART_SIZE; i < 4 * S_PART_SIZE; ++i) {
        ASSERT_UINT_EQUALS((uint8_t)(32 + (i % 90)), bytes[i]);
    }

    aws_s3_meta_request_resume_token_release(test_data->resume_token);
    aws_mutex_clean_up(&test_data->mutex);
    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

/* GET pause with sequential (gap-free) delivery: verify continues == total.
 * Same 256 KiB mock object, but part 3 is the delayed one, so parts 1 and 2 deliver
 * in order. The pause is triggered from the progress callback after 3 progress events
 * (parts 1, 2, 4 network-complete; part 4 is blocked from delivery by the part 3 gap),
 * so exactly parts 1 and 2 are delivered when the token is built. */

struct get_pause_seq_mock_test_data {
    struct aws_atomic_var progress_events;
    struct aws_atomic_var pause_initiated;
    struct aws_mutex mutex;
    struct aws_s3_meta_request_resume_token *resume_token;
    int pause_error_code;
    bool pause_callback_invoked;
};

static struct get_pause_seq_mock_test_data s_get_pause_seq_test_data;

static void s_get_pause_seq_mock_pause_complete(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_meta_request_resume_token *resume_token,
    int error_code,
    void *user_data) {
    (void)meta_request;
    struct get_pause_seq_mock_test_data *test_data = user_data;

    aws_mutex_lock(&test_data->mutex);
    test_data->pause_callback_invoked = true;
    test_data->pause_error_code = error_code;
    if (resume_token != NULL) {
        test_data->resume_token =
            aws_s3_meta_request_resume_token_acquire((struct aws_s3_meta_request_resume_token *)resume_token);
    }
    aws_mutex_unlock(&test_data->mutex);
}

static void s_get_pause_seq_mock_progress(
    struct aws_s3_meta_request *meta_request,
    const struct aws_s3_meta_request_progress *progress,
    void *user_data) {
    (void)progress;
    (void)user_data;
    struct get_pause_seq_mock_test_data *test_data = &s_get_pause_seq_test_data;

    size_t events = aws_atomic_fetch_add(&test_data->progress_events, 1) + 1;
    if (events >= 3) {
        size_t expected = false;
        if (aws_atomic_compare_exchange_int(&test_data->pause_initiated, &expected, true)) {
            aws_s3_meta_request_pause_async(meta_request, s_get_pause_seq_mock_pause_complete, test_data);
        }
    }
}

TEST_CASE(get_pause_sequential_token_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    AWS_ZERO_STRUCT(s_get_pause_seq_test_data);
    struct get_pause_seq_mock_test_data *test_data = &s_get_pause_seq_test_data;
    aws_atomic_init_int(&test_data->progress_events, 0);
    aws_atomic_init_int(&test_data->pause_initiated, false);
    aws_mutex_init(&test_data->mutex);

    struct aws_s3_tester_client_options client_options = {
        .part_size = 64 * 1024,
        .tls_usage = AWS_S3_TLS_DISABLED,
    };
    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/get_object_pause_delay_part_3");
    struct aws_s3_tester_meta_request_options get_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .progress_callback = s_get_pause_seq_mock_progress,
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

    ASSERT_UINT_EQUALS(AWS_ERROR_S3_PAUSED, out_results.finished_error_code);

    ASSERT_TRUE(test_data->pause_callback_invoked);
    ASSERT_INT_EQUALS(AWS_ERROR_SUCCESS, test_data->pause_error_code);
    ASSERT_NOT_NULL(test_data->resume_token);

    struct aws_s3_meta_request_resume_token *token = test_data->resume_token;
    /* Parts 1 and 2 were delivered back to back: gap-free, so both counters are equal. */
    ASSERT_UINT_EQUALS(2 * 64 * 1024, aws_s3_meta_request_resume_token_continuous_downloaded_bytes(token));
    ASSERT_UINT_EQUALS(2 * 64 * 1024, aws_s3_meta_request_resume_token_total_downloaded_bytes(token));
    ASSERT_UINT_EQUALS(4, aws_s3_meta_request_resume_token_total_num_parts(token));
    ASSERT_UINT_EQUALS(256 * 1024, aws_s3_meta_request_resume_token_object_size(token));

    struct aws_byte_cursor etag = aws_s3_meta_request_resume_token_etag(token);
    ASSERT_TRUE(aws_byte_cursor_eq_c_str(&etag, "pausetokenmocketag"));

    aws_s3_meta_request_resume_token_release(test_data->resume_token);
    aws_mutex_clean_up(&test_data->mutex);
    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

/* GET pause before size discovery: the discovery part is delayed and the pause is issued
 * as soon as the meta request is created, so the token must carry sane empty values:
 * no etag, zero range/size, zero byte counters. */

struct get_pause_early_mock_test_data {
    struct aws_mutex mutex;
    struct aws_s3_meta_request_resume_token *resume_token;
    int pause_error_code;
    bool pause_callback_invoked;
    struct aws_s3_tester *tester;
};

static struct get_pause_early_mock_test_data s_get_pause_early_test_data;

static void s_get_pause_early_mock_pause_complete(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_meta_request_resume_token *resume_token,
    int error_code,
    void *user_data) {
    (void)meta_request;
    struct get_pause_early_mock_test_data *test_data = user_data;

    aws_mutex_lock(&test_data->mutex);
    test_data->pause_callback_invoked = true;
    test_data->pause_error_code = error_code;
    if (resume_token != NULL) {
        test_data->resume_token =
            aws_s3_meta_request_resume_token_acquire((struct aws_s3_meta_request_resume_token *)resume_token);
    }
    aws_mutex_unlock(&test_data->mutex);
}

static struct aws_s3_meta_request *s_get_pause_early_mock_meta_request_factory(
    struct aws_s3_client *client,
    const struct aws_s3_meta_request_options *options) {
    struct aws_s3_tester *tester = client->shutdown_callback_user_data;
    struct aws_s3_client_vtable *original_client_vtable =
        aws_s3_tester_get_client_vtable_patch(tester, 0)->original_vtable;
    struct aws_s3_meta_request *meta_request = original_client_vtable->meta_request_factory(client, options);
    /* Pause as early as possible: before the discovery request can complete
     * (its response is delayed by the mock server). */
    aws_s3_meta_request_pause_async(meta_request, s_get_pause_early_mock_pause_complete, &s_get_pause_early_test_data);
    return meta_request;
}

TEST_CASE(get_pause_before_discovery_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    AWS_ZERO_STRUCT(s_get_pause_early_test_data);
    struct get_pause_early_mock_test_data *test_data = &s_get_pause_early_test_data;
    test_data->tester = &tester;
    aws_mutex_init(&test_data->mutex);

    struct aws_s3_tester_client_options client_options = {
        .part_size = 64 * 1024,
        .tls_usage = AWS_S3_TLS_DISABLED,
    };
    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_s3_client_vtable *patched_client_vtable = aws_s3_tester_patch_client_vtable(&tester, client, NULL);
    patched_client_vtable->meta_request_factory = s_get_pause_early_mock_meta_request_factory;

    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/get_object_pause_delay_first_part");
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

    ASSERT_UINT_EQUALS(AWS_ERROR_S3_PAUSED, out_results.finished_error_code);

    ASSERT_TRUE(test_data->pause_callback_invoked);
    ASSERT_INT_EQUALS(AWS_ERROR_SUCCESS, test_data->pause_error_code);
    ASSERT_NOT_NULL(test_data->resume_token);

    struct aws_s3_meta_request_resume_token *token = test_data->resume_token;
    ASSERT_INT_EQUALS(AWS_S3_META_REQUEST_TYPE_GET_OBJECT, aws_s3_meta_request_resume_token_type(token));
    /* Nothing was discovered or transferred yet: everything except part_size is zero/empty. */
    ASSERT_UINT_EQUALS(64 * 1024, aws_s3_meta_request_resume_token_part_size(token));
    ASSERT_UINT_EQUALS(0, aws_s3_meta_request_resume_token_total_num_parts(token));
    ASSERT_UINT_EQUALS(0, aws_s3_meta_request_resume_token_num_parts_completed(token));
    ASSERT_UINT_EQUALS(0, aws_s3_meta_request_resume_token_object_size(token));
    ASSERT_UINT_EQUALS(0, aws_s3_meta_request_resume_token_object_range_start(token));
    ASSERT_UINT_EQUALS(0, aws_s3_meta_request_resume_token_object_range_end(token));
    ASSERT_UINT_EQUALS(0, aws_s3_meta_request_resume_token_continuous_downloaded_bytes(token));
    ASSERT_UINT_EQUALS(0, aws_s3_meta_request_resume_token_total_downloaded_bytes(token));
    ASSERT_UINT_EQUALS(0, aws_s3_meta_request_resume_token_file_last_modified_epoch_ns(token));

    struct aws_byte_cursor etag = aws_s3_meta_request_resume_token_etag(token);
    ASSERT_UINT_EQUALS(0, etag.len);
    struct aws_byte_cursor upload_id = aws_s3_meta_request_resume_token_upload_id(token);
    ASSERT_UINT_EQUALS(0, upload_id.len);

    aws_s3_meta_request_resume_token_release(test_data->resume_token);
    aws_mutex_clean_up(&test_data->mutex);
    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

/* GET failure mid-download: part 3 fails with 403 after parts 1 and 2 were delivered.
 * The on_error_resume_token callback must fire with the meta request's error code and a
 * token carrying the delivered byte counters. */

struct get_error_token_mock_test_data {
    struct aws_mutex mutex;
    struct aws_s3_meta_request_resume_token *resume_token;
    int error_code;
    bool error_callback_invoked;
};

static struct get_error_token_mock_test_data s_get_error_token_test_data;

static void s_get_error_token_mock_on_error(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_meta_request_resume_token *resume_token,
    int error_code,
    void *user_data) {
    (void)meta_request;
    (void)user_data; /* tester's user_data; use the static test data instead. */
    struct get_error_token_mock_test_data *test_data = &s_get_error_token_test_data;

    aws_mutex_lock(&test_data->mutex);
    test_data->error_callback_invoked = true;
    test_data->error_code = error_code;
    if (resume_token != NULL) {
        test_data->resume_token =
            aws_s3_meta_request_resume_token_acquire((struct aws_s3_meta_request_resume_token *)resume_token);
    }
    aws_mutex_unlock(&test_data->mutex);
}

/* mock that download creds exipred, got 403 in the mid of download. */
TEST_CASE(get_error_token_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    AWS_ZERO_STRUCT(s_get_error_token_test_data);
    struct get_error_token_mock_test_data *test_data = &s_get_error_token_test_data;
    aws_mutex_init(&test_data->mutex);

    struct aws_s3_tester_client_options client_options = {
        .part_size = 64 * 1024,
        .tls_usage = AWS_S3_TLS_DISABLED,
    };
    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/get_object_error_part_3");
    struct aws_s3_tester_meta_request_options get_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .on_error_resume_token = s_get_error_token_mock_on_error,
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

    /* The download must have failed (403 on part 3), not paused. */
    ASSERT_TRUE(out_results.finished_error_code != AWS_ERROR_SUCCESS);
    ASSERT_TRUE(out_results.finished_error_code != AWS_ERROR_S3_PAUSED);
    ASSERT_UINT_EQUALS(AWS_HTTP_STATUS_CODE_403_FORBIDDEN, out_results.finished_response_status);

    ASSERT_TRUE(test_data->error_callback_invoked);
    ASSERT_INT_EQUALS(out_results.finished_error_code, test_data->error_code);
    ASSERT_NOT_NULL(test_data->resume_token);

    struct aws_s3_meta_request_resume_token *token = test_data->resume_token;
    ASSERT_INT_EQUALS(AWS_S3_META_REQUEST_TYPE_GET_OBJECT, aws_s3_meta_request_resume_token_type(token));
    /* Parts 1 and 2 were delivered before part 3's failure (its response is delayed);
     * part 4 completed but was blocked from delivery by the part 3 gap. */
    ASSERT_UINT_EQUALS(2 * 64 * 1024, aws_s3_meta_request_resume_token_continuous_downloaded_bytes(token));
    ASSERT_UINT_EQUALS(2 * 64 * 1024, aws_s3_meta_request_resume_token_total_downloaded_bytes(token));
    ASSERT_UINT_EQUALS(4, aws_s3_meta_request_resume_token_total_num_parts(token));
    ASSERT_UINT_EQUALS(256 * 1024, aws_s3_meta_request_resume_token_object_size(token));

    struct aws_byte_cursor etag = aws_s3_meta_request_resume_token_etag(token);
    ASSERT_TRUE(aws_byte_cursor_eq_c_str(&etag, "pausetokenmocketag"));

    aws_s3_meta_request_resume_token_release(test_data->resume_token);
    aws_mutex_clean_up(&test_data->mutex);
    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

/* Same mid-download failure, but downloading to a file with recv_file_delete_on_failure:
 * the partial file (the state a download token refers to) is deleted on error, so the
 * on_error_resume_token callback must fire with a NULL token (it always fires exactly
 * once on error — bindings may wrap it in a future, so it cannot be skipped). The tester
 * separately asserts the file is gone after the failure. */
TEST_CASE(get_error_token_delete_on_failure_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    AWS_ZERO_STRUCT(s_get_error_token_test_data);
    struct get_error_token_mock_test_data *test_data = &s_get_error_token_test_data;
    aws_mutex_init(&test_data->mutex);

    struct aws_s3_tester_client_options client_options = {
        .part_size = 64 * 1024,
        .tls_usage = AWS_S3_TLS_DISABLED,
    };
    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/get_object_error_part_3");
    struct aws_s3_tester_meta_request_options get_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .on_error_resume_token = s_get_error_token_mock_on_error,
        .client = client,
        .get_options =
            {
                .object_path = object_path,
                .file_on_disk = true,
                .recv_file_delete_on_failure = true,
            },
        .mock_server = true,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE,
    };
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);
    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &get_options, &out_results));

    /* The download must have failed (403 on part 3), not paused. */
    ASSERT_TRUE(out_results.finished_error_code != AWS_ERROR_SUCCESS);
    ASSERT_TRUE(out_results.finished_error_code != AWS_ERROR_S3_PAUSED);
    ASSERT_UINT_EQUALS(AWS_HTTP_STATUS_CODE_403_FORBIDDEN, out_results.finished_response_status);

    /* The partial file was deleted, so there is no resumable state: the callback still fires
     * exactly once with the error code, but with a NULL token. */
    ASSERT_TRUE(test_data->error_callback_invoked);
    ASSERT_INT_EQUALS(out_results.finished_error_code, test_data->error_code);
    ASSERT_NULL(test_data->resume_token);

    aws_mutex_clean_up(&test_data->mutex);
    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

/* A download that succeeds with recv_file_delete_on_failure set must keep its file. The finish call
 * only deletes on failure, but meta request destroy deletes whenever the flag is still armed, so the
 * flag has to be disarmed once the finish call has dealt with the file. The tester checks the file
 * still exists after shutdown, which is after destroy. */
TEST_CASE(get_delete_on_failure_keeps_file_on_success_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_tester_client_options client_options = {
        .part_size = 64 * 1024,
        .tls_usage = AWS_S3_TLS_DISABLED,
    };
    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_s3_tester_meta_request_options get_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .client = client,
        .get_options =
            {
                .object_path = aws_byte_cursor_from_c_str("/get_object_parallel_write"),
                .file_on_disk = true,
                .recv_file_option = AWS_S3_RECV_FILE_CREATE_OR_REPLACE,
                .recv_file_delete_on_failure = true,
            },
        .mock_server = true,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_SUCCESS,
    };
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);
    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &get_options, &out_results));
    ASSERT_UINT_EQUALS(AWS_ERROR_SUCCESS, out_results.finished_error_code);

    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

/* Sends a GET that must fail during creation with `expected_error`, then resets the tester's finish and
 * shutdown counts so the next request on the same tester is not held to them. */
static int s_send_get_expecting_creation_error(
    struct aws_s3_tester *tester,
    struct aws_s3_tester_meta_request_options *get_options,
    int expected_error) {

    get_options->validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE;
    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(tester, get_options, NULL));
    ASSERT_INT_EQUALS(expected_error, aws_last_error());

    aws_s3_tester_lock_synced_data(tester);
    ASSERT_UINT_EQUALS(0, tester->synced_data.meta_request_shutdown_count);
    ASSERT_UINT_EQUALS(0, tester->synced_data.meta_request_finish_count);
    tester->synced_data.desired_meta_request_shutdown_count = 0;
    tester->synced_data.desired_meta_request_finish_count = 0;
    aws_s3_tester_unlock_synced_data(tester);
    return AWS_OP_SUCCESS;
}

/* recv_file_delete_on_failure is rejected at creation wherever a failure would delete content this transfer
 * did not write: WRITE_TO_POSITION always, and CREATE_OR_APPEND onto an existing file. CREATE_OR_APPEND onto a
 * missing file is still allowed, since the transfer creates it. */
TEST_CASE(get_delete_on_failure_rejects_existing_file_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_tester_client_options client_options = {
        .part_size = 64 * 1024,
        .tls_usage = AWS_S3_TLS_DISABLED,
    };
    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_s3_tester_meta_request_options get_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .client = client,
        .get_options =
            {
                .object_path = aws_byte_cursor_from_c_str("/get_object_parallel_write"),
                .file_on_disk = true,
                .recv_file_delete_on_failure = true,
                .pre_exist_file_length = 10,
            },
        .mock_server = true,
    };

    get_options.get_options.recv_file_option = AWS_S3_RECV_FILE_WRITE_TO_POSITION;
    ASSERT_SUCCESS(s_send_get_expecting_creation_error(&tester, &get_options, AWS_ERROR_INVALID_ARGUMENT));

    get_options.get_options.recv_file_option = AWS_S3_RECV_FILE_CREATE_OR_APPEND;
    ASSERT_SUCCESS(s_send_get_expecting_creation_error(&tester, &get_options, AWS_ERROR_INVALID_ARGUMENT));

    /* No pre-existing file, so the tester only picks a path and the download creates it. */
    get_options.get_options.pre_exist_file_length = 0;
    get_options.validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_SUCCESS;
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);
    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &get_options, &out_results));
    ASSERT_UINT_EQUALS(AWS_ERROR_SUCCESS, out_results.finished_error_code);

    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

/* PUT failure mid-upload: part 3 fails with 403 after a delay long enough for the other
 * parts to complete. The on_error_resume_token callback must fire with the meta request's
 * error code and a token carrying the upload id and part counters. */

struct put_error_token_mock_test_data {
    struct aws_mutex mutex;
    struct aws_s3_meta_request_resume_token *resume_token;
    int error_code;
    bool error_callback_invoked;
};

static struct put_error_token_mock_test_data s_put_error_token_test_data;

static void s_put_error_token_mock_on_error(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_meta_request_resume_token *resume_token,
    int error_code,
    void *user_data) {
    (void)meta_request;
    (void)user_data; /* tester's user_data; use the static test data instead. */
    struct put_error_token_mock_test_data *test_data = &s_put_error_token_test_data;

    aws_mutex_lock(&test_data->mutex);
    test_data->error_callback_invoked = true;
    test_data->error_code = error_code;
    if (resume_token != NULL) {
        test_data->resume_token =
            aws_s3_meta_request_resume_token_acquire((struct aws_s3_meta_request_resume_token *)resume_token);
    }
    aws_mutex_unlock(&test_data->mutex);
}

/* mock that upload creds expired, got 403 in the mid of upload. */
TEST_CASE(put_error_token_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    AWS_ZERO_STRUCT(s_put_error_token_test_data);
    struct put_error_token_mock_test_data *test_data = &s_put_error_token_test_data;
    aws_mutex_init(&test_data->mutex);

    struct aws_s3_tester_client_options client_options = {
        .part_size = MB_TO_BYTES(5),
        .tls_usage = AWS_S3_TLS_DISABLED,
    };
    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/upload_part_error_part_3");
    struct aws_s3_tester_meta_request_options put_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .on_error_resume_token = s_put_error_token_mock_on_error,
        .client = client,
        .put_options =
            {
                .object_size_mb = 20, /* 4 parts of 5 MiB. */
                .object_path_override = object_path,
            },
        .mock_server = true,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE,
    };
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);
    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &put_options, &out_results));

    /* The upload must have failed (403 on part 3), not paused. */
    ASSERT_TRUE(out_results.finished_error_code != AWS_ERROR_SUCCESS);
    ASSERT_TRUE(out_results.finished_error_code != AWS_ERROR_S3_PAUSED);
    ASSERT_UINT_EQUALS(AWS_HTTP_STATUS_CODE_403_FORBIDDEN, out_results.finished_response_status);

    ASSERT_TRUE(test_data->error_callback_invoked);
    ASSERT_INT_EQUALS(out_results.finished_error_code, test_data->error_code);
    ASSERT_NOT_NULL(test_data->resume_token);

    struct aws_s3_meta_request_resume_token *token = test_data->resume_token;
    ASSERT_INT_EQUALS(AWS_S3_META_REQUEST_TYPE_PUT_OBJECT, aws_s3_meta_request_resume_token_type(token));
    ASSERT_UINT_EQUALS(MB_TO_BYTES(5), aws_s3_meta_request_resume_token_part_size(token));
    ASSERT_UINT_EQUALS(4, aws_s3_meta_request_resume_token_total_num_parts(token));
    /* All 4 parts were sent and finished before wind-down (3 succeeded, part 3 failed);
     * num_parts_completed counts every finished part, including the failed one. */
    ASSERT_UINT_EQUALS(4, aws_s3_meta_request_resume_token_num_parts_completed(token));

    struct aws_byte_cursor upload_id = aws_s3_meta_request_resume_token_upload_id(token);
    ASSERT_TRUE(aws_byte_cursor_eq_c_str(&upload_id, "defaultID"));

    /* Download-only fields must be zero/empty on an upload token. */
    ASSERT_UINT_EQUALS(0, aws_s3_meta_request_resume_token_object_size(token));
    ASSERT_UINT_EQUALS(0, aws_s3_meta_request_resume_token_continuous_downloaded_bytes(token));
    ASSERT_UINT_EQUALS(0, aws_s3_meta_request_resume_token_total_downloaded_bytes(token));
    struct aws_byte_cursor etag = aws_s3_meta_request_resume_token_etag(token);
    ASSERT_UINT_EQUALS(0, etag.len);

    aws_s3_meta_request_resume_token_release(test_data->resume_token);
    aws_mutex_clean_up(&test_data->mutex);
    aws_s3_meta_request_test_results_clean_up(&out_results);
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

    /* 2. Create request with host info mismatch endpoint override */
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
