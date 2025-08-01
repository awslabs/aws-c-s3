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

static int s_validate_mpu_mock_server_metrics(struct aws_array_list *metrics_list) {
    /* Check the size of the metrics should be the same as the number of requests, which should be create MPU, two
     * upload parts and one complete MPU */
    ASSERT_UINT_EQUALS(4, aws_array_list_length(metrics_list));
    struct aws_s3_request_metrics *metrics = NULL;

    /* First metrics should be the CreateMPU */
    aws_array_list_get_at(metrics_list, (void **)&metrics, 0);
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
    size_t connection_id = 0;
    ASSERT_SUCCESS(aws_s3_request_metrics_get_connection_id(metrics, &connection_id));
    ASSERT_UINT_EQUALS(AWS_ERROR_SUCCESS, aws_s3_request_metrics_get_error_code(metrics));
    /* Get all those time stamp */
    uint64_t time_stamp = 0;
    aws_s3_request_metrics_get_start_timestamp_ns(metrics, &time_stamp);
    ASSERT_FALSE(time_stamp == 0);
    time_stamp = 0;
    aws_s3_request_metrics_get_end_timestamp_ns(metrics, &time_stamp);
    ASSERT_FALSE(time_stamp == 0);
    time_stamp = 0;
    aws_s3_request_metrics_get_total_duration_ns(metrics, &time_stamp);
    ASSERT_FALSE(time_stamp == 0);
    time_stamp = 0;
    ASSERT_SUCCESS(aws_s3_request_metrics_get_send_start_timestamp_ns(metrics, &time_stamp));
    ASSERT_FALSE(time_stamp == 0);
    time_stamp = 0;
    ASSERT_SUCCESS(aws_s3_request_metrics_get_send_end_timestamp_ns(metrics, &time_stamp));
    ASSERT_FALSE(time_stamp == 0);
    time_stamp = 0;
    ASSERT_SUCCESS(aws_s3_request_metrics_get_sending_duration_ns(metrics, &time_stamp));
    ASSERT_FALSE(time_stamp == 0);
    time_stamp = 0;
    ASSERT_SUCCESS(aws_s3_request_metrics_get_receive_start_timestamp_ns(metrics, &time_stamp));
    ASSERT_FALSE(time_stamp == 0);
    time_stamp = 0;
    ASSERT_SUCCESS(aws_s3_request_metrics_get_receive_end_timestamp_ns(metrics, &time_stamp));
    ASSERT_FALSE(time_stamp == 0);
    time_stamp = 0;
    ASSERT_SUCCESS(aws_s3_request_metrics_get_receiving_duration_ns(metrics, &time_stamp));
    ASSERT_FALSE(time_stamp == 0);
    time_stamp = 0;
    enum aws_s3_request_type request_type = 0;
    aws_s3_request_metrics_get_request_type(metrics, &request_type);
    ASSERT_UINT_EQUALS(AWS_S3_REQUEST_TYPE_CREATE_MULTIPART_UPLOAD, request_type);

    const struct aws_string *operation_name = NULL;
    ASSERT_SUCCESS(aws_s3_request_metrics_get_operation_name(metrics, &operation_name));
    ASSERT_STR_EQUALS("CreateMultipartUpload", aws_string_c_str(operation_name));

    /* Second metrics should be the Upload Part */
    aws_array_list_get_at(metrics_list, (void **)&metrics, 1);
    struct aws_byte_cursor header_value;
    AWS_ZERO_STRUCT(header_value);
    response_headers = NULL;
    ASSERT_SUCCESS(aws_s3_request_metrics_get_response_headers(metrics, &response_headers));
    ASSERT_SUCCESS(aws_http_headers_get(response_headers, aws_byte_cursor_from_c_str("ETag"), &header_value));
    ASSERT_TRUE(aws_byte_cursor_eq_c_str(&header_value, "b54357faf0632cce46e942fa68356b38"));
    ASSERT_SUCCESS(aws_http_headers_get(response_headers, aws_byte_cursor_from_c_str("Connection"), &header_value));
    ASSERT_TRUE(aws_byte_cursor_eq_c_str(&header_value, "keep-alive"));
    request_type = 0;
    aws_s3_request_metrics_get_request_type(metrics, &request_type);
    ASSERT_UINT_EQUALS(AWS_S3_REQUEST_TYPE_UPLOAD_PART, request_type);
    ASSERT_SUCCESS(aws_s3_request_metrics_get_operation_name(metrics, &operation_name));
    ASSERT_STR_EQUALS("UploadPart", aws_string_c_str(operation_name));

    /* Third metrics still be Upload Part */
    aws_array_list_get_at(metrics_list, (void **)&metrics, 2);
    request_type = 0;
    aws_s3_request_metrics_get_request_type(metrics, &request_type);
    ASSERT_UINT_EQUALS(AWS_S3_REQUEST_TYPE_UPLOAD_PART, request_type);
    ASSERT_SUCCESS(aws_s3_request_metrics_get_operation_name(metrics, &operation_name));
    ASSERT_STR_EQUALS("UploadPart", aws_string_c_str(operation_name));

    /* Fourth should be complete MPU */
    aws_array_list_get_at(metrics_list, (void **)&metrics, 3);
    request_type = 0;
    aws_s3_request_metrics_get_request_type(metrics, &request_type);
    ASSERT_UINT_EQUALS(AWS_S3_REQUEST_TYPE_COMPLETE_MULTIPART_UPLOAD, request_type);
    ASSERT_SUCCESS(aws_s3_request_metrics_get_operation_name(metrics, &operation_name));
    ASSERT_STR_EQUALS("CompleteMultipartUpload", aws_string_c_str(operation_name));
    /* All the rest should be similar */

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
    ASSERT_SUCCESS(s_validate_mpu_mock_server_metrics(&out_results.synced_data.metrics));
    aws_s3_meta_request_test_results_clean_up(&out_results);
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
    ASSERT_SUCCESS(s_validate_mpu_mock_server_metrics(&out_results.synced_data.metrics));

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
static void s_after_prepare_upload_part_finish(struct aws_s3_request *request) {
    if (request->num_times_prepared > 1) {
        /* mock that the body buffer was messed up in memory */
        request->request_body.buffer[1]++;
    }
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
    patched_client_vtable->after_prepare_upload_part_finish = s_after_prepare_upload_part_finish;

    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/throttle");
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
