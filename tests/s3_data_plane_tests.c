/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_client_impl.h"
#include "aws/s3/private/s3_meta_request_impl.h"
#include "aws/s3/private/s3_util.h"
#include "s3_tester.h"
#include <aws/common/byte_buf.h>
#include <aws/common/clock.h>
#include <aws/common/common.h>
#include <aws/common/ref_count.h>
#include <aws/http/request_response.h>
#include <aws/io/stream.h>
#include <aws/testing/aws_test_harness.h>
#include <inttypes.h>

static const struct aws_byte_cursor s_test_body_content_type = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("text/plain");

static const struct aws_byte_cursor s_test_s3_region = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("us-west-2");
static const struct aws_byte_cursor s_test_bucket_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("aws-crt-canary-bucket");

AWS_TEST_CASE(test_s3_client_create_destroy, s_test_s3_client_create_destroy)
static int s_test_s3_client_create_destroy(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client_config client_config = {
        .region = s_test_s3_region,
    };

    ASSERT_SUCCESS(aws_s3_tester_bind_client(&tester, &client_config));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    ASSERT_TRUE(client != NULL);

    aws_s3_client_release(client);
    client = NULL;

    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(test_s3_request_create_destroy, s_test_s3_request_create_destroy)
static int s_test_s3_request_create_destroy(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    const uint32_t part_number = 1234;
    const uint32_t request_tag = 5678;

    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client *dummy_client = aws_s3_tester_dummy_client_new(&tester);
    ASSERT_TRUE(dummy_client != NULL);

    struct aws_s3_meta_request *dummy_meta_request = aws_s3_tester_dummy_meta_request_new(&tester, dummy_client);
    ASSERT_TRUE(dummy_meta_request != NULL);

    struct aws_http_message *request_message = aws_s3_tester_dummy_http_request_new(&tester);
    ASSERT_TRUE(request_message != NULL);

    struct aws_s3_request *request =
        aws_s3_request_new(dummy_meta_request, request_tag, part_number, AWS_S3_REQUEST_DESC_RECORD_RESPONSE_HEADERS);

    ASSERT_TRUE(request != NULL);

    ASSERT_TRUE(request->meta_request == dummy_meta_request);
    ASSERT_TRUE(request->desc_data.part_number == part_number);
    ASSERT_TRUE(request->desc_data.request_tag == request_tag);
    ASSERT_TRUE(request->desc_data.record_response_headers == true);

    aws_s3_request_setup_send_data(request, request_message);

    ASSERT_TRUE(request->send_data.message != NULL);
    ASSERT_TRUE(request->send_data.response_headers != NULL);

    aws_s3_request_clean_up_send_data(request);

    ASSERT_TRUE(request->send_data.message == NULL);
    ASSERT_TRUE(request->send_data.response_headers == NULL);
    ASSERT_TRUE(request->send_data.part_buffer == NULL);
    ASSERT_TRUE(request->send_data.response_status == 0);
    ASSERT_TRUE(request->send_data.finished_callback == 0);
    ASSERT_TRUE(request->send_data.user_data == 0);

    aws_s3_request_release(request);
    aws_http_message_release(request_message);
    aws_s3_meta_request_release(dummy_meta_request);
    aws_s3_client_release(dummy_client);

    aws_s3_tester_clean_up(&tester);

    return 0;
}

static int s_validate_successful_get_entire_object(struct aws_s3_tester_meta_request *tester_meta_request);

AWS_TEST_CASE(test_s3_get_object, s_test_s3_get_object)
static int s_test_s3_get_object(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    const struct aws_byte_cursor test_object_path = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("/get_object_test_1MB.txt");

    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client_config client_config = {
        .region = s_test_s3_region,
        .part_size = 64 * 1024,
    };

    ASSERT_SUCCESS(aws_s3_tester_bind_client(&tester, &client_config));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    struct aws_string *host_name =
        aws_s3_tester_build_endpoint_string(allocator, &s_test_bucket_name, &s_test_s3_region);

    /* Put together a simple S3 Get Object request. */
    struct aws_http_message *message =
        aws_s3_test_make_get_object_request(allocator, aws_byte_cursor_from_string(host_name), test_object_path);

    struct aws_s3_meta_request_options options;
    AWS_ZERO_STRUCT(options);
    options.type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT;
    options.message = message;

    /* Trigger accelerating of our Get Object request. */
    struct aws_s3_tester_meta_request tester_meta_request;

    ASSERT_SUCCESS(aws_s3_tester_bind_meta_request(&tester, &options, &tester_meta_request));

    struct aws_s3_meta_request *meta_request = aws_s3_client_make_meta_request(client, &options);

    ASSERT_TRUE(meta_request != NULL);

    /* Wait for the request to finish. */
    aws_s3_tester_wait_for_finish(&tester);

    aws_s3_tester_lock_synced_data(&tester);
    ASSERT_TRUE(tester.synced_data.finish_error_code == AWS_ERROR_SUCCESS);
    aws_s3_tester_unlock_synced_data(&tester);

    s_validate_successful_get_entire_object(&tester_meta_request);

    aws_s3_meta_request_release(meta_request);
    aws_s3_tester_meta_request_clean_up(&tester_meta_request);

    aws_http_message_release(message);
    message = NULL;

    aws_string_destroy(host_name);
    host_name = NULL;

    aws_s3_client_release(client);
    client = NULL;

    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(test_s3_get_object_multiple, s_test_s3_get_object_multiple)
static int s_test_s3_get_object_multiple(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    const struct aws_byte_cursor test_object_path = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("/get_object_test_1MB.txt");

    struct aws_s3_meta_request *meta_requests[4];
    struct aws_s3_tester_meta_request tester_meta_requests[4];
    size_t num_meta_requests = sizeof(meta_requests) / sizeof(struct aws_s3_meta_request *);

    ASSERT_TRUE(num_meta_requests == (sizeof(tester_meta_requests) / sizeof(struct aws_s3_tester_meta_request)));

    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client_config client_config = {
        .region = s_test_s3_region,
        .part_size = 64 * 1024,
    };

    aws_s3_tester_bind_client(&tester, &client_config);

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    struct aws_string *host_name =
        aws_s3_tester_build_endpoint_string(allocator, &s_test_bucket_name, &s_test_s3_region);

    /* Put together a simple S3 Get Object request. */
    struct aws_http_message *message =
        aws_s3_test_make_get_object_request(allocator, aws_byte_cursor_from_string(host_name), test_object_path);

    for (size_t i = 0; i < num_meta_requests; ++i) {
        struct aws_s3_meta_request_options options;
        AWS_ZERO_STRUCT(options);
        options.type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT;
        options.message = message;

        ASSERT_SUCCESS(aws_s3_tester_bind_meta_request(&tester, &options, &tester_meta_requests[i]));

        /* Trigger accelerating of our Get Object request. */
        meta_requests[i] = aws_s3_client_make_meta_request(client, &options);

        ASSERT_TRUE(meta_requests[i] != NULL);
    }

    /* Wait for the request to finish. */
    aws_s3_tester_wait_for_finish(&tester);

    aws_s3_tester_lock_synced_data(&tester);
    ASSERT_TRUE(tester.synced_data.finish_error_code == AWS_ERROR_SUCCESS);
    aws_s3_tester_unlock_synced_data(&tester);

    for (size_t i = 0; i < num_meta_requests; ++i) {
        s_validate_successful_get_entire_object(&tester_meta_requests[i]);
        aws_s3_tester_meta_request_clean_up(&tester_meta_requests[i]);
        aws_s3_meta_request_release(meta_requests[i]);
        meta_requests[i] = NULL;
    }

    aws_http_message_release(message);
    message = NULL;

    aws_string_destroy(host_name);
    host_name = NULL;

    aws_s3_client_release(client);
    client = NULL;

    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(test_s3_put_object, s_test_s3_put_object)
static int s_test_s3_put_object(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    const struct aws_byte_cursor test_object_path = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("/put_object_test_10MB.txt");

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client_config client_config = {
        .region = s_test_s3_region,
        .part_size = 5 * 1024 * 1024,
    };

    ASSERT_SUCCESS(aws_s3_tester_bind_client(&tester, &client_config));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    ASSERT_TRUE(client != NULL);

    struct aws_byte_buf test_buffer;
    aws_s3_create_test_buffer(allocator, 10 * 1024 * 1024, &test_buffer);

    struct aws_byte_cursor test_body_cursor = aws_byte_cursor_from_buf(&test_buffer);
    struct aws_input_stream *input_stream = aws_input_stream_new_from_cursor(allocator, &test_body_cursor);

    struct aws_string *host_name =
        aws_s3_tester_build_endpoint_string(allocator, &s_test_bucket_name, &s_test_s3_region);

    /* Put together a simple S3 Put Object request. */
    struct aws_http_message *message = aws_s3_test_make_put_object_request(
        allocator, aws_byte_cursor_from_string(host_name), test_object_path, s_test_body_content_type, input_stream);

    struct aws_s3_meta_request_options options;
    AWS_ZERO_STRUCT(options);
    options.type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT;
    options.message = message;

    struct aws_s3_tester_meta_request tester_meta_request;
    ASSERT_SUCCESS(aws_s3_tester_bind_meta_request(&tester, &options, &tester_meta_request));

    /* Wait for the request to finish. */
    struct aws_s3_meta_request *meta_request = aws_s3_client_make_meta_request(client, &options);

    ASSERT_TRUE(meta_request != NULL);

    /* Wait for the request to finish. */
    aws_s3_tester_wait_for_finish(&tester);

    ASSERT_TRUE(tester_meta_request.finished_response_status == 200);
    ASSERT_TRUE(tester_meta_request.finished_response_status == tester_meta_request.headers_response_status);
    ASSERT_TRUE(tester_meta_request.finished_error_code == AWS_ERROR_SUCCESS);

    ASSERT_TRUE(tester_meta_request.error_response_headers == NULL);
    ASSERT_TRUE(tester_meta_request.error_response_body.len == 0);

    ASSERT_FALSE(
        aws_http_headers_has(tester_meta_request.response_headers, aws_byte_cursor_from_c_str("Content-Length")));

    struct aws_byte_cursor etag_byte_cursor;
    AWS_ZERO_STRUCT(etag_byte_cursor);
    ASSERT_SUCCESS(aws_http_headers_get(
        tester_meta_request.response_headers, aws_byte_cursor_from_c_str("ETag"), &etag_byte_cursor));
    ASSERT_TRUE(etag_byte_cursor.len > 0);

    aws_s3_tester_lock_synced_data(&tester);
    ASSERT_TRUE(tester.synced_data.finish_error_code == AWS_ERROR_SUCCESS);
    aws_s3_tester_unlock_synced_data(&tester);

    aws_s3_meta_request_release(meta_request);

    aws_s3_tester_meta_request_clean_up(&tester_meta_request);

    aws_http_message_release(message);
    message = NULL;

    aws_string_destroy(host_name);
    host_name = NULL;

    aws_s3_client_release(client);
    client = NULL;

    aws_input_stream_destroy(input_stream);
    input_stream = NULL;

    aws_byte_buf_clean_up(&test_buffer);

    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(test_s3_error_response, s_test_s3_error_response)
static int s_test_s3_error_response(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    const struct aws_byte_cursor test_object_path =
        AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("/non-existing-file12345.txt");

    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client_config client_config = {
        .region = s_test_s3_region,
        .part_size = 64 * 1024,
    };

    ASSERT_SUCCESS(aws_s3_tester_bind_client(&tester, &client_config));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    struct aws_string *host_name =
        aws_s3_tester_build_endpoint_string(allocator, &s_test_bucket_name, &s_test_s3_region);

    /* Put together a simple S3 Get Object request. */
    struct aws_http_message *message =
        aws_s3_test_make_get_object_request(allocator, aws_byte_cursor_from_string(host_name), test_object_path);

    struct aws_s3_meta_request_options options;
    AWS_ZERO_STRUCT(options);
    options.type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT;
    options.message = message;

    /* Trigger accelerating of our Get Object request. */
    struct aws_s3_tester_meta_request tester_meta_request;

    ASSERT_SUCCESS(aws_s3_tester_bind_meta_request(&tester, &options, &tester_meta_request));

    struct aws_s3_meta_request *meta_request = aws_s3_client_make_meta_request(client, &options);

    ASSERT_TRUE(meta_request != NULL);

    /* Wait for the request to finish. */
    aws_s3_tester_wait_for_finish(&tester);

    aws_s3_tester_lock_synced_data(&tester);
    ASSERT_TRUE(tester.synced_data.finish_error_code != AWS_ERROR_SUCCESS);
    aws_s3_tester_unlock_synced_data(&tester);

    ASSERT_TRUE(tester_meta_request.finished_response_status == 404);
    ASSERT_TRUE(tester_meta_request.finished_error_code != AWS_ERROR_SUCCESS);

    ASSERT_TRUE(tester_meta_request.error_response_headers != NULL);
    ASSERT_TRUE(tester_meta_request.error_response_body.len > 0);

    aws_s3_meta_request_release(meta_request);
    aws_s3_tester_meta_request_clean_up(&tester_meta_request);

    aws_http_message_release(message);
    message = NULL;

    aws_string_destroy(host_name);
    host_name = NULL;

    aws_s3_client_release(client);
    client = NULL;

    aws_s3_tester_clean_up(&tester);

    return 0;
}

static int s_validate_successful_get_entire_object(struct aws_s3_tester_meta_request *tester_meta_request) {
    AWS_PRECONDITION(tester_meta_request);
    AWS_PRECONDITION(tester_meta_request->tester);

    ASSERT_TRUE(tester_meta_request->finished_response_status == 200);
    ASSERT_TRUE(tester_meta_request->finished_response_status == tester_meta_request->headers_response_status);
    ASSERT_TRUE(tester_meta_request->finished_error_code == AWS_ERROR_SUCCESS);

    ASSERT_TRUE(tester_meta_request->error_response_headers == NULL);
    ASSERT_TRUE(tester_meta_request->error_response_body.len == 0);

    ASSERT_FALSE(
        aws_http_headers_has(tester_meta_request->response_headers, aws_byte_cursor_from_c_str("accept-ranges")));
    ASSERT_FALSE(
        aws_http_headers_has(tester_meta_request->response_headers, aws_byte_cursor_from_c_str("Content-Range")));

    struct aws_s3_tester *tester = tester_meta_request->tester;

    struct aws_byte_cursor content_length_cursor;
    AWS_ZERO_STRUCT(content_length_cursor);
    ASSERT_SUCCESS(aws_http_headers_get(
        tester_meta_request->response_headers, aws_byte_cursor_from_c_str("Content-Length"), &content_length_cursor));

    struct aws_string *content_length_str = aws_string_new_from_cursor(tester->allocator, &content_length_cursor);

    char *content_length_str_end = NULL;
    uint64_t content_length = strtoull((const char *)content_length_str->bytes, &content_length_str_end, 10);

    aws_string_destroy(content_length_str);

    AWS_LOGF_TRACE(
        AWS_LS_S3_GENERAL,
        "Content length in header is %" PRIu64 " and received body size is %" PRIu64,
        content_length,
        tester_meta_request->received_body_size);

    ASSERT_TRUE(content_length == tester_meta_request->received_body_size);

    return AWS_OP_SUCCESS;
}
