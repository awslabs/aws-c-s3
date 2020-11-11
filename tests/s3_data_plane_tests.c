/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_client_impl.h"
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

#ifdef _MSC_VER
/* sscanf warning (not currently scanning for strings) */
#    pragma warning(disable : 4996)
#endif

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

static struct aws_http_message *s_make_get_object_request(
    struct aws_allocator *allocator,
    struct aws_byte_cursor host,
    struct aws_byte_cursor key);

static struct aws_http_message *s_make_put_object_request(
    struct aws_allocator *allocator,
    struct aws_byte_cursor host,
    struct aws_byte_cursor content_type,
    struct aws_byte_cursor key,
    struct aws_input_stream *body_stream);

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
        s_make_get_object_request(allocator, aws_byte_cursor_from_string(host_name), test_object_path);

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
        s_make_get_object_request(allocator, aws_byte_cursor_from_string(host_name), test_object_path);

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
    struct aws_http_message *message = s_make_put_object_request(
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
        s_make_get_object_request(allocator, aws_byte_cursor_from_string(host_name), test_object_path);

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

static struct aws_http_message *s_make_get_object_request(
    struct aws_allocator *allocator,
    struct aws_byte_cursor host,
    struct aws_byte_cursor key) {

    struct aws_http_message *message = aws_http_message_new_request(allocator);

    if (message == NULL) {
        return NULL;
    }

    struct aws_http_header host_header = {.name = g_host_header_name, .value = host};

    if (aws_http_message_add_header(message, host_header)) {
        goto error_clean_up_message;
    }

    if (aws_http_message_set_request_method(message, aws_http_method_get)) {
        goto error_clean_up_message;
    }

    if (aws_http_message_set_request_path(message, key)) {
        goto error_clean_up_message;
    }

    return message;

error_clean_up_message:

    if (message != NULL) {
        aws_http_message_release(message);
        message = NULL;
    }

    return NULL;
}

static struct aws_http_message *s_make_put_object_request(
    struct aws_allocator *allocator,
    struct aws_byte_cursor host,
    struct aws_byte_cursor key,
    struct aws_byte_cursor content_type,
    struct aws_input_stream *body_stream) {

    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(body_stream);

    int64_t body_stream_length = 0;

    if (aws_input_stream_get_length(body_stream, &body_stream_length)) {
        return NULL;
    }

    struct aws_http_message *message = aws_http_message_new_request(allocator);

    if (message == NULL) {
        return NULL;
    }

    struct aws_http_header host_header = {.name = g_host_header_name, .value = host};

    struct aws_http_header content_type_header = {.name = g_content_type_header_name, .value = content_type};

    char content_length_buffer[64] = "";
    snprintf(content_length_buffer, sizeof(content_length_buffer), "%" PRId64 "", body_stream_length);

    struct aws_http_header content_length_header = {.name = g_content_length_header_name,
                                                    .value = aws_byte_cursor_from_c_str(content_length_buffer)};

    if (aws_http_message_add_header(message, host_header)) {
        goto error_clean_up_message;
    }

    if (aws_http_message_add_header(message, content_type_header)) {
        goto error_clean_up_message;
    }

    if (aws_http_message_add_header(message, content_length_header)) {
        goto error_clean_up_message;
    }

    if (aws_http_message_set_request_method(message, aws_http_method_put)) {
        goto error_clean_up_message;
    }

    if (aws_http_message_set_request_path(message, key)) {
        goto error_clean_up_message;
    }

    aws_http_message_set_body_stream(message, body_stream);

    return message;

error_clean_up_message:

    if (message != NULL) {
        aws_http_message_release(message);
        message = NULL;
    }

    return NULL;
}

static int s_validate_successful_get_entire_object(struct aws_s3_tester_meta_request *tester_meta_request) {
    AWS_PRECONDITION(tester_meta_request);

    ASSERT_TRUE(tester_meta_request->finished_response_status == 200);
    ASSERT_TRUE(tester_meta_request->finished_response_status == tester_meta_request->headers_response_status);
    ASSERT_TRUE(tester_meta_request->finished_error_code == AWS_ERROR_SUCCESS);

    ASSERT_TRUE(tester_meta_request->error_response_headers == NULL);
    ASSERT_TRUE(tester_meta_request->error_response_body.len == 0);

    ASSERT_FALSE(
        aws_http_headers_has(tester_meta_request->response_headers, aws_byte_cursor_from_c_str("accept-ranges")));
    ASSERT_FALSE(
        aws_http_headers_has(tester_meta_request->response_headers, aws_byte_cursor_from_c_str("Content-Range")));

    struct aws_byte_cursor content_length_cursor;
    AWS_ZERO_STRUCT(content_length_cursor);
    ASSERT_SUCCESS(aws_http_headers_get(
        tester_meta_request->response_headers, aws_byte_cursor_from_c_str("Content-Length"), &content_length_cursor));

    uint64_t content_length = 0;
    sscanf((const char *)content_length_cursor.ptr, "%" PRIu64, &content_length);
    AWS_LOGF_TRACE(
        AWS_LS_S3_GENERAL,
        "Content length in header is %" PRIu64 " and received body size is %" PRIu64,
        content_length,
        tester_meta_request->received_body_size);
    ASSERT_TRUE(content_length == tester_meta_request->received_body_size);

    return AWS_OP_SUCCESS;
}
