/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/byte_buf.h>
#include <aws/common/common.h>

#include <aws/testing/aws_test_harness.h>

#include <aws/http/request_response.h>
#include <aws/io/stream.h>

#include <inttypes.h>

#include "s3_tester.h"

AWS_STATIC_STRING_FROM_LITERAL(s_test_body_stream_str, "This is an S3 test.  This is an S3 test.");
static const struct aws_byte_cursor s_test_body_content_type = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("text/plain");

static const struct aws_byte_cursor s_test_s3_region = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("us-west-2");
static const struct aws_byte_cursor s_test_bucket_name =
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("aws-crt-canary-bucket-rc");

static struct aws_input_stream *s_create_test_body_stream(struct aws_allocator *allocator) {
    struct aws_byte_cursor test_body_cursor = aws_byte_cursor_from_string(s_test_body_stream_str);
    return aws_input_stream_new_from_cursor(allocator, &test_body_cursor);
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

static int s_test_s3_get_object_body_callback(
    struct aws_s3_accel_context *context,
    struct aws_http_stream *stream,
    const struct aws_byte_cursor *body,
    void *user_data) {
    (void)context;
    (void)stream;
    (void)user_data;

    AWS_LOGF_INFO(AWS_LS_S3_GENERAL, "Body of response: %s", (const char *)body->ptr);

    return AWS_OP_SUCCESS;
}

static void s_test_s3_get_object_finish(const struct aws_s3_accel_context *context, int error_code, void *user_data) {
    (void)context;

    struct aws_s3_tester *tester = (struct aws_s3_tester *)user_data;

    aws_s3_tester_notify_finished(tester, error_code);
}

AWS_TEST_CASE(test_s3_get_object, s_test_s3_get_object)
static int s_test_s3_get_object(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    const struct aws_byte_cursor test_object_path = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("/test_object.txt");

    aws_s3_library_init(allocator);

    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester, s_test_bucket_name, s_test_s3_region));

    struct aws_s3_client_config client_config = {
        .el_group = &tester.el_group,
        .host_resolver = &tester.host_resolver,
        .region = s_test_s3_region,
        .bucket_name = s_test_bucket_name,
        .endpoint = aws_byte_cursor_from_array(tester.endpoint->bytes, tester.endpoint->len)};

    aws_s3_tester_bind_client_shutdown(&tester, &client_config);

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    /* Put together a simple S3 Get Object request. */
    struct aws_http_message *message =
        s_make_get_object_request(allocator, aws_byte_cursor_from_string(tester.endpoint), test_object_path);

    struct aws_s3_accel_request_options options;
    AWS_ZERO_STRUCT(options);
    options.message = message;
    options.user_data = &tester;
    options.body_callback = s_test_s3_get_object_body_callback;
    options.finish_callback = s_test_s3_get_object_finish;

    /* Trigger accelerating of our Get Object request. */
    struct aws_s3_accel_context *context = aws_s3_client_accel_request(client, &options);

    ASSERT_TRUE(context != NULL);

    /* Wait for the request to finish. */
    aws_s3_tester_wait_for_finish(&tester);
    ASSERT_TRUE(tester.finish_error_code == AWS_ERROR_SUCCESS);

    aws_s3_accel_context_release(context);

    if (message != NULL) {
        aws_http_message_release(message);
        message = NULL;
    }

    if (client != NULL) {
        aws_s3_client_release(client);
        client = NULL;
    }

    /* Wait for systems to clean up. */
    aws_s3_tester_wait_for_clean_up(&tester);

    aws_s3_tester_clean_up(&tester);

    aws_s3_library_clean_up();

    return 0;
}

static void s_test_s3_put_object_finish(const struct aws_s3_accel_context *context, int error_code, void *user_data) {
    (void)context;
    struct aws_s3_tester *tester = (struct aws_s3_tester *)user_data;
    aws_s3_tester_notify_finished(tester, error_code);
}

AWS_TEST_CASE(test_s3_put_object, s_test_s3_put_object)
static int s_test_s3_put_object(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    const struct aws_byte_cursor test_object_path = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("/test_put_object.txt");

    aws_s3_library_init(allocator);

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester, s_test_bucket_name, s_test_s3_region));

    struct aws_s3_client_config client_config = {
        .el_group = &tester.el_group,
        .host_resolver = &tester.host_resolver,
        .region = s_test_s3_region,
        .bucket_name = s_test_bucket_name,
        .endpoint = aws_byte_cursor_from_array(tester.endpoint->bytes, tester.endpoint->len)};

    aws_s3_tester_bind_client_shutdown(&tester, &client_config);

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    struct aws_input_stream *input_stream = s_create_test_body_stream(allocator);

    /* Put together a simple S3 Put Object request. */
    struct aws_http_message *message = s_make_put_object_request(
        allocator,
        aws_byte_cursor_from_string(tester.endpoint),
        test_object_path,
        s_test_body_content_type,
        input_stream);

    struct aws_s3_accel_request_options options;
    AWS_ZERO_STRUCT(options);
    options.message = message;
    options.user_data = &tester;
    options.finish_callback = s_test_s3_put_object_finish;

    /* Wait for the request to finish. */
    struct aws_s3_accel_context *context = aws_s3_client_accel_request(client, &options);

    ASSERT_TRUE(context != NULL);

    /* Wait for the request to finish. */
    aws_s3_tester_wait_for_finish(&tester);
    ASSERT_TRUE(tester.finish_error_code == AWS_ERROR_SUCCESS);

    aws_s3_accel_context_release(context);

    if (message != NULL) {
        aws_http_message_release(message);
        message = NULL;
    }

    if (client != NULL) {
        aws_s3_client_release(client);
        client = NULL;
    }

    if (input_stream != NULL) {
        aws_input_stream_destroy(input_stream);
        input_stream = NULL;
    }

    /* Wait for systems to clean up. */
    aws_s3_tester_wait_for_clean_up(&tester);

    aws_s3_tester_clean_up(&tester);

    aws_s3_library_clean_up();

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

    struct aws_http_header host_header = {.name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("host"), .value = host};

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

    struct aws_http_header host_header = {.name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("host"), .value = host};

    struct aws_http_header content_type_header = {.name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("content-type"),
                                                  .value = content_type};

    char content_length_buffer[64] = "";
    sprintf(content_length_buffer, "%" PRId64 "", body_stream_length);

    struct aws_http_header content_length_header = {.name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("content-length"),
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
