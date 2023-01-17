/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_auto_ranged_get.h"
#include "aws/s3/private/s3_auto_ranged_put.h"
#include "aws/s3/private/s3_client_impl.h"
#include "aws/s3/private/s3_util.h"
#include "aws/s3/s3_client.h"
#include "s3_tester.h"

#include <aws/io/stream.h>
#include <aws/testing/aws_test_harness.h>
#include <inttypes.h>

#define TEST_CASE(NAME)                                                                                                \
    AWS_TEST_CASE(NAME, s_test_##NAME);                                                                                \
    static int s_test_##NAME(struct aws_allocator *allocator, void *ctx)

#define DEFINE_HEADER(NAME, VALUE)                                                                                     \
    { .name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL(NAME), .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL(VALUE), }

TEST_CASE(meta_request_auto_ranged_get_new_error_handling) {
    (void)ctx;

    struct aws_http_message *message = aws_http_message_new_request(allocator);
    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_client *client = NULL;
    struct aws_s3_tester_client_options client_options = {
        .part_size = 5 * 1024 * 1024,
    };
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_s3_meta_request_options options = {
        .message = message,
        .type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
    };
    struct aws_s3_meta_request *meta_request =
        aws_s3_meta_request_auto_ranged_get_new(allocator, client, SIZE_MAX, &options);

    ASSERT_NULL(meta_request);
    aws_http_message_release(message);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

TEST_CASE(meta_request_auto_ranged_put_new_error_handling) {
    (void)ctx;

    struct aws_http_message *message = aws_http_message_new_request(allocator);
    struct aws_byte_cursor body = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("write more tests");
    struct aws_input_stream *body_stream = aws_input_stream_new_from_cursor(allocator, &body);
    aws_http_message_set_body_stream(message, body_stream);

    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_client *client = NULL;
    struct aws_s3_tester_client_options client_options = {
        .part_size = 5 * 1024 * 1024,
    };
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    /* First: Fail from the aws_s3_meta_request_init_base */
    struct aws_s3_meta_request_options options = {
        .message = message,
        .type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
    };
    struct aws_s3_meta_request *meta_request =
        aws_s3_meta_request_auto_ranged_put_new(allocator, client, SIZE_MAX, MB_TO_BYTES(10), 2, &options);

    ASSERT_NULL(meta_request);

    /* Second: Fail from the s_try_update_part_info_from_resume_token */
    struct aws_s3_meta_request_resume_token *token = aws_s3_meta_request_resume_token_new(allocator);
    token->part_size = 1; /* Less than g_s3_min_upload_part_size */
    options.resume_token = token;
    meta_request =
        aws_s3_meta_request_auto_ranged_put_new(allocator, client, MB_TO_BYTES(8), MB_TO_BYTES(10), 2, &options);
    ASSERT_NULL(meta_request);

    /* Third: Fail from the s_try_init_resume_state_from_persisted_data */
    meta_request =
        aws_s3_meta_request_auto_ranged_put_new(allocator, client, MB_TO_BYTES(8), MB_TO_BYTES(10), 2, &options);

    ASSERT_NULL(meta_request);

    aws_input_stream_release(body_stream);
    aws_http_message_release(message);
    aws_s3_meta_request_resume_token_release(token);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

TEST_CASE(bad_request_error_handling) {
    /* The original request without method and path. */
    (void)ctx;
    struct aws_http_message *message = aws_http_message_new_request(allocator);
    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_client *client = NULL;
    struct aws_s3_tester_client_options client_options = {
        .part_size = 5 * 1024 * 1024,
    };
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_http_header host_header = {
        .name = g_host_header_name,
        .value = aws_byte_cursor_from_c_str("s3.us-east-1.amazonaws.com"),
    };
    ASSERT_SUCCESS(aws_http_message_add_header(message, host_header));

    struct aws_s3_meta_request_options options;
    AWS_ZERO_STRUCT(options);
    options.type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT;
    options.message = message;

    struct aws_s3_meta_request_test_results meta_request_test_results;
    aws_s3_meta_request_test_results_init(&meta_request_test_results, allocator);

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request(
        &tester, client, &options, &meta_request_test_results, 0 /* Not expect success */));

    ASSERT_UINT_EQUALS(meta_request_test_results.finished_error_code, AWS_ERROR_HTTP_DATA_NOT_AVAILABLE);

    aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);

    aws_http_message_release(message);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return 0;
}
