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
#include <aws/common/environment.h>
#include <aws/common/ref_count.h>
#include <aws/http/request_response.h>
#include <aws/io/channel_bootstrap.h>
#include <aws/io/host_resolver.h>
#include <aws/io/stream.h>
#include <aws/io/tls_channel_handler.h>
#include <aws/testing/aws_test_harness.h>
#include <inttypes.h>

AWS_TEST_CASE(test_s3_client_create_destroy, s_test_s3_client_create_destroy)
static int s_test_s3_client_create_destroy(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client_config client_config;
    AWS_ZERO_STRUCT(client_config);

    ASSERT_SUCCESS(aws_s3_tester_bind_client(
        &tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION | AWS_S3_TESTER_BIND_CLIENT_SIGNING));

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

    const int request_tag = 1234;
    const uint32_t part_number = 5678;

    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_meta_request *meta_request = aws_s3_tester_mock_meta_request_new(&tester);
    ASSERT_TRUE(meta_request != NULL);

    struct aws_http_message *request_message = aws_s3_tester_dummy_http_request_new(&tester);
    ASSERT_TRUE(request_message != NULL);

    struct aws_s3_request *request =
        aws_s3_request_new(meta_request, request_tag, part_number, AWS_S3_REQUEST_DESC_RECORD_RESPONSE_HEADERS);

    ASSERT_TRUE(request != NULL);

    ASSERT_TRUE(request->meta_request == meta_request);
    ASSERT_TRUE(request->part_number == part_number);
    ASSERT_TRUE(request->request_tag == request_tag);
    ASSERT_TRUE(request->record_response_headers == true);

    aws_s3_request_setup_send_data(request, request_message);

    ASSERT_TRUE(request->send_data.message != NULL);
    ASSERT_TRUE(request->send_data.response_headers == NULL);

    request->send_data.response_headers = aws_http_headers_new(allocator);
    ASSERT_TRUE(request->send_data.response_headers != NULL);

    aws_s3_request_clean_up_send_data(request);

    ASSERT_TRUE(request->send_data.message == NULL);
    ASSERT_TRUE(request->send_data.response_headers == NULL);
    ASSERT_TRUE(request->send_data.response_status == 0);

    aws_s3_request_release(request);
    aws_http_message_release(request_message);
    aws_s3_meta_request_release(meta_request);

    aws_s3_tester_clean_up(&tester);

    return 0;
}

static int s_test_s3_get_object_helper(
    struct aws_allocator *allocator,
    enum aws_s3_client_tls_usage tls_usage,
    uint32_t extra_meta_request_flag,
    struct aws_byte_cursor s3_path) {
    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client_config client_config = {
        .part_size = 64 * 1024,
    };

    struct aws_tls_ctx_options tls_context_options;
    aws_tls_ctx_options_init_default_client(&tls_context_options, allocator);
    struct aws_tls_ctx *context = aws_tls_client_ctx_new(allocator, &tls_context_options);

    struct aws_tls_connection_options tls_connection_options;
    aws_tls_connection_options_init_from_ctx(&tls_connection_options, context);

    struct aws_string *endpoint =
        aws_s3_tester_build_endpoint_string(allocator, &g_test_bucket_name, &g_test_s3_region);
    struct aws_byte_cursor endpoint_cursor = aws_byte_cursor_from_string(endpoint);

    tls_connection_options.server_name = aws_string_new_from_cursor(allocator, &endpoint_cursor);

    switch (tls_usage) {
        case AWS_S3_TLS_ENABLED:
            client_config.tls_mode = AWS_MR_TLS_ENABLED;
            client_config.tls_connection_options = &tls_connection_options;
            break;
        case AWS_S3_TLS_DISABLED:
            client_config.tls_mode = AWS_MR_TLS_DISABLED;
            break;
        default:
            break;
    }

    ASSERT_SUCCESS(aws_s3_tester_bind_client(
        &tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION | AWS_S3_TESTER_BIND_CLIENT_SIGNING));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    uint32_t flags = AWS_S3_TESTER_SEND_META_REQUEST_EXPECT_SUCCESS | extra_meta_request_flag;
    ASSERT_SUCCESS(aws_s3_tester_send_get_object_meta_request(&tester, client, s3_path, flags, NULL));

    aws_string_destroy(endpoint);
    aws_tls_ctx_release(context);
    aws_tls_connection_options_clean_up(&tls_connection_options);
    aws_tls_ctx_options_clean_up(&tls_context_options);

    aws_s3_client_release(client);
    client = NULL;

    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(test_s3_get_object_tls_disabled, s_test_s3_get_object_tls_disabled)
static int s_test_s3_get_object_tls_disabled(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(s_test_s3_get_object_helper(allocator, AWS_S3_TLS_DISABLED, 0, g_s3_path_get_object_test_1MB));

    return 0;
}

AWS_TEST_CASE(test_s3_get_object_tls_enabled, s_test_s3_get_object_tls_enabled)
static int s_test_s3_get_object_tls_enabled(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(s_test_s3_get_object_helper(allocator, AWS_S3_TLS_ENABLED, 0, g_s3_path_get_object_test_1MB));

    return 0;
}

AWS_TEST_CASE(test_s3_get_object_tls_default, s_test_s3_get_object_tls_default)
static int s_test_s3_get_object_tls_default(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(s_test_s3_get_object_helper(allocator, AWS_S3_TLS_ENABLED, 0, g_s3_path_get_object_test_1MB));

    return 0;
}

AWS_TEST_CASE(test_s3_no_signing, s_test_s3_no_signing)
static int s_test_s3_no_signing(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client_config client_config;
    AWS_ZERO_STRUCT(client_config);

    ASSERT_SUCCESS(aws_s3_tester_bind_client(&tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    struct aws_string *host_name =
        aws_s3_tester_build_endpoint_string(allocator, &g_test_public_bucket_name, &g_test_s3_region);

    /* Put together a simple S3 Get Object request. */
    struct aws_http_message *message = aws_s3_test_get_object_request_new(
        allocator, aws_byte_cursor_from_string(host_name), g_s3_path_get_object_test_1MB);

    struct aws_s3_meta_request_options options;
    AWS_ZERO_STRUCT(options);
    options.type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT;
    options.message = message;

    /* Trigger accelerating of our Get Object request. */
    struct aws_s3_meta_request_test_results meta_request_test_results;
    AWS_ZERO_STRUCT(meta_request_test_results);

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request(
        &tester, client, &options, &meta_request_test_results, AWS_S3_TESTER_SEND_META_REQUEST_EXPECT_SUCCESS));
    ASSERT_SUCCESS(aws_s3_tester_validate_get_object_results(&meta_request_test_results, 0));

    aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);

    aws_http_message_release(message);
    aws_string_destroy(host_name);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(test_s3_signing_override, s_test_s3_signing_override)
static int s_test_s3_signing_override(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client_config client_config;
    AWS_ZERO_STRUCT(client_config);

    ASSERT_SUCCESS(aws_s3_tester_bind_client(&tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    struct aws_string *host_name =
        aws_s3_tester_build_endpoint_string(allocator, &g_test_bucket_name, &g_test_s3_region);

    /* Put together a simple S3 Get Object request. */
    struct aws_http_message *message = aws_s3_test_get_object_request_new(
        allocator, aws_byte_cursor_from_string(host_name), g_s3_path_get_object_test_1MB);

    /* Getting without signing should fail since the client has no signing set up. */
    {
        struct aws_s3_meta_request_options options;
        AWS_ZERO_STRUCT(options);
        options.type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT;
        options.message = message;

        /* Trigger accelerating of our Get Object request.*/
        struct aws_s3_meta_request_test_results meta_request_test_results;
        AWS_ZERO_STRUCT(meta_request_test_results);

        ASSERT_SUCCESS(aws_s3_tester_send_meta_request(&tester, client, &options, &meta_request_test_results, 0));
        ASSERT_TRUE(aws_s3_tester_validate_get_object_results(&meta_request_test_results, 0) != AWS_OP_SUCCESS);

        aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);
    }

    /* Getting with signing should succeed if we set up signing on the meta request. */
    {
        struct aws_s3_meta_request_options options;
        AWS_ZERO_STRUCT(options);
        options.type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT;
        options.message = message;
        options.signing_config = &tester.default_signing_config;

        /* Trigger accelerating of our Get Object request. */
        struct aws_s3_meta_request_test_results meta_request_test_results;
        AWS_ZERO_STRUCT(meta_request_test_results);

        ASSERT_SUCCESS(aws_s3_tester_send_meta_request(
            &tester, client, &options, &meta_request_test_results, AWS_S3_TESTER_SEND_META_REQUEST_EXPECT_SUCCESS));
        ASSERT_SUCCESS(aws_s3_tester_validate_get_object_results(&meta_request_test_results, 0));

        aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);
    }

    aws_http_message_release(message);
    aws_string_destroy(host_name);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(test_s3_get_object_less_than_part_size, s_test_s3_get_object_less_than_part_size)
static int s_test_s3_get_object_less_than_part_size(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client_config client_config = {
        .part_size = 20 * 1024 * 1024,
    };

    ASSERT_SUCCESS(aws_s3_tester_bind_client(
        &tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION | AWS_S3_TESTER_BIND_CLIENT_SIGNING));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    ASSERT_SUCCESS(aws_s3_tester_send_get_object_meta_request(
        &tester, client, g_s3_path_get_object_test_1MB, AWS_S3_TESTER_SEND_META_REQUEST_EXPECT_SUCCESS, NULL));

    aws_s3_client_release(client);
    client = NULL;

    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(test_s3_put_object_with_part_remainder, s_test_s3_put_object_with_part_remainder)
static int s_test_s3_put_object_with_part_remainder(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_tester_client_options client_options = {
        .part_size = 5 * 1024 * 1024,
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_s3_meta_request_test_results meta_request_test_results;
    AWS_ZERO_STRUCT(meta_request_test_results);

    struct aws_s3_tester_meta_request_options options = {
        .allocator = allocator,
        .client = client,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_SUCCESS,
        .put_options =
            {
                /* Object size meant to be one megabyte larger than the part size of the client. */
                .object_size_mb = 6,
            },
    };

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &options, &meta_request_test_results));
    aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);

    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(test_s3_get_object_multiple, s_test_s3_get_object_multiple)
static int s_test_s3_get_object_multiple(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    const struct aws_byte_cursor test_object_path = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("/get_object_test_1MB.txt");

    struct aws_s3_meta_request *meta_requests[4];
    struct aws_s3_meta_request_test_results meta_request_test_results[4];
    size_t num_meta_requests = sizeof(meta_requests) / sizeof(struct aws_s3_meta_request *);

    ASSERT_TRUE(
        num_meta_requests == (sizeof(meta_request_test_results) / sizeof(struct aws_s3_meta_request_test_results)));

    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client_config client_config = {
        .part_size = 64 * 1024,
    };

    ASSERT_SUCCESS(aws_s3_tester_bind_client(
        &tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION | AWS_S3_TESTER_BIND_CLIENT_SIGNING));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    struct aws_string *host_name =
        aws_s3_tester_build_endpoint_string(allocator, &g_test_bucket_name, &g_test_s3_region);

    /* Put together a simple S3 Get Object request. */
    struct aws_http_message *message =
        aws_s3_test_get_object_request_new(allocator, aws_byte_cursor_from_string(host_name), test_object_path);

    for (size_t i = 0; i < num_meta_requests; ++i) {
        AWS_ZERO_STRUCT(meta_request_test_results[i]);

        struct aws_s3_meta_request_options options;
        AWS_ZERO_STRUCT(options);
        options.type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT;
        options.message = message;

        ASSERT_SUCCESS(aws_s3_tester_bind_meta_request(&tester, &options, &meta_request_test_results[i]));

        /* Trigger accelerating of our Get Object request. */
        meta_requests[i] = aws_s3_client_make_meta_request(client, &options);

        ASSERT_TRUE(meta_requests[i] != NULL);
    }

    /* Wait for the request to finish. */
    aws_s3_tester_wait_for_meta_request_finish(&tester);

    aws_s3_tester_lock_synced_data(&tester);
    ASSERT_TRUE(tester.synced_data.finish_error_code == AWS_ERROR_SUCCESS);
    aws_s3_tester_unlock_synced_data(&tester);

    for (size_t i = 0; i < num_meta_requests; ++i) {
        aws_s3_meta_request_release(meta_requests[i]);
        meta_requests[i] = NULL;
    }

    aws_s3_tester_wait_for_meta_request_shutdown(&tester);

    for (size_t i = 0; i < num_meta_requests; ++i) {
        aws_s3_tester_validate_get_object_results(&meta_request_test_results[i], 0);
        aws_s3_meta_request_test_results_clean_up(&meta_request_test_results[i]);
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

AWS_TEST_CASE(test_s3_get_object_empty_object, s_test_s3_get_object_empty_default)
static int s_test_s3_get_object_empty_default(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    return (s_test_s3_get_object_helper(
        allocator, AWS_S3_TLS_ENABLED, 0, aws_byte_cursor_from_c_str("/get_object_test_0MB.txt")));
}

AWS_TEST_CASE(test_s3_get_object_sse_kms, s_test_s3_get_object_sse_kms)
static int s_test_s3_get_object_sse_kms(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    /* Keep TLS enabled for SSE related download, or it will fail. */
    return s_test_s3_get_object_helper(
        allocator,
        AWS_S3_TLS_ENABLED,
        AWS_S3_TESTER_SEND_META_REQUEST_SSE_KMS,
        aws_byte_cursor_from_c_str("/get_object_test_kms_10MB.txt"));
}

AWS_TEST_CASE(test_s3_get_object_sse_aes256, s_test_s3_get_object_sse_aes256)
static int s_test_s3_get_object_sse_aes256(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    /* Keep TLS enabled for SSE related download, or it will fail. */
    return s_test_s3_get_object_helper(
        allocator,
        AWS_S3_TLS_ENABLED,
        AWS_S3_TESTER_SEND_META_REQUEST_SSE_AES256,
        aws_byte_cursor_from_c_str("/get_object_test_aes256_10MB.txt"));
}

static int s_test_s3_put_object_helper(
    struct aws_allocator *allocator,
    enum aws_s3_client_tls_usage tls_usage,
    uint32_t extra_meta_request_flag) {
    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_tls_ctx_options tls_context_options;
    aws_tls_ctx_options_init_default_client(&tls_context_options, allocator);
    struct aws_tls_ctx *context = aws_tls_client_ctx_new(allocator, &tls_context_options);

    struct aws_tls_connection_options tls_connection_options;
    aws_tls_connection_options_init_from_ctx(&tls_connection_options, context);

    struct aws_string *endpoint =
        aws_s3_tester_build_endpoint_string(allocator, &g_test_bucket_name, &g_test_s3_region);
    struct aws_byte_cursor endpoint_cursor = aws_byte_cursor_from_string(endpoint);

    tls_connection_options.server_name = aws_string_new_from_cursor(allocator, &endpoint_cursor);

    struct aws_s3_client_config client_config = {
        .part_size = 5 * 1024 * 1024,
    };

    switch (tls_usage) {
        case AWS_S3_TLS_ENABLED:
            client_config.tls_mode = AWS_MR_TLS_ENABLED;
            client_config.tls_connection_options = &tls_connection_options;
            break;
        case AWS_S3_TLS_DISABLED:
            client_config.tls_mode = AWS_MR_TLS_DISABLED;
            break;
        default:
            break;
    }

    ASSERT_SUCCESS(aws_s3_tester_bind_client(
        &tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION | AWS_S3_TESTER_BIND_CLIENT_SIGNING));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    ASSERT_SUCCESS(aws_s3_tester_send_put_object_meta_request(
        &tester, client, 10, AWS_S3_TESTER_SEND_META_REQUEST_EXPECT_SUCCESS | extra_meta_request_flag, NULL));

    aws_string_destroy(endpoint);
    aws_tls_ctx_release(context);
    aws_tls_connection_options_clean_up(&tls_connection_options);
    aws_tls_ctx_options_clean_up(&tls_context_options);

    aws_s3_client_release(client);
    client = NULL;

    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(test_s3_put_object_tls_disabled, s_test_s3_put_object_tls_disabled)
static int s_test_s3_put_object_tls_disabled(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(s_test_s3_put_object_helper(allocator, AWS_S3_TLS_DISABLED, 0));

    return 0;
}

AWS_TEST_CASE(test_s3_put_object_tls_enabled, s_test_s3_put_object_tls_enabled)
static int s_test_s3_put_object_tls_enabled(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_meta_request_test_results meta_request_test_results;
    AWS_ZERO_STRUCT(meta_request_test_results);

    struct aws_s3_tester_meta_request_options options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_SUCCESS,
        .put_options =
            {
                .ensure_multipart = true,
            },
    };

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(NULL, &options, NULL));

    return 0;
}

AWS_TEST_CASE(test_s3_put_object_tls_default, s_test_s3_put_object_tls_default)
static int s_test_s3_put_object_tls_default(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(s_test_s3_put_object_helper(allocator, AWS_S3_TLS_DEFAULT, 0));

    return 0;
}

AWS_TEST_CASE(test_s3_multipart_put_object_with_acl, s_test_s3_multipart_put_object_with_acl)
static int s_test_s3_multipart_put_object_with_acl(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(s_test_s3_put_object_helper(allocator, AWS_S3_TLS_DEFAULT, AWS_S3_TESTER_SEND_META_REQUEST_PUT_ACL));

    return 0;
}

AWS_TEST_CASE(test_s3_put_object_multiple, s_test_s3_put_object_multiple)
static int s_test_s3_put_object_multiple(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_meta_request *meta_requests[2];
    struct aws_s3_meta_request_test_results meta_request_test_results[2];
    struct aws_http_message *messages[2];
    struct aws_input_stream *input_streams[2];
    struct aws_byte_buf input_stream_buffers[2];
    size_t num_meta_requests = sizeof(meta_requests) / sizeof(struct aws_s3_meta_request *);

    ASSERT_TRUE(
        num_meta_requests == (sizeof(meta_request_test_results) / sizeof(struct aws_s3_meta_request_test_results)));

    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client_config client_config;
    AWS_ZERO_STRUCT(client_config);

    ASSERT_SUCCESS(aws_s3_tester_bind_client(
        &tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION | AWS_S3_TESTER_BIND_CLIENT_SIGNING));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    struct aws_string *host_name =
        aws_s3_tester_build_endpoint_string(allocator, &g_test_bucket_name, &g_test_s3_region);

    for (size_t i = 0; i < num_meta_requests; ++i) {
        AWS_ZERO_STRUCT(meta_request_test_results[i]);
        char object_path_buffer[128] = "";
        snprintf(object_path_buffer, sizeof(object_path_buffer), "/get_object_test_10MB_%zu.txt", i);
        AWS_ZERO_STRUCT(input_stream_buffers[i]);
        aws_s3_create_test_buffer(allocator, 10 * 1024ULL * 1024ULL, &input_stream_buffers[i]);
        struct aws_byte_cursor test_body_cursor = aws_byte_cursor_from_buf(&input_stream_buffers[i]);
        input_streams[i] = aws_input_stream_new_from_cursor(allocator, &test_body_cursor);
        struct aws_byte_cursor test_object_path = aws_byte_cursor_from_c_str(object_path_buffer);
        messages[i] = aws_s3_test_put_object_request_new(
            allocator,
            aws_byte_cursor_from_string(host_name),
            test_object_path,
            g_test_body_content_type,
            input_streams[i],
            0);
        struct aws_s3_meta_request_options options;
        AWS_ZERO_STRUCT(options);
        options.type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT;
        options.message = messages[i];

        ASSERT_SUCCESS(aws_s3_tester_bind_meta_request(&tester, &options, &meta_request_test_results[i]));

        /* Trigger accelerating of our Put Object request. */
        meta_requests[i] = aws_s3_client_make_meta_request(client, &options);

        ASSERT_TRUE(meta_requests[i] != NULL);
    }

    /* Wait for the request to finish. */
    aws_s3_tester_wait_for_meta_request_finish(&tester);

    aws_s3_tester_lock_synced_data(&tester);
    ASSERT_TRUE(tester.synced_data.finish_error_code == AWS_ERROR_SUCCESS);
    aws_s3_tester_unlock_synced_data(&tester);

    for (size_t i = 0; i < num_meta_requests; ++i) {
        aws_s3_meta_request_release(meta_requests[i]);
        meta_requests[i] = NULL;
    }

    aws_s3_tester_wait_for_meta_request_shutdown(&tester);

    for (size_t i = 0; i < num_meta_requests; ++i) {
        aws_s3_tester_validate_get_object_results(&meta_request_test_results[i], 0);
        aws_s3_meta_request_test_results_clean_up(&meta_request_test_results[i]);
    }

    for (size_t i = 0; i < num_meta_requests; ++i) {
        aws_http_message_release(messages[i]);
        aws_input_stream_destroy(input_streams[i]);
        aws_byte_buf_clean_up(&input_stream_buffers[i]);
    }

    aws_string_destroy(host_name);
    host_name = NULL;

    aws_s3_client_release(client);
    client = NULL;

    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(test_s3_put_object_less_than_part_size, s_test_s3_put_object_less_than_part_size)
static int s_test_s3_put_object_less_than_part_size(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client_config client_config = {
        .part_size = 20 * 1024 * 1024,
    };

    ASSERT_SUCCESS(aws_s3_tester_bind_client(
        &tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION | AWS_S3_TESTER_BIND_CLIENT_SIGNING));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    ASSERT_TRUE(client != NULL);

    ASSERT_SUCCESS(aws_s3_tester_send_put_object_meta_request(
        &tester, client, 10, AWS_S3_TESTER_SEND_META_REQUEST_EXPECT_SUCCESS, NULL));

    aws_s3_client_release(client);
    client = NULL;

    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(test_s3_put_object_empty_object, s_test_s3_put_object_empty_object)
static int s_test_s3_put_object_empty_object(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client_config client_config;
    AWS_ZERO_STRUCT(client_config);

    ASSERT_SUCCESS(aws_s3_tester_bind_client(
        &tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION | AWS_S3_TESTER_BIND_CLIENT_SIGNING));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    ASSERT_TRUE(client != NULL);

    ASSERT_SUCCESS(aws_s3_tester_send_put_object_meta_request(
        &tester, client, 0, AWS_S3_TESTER_SEND_META_REQUEST_EXPECT_SUCCESS, NULL));

    aws_s3_client_release(client);
    client = NULL;

    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(test_s3_put_object_sse_kms, s_test_s3_put_object_sse_kms)
static int s_test_s3_put_object_sse_kms(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client_config client_config = {
        .part_size = 20 * 1024 * 1024,
    };

    ASSERT_SUCCESS(aws_s3_tester_bind_client(
        &tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION | AWS_S3_TESTER_BIND_CLIENT_SIGNING));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    ASSERT_TRUE(client != NULL);

    ASSERT_SUCCESS(aws_s3_tester_send_put_object_meta_request(
        &tester,
        client,
        10,
        AWS_S3_TESTER_SEND_META_REQUEST_EXPECT_SUCCESS | AWS_S3_TESTER_SEND_META_REQUEST_SSE_KMS,
        NULL));

    aws_s3_client_release(client);
    client = NULL;

    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(test_s3_put_object_sse_kms_multipart, s_test_s3_put_object_sse_kms_multipart)
static int s_test_s3_put_object_sse_kms_multipart(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client_config client_config = {
        .part_size = 5 * 1024 * 1024,
    };

    ASSERT_SUCCESS(aws_s3_tester_bind_client(
        &tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION | AWS_S3_TESTER_BIND_CLIENT_SIGNING));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    ASSERT_TRUE(client != NULL);

    ASSERT_SUCCESS(aws_s3_tester_send_put_object_meta_request(
        &tester,
        client,
        10,
        AWS_S3_TESTER_SEND_META_REQUEST_EXPECT_SUCCESS | AWS_S3_TESTER_SEND_META_REQUEST_SSE_KMS,
        NULL));

    aws_s3_client_release(client);
    client = NULL;

    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(test_s3_put_object_sse_aes256, s_test_s3_put_object_sse_aes256)
static int s_test_s3_put_object_sse_aes256(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client_config client_config = {
        .part_size = 20 * 1024 * 1024,
    };

    ASSERT_SUCCESS(aws_s3_tester_bind_client(
        &tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION | AWS_S3_TESTER_BIND_CLIENT_SIGNING));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    ASSERT_TRUE(client != NULL);

    ASSERT_SUCCESS(aws_s3_tester_send_put_object_meta_request(
        &tester,
        client,
        10,
        AWS_S3_TESTER_SEND_META_REQUEST_EXPECT_SUCCESS | AWS_S3_TESTER_SEND_META_REQUEST_SSE_AES256,
        NULL));

    aws_s3_client_release(client);
    client = NULL;

    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(test_s3_put_object_sse_aes256_multipart, s_test_s3_put_object_sse_aes256_multipart)
static int s_test_s3_put_object_sse_aes256_multipart(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client_config client_config = {
        .part_size = 5 * 1024 * 1024,
    };

    ASSERT_SUCCESS(aws_s3_tester_bind_client(
        &tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION | AWS_S3_TESTER_BIND_CLIENT_SIGNING));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    ASSERT_TRUE(client != NULL);

    ASSERT_SUCCESS(aws_s3_tester_send_put_object_meta_request(
        &tester,
        client,
        10,
        AWS_S3_TESTER_SEND_META_REQUEST_EXPECT_SUCCESS | AWS_S3_TESTER_SEND_META_REQUEST_SSE_AES256,
        NULL));

    aws_s3_client_release(client);
    client = NULL;

    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(test_s3_meta_request_default, s_test_s3_meta_request_default)
static int s_test_s3_meta_request_default(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client_config client_config;
    AWS_ZERO_STRUCT(client_config);

    ASSERT_SUCCESS(aws_s3_tester_bind_client(
        &tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION | AWS_S3_TESTER_BIND_CLIENT_SIGNING));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    const struct aws_byte_cursor test_object_path = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("/get_object_test_1MB.txt");

    struct aws_string *host_name =
        aws_s3_tester_build_endpoint_string(allocator, &g_test_bucket_name, &g_test_s3_region);

    /* Put together a simple S3 Get Object request. */
    struct aws_http_message *message =
        aws_s3_test_get_object_request_new(allocator, aws_byte_cursor_from_string(host_name), test_object_path);

    struct aws_s3_meta_request_options options;
    AWS_ZERO_STRUCT(options);

    /* Pass the request through as a default request so that it goes through as-is. */
    options.type = AWS_S3_META_REQUEST_TYPE_DEFAULT;
    options.message = message;

    struct aws_s3_meta_request_test_results meta_request_test_results;
    AWS_ZERO_STRUCT(meta_request_test_results);

    ASSERT_SUCCESS(aws_s3_tester_bind_meta_request(&tester, &options, &meta_request_test_results));

    struct aws_s3_meta_request *meta_request = aws_s3_client_make_meta_request(client, &options);

    ASSERT_TRUE(meta_request != NULL);

    /* Wait for the request to finish. */
    aws_s3_tester_wait_for_meta_request_finish(&tester);

    aws_s3_tester_lock_synced_data(&tester);

    ASSERT_TRUE(tester.synced_data.finish_error_code == AWS_ERROR_SUCCESS);

    aws_s3_tester_unlock_synced_data(&tester);

    ASSERT_SUCCESS(aws_s3_tester_validate_get_object_results(&meta_request_test_results, 0));

    aws_s3_meta_request_release(meta_request);
    meta_request = NULL;

    aws_s3_tester_wait_for_meta_request_shutdown(&tester);

    aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);

    aws_http_message_release(message);
    message = NULL;

    aws_string_destroy(host_name);
    host_name = NULL;

    aws_s3_client_release(client);
    client = NULL;

    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(test_s3_error_missing_file, s_test_s3_error_missing_file)
static int s_test_s3_error_missing_file(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    const struct aws_byte_cursor test_object_path =
        AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("/non-existing-file12345.txt");

    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client_config client_config = {
        .part_size = 64 * 1024,
    };

    ASSERT_SUCCESS(aws_s3_tester_bind_client(
        &tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION | AWS_S3_TESTER_BIND_CLIENT_SIGNING));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    struct aws_string *host_name =
        aws_s3_tester_build_endpoint_string(allocator, &g_test_bucket_name, &g_test_s3_region);

    /* Put together a simple S3 Get Object request. */
    struct aws_http_message *message =
        aws_s3_test_get_object_request_new(allocator, aws_byte_cursor_from_string(host_name), test_object_path);

    struct aws_s3_meta_request_options options;
    AWS_ZERO_STRUCT(options);
    options.type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT;
    options.message = message;

    /* Trigger accelerating of our Get Object request. */
    struct aws_s3_meta_request_test_results meta_request_test_results;
    AWS_ZERO_STRUCT(meta_request_test_results);

    ASSERT_SUCCESS(aws_s3_tester_bind_meta_request(&tester, &options, &meta_request_test_results));

    struct aws_s3_meta_request *meta_request = aws_s3_client_make_meta_request(client, &options);

    ASSERT_TRUE(meta_request != NULL);

    /* Wait for the request to finish. */
    aws_s3_tester_wait_for_meta_request_finish(&tester);

    aws_s3_tester_lock_synced_data(&tester);
    ASSERT_TRUE(tester.synced_data.finish_error_code != AWS_ERROR_SUCCESS);
    aws_s3_tester_unlock_synced_data(&tester);

    ASSERT_TRUE(meta_request_test_results.finished_response_status == 404);
    ASSERT_TRUE(meta_request_test_results.finished_error_code != AWS_ERROR_SUCCESS);

    ASSERT_TRUE(meta_request_test_results.error_response_headers != NULL);
    ASSERT_TRUE(meta_request_test_results.error_response_body.len > 0);

    aws_s3_meta_request_release(meta_request);
    meta_request = NULL;

    aws_s3_tester_wait_for_meta_request_shutdown(&tester);
    aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);

    aws_http_message_release(message);
    message = NULL;

    aws_string_destroy(host_name);
    host_name = NULL;

    aws_s3_client_release(client);
    client = NULL;

    aws_s3_tester_clean_up(&tester);

    return 0;
}

static void s_test_s3_existing_host_entry_address_resolved_callback(
    struct aws_host_resolver *resolver,
    const struct aws_string *host_name,
    int err_code,
    const struct aws_array_list *host_addresses,
    void *user_data) {
    (void)resolver;
    (void)host_name;
    (void)err_code;
    (void)host_addresses;

    struct aws_s3_tester *tester = user_data;
    AWS_ASSERT(tester);
    aws_s3_tester_notify_signal(tester);
}

AWS_TEST_CASE(test_s3_existing_host_entry, s_test_s3_existing_host_entry)
static int s_test_s3_existing_host_entry(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client_config client_config;
    AWS_ZERO_STRUCT(client_config);

    ASSERT_SUCCESS(aws_s3_tester_bind_client(
        &tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION | AWS_S3_TESTER_BIND_CLIENT_SIGNING));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    struct aws_string *host_name =
        aws_s3_tester_build_endpoint_string(allocator, &g_test_public_bucket_name, &g_test_s3_region);

    {
        struct aws_host_resolution_config host_resolver_config;
        AWS_ZERO_STRUCT(host_resolver_config);
        host_resolver_config.impl = aws_default_dns_resolve;
        host_resolver_config.max_ttl = 30;
        host_resolver_config.impl_data = NULL;

        ASSERT_SUCCESS(aws_host_resolver_resolve_host(
            client_config.client_bootstrap->host_resolver,
            host_name,
            s_test_s3_existing_host_entry_address_resolved_callback,
            &host_resolver_config,
            &tester));

        aws_s3_tester_wait_for_signal(&tester);
    }

    /* Put together a simple S3 Get Object request. */
    struct aws_http_message *message = aws_s3_test_get_object_request_new(
        allocator, aws_byte_cursor_from_string(host_name), g_s3_path_get_object_test_1MB);

    struct aws_s3_meta_request_options options;
    AWS_ZERO_STRUCT(options);
    options.type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT;
    options.message = message;

    /* Trigger accelerating of our Get Object request. */
    struct aws_s3_meta_request_test_results meta_request_test_results;
    AWS_ZERO_STRUCT(meta_request_test_results);

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request(
        &tester, client, &options, &meta_request_test_results, AWS_S3_TESTER_SEND_META_REQUEST_EXPECT_SUCCESS));
    ASSERT_SUCCESS(aws_s3_tester_validate_get_object_results(&meta_request_test_results, 0));

    aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);

    aws_http_message_release(message);
    aws_string_destroy(host_name);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(test_s3_bad_endpoint, s_test_s3_bad_endpoint)
static int s_test_s3_bad_endpoint(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client_config client_config;
    AWS_ZERO_STRUCT(client_config);

    ASSERT_SUCCESS(aws_s3_tester_bind_client(
        &tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION | AWS_S3_TESTER_BIND_CLIENT_SIGNING));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    struct aws_byte_cursor test_key = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("test_key");

    AWS_STATIC_STRING_FROM_LITERAL(invalid_host_name, "invalid_host_name");

    /* Construct a message that points to an invalid host name. Key can be anything. */
    struct aws_http_message *message =
        aws_s3_test_get_object_request_new(allocator, aws_byte_cursor_from_string(invalid_host_name), test_key);

    struct aws_s3_meta_request_options options;
    AWS_ZERO_STRUCT(options);
    options.type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT;
    options.message = message;

    struct aws_s3_meta_request_test_results meta_request_test_results;
    AWS_ZERO_STRUCT(meta_request_test_results);

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request(&tester, client, &options, &meta_request_test_results, 0));

    ASSERT_TRUE(meta_request_test_results.finished_error_code == AWS_ERROR_S3_NO_ENDPOINT_CONNECTIONS);

    aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);

    aws_http_message_release(message);
    aws_s3_client_release(client);
    client = NULL;

    aws_s3_tester_clean_up(&tester);

    return 0;
}

static int s_s3_test_headers_callback_raise_error(
    struct aws_s3_meta_request *meta_request,
    const struct aws_http_headers *headers,
    int response_status,
    void *user_data) {
    (void)meta_request;
    (void)headers;
    (void)response_status;
    (void)user_data;
    aws_raise_error(AWS_ERROR_UNKNOWN);
    return AWS_OP_ERR;
}

static int s_s3_test_body_callback_raise_error(
    struct aws_s3_meta_request *meta_request,
    const struct aws_byte_cursor *body,
    uint64_t range_start,
    void *user_data) {
    (void)meta_request;
    (void)body;
    (void)range_start;
    (void)user_data;
    aws_raise_error(AWS_ERROR_UNKNOWN);
    return AWS_OP_ERR;
}

AWS_TEST_CASE(test_s3_put_object_fail_headers_callback, s_test_s3_put_object_fail_headers_callback)
static int s_test_s3_put_object_fail_headers_callback(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_meta_request_test_results meta_request_test_results;
    AWS_ZERO_STRUCT(meta_request_test_results);

    struct aws_s3_tester_meta_request_options options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .headers_callback = s_s3_test_headers_callback_raise_error,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE,
        .put_options =
            {
                .ensure_multipart = true,
            },
    };

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(NULL, &options, &meta_request_test_results));
    ASSERT_TRUE(meta_request_test_results.finished_error_code == AWS_ERROR_UNKNOWN);

    aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);

    return 0;
}

AWS_TEST_CASE(test_s3_put_object_fail_body_callback, s_test_s3_put_object_fail_body_callback)
static int s_test_s3_put_object_fail_body_callback(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester_meta_request_options options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .body_callback = s_s3_test_body_callback_raise_error,

        /* Put object currently never invokes the body callback, which means it should not fail. */
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_SUCCESS,

        .put_options =
            {
                .ensure_multipart = true,
            },
    };

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(NULL, &options, NULL));

    return 0;
}

AWS_TEST_CASE(test_s3_get_object_fail_headers_callback, s_test_s3_get_object_fail_headers_callback)
static int s_test_s3_get_object_fail_headers_callback(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_meta_request_test_results meta_request_test_results;
    AWS_ZERO_STRUCT(meta_request_test_results);

    struct aws_s3_tester_meta_request_options options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .headers_callback = s_s3_test_headers_callback_raise_error,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE,
        .get_options =
            {
                .object_path = g_s3_path_get_object_test_1MB,
            },
    };

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(NULL, &options, &meta_request_test_results));
    ASSERT_TRUE(meta_request_test_results.finished_error_code == AWS_ERROR_UNKNOWN);

    aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);

    return 0;
}

AWS_TEST_CASE(test_s3_get_object_fail_body_callback, s_test_s3_get_object_fail_body_callback)
static int s_test_s3_get_object_fail_body_callback(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_meta_request_test_results meta_request_test_results;
    AWS_ZERO_STRUCT(meta_request_test_results);

    struct aws_s3_tester_meta_request_options options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .body_callback = s_s3_test_body_callback_raise_error,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE,
        .get_options =
            {
                .object_path = g_s3_path_get_object_test_1MB,
            },
    };

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(NULL, &options, &meta_request_test_results));
    ASSERT_TRUE(meta_request_test_results.finished_error_code == AWS_ERROR_UNKNOWN);

    aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);

    return 0;
}

AWS_TEST_CASE(test_s3_default_fail_headers_callback, s_test_s3_default_fail_headers_callback)
static int s_test_s3_default_fail_headers_callback(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_meta_request_test_results meta_request_test_results;
    AWS_ZERO_STRUCT(meta_request_test_results);

    struct aws_s3_tester_meta_request_options options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_DEFAULT,
        .headers_callback = s_s3_test_headers_callback_raise_error,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE,
        .default_type_options =
            {
                .mode = AWS_S3_TESTER_DEFAULT_TYPE_MODE_GET,
            },
        .get_options =
            {
                .object_path = g_s3_path_get_object_test_1MB,
            },
    };

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(NULL, &options, &meta_request_test_results));
    ASSERT_TRUE(meta_request_test_results.finished_error_code == AWS_ERROR_UNKNOWN);

    aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);

    return 0;
}

AWS_TEST_CASE(test_s3_default_fail_body_callback, s_test_s3_default_fail_body_callback)
static int s_test_s3_default_fail_body_callback(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_meta_request_test_results meta_request_test_results;
    AWS_ZERO_STRUCT(meta_request_test_results);

    struct aws_s3_tester_meta_request_options options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_DEFAULT,
        .body_callback = s_s3_test_body_callback_raise_error,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE,
        .default_type_options =
            {
                .mode = AWS_S3_TESTER_DEFAULT_TYPE_MODE_GET,
            },
        .get_options =
            {
                .object_path = g_s3_path_get_object_test_1MB,
            },
    };

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(NULL, &options, &meta_request_test_results));
    ASSERT_TRUE(meta_request_test_results.finished_error_code == AWS_ERROR_UNKNOWN);

    aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);

    return 0;
}

AWS_TEST_CASE(test_s3_put_fail_object_invalid_request, s_test_s3_put_fail_object_invalid_request)
static int s_test_s3_put_fail_object_invalid_request(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_meta_request_test_results meta_request_test_results;
    AWS_ZERO_STRUCT(meta_request_test_results);

    struct aws_s3_tester_meta_request_options options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE,
        .put_options =
            {
                .ensure_multipart = true,
                .invalid_request = true,
            },
    };

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(NULL, &options, &meta_request_test_results));
    ASSERT_UINT_EQUALS(meta_request_test_results.finished_error_code, AWS_ERROR_S3_INVALID_RESPONSE_STATUS);

    aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);

    return 0;
}

AWS_TEST_CASE(
    test_s3_put_single_part_fail_object_inputstream_fail_reading,
    s_test_s3_put_single_part_fail_object_inputstream_fail_reading)
static int s_test_s3_put_single_part_fail_object_inputstream_fail_reading(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_meta_request_test_results meta_request_test_results;
    AWS_ZERO_STRUCT(meta_request_test_results);

    struct aws_s3_tester_meta_request_options options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE,
        .put_options =
            {
                .ensure_multipart = true,
                .invalid_input_stream = true,
                .content_length = 10,
            },
    };

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(NULL, &options, &meta_request_test_results));

    ASSERT_TRUE(meta_request_test_results.finished_error_code != AWS_ERROR_SUCCESS);

    aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);

    return 0;
}

AWS_TEST_CASE(test_s3_put_fail_object_inputstream_fail_reading, s_test_s3_put_fail_object_inputstream_fail_reading)
static int s_test_s3_put_fail_object_inputstream_fail_reading(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_meta_request_test_results meta_request_test_results;
    AWS_ZERO_STRUCT(meta_request_test_results);

    struct aws_s3_tester_meta_request_options options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE,
        .put_options =
            {
                .ensure_multipart = true,
                .invalid_input_stream = true,
                .content_length = 10 * 1024 * 1024,
            },
    };

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(NULL, &options, &meta_request_test_results));

    ASSERT_UINT_EQUALS(meta_request_test_results.finished_error_code, AWS_IO_STREAM_READ_FAILED);

    aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);

    return 0;
}

AWS_TEST_CASE(test_s3_different_endpoints, s_test_s3_different_endpoints)
static int s_test_s3_different_endpoints(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client *client = NULL;
    struct aws_s3_tester_client_options client_options;

    AWS_ZERO_STRUCT(client_options);
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    {
        struct aws_s3_meta_request_test_results meta_request_test_results;
        AWS_ZERO_STRUCT(meta_request_test_results);

        struct aws_s3_tester_meta_request_options options = {
            .allocator = allocator,
            .client = client,
            .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
            .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_SUCCESS,
            .get_options =
                {
                    .object_path = g_s3_path_get_object_test_1MB,
                },
        };

        ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &options, &meta_request_test_results));
        aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);
    }

    {
        struct aws_s3_meta_request_test_results meta_request_test_results;
        AWS_ZERO_STRUCT(meta_request_test_results);

        struct aws_byte_cursor bucket_without_file = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("aws-crt-test-stuff");

        struct aws_s3_tester_meta_request_options options = {
            .allocator = allocator,
            .client = client,
            .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
            .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE,
            .bucket_name = &bucket_without_file,
            .get_options =
                {
                    .object_path = g_s3_path_get_object_test_1MB,
                },
        };

        ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &options, &meta_request_test_results));
        ASSERT_TRUE(meta_request_test_results.finished_error_code == AWS_ERROR_INVALID_ARGUMENT);

        aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);
    }

    aws_s3_client_release(client);

    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(test_s3_put_object_clamp_part_size, s_test_s3_put_object_clamp_part_size)
static int s_test_s3_put_object_clamp_part_size(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client_config client_config = {
        .part_size = 64 * 1024,
        .max_part_size = 64 * 1024,
    };

    ASSERT_TRUE(client_config.part_size < g_s3_min_upload_part_size);
    ASSERT_TRUE(client_config.max_part_size < g_s3_min_upload_part_size);

    ASSERT_SUCCESS(aws_s3_tester_bind_client(
        &tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION | AWS_S3_TESTER_BIND_CLIENT_SIGNING));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    ASSERT_TRUE(client != NULL);

    struct aws_s3_meta_request_test_results test_results;
    AWS_ZERO_STRUCT(test_results);

    /* Upload should now succeed even when specifying a smaller than allowed part size. */
    ASSERT_SUCCESS(aws_s3_tester_send_put_object_meta_request(
        &tester, client, 10, AWS_S3_TESTER_SEND_META_REQUEST_EXPECT_SUCCESS, &test_results));

    ASSERT_TRUE(test_results.part_size == g_s3_min_upload_part_size);

    aws_s3_meta_request_test_results_clean_up(&test_results);

    aws_s3_client_release(client);
    client = NULL;

    aws_s3_tester_clean_up(&tester);

    return 0;
}

struct replace_quote_entities_test_case {
    struct aws_string *test_string;
    struct aws_byte_cursor expected_result;
};

AWS_TEST_CASE(test_s3_replace_quote_entities, s_test_s3_replace_quote_entities)
static int s_test_s3_replace_quote_entities(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct replace_quote_entities_test_case test_cases[] = {
        {
            .test_string = aws_string_new_from_c_str(allocator, "&quot;testtest"),
            .expected_result = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("\"testtest"),
        },
        {
            .test_string = aws_string_new_from_c_str(allocator, "testtest&quot;"),
            .expected_result = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("testtest\""),
        },
        {
            .test_string = aws_string_new_from_c_str(allocator, "&quot;&quot;"),
            .expected_result = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("\"\""),
        },
        {
            .test_string = aws_string_new_from_c_str(allocator, "testtest"),
            .expected_result = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("testtest"),
        },
        {
            .test_string = aws_string_new_from_c_str(allocator, ""),
            .expected_result = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL(""),
        },
    };

    for (size_t i = 0; i < (sizeof(test_cases) / sizeof(struct replace_quote_entities_test_case)); ++i) {
        struct replace_quote_entities_test_case *test_case = &test_cases[i];

        struct aws_byte_buf result_byte_buf;
        AWS_ZERO_STRUCT(result_byte_buf);

        replace_quote_entities(allocator, test_case->test_string, &result_byte_buf);

        struct aws_byte_cursor result_byte_cursor = aws_byte_cursor_from_buf(&result_byte_buf);

        ASSERT_TRUE(aws_byte_cursor_eq(&test_case->expected_result, &result_byte_cursor));

        aws_byte_buf_clean_up(&result_byte_buf);
        aws_string_destroy(test_case->test_string);
        test_case->test_string = NULL;
    }

    aws_s3_tester_clean_up(&tester);

    return 0;
}
