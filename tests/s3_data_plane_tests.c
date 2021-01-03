/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_client_impl.h"
#include "aws/s3/private/s3_meta_request_impl.h"
#include "aws/s3/private/s3_request_messages.h"
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

    struct aws_s3_meta_request *meta_request = aws_s3_tester_meta_request_new(&tester, NULL, NULL);
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

enum aws_s3_client_tls_usage {
    AWS_S3_TLS_DEFAULT,
    AWS_S3_TLS_ENABLED,
    AWS_S3_TLS_DISABLED,
};

static int s_test_s3_get_object_helper(
    struct aws_allocator *allocator,
    enum aws_s3_client_tls_usage tls_usage,
    enum aws_s3_tester_sse_type sse_type,
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

    ASSERT_SUCCESS(aws_s3_tester_send_get_object_meta_request(
        &tester, client, s3_path, AWS_S3_TESTER_SEND_META_REQUEST_EXPECT_SUCCESS, sse_type));

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

    ASSERT_SUCCESS(s_test_s3_get_object_helper(
        allocator, AWS_S3_TLS_DISABLED, AWS_S3_TESTER_SSE_NONE, g_s3_path_get_object_test_1MB));

    return 0;
}

AWS_TEST_CASE(test_s3_get_object_tls_enabled, s_test_s3_get_object_tls_enabled)
static int s_test_s3_get_object_tls_enabled(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(s_test_s3_get_object_helper(
        allocator, AWS_S3_TLS_ENABLED, AWS_S3_TESTER_SSE_NONE, g_s3_path_get_object_test_1MB));

    return 0;
}

AWS_TEST_CASE(test_s3_get_object_tls_default, s_test_s3_get_object_tls_default)
static int s_test_s3_get_object_tls_default(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(s_test_s3_get_object_helper(
        allocator, AWS_S3_TLS_ENABLED, AWS_S3_TESTER_SSE_NONE, g_s3_path_get_object_test_1MB));

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

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request(
        &tester, client, &options, &meta_request_test_results, AWS_S3_TESTER_SEND_META_REQUEST_EXPECT_SUCCESS));
    ASSERT_SUCCESS(aws_s3_tester_validate_get_object_results(&meta_request_test_results, AWS_S3_TESTER_SSE_NONE));

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

        ASSERT_SUCCESS(aws_s3_tester_send_meta_request(&tester, client, &options, &meta_request_test_results, 0));
        ASSERT_TRUE(
            aws_s3_tester_validate_get_object_results(&meta_request_test_results, AWS_S3_TESTER_SSE_NONE) !=
            AWS_OP_SUCCESS);

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

        ASSERT_SUCCESS(aws_s3_tester_send_meta_request(
            &tester, client, &options, &meta_request_test_results, AWS_S3_TESTER_SEND_META_REQUEST_EXPECT_SUCCESS));
        ASSERT_SUCCESS(aws_s3_tester_validate_get_object_results(&meta_request_test_results, AWS_S3_TESTER_SSE_NONE));

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
        &tester,
        client,
        g_s3_path_get_object_test_1MB,
        AWS_S3_TESTER_SEND_META_REQUEST_EXPECT_SUCCESS,
        AWS_S3_TESTER_SSE_NONE));

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
    struct aws_s3_meta_request_test_results meta_request_test_resultss[4];
    size_t num_meta_requests = sizeof(meta_requests) / sizeof(struct aws_s3_meta_request *);

    ASSERT_TRUE(
        num_meta_requests == (sizeof(meta_request_test_resultss) / sizeof(struct aws_s3_meta_request_test_results)));

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
        struct aws_s3_meta_request_options options;
        AWS_ZERO_STRUCT(options);
        options.type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT;
        options.message = message;

        ASSERT_SUCCESS(aws_s3_tester_bind_meta_request(&tester, &options, &meta_request_test_resultss[i]));

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
        aws_s3_tester_validate_get_object_results(&meta_request_test_resultss[i], AWS_S3_TESTER_SSE_NONE);
        aws_s3_meta_request_test_results_clean_up(&meta_request_test_resultss[i]);
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

AWS_TEST_CASE(test_s3_get_object_sse_kms, s_test_s3_get_object_sse_kms)
static int s_test_s3_get_object_sse_kms(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    /* Keep TLS enabled for SSE related download, or it will fail. */
    return s_test_s3_get_object_helper(
        allocator,
        AWS_S3_TLS_ENABLED,
        AWS_S3_TESTER_SSE_KMS,
        aws_byte_cursor_from_c_str("/get_object_test_kms_10MB.txt"));
}

AWS_TEST_CASE(test_s3_get_object_sse_aes256, s_test_s3_get_object_sse_aes256)
static int s_test_s3_get_object_sse_aes256(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    /* Keep TLS enabled for SSE related download, or it will fail. */
    return s_test_s3_get_object_helper(
        allocator,
        AWS_S3_TLS_ENABLED,
        AWS_S3_TESTER_SSE_AES256,
        aws_byte_cursor_from_c_str("/get_object_test_aes256_10MB.txt"));
}

static int s_test_s3_put_object_helper(struct aws_allocator *allocator, enum aws_s3_client_tls_usage tls_usage) {
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
        &tester, client, 10, AWS_S3_TESTER_SEND_META_REQUEST_EXPECT_SUCCESS, AWS_S3_TESTER_SSE_NONE));

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

    ASSERT_SUCCESS(s_test_s3_put_object_helper(allocator, AWS_S3_TLS_DISABLED));

    return 0;
}

AWS_TEST_CASE(test_s3_put_object_tls_enabled, s_test_s3_put_object_tls_enabled)
static int s_test_s3_put_object_tls_enabled(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(s_test_s3_put_object_helper(allocator, AWS_S3_TLS_ENABLED));

    return 0;
}

AWS_TEST_CASE(test_s3_put_object_tls_default, s_test_s3_put_object_tls_default)
static int s_test_s3_put_object_tls_default(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(s_test_s3_put_object_helper(allocator, AWS_S3_TLS_DEFAULT));

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
        &tester, client, 10, AWS_S3_TESTER_SEND_META_REQUEST_EXPECT_SUCCESS, AWS_S3_TESTER_SSE_NONE));

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
        &tester, client, 10, AWS_S3_TESTER_SEND_META_REQUEST_EXPECT_SUCCESS, AWS_S3_TESTER_SSE_KMS));

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
        &tester, client, 10, AWS_S3_TESTER_SEND_META_REQUEST_EXPECT_SUCCESS, AWS_S3_TESTER_SSE_KMS));

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
        &tester, client, 10, AWS_S3_TESTER_SEND_META_REQUEST_EXPECT_SUCCESS, AWS_S3_TESTER_SSE_AES256));

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
        &tester, client, 10, AWS_S3_TESTER_SEND_META_REQUEST_EXPECT_SUCCESS, AWS_S3_TESTER_SSE_AES256));

    aws_s3_client_release(client);
    client = NULL;

    aws_s3_tester_clean_up(&tester);

    return 0;
}

static int s_test_s3_put_object_content_md5_helper(struct aws_allocator *allocator, bool multipart_upload, enum aws_s3_meta_request_compute_content_md5 compute_content_md5, bool expect_succeed) {
    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    size_t part_size = 5 * 1024 * 1024;
    if (!multipart_upload) {
        /* content_length < part_size */
        part_size = 15 * 1024 * 1024;
    }

    /**
     * flags metrics for successful scenarios:
     *              | default   | enabled   | disabled
     *   singlepart | correct   | incorrect | incorrect
     *   multipart  | incorrect | incorrect | correct
     *
     * flags metric for failed scenarios:
     *              | default   | enabled   | disabled
     *   singlepart | incorrect | N/A       | N/A
     *   multipart  | N/A       | N/A       | N/A
     */
    uint32_t flags = 0;
    if (expect_succeed) {
        flags = AWS_S3_TESTER_SEND_META_REQUEST_EXPECT_SUCCESS;
        if ((!multipart_upload && compute_content_md5 == AWS_MR_CONTENT_MD5_DEFAULT) ||
            (multipart_upload && compute_content_md5 == AWS_MR_CONTENT_MD5_DISABLED)) {
            flags = flags | AWS_S3_TESTER_SEND_META_REQUEST_WITH_CORRECT_CONTENT_MD5;
        } else {
            flags = flags | AWS_S3_TESTER_SEND_META_REQUEST_WITH_INCORRECT_CONTENT_MD5;
        }
    } else {
        if (!multipart_upload && compute_content_md5 == AWS_MR_CONTENT_MD5_DEFAULT) {
            flags = AWS_S3_TESTER_SEND_META_REQUEST_WITH_INCORRECT_CONTENT_MD5;
        } else {
            flags = AWS_S3_TESTER_SEND_META_REQUEST_EXPECT_SUCCESS;
        }
    }

    struct aws_s3_client_config client_config = {
        .part_size = part_size,
    };

    client_config.compute_content_md5 = compute_content_md5;

    ASSERT_SUCCESS(aws_s3_tester_bind_client(
        &tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION | AWS_S3_TESTER_BIND_CLIENT_SIGNING));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    ASSERT_TRUE(client != NULL);

    ASSERT_SUCCESS(aws_s3_tester_send_put_object_meta_request(
        &tester,
        client,
        10,
        flags,
        AWS_S3_TESTER_SSE_NONE));

    aws_s3_client_release(client);
    client = NULL;

    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(test_s3_put_object_singlepart_content_md5_default, s_test_s3_put_object_singlepart_content_md5_default)
static int s_test_s3_put_object_singlepart_content_md5_default(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(s_test_s3_put_object_content_md5_helper(allocator, false, AWS_MR_CONTENT_MD5_DEFAULT, true));
    ASSERT_SUCCESS(s_test_s3_put_object_content_md5_helper(allocator, false, AWS_MR_CONTENT_MD5_DEFAULT, false));

    return 0;
}

AWS_TEST_CASE(test_s3_put_object_singlepart_content_md5_enabled, s_test_s3_put_object_singlepart_content_md5_enabled)
static int s_test_s3_put_object_singlepart_content_md5_enabled(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(s_test_s3_put_object_content_md5_helper(allocator, false, AWS_MR_CONTENT_MD5_ENABLED, true));

    return 0;
}

AWS_TEST_CASE(test_s3_put_object_singlepart_content_md5_disabled, s_test_s3_put_object_singlepart_content_md5_disabled)
static int s_test_s3_put_object_singlepart_content_md5_disabled(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(s_test_s3_put_object_content_md5_helper(allocator, false, AWS_MR_CONTENT_MD5_DISABLED, true));

    return 0;
}

AWS_TEST_CASE(test_s3_put_object_multipart_content_md5_default, s_test_s3_put_object_multipart_content_md5_default)
static int s_test_s3_put_object_multipart_content_md5_default(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(s_test_s3_put_object_content_md5_helper(allocator, true, AWS_MR_CONTENT_MD5_DEFAULT, true));

    return 0;
}

AWS_TEST_CASE(test_s3_put_object_multipart_content_md5_enabled, s_test_s3_put_object_multipart_content_md5_enabled)
static int s_test_s3_put_object_multipart_content_md5_enabled(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(s_test_s3_put_object_content_md5_helper(allocator, true, AWS_MR_CONTENT_MD5_ENABLED, true));

    return 0;
}

AWS_TEST_CASE(test_s3_put_object_multipart_content_md5_disabled, s_test_s3_put_object_multipart_content_md5_disabled)
static int s_test_s3_put_object_multipart_content_md5_disabled(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(s_test_s3_put_object_content_md5_helper(allocator, true, AWS_MR_CONTENT_MD5_DISABLED, true));

    return 0;
}

static int s_test_s3_upload_part_message_helper(struct aws_allocator *allocator, bool should_compute_content_md5) {

    aws_s3_library_init(allocator);

    struct aws_byte_buf test_buffer;
    aws_s3_create_test_buffer(allocator, 19 /* size of "This is an S3 test." */, &test_buffer);
    /* base64 encoded md5 of "This is an S3 test." */
    struct aws_byte_cursor expected_content_md5 = aws_byte_cursor_from_c_str("+y3U+EY5uFXhVVmRoiJWyA==");

    struct aws_byte_cursor test_body_cursor = aws_byte_cursor_from_buf(&test_buffer);
    struct aws_input_stream *input_stream = aws_input_stream_new_from_cursor(allocator, &test_body_cursor);

    struct aws_byte_cursor host_name = aws_byte_cursor_from_c_str("dummy_host");

    struct aws_byte_cursor test_object_path = aws_byte_cursor_from_c_str("dummy_key");

    /* Put together a simple S3 Put Object request. */
    struct aws_http_message *base_message = aws_s3_test_put_object_request_new(
        allocator, host_name, test_object_path, g_test_body_content_type, input_stream, AWS_S3_TESTER_SSE_NONE);

    uint32_t part_number = 1;
    struct aws_string *upload_id = aws_string_new_from_c_str(allocator, "dummy_upload_id");

    struct aws_http_message *new_message =
        aws_s3_upload_part_message_new(allocator, base_message, &test_buffer, part_number, upload_id, should_compute_content_md5);

    struct aws_http_headers *new_headers = aws_http_message_get_headers(new_message);
    if (should_compute_content_md5) {
        ASSERT_TRUE(aws_http_headers_has(new_headers, g_content_md5_header_name));
        struct aws_byte_cursor content_md5;
        aws_http_headers_get(new_headers, g_content_md5_header_name, &content_md5);
        ASSERT_BIN_ARRAYS_EQUALS(expected_content_md5.ptr, expected_content_md5.len, content_md5.ptr, content_md5.len);
    } else {
        ASSERT_FALSE(aws_http_headers_has(new_headers, g_content_md5_header_name));
    }

    struct aws_input_stream *new_body_stream = aws_http_message_get_body_stream(new_message);
    aws_input_stream_destroy(new_body_stream);
    new_body_stream = NULL;

    aws_http_message_release(new_message);
    new_message = NULL;

    aws_http_message_release(base_message);
    base_message = NULL;

    aws_string_destroy(upload_id);
    upload_id = NULL;

    aws_input_stream_destroy(input_stream);
    input_stream = NULL;

    aws_byte_buf_clean_up(&test_buffer);

    aws_s3_library_clean_up();

    return 0;
}

AWS_TEST_CASE(test_s3_upload_part_message_with_content_md5, s_test_s3_upload_part_message_with_content_md5)
static int s_test_s3_upload_part_message_with_content_md5(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(s_test_s3_upload_part_message_helper(allocator, true));

    return 0;
}

AWS_TEST_CASE(test_s3_upload_part_message_without_content_md5, s_test_s3_upload_part_message_without_content_md5)
static int s_test_s3_upload_part_message_without_content_md5(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(s_test_s3_upload_part_message_helper(allocator, false));

    return 0;
}

AWS_TEST_CASE(
    test_s3_create_multipart_upload_message_with_content_md5,
    s_test_s3_create_multipart_upload_message_with_content_md5)
static int s_test_s3_create_multipart_upload_message_with_content_md5(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_byte_buf test_buffer;
    aws_s3_create_test_buffer(allocator, 19 /* size of "This is an S3 test." */, &test_buffer);

    struct aws_byte_cursor test_body_cursor = aws_byte_cursor_from_buf(&test_buffer);
    struct aws_input_stream *input_stream = aws_input_stream_new_from_cursor(allocator, &test_body_cursor);

    struct aws_byte_cursor host_name = aws_byte_cursor_from_c_str("dummy_host");

    struct aws_byte_cursor test_object_path = aws_byte_cursor_from_c_str("dummy_key");

    /* Put together a simple S3 Put Object request. */
    struct aws_http_message *base_message = aws_s3_test_put_object_request_new(
        allocator, host_name, test_object_path, g_test_body_content_type, input_stream, AWS_S3_TESTER_SSE_NONE);

    struct aws_http_header content_md5_header = {.name = g_content_md5_header_name,
                                                 .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("dummy_content_md5")};
    ASSERT_SUCCESS(aws_http_message_add_header(base_message, content_md5_header));

    struct aws_http_headers *base_headers = aws_http_message_get_headers(base_message);
    ASSERT_TRUE(aws_http_headers_has(base_headers, g_content_md5_header_name));

    struct aws_http_message *new_message = aws_s3_create_multipart_upload_message_new(allocator, base_message);

    struct aws_http_headers *new_headers = aws_http_message_get_headers(new_message);
    ASSERT_FALSE(aws_http_headers_has(new_headers, g_content_md5_header_name));

    aws_http_message_release(new_message);
    new_message = NULL;

    aws_http_message_release(base_message);
    base_message = NULL;

    aws_input_stream_destroy(input_stream);
    input_stream = NULL;

    aws_byte_buf_clean_up(&test_buffer);

    return 0;
}

AWS_TEST_CASE(
    test_s3_complete_multipart_message_with_content_md5,
    s_test_s3_complete_multipart_message_with_content_md5)
static int s_test_s3_complete_multipart_message_with_content_md5(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_byte_buf test_buffer;
    aws_s3_create_test_buffer(allocator, 19 /* size of "This is an S3 test." */, &test_buffer);

    struct aws_byte_cursor test_body_cursor = aws_byte_cursor_from_buf(&test_buffer);
    struct aws_input_stream *input_stream = aws_input_stream_new_from_cursor(allocator, &test_body_cursor);

    struct aws_byte_cursor host_name = aws_byte_cursor_from_c_str("dummy_host");

    struct aws_byte_cursor test_object_path = aws_byte_cursor_from_c_str("dummy_key");

    /* Put together a simple S3 Put Object request. */
    struct aws_http_message *base_message = aws_s3_test_put_object_request_new(
        allocator, host_name, test_object_path, g_test_body_content_type, input_stream, AWS_S3_TESTER_SSE_NONE);

    struct aws_http_header content_md5_header = {.name = g_content_md5_header_name,
                                                 .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("dummy_content_md5")};
    ASSERT_SUCCESS(aws_http_message_add_header(base_message, content_md5_header));

    struct aws_http_headers *base_headers = aws_http_message_get_headers(base_message);
    ASSERT_TRUE(aws_http_headers_has(base_headers, g_content_md5_header_name));

    struct aws_byte_buf body_buffer;
    aws_byte_buf_init(&body_buffer, allocator, 512);

    struct aws_string *upload_id = aws_string_new_from_c_str(allocator, "dummy_upload_id");

    struct aws_array_list etags;
    ASSERT_SUCCESS(aws_array_list_init_dynamic(&etags, allocator, 0, sizeof(struct aws_string)));

    struct aws_http_message *new_message =
        aws_s3_complete_multipart_message_new(allocator, base_message, &body_buffer, upload_id, &etags);

    struct aws_http_headers *new_headers = aws_http_message_get_headers(new_message);
    ASSERT_FALSE(aws_http_headers_has(new_headers, g_content_md5_header_name));

    struct aws_input_stream *new_body_stream = aws_http_message_get_body_stream(new_message);
    aws_input_stream_destroy(new_body_stream);
    new_body_stream = NULL;

    aws_http_message_release(new_message);
    new_message = NULL;

    aws_http_message_release(base_message);
    base_message = NULL;

    aws_array_list_clean_up(&etags);

    aws_string_destroy(upload_id);
    upload_id = NULL;

    aws_byte_buf_clean_up(&body_buffer);

    aws_input_stream_destroy(input_stream);
    input_stream = NULL;

    aws_byte_buf_clean_up(&test_buffer);

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

    ASSERT_SUCCESS(aws_s3_tester_bind_meta_request(&tester, &options, &meta_request_test_results));

    struct aws_s3_meta_request *meta_request = aws_s3_client_make_meta_request(client, &options);

    ASSERT_TRUE(meta_request != NULL);

    /* Wait for the request to finish. */
    aws_s3_tester_wait_for_meta_request_finish(&tester);

    aws_s3_tester_lock_synced_data(&tester);

    ASSERT_TRUE(tester.synced_data.finish_error_code == AWS_ERROR_SUCCESS);

    aws_s3_tester_unlock_synced_data(&tester);

    ASSERT_SUCCESS(aws_s3_tester_validate_get_object_results(&meta_request_test_results, AWS_S3_TESTER_SSE_NONE));

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

AWS_TEST_CASE(test_s3_error_response, s_test_s3_error_response)
static int s_test_s3_error_response(struct aws_allocator *allocator, void *ctx) {
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
    aws_condition_variable_notify_one(&tester->signal);
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

        aws_s3_tester_lock_synced_data(&tester);
        aws_condition_variable_wait(&tester.signal, &tester.synced_data.lock);
        aws_s3_tester_unlock_synced_data(&tester);
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

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request(
        &tester, client, &options, &meta_request_test_results, AWS_S3_TESTER_SEND_META_REQUEST_EXPECT_SUCCESS));
    ASSERT_SUCCESS(aws_s3_tester_validate_get_object_results(&meta_request_test_results, AWS_S3_TESTER_SSE_NONE));

    aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);

    aws_http_message_release(message);
    aws_string_destroy(host_name);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return 0;
}
