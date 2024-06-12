/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_util.h"
#include "aws/s3/private/s3express_credentials_provider_impl.h"
#include "aws/s3/s3_client.h"
#include "aws/s3/s3express_credentials_provider.h"
#include "s3_tester.h"
#include <aws/common/atomics.h>
#include <aws/common/clock.h>
#include <aws/common/lru_cache.h>
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

struct aws_s3express_client_tester {
    struct aws_allocator *allocator;
    struct aws_hash_table saver_cache;
    struct aws_atomic_var provider_requests_made;
};

static struct aws_s3express_client_tester s_tester;

static int s_s3express_client_tester_init(struct aws_allocator *allocator) {
    s_tester.allocator = allocator;
    aws_hash_table_init(
        &s_tester.saver_cache,
        allocator,
        100,
        aws_hash_string,
        aws_hash_callback_string_eq,
        aws_hash_callback_string_destroy,
        (aws_hash_callback_destroy_fn *)aws_credentials_release);
    aws_atomic_init_int(&s_tester.provider_requests_made, 0);
    return AWS_OP_SUCCESS;
}

static int s_s3express_client_tester_cleanup(void) {
    aws_hash_table_clean_up(&s_tester.saver_cache);
    return AWS_OP_SUCCESS;
}

static int s_create_s3express_request_mock_server(
    struct aws_allocator *allocator,
    struct aws_s3_tester *tester,
    struct aws_s3_client *client) {

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
        .use_s3express_signing = true,
    };
    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(tester, &put_options, NULL));
    return AWS_OP_SUCCESS;
}

TEST_CASE(s3express_client_sanity_test_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_client_config client_config = {
        .part_size = MB_TO_BYTES(5),
        .tls_mode = AWS_MR_TLS_DISABLED,
        .enable_s3express = true,
    };

    ASSERT_SUCCESS(aws_s3_tester_bind_client(
        &tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION | AWS_S3_TESTER_BIND_CLIENT_SIGNING));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    ASSERT_SUCCESS(s_create_s3express_request_mock_server(allocator, &tester, client));

    ASSERT_NOT_NULL(client->s3express_provider);

    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);
    return AWS_OP_SUCCESS;
}

static int s_s3express_get_creds_fake(
    struct aws_s3express_credentials_provider *provider,
    const struct aws_credentials *original_credentials,
    const struct aws_credentials_properties_s3express *properties,
    aws_on_get_credentials_callback_fn callback,
    void *user_data) {
    (void)properties;
    (void)provider;
    (void)original_credentials;

    struct aws_string *key_1 = aws_string_new_from_c_str(s_tester.allocator, "key_1");
    struct aws_credentials *credentials =
        aws_credentials_new_from_string(s_tester.allocator, key_1, key_1, key_1, SIZE_MAX);
    if (callback) {
        callback(credentials, AWS_ERROR_SUCCESS, user_data);
    }
    aws_credentials_release(credentials);
    aws_string_destroy(key_1);
    return AWS_OP_SUCCESS;
}

static void s_s3express_destroy_fake(struct aws_s3express_credentials_provider *provider) {
    provider->shutdown_complete_callback(provider->shutdown_user_data);
    aws_mem_release(provider->allocator, provider);
}

static struct aws_s3express_credentials_provider_vtable s_fake_s3express_vtable = {
    .get_credentials = s_s3express_get_creds_fake,
    .destroy = s_s3express_destroy_fake,
};

struct aws_s3express_credentials_provider *s_s3express_provider_fake_factory(
    struct aws_allocator *allocator,
    struct aws_s3_client *client,
    aws_simple_completion_callback shutdown_complete_callback,
    void *shutdown_user_data,
    void *factory_user_data) {

    (void)client;
    (void)factory_user_data;
    struct aws_s3express_credentials_provider *provider =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_s3express_credentials_provider));
    aws_s3express_credentials_provider_init_base(provider, allocator, &s_fake_s3express_vtable, NULL);
    provider->shutdown_complete_callback = shutdown_complete_callback;
    provider->shutdown_user_data = shutdown_user_data;
    return provider;
}

TEST_CASE(s3express_client_sanity_override_test_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    s_s3express_client_tester_init(allocator);
    struct aws_s3_client_config client_config = {
        .part_size = MB_TO_BYTES(5),
        .tls_mode = AWS_MR_TLS_DISABLED,
        .enable_s3express = true,
        .s3express_provider_override_factory = s_s3express_provider_fake_factory,
    };

    ASSERT_SUCCESS(aws_s3_tester_bind_client(
        &tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION | AWS_S3_TESTER_BIND_CLIENT_SIGNING));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    ASSERT_SUCCESS(s_create_s3express_request_mock_server(allocator, &tester, client));
    ASSERT_NOT_NULL(client->s3express_provider);

    aws_s3_client_release(client);
    s_s3express_client_tester_cleanup();
    aws_s3_tester_clean_up(&tester);
    return AWS_OP_SUCCESS;
}

static int s_s3express_put_object_request(
    struct aws_allocator *allocator,
    struct aws_s3_client *client,
    size_t content_length,
    struct aws_s3_tester *tester,
    struct aws_byte_cursor host_cursor,
    struct aws_byte_cursor key_cursor,
    struct aws_byte_cursor region) {

    struct aws_input_stream *upload_stream = aws_s3_test_input_stream_new(allocator, content_length);

    struct aws_http_message *message = aws_s3_test_put_object_request_new(
        allocator, &host_cursor, key_cursor, g_test_body_content_type, upload_stream, 0);

    struct aws_s3_meta_request_options options;
    AWS_ZERO_STRUCT(options);
    options.type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT;
    options.message = message;
    struct aws_signing_config_aws s3express_signing_config = {
        .algorithm = AWS_SIGNING_ALGORITHM_V4_S3EXPRESS,
        .service = g_s3express_service_name,
        .region = region,
    };
    options.signing_config = &s3express_signing_config;
    struct aws_s3_meta_request_test_results meta_request_test_results;
    aws_s3_meta_request_test_results_init(&meta_request_test_results, allocator);

    ASSERT_SUCCESS(aws_s3_tester_bind_meta_request(tester, &options, &meta_request_test_results));

    struct aws_s3_meta_request *meta_request = aws_s3_client_make_meta_request(client, &options);
    ASSERT_TRUE(meta_request != NULL);
    /* Wait for the request to finish. */
    aws_s3_tester_wait_for_meta_request_finish(tester);

    ASSERT_SUCCESS(aws_s3_tester_validate_put_object_results(&meta_request_test_results, 0));
    meta_request = aws_s3_meta_request_release(meta_request);
    aws_s3_tester_wait_for_meta_request_shutdown(tester);

    aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);

    aws_http_message_release(message);
    aws_input_stream_release(upload_stream);

    return AWS_OP_SUCCESS;
}

static int s_s3express_client_put_test_helper(struct aws_allocator *allocator, size_t content_length) {

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_byte_cursor region_cursor = aws_byte_cursor_from_c_str("us-east-1");

    struct aws_byte_cursor key_cursor = aws_byte_cursor_from_c_str("/crt-test");

    struct aws_s3_client_config client_config = {
        .part_size = MB_TO_BYTES(5),
        .enable_s3express = true,
        .region = region_cursor,
    };

    ASSERT_SUCCESS(aws_s3_tester_bind_client(&tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_SIGNING));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    ASSERT_SUCCESS(s_s3express_put_object_request(
        allocator,
        client,
        content_length,
        &tester,
        g_test_s3express_bucket_use1_az4_endpoint,
        key_cursor,
        region_cursor));

    struct aws_byte_cursor west2_region_cursor = aws_byte_cursor_from_c_str("us-west-2");
    ASSERT_SUCCESS(s_s3express_put_object_request(
        allocator,
        client,
        content_length,
        &tester,
        g_test_s3express_bucket_usw2_az1_endpoint,
        key_cursor,
        west2_region_cursor));

    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);
    return AWS_OP_SUCCESS;
}

TEST_CASE(s3express_client_put_object) {
    (void)ctx;
    return s_s3express_client_put_test_helper(allocator, MB_TO_BYTES(1));
}

TEST_CASE(s3express_client_put_object_multipart) {
    (void)ctx;
    return s_s3express_client_put_test_helper(allocator, MB_TO_BYTES(100));
}

TEST_CASE(s3express_client_put_object_multipart_multiple) {
    (void)ctx;

    enum s_numbers { NUM_REQUESTS = 100 };

    struct aws_s3_meta_request *meta_requests[NUM_REQUESTS];
    struct aws_s3_meta_request_test_results meta_request_test_results[NUM_REQUESTS];
    struct aws_input_stream *input_streams[NUM_REQUESTS];

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_byte_cursor region_cursor = aws_byte_cursor_from_c_str("us-east-1");

    struct aws_byte_cursor key_cursor = aws_byte_cursor_from_c_str("/crt-test");

    struct aws_byte_cursor west2_region_cursor = aws_byte_cursor_from_c_str("us-west-2");

    struct aws_s3_client_config client_config = {
        .part_size = MB_TO_BYTES(5),
        .enable_s3express = true,
        .region = region_cursor,
    };

    ASSERT_SUCCESS(aws_s3_tester_bind_client(&tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_SIGNING));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    for (size_t i = 0; i < NUM_REQUESTS; ++i) {
        input_streams[i] = aws_s3_test_input_stream_new(allocator, MB_TO_BYTES(10));

        struct aws_byte_cursor request_region = region_cursor;
        struct aws_byte_cursor request_host = g_test_s3express_bucket_use1_az4_endpoint;
        if (i % 2 == 0) {
            /* Make half of request to east1 and rest half to west2 */
            request_region = west2_region_cursor;
            request_host = g_test_s3express_bucket_usw2_az1_endpoint;
        }

        struct aws_http_message *message = aws_s3_test_put_object_request_new(
            allocator, &request_host, key_cursor, g_test_body_content_type, input_streams[i], 0);

        struct aws_s3_meta_request_options options;
        AWS_ZERO_STRUCT(options);
        options.type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT;
        options.message = message;
        struct aws_signing_config_aws s3express_signing_config = {
            .algorithm = AWS_SIGNING_ALGORITHM_V4_S3EXPRESS,
            .service = g_s3express_service_name,
            .region = request_region,
        };
        options.signing_config = &s3express_signing_config;
        aws_s3_meta_request_test_results_init(&meta_request_test_results[i], allocator);

        ASSERT_SUCCESS(aws_s3_tester_bind_meta_request(&tester, &options, &meta_request_test_results[i]));

        meta_requests[i] = aws_s3_client_make_meta_request(client, &options);
        ASSERT_TRUE(meta_requests[i] != NULL);
        aws_http_message_release(message);
    }
    /* Wait for the request to finish. */
    aws_s3_tester_wait_for_meta_request_finish(&tester);
    aws_s3_tester_lock_synced_data(&tester);
    ASSERT_TRUE(tester.synced_data.finish_error_code == AWS_ERROR_SUCCESS);
    aws_s3_tester_unlock_synced_data(&tester);

    for (size_t i = 0; i < NUM_REQUESTS; ++i) {
        meta_requests[i] = aws_s3_meta_request_release(meta_requests[i]);
    }

    aws_s3_tester_wait_for_meta_request_shutdown(&tester);

    for (size_t i = 0; i < NUM_REQUESTS; ++i) {
        aws_s3_tester_validate_put_object_results(&meta_request_test_results[i], 0);
        aws_s3_meta_request_test_results_clean_up(&meta_request_test_results[i]);
    }

    for (size_t i = 0; i < NUM_REQUESTS; ++i) {
        aws_input_stream_release(input_streams[i]);
    }

    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);
    return AWS_OP_SUCCESS;
}

void s_meta_request_finished_overhead(
    struct aws_s3_meta_request *meta_request,
    const struct aws_s3_meta_request_result *meta_request_result,
    void *user_data) {
    (void)meta_request;
    (void)meta_request_result;
    (void)user_data;
    aws_atomic_fetch_add(&s_tester.provider_requests_made, 1);
}

struct aws_s3express_credentials_provider *s_s3express_provider_mock_factory(
    struct aws_allocator *allocator,
    struct aws_s3_client *client,
    aws_simple_completion_callback on_provider_shutdown_callback,
    void *shutdown_user_data,
    void *factory_user_data) {
    (void)factory_user_data;

    struct aws_s3express_credentials_provider_default_options options = {
        .client = client,
        .shutdown_complete_callback = on_provider_shutdown_callback,
        .shutdown_user_data = shutdown_user_data,
    };
    struct aws_s3express_credentials_provider *s3express_provider =
        aws_s3express_credentials_provider_new_default(allocator, &options);

    struct aws_s3express_credentials_provider_impl *impl = s3express_provider->impl;

    impl->mock_test.meta_request_finished_overhead = s_meta_request_finished_overhead;

    return s3express_provider;
}

/* Long running test to make sure our refresh works properly */
TEST_CASE(s3express_client_put_object_long_running_session_refresh) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    s_s3express_client_tester_init(allocator);

    size_t num_meta_requests = 7;

    struct aws_byte_cursor region_cursor = aws_byte_cursor_from_c_str("us-east-1");

    struct aws_byte_cursor key_cursor = aws_byte_cursor_from_c_str("/crt-test");

    struct aws_s3_client_config client_config = {
        .part_size = MB_TO_BYTES(5),
        .enable_s3express = true,
        .region = region_cursor,
        .s3express_provider_override_factory = s_s3express_provider_mock_factory,
    };

    ASSERT_SUCCESS(aws_s3_tester_bind_client(&tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_SIGNING));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    for (size_t i = 0; i < num_meta_requests; i++) {

        struct aws_input_stream *upload_stream = aws_s3_test_input_stream_new(allocator, MB_TO_BYTES(10));

        struct aws_http_message *message = aws_s3_test_put_object_request_new(
            allocator,
            &g_test_s3express_bucket_use1_az4_endpoint,
            key_cursor,
            g_test_body_content_type,
            upload_stream,
            0);
        struct aws_s3_meta_request_options options;
        AWS_ZERO_STRUCT(options);
        options.type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT;
        options.message = message;
        struct aws_signing_config_aws s3express_signing_config = {
            .algorithm = AWS_SIGNING_ALGORITHM_V4_S3EXPRESS,
            .service = g_s3express_service_name,
        };
        options.signing_config = &s3express_signing_config;

        struct aws_s3_meta_request_test_results meta_request_test_results;
        aws_s3_meta_request_test_results_init(&meta_request_test_results, allocator);
        ASSERT_SUCCESS(aws_s3_tester_bind_meta_request(&tester, &options, &meta_request_test_results));

        struct aws_s3_meta_request *meta_request = aws_s3_client_make_meta_request(client, &options);
        ASSERT_TRUE(meta_request != NULL);
        /* Wait for the request to finish. */
        aws_s3_tester_wait_for_meta_request_finish(&tester);
        ASSERT_SUCCESS(aws_s3_tester_validate_put_object_results(&meta_request_test_results, 0));

        meta_request = aws_s3_meta_request_release(meta_request);
        aws_s3_tester_wait_for_meta_request_shutdown(&tester);
        aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);
        aws_http_message_release(message);
        aws_input_stream_release(upload_stream);

        /* Sleep for one mins before next request */
        if (i != num_meta_requests - 1) {
            aws_thread_current_sleep(aws_timestamp_convert(60, AWS_TIMESTAMP_SECS, AWS_TIMESTAMP_NANOS, NULL));
        }
    }

    /* More than two create session was invoked */
    /* Server can return a credentials that expires around 2-3 mins sometime. */
    size_t session_made = aws_atomic_load_int(&s_tester.provider_requests_made);
    ASSERT_TRUE(session_made >= 2);

    aws_s3_client_release(client);
    s_s3express_client_tester_cleanup();
    aws_s3_tester_clean_up(&tester);
    return AWS_OP_SUCCESS;
}

TEST_CASE(s3express_client_get_object) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_byte_cursor region_cursor = aws_byte_cursor_from_c_str("us-east-1");

    struct aws_s3_client_config client_config = {
        .part_size = MB_TO_BYTES(5),
        .enable_s3express = true,
        .region = region_cursor,
    };

    ASSERT_SUCCESS(aws_s3_tester_bind_client(&tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_SIGNING));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    struct aws_http_message *message = aws_s3_test_get_object_request_new(
        allocator, g_test_s3express_bucket_use1_az4_endpoint, g_pre_existing_object_10MB);

    struct aws_s3_meta_request_options options;
    AWS_ZERO_STRUCT(options);
    options.type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT;
    options.message = message;
    struct aws_signing_config_aws s3express_signing_config = {
        .algorithm = AWS_SIGNING_ALGORITHM_V4_S3EXPRESS,
        .service = g_s3express_service_name,
    };
    options.signing_config = &s3express_signing_config;
    struct aws_s3_meta_request_test_results meta_request_test_results;
    aws_s3_meta_request_test_results_init(&meta_request_test_results, allocator);

    ASSERT_SUCCESS(aws_s3_tester_bind_meta_request(&tester, &options, &meta_request_test_results));

    struct aws_s3_meta_request *meta_request = aws_s3_client_make_meta_request(client, &options);
    ASSERT_TRUE(meta_request != NULL);
    /* Wait for the request to finish. */
    aws_s3_tester_wait_for_meta_request_finish(&tester);

    ASSERT_SUCCESS(aws_s3_tester_validate_get_object_results(&meta_request_test_results, 0));
    meta_request = aws_s3_meta_request_release(meta_request);
    aws_s3_tester_wait_for_meta_request_shutdown(&tester);

    aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);

    aws_http_message_release(message);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);
    return AWS_OP_SUCCESS;
}

TEST_CASE(s3express_client_get_object_multiple) {
    (void)ctx;

    struct aws_s3_meta_request *meta_requests[100];
    struct aws_s3_meta_request_test_results meta_request_test_results[100];
    size_t num_meta_requests = AWS_ARRAY_SIZE(meta_requests);

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_byte_cursor region_cursor = aws_byte_cursor_from_c_str("us-east-1");

    struct aws_s3_client_config client_config = {
        .part_size = MB_TO_BYTES(5),
        .enable_s3express = true,
        .region = region_cursor,
    };

    ASSERT_SUCCESS(aws_s3_tester_bind_client(&tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_SIGNING));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    for (size_t i = 0; i < num_meta_requests; ++i) {

        struct aws_http_message *message = aws_s3_test_get_object_request_new(
            allocator, g_test_s3express_bucket_use1_az4_endpoint, g_pre_existing_object_10MB);

        struct aws_s3_meta_request_options options;
        AWS_ZERO_STRUCT(options);
        options.type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT;
        options.message = message;
        struct aws_signing_config_aws s3express_signing_config = {
            .algorithm = AWS_SIGNING_ALGORITHM_V4_S3EXPRESS,
            .service = g_s3express_service_name,
        };
        options.signing_config = &s3express_signing_config;
        aws_s3_meta_request_test_results_init(&meta_request_test_results[i], allocator);

        ASSERT_SUCCESS(aws_s3_tester_bind_meta_request(&tester, &options, &meta_request_test_results[i]));

        meta_requests[i] = aws_s3_client_make_meta_request(client, &options);
        ASSERT_TRUE(meta_requests[i] != NULL);

        aws_http_message_release(message);
    }
    /* Wait for the request to finish. */
    aws_s3_tester_wait_for_meta_request_finish(&tester);
    aws_s3_tester_lock_synced_data(&tester);
    ASSERT_TRUE(tester.synced_data.finish_error_code == AWS_ERROR_SUCCESS);
    aws_s3_tester_unlock_synced_data(&tester);

    for (size_t i = 0; i < num_meta_requests; ++i) {
        meta_requests[i] = aws_s3_meta_request_release(meta_requests[i]);
    }

    aws_s3_tester_wait_for_meta_request_shutdown(&tester);

    for (size_t i = 0; i < num_meta_requests; ++i) {
        aws_s3_tester_validate_get_object_results(&meta_request_test_results[i], 0);
        aws_s3_meta_request_test_results_clean_up(&meta_request_test_results[i]);
    }

    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);
    return AWS_OP_SUCCESS;
}

TEST_CASE(s3express_client_get_object_create_session_error) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_byte_cursor region_cursor = aws_byte_cursor_from_c_str("us-east-1");

    struct aws_s3_client_config client_config = {
        .part_size = MB_TO_BYTES(5),
        .enable_s3express = true,
        .region = region_cursor,
    };

    ASSERT_SUCCESS(aws_s3_tester_bind_client(&tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_SIGNING));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    struct aws_byte_cursor my_dummy_endpoint = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL(
        "non-exist-bucket-test--use1-az4--x-s3.s3express-use1-az4.us-east-1.amazonaws.com");

    struct aws_http_message *message =
        aws_s3_test_get_object_request_new(allocator, my_dummy_endpoint, g_pre_existing_object_10MB);

    struct aws_s3_meta_request_options options;
    AWS_ZERO_STRUCT(options);
    options.type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT;
    options.message = message;
    struct aws_signing_config_aws s3express_signing_config = {
        .algorithm = AWS_SIGNING_ALGORITHM_V4_S3EXPRESS,
        .service = g_s3express_service_name,
    };
    options.signing_config = &s3express_signing_config;
    struct aws_s3_meta_request_test_results meta_request_test_results;
    aws_s3_meta_request_test_results_init(&meta_request_test_results, allocator);

    ASSERT_SUCCESS(aws_s3_tester_bind_meta_request(&tester, &options, &meta_request_test_results));

    struct aws_s3_meta_request *meta_request = aws_s3_client_make_meta_request(client, &options);
    ASSERT_TRUE(meta_request != NULL);
    /* Wait for the request to finish. */
    aws_s3_tester_wait_for_meta_request_finish(&tester);
    ASSERT_UINT_EQUALS(meta_request_test_results.finished_error_code, AWS_ERROR_S3EXPRESS_CREATE_SESSION_FAILED);

    meta_request = aws_s3_meta_request_release(meta_request);
    aws_s3_tester_wait_for_meta_request_shutdown(&tester);

    aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);

    aws_http_message_release(message);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);
    return AWS_OP_SUCCESS;
}
