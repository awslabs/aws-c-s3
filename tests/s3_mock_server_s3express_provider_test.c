/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_util.h"
#include "aws/s3/private/s3express_credentials_provider_impl.h"
#include "aws/s3/s3_client.h"
#include "aws/s3/s3express_credentials_provider.h"
#include "s3_tester.h"
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

static uint64_t s_bg_refresh_secs_override = 60;

struct aws_s3express_provider_tester {
    struct aws_allocator *allocator;

    struct aws_mutex lock;
    struct aws_condition_variable signal;

    size_t credentials_callbacks_received;
    bool has_received_shutdown_callback;

    /* Last received credentials */
    struct aws_credentials *credentials;

    /* Number of different credentials received */
    int number_of_credentials;

    struct aws_uri mock_server;
    struct aws_s3_client *client;

    int error_code;
};

static struct aws_s3express_provider_tester s_s3express_tester;

static void s_on_shutdown_complete(void *user_data) {
    (void)user_data;

    aws_mutex_lock(&s_s3express_tester.lock);
    s_s3express_tester.has_received_shutdown_callback = true;

    aws_condition_variable_notify_one(&s_s3express_tester.signal);
    aws_mutex_unlock(&s_s3express_tester.lock);
}

static bool s_has_s3express_tester_received_shutdown_callback(void *user_data) {
    (void)user_data;

    return s_s3express_tester.has_received_shutdown_callback;
}

static void s_aws_wait_for_provider_shutdown_callback(void) {
    aws_mutex_lock(&s_s3express_tester.lock);
    aws_condition_variable_wait_pred(
        &s_s3express_tester.signal, &s_s3express_tester.lock, s_has_s3express_tester_received_shutdown_callback, NULL);
    aws_mutex_unlock(&s_s3express_tester.lock);
}

static bool s_has_s3express_tester_received_credentials_callback(void *user_data) {
    size_t result_num = *(size_t *)user_data;

    return s_s3express_tester.credentials_callbacks_received >= result_num;
}

static void s_aws_wait_for_credentials_result(size_t result_num) {
    aws_mutex_lock(&s_s3express_tester.lock);
    aws_condition_variable_wait_pred(
        &s_s3express_tester.signal,
        &s_s3express_tester.lock,
        s_has_s3express_tester_received_credentials_callback,
        &result_num);
    aws_mutex_unlock(&s_s3express_tester.lock);
}

static void s_get_credentials_callback(struct aws_credentials *credentials, int error_code, void *user_data) {
    (void)user_data;

    aws_mutex_lock(&s_s3express_tester.lock);

    ++s_s3express_tester.credentials_callbacks_received;
    s_s3express_tester.error_code = error_code;

    if (credentials != s_s3express_tester.credentials) {
        ++s_s3express_tester.number_of_credentials;
        if (s_s3express_tester.credentials) {
            aws_credentials_release(s_s3express_tester.credentials);
        }
        s_s3express_tester.credentials = credentials;
        aws_credentials_acquire(credentials);
    }

    aws_condition_variable_notify_one(&s_s3express_tester.signal);
    aws_mutex_unlock(&s_s3express_tester.lock);
}

static int s_s3express_tester_init(struct aws_allocator *allocator) {
    s_s3express_tester.allocator = allocator;
    if (aws_mutex_init(&s_s3express_tester.lock)) {
        return AWS_OP_ERR;
    }
    if (aws_condition_variable_init(&s_s3express_tester.signal)) {
        return AWS_OP_ERR;
    }
    s_s3express_tester.error_code = AWS_ERROR_SUCCESS;

    ASSERT_SUCCESS(aws_uri_init_parse(&s_s3express_tester.mock_server, allocator, &g_mock_server_uri));
    return AWS_OP_SUCCESS;
}
static int s_s3express_tester_cleanup(void) {
    aws_condition_variable_clean_up(&s_s3express_tester.signal);
    aws_mutex_clean_up(&s_s3express_tester.lock);
    aws_uri_clean_up(&s_s3express_tester.mock_server);

    aws_s3_client_release(s_s3express_tester.client);
    aws_credentials_release(s_s3express_tester.credentials);
    return AWS_OP_SUCCESS;
}

static bool s_s3express_session_always_true(struct aws_s3express_session *session, uint64_t now_seconds) {
    (void)session;
    (void)now_seconds;
    return true;
}

static struct aws_s3express_credentials_provider *s_s3express_provider_new_mock_server(struct aws_s3_tester *tester) {

    struct aws_s3_tester_client_options client_options = {
        .part_size = MB_TO_BYTES(5),
        .tls_usage = AWS_S3_TLS_DISABLED,
    };

    if (s_s3express_tester.client == NULL) {
        if (aws_s3_tester_client_new(tester, &client_options, &s_s3express_tester.client)) {
            return NULL;
        }
    }

    struct aws_s3express_credentials_provider_default_options options = {
        .client = s_s3express_tester.client,
        .shutdown_complete_callback = s_on_shutdown_complete,
        .shutdown_user_data = &s_s3express_tester,
        .mock_test.bg_refresh_secs_override = s_bg_refresh_secs_override,
    };
    struct aws_s3express_credentials_provider *provider =
        aws_s3express_credentials_provider_new_default(tester->allocator, &options);
    struct aws_s3express_credentials_provider_impl *impl = provider->impl;
    impl->mock_test.endpoint_override = &s_s3express_tester.mock_server;
    impl->mock_test.s3express_session_is_valid_override = s_s3express_session_always_true;

    return provider;
}

TEST_CASE(s3express_provider_sanity_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    ASSERT_SUCCESS(s_s3express_tester_init(allocator));
    struct aws_s3express_credentials_provider *provider = s_s3express_provider_new_mock_server(&tester);
    ASSERT_NOT_NULL(provider);

    aws_s3express_credentials_provider_release(provider);
    s_aws_wait_for_provider_shutdown_callback();

    ASSERT_SUCCESS(s_s3express_tester_cleanup());
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

struct s3express_creds_from_ori_provider_user_data {
    struct aws_s3express_credentials_provider *s3express_provider;
    struct aws_credentials_properties_s3express *property;
    aws_on_get_credentials_callback_fn *callback;
    void *user_data;
};

static void s_get_original_credentials_callback(struct aws_credentials *credentials, int error_code, void *user_data) {
    struct s3express_creds_from_ori_provider_user_data *context = user_data;
    AWS_FATAL_ASSERT(error_code == AWS_ERROR_SUCCESS);

    error_code |= aws_s3express_credentials_provider_get_credentials(
        context->s3express_provider, credentials, context->property, context->callback, context->user_data);
    AWS_FATAL_ASSERT(error_code == AWS_ERROR_SUCCESS);

    aws_mem_release(context->s3express_provider->allocator, context);
}

static int s_tester_get_s3express_creds_from_ori_provider(
    struct aws_s3express_credentials_provider *s3express_provider,
    struct aws_credentials_properties_s3express *property,
    aws_on_get_credentials_callback_fn *callback,
    void *user_data,
    struct aws_credentials_provider *ori_provider) {
    struct s3express_creds_from_ori_provider_user_data *context =
        aws_mem_calloc(s3express_provider->allocator, 1, sizeof(struct s3express_creds_from_ori_provider_user_data));
    context->s3express_provider = s3express_provider;
    context->property = property;
    context->callback = callback;
    context->user_data = user_data;

    ASSERT_SUCCESS(
        aws_credentials_provider_get_credentials(ori_provider, s_get_original_credentials_callback, context));

    return AWS_OP_SUCCESS;
}

TEST_CASE(s3express_provider_get_credentials_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    ASSERT_SUCCESS(s_s3express_tester_init(allocator));

    struct aws_s3express_credentials_provider *provider = s_s3express_provider_new_mock_server(&tester);
    ASSERT_NOT_NULL(provider);

    struct aws_credentials_properties_s3express property = {
        .host = *aws_uri_authority(&s_s3express_tester.mock_server),
    };

    ASSERT_SUCCESS(aws_s3express_credentials_provider_get_credentials(
        provider, tester.anonymous_creds, &property, s_get_credentials_callback, &s_s3express_tester));

    s_aws_wait_for_credentials_result(1);
    ASSERT_SUCCESS(aws_s3_tester_check_s3express_creds_for_default_mock_response(s_s3express_tester.credentials));

    aws_s3express_credentials_provider_release(provider);
    s_aws_wait_for_provider_shutdown_callback();

    ASSERT_SUCCESS(s_s3express_tester_cleanup());
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

TEST_CASE(s3express_provider_get_credentials_multiple_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    ASSERT_SUCCESS(s_s3express_tester_init(allocator));

    struct aws_s3express_credentials_provider *provider = s_s3express_provider_new_mock_server(&tester);
    ASSERT_NOT_NULL(provider);
    size_t number_calls = 10;

    struct aws_credentials_properties_s3express property = {
        .host = *aws_uri_authority(&s_s3express_tester.mock_server),
    };
    for (size_t i = 0; i < number_calls; i++) {
        ASSERT_SUCCESS(aws_s3express_credentials_provider_get_credentials(
            provider, tester.anonymous_creds, &property, s_get_credentials_callback, &s_s3express_tester));
    }

    s_aws_wait_for_credentials_result(number_calls);
    ASSERT_SUCCESS(aws_s3_tester_check_s3express_creds_for_default_mock_response(s_s3express_tester.credentials));

    /* Only one credentials received as only one create session should be invoked. */
    ASSERT_UINT_EQUALS(1, s_s3express_tester.number_of_credentials);

    aws_s3express_credentials_provider_release(provider);
    s_aws_wait_for_provider_shutdown_callback();

    ASSERT_SUCCESS(s_s3express_tester_cleanup());
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

TEST_CASE(s3express_provider_get_credentials_cancel_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    ASSERT_SUCCESS(s_s3express_tester_init(allocator));
    struct aws_s3express_credentials_provider *provider = s_s3express_provider_new_mock_server(&tester);
    ASSERT_NOT_NULL(provider);

    struct aws_credentials_properties_s3express property = {
        .host = *aws_uri_authority(&s_s3express_tester.mock_server),
    };

    ASSERT_SUCCESS(aws_s3express_credentials_provider_get_credentials(
        provider, tester.anonymous_creds, &property, s_get_credentials_callback, &s_s3express_tester));
    /* Release the provider right after we fetch the credentials, which will cancel the create session call. */
    aws_s3express_credentials_provider_release(provider);
    s_aws_wait_for_provider_shutdown_callback();

    s_aws_wait_for_credentials_result(1);
    /* The error code will be AWS_ERROR_S3_CANCELED. */
    ASSERT_UINT_EQUALS(AWS_ERROR_S3_CANCELED, s_s3express_tester.error_code);
    ASSERT_SUCCESS(s_s3express_tester_cleanup());
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

static bool s_s3express_session_always_false(struct aws_s3express_session *session, uint64_t now_seconds) {
    (void)session;
    (void)now_seconds;
    return false;
}

TEST_CASE(s3express_provider_get_credentials_cache_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    ASSERT_SUCCESS(s_s3express_tester_init(allocator));

    struct aws_s3express_credentials_provider *provider = s_s3express_provider_new_mock_server(&tester);
    ASSERT_NOT_NULL(provider);
    struct aws_s3express_credentials_provider_impl *impl = provider->impl;

    struct aws_credentials_properties_s3express property = {
        .host = *aws_uri_authority(&s_s3express_tester.mock_server),
    };

    /* Makes one get credentials call. */
    ASSERT_SUCCESS(aws_s3express_credentials_provider_get_credentials(
        provider, tester.anonymous_creds, &property, s_get_credentials_callback, &s_s3express_tester));
    s_aws_wait_for_credentials_result(1);
    /* Only one credentials received as only one create session should be invoked. */
    ASSERT_UINT_EQUALS(1, s_s3express_tester.number_of_credentials);

    /* Let the mock always treat the session invalid, so, the cache will miss. */
    s_s3express_tester.credentials_callbacks_received = 0;
    impl->mock_test.s3express_session_is_valid_override = s_s3express_session_always_false;
    ASSERT_SUCCESS(aws_s3express_credentials_provider_get_credentials(
        provider, tester.anonymous_creds, &property, s_get_credentials_callback, &s_s3express_tester));
    s_aws_wait_for_credentials_result(1);
    /* We get the second credentials as we needs to create a new session for the invalid session */
    ASSERT_UINT_EQUALS(2, s_s3express_tester.number_of_credentials);

    /* Now the mock always treat the session valid, so, we will hit the cache. */
    s_s3express_tester.credentials_callbacks_received = 0;
    impl->mock_test.s3express_session_is_valid_override = s_s3express_session_always_true;
    ASSERT_SUCCESS(aws_s3express_credentials_provider_get_credentials(
        provider, tester.anonymous_creds, &property, s_get_credentials_callback, &s_s3express_tester));
    s_aws_wait_for_credentials_result(1);
    /* We still has only 2 credentials, as the cache hits and returns the same credentials back */
    ASSERT_UINT_EQUALS(2, s_s3express_tester.number_of_credentials);

    ASSERT_SUCCESS(aws_s3_tester_check_s3express_creds_for_default_mock_response(s_s3express_tester.credentials));

    aws_s3express_credentials_provider_release(provider);
    s_aws_wait_for_provider_shutdown_callback();

    ASSERT_SUCCESS(s_s3express_tester_cleanup());
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

TEST_CASE(s3express_provider_background_refresh_mock_server) {
    (void)ctx;

    s_bg_refresh_secs_override = 10;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    ASSERT_SUCCESS(s_s3express_tester_init(allocator));

    struct aws_s3express_credentials_provider *provider = s_s3express_provider_new_mock_server(&tester);
    ASSERT_NOT_NULL(provider);
    struct aws_s3express_credentials_provider_impl *impl = provider->impl;
    /* Always about to expire */
    impl->mock_test.s3express_session_about_to_expire_override = s_s3express_session_always_true;

    struct aws_credentials_properties_s3express property = {
        .host = *aws_uri_authority(&s_s3express_tester.mock_server),
    };

    /* Makes one get credentials call. */
    ASSERT_SUCCESS(s_tester_get_s3express_creds_from_ori_provider(
        provider, &property, s_get_credentials_callback, &s_s3express_tester, tester.credentials_provider));
    s_aws_wait_for_credentials_result(1);
    /* Only one credentials received as only one create session should be invoked. */
    ASSERT_UINT_EQUALS(1, s_s3express_tester.number_of_credentials);

    /* Before refresh, we will get the same creds as the cache returns the same creds back. */
    s_s3express_tester.credentials_callbacks_received = 0;
    ASSERT_SUCCESS(s_tester_get_s3express_creds_from_ori_provider(
        provider, &property, s_get_credentials_callback, &s_s3express_tester, tester.credentials_provider));
    s_aws_wait_for_credentials_result(1);
    ASSERT_UINT_EQUALS(1, s_s3express_tester.number_of_credentials);

    /* Sleep to wait for the background refresh happens */
    aws_thread_current_sleep(
        aws_timestamp_convert(s_bg_refresh_secs_override + 2, AWS_TIMESTAMP_SECS, AWS_TIMESTAMP_NANOS, NULL));

    s_s3express_tester.credentials_callbacks_received = 0;
    ASSERT_SUCCESS(s_tester_get_s3express_creds_from_ori_provider(
        provider, &property, s_get_credentials_callback, &s_s3express_tester, tester.credentials_provider));
    s_aws_wait_for_credentials_result(1);
    /* We have 2 credentials, even though we hit the same session, as the background refresh updated the credentials */
    ASSERT_UINT_EQUALS(2, s_s3express_tester.number_of_credentials);

    ASSERT_SUCCESS(aws_s3_tester_check_s3express_creds_for_default_mock_response(s_s3express_tester.credentials));

    aws_s3express_credentials_provider_release(provider);
    s_aws_wait_for_provider_shutdown_callback();

    ASSERT_SUCCESS(s_s3express_tester_cleanup());
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

/* Helper to get the index of the hash key from the cache. Returns SIZE_MAX if not found */
static size_t s_get_index_from_s3express_cache(
    struct aws_s3express_credentials_provider_impl *impl,
    const struct aws_credentials *original_credentials,
    struct aws_byte_cursor host_value) {

    { /* BEGIN CRITICAL SECTION */
        aws_mutex_lock(&impl->synced_data.lock);

        const struct aws_linked_list *session_list =
            aws_linked_hash_table_get_iteration_list(&impl->synced_data.cache->table);

        size_t index = 0;
        struct aws_linked_list_node *node = NULL;
        for (node = aws_linked_list_begin(session_list); node != aws_linked_list_end(session_list);) {
            struct aws_linked_hash_table_node *table_node =
                AWS_CONTAINER_OF(node, struct aws_linked_hash_table_node, node);
            node = aws_linked_list_next(node);
            struct aws_s3express_session *session = table_node->value;
            struct aws_string *hash_key =
                aws_encode_s3express_hash_key_new(s_s3express_tester.allocator, original_credentials, host_value);
            if (aws_string_eq(session->hash_key, hash_key)) {
                aws_string_destroy(hash_key);
                aws_mutex_unlock(&impl->synced_data.lock);
                return index;
            }
            aws_string_destroy(hash_key);
            ++index;
        }
        aws_mutex_unlock(&impl->synced_data.lock);
    } /* END CRITICAL SECTION */

    return SIZE_MAX;
}

TEST_CASE(s3express_provider_background_refresh_remove_inactive_creds_mock_server) {
    (void)ctx;

    s_bg_refresh_secs_override = 10;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    ASSERT_SUCCESS(s_s3express_tester_init(allocator));

    struct aws_s3_client_config client_config = {
        .part_size = MB_TO_BYTES(5),
        .tls_mode = AWS_MR_TLS_DISABLED,
        .signing_config = &tester.anonymous_signing_config,
    };

    ASSERT_SUCCESS(aws_s3_tester_bind_client(&tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION));

    s_s3express_tester.client = aws_s3_client_new(allocator, &client_config);

    struct aws_s3express_credentials_provider *provider = s_s3express_provider_new_mock_server(&tester);
    ASSERT_NOT_NULL(provider);
    struct aws_s3express_credentials_provider_impl *impl = provider->impl;
    /* Always about to expire */
    impl->mock_test.s3express_session_about_to_expire_override = s_s3express_session_always_true;

    struct aws_credentials_properties_s3express property_1 = {
        .host = aws_byte_cursor_from_c_str("bucket1"),
    };

    struct aws_credentials_properties_s3express property_2 = {
        .host = aws_byte_cursor_from_c_str("bucket2"),
    };
    ASSERT_SUCCESS(aws_s3express_credentials_provider_get_credentials(
        provider, tester.anonymous_creds, &property_1, s_get_credentials_callback, &s_s3express_tester));
    s_aws_wait_for_credentials_result(1);
    ASSERT_SUCCESS(aws_s3express_credentials_provider_get_credentials(
        provider, tester.anonymous_creds, &property_2, s_get_credentials_callback, &s_s3express_tester));
    s_aws_wait_for_credentials_result(2);

    /* Check the cache has two session */
    ASSERT_UINT_EQUALS(0, s_get_index_from_s3express_cache(impl, tester.anonymous_creds, property_1.host));
    ASSERT_UINT_EQUALS(1, s_get_index_from_s3express_cache(impl, tester.anonymous_creds, property_2.host));
    /* Sleep to wait for the background refresh happens */
    aws_thread_current_sleep(
        aws_timestamp_convert(s_bg_refresh_secs_override + 2, AWS_TIMESTAMP_SECS, AWS_TIMESTAMP_NANOS, NULL));
    /* Check the cache has two session */
    ASSERT_UINT_EQUALS(0, s_get_index_from_s3express_cache(impl, tester.anonymous_creds, property_1.host));
    ASSERT_UINT_EQUALS(1, s_get_index_from_s3express_cache(impl, tester.anonymous_creds, property_2.host));
    /* Use the first property to keep it active. */
    s_s3express_tester.credentials_callbacks_received = 0;
    ASSERT_SUCCESS(aws_s3express_credentials_provider_get_credentials(
        provider, tester.anonymous_creds, &property_1, s_get_credentials_callback, &s_s3express_tester));
    s_aws_wait_for_credentials_result(1);
    /* Now the first should be the second */
    ASSERT_UINT_EQUALS(1, s_get_index_from_s3express_cache(impl, tester.anonymous_creds, property_1.host));
    ASSERT_UINT_EQUALS(0, s_get_index_from_s3express_cache(impl, tester.anonymous_creds, property_2.host));
    /* Sleep to wait for another background refresh happens */
    aws_thread_current_sleep(
        aws_timestamp_convert(s_bg_refresh_secs_override, AWS_TIMESTAMP_SECS, AWS_TIMESTAMP_NANOS, NULL));
    /* Now we still have the second in the cache, but the first should gone */
    ASSERT_UINT_EQUALS(0, s_get_index_from_s3express_cache(impl, tester.anonymous_creds, property_1.host));
    ASSERT_UINT_EQUALS(SIZE_MAX, s_get_index_from_s3express_cache(impl, tester.anonymous_creds, property_2.host));

    aws_s3express_credentials_provider_release(provider);
    s_aws_wait_for_provider_shutdown_callback();

    ASSERT_SUCCESS(s_s3express_tester_cleanup());
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

TEST_CASE(s3express_provider_stress_mock_server) {
    (void)ctx;

    /* Make refresh happens very frequently */
    s_bg_refresh_secs_override = 1;
    size_t num_requests = 5000;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    ASSERT_SUCCESS(s_s3express_tester_init(allocator));

    struct aws_s3express_credentials_provider *provider = s_s3express_provider_new_mock_server(&tester);
    ASSERT_NOT_NULL(provider);
    struct aws_s3express_credentials_provider_impl *impl = provider->impl;
    /* Always about to expire */
    impl->mock_test.s3express_session_about_to_expire_override = s_s3express_session_always_true;

    /* Stress about under load, keep hitting 10 hosts */
    for (size_t i = 0; i < num_requests; i++) {
        char key_buffer[128] = "";
        snprintf(key_buffer, sizeof(key_buffer), "test-%zu", (size_t)(i % 10));
        struct aws_credentials_properties_s3express property = {
            .host = aws_byte_cursor_from_c_str(key_buffer),
        };
        ASSERT_SUCCESS(aws_s3express_credentials_provider_get_credentials(
            provider, tester.anonymous_creds, &property, s_get_credentials_callback, &s_s3express_tester));
    }
    s_aws_wait_for_credentials_result(num_requests);
    ASSERT_SUCCESS(aws_s3_tester_check_s3express_creds_for_default_mock_response(s_s3express_tester.credentials));

    /* Stress about over load, keep hitting different hosts */
    s_s3express_tester.credentials_callbacks_received = 0;
    for (size_t i = 0; i < num_requests; i++) {
        char key_buffer[128] = "";
        snprintf(key_buffer, sizeof(key_buffer), "test-%zu", i);
        struct aws_credentials_properties_s3express property = {
            .host = aws_byte_cursor_from_c_str(key_buffer),
        };
        ASSERT_SUCCESS(aws_s3express_credentials_provider_get_credentials(
            provider, tester.anonymous_creds, &property, s_get_credentials_callback, &s_s3express_tester));
    }
    s_aws_wait_for_credentials_result(num_requests);
    ASSERT_SUCCESS(aws_s3_tester_check_s3express_creds_for_default_mock_response(s_s3express_tester.credentials));

    aws_s3express_credentials_provider_release(provider);
    s_aws_wait_for_provider_shutdown_callback();

    ASSERT_SUCCESS(s_s3express_tester_cleanup());
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

TEST_CASE(s3express_provider_long_running_session_refresh) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    ASSERT_SUCCESS(s_s3express_tester_init(allocator));

    struct aws_byte_cursor region_cursor = aws_byte_cursor_from_c_str("us-east-1");

    struct aws_s3_client_config client_config = {
        .part_size = MB_TO_BYTES(5),
        .enable_s3express = true,
        .region = region_cursor,
    };

    ASSERT_SUCCESS(aws_s3_tester_bind_client(&tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_SIGNING));
    s_s3express_tester.client = aws_s3_client_new(allocator, &client_config);
    ASSERT_NOT_NULL(s_s3express_tester.client);

    struct aws_s3express_credentials_provider_default_options options = {
        .client = s_s3express_tester.client,
        .shutdown_complete_callback = s_on_shutdown_complete,
        .shutdown_user_data = &s_s3express_tester,
        .mock_test.bg_refresh_secs_override = 600, /* Disable the background refresh. */
    };
    struct aws_s3express_credentials_provider *provider =
        aws_s3express_credentials_provider_new_default(allocator, &options);
    ASSERT_NOT_NULL(provider);

    /* 300 secs to make sure we will refresh it at least once. */
    size_t num_requests = 600;

    struct aws_credentials_properties_s3express property = {
        .host = g_test_s3express_bucket_use1_az4_endpoint,
    };

    for (size_t i = 0; i < num_requests; i++) {
        ASSERT_SUCCESS(s_tester_get_s3express_creds_from_ori_provider(
            provider, &property, s_get_credentials_callback, &s_s3express_tester, tester.credentials_provider));
        s_aws_wait_for_credentials_result(i + 1);
        uint64_t expire_time_secs = aws_credentials_get_expiration_timepoint_seconds(s_s3express_tester.credentials);
        uint64_t current_stamp = UINT64_MAX;
        aws_sys_clock_get_ticks(&current_stamp);
        uint64_t now_seconds = aws_timestamp_convert(current_stamp, AWS_TIMESTAMP_NANOS, AWS_TIMESTAMP_SECS, NULL);
        /* We should always return a credentials being valid at least for 5 secs */
        ASSERT_TRUE(expire_time_secs > now_seconds + 5);

        /* Sleep for 0.5 sec */
        aws_thread_current_sleep(aws_timestamp_convert(500, AWS_TIMESTAMP_MILLIS, AWS_TIMESTAMP_NANOS, NULL));
    }
    /**
     * We should have more than 2 different creds.
     **/
    ASSERT_TRUE(s_s3express_tester.number_of_credentials >= 2);

    aws_s3express_credentials_provider_release(provider);
    s_aws_wait_for_provider_shutdown_callback();

    ASSERT_SUCCESS(s_s3express_tester_cleanup());
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}
