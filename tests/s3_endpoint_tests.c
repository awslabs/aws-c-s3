/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_client_impl.h"
#include "s3_tester.h"
#include <aws/io/channel_bootstrap.h>
#include <aws/testing/aws_test_harness.h>

static bool s_test_s3_endpoint_ref_zero_ref_callback(struct aws_s3_endpoint *endpoint) {
    struct aws_s3_tester *tester = endpoint->user_data;

    if (aws_s3_tester_inc_counter1(tester) == 1) {
        return false;
    }

    return true;
}

static void s_test_s3_endpoint_ref_shutdown(void *user_data) {
    struct aws_s3_tester *tester = user_data;

    aws_s3_tester_inc_counter1(tester);
}

AWS_TEST_CASE(test_s3_endpoint_ref, s_test_s3_endpoint_ref)
static int s_test_s3_endpoint_ref(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    aws_s3_tester_set_counter1_desired(&tester, 3);

    struct aws_string *host_name =
        aws_s3_tester_build_endpoint_string(allocator, &g_test_public_bucket_name, &g_test_s3_region);

    struct aws_s3_endpoint_options endpoint_options = {
        .host_name = host_name,
        .ref_count_zero_callback = s_test_s3_endpoint_ref_zero_ref_callback,
        .shutdown_callback = s_test_s3_endpoint_ref_shutdown,
        .client_bootstrap = tester.client_bootstrap,
        .tls_connection_options = NULL,
        .dns_host_address_ttl_seconds = 1,
        .user_data = &tester,
        .max_connections = 4,
    };

    struct aws_s3_endpoint *endpoint = aws_s3_endpoint_new(allocator, &endpoint_options);

    ASSERT_TRUE(endpoint->http_connection_manager != NULL);

    /* During the first release, s_test_s3_endpoint_ref_zero_ref_callback will return false, which will mean it does not
     * get cleaned up. */
    aws_s3_endpoint_release(endpoint);
    ASSERT_TRUE(aws_atomic_load_int(&endpoint->ref_count.ref_count) == 0);

    /* Perform an acquire, bumping the ref count to 1. */
    aws_s3_endpoint_acquire(endpoint);
    ASSERT_TRUE(aws_atomic_load_int(&endpoint->ref_count.ref_count) == 1);

    /* During the secnod release, s_test_s3_endpoint_ref_zero_ref_callback will return true, causing clean up to
     * occur.*/
    aws_s3_endpoint_release(endpoint);

    /* Wait for shutdown callback to increment the counter, hitting 3. */
    aws_s3_tester_wait_for_counters(&tester);

    aws_string_destroy(host_name);
    aws_s3_tester_clean_up(&tester);

    return 0;
}

static bool s_test_s3_endpoint_resurrect_endpoint_ref_count_zero(struct aws_s3_endpoint *endpoint) {
    struct aws_s3_client *client = endpoint->user_data;
    struct aws_s3_tester *tester = client->shutdown_callback_user_data;

    bool acquire_and_release_endpoint = false;

    if (aws_s3_tester_inc_counter1(tester) == 1) {
        acquire_and_release_endpoint = true;
        aws_s3_endpoint_acquire(endpoint);
    }

    struct aws_s3_client_vtable *original_client_vtable =
        aws_s3_tester_get_client_vtable_patch(tester, 0)->original_vtable;

    bool result = original_client_vtable->endpoint_ref_count_zero(endpoint);

    if (acquire_and_release_endpoint) {
        aws_s3_endpoint_release(endpoint);
    }

    return result;
}

AWS_TEST_CASE(test_s3_endpoint_resurrect, s_test_s3_endpoint_resurrect)
static int s_test_s3_endpoint_resurrect(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_tester_client_options client_options;
    AWS_ZERO_STRUCT(client_options);

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_s3_client_vtable *patched_client_vtable = aws_s3_tester_patch_client_vtable(&tester, client, NULL);
    patched_client_vtable->endpoint_ref_count_zero = s_test_s3_endpoint_resurrect_endpoint_ref_count_zero;

    struct aws_s3_tester_meta_request_options options = {
        .allocator = allocator,
        .client = client,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_SUCCESS,
        .get_options =
            {
                .object_path = g_pre_existing_object_1MB,
            },
    };

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &options, NULL));

    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

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

        struct aws_s3_tester_meta_request_options options = {
            .allocator = allocator,
            .client = client,
            .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
            .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_SUCCESS,
            .bucket_name = &g_test_public_bucket_name,
            .get_options =
                {
                    .object_path = g_s3_path_get_object_test_1MB,
                },
        };

        ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &options, &meta_request_test_results));
        aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);
    }

    aws_s3_client_release(client);

    aws_s3_tester_clean_up(&tester);

    return 0;
}
