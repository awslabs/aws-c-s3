/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_client_impl.h"
#include "s3_tester.h"
#include <aws/io/channel_bootstrap.h>
#include <aws/testing/aws_test_harness.h>

static void s_test_s3_endpoint_resurrect_endpoint_ref_count_zero(struct aws_s3_endpoint *endpoint) {
    struct aws_s3_client *client = endpoint->user_data;
    struct aws_s3_tester *tester = client->shutdown_callback_user_data;

    bool acquire_and_release_endpoint = false;

    if (aws_s3_tester_inc_counter1(tester) == 1) {
        acquire_and_release_endpoint = true;
        aws_s3_endpoint_acquire(endpoint);
    }

    struct aws_s3_client_vtable *original_client_vtable =
        aws_s3_tester_get_client_vtable_patch(tester, 0)->original_vtable;

    original_client_vtable->endpoint_ref_count_zero(endpoint);

    if (acquire_and_release_endpoint) {
        aws_s3_endpoint_release(endpoint);
    }
}

AWS_TEST_CASE(test_s3_endpoint_resurrect, s_test_s3_endpoint_resurrect)
static int s_test_s3_endpoint_resurrect(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester, false));

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
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester, false));

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
