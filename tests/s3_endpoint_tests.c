/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_client_impl.h"
#include "s3_tester.h"
#include <aws/io/channel_bootstrap.h>
#include <aws/testing/aws_test_harness.h>

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
        aws_s3_meta_request_test_results_init(&meta_request_test_results, allocator);

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

        ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &options, &meta_request_test_results));
        aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);
    }

    {
        struct aws_s3_meta_request_test_results meta_request_test_results;
        aws_s3_meta_request_test_results_init(&meta_request_test_results, allocator);

        struct aws_s3_tester_meta_request_options options = {
            .allocator = allocator,
            .client = client,
            .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
            .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_SUCCESS,
            .bucket_name = &g_test_public_bucket_name,
            .get_options =
                {
                    .object_path = g_pre_existing_object_1MB,
                },
        };

        ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &options, &meta_request_test_results));
        aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);
    }

    aws_s3_client_release(client);

    aws_s3_tester_clean_up(&tester);

    return 0;
}
