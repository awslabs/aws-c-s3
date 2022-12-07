/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_util.h"
#include "aws/s3/s3_client.h"
#include "s3_tester.h"
#include <aws/io/uri.h>
#include <aws/testing/aws_test_harness.h>
#include <inttypes.h>

#define TEST_CASE(NAME)                                                                                                \
    AWS_TEST_CASE(NAME, s_test_##NAME);                                                                                \
    static int s_test_##NAME(struct aws_allocator *allocator, void *ctx)

#define DEFINE_HEADER(NAME, VALUE)                                                                                     \
    { .name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL(NAME), .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL(VALUE), }

TEST_CASE(multipart_upload_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_tester_client_options client_options = {
        .part_size = MB_TO_BYTES(5),
        .tls_usage = AWS_S3_TLS_DISABLED,
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

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
    };

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &put_options, NULL));

    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

TEST_CASE(async_internal_error_from_complete_multipart_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_tester_client_options client_options = {
        .part_size = MB_TO_BYTES(5),
        .tls_usage = AWS_S3_TLS_DISABLED,
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    /* Checkout the ./mock_s3_server/CompleteMultipartUpload/async_internal_error.json for the response details */
    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/async_internal_error");

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
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE,
    };
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &put_options, &out_results));

    /* Internal error will be retried and failed with internal error. */
    ASSERT_UINT_EQUALS(out_results.finished_error_code, AWS_ERROR_S3_INTERNAL_ERROR);

    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

TEST_CASE(async_access_denied_from_complete_multipart_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_tester_client_options client_options = {
        .part_size = MB_TO_BYTES(5),
        .tls_usage = AWS_S3_TLS_DISABLED,
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    /* Checkout the ./mock_s3_server/CompleteMultipartUpload/async_access_denied_error.json for the response details */
    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/async_access_denied_error");

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
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE,
    };
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &put_options, &out_results));

    ASSERT_UINT_EQUALS(out_results.finished_error_code, AWS_ERROR_S3_NON_RECOVERABLE_ASYNC_ERROR);
    ASSERT_UINT_EQUALS(out_results.headers_response_status, AWS_S3_RESPONSE_STATUS_SUCCESS);
    ASSERT_TRUE(out_results.error_response_body.len != 0);

    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}
