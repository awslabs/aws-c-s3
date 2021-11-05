/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_client_impl.h"
#include "aws/s3/private/s3_file_system_support.h"
#include "s3_tester.h"

#include <aws/io/channel_bootstrap.h>
#include <aws/testing/aws_test_harness.h>

static int s_test_s3_list_bucket_init_mem_safety(struct aws_allocator *allocator, void *ctx) {
    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_tester_client_options client_options;
    AWS_ZERO_STRUCT(client_options);

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_s3_list_bucket_v2_params params = {
        .client = client,
        .endpoint = aws_byte_cursor_from_c_str("test-endpoint.com"),
        .bucket_name = aws_byte_cursor_from_c_str("test-bucket"),
    };

    struct aws_s3_paginator *paginator = aws_s3_initiate_list_bucket(allocator, &params);
    ASSERT_NOT_NULL(paginator);

    aws_s3_paginator_release(paginator);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(test_s3_list_bucket_init_mem_safety, s_test_s3_list_bucket_init_mem_safety)

static int s_test_s3_list_bucket_init_mem_safety_optional_copies(struct aws_allocator *allocator, void *ctx) {
    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_tester_client_options client_options;
    AWS_ZERO_STRUCT(client_options);

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_s3_list_bucket_v2_params params = {
        .client = client,
        .endpoint = aws_byte_cursor_from_c_str("test-endpoint.com"),
        .bucket_name = aws_byte_cursor_from_c_str("test-bucket"),
        .prefix = aws_byte_cursor_from_c_str("foo/bar"),
        .delimiter = aws_byte_cursor_from_c_str("/"),
        .continuation_token = aws_byte_cursor_from_c_str("base64_encrypted_thing"),
    };

    struct aws_s3_paginator *paginator = aws_s3_initiate_list_bucket(allocator, &params);
    ASSERT_NOT_NULL(paginator);

    aws_s3_paginator_release(paginator);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(
    test_s3_list_bucket_init_mem_safety_optional_copies,
    s_test_s3_list_bucket_init_mem_safety_optional_copies)

/*
static const char *s_prefix_and_delimiter_happy_path_sample =
    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
    "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n"
    "  <Name>example-bucket</Name>\n"
    "  <Prefix></Prefix>\n"
    "  <KeyCount>2</KeyCount>\n"
    "  <MaxKeys>1000</MaxKeys>\n"
    "  <Delimiter>/</Delimiter>\n"
    "  <IsTruncated>false</IsTruncated>\n"
    "  <Contents>\n"
    "    <Key>sample.jpg</Key>\n"
    "    <LastModified>2011-02-26T01:56:20.000Z</LastModified>\n"
    "    <ETag>\"bf1d737a4d46a19f3bced6905cc8b902\"</ETag>\n"
    "    <Size>142863</Size>\n"
    "    <StorageClass>STANDARD</StorageClass>\n"
    "  </Contents>\n"
    "  <CommonPrefixes>\n"
    "    <Prefix>photos/</Prefix>\n"
    "  </CommonPrefixes>\n"
    "</ListBucketResult>\t";
    */
