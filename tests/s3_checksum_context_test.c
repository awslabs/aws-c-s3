/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_checksum_context.h"
#include "aws/s3/private/s3_checksums.h"
#include <aws/common/byte_buf.h>
#include <aws/testing/aws_test_harness.h>

static int s_test_upload_request_checksum_context_creation(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    /* Test 1: Create context with no checksum config */
    struct aws_s3_upload_request_checksum_context *context =
        aws_s3_upload_request_checksum_context_new(allocator, NULL);
    ASSERT_NOT_NULL(context);
    ASSERT_INT_EQUALS(AWS_SCA_NONE, context->algorithm);
    ASSERT_INT_EQUALS(AWS_SCL_NONE, context->location);
    ASSERT_FALSE(context->checksum_calculated);
    aws_s3_upload_request_checksum_context_release(context);

    /* Test 2: Create context with checksum config */
    struct aws_s3_meta_request_checksum_config_storage config = {
        .allocator = allocator,
        .checksum_algorithm = AWS_SCA_CRC32,
        .location = AWS_SCL_HEADER,
        .has_full_object_checksum = false,
    };
    AWS_ZERO_STRUCT(config.full_object_checksum);

    context = aws_s3_upload_request_checksum_context_new(allocator, &config);
    ASSERT_NOT_NULL(context);
    ASSERT_INT_EQUALS(AWS_SCA_CRC32, context->algorithm);
    ASSERT_INT_EQUALS(AWS_SCL_HEADER, context->location);
    ASSERT_FALSE(context->checksum_calculated);
    ASSERT_TRUE(context->encoded_checksum_size > 0);
    aws_s3_upload_request_checksum_context_release(context);

    /* Test 3: Create context with existing checksum */
    struct aws_byte_cursor existing_checksum = aws_byte_cursor_from_c_str("test_checksum");
    context = aws_s3_upload_request_checksum_context_new_with_exist_checksum(allocator, &config, existing_checksum);
    ASSERT_NOT_NULL(context);
    ASSERT_INT_EQUALS(AWS_SCA_CRC32, context->algorithm);
    ASSERT_INT_EQUALS(AWS_SCL_HEADER, context->location);
    ASSERT_TRUE(context->checksum_calculated);
    aws_s3_upload_request_checksum_context_release(context);

    return AWS_OP_SUCCESS;
}

static int s_test_upload_request_checksum_context_helper_functions(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    /* Test helper functions with header checksum */
    struct aws_s3_meta_request_checksum_config_storage config = {
        .allocator = allocator,
        .checksum_algorithm = AWS_SCA_CRC32,
        .location = AWS_SCL_HEADER,
        .has_full_object_checksum = false,
    };
    AWS_ZERO_STRUCT(config.full_object_checksum);

    struct aws_s3_upload_request_checksum_context *context =
        aws_s3_upload_request_checksum_context_new(allocator, &config);
    ASSERT_NOT_NULL(context);

    ASSERT_TRUE(aws_s3_upload_request_checksum_context_should_add_header(context));
    ASSERT_FALSE(aws_s3_upload_request_checksum_context_should_add_trailer(context));
    ASSERT_TRUE(aws_s3_upload_request_checksum_context_should_calculate(context));

    aws_s3_upload_request_checksum_context_release(context);

    /* Test helper functions with trailer checksum */
    config.location = AWS_SCL_TRAILER;
    context = aws_s3_upload_request_checksum_context_new(allocator, &config);
    ASSERT_NOT_NULL(context);

    ASSERT_FALSE(aws_s3_upload_request_checksum_context_should_add_header(context));
    ASSERT_TRUE(aws_s3_upload_request_checksum_context_should_add_trailer(context));
    ASSERT_TRUE(aws_s3_upload_request_checksum_context_should_calculate(context));

    aws_s3_upload_request_checksum_context_release(context);

    /* Test helper functions with no checksum */
    config.location = AWS_SCL_NONE;
    context = aws_s3_upload_request_checksum_context_new(allocator, &config);
    ASSERT_NOT_NULL(context);

    ASSERT_FALSE(aws_s3_upload_request_checksum_context_should_add_header(context));
    ASSERT_FALSE(aws_s3_upload_request_checksum_context_should_add_trailer(context));
    ASSERT_FALSE(aws_s3_upload_request_checksum_context_should_calculate(context));

    aws_s3_upload_request_checksum_context_release(context);

    return AWS_OP_SUCCESS;
}

static int s_test_upload_request_checksum_context_reference_counting(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_meta_request_checksum_config_storage config = {
        .allocator = allocator,
        .checksum_algorithm = AWS_SCA_CRC32,
        .location = AWS_SCL_HEADER,
        .has_full_object_checksum = false,
    };
    AWS_ZERO_STRUCT(config.full_object_checksum);

    /* Test reference counting */
    struct aws_s3_upload_request_checksum_context *context =
        aws_s3_upload_request_checksum_context_new(allocator, &config);
    ASSERT_NOT_NULL(context);

    /* Acquire additional reference */
    struct aws_s3_upload_request_checksum_context *context2 = aws_s3_upload_request_checksum_context_acquire(context);
    ASSERT_PTR_EQUALS(context, context2);

    /* Release both references */
    aws_s3_upload_request_checksum_context_release(context);
    aws_s3_upload_request_checksum_context_release(context2);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(test_upload_request_checksum_context_creation, s_test_upload_request_checksum_context_creation)
AWS_TEST_CASE(
    test_upload_request_checksum_context_helper_functions,
    s_test_upload_request_checksum_context_helper_functions)
AWS_TEST_CASE(
    test_upload_request_checksum_context_reference_counting,
    s_test_upload_request_checksum_context_reference_counting)
