/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_checksum_context.h"
#include "aws/s3/private/s3_checksums.h"
#include <aws/common/byte_buf.h>
#include <aws/testing/aws_test_harness.h>

static int s_test_checksum_context_creation(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    /* Test 1: Create context with no checksum config */
    struct aws_s3_checksum_context *context = aws_s3_checksum_context_new(allocator, NULL, NULL);
    ASSERT_NOT_NULL(context);
    ASSERT_TRUE(context->is_valid);
    ASSERT_INT_EQUALS(AWS_SCA_NONE, context->algorithm);
    ASSERT_INT_EQUALS(AWS_SCL_NONE, context->location);
    ASSERT_INT_EQUALS(AWS_S3_CHECKSUM_BUFFER_NONE, context->buffer_state);
    aws_s3_checksum_context_destroy(context);

    /* Test 2: Create context with checksum config but no buffer */
    struct aws_s3_meta_request_checksum_config_storage config = {
        .allocator = allocator,
        .checksum_algorithm = AWS_SCA_CRC32,
        .location = AWS_SCL_HEADER,
        .has_full_object_checksum = false,
    };
    AWS_ZERO_STRUCT(config.full_object_checksum);

    context = aws_s3_checksum_context_new(allocator, &config, NULL);
    ASSERT_NOT_NULL(context);
    ASSERT_TRUE(context->is_valid);
    ASSERT_INT_EQUALS(AWS_SCA_CRC32, context->algorithm);
    ASSERT_INT_EQUALS(AWS_SCL_HEADER, context->location);
    ASSERT_INT_EQUALS(AWS_S3_CHECKSUM_BUFFER_NONE, context->buffer_state);
    ASSERT_TRUE(context->encoded_checksum_size > 0);
    aws_s3_checksum_context_destroy(context);

    /* Test 3: Create context with empty buffer */
    struct aws_byte_buf empty_buffer;
    aws_byte_buf_init(&empty_buffer, allocator, 16);

    context = aws_s3_checksum_context_new(allocator, &config, &empty_buffer);
    ASSERT_NOT_NULL(context);
    ASSERT_TRUE(context->is_valid);
    ASSERT_INT_EQUALS(AWS_S3_CHECKSUM_BUFFER_CALCULATE, context->buffer_state);
    ASSERT_PTR_EQUALS(&empty_buffer, context->checksum_buffer);
    aws_s3_checksum_context_destroy(context);
    aws_byte_buf_clean_up(&empty_buffer);

    return AWS_OP_SUCCESS;
}

static int s_test_checksum_context_helper_functions(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    /* Test helper functions with header checksum */
    struct aws_s3_meta_request_checksum_config_storage config = {
        .allocator = allocator,
        .checksum_algorithm = AWS_SCA_CRC32,
        .location = AWS_SCL_HEADER,
        .has_full_object_checksum = false,
    };
    AWS_ZERO_STRUCT(config.full_object_checksum);

    struct aws_s3_checksum_context *context = aws_s3_checksum_context_new(allocator, &config, NULL);
    ASSERT_NOT_NULL(context);

    ASSERT_TRUE(aws_s3_checksum_context_should_add_header(context));
    ASSERT_FALSE(aws_s3_checksum_context_should_add_trailer(context));
    ASSERT_TRUE(aws_s3_checksum_context_should_calculate(context));

    aws_s3_checksum_context_destroy(context);

    /* Test helper functions with trailer checksum */
    config.location = AWS_SCL_TRAILER;
    context = aws_s3_checksum_context_new(allocator, &config, NULL);
    ASSERT_NOT_NULL(context);

    ASSERT_FALSE(aws_s3_checksum_context_should_add_header(context));
    ASSERT_TRUE(aws_s3_checksum_context_should_add_trailer(context));
    ASSERT_TRUE(aws_s3_checksum_context_should_calculate(context));

    aws_s3_checksum_context_destroy(context);

    /* Test helper functions with no checksum */
    config.location = AWS_SCL_NONE;
    context = aws_s3_checksum_context_new(allocator, &config, NULL);
    ASSERT_NOT_NULL(context);

    ASSERT_FALSE(aws_s3_checksum_context_should_add_header(context));
    ASSERT_FALSE(aws_s3_checksum_context_should_add_trailer(context));
    ASSERT_FALSE(aws_s3_checksum_context_should_calculate(context));

    aws_s3_checksum_context_destroy(context);

    return AWS_OP_SUCCESS;
}

static int s_test_checksum_context_buffer_management(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_meta_request_checksum_config_storage config = {
        .allocator = allocator,
        .checksum_algorithm = AWS_SCA_CRC32,
        .location = AWS_SCL_HEADER,
        .has_full_object_checksum = false,
    };
    AWS_ZERO_STRUCT(config.full_object_checksum);

    /* Test getting output buffer when none provided */
    struct aws_s3_checksum_context *context = aws_s3_checksum_context_new(allocator, &config, NULL);
    ASSERT_NOT_NULL(context);

    struct aws_byte_buf *output_buffer = aws_s3_checksum_context_get_output_buffer(context);
    ASSERT_NOT_NULL(output_buffer);
    ASSERT_TRUE(context->owns_internal_buffer);

    aws_s3_checksum_context_destroy(context);

    /* Test getting output buffer when user buffer provided */
    struct aws_byte_buf user_buffer;
    aws_byte_buf_init(&user_buffer, allocator, 16);

    context = aws_s3_checksum_context_new(allocator, &config, &user_buffer);
    ASSERT_NOT_NULL(context);

    output_buffer = aws_s3_checksum_context_get_output_buffer(context);
    ASSERT_PTR_EQUALS(&user_buffer, output_buffer);
    ASSERT_FALSE(context->owns_internal_buffer);

    aws_s3_checksum_context_destroy(context);
    aws_byte_buf_clean_up(&user_buffer);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(test_checksum_context_creation, s_test_checksum_context_creation)
AWS_TEST_CASE(test_checksum_context_helper_functions, s_test_checksum_context_helper_functions)
AWS_TEST_CASE(test_checksum_context_buffer_management, s_test_checksum_context_buffer_management)
