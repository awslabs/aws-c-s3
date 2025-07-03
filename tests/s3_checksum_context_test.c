/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_checksum_context.h"
#include "aws/s3/private/s3_checksums.h"
#include <aws/common/byte_buf.h>
#include <aws/testing/aws_test_harness.h>

static int s_test_upload_request_checksum_context_get_output_buffer(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_meta_request_checksum_config_storage config = {
        .allocator = allocator,
        .checksum_algorithm = AWS_SCA_CRC32,
        .location = AWS_SCL_HEADER,
        .has_full_object_checksum = false,
    };
    AWS_ZERO_STRUCT(config.full_object_checksum);

    /* Test get output buffer with valid context */
    struct aws_s3_upload_request_checksum_context context;
    ASSERT_SUCCESS(aws_s3_upload_request_checksum_context_init(allocator, &context, &config));

    struct aws_byte_buf *output_buffer = aws_s3_upload_request_checksum_context_get_output_buffer(&context);
    ASSERT_NOT_NULL(output_buffer);
    ASSERT_TRUE(output_buffer->capacity > 0);

    aws_s3_upload_request_checksum_context_clean_up(&context);

    /* Test get output buffer with NULL context */
    output_buffer = aws_s3_upload_request_checksum_context_get_output_buffer(NULL);
    ASSERT_NULL(output_buffer);

    return AWS_OP_SUCCESS;
}
AWS_TEST_CASE(
    test_upload_request_checksum_context_get_output_buffer,
    s_test_upload_request_checksum_context_get_output_buffer)

static int s_test_upload_request_checksum_context_get_checksum_cursor(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_meta_request_checksum_config_storage config = {
        .allocator = allocator,
        .checksum_algorithm = AWS_SCA_CRC32,
        .location = AWS_SCL_HEADER,
        .has_full_object_checksum = false,
    };
    AWS_ZERO_STRUCT(config.full_object_checksum);

    /* Test get checksum cursor with context that has no calculated checksum */
    struct aws_s3_upload_request_checksum_context context;
    ASSERT_SUCCESS(aws_s3_upload_request_checksum_context_init(allocator, &context, &config));

    struct aws_byte_cursor cursor = aws_s3_upload_request_checksum_context_get_checksum_cursor(&context);
    ASSERT_TRUE(cursor.len == 0);
    ASSERT_NULL(cursor.ptr);

    aws_s3_upload_request_checksum_context_clean_up(&context);

    /* Test get checksum cursor with context that has calculated checksum */
    struct aws_byte_cursor existing_checksum = aws_byte_cursor_from_c_str("dGVzdA==");
    ASSERT_SUCCESS(aws_s3_upload_request_checksum_context_init_with_existing_checksum(
        allocator, &context, &config, existing_checksum));

    cursor = aws_s3_upload_request_checksum_context_get_checksum_cursor(&context);
    ASSERT_TRUE(cursor.len == existing_checksum.len);
    ASSERT_TRUE(aws_byte_cursor_eq(&cursor, &existing_checksum));

    aws_s3_upload_request_checksum_context_clean_up(&context);

    /* Test get checksum cursor with NULL context */
    cursor = aws_s3_upload_request_checksum_context_get_checksum_cursor(NULL);
    ASSERT_TRUE(cursor.len == 0);
    ASSERT_NULL(cursor.ptr);

    return AWS_OP_SUCCESS;
}
AWS_TEST_CASE(
    test_upload_request_checksum_context_get_checksum_cursor,
    s_test_upload_request_checksum_context_get_checksum_cursor)

static int s_test_upload_request_checksum_context_error_cases(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_meta_request_checksum_config_storage config = {
        .allocator = allocator,
        .checksum_algorithm = AWS_SCA_CRC32,
        .location = AWS_SCL_HEADER,
        .has_full_object_checksum = false,
    };
    AWS_ZERO_STRUCT(config.full_object_checksum);

    /* Test creation with mismatched checksum size */
    struct aws_byte_cursor wrong_size_checksum = aws_byte_cursor_from_c_str("short");
    struct aws_s3_upload_request_checksum_context context;
    ASSERT_FAILS(aws_s3_upload_request_checksum_context_init_with_existing_checksum(
        allocator, &context, &config, wrong_size_checksum));

    /* Test helper functions with NULL context */
    ASSERT_FALSE(aws_s3_upload_request_checksum_context_should_calculate(NULL));
    ASSERT_FALSE(aws_s3_upload_request_checksum_context_should_add_header(NULL));
    ASSERT_FALSE(aws_s3_upload_request_checksum_context_should_add_trailer(NULL));

    return AWS_OP_SUCCESS;
}
AWS_TEST_CASE(test_upload_request_checksum_context_error_cases, s_test_upload_request_checksum_context_error_cases)

static int s_test_upload_request_checksum_context_different_algorithms(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    /* Test different checksum algorithms */
    enum aws_s3_checksum_algorithm algorithms[] = {
        AWS_SCA_CRC32, AWS_SCA_CRC32C, AWS_SCA_SHA1, AWS_SCA_SHA256, AWS_SCA_CRC64NVME};

    for (size_t i = 0; i < AWS_ARRAY_SIZE(algorithms); ++i) {
        struct aws_s3_meta_request_checksum_config_storage config = {
            .allocator = allocator,
            .checksum_algorithm = algorithms[i],
            .location = AWS_SCL_HEADER,
            .has_full_object_checksum = false,
        };
        AWS_ZERO_STRUCT(config.full_object_checksum);

        struct aws_s3_upload_request_checksum_context context;
        ASSERT_SUCCESS(aws_s3_upload_request_checksum_context_init(allocator, &context, &config));
        ASSERT_INT_EQUALS(algorithms[i], context.algorithm);
        ASSERT_INT_EQUALS(AWS_SCL_HEADER, context.location);
        ASSERT_TRUE(context.encoded_checksum_size > 0);
        ASSERT_TRUE(aws_s3_upload_request_checksum_context_should_calculate(&context));
        ASSERT_TRUE(aws_s3_upload_request_checksum_context_should_add_header(&context));
        ASSERT_FALSE(aws_s3_upload_request_checksum_context_should_add_trailer(&context));

        aws_s3_upload_request_checksum_context_clean_up(&context);
    }

    return AWS_OP_SUCCESS;
}
AWS_TEST_CASE(
    test_upload_request_checksum_context_different_algorithms,
    s_test_upload_request_checksum_context_different_algorithms)
