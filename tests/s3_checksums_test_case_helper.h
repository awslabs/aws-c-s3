/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */
#include "aws/s3/private/s3_checksums.h"
#include <aws/testing/aws_test_harness.h>

typedef struct aws_s3_checksum *aws_checksum_new_fn(
    struct aws_allocator *allocator,
    enum aws_s3_checksum_algorithm algorithm);

static inline int s_verify_checksum_test_case(
    struct aws_allocator *allocator,
    struct aws_byte_cursor *input,
    struct aws_byte_cursor *expected,
    aws_checksum_new_fn *new_fn,
    enum aws_s3_checksum_algorithm algorithm) {

    aws_s3_library_init(allocator);

    /* test all possible segmentation lengths from 1 byte at a time to the entire
     * input. */
    for (size_t i = 1; i < input->len; ++i) {
        uint8_t output[128] = {0};
        struct aws_byte_buf output_buf = aws_byte_buf_from_array(output, expected->len);
        output_buf.len = 0;

        struct aws_s3_checksum *checksum = new_fn(allocator, algorithm);
        ASSERT_NOT_NULL(checksum);

        struct aws_byte_cursor input_cpy = *input;

        while (input_cpy.len) {
            size_t max_advance = input_cpy.len > i ? i : input_cpy.len;
            struct aws_byte_cursor segment = aws_byte_cursor_from_array(input_cpy.ptr, max_advance);
            ASSERT_SUCCESS(aws_checksum_update(checksum, &segment));
            aws_byte_cursor_advance(&input_cpy, max_advance);
        }

        size_t truncation_size = checksum->digest_size - expected->len;

        ASSERT_SUCCESS(aws_checksum_finalize(checksum, &output_buf, truncation_size));
        ASSERT_BIN_ARRAYS_EQUALS(expected->ptr, expected->len, output_buf.buffer, output_buf.len);

        aws_checksum_destroy(checksum);
    }

    aws_s3_library_clean_up();

    return AWS_OP_SUCCESS;
}
