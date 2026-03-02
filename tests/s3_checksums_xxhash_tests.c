/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */
#include "aws/s3/private/s3_checksums.h"
#include <aws/cal/hash.h>
#include <aws/common/byte_buf.h>
#include <aws/testing/aws_test_harness.h>

#include <s3_checksums_test_case_helper.h>
/*
 * There is no standard test vectors for xxhash.
 * Just test that piping is correct.
 */

AWS_TEST_CASE(xxhash64_test_piping, s_xxhash64_test_piping)
static int s_xxhash64_test_piping(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_byte_cursor input = aws_byte_cursor_from_c_str("abc");
    uint8_t expected[] = {0x44, 0xbc, 0x2c, 0xf5, 0xad, 0x77, 0x09, 0x99};
    struct aws_byte_cursor expected_buf = aws_byte_cursor_from_array(expected, sizeof(expected));

    return s_verify_checksum_test_case(allocator, &input, &expected_buf, aws_checksum_new, AWS_SCA_XXHASH64);
}

AWS_TEST_CASE(xxhash3_64_test_piping, s_xxhash3_64_test_piping)
static int s_xxhash3_64_test_piping(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_byte_cursor input = aws_byte_cursor_from_c_str("abc");
    uint8_t expected[] = {0xcb, 0x99, 0x6e, 0x0a, 0x89, 0x42, 0xbf, 0xd5};
    struct aws_byte_cursor expected_buf = aws_byte_cursor_from_array(expected, sizeof(expected));

    return s_verify_checksum_test_case(allocator, &input, &expected_buf, aws_checksum_new, AWS_SCA_XXHASH3_64);
}

AWS_TEST_CASE(xxhash3_128_test_piping, s_xxhash3_128_test_piping)
static int s_xxhash3_128_test_piping(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_byte_cursor input = aws_byte_cursor_from_c_str("abc");
    uint8_t expected[] = {
        0x2b, 0x2d, 0xc1, 0x90, 0x6c, 0x54, 0x1b, 0x29, 0x25, 0x60, 0x07, 0xe0, 0x2e, 0x49, 0x43, 0x39};
    struct aws_byte_cursor expected_buf = aws_byte_cursor_from_array(expected, sizeof(expected));

    return s_verify_checksum_test_case(allocator, &input, &expected_buf, aws_checksum_new, AWS_SCA_XXHASH3_128);
}
