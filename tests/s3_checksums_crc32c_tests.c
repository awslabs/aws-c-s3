/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */
#include "aws/s3/private/s3_checksums.h"
#include <aws/common/byte_buf.h>
#include <aws/testing/aws_test_harness.h>

#include <s3_checksums_test_case_helper.h>

#define AWS_CRC32C_LEN 4
/*
 * these are the NIST test vectors, as compiled here:
 * https://www.di-mgt.com.au/sha_testvectors.html
 */

static int s_crc32c_nist_test_case_1_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_byte_cursor input = aws_byte_cursor_from_c_str("abc");
    uint8_t expected[] = {0x36, 0x4b, 0x3f, 0xb7};
    struct aws_byte_cursor expected_buf = aws_byte_cursor_from_array(expected, sizeof(expected));

    return s_verify_checksum_test_case(allocator, &input, &expected_buf, aws_checksum_new, AWS_SCA_CRC32C);
}

AWS_TEST_CASE(crc32c_nist_test_case_1, s_crc32c_nist_test_case_1_fn)

static int s_crc32c_nist_test_case_2_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_byte_cursor input = aws_byte_cursor_from_c_str("");
    uint8_t expected[] = {0x00, 0x00, 0x00, 0x00};
    struct aws_byte_cursor expected_buf = aws_byte_cursor_from_array(expected, sizeof(expected));

    return s_verify_checksum_test_case(allocator, &input, &expected_buf, aws_checksum_new, AWS_SCA_CRC32C);
}

AWS_TEST_CASE(crc32c_nist_test_case_2, s_crc32c_nist_test_case_2_fn)

static int s_crc32c_nist_test_case_3_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_byte_cursor input =
        aws_byte_cursor_from_c_str("abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq");
    uint8_t expected[] = {0x07, 0x13, 0x25, 0xf5};
    struct aws_byte_cursor expected_buf = aws_byte_cursor_from_array(expected, sizeof(expected));

    return s_verify_checksum_test_case(allocator, &input, &expected_buf, aws_checksum_new, AWS_SCA_CRC32C);
}

AWS_TEST_CASE(crc32c_nist_test_case_3, s_crc32c_nist_test_case_3_fn)

static int s_crc32c_nist_test_case_4_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_byte_cursor input = aws_byte_cursor_from_c_str("abcdefghbcdefghicdefghijdefghijkefghijklfghij"
                                                              "klmghijklmnhijklmnoijklmnopjklmnopqklm"
                                                              "nopqrlmnopqrsmnopqrstnopqrstu");
    uint8_t expected[] = {0x3f, 0x60, 0xa4, 0xb9};
    struct aws_byte_cursor expected_buf = aws_byte_cursor_from_array(expected, sizeof(expected));

    return s_verify_checksum_test_case(allocator, &input, &expected_buf, aws_checksum_new, AWS_SCA_CRC32C);
}

AWS_TEST_CASE(crc32c_nist_test_case_4, s_crc32c_nist_test_case_4_fn)

static int s_crc32c_nist_test_case_5_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_s3_library_init(allocator);

    struct aws_s3_checksum *checksum = aws_checksum_new(allocator, AWS_SCA_CRC32C);
    ASSERT_NOT_NULL(checksum);
    struct aws_byte_cursor input = aws_byte_cursor_from_c_str("a");

    for (size_t i = 0; i < 1000000; ++i) {
        ASSERT_SUCCESS(aws_checksum_update(checksum, &input));
    }

    uint8_t output[AWS_CRC32C_LEN] = {0};
    struct aws_byte_buf output_buf = aws_byte_buf_from_array(output, sizeof(output));
    output_buf.len = 0;
    ASSERT_SUCCESS(aws_checksum_finalize(checksum, &output_buf, 0));

    uint8_t expected[] = {0x43, 0x6f, 0xe2, 0x40};
    struct aws_byte_cursor expected_buf = aws_byte_cursor_from_array(expected, sizeof(expected));
    ASSERT_BIN_ARRAYS_EQUALS(expected_buf.ptr, expected_buf.len, output_buf.buffer, output_buf.len);

    aws_checksum_destroy(checksum);

    aws_s3_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(crc32c_nist_test_case_5, s_crc32c_nist_test_case_5_fn)

static int s_crc32c_nist_test_case_5_truncated_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_s3_library_init(allocator);

    struct aws_s3_checksum *checksum = aws_checksum_new(allocator, AWS_SCA_CRC32C);
    ASSERT_NOT_NULL(checksum);
    struct aws_byte_cursor input = aws_byte_cursor_from_c_str("a");

    for (size_t i = 0; i < 1000000; ++i) {
        ASSERT_SUCCESS(aws_checksum_update(checksum, &input));
    }

    uint8_t expected[] = {0x43, 0x6f};
    struct aws_byte_cursor expected_buf = aws_byte_cursor_from_array(expected, sizeof(expected));
    uint8_t output[AWS_CRC32C_LEN] = {0};
    struct aws_byte_buf output_buf = aws_byte_buf_from_array(output, expected_buf.len);
    output_buf.len = 0;
    ASSERT_SUCCESS(aws_checksum_finalize(checksum, &output_buf, 2));

    ASSERT_BIN_ARRAYS_EQUALS(expected_buf.ptr, expected_buf.len, output_buf.buffer, output_buf.len);

    aws_checksum_destroy(checksum);

    aws_s3_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(crc32c_nist_test_case_5_truncated, s_crc32c_nist_test_case_5_truncated_fn)

static int s_crc32c_nist_test_case_6_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_s3_library_init(allocator);

    struct aws_s3_checksum *checksum = aws_checksum_new(allocator, AWS_SCA_CRC32C);
    ASSERT_NOT_NULL(checksum);
    struct aws_byte_cursor input =
        aws_byte_cursor_from_c_str("abcdefghbcdefghicdefghijdefghijkefghijklfghijklmghijklmnhijklmno");

    for (size_t i = 0; i < 16777216; ++i) {
        ASSERT_SUCCESS(aws_checksum_update(checksum, &input));
    }

    uint8_t output[AWS_CRC32C_LEN] = {0};
    struct aws_byte_buf output_buf = aws_byte_buf_from_array(output, sizeof(output));
    output_buf.len = 0;
    ASSERT_SUCCESS(aws_checksum_finalize(checksum, &output_buf, 0));

    uint8_t expected[] = {0x0d, 0xcd, 0x03, 0xc6};

    struct aws_byte_cursor expected_buf = aws_byte_cursor_from_array(expected, sizeof(expected));
    ASSERT_BIN_ARRAYS_EQUALS(expected_buf.ptr, expected_buf.len, output_buf.buffer, output_buf.len);

    aws_checksum_destroy(checksum);

    aws_s3_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(crc32c_nist_test_case_6, s_crc32c_nist_test_case_6_fn)

static int s_crc32c_test_invalid_buffer_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_s3_library_init(allocator);

    struct aws_byte_cursor input = aws_byte_cursor_from_c_str("abcdefghbcdefghicdefghijdefghijkefghijklfghij"
                                                              "klmghijklmnhijklmnoijklmnopjklmnopqklm"
                                                              "nopqrlmnopqrsmnopqrstnopqrstu");
    uint8_t output[AWS_CRC32C_LEN] = {0};
    struct aws_byte_buf output_buf = aws_byte_buf_from_array(output, sizeof(output));
    output_buf.len = 1;

    ASSERT_ERROR(AWS_ERROR_SHORT_BUFFER, aws_checksum_compute(allocator, AWS_SCA_CRC32C, &input, &output_buf, 0));

    aws_s3_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(crc32c_test_invalid_buffer, s_crc32c_test_invalid_buffer_fn)

static int s_crc32c_test_oneshot_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_s3_library_init(allocator);

    struct aws_byte_cursor input = aws_byte_cursor_from_c_str("abcdefghbcdefghicdefghijdefghijkefghijklfghij"
                                                              "klmghijklmnhijklmnoijklmnopjklmnopqklm"
                                                              "nopqrlmnopqrsmnopqrstnopqrstu");
    uint8_t expected[] = {0x3f, 0x60, 0xa4, 0xb9};

    uint8_t output[AWS_CRC32C_LEN] = {0};
    struct aws_byte_buf output_buf = aws_byte_buf_from_array(output, sizeof(output));
    output_buf.len = 0;

    ASSERT_SUCCESS(aws_checksum_compute(allocator, AWS_SCA_CRC32C, &input, &output_buf, 0));
    ASSERT_BIN_ARRAYS_EQUALS(expected, sizeof(expected), output_buf.buffer, output_buf.len);

    aws_s3_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(crc32c_test_oneshot, s_crc32c_test_oneshot_fn)

static int s_crc32c_test_invalid_state_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_s3_library_init(allocator);

    struct aws_byte_cursor input = aws_byte_cursor_from_c_str("abcdefghbcdefghicdefghijdefghijkefghijklfghij"
                                                              "klmghijklmnhijklmnoijklmnopjklmnopqklm"
                                                              "nopqrlmnopqrsmnopqrstnopqrstu");

    struct aws_s3_checksum *checksum = aws_checksum_new(allocator, AWS_SCA_CRC32C);
    ASSERT_NOT_NULL(checksum);

    uint8_t output[AWS_CRC32C_LEN] = {0};
    struct aws_byte_buf output_buf = aws_byte_buf_from_array(output, sizeof(output));
    output_buf.len = 0;

    ASSERT_SUCCESS(aws_checksum_update(checksum, &input));
    ASSERT_SUCCESS(aws_checksum_finalize(checksum, &output_buf, 0));
    ASSERT_ERROR(AWS_ERROR_INVALID_STATE, aws_checksum_update(checksum, &input));
    ASSERT_ERROR(AWS_ERROR_INVALID_STATE, aws_checksum_finalize(checksum, &output_buf, 0));

    aws_checksum_destroy(checksum);

    aws_s3_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(crc32c_test_invalid_state, s_crc32c_test_invalid_state_fn)
