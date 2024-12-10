/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */
#include "aws/s3/private/s3_checksums.h"
#include <aws/common/byte_buf.h>
#include <aws/testing/aws_test_harness.h>

#include <s3_checksums_test_case_helper.h>
#define AWS_CRC64_LEN sizeof(uint64_t)

static int s_crc64nvme_nist_test_case_1_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_byte_cursor input = aws_byte_cursor_from_c_str("aaaaaaaaaa");
    uint8_t expected[] = {0x0C, 0x1A, 0x80, 0x03, 0x6D, 0x65, 0xC5, 0x55};
    struct aws_byte_cursor expected_buf = aws_byte_cursor_from_array(expected, sizeof(expected));

    return s_verify_checksum_test_case(allocator, &input, &expected_buf, aws_checksum_new, AWS_SCA_CRC64NVME);
}

AWS_TEST_CASE(crc64nvme_nist_test_case_1, s_crc64nvme_nist_test_case_1_fn)

static int s_crc64nvme_nist_test_case_2_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_byte_cursor input = aws_byte_cursor_from_c_str("");
    uint8_t expected[] = {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
    struct aws_byte_cursor expected_buf = aws_byte_cursor_from_array(expected, sizeof(expected));

    return s_verify_checksum_test_case(allocator, &input, &expected_buf, aws_checksum_new, AWS_SCA_CRC64NVME);
}

AWS_TEST_CASE(crc64nvme_nist_test_case_2, s_crc64nvme_nist_test_case_2_fn)

static int s_crc64nvme_nist_test_case_3_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_s3_library_init(allocator);

    struct aws_s3_checksum *checksum = aws_checksum_new(allocator, AWS_SCA_CRC64NVME);
    ASSERT_NOT_NULL(checksum);
    struct aws_byte_cursor input = aws_byte_cursor_from_c_str("a");

    for (size_t i = 0; i < 10; ++i) {
        ASSERT_SUCCESS(aws_checksum_update(checksum, &input));
    }

    uint8_t output[AWS_CRC64_LEN] = {0};
    struct aws_byte_buf output_buf = aws_byte_buf_from_array(output, sizeof(output));
    output_buf.len = 0;
    ASSERT_SUCCESS(aws_checksum_finalize(checksum, &output_buf));

    uint8_t expected[] = {0x0C, 0x1A, 0x80, 0x03, 0x6D, 0x65, 0xC5, 0x55};
    struct aws_byte_cursor expected_buf = aws_byte_cursor_from_array(expected, sizeof(expected));
    ASSERT_BIN_ARRAYS_EQUALS(expected_buf.ptr, expected_buf.len, output_buf.buffer, output_buf.len);

    aws_checksum_destroy(checksum);

    aws_s3_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(crc64nvme_nist_test_case_3, s_crc64nvme_nist_test_case_3_fn)

static int s_crc64nvme_nist_test_case_4_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_s3_library_init(allocator);

    struct aws_s3_checksum *checksum = aws_checksum_new(allocator, AWS_SCA_CRC64NVME);
    ASSERT_NOT_NULL(checksum);
    struct aws_byte_cursor input = aws_byte_cursor_from_c_str("aa");

    for (size_t i = 0; i < 5; ++i) {
        ASSERT_SUCCESS(aws_checksum_update(checksum, &input));
    }

    uint8_t output[AWS_CRC64_LEN] = {0};
    struct aws_byte_buf output_buf = aws_byte_buf_from_array(output, sizeof(output));
    output_buf.len = 0;
    ASSERT_SUCCESS(aws_checksum_finalize(checksum, &output_buf));

    uint8_t expected[] = {0x0C, 0x1A, 0x80, 0x03, 0x6D, 0x65, 0xC5, 0x55};

    struct aws_byte_cursor expected_buf = aws_byte_cursor_from_array(expected, sizeof(expected));
    ASSERT_BIN_ARRAYS_EQUALS(expected_buf.ptr, expected_buf.len, output_buf.buffer, output_buf.len);

    aws_checksum_destroy(checksum);

    aws_s3_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(crc64nvme_nist_test_case_4, s_crc64nvme_nist_test_case_4_fn)

static int s_crc64nvme_test_invalid_buffer_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_s3_library_init(allocator);

    struct aws_byte_cursor input = aws_byte_cursor_from_c_str("abcdefghbcdefghicdefghijdefghijkefghijklfghij"
                                                              "klmghijklmnhijklmnoijklmnopjklmnopqklm"
                                                              "nopqrlmnopqrsmnopqrstnopqrstu");
    uint8_t output[AWS_CRC64_LEN] = {0};
    struct aws_byte_buf output_buf = aws_byte_buf_from_array(output, sizeof(output));
    output_buf.len = 1;

    ASSERT_ERROR(AWS_ERROR_SHORT_BUFFER, aws_checksum_compute(allocator, AWS_SCA_CRC64NVME, &input, &output_buf));

    aws_s3_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(crc64nvme_test_invalid_buffer, s_crc64nvme_test_invalid_buffer_fn)

static int s_crc64nvme_test_invalid_state_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_s3_library_init(allocator);

    struct aws_byte_cursor input = aws_byte_cursor_from_c_str("abcdefghbcdefghicdefghijdefghijkefghijklfghij"
                                                              "klmghijklmnhijklmnoijklmnopjklmnopqklm"
                                                              "nopqrlmnopqrsmnopqrstnopqrstu");

    struct aws_s3_checksum *checksum = aws_checksum_new(allocator, AWS_SCA_CRC64NVME);
    ASSERT_NOT_NULL(checksum);

    uint8_t output[AWS_CRC64_LEN] = {0};
    struct aws_byte_buf output_buf = aws_byte_buf_from_array(output, sizeof(output));
    output_buf.len = 0;

    ASSERT_SUCCESS(aws_checksum_update(checksum, &input));
    ASSERT_SUCCESS(aws_checksum_finalize(checksum, &output_buf));
    ASSERT_ERROR(AWS_ERROR_INVALID_STATE, aws_checksum_update(checksum, &input));
    ASSERT_ERROR(AWS_ERROR_INVALID_STATE, aws_checksum_finalize(checksum, &output_buf));

    aws_checksum_destroy(checksum);

    aws_s3_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(crc64nvme_test_invalid_state, s_crc64nvme_test_invalid_state_fn)
