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
 * these are the rfc1321 test vectors
 */

static int s_md5_rfc1321_test_case_1_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_byte_cursor input = aws_byte_cursor_from_c_str("");
    uint8_t expected[] = {
        0xd4,
        0x1d,
        0x8c,
        0xd9,
        0x8f,
        0x00,
        0xb2,
        0x04,
        0xe9,
        0x80,
        0x09,
        0x98,
        0xec,
        0xf8,
        0x42,
        0x7e,
    };
    struct aws_byte_cursor expected_buf = aws_byte_cursor_from_array(expected, sizeof(expected));

    return s_verify_checksum_test_case(allocator, &input, &expected_buf, aws_checksum_new, AWS_SCA_MD5);
}

AWS_TEST_CASE(md5_rfc1321_test_case_1, s_md5_rfc1321_test_case_1_fn)

static int s_md5_rfc1321_test_case_2_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_byte_cursor input = aws_byte_cursor_from_c_str("a");
    uint8_t expected[] = {
        0x0c,
        0xc1,
        0x75,
        0xb9,
        0xc0,
        0xf1,
        0xb6,
        0xa8,
        0x31,
        0xc3,
        0x99,
        0xe2,
        0x69,
        0x77,
        0x26,
        0x61,
    };
    struct aws_byte_cursor expected_buf = aws_byte_cursor_from_array(expected, sizeof(expected));

    return s_verify_checksum_test_case(allocator, &input, &expected_buf, aws_checksum_new, AWS_SCA_MD5);
}

AWS_TEST_CASE(md5_rfc1321_test_case_2, s_md5_rfc1321_test_case_2_fn)

static int s_md5_rfc1321_test_case_3_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_byte_cursor input = aws_byte_cursor_from_c_str("abc");
    uint8_t expected[] = {
        0x90,
        0x01,
        0x50,
        0x98,
        0x3c,
        0xd2,
        0x4f,
        0xb0,
        0xd6,
        0x96,
        0x3f,
        0x7d,
        0x28,
        0xe1,
        0x7f,
        0x72,
    };
    struct aws_byte_cursor expected_buf = aws_byte_cursor_from_array(expected, sizeof(expected));

    return s_verify_checksum_test_case(allocator, &input, &expected_buf, aws_checksum_new, AWS_SCA_MD5);
}

AWS_TEST_CASE(md5_rfc1321_test_case_3, s_md5_rfc1321_test_case_3_fn)

static int s_md5_rfc1321_test_case_4_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_byte_cursor input = aws_byte_cursor_from_c_str("message digest");
    uint8_t expected[] = {
        0xf9,
        0x6b,
        0x69,
        0x7d,
        0x7c,
        0xb7,
        0x93,
        0x8d,
        0x52,
        0x5a,
        0x2f,
        0x31,
        0xaa,
        0xf1,
        0x61,
        0xd0,
    };
    struct aws_byte_cursor expected_buf = aws_byte_cursor_from_array(expected, sizeof(expected));

    return s_verify_checksum_test_case(allocator, &input, &expected_buf, aws_checksum_new, AWS_SCA_MD5);
}

AWS_TEST_CASE(md5_rfc1321_test_case_4, s_md5_rfc1321_test_case_4_fn)

static int s_md5_rfc1321_test_case_5_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_byte_cursor input = aws_byte_cursor_from_c_str("abcdefghijklmnopqrstuvwxyz");
    uint8_t expected[] = {
        0xc3,
        0xfc,
        0xd3,
        0xd7,
        0x61,
        0x92,
        0xe4,
        0x00,
        0x7d,
        0xfb,
        0x49,
        0x6c,
        0xca,
        0x67,
        0xe1,
        0x3b,
    };
    struct aws_byte_cursor expected_buf = aws_byte_cursor_from_array(expected, sizeof(expected));

    return s_verify_checksum_test_case(allocator, &input, &expected_buf, aws_checksum_new, AWS_SCA_MD5);
}

AWS_TEST_CASE(md5_rfc1321_test_case_5, s_md5_rfc1321_test_case_5_fn)

static int s_md5_rfc1321_test_case_6_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_byte_cursor input =
        aws_byte_cursor_from_c_str("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789");
    uint8_t expected[] = {
        0xd1,
        0x74,
        0xab,
        0x98,
        0xd2,
        0x77,
        0xd9,
        0xf5,
        0xa5,
        0x61,
        0x1c,
        0x2c,
        0x9f,
        0x41,
        0x9d,
        0x9f,
    };
    struct aws_byte_cursor expected_buf = aws_byte_cursor_from_array(expected, sizeof(expected));

    return s_verify_checksum_test_case(allocator, &input, &expected_buf, aws_checksum_new, AWS_SCA_MD5);
}

AWS_TEST_CASE(md5_rfc1321_test_case_6, s_md5_rfc1321_test_case_6_fn)

static int s_md5_rfc1321_test_case_7_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_byte_cursor input = aws_byte_cursor_from_c_str("123456789012345678901234567890123456789012345"
                                                              "67890123456789012345678901234567890");
    uint8_t expected[] = {
        0x57,
        0xed,
        0xf4,
        0xa2,
        0x2b,
        0xe3,
        0xc9,
        0x55,
        0xac,
        0x49,
        0xda,
        0x2e,
        0x21,
        0x07,
        0xb6,
        0x7a,
    };
    struct aws_byte_cursor expected_buf = aws_byte_cursor_from_array(expected, sizeof(expected));

    return s_verify_checksum_test_case(allocator, &input, &expected_buf, aws_checksum_new, AWS_SCA_MD5);
}

AWS_TEST_CASE(md5_rfc1321_test_case_7, s_md5_rfc1321_test_case_7_fn)

static int s_md5_rfc1321_test_case_7_truncated_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_byte_cursor input = aws_byte_cursor_from_c_str("123456789012345678901234567890123456789012345"
                                                              "67890123456789012345678901234567890");
    uint8_t expected[] = {
        0x57,
        0xed,
        0xf4,
        0xa2,
        0x2b,
        0xe3,
        0xc9,
        0x55,
    };
    struct aws_byte_cursor expected_buf = aws_byte_cursor_from_array(expected, sizeof(expected));

    return s_verify_checksum_test_case(allocator, &input, &expected_buf, aws_checksum_new, AWS_SCA_MD5);
}

AWS_TEST_CASE(md5_rfc1321_test_case_7_truncated, s_md5_rfc1321_test_case_7_truncated_fn)

static int s_md5_verify_known_collision_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_s3_library_init(allocator);

    uint8_t message_1[] = {
        0xd1, 0x31, 0xdd, 0x02, 0xc5, 0xe6, 0xee, 0xc4, 0x69, 0x3d, 0x9a, 0x06, 0x98, 0xaf, 0xf9, 0x5c,
        0x2f, 0xca, 0xb5, 0x87, 0x12, 0x46, 0x7e, 0xab, 0x40, 0x04, 0x58, 0x3e, 0xb8, 0xfb, 0x7f, 0x89,
        0x55, 0xad, 0x34, 0x06, 0x09, 0xf4, 0xb3, 0x02, 0x83, 0xe4, 0x88, 0x83, 0x25, 0x71, 0x41, 0x5a,
        0x08, 0x51, 0x25, 0xe8, 0xf7, 0xcd, 0xc9, 0x9f, 0xd9, 0x1d, 0xbd, 0xf2, 0x80, 0x37, 0x3c, 0x5b,
        0xd8, 0x82, 0x3e, 0x31, 0x56, 0x34, 0x8f, 0x5b, 0xae, 0x6d, 0xac, 0xd4, 0x36, 0xc9, 0x19, 0xc6,
        0xdd, 0x53, 0xe2, 0xb4, 0x87, 0xda, 0x03, 0xfd, 0x02, 0x39, 0x63, 0x06, 0xd2, 0x48, 0xcd, 0xa0,
        0xe9, 0x9f, 0x33, 0x42, 0x0f, 0x57, 0x7e, 0xe8, 0xce, 0x54, 0xb6, 0x70, 0x80, 0xa8, 0x0d, 0x1e,
        0xc6, 0x98, 0x21, 0xbc, 0xb6, 0xa8, 0x83, 0x93, 0x96, 0xf9, 0x65, 0x2b, 0x6f, 0xf7, 0x2a, 0x70,
    };

    uint8_t message_2[] = {
        0xd1, 0x31, 0xdd, 0x02, 0xc5, 0xe6, 0xee, 0xc4, 0x69, 0x3d, 0x9a, 0x06, 0x98, 0xaf, 0xf9, 0x5c,
        0x2f, 0xca, 0xb5, 0x07, 0x12, 0x46, 0x7e, 0xab, 0x40, 0x04, 0x58, 0x3e, 0xb8, 0xfb, 0x7f, 0x89,
        0x55, 0xad, 0x34, 0x06, 0x09, 0xf4, 0xb3, 0x02, 0x83, 0xe4, 0x88, 0x83, 0x25, 0xf1, 0x41, 0x5a,
        0x08, 0x51, 0x25, 0xe8, 0xf7, 0xcd, 0xc9, 0x9f, 0xd9, 0x1d, 0xbd, 0x72, 0x80, 0x37, 0x3c, 0x5b,
        0xd8, 0x82, 0x3e, 0x31, 0x56, 0x34, 0x8f, 0x5b, 0xae, 0x6d, 0xac, 0xd4, 0x36, 0xc9, 0x19, 0xc6,
        0xdd, 0x53, 0xe2, 0x34, 0x87, 0xda, 0x03, 0xfd, 0x02, 0x39, 0x63, 0x06, 0xd2, 0x48, 0xcd, 0xa0,
        0xe9, 0x9f, 0x33, 0x42, 0x0f, 0x57, 0x7e, 0xe8, 0xce, 0x54, 0xb6, 0x70, 0x80, 0x28, 0x0d, 0x1e,
        0xc6, 0x98, 0x21, 0xbc, 0xb6, 0xa8, 0x83, 0x93, 0x96, 0xf9, 0x65, 0xab, 0x6f, 0xf7, 0x2a, 0x70,
    };

    uint8_t collision_result[] = {
        0x79,
        0x05,
        0x40,
        0x25,
        0x25,
        0x5f,
        0xb1,
        0xa2,
        0x6e,
        0x4b,
        0xc4,
        0x22,
        0xae,
        0xf5,
        0x4e,
        0xb4,
    };

    uint8_t output1[AWS_MD5_LEN] = {0};
    struct aws_byte_buf output1_buf = aws_byte_buf_from_array(output1, sizeof(output1));
    output1_buf.len = 0;

    struct aws_byte_cursor message_1_buf = aws_byte_cursor_from_array(message_1, sizeof(message_1));

    ASSERT_SUCCESS(aws_checksum_compute(allocator, AWS_SCA_MD5, &message_1_buf, &output1_buf, 0));
    ASSERT_BIN_ARRAYS_EQUALS(collision_result, sizeof(collision_result), output1, sizeof(output1));

    uint8_t output2[AWS_MD5_LEN] = {0};
    struct aws_byte_buf output2_buf = aws_byte_buf_from_array(output2, sizeof(output2));
    output2_buf.len = 0;

    struct aws_byte_cursor message_2_buf = aws_byte_cursor_from_array(message_2, sizeof(message_2));

    ASSERT_SUCCESS(aws_checksum_compute(allocator, AWS_SCA_MD5, &message_2_buf, &output2_buf, 0));
    ASSERT_BIN_ARRAYS_EQUALS(collision_result, sizeof(collision_result), output2, sizeof(output2));

    aws_s3_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(md5_verify_known_collision, s_md5_verify_known_collision_fn)

static int s_md5_invalid_buffer_size_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_s3_library_init(allocator);

    struct aws_byte_cursor input = aws_byte_cursor_from_c_str("123456789012345678901234567890123456789012345"
                                                              "67890123456789012345678901234567890");

    uint8_t output[AWS_MD5_LEN] = {0};
    struct aws_byte_buf output_buf = aws_byte_buf_from_array(output, sizeof(output));
    output_buf.len = 1;

    ASSERT_ERROR(AWS_ERROR_SHORT_BUFFER, aws_checksum_compute(allocator, AWS_SCA_MD5, &input, &output_buf, 0));

    aws_s3_library_clean_up();

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(md5_invalid_buffer_size, s_md5_invalid_buffer_size_fn)

static int s_md5_test_invalid_state_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_s3_library_init(allocator);

    struct aws_byte_cursor input = aws_byte_cursor_from_c_str("123456789012345678901234567890123456789012345"
                                                              "67890123456789012345678901234567890");

    struct aws_s3_checksum *checksum = aws_checksum_new(allocator, AWS_SCA_MD5);
    ASSERT_NOT_NULL(checksum);

    uint8_t output[AWS_MD5_LEN] = {0};
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

AWS_TEST_CASE(md5_test_invalid_state, s_md5_test_invalid_state_fn)
