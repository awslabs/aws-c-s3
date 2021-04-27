/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/byte_buf.h>
#include <aws/s3/private/s3_checksum.h>
#include <aws/s3/s3_streaming_checksum.h>
#include <aws/testing/aws_test_harness.h>

static const uint8_t DATA_32_ZEROS[32] = {0};
static const uint32_t KNOWN_CRC32_32_ZEROES = 0x190A55AD;
static const uint32_t KNOWN_CRC32C_32_ZEROES = 0x8A9136AA;

static const uint8_t DATA_32_VALUES[32] = {0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15,
                                           16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31};
static const uint32_t KNOWN_CRC32_32_VALUES = 0x91267E8A;
static const uint32_t KNOWN_CRC32C_32_VALUES = 0x46DD794E;

static const uint8_t TEST_VECTOR[] = {'1', '2', '3', '4', '5', '6', '7', '8', '9'};
static const uint32_t KNOWN_CRC32_TEST_VECTOR = 0xCBF43926;
static const uint32_t KNOWN_CRC32C_TEST_VECTOR = 0xE3069283;

typedef struct aws_checksum *(crc_fn)(struct aws_allocator *);

static int s_byte_buf_to_int_buf(const struct aws_byte_buf *in_value, uint32_t *out_value) {
    struct aws_byte_cursor cursor_value = aws_byte_cursor_from_buf(in_value);
    if (aws_byte_cursor_read_be32(&cursor_value, out_value) == false) {
        return aws_raise_error(AWS_ERROR_UNKNOWN);
    }
    return AWS_OP_SUCCESS;
}

static int s_finalize(struct aws_checksum *checksum, uint32_t *out_value) {
    uint8_t output[4] = {0};
    struct aws_byte_buf output_buf = aws_byte_buf_from_array(output, sizeof(uint32_t));
    output_buf.len = 0;
    size_t truncation_size = checksum->digest_size - sizeof(uint32_t);
    ASSERT_SUCCESS(aws_checksum_finalize(checksum, &output_buf, truncation_size));
    return s_byte_buf_to_int_buf(&output_buf, out_value);
}

/* Makes sure that the specified crc function produces the expected results for known input and output*/
static int s_test_known_crc(
    const char *func_name,
    crc_fn *func,
    const char *data_name,
    const struct aws_byte_cursor *input,
    uint32_t expected,
    struct aws_allocator *allocator) {

    size_t len = (size_t)input->len;
    struct aws_checksum *crc = func(allocator);
    ASSERT_SUCCESS(aws_checksum_update(crc, input));
    // uintptr_t result = (uintptr_t)crc->impl;
    uint32_t result = 0;
    ASSERT_SUCCESS(s_finalize(crc, &result));
    ASSERT_HEX_EQUALS(expected, result, "%s(%s)", func_name, data_name);

    /* chain the crc computation so 2 calls each operate on about 1/2 of the buffer*/
    const struct aws_byte_cursor input_first_half = {
        .len = (int)(len / 2),
        .ptr = input->ptr,
    };
    const struct aws_byte_cursor input_second_half = {
        .len = (int)(len - len / 2),
        .ptr = (input->ptr) + (len / 2),
    };
    struct aws_checksum *crc1 = func(allocator);
    ASSERT_SUCCESS(aws_checksum_update(crc1, &input_first_half));
    ASSERT_SUCCESS(aws_checksum_update(crc1, &input_second_half));
    uintptr_t result1 = (uintptr_t)crc1->impl;
    ASSERT_HEX_EQUALS(expected, result1, "chaining %s(%s)", func_name, data_name);

    struct aws_checksum *crc2 = func(allocator);
    for (size_t i = 0; i < len; ++i) {
        const struct aws_byte_cursor input_i = {
            .len = 1,
            .ptr = input->ptr + i,
        };
        ASSERT_SUCCESS(aws_checksum_update(crc2, &input_i));
    }
    uintptr_t result2 = (uintptr_t)crc2->impl;
    ASSERT_HEX_EQUALS(expected, result2, "one byte at a time %s(%s)", func_name, data_name);
    aws_checksum_destroy(crc);
    aws_checksum_destroy(crc1);
    aws_checksum_destroy(crc2);
    return AWS_OP_SUCCESS;
}

/* helper function that groups crc32 tests*/
static int s_test_known_crc32(const char *func_name, crc_fn *func, struct aws_allocator *allocator) {
    uint8_t *mut_DATA_32_ZEROS = (uint8_t *)DATA_32_ZEROS;
    uint8_t *mut_DATA_32_VALUES = (uint8_t *)DATA_32_VALUES;
    uint8_t *mut_TEST_VECTOR = (uint8_t *)TEST_VECTOR;
    const struct aws_byte_cursor input = {
        .len = sizeof(DATA_32_ZEROS),
        .ptr = mut_DATA_32_ZEROS,
    };
    const struct aws_byte_cursor input1 = {
        .len = sizeof(DATA_32_VALUES),
        .ptr = mut_DATA_32_VALUES,
    };
    const struct aws_byte_cursor input2 = {
        .len = sizeof(TEST_VECTOR),
        .ptr = mut_TEST_VECTOR,
    };
    ASSERT_SUCCESS(s_test_known_crc(func_name, func, "DATA_32_ZEROS", &input, KNOWN_CRC32_32_ZEROES, allocator));
    ASSERT_SUCCESS(s_test_known_crc(func_name, func, "DATA_32_VALUES", &input1, KNOWN_CRC32_32_VALUES, allocator));
    ASSERT_SUCCESS(s_test_known_crc(func_name, func, "TEST_VECTOR", &input2, KNOWN_CRC32_TEST_VECTOR, allocator));
    return AWS_OP_SUCCESS;
}

/* helper function that groups crc32c tests*/
static int s_test_known_crc32c(const char *func_name, crc_fn *func, struct aws_allocator *allocator) {
    uint8_t *mut_DATA_32_ZEROS = (uint8_t *)DATA_32_ZEROS;
    uint8_t *mut_DATA_32_VALUES = (uint8_t *)DATA_32_VALUES;
    uint8_t *mut_TEST_VECTOR = (uint8_t *)TEST_VECTOR;
    const struct aws_byte_cursor input = {
        .len = sizeof(DATA_32_ZEROS),
        .ptr = mut_DATA_32_ZEROS,
    };
    const struct aws_byte_cursor input1 = {
        .len = sizeof(DATA_32_VALUES),
        .ptr = mut_DATA_32_VALUES,
    };
    const struct aws_byte_cursor input2 = {
        .len = sizeof(TEST_VECTOR),
        .ptr = mut_TEST_VECTOR,
    };
    ASSERT_SUCCESS(s_test_known_crc(func_name, func, "DATA_32_ZEROS", &input, KNOWN_CRC32C_32_ZEROES, allocator));
    ASSERT_SUCCESS(s_test_known_crc(func_name, func, "DATA_32_VALUES", &input1, KNOWN_CRC32C_32_VALUES, allocator));
    ASSERT_SUCCESS(s_test_known_crc(func_name, func, "TEST_VECTOR", &input2, KNOWN_CRC32C_TEST_VECTOR, allocator));

    /*this tests three things, first it tests the case where we aren't 8-byte aligned*/
    /*second, it tests that reads aren't performed before start of buffer*/
    /*third, it tests that writes aren't performed after the end of the buffer.*/
    /*if any of those things happen, then the checksum will be wrong and the assertion will fail */
    uint8_t *s_non_mem_aligned_vector;
    s_non_mem_aligned_vector = aws_mem_acquire(allocator, sizeof(DATA_32_VALUES) + 6);
    memset(s_non_mem_aligned_vector, 1, sizeof(DATA_32_VALUES) + 6);
    memcpy(s_non_mem_aligned_vector + 3, DATA_32_VALUES, sizeof(DATA_32_VALUES));

    const struct aws_byte_cursor input3 = {
        .len = sizeof(DATA_32_VALUES),
        .ptr = s_non_mem_aligned_vector + 3,
    };
    ASSERT_SUCCESS(
        s_test_known_crc(func_name, func, "non_mem_aligned_vector", &input3, KNOWN_CRC32C_32_VALUES, allocator));
    aws_mem_release(allocator, s_non_mem_aligned_vector);
    return AWS_OP_SUCCESS;
}

/**
 * Quick sanity check of some known CRC values for known input.
 * The reference functions are included in these tests to verify that they aren't obviously broken.
 */
static int s_test_crc32c(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(s_test_known_crc32c("aws_checksum_crc32c_new", aws_checksum_crc32c_new, allocator));

    return AWS_OP_SUCCESS;
}
AWS_TEST_CASE(test_crc32c, s_test_crc32c)

static int s_test_crc32(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(s_test_known_crc32("aws_checksum_crc32_new", aws_checksum_crc32_new, allocator));

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(test_crc32, s_test_crc32)
