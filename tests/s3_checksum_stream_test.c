/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_checksums.h"
#include "s3_tester.h"
#include <aws/common/byte_buf.h>
#include <aws/testing/aws_test_harness.h>

static int compare_checksum_stream(struct aws_allocator *allocator, struct aws_byte_cursor *input, size_t buffer_size) {
    struct aws_byte_buf compute_checksum_output;
    struct aws_byte_buf stream_checksum_output;
    struct aws_byte_buf read_buf;
    aws_byte_buf_init(&read_buf, allocator, buffer_size);
    for (int i = AWS_CRC32C; i <= AWS_MD5; i++) {
        aws_byte_buf_init(&compute_checksum_output, allocator, digest_size_from_algorithm(i));
        aws_byte_buf_init(&stream_checksum_output, allocator, digest_size_from_algorithm(i));
        aws_checksum_compute(allocator, i, input, &compute_checksum_output, 0);
        struct aws_input_stream *cursor_stream = aws_input_stream_new_from_cursor(allocator, input);
        struct aws_input_stream *stream = aws_checksum_stream_new(allocator, cursor_stream, i, &stream_checksum_output);
        struct aws_stream_status status;
        AWS_ZERO_STRUCT(status);
        while (!status.is_end_of_stream) {
            ASSERT_SUCCESS(aws_input_stream_read(stream, &read_buf));
            read_buf.len = 0;
            ASSERT_TRUE(aws_input_stream_get_status(stream, &status) == 0);
        }
        aws_input_stream_destroy(cursor_stream);
        aws_input_stream_destroy(stream);
        ASSERT_TRUE(aws_byte_buf_eq(&compute_checksum_output, &stream_checksum_output));
        aws_byte_buf_clean_up(&compute_checksum_output);
        aws_byte_buf_clean_up(&stream_checksum_output);
    }
    aws_byte_buf_clean_up(&read_buf);
    return AWS_OP_SUCCESS;
}

static int s_verify_checksum_stream_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    // struct aws_byte_cursor input0 = aws_byte_cursor_from_c_str("abc");
    struct aws_byte_cursor input1 =
        aws_byte_cursor_from_c_str("abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq");
    // struct aws_byte_cursor input2 = aws_byte_cursor_from_c_str("abcdefghbcdefghicdefghijdefghijkefghijklfghij"
    //                                                            "klmghijklmnhijklmnoijklmnopjklmnopqklm"
    //                                                            "nopqrlmnopqrsmnopqrstnopqrstu");
    // for (int i = 1; i < input0.len + 3; i++) {
    //     ASSERT_SUCCESS(compare_checksum_stream(allocator, &input0, i));
    // }
    ASSERT_SUCCESS(compare_checksum_stream(allocator, &input1, 49));
    for (int i = 1; i < input1.len + 3; i++) {
        ASSERT_SUCCESS(compare_checksum_stream(allocator, &input1, i));
    }
    // for (int i = 1; i < input2.len + 3; i++) {
    //     ASSERT_SUCCESS(compare_checksum_stream(allocator, &input2, i));
    // }
    return AWS_OP_SUCCESS;
}
AWS_TEST_CASE(verify_checksum_stream, s_verify_checksum_stream_fn)
