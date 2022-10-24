/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_checksums.h"
#include "s3_tester.h"
#include <aws/common/byte_buf.h>
#include <aws/common/encoding.h>
#include <aws/io/stream.h>
#include <aws/testing/aws_test_harness.h>

static int compare_checksum_stream(struct aws_allocator *allocator, struct aws_byte_cursor *input, size_t buffer_size) {
    struct aws_byte_buf compute_checksum_output;
    struct aws_byte_buf compute_encoded_checksum_output;
    struct aws_byte_buf stream_checksum_output;
    struct aws_byte_buf read_buf;
    size_t encoded_len = 0;
    aws_byte_buf_init(&read_buf, allocator, buffer_size);
    for (int algorithm = AWS_SCA_INIT; algorithm <= AWS_SCA_END; algorithm++) {
        aws_base64_compute_encoded_len(aws_get_digest_size_from_algorithm(algorithm), &encoded_len);
        aws_byte_buf_init(&compute_checksum_output, allocator, aws_get_digest_size_from_algorithm(algorithm));
        aws_byte_buf_init(&stream_checksum_output, allocator, encoded_len);
        aws_byte_buf_init(&compute_encoded_checksum_output, allocator, encoded_len);
        aws_checksum_compute(allocator, algorithm, input, &compute_checksum_output, 0);
        struct aws_byte_cursor checksum_result_cursor = aws_byte_cursor_from_buf(&compute_checksum_output);
        aws_base64_encode(&checksum_result_cursor, &compute_encoded_checksum_output);
        struct aws_input_stream *cursor_stream = aws_input_stream_new_from_cursor(allocator, input);
        struct aws_input_stream *stream =
            aws_checksum_stream_new(allocator, cursor_stream, algorithm, &stream_checksum_output);
        aws_input_stream_release(cursor_stream);
        struct aws_stream_status status;
        AWS_ZERO_STRUCT(status);
        while (!status.is_end_of_stream) {
            ASSERT_SUCCESS(aws_input_stream_read(stream, &read_buf));
            read_buf.len = 0;
            ASSERT_TRUE(aws_input_stream_get_status(stream, &status) == 0);
        }
        aws_input_stream_release(stream);
        ASSERT_TRUE(aws_byte_buf_eq(&compute_encoded_checksum_output, &stream_checksum_output));
        aws_byte_buf_clean_up(&compute_checksum_output);
        aws_byte_buf_clean_up(&stream_checksum_output);
        aws_byte_buf_clean_up(&compute_encoded_checksum_output);
    }
    aws_byte_buf_clean_up(&read_buf);
    return AWS_OP_SUCCESS;
}

AWS_STATIC_STRING_FROM_LITERAL(s_0pre_chunk, "0\r\n");
AWS_STATIC_STRING_FROM_LITERAL(s_3pre_chunk, "3\r\n");
AWS_STATIC_STRING_FROM_LITERAL(s_56pre_chunk, "38\r\n");
AWS_STATIC_STRING_FROM_LITERAL(s_112pre_chunk, "70\r\n");
AWS_STATIC_STRING_FROM_LITERAL(s_11pre_chunk, "B\r\n");
AWS_STATIC_STRING_FROM_LITERAL(s_final_chunk, "\r\n0\r\n");
AWS_STATIC_STRING_FROM_LITERAL(s_colon, ":");
AWS_STATIC_STRING_FROM_LITERAL(s_post_trailer, "\r\n\r\n");

static int s_compute_chunk_stream(
    struct aws_allocator *allocator,
    const struct aws_string *pre_chunk,
    struct aws_byte_cursor *input,
    struct aws_byte_buf *output,
    enum aws_s3_checksum_algorithm algorithm,
    struct aws_byte_buf *encoded_checksum_output) {
    struct aws_byte_cursor pre_chunk_cursor = aws_byte_cursor_from_string(pre_chunk);
    struct aws_byte_cursor final_chunk = aws_byte_cursor_from_string(s_final_chunk);
    const struct aws_byte_cursor *checksum_header_name = aws_get_http_header_name_from_algorithm(algorithm);
    struct aws_byte_cursor colon = aws_byte_cursor_from_string(s_colon);
    struct aws_byte_cursor post_trailer = aws_byte_cursor_from_string(s_post_trailer);
    struct aws_byte_buf checksum_result;
    aws_byte_buf_init(&checksum_result, allocator, aws_get_digest_size_from_algorithm(algorithm));
    if (aws_byte_buf_append(output, &pre_chunk_cursor)) {
        return AWS_OP_ERR;
    }
    if (aws_byte_buf_append(output, input)) {
        return AWS_OP_ERR;
    }
    if (input->len > 0) {
        if (aws_byte_buf_append(output, &final_chunk)) {
            return AWS_OP_ERR;
        }
    }
    if (aws_byte_buf_append(output, checksum_header_name)) {
        return AWS_OP_ERR;
    }
    if (aws_byte_buf_append(output, &colon)) {
        return AWS_OP_ERR;
    }
    if (aws_checksum_compute(allocator, algorithm, input, &checksum_result, 0)) {
        return AWS_OP_ERR;
    }
    struct aws_byte_cursor checksum_result_cursor = aws_byte_cursor_from_buf(&checksum_result);
    if (aws_base64_encode(&checksum_result_cursor, encoded_checksum_output)) {
        return AWS_OP_ERR;
    }
    if (aws_base64_encode(&checksum_result_cursor, output)) {
        return AWS_OP_ERR;
    }
    if (aws_byte_buf_append(output, &post_trailer)) {
        return AWS_OP_ERR;
    }
    aws_byte_buf_clean_up(&checksum_result);
    return AWS_OP_SUCCESS;
}

static int s_stream_chunk(
    struct aws_allocator *allocator,
    struct aws_byte_cursor *input,
    struct aws_byte_buf *read_buf,
    struct aws_byte_buf *output,
    enum aws_s3_checksum_algorithm algorithm,
    struct aws_byte_buf *checksum_result) {
    struct aws_input_stream *cursor_stream = aws_input_stream_new_from_cursor(allocator, input);
    struct aws_input_stream *stream = aws_chunk_stream_new(allocator, cursor_stream, algorithm, checksum_result);
    aws_input_stream_release(cursor_stream);
    struct aws_stream_status status;
    AWS_ZERO_STRUCT(status);
    while (!status.is_end_of_stream) {
        ASSERT_SUCCESS(aws_input_stream_read(stream, read_buf));
        struct aws_byte_cursor read_cursor = aws_byte_cursor_from_buf(read_buf);
        aws_byte_buf_append(output, &read_cursor);
        read_buf->len = 0;
        ASSERT_TRUE(aws_input_stream_get_status(stream, &status) == 0);
    }
    aws_input_stream_release(stream);
    return AWS_OP_SUCCESS;
}

static int compare_chunk_stream(
    struct aws_allocator *allocator,
    const struct aws_string *pre_chunk,
    struct aws_byte_cursor *input,
    size_t buffer_size) {

    struct aws_byte_buf compute_chunk_output;
    struct aws_byte_buf stream_chunk_output;
    struct aws_byte_buf stream_chunk_output1;
    struct aws_byte_buf streamed_encoded_checksum;
    struct aws_byte_buf computed_encoded_checksum;
    size_t len_no_checksum = pre_chunk->len + input->len + s_final_chunk->len + s_post_trailer->len + s_colon->len;
    size_t encoded_len = 0;
    struct aws_byte_buf read_buf;
    aws_byte_buf_init(&read_buf, allocator, buffer_size);
    for (int algorithm = AWS_SCA_INIT; algorithm <= AWS_SCA_END; algorithm++) {
        aws_base64_compute_encoded_len(aws_get_digest_size_from_algorithm(algorithm), &encoded_len);
        size_t total_len = len_no_checksum + encoded_len + aws_get_http_header_name_from_algorithm(algorithm)->len;
        aws_byte_buf_init(&computed_encoded_checksum, allocator, encoded_len);
        aws_byte_buf_init(&compute_chunk_output, allocator, total_len);
        aws_byte_buf_init(&stream_chunk_output, allocator, total_len);
        aws_byte_buf_init(&stream_chunk_output1, allocator, total_len);
        ASSERT_SUCCESS(s_compute_chunk_stream(
            allocator, pre_chunk, input, &compute_chunk_output, algorithm, &computed_encoded_checksum));
        ASSERT_SUCCESS(s_stream_chunk(allocator, input, &read_buf, &stream_chunk_output, algorithm, NULL));
        ASSERT_SUCCESS(
            s_stream_chunk(allocator, input, &read_buf, &stream_chunk_output1, algorithm, &streamed_encoded_checksum));
        ASSERT_TRUE(aws_byte_buf_eq(&compute_chunk_output, &stream_chunk_output));
        ASSERT_TRUE(aws_byte_buf_eq(&compute_chunk_output, &stream_chunk_output1));
        ASSERT_TRUE(aws_byte_buf_eq(&computed_encoded_checksum, &streamed_encoded_checksum));
        aws_byte_buf_clean_up(&compute_chunk_output);
        aws_byte_buf_clean_up(&stream_chunk_output);
        aws_byte_buf_clean_up(&stream_chunk_output1);
        aws_byte_buf_clean_up(&streamed_encoded_checksum);
        aws_byte_buf_clean_up(&computed_encoded_checksum);
    }
    aws_byte_buf_clean_up(&read_buf);
    return AWS_OP_SUCCESS;
}

static int s_verify_checksum_stream_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_s3_library_init(allocator);
    struct aws_byte_cursor input0 = aws_byte_cursor_from_c_str("");
    struct aws_byte_cursor input1 = aws_byte_cursor_from_c_str("abc");
    struct aws_byte_cursor input2 =
        aws_byte_cursor_from_c_str("abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq");
    struct aws_byte_cursor input3 = aws_byte_cursor_from_c_str("abcdefghbcdefghicdefghijdefghijkefghijklfghij"
                                                               "klmghijklmnhijklmnoijklmnopjklmnopqklm"
                                                               "nopqrlmnopqrsmnopqrstnopqrstu");
    struct aws_byte_cursor input4 = aws_byte_cursor_from_c_str("Hello world");
    for (size_t buffer_size = 1; buffer_size < input0.len + 3; buffer_size++) {
        ASSERT_SUCCESS(compare_checksum_stream(allocator, &input0, buffer_size));
    }
    for (size_t buffer_size = 1; buffer_size < input1.len + 3; buffer_size++) {
        ASSERT_SUCCESS(compare_checksum_stream(allocator, &input1, buffer_size));
    }
    for (size_t buffer_size = 1; buffer_size < input2.len + 3; buffer_size++) {
        ASSERT_SUCCESS(compare_checksum_stream(allocator, &input2, buffer_size));
    }
    for (size_t buffer_size = 1; buffer_size < input3.len + 3; buffer_size++) {
        ASSERT_SUCCESS(compare_checksum_stream(allocator, &input3, buffer_size));
    }
    for (size_t buffer_size = 1; buffer_size < input4.len + 3; buffer_size++) {
        ASSERT_SUCCESS(compare_checksum_stream(allocator, &input4, buffer_size));
    }
    aws_s3_library_clean_up();
    return AWS_OP_SUCCESS;
}
AWS_TEST_CASE(verify_checksum_stream, s_verify_checksum_stream_fn)

static int s_verify_chunk_stream_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_s3_library_init(allocator);
    struct aws_byte_cursor input0 = aws_byte_cursor_from_c_str("");
    struct aws_byte_cursor input1 = aws_byte_cursor_from_c_str("abc");
    struct aws_byte_cursor input2 =
        aws_byte_cursor_from_c_str("abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq");
    struct aws_byte_cursor input3 = aws_byte_cursor_from_c_str("abcdefghbcdefghicdefghijdefghijkefghijklfghij"
                                                               "klmghijklmnhijklmnoijklmnopjklmnopqklm"
                                                               "nopqrlmnopqrsmnopqrstnopqrstu");
    struct aws_byte_cursor input4 = aws_byte_cursor_from_c_str("Hello world");
    for (size_t buffer_size = 1; buffer_size < input0.len + 70; buffer_size++) {
        ASSERT_SUCCESS(compare_chunk_stream(allocator, s_0pre_chunk, &input0, buffer_size));
    }
    for (size_t buffer_size = 1; buffer_size < input1.len + 70; buffer_size++) {
        ASSERT_SUCCESS(compare_chunk_stream(allocator, s_3pre_chunk, &input1, buffer_size));
    }
    for (size_t buffer_size = 1; buffer_size < input2.len + 70; buffer_size++) {
        ASSERT_SUCCESS(compare_chunk_stream(allocator, s_56pre_chunk, &input2, buffer_size));
    }
    for (size_t buffer_size = 1; buffer_size < input3.len + 70; buffer_size++) {
        ASSERT_SUCCESS(compare_chunk_stream(allocator, s_112pre_chunk, &input3, buffer_size));
    }
    for (size_t buffer_size = 1; buffer_size < input4.len + 70; buffer_size++) {
        ASSERT_SUCCESS(compare_chunk_stream(allocator, s_11pre_chunk, &input4, buffer_size));
    }
    aws_s3_library_clean_up();
    return AWS_OP_SUCCESS;
}
AWS_TEST_CASE(verify_chunk_stream, s_verify_chunk_stream_fn)
