// /**
//  * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
//  * SPDX-License-Identifier: Apache-2.0.
//  */
// #include "aws/s3/private/s3_checksums.h"
// #include <aws/common/byte_buf.h>
// #include <aws/io/stream.h>
// #include <aws/testing/aws_test_harness.h>

// static int s_sha1_nist_test_case_2_fn(struct aws_allocator *allocator, void *ctx) {
//     (void)ctx;

//     struct aws_byte_cursor input = aws_byte_cursor_from_c_str("");
//     struct aws_input_stream *empty_stream = aws_input_stream_new_from_cursor(aws_default_allocator(), &input);
//     struct aws_input_stream *empty_checksum_stream = aws_chunk_stream_new(aws_default_allocator(), )

//     return s_verify_checksum_test_case(allocator, &input, &expected_buf, aws_sha1_checksum_new);
// }

// AWS_TEST_CASE(sha1_nist_test_case_2, s_sha1_nist_test_case_2_fn)

// // static int s_sha1_nist_test_case_3_fn(struct aws_allocator *allocator, void *ctx) {
// //     (void)ctx;

// //     struct aws_byte_cursor input =
// //         aws_byte_cursor_from_c_str("abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq");
// //     struct aws_byte_cursor input =
// //         aws_byte_cursor_from_c_str("abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq");
// //     struct aws_byte_cursor expected_buf = aws_byte_cursor_from_array(expected, sizeof(expected));

// //     return s_verify_checksum_test_case(allocator, &input, &expected_buf, aws_sha1_checksum_new);
// // }

// // AWS_TEST_CASE(sha1_nist_test_case_3, s_sha1_nist_test_case_3_fn)

// // static int s_sha1_nist_test_case_6_fn(struct aws_allocator *allocator, void *ctx) {
// //     (void)ctx;

// //     aws_s3_library_init(allocator);

// //     struct aws_checksum *checksum = aws_sha1_checksum_new(allocator);
// //     ASSERT_NOT_NULL(checksum);
// //     struct aws_byte_cursor input =
// //         aws_byte_cursor_from_c_str("abcdefghbcdefghicdefghijdefghijkefghijklfghijklmghijklmnhijklmno");

// //     for (size_t i = 0; i < 16777216; ++i) {
// //         ASSERT_SUCCESS(aws_checksum_update(checksum, &input));
// //     }

// //     uint8_t output[AWS_SHA1_LEN] = {0};
// //     struct aws_byte_buf output_buf = aws_byte_buf_from_array(output, sizeof(output));
// //     output_buf.len = 0;
// //     ASSERT_SUCCESS(aws_checksum_finalize(checksum, &output_buf, 0));

// //     uint8_t expected[] = {
// //         0x77, 0x89, 0xf0, 0xc9, 0xef, 0x7b, 0xfc, 0x40, 0xd9, 0x33,
// //         0x11, 0x14, 0x3d, 0xfb, 0xe6, 0x9e, 0x20, 0x17, 0xf5, 0x92,
// //     };

// //     struct aws_byte_cursor expected_buf = aws_byte_cursor_from_array(expected, sizeof(expected));
// //     ASSERT_BIN_ARRAYS_EQUALS(expected_buf.ptr, expected_buf.len, output_buf.buffer, output_buf.len);

// //     aws_checksum_destroy(checksum);

// //     aws_s3_library_clean_up();

// //     return AWS_OP_SUCCESS;
// // }
