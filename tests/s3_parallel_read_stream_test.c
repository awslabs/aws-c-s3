/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_parallel_read_stream.h"
#include "aws/s3/private/s3_util.h"
#include "aws/s3/s3_client.h"
#include "s3_tester.h"
#include <aws/common/clock.h>
#include <aws/common/file.h>
#include <aws/common/string.h>
#include <aws/io/stream.h>
#include <aws/io/uri.h>
#include <aws/testing/aws_test_harness.h>
#include <inttypes.h>

#include <sys/stat.h>

#define TEST_CASE(NAME)                                                                                                \
    AWS_TEST_CASE(NAME, s_test_##NAME);                                                                                \
    static int s_test_##NAME(struct aws_allocator *allocator, void *ctx)

#define DEFINE_HEADER(NAME, VALUE)                                                                                     \
    { .name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL(NAME), .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL(VALUE), }

#define ONE_SEC_IN_NS ((uint64_t)AWS_TIMESTAMP_NANOS)
#define MAX_TIMEOUT_NS (10 * ONE_SEC_IN_NS)

AWS_STATIC_STRING_FROM_LITERAL(s_parallel_stream_test, "SimpleParallelStreamTest");

static int s_create_read_file(struct aws_allocator *allocator, const char *file_path, size_t length) {
    remove(file_path);

    FILE *file = aws_fopen(file_path, "w");
    size_t loop = length / s_parallel_stream_test->len;
    for (int i = 0; i < loop; ++i) {
        fprintf(file, "%s", (char *)s_parallel_stream_test->bytes);
    }
    int reminder = length % s_parallel_stream_test->len;
    if (reminder) {
        fprintf(file, "%.*s", reminder, s_parallel_stream_test->bytes);
    }
    fclose(file);
    return AWS_OP_SUCCESS;
}

TEST_CASE(parallel_read_stream_from_file_sanity_test) {
    (void)ctx;
    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    const char *file_path = "s3_test_parallel_input_stream_read.txt"; /* unique name */
    ASSERT_SUCCESS(s_create_read_file(allocator, file_path, s_parallel_stream_test->len));

    struct aws_parallel_input_stream *parallel_read_stream =
        aws_parallel_input_stream_new_from_file(allocator, aws_byte_cursor_from_c_str(file_path), tester.el_group, 8);

    struct aws_byte_buf read_buf;
    aws_byte_buf_init(&read_buf, allocator, s_parallel_stream_test->len);

    struct aws_future_bool *future =
        aws_parallel_input_stream_read(parallel_read_stream, 0, s_parallel_stream_test->len, &read_buf);

    ASSERT_TRUE(aws_future_bool_wait(future, MAX_TIMEOUT_NS));

    ASSERT_TRUE(aws_string_eq_byte_buf(s_parallel_stream_test, &read_buf));

    remove(file_path);
    aws_byte_buf_clean_up(&read_buf);
    aws_future_bool_release(future);
    aws_parallel_input_stream_release(parallel_read_stream);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

TEST_CASE(parallel_read_stream_from_large_file_test) {
    (void)ctx;
    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    size_t file_length = MB_TO_BYTES(10);

    const char *file_path = "s3_test_parallel_input_stream_read_large.txt"; /* unique name */
    ASSERT_SUCCESS(s_create_read_file(allocator, file_path, file_length));

    struct aws_parallel_input_stream *parallel_read_stream =
        aws_parallel_input_stream_new_from_file(allocator, aws_byte_cursor_from_c_str(file_path), tester.el_group, 8);

    {
        /* The whole file */
        struct aws_byte_buf read_buf;
        aws_byte_buf_init(&read_buf, allocator, file_length);
        struct aws_byte_buf expected_read_buf;
        aws_byte_buf_init(&expected_read_buf, allocator, file_length);

        struct aws_future_bool *future =
            aws_parallel_input_stream_read(parallel_read_stream, 0, file_length, &read_buf);

        struct aws_input_stream *stream = aws_input_stream_new_from_file(allocator, file_path);
        ASSERT_SUCCESS(aws_input_stream_read(stream, &expected_read_buf));

        ASSERT_TRUE(aws_future_bool_wait(future, MAX_TIMEOUT_NS));

        ASSERT_TRUE(aws_byte_buf_eq(&expected_read_buf, &read_buf));
        aws_byte_buf_clean_up(&read_buf);
        aws_byte_buf_clean_up(&expected_read_buf);
        aws_input_stream_release(stream);
        aws_future_bool_release(future);
    }

    {
        /* First string */
        struct aws_byte_buf read_buf;
        aws_byte_buf_init(&read_buf, allocator, file_length);

        struct aws_future_bool *future =
            aws_parallel_input_stream_read(parallel_read_stream, 0, s_parallel_stream_test->len, &read_buf);

        ASSERT_TRUE(aws_future_bool_wait(future, MAX_TIMEOUT_NS));

        ASSERT_TRUE(aws_string_eq_byte_buf(s_parallel_stream_test, &read_buf));
        aws_byte_buf_clean_up(&read_buf);
        aws_future_bool_release(future);
    }

    {
        /* Second string */
        struct aws_byte_buf read_buf;
        aws_byte_buf_init(&read_buf, allocator, file_length);

        struct aws_future_bool *future = aws_parallel_input_stream_read(
            parallel_read_stream,
            s_parallel_stream_test->len,
            s_parallel_stream_test->len + s_parallel_stream_test->len,
            &read_buf);

        ASSERT_TRUE(aws_future_bool_wait(future, MAX_TIMEOUT_NS));

        ASSERT_TRUE(aws_string_eq_byte_buf(s_parallel_stream_test, &read_buf));
        aws_byte_buf_clean_up(&read_buf);
        aws_future_bool_release(future);
    }
    remove(file_path);
    aws_parallel_input_stream_release(parallel_read_stream);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}
