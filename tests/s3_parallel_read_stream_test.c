/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_parallel_input_stream.h"
#include "aws/s3/private/s3_util.h"
#include "aws/s3/s3_client.h"
#include "s3_tester.h"
#include <aws/common/clock.h>
#include <aws/common/file.h>
#include <aws/common/string.h>
#include <aws/io/event_loop.h>
#include <aws/io/future.h>
#include <aws/io/stream.h>
#include <aws/io/uri.h>
#include <aws/testing/aws_test_harness.h>
#include <inttypes.h>

#include <sys/stat.h>

#define TEST_CASE(NAME)                                                                                                \
    AWS_TEST_CASE(NAME, s_test_##NAME);                                                                                \
    static int s_test_##NAME(struct aws_allocator *allocator, void *ctx)

#define DEFINE_HEADER(NAME, VALUE)                                                                                     \
    {                                                                                                                  \
        .name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL(NAME),                                                           \
        .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL(VALUE),                                                         \
    }

#define ONE_SEC_IN_NS ((uint64_t)AWS_TIMESTAMP_NANOS)
#define MAX_TIMEOUT_NS (600 * ONE_SEC_IN_NS)

AWS_STATIC_STRING_FROM_LITERAL(s_parallel_stream_test, "SimpleParallelStreamTest");

static int s_create_read_file(const char *file_path, size_t length) {
    remove(file_path);

    FILE *file = aws_fopen(file_path, "w");
    size_t loop = length / s_parallel_stream_test->len;
    for (size_t i = 0; i < loop; ++i) {
        fprintf(file, "%s", (char *)s_parallel_stream_test->bytes);
    }
    size_t reminder = length % s_parallel_stream_test->len;
    if (reminder) {
        fprintf(file, "%.*s", (int)reminder, s_parallel_stream_test->bytes);
    }
    fclose(file);
    return AWS_OP_SUCCESS;
}

struct aws_parallel_read_from_test_args {
    struct aws_allocator *alloc;

    size_t buffer_start_pos;
    size_t file_start_pos;
    size_t read_length;
    struct aws_future_bool *final_end_future;
    struct aws_byte_buf *final_dest;

    struct aws_parallel_input_stream *parallel_read_stream;
    struct aws_atomic_var *completed_count;
    struct aws_atomic_var *end_of_stream;

    size_t split_num;
};

static void s_s3_parallel_from_file_read_test_task(struct aws_task *task, void *arg, enum aws_task_status task_status) {
    (void)task_status;
    struct aws_parallel_read_from_test_args *test_args = arg;

    struct aws_byte_buf read_buf = {
        .allocator = NULL,
        .buffer = test_args->final_dest->buffer + test_args->buffer_start_pos,
        .len = 0,
        .capacity = test_args->read_length,
    };
    struct aws_future_bool *read_future =
        aws_parallel_input_stream_read(test_args->parallel_read_stream, test_args->file_start_pos, &read_buf);
    aws_future_bool_wait(read_future, MAX_TIMEOUT_NS);
    bool end_of_stream = aws_future_bool_get_result(read_future);
    aws_future_bool_release(read_future);

    struct aws_future_bool *end_future = test_args->final_end_future;
    size_t read_completed = aws_atomic_fetch_add(test_args->completed_count, 1);
    if (end_of_stream) {
        aws_atomic_store_int(test_args->end_of_stream, 1);
    }
    bool completed = read_completed == test_args->split_num - 1;

    bool reached_eos = aws_atomic_load_int(test_args->end_of_stream) == 1;
    aws_mem_release(test_args->alloc, task);
    aws_mem_release(test_args->alloc, test_args);
    if (completed) {
        aws_future_bool_set_result(end_future, reached_eos);
    }
    aws_future_bool_release(end_future);
}

static int s_parallel_read_test_helper(
    struct aws_allocator *alloc,
    struct aws_parallel_input_stream *parallel_read_stream,
    struct aws_byte_buf *read_buf,
    struct aws_event_loop_group *elg,
    size_t start_pos,
    size_t total_length,
    size_t split_num,
    bool *out_eos) {

    struct aws_atomic_var completed_count;
    aws_atomic_store_int(&completed_count, 0);
    struct aws_atomic_var end_of_stream;
    aws_atomic_store_int(&end_of_stream, 0);
    size_t number_bytes_per_read = total_length / split_num;
    if (number_bytes_per_read == 0) {
        struct aws_future_bool *read_future = aws_parallel_input_stream_read(parallel_read_stream, 0, read_buf);
        ASSERT_TRUE(aws_future_bool_wait(read_future, MAX_TIMEOUT_NS));
        aws_future_bool_release(read_future);
        return AWS_OP_SUCCESS;
    }

    struct aws_future_bool *future = aws_future_bool_new(alloc);
    for (size_t i = 0; i < split_num; i++) {
        struct aws_event_loop *loop = aws_event_loop_group_get_next_loop(elg);
        struct aws_parallel_read_from_test_args *test_args =
            aws_mem_calloc(alloc, 1, sizeof(struct aws_parallel_read_from_test_args));

        size_t read_length = number_bytes_per_read;
        if (i == split_num - 1) {
            /* Last part, adjust the size */
            read_length += total_length % split_num;
        }

        test_args->alloc = alloc;
        test_args->buffer_start_pos = i * number_bytes_per_read;
        test_args->file_start_pos = start_pos + test_args->buffer_start_pos;
        test_args->final_end_future = aws_future_bool_acquire(future);
        test_args->read_length = read_length;
        test_args->final_dest = read_buf;
        test_args->parallel_read_stream = parallel_read_stream;
        test_args->completed_count = &completed_count;
        test_args->end_of_stream = &end_of_stream;
        test_args->split_num = split_num;

        struct aws_task *read_task = aws_mem_calloc(alloc, 1, sizeof(struct aws_task));
        aws_task_init(read_task, s_s3_parallel_from_file_read_test_task, test_args, "s3_parallel_read_test_task");
        aws_event_loop_schedule_task_now(loop, read_task);
    }

    ASSERT_TRUE(aws_future_bool_wait(future, MAX_TIMEOUT_NS));
    *out_eos = aws_future_bool_get_result(future);
    aws_future_bool_release(future);
    read_buf->len = total_length;
    return AWS_OP_SUCCESS;
}

TEST_CASE(parallel_read_stream_from_file_sanity_test) {
    (void)ctx;
    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    const char *file_path = "s3_test_parallel_input_stream_read.txt"; /* unique name */
    ASSERT_SUCCESS(s_create_read_file(file_path, s_parallel_stream_test->len));
    struct aws_byte_cursor path_cursor = aws_byte_cursor_from_c_str(file_path);

    struct aws_parallel_input_stream *parallel_read_stream =
        aws_parallel_input_stream_new_from_file(allocator, path_cursor);
    ASSERT_NOT_NULL(parallel_read_stream);

    aws_parallel_input_stream_acquire(parallel_read_stream);
    aws_parallel_input_stream_release(parallel_read_stream);
    struct aws_event_loop_group *el_group = aws_event_loop_group_new_default(allocator, 0, NULL);

    {
        struct aws_byte_buf read_buf;
        aws_byte_buf_init(&read_buf, allocator, s_parallel_stream_test->len);
        bool eos_reached = false;
        ASSERT_SUCCESS(s_parallel_read_test_helper(
            allocator, parallel_read_stream, &read_buf, el_group, 0, s_parallel_stream_test->len, 8, &eos_reached));

        /* Read the exact number of bytes will not reach to the EOS */
        ASSERT_FALSE(eos_reached);
        ASSERT_TRUE(aws_string_eq_byte_buf(s_parallel_stream_test, &read_buf));
        aws_byte_buf_clean_up(&read_buf);
    }

    {
        size_t extra_byte_len = s_parallel_stream_test->len + 1;
        struct aws_byte_buf read_buf;
        aws_byte_buf_init(&read_buf, allocator, extra_byte_len);
        bool eos_reached = false;
        ASSERT_SUCCESS(s_parallel_read_test_helper(
            allocator, parallel_read_stream, &read_buf, el_group, 0, extra_byte_len, 8, &eos_reached));

        /* Read the exact number of bytes will not reach to the EOS */
        ASSERT_TRUE(eos_reached);
        aws_byte_buf_clean_up(&read_buf);
    }

    {
        /* Failure from short buffer */
        struct aws_byte_buf read_buf;
        aws_byte_buf_init(&read_buf, allocator, s_parallel_stream_test->len);
        /* Set the buffer length to be capacity */
        read_buf.len = s_parallel_stream_test->len;
        struct aws_future_bool *read_future = aws_parallel_input_stream_read(parallel_read_stream, 0, &read_buf);
        ASSERT_TRUE(aws_future_bool_is_done(read_future));
        int error = aws_future_bool_get_error(read_future);
        ASSERT_UINT_EQUALS(AWS_ERROR_SHORT_BUFFER, error);
        aws_byte_buf_clean_up(&read_buf);
        aws_future_bool_release(read_future);
    }

    {
        /* offset larger than the length of file, will read nothing and return EOS */
        struct aws_byte_buf read_buf;
        aws_byte_buf_init(&read_buf, allocator, s_parallel_stream_test->len);
        struct aws_future_bool *read_future =
            aws_parallel_input_stream_read(parallel_read_stream, 2 * s_parallel_stream_test->len, &read_buf);
        ASSERT_TRUE(aws_future_bool_is_done(read_future));
        int error = aws_future_bool_get_error(read_future);
        bool eos = aws_future_bool_get_result(read_future);
        /* Seek to offset larger than the length will not fail. */
        ASSERT_UINT_EQUALS(AWS_ERROR_SUCCESS, error);
        ASSERT_TRUE(eos);
        ASSERT_UINT_EQUALS(0, read_buf.len);
        aws_byte_buf_clean_up(&read_buf);
        aws_future_bool_release(read_future);
    }

    remove(file_path);
    aws_parallel_input_stream_release(parallel_read_stream);
    aws_event_loop_group_release(el_group);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

TEST_CASE(parallel_read_stream_from_large_file_test) {
    (void)ctx;
    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    size_t file_length = MB_TO_BYTES(10);

    const char *file_path = "s3_test_parallel_input_stream_read_large.txt"; /* unique name */
    ASSERT_SUCCESS(s_create_read_file(file_path, file_length));
    struct aws_event_loop_group *el_group = aws_event_loop_group_new_default(allocator, 0, NULL);
    struct aws_byte_cursor path_cursor = aws_byte_cursor_from_c_str(file_path);

    struct aws_parallel_input_stream *parallel_read_stream =
        aws_parallel_input_stream_new_from_file(allocator, path_cursor);
    ASSERT_NOT_NULL(parallel_read_stream);

    {
        /* The whole file */
        struct aws_byte_buf read_buf;
        aws_byte_buf_init(&read_buf, allocator, file_length);
        struct aws_byte_buf expected_read_buf;
        aws_byte_buf_init(&expected_read_buf, allocator, file_length);
        bool eos_reached = false;

        ASSERT_SUCCESS(s_parallel_read_test_helper(
            allocator, parallel_read_stream, &read_buf, el_group, 0, file_length, 8, &eos_reached));

        /* Read the exact number of bytes will not reach to the EOS */
        ASSERT_FALSE(eos_reached);
        struct aws_input_stream *stream = aws_input_stream_new_from_file(allocator, file_path);
        ASSERT_SUCCESS(aws_input_stream_read(stream, &expected_read_buf));

        ASSERT_TRUE(aws_byte_buf_eq(&expected_read_buf, &read_buf));
        aws_byte_buf_clean_up(&read_buf);
        aws_byte_buf_clean_up(&expected_read_buf);
        aws_input_stream_release(stream);
    }

    {
        /* First string */
        struct aws_byte_buf read_buf;
        aws_byte_buf_init(&read_buf, allocator, file_length);
        bool eos_reached = true;

        ASSERT_SUCCESS(s_parallel_read_test_helper(
            allocator, parallel_read_stream, &read_buf, el_group, 0, s_parallel_stream_test->len, 8, &eos_reached));

        ASSERT_FALSE(eos_reached);
        ASSERT_TRUE(aws_string_eq_byte_buf(s_parallel_stream_test, &read_buf));
        aws_byte_buf_clean_up(&read_buf);
    }

    {
        /* Second string */
        struct aws_byte_buf read_buf;
        aws_byte_buf_init(&read_buf, allocator, file_length);

        bool eos_reached = true;
        ASSERT_SUCCESS(s_parallel_read_test_helper(
            allocator,
            parallel_read_stream,
            &read_buf,
            el_group,
            s_parallel_stream_test->len,
            s_parallel_stream_test->len,
            8,
            &eos_reached));

        ASSERT_FALSE(eos_reached);
        ASSERT_TRUE(aws_string_eq_byte_buf(s_parallel_stream_test, &read_buf));
        aws_byte_buf_clean_up(&read_buf);
    }
    remove(file_path);
    aws_event_loop_group_release(el_group);
    aws_parallel_input_stream_release(parallel_read_stream);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}
