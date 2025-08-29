/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_default_buffer_pool.h"
#include "aws/s3/private/s3_parallel_input_stream.h"
#include "aws/s3/private/s3_part_streaming_input_stream.h"
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
    uint64_t file_offset;
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

    /* Updated API call with offset and max_length */
    struct aws_future_bool *read_future = aws_parallel_input_stream_read(
        test_args->parallel_read_stream, test_args->file_offset, test_args->read_length, &read_buf);

    aws_future_bool_wait(read_future, MAX_TIMEOUT_NS);
    bool eos_reached = aws_future_bool_get_result(read_future);
    aws_future_bool_release(read_future);

    struct aws_future_bool *end_future = test_args->final_end_future;
    size_t read_completed = aws_atomic_fetch_add(test_args->completed_count, 1);
    if (eos_reached) {
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
        /* Updated API call with offset and max_length */
        struct aws_byte_buf temp_buf = {
            .allocator = NULL,
            .buffer = read_buf->buffer,
            .len = 0,
            .capacity = total_length,
        };
        struct aws_future_bool *read_future =
            aws_parallel_input_stream_read(parallel_read_stream, start_pos, total_length, &temp_buf);

        ASSERT_TRUE(aws_future_bool_wait(read_future, MAX_TIMEOUT_NS));
        aws_future_bool_release(read_future);
        read_buf->len = temp_buf.len;
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
        test_args->file_offset = start_pos + test_args->buffer_start_pos;
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

    /* Create an event loop group for the parallel input stream */
    struct aws_event_loop_group *reading_elg = aws_event_loop_group_new_default(allocator, 1, NULL);
    ASSERT_NOT_NULL(reading_elg);

    struct aws_parallel_input_stream *parallel_read_stream =
        aws_parallel_input_stream_new_from_file(allocator, path_cursor, reading_elg, false /*direct_io*/);
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

        /* Updated API call with offset and max_length */
        struct aws_future_bool *read_future =
            aws_parallel_input_stream_read(parallel_read_stream, 0, s_parallel_stream_test->len, &read_buf);

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

        /* Updated API call with offset and max_length */
        struct aws_future_bool *read_future = aws_parallel_input_stream_read(
            parallel_read_stream, 2 * s_parallel_stream_test->len, s_parallel_stream_test->len, &read_buf);

        /* Wait for the future to finish */
        ASSERT_TRUE(aws_future_bool_wait(read_future, MAX_TIMEOUT_NS));
        ASSERT_TRUE(aws_future_bool_is_done(read_future));
        int error = aws_future_bool_get_error(read_future);
        bool eos_reached = aws_future_bool_get_result(read_future);
        /* Seek to offset larger than the length will not fail. */
        ASSERT_UINT_EQUALS(AWS_ERROR_SUCCESS, error);
        ASSERT_TRUE(eos_reached);
        ASSERT_UINT_EQUALS(0, read_buf.len);
        aws_byte_buf_clean_up(&read_buf);
        aws_future_bool_release(read_future);
    }

    remove(file_path);
    struct aws_future_void *shutdown_future = aws_parallel_input_stream_get_shutdown_future(parallel_read_stream);
    aws_parallel_input_stream_release(parallel_read_stream);
    aws_future_void_wait(shutdown_future, SIZE_MAX);
    aws_future_void_release(shutdown_future);
    aws_event_loop_group_release(el_group);
    aws_event_loop_group_release(reading_elg);
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

    /* Create an event loop group for the parallel input stream */
    struct aws_event_loop_group *reading_elg = aws_event_loop_group_new_default(allocator, 1, NULL);
    ASSERT_NOT_NULL(reading_elg);

    struct aws_event_loop_group *el_group = aws_event_loop_group_new_default(allocator, 0, NULL);
    struct aws_byte_cursor path_cursor = aws_byte_cursor_from_c_str(file_path);

    struct aws_parallel_input_stream *parallel_read_stream =
        aws_parallel_input_stream_new_from_file(allocator, path_cursor, reading_elg, false /*direct_io*/);
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
    aws_event_loop_group_release(reading_elg);
    struct aws_future_void *shutdown_future = aws_parallel_input_stream_get_shutdown_future(parallel_read_stream);
    aws_parallel_input_stream_release(parallel_read_stream);
    aws_future_void_wait(shutdown_future, SIZE_MAX);
    aws_future_void_release(shutdown_future);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

/* Helper structure for part streaming tests */
struct part_streaming_test_fixture {
    struct aws_s3_tester tester;
    struct aws_event_loop_group *reading_elg;
    struct aws_parallel_input_stream *parallel_read_stream;
    struct aws_s3_buffer_pool *buffer_pool;
    struct aws_s3_buffer_ticket *ticket;
    const char *file_path;
};

/* Helper function to set up part streaming test fixture */
static int s_part_streaming_test_setup(
    struct aws_allocator *allocator,
    struct part_streaming_test_fixture *fixture,
    const char *file_path,
    size_t file_length,
    size_t buffer_size) {

    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &fixture->tester));

    fixture->file_path = file_path;
    ASSERT_SUCCESS(s_create_read_file(file_path, file_length));

    fixture->reading_elg = aws_event_loop_group_new_default(allocator, 1, NULL);
    ASSERT_NOT_NULL(fixture->reading_elg);

    struct aws_byte_cursor path_cursor = aws_byte_cursor_from_c_str(file_path);
    fixture->parallel_read_stream =
        aws_parallel_input_stream_new_from_file(allocator, path_cursor, fixture->reading_elg, false /*direct_io*/);
    ASSERT_NOT_NULL(fixture->parallel_read_stream);

    fixture->buffer_pool = aws_s3_default_buffer_pool_new(
        allocator, (struct aws_s3_buffer_pool_config){.part_size = buffer_size, .memory_limit = GB_TO_BYTES(1)});

    struct aws_future_s3_buffer_ticket *future = aws_s3_default_buffer_pool_reserve(
        fixture->buffer_pool, (struct aws_s3_buffer_pool_reserve_meta){.size = buffer_size});
    ASSERT_NOT_NULL(future);
    AWS_FATAL_ASSERT(aws_future_s3_buffer_ticket_is_done(future));
    AWS_FATAL_ASSERT(aws_future_s3_buffer_ticket_get_error(future) == AWS_OP_SUCCESS);
    fixture->ticket = aws_future_s3_buffer_ticket_get_result_by_move(future);
    aws_future_s3_buffer_ticket_release(future);

    return AWS_OP_SUCCESS;
}

/* Helper function to clean up part streaming test fixture */
static void s_part_streaming_test_cleanup(struct part_streaming_test_fixture *fixture) {
    remove(fixture->file_path);
    aws_event_loop_group_release(fixture->reading_elg);
    struct aws_future_void *shutdown_future =
        aws_parallel_input_stream_get_shutdown_future(fixture->parallel_read_stream);
    aws_parallel_input_stream_release(fixture->parallel_read_stream);
    aws_future_void_wait(shutdown_future, SIZE_MAX);
    aws_future_void_release(shutdown_future);
    aws_s3_buffer_ticket_release(fixture->ticket);
    aws_s3_default_buffer_pool_destroy(fixture->buffer_pool);
    aws_s3_tester_clean_up(&fixture->tester);
}

/* Helper function to validate stream content against file */
static int s_validate_stream_content(
    struct aws_allocator *allocator,
    struct aws_input_stream *stream,
    const char *file_path,
    size_t offset,
    size_t expected_length) {

    struct aws_byte_buf read_buf;
    aws_byte_buf_init(&read_buf, allocator, expected_length);
    /* Read entire file and compare with expected content */

    while (read_buf.len < read_buf.capacity) {
        struct aws_stream_status status;
        ASSERT_SUCCESS(aws_input_stream_get_status(stream, &status));
        ASSERT_TRUE(status.is_valid);
        if (status.is_end_of_stream) {
            break;
        }
        ASSERT_SUCCESS(aws_input_stream_read(stream, &read_buf));
    }
    ASSERT_UINT_EQUALS(expected_length, read_buf.len);

    /* Compare with expected content from file */
    struct aws_byte_buf expected_buf;
    aws_byte_buf_init(&expected_buf, allocator, expected_length);
    struct aws_input_stream *file_stream = aws_input_stream_new_from_file(allocator, file_path);
    ASSERT_SUCCESS(aws_input_stream_seek(file_stream, offset, AWS_SSB_BEGIN));

    while (expected_buf.len < expected_buf.capacity) {
        struct aws_stream_status status;
        ASSERT_SUCCESS(aws_input_stream_get_status(file_stream, &status));
        ASSERT_TRUE(status.is_valid);
        if (status.is_end_of_stream) {
            break;
        }
        ASSERT_SUCCESS(aws_input_stream_read(file_stream, &expected_buf));
    }
    ASSERT_UINT_EQUALS(expected_length, expected_buf.len);

    struct aws_byte_cursor expected_cursor = aws_byte_cursor_from_buf(&expected_buf);
    struct aws_byte_cursor read_cursor = aws_byte_cursor_from_buf(&read_buf);
    ASSERT_TRUE(aws_byte_cursor_eq(&expected_cursor, &read_cursor));

    aws_byte_buf_clean_up(&read_buf);
    aws_byte_buf_clean_up(&expected_buf);
    aws_input_stream_release(file_stream);

    return AWS_OP_SUCCESS;
}

/* Helper function to validate stream content against file */
static int s_validate_stream_content_with_chunks(
    struct aws_allocator *allocator,
    struct aws_input_stream *stream,
    const char *file_path,
    size_t offset,
    size_t total_expected_length,
    size_t chunk_size) {

    struct aws_byte_buf read_buf;
    aws_byte_buf_init(&read_buf, allocator, total_expected_length);
    /* Read entire file and compare with expected content */
    struct aws_byte_buf chunk_buf;
    aws_byte_buf_init(&chunk_buf, allocator, chunk_size);
    /* Read in chunks until we reach end of stream */
    while (true) {
        struct aws_stream_status status;
        ASSERT_SUCCESS(aws_input_stream_get_status(stream, &status));
        ASSERT_TRUE(status.is_valid);
        if (status.is_end_of_stream) {
            break;
        }

        aws_byte_buf_reset(&chunk_buf, 0);
        ASSERT_SUCCESS(aws_input_stream_read(stream, &chunk_buf));

        /* Append chunk to accumulated buffer */
        struct aws_byte_cursor chunk_cursor = aws_byte_cursor_from_buf(&chunk_buf);
        aws_byte_buf_append(&read_buf, &chunk_cursor);
    }

    aws_byte_buf_clean_up(&chunk_buf);
    ASSERT_UINT_EQUALS(total_expected_length, read_buf.len);

    /* Compare with expected content from file */
    struct aws_byte_buf expected_buf;
    aws_byte_buf_init(&expected_buf, allocator, total_expected_length);
    struct aws_input_stream *file_stream = aws_input_stream_new_from_file(allocator, file_path);
    ASSERT_SUCCESS(aws_input_stream_seek(file_stream, offset, AWS_SSB_BEGIN));
    while (expected_buf.len < expected_buf.capacity) {
        struct aws_stream_status status;
        ASSERT_SUCCESS(aws_input_stream_get_status(file_stream, &status));
        ASSERT_TRUE(status.is_valid);
        if (status.is_end_of_stream) {
            break;
        }
        ASSERT_SUCCESS(aws_input_stream_read(file_stream, &expected_buf));
    }
    ASSERT_UINT_EQUALS(total_expected_length, expected_buf.len);

    struct aws_byte_cursor expected_cursor = aws_byte_cursor_from_buf(&expected_buf);
    struct aws_byte_cursor read_cursor = aws_byte_cursor_from_buf(&read_buf);
    ASSERT_TRUE(aws_byte_cursor_eq(&expected_cursor, &read_cursor));

    aws_byte_buf_clean_up(&read_buf);
    aws_byte_buf_clean_up(&expected_buf);
    aws_input_stream_release(file_stream);

    return AWS_OP_SUCCESS;
}

TEST_CASE(part_streaming_stream_from_large_file_test) {
    (void)ctx;
    struct part_streaming_test_fixture fixture;
    size_t file_length = MB_TO_BYTES(100);

    ASSERT_SUCCESS(s_part_streaming_test_setup(
        allocator, &fixture, "s3_part_streaming_stream_read_large.txt", file_length, KB_TO_BYTES(16)));
    /* Test reading from unaligned offset */
    struct aws_input_stream *part_streaming_stream =
        aws_part_streaming_input_stream_new(allocator, fixture.parallel_read_stream, fixture.ticket, 0, file_length);
    ASSERT_NOT_NULL(part_streaming_stream);

    /* Test initial status */
    struct aws_stream_status status;
    ASSERT_SUCCESS(aws_input_stream_get_status(part_streaming_stream, &status));
    ASSERT_TRUE(status.is_valid);
    ASSERT_FALSE(status.is_end_of_stream);

    /* Validate content */
    ASSERT_SUCCESS(s_validate_stream_content(allocator, part_streaming_stream, fixture.file_path, 0, file_length));

    aws_input_stream_release(part_streaming_stream);
    s_part_streaming_test_cleanup(&fixture);

    return AWS_OP_SUCCESS;
}

TEST_CASE(part_streaming_stream_offset_test) {
    (void)ctx;
    struct part_streaming_test_fixture fixture;
    size_t file_length = MB_TO_BYTES(5);
    size_t offset = KB_TO_BYTES(100);
    size_t read_size = KB_TO_BYTES(500);

    ASSERT_SUCCESS(s_part_streaming_test_setup(
        allocator, &fixture, "s3_part_streaming_stream_offset_test.txt", file_length, MB_TO_BYTES(4)));

    /* Test reading from offset */
    struct aws_input_stream *part_streaming_stream =
        aws_part_streaming_input_stream_new(allocator, fixture.parallel_read_stream, fixture.ticket, offset, read_size);
    ASSERT_NOT_NULL(part_streaming_stream);

    /* Validate content */
    ASSERT_SUCCESS(s_validate_stream_content(allocator, part_streaming_stream, fixture.file_path, offset, read_size));

    aws_input_stream_release(part_streaming_stream);
    s_part_streaming_test_cleanup(&fixture);
    return AWS_OP_SUCCESS;
}

TEST_CASE(part_streaming_stream_chunked_read_test) {
    (void)ctx;
    size_t file_length = MB_TO_BYTES(3);
    /* small chunks */
    size_t chunk_size = KB_TO_BYTES(1);

    struct part_streaming_test_fixture fixture;
    ASSERT_SUCCESS(s_part_streaming_test_setup(
        allocator, &fixture, "s3_part_streaming_stream_chunked_test.txt", file_length, MB_TO_BYTES(2)));

    /* Test reading in small chunks */
    struct aws_input_stream *part_streaming_stream =
        aws_part_streaming_input_stream_new(allocator, fixture.parallel_read_stream, fixture.ticket, 0, file_length);
    ASSERT_NOT_NULL(part_streaming_stream);

    ASSERT_SUCCESS(s_validate_stream_content_with_chunks(
        allocator, part_streaming_stream, fixture.file_path, 0, file_length, chunk_size));
    aws_input_stream_release(part_streaming_stream);

    s_part_streaming_test_cleanup(&fixture);
    return AWS_OP_SUCCESS;
}

TEST_CASE(part_streaming_stream_unaligned_offset_test) {
    (void)ctx;
    struct part_streaming_test_fixture fixture;
    size_t file_length = MB_TO_BYTES(10);
    /* Use an offset that's not aligned to 4KB page boundary */
    size_t offset = KB_TO_BYTES(4) + 1234; /* 4KB + 1234 bytes */
    /* small chunks */
    size_t chunk_size = KB_TO_BYTES(123);

    ASSERT_SUCCESS(s_part_streaming_test_setup(
        allocator, &fixture, "s3_part_streaming_stream_unaligned_test.txt", file_length, MB_TO_BYTES(1)));

    /* Test reading from unaligned offset */
    /* We can create the input stream with more than the file has. And it should still work. */
    struct aws_input_stream *part_streaming_stream = aws_part_streaming_input_stream_new(
        allocator, fixture.parallel_read_stream, fixture.ticket, offset, 2 * file_length);
    ASSERT_NOT_NULL(part_streaming_stream);

    ASSERT_SUCCESS(s_validate_stream_content_with_chunks(
        allocator, part_streaming_stream, fixture.file_path, offset, file_length - offset, chunk_size));

    aws_input_stream_release(part_streaming_stream);
    s_part_streaming_test_cleanup(&fixture);

    return AWS_OP_SUCCESS;
}

TEST_CASE(part_streaming_stream_small_buffer_test) {
    (void)ctx;
    struct part_streaming_test_fixture fixture;
    size_t file_length = MB_TO_BYTES(10);

    ASSERT_SUCCESS(s_part_streaming_test_setup(
        allocator, &fixture, "s3_part_streaming_stream_small_buffer_test.txt", file_length, KB_TO_BYTES(16)));

    /* Test with small buffer that requires multiple loads */
    struct aws_input_stream *part_streaming_stream =
        aws_part_streaming_input_stream_new(allocator, fixture.parallel_read_stream, fixture.ticket, 0, file_length);
    ASSERT_NOT_NULL(part_streaming_stream);

    /* Validate content */
    ASSERT_SUCCESS(s_validate_stream_content(allocator, part_streaming_stream, fixture.file_path, 0, file_length));

    aws_input_stream_release(part_streaming_stream);
    s_part_streaming_test_cleanup(&fixture);

    return AWS_OP_SUCCESS;
}

TEST_CASE(part_streaming_stream_seek_unsupported_test) {
    (void)ctx;
    struct part_streaming_test_fixture fixture;
    size_t file_length = KB_TO_BYTES(10);

    ASSERT_SUCCESS(s_part_streaming_test_setup(
        allocator, &fixture, "s3_part_streaming_stream_seek_test.txt", file_length, KB_TO_BYTES(32)));

    /* Test that seek operation is not supported */
    struct aws_input_stream *part_streaming_stream =
        aws_part_streaming_input_stream_new(allocator, fixture.parallel_read_stream, fixture.ticket, 0, file_length);
    ASSERT_NOT_NULL(part_streaming_stream);

    /* Test seek operations - all should fail */
    ASSERT_FAILS(aws_input_stream_seek(part_streaming_stream, 0, AWS_SSB_BEGIN));
    ASSERT_UINT_EQUALS(AWS_ERROR_UNSUPPORTED_OPERATION, aws_last_error());

    aws_reset_error();
    ASSERT_FAILS(aws_input_stream_seek(part_streaming_stream, 100, AWS_SSB_BEGIN));
    ASSERT_UINT_EQUALS(AWS_ERROR_UNSUPPORTED_OPERATION, aws_last_error());

    aws_reset_error();
    ASSERT_FAILS(aws_input_stream_seek(part_streaming_stream, 0, AWS_SSB_END));
    ASSERT_UINT_EQUALS(AWS_ERROR_UNSUPPORTED_OPERATION, aws_last_error());

    aws_input_stream_release(part_streaming_stream);
    s_part_streaming_test_cleanup(&fixture);

    return AWS_OP_SUCCESS;
}

TEST_CASE(part_streaming_stream_get_length_test) {
    (void)ctx;
    struct part_streaming_test_fixture fixture;
    size_t file_length = KB_TO_BYTES(10);

    ASSERT_SUCCESS(s_part_streaming_test_setup(
        allocator, &fixture, "s3_part_streaming_stream_get_length_test.txt", file_length, KB_TO_BYTES(32)));

    /* Test that get_length operation is now supported */
    struct aws_input_stream *part_streaming_stream =
        aws_part_streaming_input_stream_new(allocator, fixture.parallel_read_stream, fixture.ticket, 0, file_length);
    ASSERT_NOT_NULL(part_streaming_stream);

    int64_t length;
    ASSERT_SUCCESS(aws_input_stream_get_length(part_streaming_stream, &length));
    ASSERT_UINT_EQUALS(file_length, (size_t)length);

    /* Test with a different length */
    size_t custom_length = KB_TO_BYTES(5);
    struct aws_input_stream *part_streaming_stream2 =
        aws_part_streaming_input_stream_new(allocator, fixture.parallel_read_stream, fixture.ticket, 0, custom_length);
    ASSERT_NOT_NULL(part_streaming_stream2);

    ASSERT_SUCCESS(aws_input_stream_get_length(part_streaming_stream2, &length));
    ASSERT_UINT_EQUALS(custom_length, (size_t)length);

    /* Test with an offset */
    size_t offset = KB_TO_BYTES(2);
    struct aws_input_stream *part_streaming_stream3 = aws_part_streaming_input_stream_new(
        allocator, fixture.parallel_read_stream, fixture.ticket, offset, custom_length);
    ASSERT_NOT_NULL(part_streaming_stream3);

    ASSERT_SUCCESS(aws_input_stream_get_length(part_streaming_stream3, &length));
    ASSERT_UINT_EQUALS(custom_length, (size_t)length);

    /* Test with an offset and passed-in length is larger than available */
    struct aws_input_stream *part_streaming_stream4 = aws_part_streaming_input_stream_new(
        allocator, fixture.parallel_read_stream, fixture.ticket, offset, file_length);
    ASSERT_NOT_NULL(part_streaming_stream4);

    ASSERT_SUCCESS(aws_input_stream_get_length(part_streaming_stream4, &length));
    ASSERT_UINT_EQUALS(file_length - offset, (size_t)length);

    /* Test with an offset is larger than available */
    struct aws_input_stream *part_streaming_stream5 = aws_part_streaming_input_stream_new(
        allocator, fixture.parallel_read_stream, fixture.ticket, file_length + offset, file_length);
    ASSERT_NOT_NULL(part_streaming_stream5);

    ASSERT_SUCCESS(aws_input_stream_get_length(part_streaming_stream5, &length));
    ASSERT_UINT_EQUALS(0, (size_t)length);

    aws_input_stream_release(part_streaming_stream);
    aws_input_stream_release(part_streaming_stream2);
    aws_input_stream_release(part_streaming_stream3);
    aws_input_stream_release(part_streaming_stream4);
    aws_input_stream_release(part_streaming_stream5);
    s_part_streaming_test_cleanup(&fixture);

    return AWS_OP_SUCCESS;
}

TEST_CASE(part_streaming_stream_file_deleted_during_read_test) {
    (void)ctx;
    struct part_streaming_test_fixture fixture;
    size_t file_length = MB_TO_BYTES(1);

    ASSERT_SUCCESS(s_part_streaming_test_setup(
        allocator, &fixture, "s3_part_streaming_stream_delete_test.txt", file_length, KB_TO_BYTES(16)));

    /* Create the part streaming stream */
    struct aws_input_stream *part_streaming_stream =
        aws_part_streaming_input_stream_new(allocator, fixture.parallel_read_stream, fixture.ticket, 0, file_length);
    ASSERT_NOT_NULL(part_streaming_stream);

    /* Delete the file while the stream is still active */
    remove(fixture.file_path);
    struct aws_byte_buf read_buf;
    aws_byte_buf_init(&read_buf, allocator, file_length);

    /* Keep reading until we get an error or reach end of stream */
    int error_code = AWS_OP_SUCCESS;
    while (read_buf.len < read_buf.capacity) {
        struct aws_stream_status status;
        ASSERT_SUCCESS(aws_input_stream_get_status(part_streaming_stream, &status));
        ASSERT_TRUE(status.is_valid);

        if (status.is_end_of_stream) {
            break;
        }

        if (aws_input_stream_read(part_streaming_stream, &read_buf) != AWS_OP_SUCCESS) {
            /* Error occurred - this is expected when file is deleted during read */
            error_code = aws_last_error();
            break;
        }
    }

    ASSERT_TRUE(error_code != AWS_OP_SUCCESS);

    aws_byte_buf_clean_up(&read_buf);
    aws_input_stream_release(part_streaming_stream);

    /* Clean up the rest of the fixture (file is already deleted) */
    aws_event_loop_group_release(fixture.reading_elg);
    struct aws_future_void *shutdown_future =
        aws_parallel_input_stream_get_shutdown_future(fixture.parallel_read_stream);
    aws_parallel_input_stream_release(fixture.parallel_read_stream);
    aws_future_void_wait(shutdown_future, SIZE_MAX);
    aws_future_void_release(shutdown_future);
    aws_s3_buffer_ticket_release(fixture.ticket);
    aws_s3_default_buffer_pool_destroy(fixture.buffer_pool);
    aws_s3_tester_clean_up(&fixture.tester);

    return AWS_OP_SUCCESS;
}

TEST_CASE(part_streaming_stream_zero_length_request_test) {
    (void)ctx;
    struct part_streaming_test_fixture fixture;
    size_t file_length = KB_TO_BYTES(10);

    ASSERT_SUCCESS(s_part_streaming_test_setup(
        allocator, &fixture, "s3_part_streaming_stream_zero_length_test.txt", file_length, KB_TO_BYTES(32)));

    /* Test with zero length request */
    struct aws_input_stream *part_streaming_stream =
        aws_part_streaming_input_stream_new(allocator, fixture.parallel_read_stream, fixture.ticket, 0, 0);
    ASSERT_NOT_NULL(part_streaming_stream);

    /* Check initial status - should be end of stream immediately */
    struct aws_stream_status status;
    ASSERT_SUCCESS(aws_input_stream_get_status(part_streaming_stream, &status));
    ASSERT_TRUE(status.is_valid);
    ASSERT_TRUE(status.is_end_of_stream);

    /* Try to read - should return immediately with no data */
    struct aws_byte_buf read_buf;
    aws_byte_buf_init(&read_buf, allocator, KB_TO_BYTES(1));
    ASSERT_SUCCESS(aws_input_stream_read(part_streaming_stream, &read_buf));
    ASSERT_UINT_EQUALS(0, read_buf.len);

    aws_byte_buf_clean_up(&read_buf);
    aws_input_stream_release(part_streaming_stream);
    s_part_streaming_test_cleanup(&fixture);

    return AWS_OP_SUCCESS;
}

TEST_CASE(part_streaming_stream_large_offset_test) {
    (void)ctx;
    struct part_streaming_test_fixture fixture;
    size_t file_length = KB_TO_BYTES(10);

    ASSERT_SUCCESS(s_part_streaming_test_setup(
        allocator, &fixture, "s3_part_streaming_stream_large_offset_test.txt", file_length, KB_TO_BYTES(32)));

    /* Test with offset larger than file size */
    uint64_t large_offset = file_length * 2;
    struct aws_input_stream *part_streaming_stream = aws_part_streaming_input_stream_new(
        allocator, fixture.parallel_read_stream, fixture.ticket, large_offset, file_length);
    ASSERT_NOT_NULL(part_streaming_stream);

    /* Try to read - should reach end of stream immediately */
    struct aws_byte_buf read_buf;
    aws_byte_buf_init(&read_buf, allocator, KB_TO_BYTES(1));
    ASSERT_SUCCESS(aws_input_stream_read(part_streaming_stream, &read_buf));
    ASSERT_UINT_EQUALS(0, read_buf.len);

    /* Check status - should be end of stream */
    struct aws_stream_status status;
    ASSERT_SUCCESS(aws_input_stream_get_status(part_streaming_stream, &status));
    ASSERT_TRUE(status.is_valid);
    ASSERT_TRUE(status.is_end_of_stream);

    aws_byte_buf_clean_up(&read_buf);
    aws_input_stream_release(part_streaming_stream);
    s_part_streaming_test_cleanup(&fixture);

    return AWS_OP_SUCCESS;
}
