/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/s3/private/s3_parallel_input_stream.h>

#include <aws/common/atomics.h>
#include <aws/common/clock.h>
#include <aws/common/file.h>
#include <aws/common/string.h>
#include <aws/common/task_scheduler.h>

#include <aws/io/event_loop.h>
#include <aws/io/future.h>
#include <aws/io/stream.h>

#define ONE_SEC_IN_NS_P ((uint64_t)AWS_TIMESTAMP_NANOS)
#define MAX_TIMEOUT_NS_P (600 * ONE_SEC_IN_NS_P)

void aws_parallel_input_stream_init_base(
    struct aws_parallel_input_stream *stream,
    struct aws_allocator *alloc,
    const struct aws_parallel_input_stream_vtable *vtable,
    void *impl) {

    AWS_ZERO_STRUCT(*stream);
    stream->alloc = alloc;
    stream->vtable = vtable;
    stream->impl = impl;
    aws_ref_count_init(&stream->ref_count, stream, (aws_simple_completion_callback *)vtable->destroy);
}

struct aws_parallel_input_stream *aws_parallel_input_stream_acquire(struct aws_parallel_input_stream *stream) {
    if (stream != NULL) {
        aws_ref_count_acquire(&stream->ref_count);
    }
    return stream;
}

struct aws_parallel_input_stream *aws_parallel_input_stream_release(struct aws_parallel_input_stream *stream) {
    if (stream != NULL) {
        aws_ref_count_release(&stream->ref_count);
    }
    return NULL;
}

struct aws_future_bool *aws_parallel_input_stream_read(
    struct aws_parallel_input_stream *stream,
    size_t start_position,
    size_t end_position,
    struct aws_byte_buf *dest) {
    /* Ensure the buffer has space available */
    if (dest->len == dest->capacity) {
        struct aws_future_bool *future = aws_future_bool_new(stream->alloc);
        aws_future_bool_set_error(future, AWS_ERROR_SHORT_BUFFER);
        return future;
    }

    struct aws_future_bool *future = stream->vtable->read(stream, start_position, end_position, dest);
    AWS_POSTCONDITION(future != NULL);
    return future;
}

struct aws_parallel_input_stream_from_file_impl {
    struct aws_parallel_input_stream base;

    struct aws_string *file_path;
    struct aws_event_loop_group *reading_elg;
    size_t num_of_thread;
};

static void s_para_from_file_destroy(struct aws_parallel_input_stream *stream) {
    struct aws_parallel_input_stream_from_file_impl *impl =
        AWS_CONTAINER_OF(stream, struct aws_parallel_input_stream_from_file_impl, base);

    aws_string_destroy(impl->file_path);
    aws_event_loop_group_release(impl->reading_elg);

    aws_mem_release(stream->alloc, impl);

    return;
}

struct read_task_impl {
    struct aws_parallel_input_stream_from_file_impl *para_impl;

    struct aws_future_bool *end_future;
    size_t offset;
    size_t length;
    struct aws_byte_buf *dest;
};
static void s_s3_parallel_from_file_read_task(struct aws_task *task, void *arg, enum aws_task_status task_status) {
    (void)task_status;
    struct read_task_impl *read_task = arg;
    struct aws_parallel_input_stream_from_file_impl *impl = read_task->para_impl;
    FILE *file_stream = aws_fopen(aws_string_c_str(impl->file_path), "rb");
    AWS_ASSERT(file_stream != NULL);

    /* seek to the right position and then read */
    if (aws_fseek(file_stream, (int64_t)read_task->offset, SEEK_SET)) {
        printf("############aws_fseek failed!");
    }

    size_t actually_read = fread(read_task->dest->buffer, 1, read_task->length, file_stream);
    if (actually_read == 0) {
        printf("############fread failed!");
    }

    fclose(file_stream);
    struct aws_future_bool *end_future = read_task->end_future;
    aws_future_bool_set_result(end_future, true);
    aws_future_bool_release(end_future);

    aws_mem_release(impl->base.alloc, task);
    aws_mem_release(impl->base.alloc, read_task);
}

struct aws_future_bool *s_para_from_file_read(
    struct aws_parallel_input_stream *stream,
    size_t start_position,
    size_t end_position,
    struct aws_byte_buf *dest) {

    struct aws_future_bool *future = aws_future_bool_new(stream->alloc);
    struct aws_parallel_input_stream_from_file_impl *impl =
        AWS_CONTAINER_OF(stream, struct aws_parallel_input_stream_from_file_impl, base);

    size_t read_length = end_position - start_position;
    if (!read_length) {
        /* Nothing to read. Complete the read with success. */
        aws_future_bool_set_result(future, true);
        return future;
    }
    if (read_length > dest->capacity - dest->len) {
        AWS_LOGF_ERROR(AWS_LS_S3_GENERAL, "id=%p: The buffer read to cannot fit the data.", (void *)stream);

        aws_future_bool_set_error(future, AWS_ERROR_SHORT_BUFFER);
        return future;
    }

    struct read_task_impl *read_task = aws_mem_calloc(impl->base.alloc, 1, sizeof(struct read_task_impl));

    AWS_LOGF_TRACE(AWS_LS_S3_GENERAL, "id=%p: Read from %zu to %zu", (void *)stream, start_position, end_position);

    /* Initialize for one read */
    read_task->dest = dest;
    read_task->offset = start_position;
    read_task->length = read_length;
    read_task->end_future = aws_future_bool_acquire(future);
    /* May need to keep the impl alive */
    read_task->para_impl = impl;

    struct aws_event_loop *loop = aws_event_loop_group_get_next_loop(impl->reading_elg);
    struct aws_task *task = aws_mem_calloc(impl->base.alloc, 1, sizeof(struct aws_task));
    aws_task_init(task, s_s3_parallel_from_file_read_task, read_task, "s3_parallel_read_task");
    aws_event_loop_schedule_task_now(loop, task);
    return future;
}

static struct aws_parallel_input_stream_vtable s_parallel_input_stream_from_file_vtable = {
    .destroy = s_para_from_file_destroy,
    .read = s_para_from_file_read,
};

struct aws_parallel_input_stream *aws_parallel_input_stream_new_from_file(
    struct aws_allocator *allocator,
    struct aws_byte_cursor file_name,
    struct aws_event_loop_group *reading_elg) {

    struct aws_parallel_input_stream_from_file_impl *impl =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_parallel_input_stream_from_file_impl));
    impl->file_path = aws_string_new_from_cursor(allocator, &file_name);
    impl->reading_elg = aws_event_loop_group_acquire(reading_elg);
    aws_parallel_input_stream_init_base(&impl->base, allocator, &s_parallel_input_stream_from_file_vtable, impl);

    return &impl->base;
}
/****************** Open the file descriptor every time ***************************/

struct aws_s3_part_streaming_input_stream_impl {
    struct aws_input_stream base;
    struct aws_input_stream *base_stream;
    size_t offset;
    size_t total_length;
    size_t total_length_read;
    struct aws_allocator *allocator;
};

static int s_aws_s3_part_streaming_input_stream_seek(
    struct aws_input_stream *stream,
    int64_t offset,
    enum aws_stream_seek_basis basis) {
    (void)stream;
    (void)offset;
    (void)basis;
    return aws_raise_error(AWS_ERROR_UNSUPPORTED_OPERATION);
}

static int s_aws_s3_part_streaming_input_stream_read(struct aws_input_stream *stream, struct aws_byte_buf *dest) {
    struct aws_s3_part_streaming_input_stream_impl *test_input_stream =
        AWS_CONTAINER_OF(stream, struct aws_s3_part_streaming_input_stream_impl, base);
    int rt = aws_input_stream_read(test_input_stream->base_stream, dest);
    test_input_stream->total_length_read += dest->len;
    if (test_input_stream->total_length_read > test_input_stream->total_length) {
        size_t gap = test_input_stream->total_length_read - test_input_stream->total_length;
        size_t new_len = dest->len - gap;
        dest->len = new_len;
        test_input_stream->total_length_read = test_input_stream->total_length;
    }
    return rt;
}

static int s_aws_s3_part_streaming_input_stream_get_status(
    struct aws_input_stream *stream,
    struct aws_stream_status *status) {
    (void)stream;
    (void)status;

    struct aws_s3_part_streaming_input_stream_impl *test_input_stream =
        AWS_CONTAINER_OF(stream, struct aws_s3_part_streaming_input_stream_impl, base);

    status->is_end_of_stream = test_input_stream->total_length_read == test_input_stream->total_length;
    status->is_valid = true;

    return AWS_OP_SUCCESS;
}

static int s_aws_s3_part_streaming_input_stream_get_length(struct aws_input_stream *stream, int64_t *out_length) {
    AWS_ASSERT(stream != NULL);
    struct aws_s3_part_streaming_input_stream_impl *test_input_stream =
        AWS_CONTAINER_OF(stream, struct aws_s3_part_streaming_input_stream_impl, base);
    *out_length = (int64_t)test_input_stream->total_length;
    return AWS_OP_SUCCESS;
}

static void s_aws_s3_part_streaming_input_stream_destroy(
    struct aws_s3_part_streaming_input_stream_impl *test_input_stream) {
    aws_input_stream_release(test_input_stream->base_stream);
    aws_mem_release(test_input_stream->allocator, test_input_stream);
}

static struct aws_input_stream_vtable s_aws_s3_part_streaming_input_stream_vtable = {
    .seek = s_aws_s3_part_streaming_input_stream_seek,
    .read = s_aws_s3_part_streaming_input_stream_read,
    .get_status = s_aws_s3_part_streaming_input_stream_get_status,
    .get_length = s_aws_s3_part_streaming_input_stream_get_length,
};

void aws_s3_part_streaming_input_stream_reset(struct aws_input_stream *stream) {
    struct aws_s3_part_streaming_input_stream_impl *test_input_stream =
        AWS_CONTAINER_OF(stream, struct aws_s3_part_streaming_input_stream_impl, base);
    test_input_stream->total_length_read = 0;
    aws_input_stream_seek(test_input_stream->base_stream, test_input_stream->offset, AWS_SSB_BEGIN);
}

struct aws_input_stream *aws_input_stream_new_from_parallel(
    struct aws_allocator *allocator,
    struct aws_parallel_input_stream *parallel_stream,
    uint64_t offset,
    size_t request_body_size) {

    struct aws_s3_part_streaming_input_stream_impl *test_input_stream =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_part_streaming_input_stream_impl));
    aws_ref_count_init(
        &test_input_stream->base.ref_count,
        test_input_stream,
        (aws_simple_completion_callback *)s_aws_s3_part_streaming_input_stream_destroy);
    test_input_stream->allocator = allocator;
    test_input_stream->base.vtable = &s_aws_s3_part_streaming_input_stream_vtable;

    struct aws_parallel_input_stream_from_file_impl *impl = parallel_stream->impl;
    test_input_stream->base_stream = aws_input_stream_new_from_file(allocator, aws_string_c_str(impl->file_path));
    AWS_FATAL_ASSERT(test_input_stream->base_stream != NULL);
    test_input_stream->total_length = request_body_size;
    test_input_stream->offset = offset;
    test_input_stream->total_length_read = 0;
    aws_input_stream_seek(test_input_stream->base_stream, offset, AWS_SSB_BEGIN);

    return &test_input_stream->base;
}

/****************** Take mmap context ***************************/

struct aws_s3_mmap_part_streaming_input_stream_impl {
    struct aws_input_stream base;
    struct aws_allocator *allocator;

    struct aws_parallel_input_stream *stream;
    size_t offset;

    size_t chunk_load_size;
    void *page_address;
    size_t in_chunk_offset;

    size_t total_length;
    size_t total_length_read;

    struct aws_byte_buf *reading_chunk_buf;
    struct aws_byte_buf *loading_chunk_buf;
    struct aws_future_bool *loading_future;

    struct aws_byte_buf chunk_buf_1;
    struct aws_byte_buf chunk_buf_2;
};

static int s_aws_s3_mmap_part_streaming_input_stream_seek(
    struct aws_input_stream *stream,
    int64_t offset,
    enum aws_stream_seek_basis basis) {
    (void)stream;
    (void)offset;
    (void)basis;
    return aws_raise_error(AWS_ERROR_UNSUPPORTED_OPERATION);
}

static int s_aws_s3_mmap_part_streaming_input_stream_read(struct aws_input_stream *stream, struct aws_byte_buf *dest) {
    struct aws_s3_mmap_part_streaming_input_stream_impl *impl =
        AWS_CONTAINER_OF(stream, struct aws_s3_mmap_part_streaming_input_stream_impl, base);
    /* Map the content */
    size_t read_length = aws_min_size(dest->capacity - dest->len, impl->total_length - impl->total_length_read);

    if (impl->in_chunk_offset == SIZE_MAX) {
        /* The reading buf is invalid. Block until the loading buf is available. */
        AWS_ASSERT(impl->loading_future != NULL);
        aws_future_bool_wait(impl->loading_future, MAX_TIMEOUT_NS_P);
        impl->loading_future = aws_future_bool_release(impl->loading_future);
        /* Swap the reading the loading pointer. */
        AWS_ASSERT(impl->reading_chunk_buf->len == 0);
        struct aws_byte_buf *tmp = impl->reading_chunk_buf;
        impl->reading_chunk_buf = impl->loading_chunk_buf;
        impl->loading_chunk_buf = tmp;
        size_t new_offset = impl->offset + impl->total_length_read + impl->chunk_load_size;
        /* Kick off loading the next chunk. */
        impl->loading_future = aws_parallel_input_stream_read(
            impl->stream, new_offset, new_offset + impl->chunk_load_size, impl->loading_chunk_buf);
        impl->in_chunk_offset = 0;
    }
    read_length = aws_min_size(read_length, impl->chunk_load_size - impl->in_chunk_offset);
    struct aws_byte_cursor chunk_cursor = aws_byte_cursor_from_buf(impl->reading_chunk_buf);
    aws_byte_cursor_advance(&chunk_cursor, impl->in_chunk_offset);
    chunk_cursor.len = read_length;
    int rt = aws_byte_buf_append(dest, &chunk_cursor);
    impl->in_chunk_offset += read_length;
    impl->total_length_read += read_length;

    if (impl->in_chunk_offset == impl->chunk_load_size) {
        /* We finished reading the reading buffer, reset it. */
        aws_byte_buf_reset(impl->reading_chunk_buf, false);
        impl->in_chunk_offset = SIZE_MAX;
    }

    return rt;
}

static int s_aws_s3_mmap_part_streaming_input_stream_get_status(
    struct aws_input_stream *stream,
    struct aws_stream_status *status) {
    (void)stream;
    (void)status;

    struct aws_s3_mmap_part_streaming_input_stream_impl *mmap_input_stream =
        AWS_CONTAINER_OF(stream, struct aws_s3_mmap_part_streaming_input_stream_impl, base);

    status->is_end_of_stream = mmap_input_stream->total_length_read == mmap_input_stream->total_length;
    status->is_valid = true;

    return AWS_OP_SUCCESS;
}

static int s_aws_s3_mmap_part_streaming_input_stream_get_length(struct aws_input_stream *stream, int64_t *out_length) {
    AWS_ASSERT(stream != NULL);
    struct aws_s3_mmap_part_streaming_input_stream_impl *mmap_input_stream =
        AWS_CONTAINER_OF(stream, struct aws_s3_mmap_part_streaming_input_stream_impl, base);
    *out_length = (int64_t)mmap_input_stream->total_length;
    return AWS_OP_SUCCESS;
}

static void s_aws_s3_mmap_part_streaming_input_stream_destroy(
    struct aws_s3_mmap_part_streaming_input_stream_impl *mmap_input_stream) {
    aws_parallel_input_stream_release(mmap_input_stream->stream);
    aws_mem_release(mmap_input_stream->allocator, mmap_input_stream);
}

static struct aws_input_stream_vtable s_aws_s3_mmap_part_streaming_input_stream_vtable = {
    .seek = s_aws_s3_mmap_part_streaming_input_stream_seek,
    .read = s_aws_s3_mmap_part_streaming_input_stream_read,
    .get_status = s_aws_s3_mmap_part_streaming_input_stream_get_status,
    .get_length = s_aws_s3_mmap_part_streaming_input_stream_get_length,
};

void aws_s3_mmap_part_streaming_input_stream_reset(struct aws_input_stream *stream) {
    struct aws_s3_mmap_part_streaming_input_stream_impl *mmap_input_stream =
        AWS_CONTAINER_OF(stream, struct aws_s3_mmap_part_streaming_input_stream_impl, base);
    mmap_input_stream->total_length_read = 0;
}

struct aws_input_stream *aws_input_stream_new_from_mmap_context(
    struct aws_allocator *allocator,
    struct aws_parallel_input_stream *stream,
    uint64_t offset,
    size_t request_body_size) {

    struct aws_s3_mmap_part_streaming_input_stream_impl *mmap_input_stream =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_mmap_part_streaming_input_stream_impl));
    aws_ref_count_init(
        &mmap_input_stream->base.ref_count,
        mmap_input_stream,
        (aws_simple_completion_callback *)s_aws_s3_mmap_part_streaming_input_stream_destroy);
    mmap_input_stream->allocator = allocator;
    mmap_input_stream->base.vtable = &s_aws_s3_mmap_part_streaming_input_stream_vtable;

    mmap_input_stream->total_length = request_body_size;
    mmap_input_stream->offset = offset;

    mmap_input_stream->in_chunk_offset = SIZE_MAX;
    mmap_input_stream->chunk_load_size = 8 * 1024 * 1024;

    mmap_input_stream->stream = aws_parallel_input_stream_acquire(stream);
    aws_byte_buf_init(&mmap_input_stream->chunk_buf_1, allocator, mmap_input_stream->chunk_load_size);
    aws_byte_buf_init(&mmap_input_stream->chunk_buf_2, allocator, mmap_input_stream->chunk_load_size);

    mmap_input_stream->loading_chunk_buf = &mmap_input_stream->chunk_buf_1;
    mmap_input_stream->reading_chunk_buf = &mmap_input_stream->chunk_buf_1;

    /* Start to load into the loading buffer. */
    mmap_input_stream->loading_future = aws_parallel_input_stream_read(
        stream, offset, offset + mmap_input_stream->chunk_load_size, mmap_input_stream->loading_chunk_buf);

    return &mmap_input_stream->base;
}
