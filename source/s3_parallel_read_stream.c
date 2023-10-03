/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_parallel_read_stream.h"
#include "aws/s3/private/aws_mmap.h"

#include <aws/common/atomics.h>
#include <aws/common/file.h>
#include <aws/common/string.h>
#include <aws/common/task_scheduler.h>

#include <aws/io/event_loop.h>
#include <aws/io/future.h>
#include <aws/io/stream.h>

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
    return stream;
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

    struct aws_mmap_context *mmap_context;

    struct aws_event_loop_group *reading_elg;
    size_t num_workers;

    struct aws_event_loop **assigned_event_loops;

    struct aws_atomic_var read_count;
};

static void s_para_from_file_destroy(struct aws_parallel_input_stream *stream) {
    struct aws_parallel_input_stream_from_file_impl *impl =
        AWS_CONTAINER_OF(stream, struct aws_parallel_input_stream_from_file_impl, base);
    aws_mmap_context_release(impl->mmap_context);

    aws_event_loop_group_release(impl->reading_elg);
    aws_mem_release(stream->alloc, impl->assigned_event_loops);

    aws_mem_release(stream->alloc, impl);

    return;
}

struct aws_parallel_read_from_file_task_args {
    struct aws_allocator *alloc;

    void *log_id;

    size_t start_position;
    struct aws_future_bool *end_future;
    struct aws_byte_buf *dest;
    struct aws_mmap_context *mmap_context;
};

static void s_s3_parallel_from_file_read_task(struct aws_task *task, void *arg, enum aws_task_status task_status) {
    (void)task_status;
    struct aws_parallel_read_from_file_task_args *args = arg;
    bool error_occurred = true;
    /* TODO: handle the task cancelled. */
    AWS_ASSERT(task_status == AWS_TASK_STATUS_RUN_READY);
    struct aws_future_bool *end_future = args->end_future;
    void *out_start_addr = NULL;
    size_t read_length = args->dest->capacity - args->dest->len;
    void *content =
        aws_mmap_context_map_content(args->mmap_context, read_length, args->start_position, &out_start_addr);

    if (content == NULL) {
        goto error;
    }

    memcpy(args->dest->buffer, content, read_length);
    args->dest->len = args->dest->capacity;

    int error = aws_mmap_context_unmap_content(out_start_addr, read_length);
    /**
     * unmpa only fails on:
     *  - Addresses in the range [addr,addr+len) are outside the valid range for the address space of a process.
     *  - The len argument is 0.
     *  - The addr argument is not a multiple of the page size as returned by sysconf().
     *
     * None of them should happen here.
     */
    AWS_FATAL_ASSERT(!error);

    error_occurred = false;
error:
    aws_mem_release(args->alloc, task);
    if (error_occurred) {
        AWS_LOGF_TRACE(
            AWS_LS_S3_PARALLEL_INPUT_STREAM,
            "id=%p: Read from %zu to %zu finished with error %d (%s)",
            args->log_id,
            args->start_position,
            args->start_position + args->dest->len,
            aws_last_error(),
            aws_error_str(aws_last_error()));
        aws_mem_release(args->alloc, args);
        aws_future_bool_set_error(end_future, aws_last_error());
    } else {
        AWS_LOGF_TRACE(
            AWS_LS_S3_PARALLEL_INPUT_STREAM,
            "id=%p: Read from %zu to %zu finished",
            args->log_id,
            args->start_position,
            args->start_position + args->dest->len);
        aws_mem_release(args->alloc, args);
        aws_future_bool_set_result(end_future, true);
    }
    aws_future_bool_release(end_future);
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
        AWS_LOGF_ERROR(
            AWS_LS_S3_PARALLEL_INPUT_STREAM, "id=%p: The buffer read to cannot fit the data.", (void *)stream);

        aws_future_bool_set_error(future, AWS_ERROR_SHORT_BUFFER);
        return future;
    }

    /* TODO: Not handling the read_length larger than the dest size for now, maybe just remove the end_position. */
    AWS_ASSERT(read_length == dest->capacity - dest->len);

    size_t read_count = aws_atomic_fetch_add(&impl->read_count, 1);
    size_t index = read_count % impl->num_workers;

    /* file handler will be assigned to the same loop every time. */
    struct aws_event_loop *loop = impl->assigned_event_loops[index];
    struct aws_task *read_task = aws_mem_calloc(impl->base.alloc, 1, sizeof(struct aws_task));
    struct aws_parallel_read_from_file_task_args *task_args =
        aws_mem_calloc(impl->base.alloc, 1, sizeof(struct aws_parallel_read_from_file_task_args));

    task_args->alloc = impl->base.alloc;
    task_args->start_position = start_position;
    task_args->dest = dest;
    task_args->end_future = aws_future_bool_acquire(future);
    task_args->mmap_context = impl->mmap_context;
    task_args->log_id = &impl->base;

    aws_task_init(read_task, s_s3_parallel_from_file_read_task, task_args, "s3_parallel_read_task");
    aws_event_loop_schedule_task_now(loop, read_task);

    AWS_LOGF_TRACE(
        AWS_LS_S3_PARALLEL_INPUT_STREAM,
        "id=%p: Read from %zu to %zu requested",
        (void *)stream,
        start_position,
        end_position);

    return future;
}

static struct aws_parallel_input_stream_vtable s_parallel_input_stream_from_file_vtable = {
    .destroy = s_para_from_file_destroy,
    .read = s_para_from_file_read,
};

struct aws_parallel_input_stream *aws_parallel_input_stream_new_from_file(
    struct aws_allocator *allocator,
    const char *file_name,
    struct aws_event_loop_group *reading_elg,
    size_t num_workers) {

    struct aws_parallel_input_stream_from_file_impl *impl =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_parallel_input_stream_from_file_impl));
    impl->reading_elg = aws_event_loop_group_acquire(reading_elg);
    impl->num_workers = num_workers;
    aws_parallel_input_stream_init_base(&impl->base, allocator, &s_parallel_input_stream_from_file_vtable, impl);

    aws_atomic_store_int(&impl->read_count, 0);
    impl->assigned_event_loops = aws_mem_calloc(allocator, num_workers, sizeof(struct aws_event_loop *));

    impl->mmap_context = aws_mmap_context_new(allocator, file_name);
    if (!impl->mmap_context) {
        goto error;
    }

    for (size_t i = 0; i < num_workers; i++) {
        impl->assigned_event_loops[i] = aws_event_loop_group_get_next_loop(reading_elg);
    }

    return &impl->base;
error:
    s_para_from_file_destroy(&impl->base);
    return NULL;
}
