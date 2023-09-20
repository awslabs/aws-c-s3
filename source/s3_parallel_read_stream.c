/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/s3/private/s3_parallel_read_stream.h>

#include "aws/s3/private/s3_tracing.h"
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

    struct aws_string *file_path;
    struct aws_event_loop_group *reading_elg;
    size_t num_to_split;

    struct {
        /* TODO: needs to protect this on_going from multi-thread */
        bool on_going;
        size_t start_position;
        size_t total_read_length;
        struct aws_atomic_var read_count;
        struct aws_atomic_var read_complete_count;
        struct aws_future_bool *end_future;
        struct aws_atomic_var last_error;
        struct aws_byte_buf *dest;
    } current_read;
};

static void s_para_from_file_destroy(struct aws_parallel_input_stream *stream) {
    struct aws_parallel_input_stream_from_file_impl *impl =
        AWS_CONTAINER_OF(stream, struct aws_parallel_input_stream_from_file_impl, base);
    AWS_FATAL_ASSERT(impl->current_read.on_going == false);

    aws_string_destroy(impl->file_path);
    aws_event_loop_group_release(impl->reading_elg);

    aws_mem_release(stream->alloc, impl);

    return;
}

static int s_read_from_file(struct aws_parallel_input_stream_from_file_impl *impl, size_t read_index) {

    size_t number_bytes_per_read = impl->current_read.total_read_length / impl->num_to_split;
    if (number_bytes_per_read == 0) {
        if (read_index > 0) {
            return AWS_OP_SUCCESS;
        } else {
            number_bytes_per_read = impl->current_read.total_read_length;
        }
    }
    size_t current_read_length = number_bytes_per_read;
    if (read_index == impl->num_to_split - 1) {
        /* Last part, adjust the size */
        current_read_length += impl->current_read.total_read_length % impl->num_to_split;
    }

    size_t buffer_start_pos = read_index * number_bytes_per_read;
    size_t file_start_pos = impl->current_read.start_position + buffer_start_pos;

    /* TODO: I am using FILE instead our input stream to read to a certain block of memory instead of byte buffer */
    FILE *file_stream = aws_fopen(aws_string_c_str(impl->file_path), "rb");
    if (!file_stream) {
        AWS_LOGF_ERROR(AWS_LS_S3_PARALLEL_INPUT_STREAM, "id=%p: Error during fopen", (void *)&impl->base);
        return AWS_OP_ERR;
    }

    /* seek to the right position and then read */
    if (aws_fseek(file_stream, (int64_t)file_start_pos, SEEK_SET)) {
        AWS_LOGF_ERROR(AWS_LS_S3_PARALLEL_INPUT_STREAM, "id=%p: Error during fseek", (void *)&impl->base);
        goto error;
    }
    size_t actually_read =
        fread(impl->current_read.dest->buffer + buffer_start_pos, 1, current_read_length, file_stream);
    if (actually_read == 0) {
        if (ferror(file_stream)) {
            /* TODO: Some error code */
            aws_raise_error(AWS_IO_STREAM_READ_FAILED);
            goto error;
        }
    }

    if (actually_read < current_read_length) {
        /* TODO: Some error code. Maybe read again fill the buffer? */
        aws_raise_error(AWS_IO_STREAM_READ_FAILED);
        goto error;
    }

    fclose(file_stream);
    return AWS_OP_SUCCESS;

error:
    fclose(file_stream);
    return AWS_OP_ERR;
}

static void s_current_read_completes(struct aws_parallel_input_stream_from_file_impl *impl, int error_code) {
    if (!error_code) {
        /* Hack to read into the dest directly */
        impl->current_read.dest->len = impl->current_read.total_read_length;
    }
    /* TODO: Restore the dest buffer? Or, as the len is not changed, just ignore it. */
    impl->current_read.on_going = false;
    struct aws_future_bool *end_future = impl->current_read.end_future;

    if (error_code) {
        aws_future_bool_set_error(impl->current_read.end_future, error_code);
    } else {
        aws_future_bool_set_result(impl->current_read.end_future, true);
    }
    aws_future_bool_release(end_future);
}

static void s_s3_parallel_from_file_read_task(struct aws_task *task, void *arg, enum aws_task_status task_status) {
    (void)task_status;
    __itt_task_begin(s3_domain, __itt_null, __itt_null, __itt_string_handle_create("parallel_read"));
    struct aws_parallel_input_stream_from_file_impl *impl = arg;

    /* TODO: handle the task cancelled. */
    AWS_ASSERT(task_status == AWS_TASK_STATUS_RUN_READY);

    size_t read_index = aws_atomic_fetch_add(&impl->current_read.read_count, 1);

    if (s_read_from_file(impl, read_index)) {
        /* If there are multiple errors, we only need the latest one. */
        aws_atomic_store_int(&impl->current_read.last_error, aws_last_error());
    }

    size_t read_completed = aws_atomic_fetch_add(&impl->current_read.read_complete_count, 1);
    aws_mem_release(impl->base.alloc, task);
    if (read_completed == impl->num_to_split - 1) {
        /* We just completed the last read, now we can finish the read */
        size_t error = aws_atomic_load_int(&impl->current_read.last_error);
        s_current_read_completes(impl, (int)error);
    }
    __itt_task_end(s3_domain);
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
    if (impl->current_read.on_going) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_PARALLEL_INPUT_STREAM,
            "id=%p: Error trying to read while the previous read is still ongoing",
            (void *)stream);
        aws_future_bool_set_error(future, AWS_ERROR_INVALID_STATE);
        return future;
    }
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

    AWS_LOGF_TRACE(
        AWS_LS_S3_PARALLEL_INPUT_STREAM, "id=%p: Read from %zu to %zu", (void *)stream, start_position, end_position);

    /* Initialize for one read */
    aws_atomic_store_int(&impl->current_read.read_count, 0);
    aws_atomic_store_int(&impl->current_read.read_complete_count, 0);
    impl->current_read.start_position = start_position;
    impl->current_read.total_read_length = read_length;
    impl->current_read.end_future = aws_future_bool_acquire(future);
    impl->current_read.on_going = true;
    impl->current_read.dest = dest;
    aws_atomic_store_int(&impl->current_read.last_error, AWS_ERROR_SUCCESS);

    for (size_t i = 0; i < impl->num_to_split; i++) {
        struct aws_event_loop *loop = aws_event_loop_group_get_next_loop(impl->reading_elg);
        struct aws_task *read_task = aws_mem_calloc(impl->base.alloc, 1, sizeof(struct aws_task));
        aws_task_init(read_task, s_s3_parallel_from_file_read_task, impl, "s3_parallel_read_task");
        aws_event_loop_schedule_task_now(loop, read_task);
    }
    return future;
}

static struct aws_parallel_input_stream_vtable s_parallel_input_stream_from_file_vtable = {
    .destroy = s_para_from_file_destroy,
    .read = s_para_from_file_read,
};

struct aws_parallel_input_stream *aws_parallel_input_stream_new_from_file(
    struct aws_allocator *allocator,
    struct aws_byte_cursor file_name,
    struct aws_event_loop_group *reading_elg,
    size_t num_to_split) {

    struct aws_parallel_input_stream_from_file_impl *impl =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_parallel_input_stream_from_file_impl));
    impl->file_path = aws_string_new_from_cursor(allocator, &file_name);
    impl->reading_elg = aws_event_loop_group_acquire(reading_elg);
    impl->num_to_split = num_to_split;
    aws_parallel_input_stream_init_base(&impl->base, allocator, &s_parallel_input_stream_from_file_vtable, impl);

    aws_atomic_store_int(&impl->current_read.read_count, 0);
    aws_atomic_store_int(&impl->current_read.read_complete_count, 0);
    return &impl->base;
}
