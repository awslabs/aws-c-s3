/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_parallel_input_stream.h"

#include <aws/common/file.h>
#include <aws/common/task_scheduler.h>

#include <aws/io/event_loop.h>
#include <aws/io/future.h>
#include <aws/io/stream.h>

#include <errno.h>
#include <inttypes.h>

AWS_STATIC_STRING_FROM_LITERAL(s_readonly_bytes_mode, "rb");

void aws_parallel_input_stream_init_base(
    struct aws_parallel_input_stream *stream,
    struct aws_allocator *alloc,
    const struct aws_parallel_input_stream_vtable *vtable,
    void *impl) {

    AWS_ZERO_STRUCT(*stream);
    stream->alloc = alloc;
    stream->vtable = vtable;
    stream->impl = impl;
    stream->shutdown_future = aws_future_void_new(alloc);
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

int aws_parallel_input_stream_get_length(struct aws_parallel_input_stream *stream, int64_t *out_length) {
    if (stream->vtable->get_length) {
        return stream->vtable->get_length(stream, out_length);
    } else {
        return aws_raise_error(AWS_ERROR_UNSUPPORTED_OPERATION);
    }
}

struct aws_future_bool *aws_parallel_input_stream_read(
    struct aws_parallel_input_stream *stream,
    uint64_t offset,
    size_t max_length,
    struct aws_byte_buf *dest) {
    /* Ensure the buffer has space available */
    if (dest->len == dest->capacity) {
        struct aws_future_bool *future = aws_future_bool_new(stream->alloc);
        aws_future_bool_set_error(future, AWS_ERROR_SHORT_BUFFER);
        return future;
    }

    struct aws_future_bool *future = stream->vtable->read(stream, offset, max_length, dest);
    AWS_POSTCONDITION(future != NULL);
    return future;
}

struct aws_future_void *aws_parallel_input_stream_get_shutdown_future(struct aws_parallel_input_stream *stream) {
    return aws_future_void_acquire(stream->shutdown_future);
}

struct aws_parallel_input_stream_from_file_impl {
    struct aws_parallel_input_stream base;

    struct aws_string *file_path;
    struct aws_event_loop_group *reading_elg;

    bool direct_io_read;
};

static void s_parallel_from_file_destroy(struct aws_parallel_input_stream *stream) {
    struct aws_parallel_input_stream_from_file_impl *impl =
        AWS_CONTAINER_OF(stream, struct aws_parallel_input_stream_from_file_impl, base);

    aws_string_destroy(impl->file_path);
    aws_event_loop_group_release(impl->reading_elg);

    aws_future_void_set_result(stream->shutdown_future);
    aws_future_void_release(stream->shutdown_future);
    aws_mem_release(stream->alloc, impl);

    return;
}

struct read_task_impl {
    struct aws_parallel_input_stream *para_stream;

    struct aws_future_bool *end_future;
    uint64_t offset;
    size_t length;
    struct aws_byte_buf *dest;
};

static void s_s3_parallel_from_file_read_task(struct aws_task *task, void *arg, enum aws_task_status task_status) {
    (void)task_status;
    struct read_task_impl *read_task = arg;
    struct aws_parallel_input_stream *stream = read_task->para_stream;
    struct aws_parallel_input_stream_from_file_impl *impl =
        AWS_CONTAINER_OF(stream, struct aws_parallel_input_stream_from_file_impl, base);
    struct aws_future_bool *end_future = read_task->end_future;
    bool eof_reached = false;
    size_t actually_read = 0;
    int error_code = AWS_ERROR_SUCCESS;
    if (impl->direct_io_read) {
        /* Try direct IO. */
        if (aws_file_path_read_from_offset_direct_io(
                impl->file_path, read_task->offset, read_task->length, read_task->dest, &actually_read)) {
            if (aws_last_error() == AWS_ERROR_UNSUPPORTED_OPERATION) {
                /* Direct IO not supported, fallback to normal read */
                /* Log the warning */
                AWS_LOGF_WARN(
                    AWS_LS_S3_GENERAL,
                    "Direct IO not supported, fallback to normal read. File path: %s",
                    aws_string_c_str(impl->file_path));
                /* Set direct IO to be false to avoid extra checks. */
                impl->direct_io_read = false;
                aws_reset_error();
            } else {
                error_code = aws_last_error();
                goto finish;
            }
        } else {
            /* Succeed. */
            goto finish;
        }
    }

    if (aws_file_path_read_from_offset(
            impl->file_path, read_task->offset, read_task->length, read_task->dest, &actually_read)) {
        error_code = aws_last_error();
    }

finish:
    if (error_code != AWS_ERROR_SUCCESS) {
        aws_future_bool_set_error(end_future, error_code);
    } else {
        /* If the reading length is smaller than expected, and no error raised, we encountered the EOS. */
        /* The length is guaranteed to be not larger than the available space in the buffer.  */
        eof_reached = (actually_read < read_task->length);
        aws_future_bool_set_result(end_future, eof_reached);
    }
    aws_future_bool_release(end_future);
    aws_mem_release(stream->alloc, task);
    aws_mem_release(stream->alloc, read_task);
    aws_parallel_input_stream_release(stream);
}

struct aws_future_bool *s_parallel_from_file_read(
    struct aws_parallel_input_stream *stream,
    uint64_t offset,
    size_t max_length,
    struct aws_byte_buf *dest) {

    struct aws_future_bool *future = aws_future_bool_new(stream->alloc);
    struct aws_parallel_input_stream_from_file_impl *impl =
        AWS_CONTAINER_OF(stream, struct aws_parallel_input_stream_from_file_impl, base);

    /* Calculate how much we can read based on available buffer space and max_length */
    size_t available_space = dest->capacity - dest->len;
    size_t length = aws_min_size(available_space, max_length);

    if (length == 0) {
        /* Nothing to read. Complete the read with success. */
        aws_future_bool_set_result(future, false);
        return future;
    }

    struct read_task_impl *read_task = aws_mem_calloc(impl->base.alloc, 1, sizeof(struct read_task_impl));

    AWS_LOGF_TRACE(AWS_LS_S3_GENERAL, "id=%p: Read %zu bytes from offset %" PRIu64 "", (void *)stream, length, offset);

    /* Initialize for one read */
    read_task->dest = dest;

    AWS_FATAL_ASSERT(dest->buffer);
    read_task->offset = offset;
    read_task->length = length;
    read_task->end_future = aws_future_bool_acquire(future);
    read_task->para_stream = aws_parallel_input_stream_acquire(&impl->base);

    struct aws_event_loop *loop = aws_event_loop_group_get_next_loop(impl->reading_elg);
    struct aws_task *task = aws_mem_calloc(impl->base.alloc, 1, sizeof(struct aws_task));
    aws_task_init(task, s_s3_parallel_from_file_read_task, read_task, "s3_parallel_read_task");
    aws_event_loop_schedule_task_now(loop, task);
    return future;
}

int s_parallel_from_file_get_length(struct aws_parallel_input_stream *stream, int64_t *length) {
    struct aws_parallel_input_stream_from_file_impl *impl =
        AWS_CONTAINER_OF(stream, struct aws_parallel_input_stream_from_file_impl, base);
    FILE *file = aws_fopen_safe(impl->file_path, s_readonly_bytes_mode);
    if (!file) {
        return AWS_OP_ERR;
    }

    int ret_val = aws_file_get_length(file, length);
    fclose(file);
    return ret_val;
}

static struct aws_parallel_input_stream_vtable s_parallel_input_stream_from_file_vtable = {
    .destroy = s_parallel_from_file_destroy,
    .read = s_parallel_from_file_read,
    .get_length = s_parallel_from_file_get_length,
};

struct aws_parallel_input_stream *aws_parallel_input_stream_new_from_file(
    struct aws_allocator *allocator,
    struct aws_byte_cursor file_name,
    struct aws_event_loop_group *reading_elg,
    bool direct_io_read) {

    struct aws_parallel_input_stream_from_file_impl *impl =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_parallel_input_stream_from_file_impl));

    aws_parallel_input_stream_init_base(&impl->base, allocator, &s_parallel_input_stream_from_file_vtable, impl);
    impl->file_path = aws_string_new_from_cursor(allocator, &file_name);
    impl->reading_elg = aws_event_loop_group_acquire(reading_elg);
    impl->direct_io_read = direct_io_read;

    if (!aws_path_exists(impl->file_path)) {
        /* If file path not exists, raise error from errno. */
        aws_translate_and_raise_io_error(errno);
        s_parallel_from_file_destroy(&impl->base);
        return NULL;
    }

    return &impl->base;
}
