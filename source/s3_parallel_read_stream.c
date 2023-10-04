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

    struct aws_string *file_path;
};

static void s_para_from_file_destroy(struct aws_parallel_input_stream *stream) {
    struct aws_parallel_input_stream_from_file_impl *impl =
        AWS_CONTAINER_OF(stream, struct aws_parallel_input_stream_from_file_impl, base);

    aws_string_destroy(impl->file_path);

    aws_mem_release(stream->alloc, impl);

    return;
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

    struct aws_input_stream *file_stream =
        aws_input_stream_new_from_file(stream->alloc, aws_string_c_str(impl->file_path));
    aws_input_stream_seek(file_stream, start_position, AWS_SSB_BEGIN);
    aws_input_stream_read(file_stream, dest);
    aws_future_bool_set_result(future, true);
    aws_input_stream_release(file_stream);

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
    aws_parallel_input_stream_init_base(&impl->base, allocator, &s_parallel_input_stream_from_file_vtable, impl);
    impl->file_path = aws_string_new_from_c_str(allocator, file_name);

    return &impl->base;
error:
    s_para_from_file_destroy(&impl->base);
    return NULL;
}
