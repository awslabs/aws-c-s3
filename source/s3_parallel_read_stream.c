/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_parallel_read_stream.h"

#include <aws/common/atomics.h>
#include <aws/common/file.h>
#include <aws/common/string.h>
#include <aws/common/task_scheduler.h>

#include <aws/io/event_loop.h>
#include <aws/io/future.h>
#include <aws/io/stream.h>

#include <errno.h>
#include <sys/stat.h>

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
    size_t offset,
    struct aws_byte_buf *dest) {
    /* Ensure the buffer has space available */
    if (dest->len == dest->capacity) {
        struct aws_future_bool *future = aws_future_bool_new(stream->alloc);
        aws_future_bool_set_error(future, AWS_ERROR_SHORT_BUFFER);
        return future;
    }
    /* TODO: restore the buffer on failure. */
    struct aws_future_bool *future = stream->vtable->read(stream, offset, dest);
    return future;
}

struct aws_parallel_input_stream_from_file_impl {
    struct aws_parallel_input_stream base;

    struct aws_string *file_path;
    uint64_t last_modified_time;
};

static void s_para_from_file_destroy(struct aws_parallel_input_stream *stream) {
    struct aws_parallel_input_stream_from_file_impl *impl =
        AWS_CONTAINER_OF(stream, struct aws_parallel_input_stream_from_file_impl, base);

    aws_string_destroy(impl->file_path);

    aws_mem_release(stream->alloc, impl);

    return;
}

static int s_get_last_modified_time(const char *file_name, uint64_t *out_time) {
    struct stat attrib;
    if (stat(file_name, &attrib)) {
        return aws_translate_and_raise_io_error(errno);
    }
    *out_time = (uint64_t)attrib.st_mtime;
    return AWS_OP_SUCCESS;
}

struct aws_future_bool *s_para_from_file_read(
    struct aws_parallel_input_stream *stream,
    size_t offset,
    struct aws_byte_buf *dest) {

    struct aws_future_bool *future = aws_future_bool_new(stream->alloc);
    struct aws_parallel_input_stream_from_file_impl *impl =
        AWS_CONTAINER_OF(stream, struct aws_parallel_input_stream_from_file_impl, base);
    bool success = false;
    uint64_t last_modified_time = 0;
    if (s_get_last_modified_time(aws_string_c_str(impl->file_path), &last_modified_time)) {
        goto done;
    }
    if (!last_modified_time == impl->last_modified_time) {
        aws_raise_error(AWS_ERROR_S3_FILE_MODIFIED);
        goto done;
    }
    struct aws_input_stream *file_stream =
        aws_input_stream_new_from_file(stream->alloc, aws_string_c_str(impl->file_path));
    if (aws_input_stream_seek(file_stream, offset, AWS_SSB_BEGIN)) {
        goto done;
    }
    struct aws_stream_status status = {
        .is_end_of_stream = false,
        .is_valid = true,
    };
    /* Keep reading until fill the buffer */
    while ((dest->len < dest->capacity) && !status.is_end_of_stream) {
        /* Read from stream */
        if (aws_input_stream_read(file_stream, dest) != AWS_OP_SUCCESS) {
            goto done;
        }

        /* Check if stream is done */
        if (aws_input_stream_get_status(file_stream, &status) != AWS_OP_SUCCESS) {
            goto done;
        }
    }
    success = true;
done:
    if (success) {
        aws_future_bool_set_result(future, true);
    } else {
        aws_future_bool_set_error(future, aws_last_error());
    }

    aws_input_stream_release(file_stream);

    return future;
}

static struct aws_parallel_input_stream_vtable s_parallel_input_stream_from_file_vtable = {
    .destroy = s_para_from_file_destroy,
    .read = s_para_from_file_read,
};

struct aws_parallel_input_stream *aws_parallel_input_stream_new_from_file(
    struct aws_allocator *allocator,
    const struct aws_byte_cursor *file_name) {

    struct aws_parallel_input_stream_from_file_impl *impl =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_parallel_input_stream_from_file_impl));
    aws_parallel_input_stream_init_base(&impl->base, allocator, &s_parallel_input_stream_from_file_vtable, impl);
    impl->file_path = aws_string_new_from_cursor(allocator, file_name);
    if (s_get_last_modified_time(aws_string_c_str(impl->file_path), &impl->last_modified_time)) {
        goto error;
    }

    return &impl->base;
error:
    s_para_from_file_destroy(&impl->base);
    return NULL;
}
