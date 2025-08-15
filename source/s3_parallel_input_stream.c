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

struct aws_parallel_input_stream_from_file_impl {
    struct aws_parallel_input_stream base;

    struct aws_string *file_path;
    struct aws_event_loop_group *reading_elg;
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
    uint64_t offset;
    size_t length;
    struct aws_byte_buf *dest;
};

/* TODO: move the platform specific code to aws-c-common. file.c */
static int s_read_from_file_impl(
    struct aws_string *file_path,
    struct aws_byte_buf *output_buf,
    uint64_t offset,
    size_t length,
    bool *out_eof_reached,
    bool direct_io) {
    /* TODO: support direct io */
    (void)direct_io;
    int rt_code = AWS_OP_ERR;
    FILE *file_stream = aws_fopen_safe(file_path, s_readonly_bytes_mode);
    if (file_stream == NULL) {
        AWS_LOGF_ERROR(AWS_LS_S3_GENERAL, "Failed to open file %s", aws_string_c_str(file_path));
        return AWS_OP_ERR;
    }

    /* seek to the right position and then read */
    if (aws_fseek(file_stream, (int64_t)offset, SEEK_SET)) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_GENERAL,
            "Failed to seek to position %" PRIu64 " in file %s",
            offset,
            aws_string_c_str(file_path));
        goto cleanup;
    }

    size_t actually_read = fread(output_buf->buffer + output_buf->len, 1, length, file_stream);
    if (actually_read == 0 && ferror(file_stream)) {
        AWS_LOGF_ERROR(AWS_LS_S3_GENERAL, "Failed to read %zu bytes from file %s", length, aws_string_c_str(file_path));
        aws_raise_error(aws_translate_and_raise_io_error(errno));
        goto cleanup;
    }
    /* If we cannot fill the length ask for, which means we hit the EOF. */
    *out_eof_reached = (actually_read < length);
    output_buf->len += actually_read;
    AWS_LOGF_TRACE(
        AWS_LS_S3_GENERAL,
        "Successfully read %zu bytes from file %s at position %" PRIu64 "",
        actually_read,
        aws_string_c_str(file_path),
        offset);
    rt_code = AWS_OP_SUCCESS;
cleanup:
    if (file_stream != NULL) {
        fclose(file_stream);
    }
    return rt_code;
}

static void s_s3_parallel_from_file_read_task(struct aws_task *task, void *arg, enum aws_task_status task_status) {
    (void)task_status;
    struct read_task_impl *read_task = arg;
    struct aws_parallel_input_stream_from_file_impl *impl = read_task->para_impl;
    struct aws_future_bool *end_future = read_task->end_future;
    bool eof_reached = false;

    if (s_read_from_file_impl(
            impl->file_path, read_task->dest, read_task->offset, read_task->length, &eof_reached, false)) {
        /* If reading from file failed, set the error on the future and return */
        aws_future_bool_set_error(end_future, aws_last_error());
    } else {
        aws_future_bool_set_result(end_future, eof_reached);
    }

    aws_future_bool_release(end_future);
    aws_mem_release(impl->base.alloc, task);
    aws_mem_release(impl->base.alloc, read_task);
}

struct aws_future_bool *s_para_from_file_read(
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
    /* May need to keep the impl alive */
    read_task->para_impl = impl;

    struct aws_event_loop *loop = aws_event_loop_group_get_next_loop(impl->reading_elg);
    struct aws_task *task = aws_mem_calloc(impl->base.alloc, 1, sizeof(struct aws_task));
    aws_task_init(task, s_s3_parallel_from_file_read_task, read_task, "s3_parallel_read_task");
    aws_event_loop_schedule_task_now(loop, task);
    return future;
}

int s_para_from_file_get_length(struct aws_parallel_input_stream *stream, int64_t *length) {
    struct aws_parallel_input_stream_from_file_impl *impl =
        AWS_CONTAINER_OF(stream, struct aws_parallel_input_stream_from_file_impl, base);
    FILE *file = aws_fopen_safe(impl->file_path, s_readonly_bytes_mode);
    if (!file) {
        return aws_raise_error(AWS_ERROR_FILE_INVALID_PATH);
    }

    int ret_val = aws_file_get_length(file, length);
    fclose(file);
    return ret_val;
}

static struct aws_parallel_input_stream_vtable s_parallel_input_stream_from_file_vtable = {
    .destroy = s_para_from_file_destroy,
    .read = s_para_from_file_read,
    .get_length = s_para_from_file_get_length,
};

struct aws_parallel_input_stream *aws_parallel_input_stream_new_from_file(
    struct aws_allocator *allocator,
    struct aws_byte_cursor file_name,
    struct aws_event_loop_group *reading_elg) {

    struct aws_parallel_input_stream_from_file_impl *impl =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_parallel_input_stream_from_file_impl));

    aws_parallel_input_stream_init_base(&impl->base, allocator, &s_parallel_input_stream_from_file_vtable, impl);
    impl->file_path = aws_string_new_from_cursor(allocator, &file_name);
    impl->reading_elg = aws_event_loop_group_acquire(reading_elg);

    if (!aws_path_exists(impl->file_path)) {
        /* If file path not exists, raise error from errno. */
        aws_translate_and_raise_io_error(errno);
        s_para_from_file_destroy(&impl->base);
        return NULL;
    }

    return &impl->base;
}
