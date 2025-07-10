/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_parallel_input_stream.h"
#include "aws/s3/private/aws_mmap.h"

#include <aws/common/file.h>

#include <aws/io/future.h>
#include <aws/io/stream.h>

#include <errno.h>

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
    uint64_t offset,
    struct aws_byte_buf *dest) {
    /* Ensure the buffer has space available */
    if (dest->len == dest->capacity) {
        struct aws_future_bool *future = aws_future_bool_new(stream->alloc);
        aws_future_bool_set_error(future, AWS_ERROR_SHORT_BUFFER);
        return future;
    }
    struct aws_future_bool *future = stream->vtable->read(stream, offset, dest);
    return future;
}

struct aws_parallel_input_stream_from_file_impl {
    struct aws_parallel_input_stream base;

    struct aws_string *file_path;
};

static void s_para_from_file_destroy(struct aws_parallel_input_stream *stream) {
    struct aws_parallel_input_stream_from_file_impl *impl = stream->impl;

    aws_string_destroy(impl->file_path);

    aws_mem_release(stream->alloc, impl);
}

struct aws_future_bool *s_para_from_file_read(
    struct aws_parallel_input_stream *stream,
    uint64_t offset,
    struct aws_byte_buf *dest) {

    struct aws_future_bool *future = aws_future_bool_new(stream->alloc);
    struct aws_parallel_input_stream_from_file_impl *impl = stream->impl;
    bool success = false;
    struct aws_input_stream *file_stream = NULL;
    struct aws_stream_status status = {
        .is_end_of_stream = false,
        .is_valid = true,
    };

    file_stream = aws_input_stream_new_from_file(stream->alloc, aws_string_c_str(impl->file_path));
    if (!file_stream) {
        goto done;
    }

    if (aws_input_stream_seek(file_stream, offset, AWS_SSB_BEGIN)) {
        goto done;
    }
    /* Keep reading until fill the buffer.
     * Note that we must read() after seek() to determine if we're EOF, the seek alone won't trigger it. */
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
        aws_future_bool_set_result(future, status.is_end_of_stream);
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
    struct aws_byte_cursor file_name) {

    struct aws_parallel_input_stream_from_file_impl *impl =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_parallel_input_stream_from_file_impl));
    aws_parallel_input_stream_init_base(&impl->base, allocator, &s_parallel_input_stream_from_file_vtable, impl);
    impl->file_path = aws_string_new_from_cursor(allocator, &file_name);
    if (!aws_path_exists(impl->file_path)) {
        /* If file path not exists, raise error from errno. */
        aws_translate_and_raise_io_error(errno);
        goto error;
    }
    return &impl->base;
error:
    s_para_from_file_destroy(&impl->base);
    return NULL;
}

/****************** Open the file descriptor every time ***************************/

struct aws_s3_part_streaming_input_stream_impl {
    struct aws_input_stream base;
    struct aws_input_stream *base_stream;
    size_t offset;
    size_t total_length;
    size_t length_read;
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
    test_input_stream->length_read += dest->len;
    if (test_input_stream->length_read > test_input_stream->total_length) {
        size_t gap = test_input_stream->length_read - test_input_stream->total_length;
        size_t new_len = dest->len - gap;
        dest->len = new_len;
        test_input_stream->length_read = test_input_stream->total_length;
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

    status->is_end_of_stream = test_input_stream->length_read == test_input_stream->total_length;
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
    test_input_stream->length_read = 0;
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
    test_input_stream->length_read = 0;
    aws_input_stream_seek(test_input_stream->base_stream, offset, AWS_SSB_BEGIN);

    return &test_input_stream->base;
}

/****************** Take mmap context ***************************/

struct aws_s3_mmap_part_streaming_input_stream_impl {
    struct aws_input_stream base;
    struct aws_allocator *allocator;

    struct aws_mmap_context *mmap_context;
    size_t offset;
    size_t total_length;
    size_t length_read;

    void *content;
    void *page_address;
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
    struct aws_s3_mmap_part_streaming_input_stream_impl *test_input_stream =
        AWS_CONTAINER_OF(stream, struct aws_s3_mmap_part_streaming_input_stream_impl, base);
    /* Map the content */
    void *out_start_addr = NULL;
    size_t read_length =
        aws_min_size(dest->capacity - dest->len, test_input_stream->total_length - test_input_stream->length_read);

    struct aws_byte_cursor content_cursor = aws_byte_cursor_from_array(
        (const uint8_t *)test_input_stream->content + test_input_stream->length_read, read_length);
    test_input_stream->length_read += read_length;
    AWS_FATAL_ASSERT(test_input_stream->length_read <= test_input_stream->total_length);

    int rt = aws_byte_buf_append(dest, &content_cursor);

    return rt;
}

static int s_aws_s3_mmap_part_streaming_input_stream_get_status(
    struct aws_input_stream *stream,
    struct aws_stream_status *status) {
    (void)stream;
    (void)status;

    struct aws_s3_mmap_part_streaming_input_stream_impl *mmap_input_stream =
        AWS_CONTAINER_OF(stream, struct aws_s3_mmap_part_streaming_input_stream_impl, base);

    status->is_end_of_stream = mmap_input_stream->length_read == mmap_input_stream->total_length;
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
    aws_mmap_context_unmap_content(mmap_input_stream->page_address, mmap_input_stream->total_length);
    aws_mmap_context_release(mmap_input_stream->mmap_context);
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
    mmap_input_stream->length_read = 0;
}

struct aws_input_stream *aws_input_stream_new_from_mmap_context(
    struct aws_allocator *allocator,
    struct aws_mmap_context *mmap_context,
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
    mmap_input_stream->length_read = 0;
    mmap_input_stream->mmap_context = aws_mmap_context_acquire(mmap_context);

    mmap_input_stream->content = aws_mmap_context_map_content(
        mmap_input_stream->mmap_context,
        mmap_input_stream->total_length,
        mmap_input_stream->offset + mmap_input_stream->length_read,
        &mmap_input_stream->page_address);
    if (mmap_input_stream->content == NULL) {
        printf("Failed to map the content\n");
        s_aws_s3_mmap_part_streaming_input_stream_destroy(mmap_input_stream);
        return NULL;
    }
    return &mmap_input_stream->base;
}
