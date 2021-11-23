/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_checksums.h"
#include <aws/io/stream.h>

struct aws_chunk_stream {
    /* aws_input_stream_byte_cursor provides our actual functionality  */
    struct aws_input_stream *old_stream;
    struct aws_checksum *checksum;
};

static int s_aws_input_chunk_stream_seek(
    struct aws_input_stream *stream,
    int64_t offset,
    enum aws_stream_seek_basis basis) {

    struct aws_chunk_stream *impl = stream->impl;
    return aws_input_stream_seek(impl->old_stream, offset, basis);
}

static int s_aws_input_chunk_stream_read(struct aws_input_stream *stream, struct aws_byte_buf *dest) {
    struct aws_chunk_stream *impl = stream->impl;

    size_t start = dest->len;
    int err = aws_input_stream_read(impl->old_stream, dest);
    size_t end = dest->len;
    struct aws_byte_cursor to_sum = aws_byte_cursor_from_buf(dest);
    to_sum.ptr += start;
    to_sum.len = end - start;
    if (!err) {
        return aws_checksum_update(impl->checksum, &to_sum);
    }
    return err;
}

static int s_aws_input_chunk_stream_get_status(struct aws_input_stream *stream, struct aws_stream_status *status) {
    struct aws_chunk_stream *impl = stream->impl;
    return aws_input_stream_get_status(impl->old_stream, status);
}

static int s_aws_input_chunk_stream_get_length(struct aws_input_stream *stream, int64_t *out_length) {
    struct aws_chunk_stream *impl = stream->impl;
    return aws_input_stream_get_length(impl->old_stream, out_length);
}

static void s_aws_input_chunk_stream_destroy(struct aws_input_stream *stream) {
    if (stream) {
        struct aws_chunk_stream *impl = stream->impl;
        aws_input_stream_destroy(impl->old_stream);
        aws_mem_release(stream->allocator, stream);
        /* we don't own or destroy the aws_checksum, since we'll need it to finazile and extract the checksum */
    }
}

static struct aws_input_stream_vtable s_aws_input_chunk_stream_vtable = {
    .seek = s_aws_input_chunk_stream_seek,
    .read = s_aws_input_chunk_stream_read,
    .get_status = s_aws_input_chunk_stream_get_status,
    .get_length = s_aws_input_chunk_stream_get_length,
    .destroy = s_aws_input_chunk_stream_destroy,
};

struct aws_input_stream *aws_chunk_stream_new(
    struct aws_allocator *alloc,
    struct aws_input_stream *existing_stream,
    struct aws_checksum *checksum) {

    struct aws_input_stream *stream = NULL;
    struct aws_chunk_stream *impl = NULL;
    aws_mem_acquire_many(alloc, 2, &stream, sizeof(struct aws_input_stream), &impl, sizeof(struct aws_chunk_stream));
    AWS_FATAL_ASSERT(stream);

    AWS_ZERO_STRUCT(*stream);
    AWS_ZERO_STRUCT(*impl);

    stream->allocator = alloc;
    stream->impl = impl;
    stream->vtable = &s_aws_input_chunk_stream_vtable;

    impl->old_stream = existing_stream;
    impl->checksum = checksum;
    AWS_FATAL_ASSERT(impl->old_stream);

    return stream;
}
