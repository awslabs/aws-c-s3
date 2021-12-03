/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_checksums.h"
#include <aws/common/string.h>
#include <aws/io/stream.h>
#include <inttypes.h>

AWS_STATIC_STRING_FROM_LITERAL(s_pre_chunk, ";\r\n");
AWS_STATIC_STRING_FROM_LITERAL(s_final_chunk, "\r\n0;\r\n");
AWS_STATIC_STRING_FROM_LITERAL(s_post_trailer, "\r\n\r\n");

struct aws_chunk_stream;

typedef int(set_stream_fn)(struct aws_chunk_stream *parent_stream);

struct aws_chunk_stream {
    /* aws_input_stream_byte_cursor provides our actual functionality  */
    struct aws_input_stream *current_stream;
    struct aws_input_stream *checksum_stream;
    struct aws_byte_buf *checksum_result;
    set_stream_fn *set_current_stream_fn;
};

static int s_set_null_stream(struct aws_chunk_stream *parent_stream) {
    aws_input_stream_destroy(parent_stream->current_stream);
    parent_stream->current_stream = NULL;
    parent_stream->set_current_stream_fn = NULL;
    return AWS_OP_SUCCESS;
}

static int s_set_post_chunk_stream(struct aws_chunk_stream *parent_stream) {
    aws_input_stream_destroy(parent_stream->current_stream);
    struct aws_byte_cursor final_chunk_cursor = aws_byte_cursor_from_string(s_final_chunk);
    struct aws_byte_cursor post_trailer_cursor = aws_byte_cursor_from_string(s_post_trailer);
    struct aws_byte_cursor checksum_result_cursor = aws_byte_cursor_from_buf(parent_stream->checksum_result);
    struct aws_byte_buf post_chunk_buffer;
    aws_byte_buf_init(
        &post_chunk_buffer,
        aws_default_allocator(),
        final_chunk_cursor.len + checksum_result_cursor.len + post_trailer_cursor.len);
    aws_byte_buf_append(&post_chunk_buffer, &final_chunk_cursor);
    aws_byte_buf_append(&post_chunk_buffer, &checksum_result_cursor);
    aws_byte_buf_append(&post_chunk_buffer, &post_trailer_cursor);
    struct aws_byte_cursor post_chunk_cursor = aws_byte_cursor_from_buf(&post_chunk_buffer);
    parent_stream->current_stream = aws_input_stream_new_from_cursor(aws_default_allocator(), &post_chunk_cursor);
    parent_stream->set_current_stream_fn = s_set_null_stream;
    return AWS_OP_SUCCESS;
}

static int s_set_chunk_stream(struct aws_chunk_stream *parent_stream) {
    aws_input_stream_destroy(parent_stream->current_stream);
    parent_stream->current_stream = parent_stream->checksum_stream;
    parent_stream->set_current_stream_fn = s_set_post_chunk_stream;
    return AWS_OP_SUCCESS;
}

static int s_aws_input_chunk_stream_seek(
    struct aws_input_stream *stream,
    int64_t offset,
    enum aws_stream_seek_basis basis) {

    struct aws_chunk_stream *impl = stream->impl;
    return aws_input_stream_seek(impl->current_stream, offset, basis);
}

static int s_aws_input_chunk_stream_read(struct aws_input_stream *stream, struct aws_byte_buf *dest) {
    struct aws_chunk_stream *impl = stream->impl;

    size_t start = dest->len;
    int err = aws_input_stream_read(impl->current_stream, dest);
    if (err) {
        return err;
    }
    size_t end = dest->len;
    struct aws_byte_cursor to_sum = aws_byte_cursor_from_buf(dest);
    to_sum.ptr += start;
    to_sum.len = end - start;
    return AWS_OP_SUCCESS;
}

static int s_aws_input_chunk_stream_get_status(struct aws_input_stream *stream, struct aws_stream_status *status) {
    struct aws_chunk_stream *impl = stream->impl;
    return aws_input_stream_get_status(impl->current_stream, status);
}

static int s_aws_input_chunk_stream_get_length(struct aws_input_stream *stream, int64_t *out_length) {
    /* could be calculated I guess, but setting as null until someone says it's necessary */
    (void)stream;
    (void)out_length;
    return AWS_OP_ERR;
}

static void s_aws_input_chunk_stream_destroy(struct aws_input_stream *stream) {
    if (stream) {
        struct aws_chunk_stream *impl = stream->impl;
        aws_input_stream_destroy(impl->current_stream);
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
    struct aws_allocator *allocator,
    struct aws_input_stream *existing_stream,
    enum aws_s3_checksum_algorithm algorithm) {

    struct aws_input_stream *stream = NULL;
    struct aws_chunk_stream *impl = NULL;
    aws_mem_acquire_many(
        allocator, 2, &stream, sizeof(struct aws_input_stream), &impl, sizeof(struct aws_chunk_stream));
    AWS_FATAL_ASSERT(stream);

    AWS_ZERO_STRUCT(*stream);
    AWS_ZERO_STRUCT(*impl);

    stream->allocator = allocator;
    stream->impl = impl;
    stream->vtable = &s_aws_input_chunk_stream_vtable;
    int64_t stream_length = 0;
    aws_input_stream_get_length(existing_stream, &stream_length);
    struct aws_byte_cursor pre_chunk_cursor = aws_byte_cursor_from_string(s_pre_chunk);
    // Should a claculate the length here with a log algirthm?
    char stream_length_string[128];
    AWS_ZERO_ARRAY(stream_length_string);
    sprintf(stream_length_string, "%" PRId64, stream_length);
    struct aws_byte_cursor stream_length_cursor =
        aws_byte_cursor_from_string(aws_string_new_from_c_str(allocator, stream_length_string));
    struct aws_byte_buf pre_chunk_buffer;
    aws_byte_buf_init(&pre_chunk_buffer, allocator, stream_length_cursor.len + pre_chunk_cursor.len);
    aws_byte_buf_append(&pre_chunk_buffer, &stream_length_cursor);
    aws_byte_buf_append(&pre_chunk_buffer, &pre_chunk_cursor);
    struct aws_byte_cursor complete_pre_chunk_cursor = aws_byte_cursor_from_buf(&pre_chunk_buffer);
    impl->current_stream = aws_input_stream_new_from_cursor(allocator, &complete_pre_chunk_cursor);
    impl->checksum_stream = aws_checksum_stream_new(allocator, existing_stream, algorithm, impl->checksum_result);
    impl->set_current_stream_fn = s_set_chunk_stream;
    AWS_FATAL_ASSERT(impl->current_stream);
    AWS_FATAL_ASSERT(impl->checksum_stream);

    return stream;
}
