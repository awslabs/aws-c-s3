/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_checksums.h"
#include <aws/common/encoding.h>
#include <aws/io/stream.h>

struct aws_checksum_stream {
    struct aws_input_stream *old_stream;
    struct aws_s3_checksum *checksum;
    struct aws_byte_buf checksum_result;
    /* base64 encoded checksum of the stream, updated on destruction of sream */
    struct aws_byte_buf *encoded_checksum_output;
    bool did_seek;
};

static int s_aws_input_checksum_stream_seek(
    struct aws_input_stream *stream,
    int64_t offset,
    enum aws_stream_seek_basis basis) {
    (void)stream;
    (void)offset;
    (void)basis;
    AWS_LOGF_ERROR(
        AWS_LS_S3_CLIENT,
        "Cannot seek on checksum stream, as it will cause the checksum output to mismatch the checksum of the stream "
        "contents");
    AWS_ASSERT(false);
    return aws_raise_error(AWS_ERROR_UNSUPPORTED_OPERATION);
}

static int s_aws_input_checksum_stream_read(struct aws_input_stream *stream, struct aws_byte_buf *dest) {
    struct aws_checksum_stream *impl = stream->impl;

    size_t start = dest->len;
    int err = aws_input_stream_read(impl->old_stream, dest);
    size_t end = dest->len;
    struct aws_byte_cursor to_sum = aws_byte_cursor_from_buf(dest);
    to_sum.ptr += start;
    to_sum.len = end - start;
    if (!err) {
        int checksum_res = aws_checksum_update(impl->checksum, &to_sum);
        if (checksum_res) {
            dest->len = start;
            impl->did_seek = true;
        }
        return checksum_res;
    }
    return err;
}

static int s_aws_input_checksum_stream_get_status(struct aws_input_stream *stream, struct aws_stream_status *status) {
    struct aws_checksum_stream *impl = stream->impl;
    return aws_input_stream_get_status(impl->old_stream, status);
}

static int s_aws_input_checksum_stream_get_length(struct aws_input_stream *stream, int64_t *out_length) {
    struct aws_checksum_stream *impl = stream->impl;
    return aws_input_stream_get_length(impl->old_stream, out_length);
}

/* We take ownership of the old inputstream, and destroy it with this input stream. This is because we want to be able
 * to substitute in the chunk_stream for the cursor stream currently used in s_s3_meta_request_default_prepare_request
 * which returns the new stream. So in order to prevent the need of keeping track of two input streams we instead
 * consume the cursor stream and destroy it with this one */
static void s_aws_input_checksum_stream_destroy(struct aws_input_stream *stream) {
    if (stream) {
        struct aws_checksum_stream *impl = stream->impl;
        int result = aws_checksum_finalize(impl->checksum, &impl->checksum_result, 0);
        if (result != AWS_OP_SUCCESS) {
            aws_byte_buf_reset(&impl->checksum_result, true);
        }
        AWS_ASSERT(result == AWS_OP_SUCCESS);
        struct aws_byte_cursor checksum_result_cursor = aws_byte_cursor_from_buf(&impl->checksum_result);
        AWS_FATAL_ASSERT(aws_base64_encode(&checksum_result_cursor, impl->encoded_checksum_output) == AWS_OP_SUCCESS);
        aws_checksum_destroy(impl->checksum);
        aws_input_stream_destroy(impl->old_stream);
        aws_byte_buf_clean_up(&impl->checksum_result);
        aws_mem_release(stream->allocator, stream);
    }
}

static struct aws_input_stream_vtable s_aws_input_checksum_stream_vtable = {
    .seek = s_aws_input_checksum_stream_seek,
    .read = s_aws_input_checksum_stream_read,
    .get_status = s_aws_input_checksum_stream_get_status,
    .get_length = s_aws_input_checksum_stream_get_length,
    .destroy = s_aws_input_checksum_stream_destroy,
};

struct aws_input_stream *aws_checksum_stream_new(
    struct aws_allocator *allocator,
    struct aws_input_stream *existing_stream,
    enum aws_s3_checksum_algorithm algorithm,
    struct aws_byte_buf *checksum_output) {

    struct aws_input_stream *stream = NULL;
    struct aws_checksum_stream *impl = NULL;
    aws_mem_acquire_many(
        allocator, 2, &stream, sizeof(struct aws_input_stream), &impl, sizeof(struct aws_checksum_stream));
    AWS_FATAL_ASSERT(stream);

    AWS_ZERO_STRUCT(*stream);
    AWS_ZERO_STRUCT(*impl);

    stream->allocator = allocator;
    stream->impl = impl;
    stream->vtable = &s_aws_input_checksum_stream_vtable;

    impl->old_stream = existing_stream;
    impl->checksum = aws_checksum_new(allocator, algorithm);
    if (impl->checksum == NULL) {
        goto on_error;
    }
    aws_byte_buf_init(&impl->checksum_result, allocator, impl->checksum->digest_size);
    impl->encoded_checksum_output = checksum_output;
    impl->did_seek = false;
    AWS_FATAL_ASSERT(impl->old_stream);

    return stream;
on_error:
    aws_mem_release(stream->allocator, stream);
    return NULL;
}
