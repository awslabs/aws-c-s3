/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_checksum_context.h"
#include "aws/s3/private/s3_checksums.h"
#include <aws/common/encoding.h>
#include <aws/io/stream.h>

struct aws_checksum_stream {
    struct aws_input_stream base;
    struct aws_allocator *allocator;

    struct aws_input_stream *old_stream;
    struct aws_s3_checksum *checksum;
    struct aws_s3_upload_request_checksum_context *context;
    struct aws_byte_buf checksum_result;
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
    struct aws_checksum_stream *impl = AWS_CONTAINER_OF(stream, struct aws_checksum_stream, base);

    size_t original_len = dest->len;
    if (aws_input_stream_read(impl->old_stream, dest)) {
        return AWS_OP_ERR;
    }
    struct aws_byte_cursor to_sum = aws_byte_cursor_from_buf(dest);
    /* Move the cursor to the part to calculate the checksum */
    aws_byte_cursor_advance(&to_sum, original_len);
    /* If read failed, `aws_input_stream_read` will handle the error to restore the dest. No need to handle error here
     */
    if (aws_checksum_update(impl->checksum, &to_sum)) {
        return AWS_OP_ERR;
    }
    if (impl->context) {
        /* If we're at the end of the stream, compute and store the final checksum */
        struct aws_stream_status status;
        if (aws_input_stream_get_status(impl->old_stream, &status)) {
            return AWS_OP_ERR;
        }
        if (status.is_end_of_stream) {
            if (aws_checksum_finalize(impl->checksum, &impl->checksum_result) != AWS_OP_SUCCESS) {
                AWS_LOGF_ERROR(
                    AWS_LS_S3_CLIENT,
                    "Failed to calculate checksum with error code %d (%s).",
                    aws_last_error(),
                    aws_error_str(aws_last_error()));
                aws_byte_buf_reset(&impl->checksum_result, true);
                return aws_raise_error(AWS_ERROR_S3_CHECKSUM_CALCULATION_FAILED);
            }
            struct aws_byte_cursor checksum_result_cursor = aws_byte_cursor_from_buf(&impl->checksum_result);
            if (aws_s3_upload_request_checksum_context_finalize_checksum(impl->context, checksum_result_cursor) !=
                AWS_OP_SUCCESS) {
                AWS_LOGF_ERROR(
                    AWS_LS_S3_CLIENT,
                    "Failed to finalize checksum context with error code %d (%s).",
                    aws_last_error(),
                    aws_error_str(aws_last_error()));
                aws_byte_buf_reset(&impl->checksum_result, true);
                return aws_raise_error(AWS_ERROR_S3_CHECKSUM_CALCULATION_FAILED);
            }
        }
    }
    return AWS_OP_SUCCESS;
}

static int s_aws_input_checksum_stream_get_status(struct aws_input_stream *stream, struct aws_stream_status *status) {
    struct aws_checksum_stream *impl = AWS_CONTAINER_OF(stream, struct aws_checksum_stream, base);
    return aws_input_stream_get_status(impl->old_stream, status);
}

static int s_aws_input_checksum_stream_get_length(struct aws_input_stream *stream, int64_t *out_length) {
    struct aws_checksum_stream *impl = AWS_CONTAINER_OF(stream, struct aws_checksum_stream, base);
    return aws_input_stream_get_length(impl->old_stream, out_length);
}

/* We take ownership of the old input stream, and destroy it with this input stream. This is because we want to be able
 * to substitute in the chunk_stream for the cursor stream currently used in s_s3_meta_request_default_prepare_request
 * which returns the new stream. So in order to prevent the need of keeping track of two input streams we instead
 * consume the cursor stream and destroy it with this one */
static void s_aws_input_checksum_stream_destroy(struct aws_checksum_stream *impl) {
    if (!impl) {
        return;
    }
    aws_checksum_destroy(impl->checksum);
    aws_input_stream_release(impl->old_stream);
    aws_byte_buf_clean_up(&impl->checksum_result);
    aws_s3_upload_request_checksum_context_release(impl->context);
    aws_mem_release(impl->allocator, impl);
}

static struct aws_input_stream_vtable s_aws_input_checksum_stream_vtable = {
    .seek = s_aws_input_checksum_stream_seek,
    .read = s_aws_input_checksum_stream_read,
    .get_status = s_aws_input_checksum_stream_get_status,
    .get_length = s_aws_input_checksum_stream_get_length,
};

static struct aws_checksum_stream *s_aws_checksum_input_checksum_stream_new(
    struct aws_allocator *allocator,
    struct aws_input_stream *existing_stream,
    enum aws_s3_checksum_algorithm algorithm) {
    AWS_PRECONDITION(existing_stream);

    struct aws_checksum_stream *impl = aws_mem_calloc(allocator, 1, sizeof(struct aws_checksum_stream));
    impl->allocator = allocator;
    impl->base.vtable = &s_aws_input_checksum_stream_vtable;

    impl->checksum = aws_checksum_new(allocator, algorithm);
    if (impl->checksum == NULL) {
        goto on_error;
    }
    aws_byte_buf_init(&impl->checksum_result, allocator, impl->checksum->digest_size);
    impl->old_stream = aws_input_stream_acquire(existing_stream);
    aws_ref_count_init(
        &impl->base.ref_count, impl, (aws_simple_completion_callback *)s_aws_input_checksum_stream_destroy);
    return impl;
on_error:
    aws_mem_release(impl->allocator, impl);
    return NULL;
}

struct aws_input_stream *aws_checksum_stream_new(
    struct aws_allocator *allocator,
    struct aws_input_stream *existing_stream,
    enum aws_s3_checksum_algorithm algorithm) {
    AWS_PRECONDITION(existing_stream);
    struct aws_checksum_stream *impl = s_aws_checksum_input_checksum_stream_new(allocator, existing_stream, algorithm);
    if (impl) {
        return &impl->base;
    }
    return NULL;
}

struct aws_input_stream *aws_checksum_stream_new_with_context(
    struct aws_allocator *allocator,
    struct aws_input_stream *existing_stream,
    struct aws_s3_upload_request_checksum_context *context) {
    AWS_PRECONDITION(existing_stream);
    AWS_PRECONDITION(context);
    struct aws_checksum_stream *impl =
        s_aws_checksum_input_checksum_stream_new(allocator, existing_stream, context->algorithm);
    if (impl) {
        impl->context = aws_s3_upload_request_checksum_context_acquire(context);
        return &impl->base;
    }
    return NULL;
}

int aws_checksum_stream_finalize_checksum(struct aws_input_stream *checksum_stream, struct aws_byte_buf *checksum_buf) {
    AWS_PRECONDITION(checksum_buf);
    AWS_PRECONDITION(checksum_buf->len == 0 && "Checksum output buffer is not empty");
    int rt_code = AWS_OP_ERR;

    struct aws_checksum_stream *impl = AWS_CONTAINER_OF(checksum_stream, struct aws_checksum_stream, base);
    struct aws_byte_buf checksum_result;
    aws_byte_buf_init(&checksum_result, impl->allocator, impl->checksum->digest_size);

    if (aws_checksum_finalize(impl->checksum, &checksum_result) != AWS_OP_SUCCESS) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_CLIENT,
            "Failed to calculate checksum with error code %d (%s).",
            aws_last_error(),
            aws_error_str(aws_last_error()));
        aws_raise_error(AWS_ERROR_S3_CHECKSUM_CALCULATION_FAILED);
        goto done;
    }
    struct aws_byte_cursor checksum_result_cursor = aws_byte_cursor_from_buf(&checksum_result);
    if (aws_base64_encode(&checksum_result_cursor, checksum_buf) != AWS_OP_SUCCESS) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_CLIENT,
            "Failed to base64 encode checksum with error code %d (%s). Output capacity: %zu length %zu",
            aws_last_error(),
            aws_error_str(aws_last_error()),
            checksum_buf->capacity,
            checksum_buf->len);
        aws_raise_error(AWS_ERROR_S3_CHECKSUM_CALCULATION_FAILED);
        goto done;
    }
    rt_code = AWS_OP_SUCCESS;
done:
    aws_byte_buf_clean_up(&checksum_result);
    return rt_code;
}
