/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_checksum_context.h"
#include "aws/s3/private/s3_checksums.h"
#include <aws/common/encoding.h>
#include <aws/common/string.h>
#include <aws/io/stream.h>
#include <inttypes.h>

AWS_STATIC_STRING_FROM_LITERAL(s_carriage_return, "\r\n");
AWS_STATIC_STRING_FROM_LITERAL(s_empty_chunk, "0\r\n");
AWS_STATIC_STRING_FROM_LITERAL(s_final_chunk, "\r\n0\r\n");
AWS_STATIC_STRING_FROM_LITERAL(s_colon, ":");
AWS_STATIC_STRING_FROM_LITERAL(s_post_trailer, "\r\n\r\n");

struct aws_chunk_stream;

typedef int(set_stream_fn)(struct aws_chunk_stream *parent_stream);

struct aws_chunk_stream {
    struct aws_input_stream base;
    struct aws_allocator *allocator;
    /* aws_input_stream_byte_cursor provides our actual functionality  */
    /* Pointing to the stream we read from */
    struct aws_input_stream *current_stream;
    struct aws_input_stream *chunk_body_stream;

    struct aws_s3_upload_request_checksum_context *checksum_context;
    struct aws_byte_buf pre_chunk_buffer;
    struct aws_byte_buf post_chunk_buffer;
    struct aws_byte_cursor checksum_header_name;
    int64_t length;
    set_stream_fn *set_current_stream_fn;
};

static int s_set_null_stream(struct aws_chunk_stream *parent_stream) {
    aws_input_stream_release(parent_stream->current_stream);
    parent_stream->current_stream = NULL;
    parent_stream->set_current_stream_fn = NULL;
    aws_byte_buf_clean_up(&parent_stream->post_chunk_buffer);
    return AWS_OP_SUCCESS;
}

static int s_set_post_chunk_stream(struct aws_chunk_stream *parent_stream) {
    int64_t current_stream_length;
    if (aws_input_stream_get_length(parent_stream->current_stream, &current_stream_length)) {
        aws_input_stream_release(parent_stream->current_stream);
        return AWS_OP_ERR;
    }
    aws_input_stream_release(parent_stream->current_stream);

    struct aws_byte_cursor final_chunk_cursor;

    if (current_stream_length > 0) {
        final_chunk_cursor = aws_byte_cursor_from_string(s_final_chunk);
    } else {
        final_chunk_cursor = aws_byte_cursor_from_string(s_empty_chunk);
    }
    struct aws_byte_cursor post_trailer_cursor = aws_byte_cursor_from_string(s_post_trailer);
    struct aws_byte_cursor colon_cursor = aws_byte_cursor_from_string(s_colon);
    struct aws_byte_cursor checksum_result_cursor =
        aws_s3_upload_request_checksum_context_get_checksum_cursor(parent_stream->checksum_context);
    AWS_ASSERT(parent_stream->checksum_context->encoded_checksum_size == checksum_result_cursor.len);

    if (aws_byte_buf_init(
            &parent_stream->post_chunk_buffer,
            parent_stream->allocator,
            final_chunk_cursor.len + parent_stream->checksum_header_name.len + colon_cursor.len +
                checksum_result_cursor.len + post_trailer_cursor.len)) {
        goto error;
    }
    if (aws_byte_buf_append(&parent_stream->post_chunk_buffer, &final_chunk_cursor) ||
        aws_byte_buf_append(&parent_stream->post_chunk_buffer, &parent_stream->checksum_header_name) ||
        aws_byte_buf_append(&parent_stream->post_chunk_buffer, &colon_cursor) ||
        aws_byte_buf_append(&parent_stream->post_chunk_buffer, &checksum_result_cursor) ||
        aws_byte_buf_append(&parent_stream->post_chunk_buffer, &post_trailer_cursor)) {
        goto error;
    }
    struct aws_byte_cursor post_chunk_cursor = aws_byte_cursor_from_buf(&parent_stream->post_chunk_buffer);
    parent_stream->current_stream = aws_input_stream_new_from_cursor(parent_stream->allocator, &post_chunk_cursor);
    parent_stream->set_current_stream_fn = s_set_null_stream;
    return AWS_OP_SUCCESS;
error:
    aws_byte_buf_clean_up(&parent_stream->post_chunk_buffer);
    return AWS_OP_ERR;
}

static int s_set_chunk_stream(struct aws_chunk_stream *parent_stream) {
    aws_input_stream_release(parent_stream->current_stream);
    parent_stream->current_stream = parent_stream->chunk_body_stream;
    aws_byte_buf_clean_up(&parent_stream->pre_chunk_buffer);
    parent_stream->chunk_body_stream = NULL;
    parent_stream->set_current_stream_fn = s_set_post_chunk_stream;
    return AWS_OP_SUCCESS;
}

static int s_aws_input_chunk_stream_seek(
    struct aws_input_stream *stream,
    int64_t offset,
    enum aws_stream_seek_basis basis) {
    (void)stream;
    (void)offset;
    (void)basis;
    AWS_LOGF_ERROR(
        AWS_LS_S3_CLIENT,
        "Cannot seek on chunk stream, as it will cause the checksum output to mismatch the checksum of the stream"
        "contents");
    AWS_ASSERT(false);
    return aws_raise_error(AWS_ERROR_UNSUPPORTED_OPERATION);
}

static int s_aws_input_chunk_stream_read(struct aws_input_stream *stream, struct aws_byte_buf *dest) {
    struct aws_chunk_stream *impl = AWS_CONTAINER_OF(stream, struct aws_chunk_stream, base);

    struct aws_stream_status status;
    AWS_ZERO_STRUCT(status);
    while (impl->current_stream != NULL && dest->len < dest->capacity) {
        int err = aws_input_stream_read(impl->current_stream, dest);
        if (err) {
            return err;
        }
        if (aws_input_stream_get_status(impl->current_stream, &status)) {
            return AWS_OP_ERR;
        }
        if (status.is_end_of_stream && impl->set_current_stream_fn(impl)) {
            return AWS_OP_ERR;
        }
    }
    return AWS_OP_SUCCESS;
}

static int s_aws_input_chunk_stream_get_status(struct aws_input_stream *stream, struct aws_stream_status *status) {
    struct aws_chunk_stream *impl = AWS_CONTAINER_OF(stream, struct aws_chunk_stream, base);

    if (impl->current_stream == NULL) {
        status->is_end_of_stream = true;
        status->is_valid = true;
        return AWS_OP_SUCCESS;
    }
    int res = aws_input_stream_get_status(impl->current_stream, status);
    if (res != AWS_OP_SUCCESS) {
        /* Only when the current_stream is NULL, it is end of stream, as the current stream will be updated to feed to
         * data */
        status->is_end_of_stream = false;
    }
    return res;
}

static int s_aws_input_chunk_stream_get_length(struct aws_input_stream *stream, int64_t *out_length) {
    struct aws_chunk_stream *impl = AWS_CONTAINER_OF(stream, struct aws_chunk_stream, base);
    *out_length = impl->length;
    return AWS_OP_SUCCESS;
}

static void s_aws_input_chunk_stream_destroy(struct aws_chunk_stream *impl) {
    if (impl) {
        aws_input_stream_release(impl->current_stream);
        aws_input_stream_release(impl->chunk_body_stream);
        aws_byte_buf_clean_up(&impl->pre_chunk_buffer);
        aws_byte_buf_clean_up(&impl->post_chunk_buffer);
        /* Either we calculated the checksum, or we the checksum is empty. Otherwise, something was wrong. */
        aws_s3_upload_request_checksum_context_release(impl->checksum_context);
        aws_mem_release(impl->allocator, impl);
    }
}

static struct aws_input_stream_vtable s_aws_input_chunk_stream_vtable = {
    .seek = s_aws_input_chunk_stream_seek,
    .read = s_aws_input_chunk_stream_read,
    .get_status = s_aws_input_chunk_stream_get_status,
    .get_length = s_aws_input_chunk_stream_get_length,
};

struct aws_input_stream *aws_chunk_stream_new(
    struct aws_allocator *allocator,
    struct aws_input_stream *existing_stream,
    struct aws_s3_upload_request_checksum_context *checksum_context) {
    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(existing_stream);
    AWS_PRECONDITION(checksum_context);

    struct aws_chunk_stream *impl = aws_mem_calloc(allocator, 1, sizeof(struct aws_chunk_stream));

    impl->allocator = allocator;
    impl->base.vtable = &s_aws_input_chunk_stream_vtable;

    /* Extract algorithm and buffer from context */
    enum aws_s3_checksum_algorithm algorithm = AWS_SCA_NONE;

    impl->checksum_context = aws_s3_upload_request_checksum_context_acquire(checksum_context);

    algorithm = checksum_context->algorithm;
    bool should_calculate_checksum = aws_s3_upload_request_checksum_context_should_calculate(impl->checksum_context);

    int64_t stream_length = 0;
    int64_t final_chunk_len = 0;
    if (aws_input_stream_get_length(existing_stream, &stream_length)) {
        goto error;
    }
    struct aws_byte_cursor pre_chunk_cursor = aws_byte_cursor_from_string(s_carriage_return);
    char stream_length_string[32];
    AWS_ZERO_ARRAY(stream_length_string);
    snprintf(stream_length_string, AWS_ARRAY_SIZE(stream_length_string), "%" PRIX64, stream_length);
    struct aws_string *stream_length_aws_string = aws_string_new_from_c_str(allocator, stream_length_string);
    struct aws_byte_cursor stream_length_cursor = aws_byte_cursor_from_string(stream_length_aws_string);
    if (aws_byte_buf_init(&impl->pre_chunk_buffer, allocator, stream_length_cursor.len + pre_chunk_cursor.len)) {
        goto error;
    }
    if (aws_byte_buf_append(&impl->pre_chunk_buffer, &stream_length_cursor)) {
        goto error;
    }
    aws_string_destroy(stream_length_aws_string);
    if (aws_byte_buf_append(&impl->pre_chunk_buffer, &pre_chunk_cursor)) {
        goto error;
    }
    if (should_calculate_checksum) {
        /* Wrap the existing stream with checksum stream to calculate the checksum when reading from it. */
        impl->chunk_body_stream =
            aws_checksum_stream_new_with_context(allocator, existing_stream, impl->checksum_context);
        if (impl->chunk_body_stream == NULL) {
            goto error;
        }
    } else {
        /* No need to calculate the checksum during read, use the existing stream directly. */
        impl->chunk_body_stream = aws_input_stream_acquire(existing_stream);
    }
    int64_t prechunk_stream_len = 0;
    int64_t colon_len = s_colon->len;
    int64_t post_trailer_len = s_post_trailer->len;

    struct aws_byte_cursor complete_pre_chunk_cursor = aws_byte_cursor_from_buf(&impl->pre_chunk_buffer);

    if (stream_length > 0) {
        impl->current_stream = aws_input_stream_new_from_cursor(allocator, &complete_pre_chunk_cursor);
        final_chunk_len = s_final_chunk->len;
        if (impl->current_stream == NULL) {
            goto error;
        }
        impl->set_current_stream_fn = s_set_chunk_stream;
    } else {
        impl->current_stream = impl->chunk_body_stream;
        final_chunk_len = s_empty_chunk->len;
        impl->chunk_body_stream = NULL;
        impl->set_current_stream_fn = s_set_post_chunk_stream;
    }

    impl->checksum_header_name = aws_get_http_header_name_from_checksum_algorithm(algorithm);

    if (aws_input_stream_get_length(impl->current_stream, &prechunk_stream_len)) {
        goto error;
    }

    impl->length = prechunk_stream_len + stream_length + final_chunk_len + impl->checksum_header_name.len + colon_len +
                   checksum_context->encoded_checksum_size + post_trailer_len;

    AWS_ASSERT(impl->current_stream);
    aws_ref_count_init(&impl->base.ref_count, impl, (aws_simple_completion_callback *)s_aws_input_chunk_stream_destroy);
    return &impl->base;

error:
    aws_input_stream_release(impl->chunk_body_stream);
    aws_input_stream_release(impl->current_stream);
    aws_byte_buf_clean_up(&impl->pre_chunk_buffer);
    aws_mem_release(impl->allocator, impl);
    return NULL;
}
