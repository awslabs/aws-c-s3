/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_client_impl.h"
#include "aws/s3/private/s3_meta_request_impl.h"
#include "aws/s3/private/s3_request.h"
#include "aws/s3/private/s3_util.h"
#include <aws/common/string.h>
#include <aws/http/request_response.h>
#include <aws/io/stream.h>
#include <inttypes.h>

extern struct aws_s3_request_vtable g_aws_s3_put_object_request_vtable;

static int s_s3_put_object_prepare_for_send(struct aws_s3_request *request);

static int s_s3_put_object_incoming_headers(
    struct aws_http_stream *stream,
    enum aws_http_header_block header_block,
    const struct aws_http_header *headers,
    size_t headers_count,
    void *user_data);

struct aws_s3_request_vtable g_aws_s3_put_object_request_vtable = {.prepare_for_send = s_s3_put_object_prepare_for_send,
                                                                   .incoming_headers =
                                                                       s_s3_put_object_incoming_headers};

static int s_s3_put_object_prepare_for_send(struct aws_s3_request *request) {
    AWS_PRECONDITION(request);
    AWS_PRECONDITION(request->part_buffer);

    struct aws_s3_meta_request *meta_request = request->meta_request;
    AWS_PRECONDITION(meta_request);

    struct aws_http_message *message =
        aws_s3_request_util_copy_http_message(meta_request->allocator, meta_request->initial_request_message);

    if (message == NULL) {
        return AWS_OP_ERR;
    }

    if (request->part_number > 0) {

        if (aws_s3_request_util_set_multipart_request_path(
                meta_request->allocator, meta_request, request->part_number, message)) {
            goto error_clean_up;
        }

        if (aws_s3_meta_request_copy_part_to_part_buffer(meta_request, request->part_number, request->part_buffer)) {
            goto error_clean_up;
        }

        request->input_stream =
            aws_s3_request_util_assign_body(meta_request->allocator, &request->part_buffer->buffer, message);

        if (request->input_stream == NULL) {
            goto error_clean_up;
        }
    }

    request->message = message;

    return AWS_OP_SUCCESS;

error_clean_up:

    if (message != NULL) {
        aws_http_message_destroy(message);
        message = NULL;
    }

    return AWS_OP_ERR;
}

static int s_s3_put_object_incoming_headers(
    struct aws_http_stream *stream,
    enum aws_http_header_block header_block,
    const struct aws_http_header *headers,
    size_t headers_count,
    void *user_data) {

    AWS_PRECONDITION(stream);
    AWS_PRECONDITION(user_data);

    struct aws_s3_request *request = user_data;

    (void)stream;
    (void)header_block;

    struct aws_s3_meta_request *meta_request = request->meta_request;
    AWS_PRECONDITION(meta_request);

    struct aws_allocator *allocator = meta_request->allocator;
    AWS_PRECONDITION(allocator);

    /* Find the ETag header if it exists and cache it. */
    for (size_t i = 0; i < headers_count; ++i) {
        const struct aws_byte_cursor *name = &headers[i].name;
        const struct aws_byte_cursor *value = &headers[i].value;

        if (aws_http_header_name_eq(*name, g_etag_header_name)) {
            struct aws_byte_cursor value_within_quotes = *value;

            if (value_within_quotes.len >= 2) {
                value_within_quotes.len -= 2;
                value_within_quotes.ptr++;
            }

            request->etag = aws_string_new_from_cursor(allocator, &value_within_quotes);
            break;
        }
    }

    return AWS_OP_SUCCESS;
}
