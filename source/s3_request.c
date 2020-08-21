/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_request.h"
#include "aws/s3/private/s3_client_impl.h"
#include "aws/s3/private/s3_get_object_request.h"
#include "aws/s3/private/s3_put_object_request.h"

#include <aws/auth/signable.h>
#include <aws/common/assert.h>
#include <aws/common/string.h>
#include <aws/http/request_response.h>

/* TODO look at finding a less error prone place to initialize this. (And somewhere where we can initialize by
 * s_s3_reqeust_vtables[request_type] = request_vtable;) */
static struct aws_s3_request_vtable *s_s3_request_vtables[] = {
    &g_aws_s3_request_get_object_vtable, /* AWS_S3_REQUEST_TYPE_GET_OBJECT */
    &g_aws_s3_put_object_request_vtable, /* AWS_S3_REQUEST_TYPE_PUT_OBJECT */
    NULL,                                /* AWS_S3_REQUEST_TYPE_CREATE_MULTIPART */
    NULL,                                /* AWS_S3_REQUEST_TYPE_UPLOAD_PART */
    NULL,                                /* AWS_S3_REQUEST_TYPE_COMPLETE_MULTIPART */
};

static struct aws_s3_request_vtable *s_s3_request_get_vtable(struct aws_s3_request *request) {
    AWS_PRECONDITION(request);
    return s_s3_request_vtables[request->request_type];
}

int aws_s3_request_init(
    struct aws_s3_request *request,
    struct aws_allocator *allocator,
    enum aws_s3_request_type request_type,
    const struct aws_s3_request_options *options) {

    AWS_PRECONDITION(request);
    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(options);

    if (options->message == NULL) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_CLIENT, "Cannot not allocate aws_s3_get_object_request; message provided in options is invalid.");
        aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        return AWS_OP_ERR;
    }

    request->request_type = request_type;
    request->allocator = allocator;
    request->signable = NULL;

    struct aws_http_message *message_copy = aws_http_message_new_request(allocator);

    /* Make a copy of the message so we can modify it without touching the original. */
    {
        struct aws_byte_cursor request_method;
        if (aws_http_message_get_request_method(options->message, &request_method)) {
            goto error_clean_up;
        }

        if (aws_http_message_set_request_method(message_copy, request_method)) {
            goto error_clean_up;
        }

        struct aws_byte_cursor request_path;
        if (aws_http_message_get_request_path(options->message, &request_path)) {
            goto error_clean_up;
        }

        if (aws_http_message_set_request_path(message_copy, request_path)) {
            goto error_clean_up;
        }

        size_t num_headers = aws_http_message_get_header_count(options->message);

        for (size_t header_index = 0; header_index < num_headers; ++header_index) {
            struct aws_http_header header;

            if (aws_http_message_get_header(options->message, &header, header_index)) {
                goto error_clean_up;
            }

            if (aws_http_message_add_header(message_copy, header)) {
                goto error_clean_up;
            }
        }

        struct aws_input_stream *body_stream = aws_http_message_get_body_stream(options->message);
        aws_http_message_set_body_stream(message_copy, body_stream);
    }

    request->message = message_copy;

    return AWS_OP_SUCCESS;

error_clean_up:

    if (message_copy != NULL) {
        aws_http_message_release(message_copy);
        message_copy = NULL;
    }

    return AWS_OP_ERR;
}

int aws_s3_request_incoming_headers(
    struct aws_s3_request *request,
    struct aws_http_stream *stream,
    enum aws_http_header_block header_block,
    const struct aws_http_header *headers,
    size_t headers_count) {

    AWS_PRECONDITION(request);
    AWS_PRECONDITION(stream);

    struct aws_s3_request_vtable *vtable = s_s3_request_get_vtable(request);
    AWS_FATAL_ASSERT(vtable != NULL);

    if (vtable->incoming_headers != NULL) {
        return vtable->incoming_headers(request, stream, header_block, headers, headers_count);
    }

    return AWS_OP_SUCCESS;
}

int aws_s3_request_incoming_header_block_done(
    struct aws_s3_request *request,
    struct aws_http_stream *stream,
    enum aws_http_header_block header_block) {

    AWS_PRECONDITION(request);
    AWS_PRECONDITION(stream);

    struct aws_s3_request_vtable *vtable = s_s3_request_get_vtable(request);
    AWS_FATAL_ASSERT(vtable != NULL);

    if (vtable->incoming_header_block_done != NULL) {
        return vtable->incoming_header_block_done(request, stream, header_block);
    }

    return AWS_OP_SUCCESS;
}

int aws_s3_request_incoming_body(
    struct aws_s3_request *request,
    struct aws_http_stream *stream,
    const struct aws_byte_cursor *data,
    struct aws_s3_body_meta_data *out_meta_data) {

    AWS_PRECONDITION(request);
    AWS_PRECONDITION(stream);

    struct aws_s3_request_vtable *vtable = s_s3_request_get_vtable(request);
    AWS_FATAL_ASSERT(vtable != NULL);

    if (vtable->incoming_body != NULL) {
        if (vtable->incoming_body(request, stream, data, out_meta_data)) {
            return AWS_OP_ERR;
        }
    }

    return AWS_OP_SUCCESS;
}

void aws_s3_request_stream_complete(struct aws_s3_request *request, struct aws_http_stream *stream, int error_code) {
    AWS_PRECONDITION(request);
    AWS_PRECONDITION(stream);

    struct aws_s3_request_vtable *vtable = s_s3_request_get_vtable(request);
    AWS_FATAL_ASSERT(vtable != NULL);

    if (vtable->stream_complete != NULL) {
        vtable->stream_complete(request, stream, error_code);
    }
}

void aws_s3_request_finish(struct aws_s3_request *request, int error_code) {
    AWS_PRECONDITION(request);

    struct aws_s3_request_vtable *vtable = s_s3_request_get_vtable(request);
    AWS_FATAL_ASSERT(vtable != NULL);

    if (vtable->request_finish != NULL) {
        vtable->request_finish(request, error_code);
    }
}

void aws_s3_request_destroy(struct aws_s3_request *request) {
    AWS_PRECONDITION(request);

    struct aws_s3_request_vtable *vtable = s_s3_request_vtables[request->request_type];
    AWS_PRECONDITION(vtable != NULL && vtable->destroy);

    if (request->signable != NULL) {
        aws_signable_destroy(request->signable);
        request->signable = NULL;
    }

    if (request->message != NULL) {
        aws_http_message_release(request->message);
        request->message = NULL;
    }

    vtable->destroy(request);
}
