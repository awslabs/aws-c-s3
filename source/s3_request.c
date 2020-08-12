/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_request.h"
#include "aws/s3/private/s3_client_impl.h"

#include <aws/auth/signable.h>
#include <aws/common/assert.h>
#include <aws/http/connection_manager.h>
#include <aws/http/request_response.h>

int aws_s3_request_init(
    struct aws_s3_request *request,
    struct aws_allocator *allocator,
    struct aws_s3_request_vtable *vtable,
    void *impl,
    const struct aws_s3_request_options *options) {

    AWS_PRECONDITION(request);
    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(vtable);
    AWS_PRECONDITION(impl);
    AWS_PRECONDITION(options);

    if (options->message == NULL) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_CLIENT, "Cannot not allocate aws_s3_get_object_request; message provided in options is invalid.");
        aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        return AWS_OP_ERR;
    }

    request->allocator = allocator;
    request->vtable = vtable;
    request->impl = impl;

    /* Set up reference count. */
    aws_atomic_init_int(&request->ref_count, 0);
    aws_s3_request_acquire(request);

    struct aws_http_message *message_copy = aws_http_message_new_request(allocator);

    /* Make a copy of the message so we can modify it without touching the original. */
    {
        /* TODO these function calls need error handling. */

        struct aws_byte_cursor request_method;
        aws_http_message_get_request_method(options->message, &request_method);
        aws_http_message_set_request_method(message_copy, request_method);

        struct aws_byte_cursor request_path;
        aws_http_message_get_request_path(options->message, &request_path);
        aws_http_message_set_request_path(message_copy, request_path);

        size_t num_headers = aws_http_message_get_header_count(options->message);

        for (size_t header_index = 0; header_index < num_headers; ++header_index) {
            struct aws_http_header header;
            aws_http_message_get_header(options->message, &header, header_index);
            aws_http_message_add_header(message_copy, header);
        }

        struct aws_input_stream *body_stream = aws_http_message_get_body_stream(options->message);
        aws_http_message_set_body_stream(message_copy, body_stream);
    }

    request->message = message_copy;

    request->body_callback = options->body_callback;
    request->finish_callback = options->finish_callback;
    request->user_data = options->user_data;

    return AWS_OP_SUCCESS;
}

void aws_s3_request_set_meta_request(struct aws_s3_request *request, struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(request);

    if (request->meta_request == meta_request) {
        return;
    }

    if (request->meta_request != NULL) {
        aws_s3_meta_request_release(request->meta_request);
        request->meta_request = NULL;
    }

    if (meta_request != NULL) {
        aws_s3_meta_request_acquire(meta_request);
        request->meta_request = meta_request;
    }
}

void aws_s3_request_acquire(struct aws_s3_request *request) {
    AWS_PRECONDITION(request);
    aws_atomic_fetch_add(&request->ref_count, 1);
}

void aws_s3_request_release(struct aws_s3_request *request) {
    AWS_PRECONDITION(request);

    size_t prev_ref_count = aws_atomic_fetch_sub(&request->ref_count, 1);

    if (prev_ref_count > 1) {
        return;
    }

    if (request->meta_request != NULL) {
        aws_s3_meta_request_release(request->meta_request);
        request->meta_request = NULL;
    }

    if (request->stream != NULL) {
        aws_http_stream_release(request->stream);
        request->stream = NULL;
    }

    if (request->signable != NULL) {
        aws_signable_destroy(request->signable);
        request->signable = NULL;
    }

    if (request->message != NULL) {
        aws_http_message_release(request->message);
        request->message = NULL;
    }

    request->vtable->destroy(request);
}

int aws_s3_request_incoming_headers(
    struct aws_s3_request *request,
    enum aws_http_header_block header_block,
    const struct aws_http_header *headers,
    size_t headers_count) {
    AWS_PRECONDITION(request);

    struct aws_s3_request_vtable *vtable = request->vtable;

    if (vtable->incoming_headers != NULL) {
        return vtable->incoming_headers(request, header_block, headers, headers_count);
    }
    return AWS_OP_SUCCESS;
}

int aws_s3_request_incoming_header_block_done(struct aws_s3_request *request, enum aws_http_header_block header_block) {
    AWS_PRECONDITION(request);

    if (aws_http_stream_get_incoming_response_status(request->stream, &request->result.response_status)) {
        AWS_LOGF_ERROR(AWS_LS_S3_REQUEST, "id=%p Could not get response status for s3 request.", (void *)request);
        return AWS_OP_ERR;
    }

    struct aws_s3_request_vtable *vtable = request->vtable;

    if (vtable->incoming_header_block_done != NULL) {
        return vtable->incoming_header_block_done(request, header_block);
    }
    return AWS_OP_SUCCESS;
}

int aws_s3_request_incoming_body(struct aws_s3_request *request, const struct aws_byte_cursor *data) {
    AWS_PRECONDITION(request);

    struct aws_s3_request_vtable *vtable = request->vtable;

    if (vtable->incoming_body != NULL) {
        if (vtable->incoming_body(request, data)) {
            return AWS_OP_ERR;
        }
    }

    if (request->body_callback != NULL) {
        return request->body_callback(request, request->stream, data, request->user_data);
    }

    return AWS_OP_SUCCESS;
}

void aws_s3_request_stream_complete(struct aws_s3_request *request, int error_code) {
    AWS_PRECONDITION(request);

    struct aws_s3_request_vtable *vtable = request->vtable;

    if (vtable->stream_complete != NULL) {
        vtable->stream_complete(request, error_code);
    }
}

void aws_s3_request_finish(struct aws_s3_request *request, int error_code) {
    AWS_PRECONDITION(request);

    struct aws_s3_request_vtable *vtable = request->vtable;

    if (vtable->request_finish != NULL) {
        vtable->request_finish(request, error_code);
    }

    if (request->finish_callback != NULL) {
        request->finish_callback(request, error_code, request->user_data);
    }
}
