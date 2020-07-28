/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_request.h"
#include "aws/s3/private/s3_client_impl.h"

#include <aws/auth/signable.h>
#include <aws/common/assert.h>
#include <aws/http/connection_manager.h>

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

    if (options->client == NULL || options->message == NULL) {
        AWS_LOGF_ERROR(AWS_LS_S3_CLIENT, "Cannot not allocate aws_s3_get_object_request; options are invalid.");
        aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        return AWS_OP_ERR;
    }

    request->allocator = allocator;
    request->vtable = vtable;
    request->impl = impl;

    /* Initialize to 1 so the caller gets an initial reference. */
    aws_atomic_init_int(&request->ref_count, 1);

    /* Increase reference count of the client and message to ensure they stay around. */
    aws_s3_client_acquire(options->client);
    request->client = options->client;

    aws_http_message_acquire(options->message);
    request->message = options->message;

    request->body_callback = options->body_callback;
    request->finish_callback = options->finish_callback;
    request->user_data = options->user_data;

    return AWS_OP_SUCCESS;
}

void aws_s3_request_acquire(struct aws_s3_request *request) {
    AWS_PRECONDITION(request);
    aws_atomic_fetch_add(&request->ref_count, 1);
}

void aws_s3_request_release(struct aws_s3_request *request) {
    AWS_PRECONDITION(request);

    size_t new_ref_count = aws_atomic_fetch_sub(&request->ref_count, 1) - 1;

    if (new_ref_count > 0) {
        return;
    }

    if (request->stream != NULL) {

        struct aws_http_connection *connection = aws_http_stream_get_connection(request->stream);

        if (connection != NULL) {
            aws_http_connection_manager_release_connection(request->client->connection_manager, connection);
            connection = NULL;
        }

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

    if (request->client != NULL) {
        aws_s3_client_release(request->client);
        request->client = NULL;
    }

    request->vtable->destroy(request);
}

int aws_s3_request_incoming_headers(
    struct aws_s3_request *request,
    enum aws_http_header_block header_block,
    const struct aws_http_header *headers,
    size_t headers_count) {
    AWS_PRECONDITION(request);
    return request->vtable->incoming_headers(request, header_block, headers, headers_count);
}

int aws_s3_request_incoming_header_block_done(struct aws_s3_request *request, enum aws_http_header_block header_block) {
    AWS_PRECONDITION(request);
    return request->vtable->incoming_header_block_done(request, header_block);
}

int aws_s3_request_incoming_body(struct aws_s3_request *request, const struct aws_byte_cursor *data) {
    AWS_PRECONDITION(request);

    if (request->vtable->incoming_body(request, data)) {
        return AWS_OP_ERR;
    }

    if (request->body_callback != NULL) {
        return request->body_callback(request, request->stream, data, request->user_data);
    }

    return AWS_OP_SUCCESS;
}

void aws_s3_request_stream_complete(struct aws_s3_request *request, int error_code) {
    AWS_PRECONDITION(request);

    if (error_code == AWS_ERROR_SUCCESS) {
        if (aws_http_stream_get_incoming_response_status(request->stream, &request->result.response_status)) {
            error_code = aws_last_error();
        }
    }

    request->vtable->stream_complete(request, error_code);
}

void aws_s3_request_finish(struct aws_s3_request *request, int error_code) {
    AWS_PRECONDITION(request);

    request->vtable->request_finish(request, error_code);

    if (request->finish_callback != NULL) {
        request->finish_callback(request, error_code, request->user_data);
    }
}
