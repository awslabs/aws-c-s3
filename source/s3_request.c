/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/s3_request.h"
#include "aws/s3/private/s3_request_impl.h"
#include "aws/s3/s3_request_result.h"

#include <aws/common/assert.h>

int aws_s3_request_init(
    struct aws_s3_request *request,
    const struct aws_s3_request_options *request_options,
    struct aws_allocator *allocator,
    struct aws_s3_request_vtable *vtable,
    void *impl) {
    request->allocator = allocator;
    request->vtable = vtable;
    request->impl = impl;

    aws_atomic_init_int(&request->ref_count, 1);

    request->finish_callback = request_options->finish_callback;
    request->user_data = request_options->user_data;

    return AWS_OP_SUCCESS;
}

struct aws_s3_request_result *aws_s3_request_result_new(
    struct aws_s3_request *request,
    struct aws_allocator *allocator) {
    return request->vtable->request_result_new(request, allocator);
}

int aws_s3_request_build_http_request(
    struct aws_s3_request *request,
    struct aws_s3_request_context *context,
    struct aws_http_message *message) {
    return request->vtable->build_http_request(request, context, message);
}

void aws_s3_request_acquire(struct aws_s3_request *request) {
    aws_atomic_fetch_add(&request->ref_count, 1);
}

void aws_s3_request_release(struct aws_s3_request *request) {
    size_t new_ref_count = aws_atomic_fetch_sub(&request->ref_count, 1) - 1;

    if (new_ref_count > 0) {
        return;
    }

    request->vtable->destroy(request);
}

int aws_s3_request_incoming_headers(
    struct aws_s3_request *request,
    struct aws_s3_request_context *context,
    enum aws_http_header_block header_block,
    const struct aws_http_header *headers,
    size_t headers_count) {
    return request->vtable->incoming_headers(request, context, header_block, headers, headers_count);
}

int aws_s3_request_incoming_header_block_done(
    struct aws_s3_request *request,
    struct aws_s3_request_context *context,
    enum aws_http_header_block header_block) {
    return request->vtable->incoming_header_block_done(request, context, header_block);
}

int aws_s3_request_incoming_body(
    struct aws_s3_request *request,
    struct aws_s3_request_context *context,
    const struct aws_byte_cursor *data) {
    return request->vtable->incoming_body(request, context, data);
}

void aws_s3_request_stream_complete(
    struct aws_s3_request *request,
    struct aws_s3_request_context *context,
    int error_code) {

    struct aws_s3_request_result *result = aws_s3_request_context_get_request_result(context);
    aws_http_stream_get_incoming_response_status(context->stream, &result->response_status);

    request->vtable->stream_complete(request, context, error_code);
}

void aws_s3_request_finish(struct aws_s3_request *request, struct aws_s3_request_context *context, int error_code) {

    request->vtable->request_finish(request, context, error_code);

    if (request->finish_callback != NULL) {
        struct aws_s3_request_result *result = aws_s3_request_context_get_request_result(context);
        request->finish_callback(request, result, request->user_data);
    }
}
