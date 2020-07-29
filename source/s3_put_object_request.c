/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_put_object_request.h"
#include "aws/s3/private/s3_request.h"
#include "aws/s3/s3_client.h"

#include <aws/common/string.h>
#include <aws/http/request_response.h>
#include <aws/io/stream.h>
#include <inttypes.h>

static void s_s3_put_object_request_destroy(struct aws_s3_request *request);

static int s_s3_put_object_request_incoming_headers(
    struct aws_s3_request *request,
    enum aws_http_header_block header_block,
    const struct aws_http_header *headers,
    size_t headers_count);

static int s_s3_put_object_request_incoming_header_block_done(
    struct aws_s3_request *request,
    enum aws_http_header_block header_block);

static int s_s3_put_object_request_incoming_body(struct aws_s3_request *request, const struct aws_byte_cursor *data);

static void s_s3_put_object_request_stream_complete(struct aws_s3_request *request, int error_code);

static void s_s3_put_object_request_finish(struct aws_s3_request *request, int error_code);

static struct aws_s3_request_vtable s_s3_put_object_request_vtable = {
    .destroy = s_s3_put_object_request_destroy,
    .incoming_headers = s_s3_put_object_request_incoming_headers,
    .incoming_header_block_done = s_s3_put_object_request_incoming_header_block_done,
    .incoming_body = s_s3_put_object_request_incoming_body,
    .stream_complete = s_s3_put_object_request_stream_complete,
    .request_finish = s_s3_put_object_request_finish};

struct aws_s3_request *aws_s3_put_object_request_new(
    struct aws_allocator *allocator,
    const struct aws_s3_request_options *options) {

    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(options);

    struct aws_s3_put_object_request *put_object =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_put_object_request));

    if (put_object == NULL) {
        AWS_LOGF_ERROR(AWS_LS_S3_CLIENT, "Could not allocate aws_s3_put_object_request");
        return NULL;
    }

    struct aws_s3_request *s3_request = &put_object->s3_request;

    /* Initialize the base type. */
    if (aws_s3_request_init(s3_request, allocator, &s_s3_put_object_request_vtable, put_object, options)) {
        AWS_LOGF_ERROR(AWS_LS_S3_CLIENT, "id=%p Could not initialize base aws_s3_request type", (void *)s3_request);
        goto error_clean_up_request;
    }

    return s3_request;

error_clean_up_request:

    if (s3_request != NULL) {
        aws_s3_request_release(s3_request);
        s3_request = NULL;
    }

    return NULL;
}

static void s_s3_put_object_request_destroy(struct aws_s3_request *request) {
    AWS_PRECONDITION(request);

    struct aws_s3_put_object_request *put_object = request->impl;
    aws_mem_release(request->allocator, put_object);
}

static int s_s3_put_object_request_incoming_headers(
    struct aws_s3_request *request,
    enum aws_http_header_block header_block,
    const struct aws_http_header *headers,
    size_t headers_count) {

    AWS_PRECONDITION(request);
    (void)request;
    (void)header_block;
    (void)headers;
    (void)headers_count;
    return AWS_OP_SUCCESS;
}

static int s_s3_put_object_request_incoming_header_block_done(
    struct aws_s3_request *request,
    enum aws_http_header_block header_block) {
    AWS_PRECONDITION(request);
    (void)request;
    (void)header_block;
    return AWS_OP_SUCCESS;
}

static int s_s3_put_object_request_incoming_body(struct aws_s3_request *request, const struct aws_byte_cursor *data) {
    AWS_PRECONDITION(request);
    (void)request;
    (void)data;
    return AWS_OP_SUCCESS;
}

static void s_s3_put_object_request_stream_complete(struct aws_s3_request *request, int error_code) {
    AWS_PRECONDITION(request);
    (void)request;
    (void)error_code;
}

static void s_s3_put_object_request_finish(struct aws_s3_request *request, int error_code) {
    AWS_PRECONDITION(request);
    (void)request;
    (void)error_code;
}
