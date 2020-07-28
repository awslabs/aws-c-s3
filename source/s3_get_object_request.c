/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_get_object_request.h"
#include "aws/s3/private/s3_request.h"
#include "aws/s3/s3_client.h"

#include <aws/common/string.h>
#include <aws/http/request_response.h>
#include <aws/io/stream.h>
#include <inttypes.h>

static void s_s3_get_object_request_destroy(struct aws_s3_request *request);

static int s_s3_get_object_request_incoming_headers(
    struct aws_s3_request *request,
    enum aws_http_header_block header_block,
    const struct aws_http_header *headers,
    size_t headers_count);

static int s_s3_get_object_request_incoming_header_block_done(
    struct aws_s3_request *request,
    enum aws_http_header_block header_block);

static int s_s3_get_object_request_incoming_body(struct aws_s3_request *request, const struct aws_byte_cursor *data);

static void s_s3_get_object_request_stream_complete(struct aws_s3_request *request, int error_code);

static void s_s3_get_object_request_finish(struct aws_s3_request *request, int error_code);

static struct aws_s3_request_vtable s_s3_get_object_request_vtable = {
    .destroy = s_s3_get_object_request_destroy,
    .incoming_headers = s_s3_get_object_request_incoming_headers,
    .incoming_header_block_done = s_s3_get_object_request_incoming_header_block_done,
    .incoming_body = s_s3_get_object_request_incoming_body,
    .stream_complete = s_s3_get_object_request_stream_complete,
    .request_finish = s_s3_get_object_request_finish};

struct aws_s3_request *aws_s3_get_object_request_new(
    struct aws_allocator *allocator,
    const struct aws_s3_request_options *options) {

    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(options);

    struct aws_s3_get_object_request *get_object =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_get_object_request));

    if (get_object == NULL) {
        AWS_LOGF_ERROR(AWS_LS_S3_CLIENT, "Could not allocate aws_s3_get_object_request");
        return NULL;
    }

    struct aws_s3_request *s3_request = &get_object->s3_request;

    /* Initialize the base type. */
    if (aws_s3_request_init(s3_request, allocator, &s_s3_get_object_request_vtable, get_object, options)) {
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

static void s_s3_get_object_request_destroy(struct aws_s3_request *request) {
    AWS_PRECONDITION(request);

    struct aws_s3_get_object_request *get_object = (struct aws_s3_get_object_request *)request->impl;
    struct aws_s3_get_object_result_output *output = &get_object->result.output;

    if (output->content_type != NULL) {
        aws_string_destroy(output->content_type);
        output->content_type = NULL;
    }

    aws_mem_release(request->allocator, get_object);
}

static int s_s3_get_object_request_incoming_headers(
    struct aws_s3_request *request,
    enum aws_http_header_block header_block,
    const struct aws_http_header *headers,
    size_t headers_count) {

    AWS_PRECONDITION(request);

    (void)header_block;

    /* TODO look at removing in the future--these particular headers likely don't need to be get_object specific */
    struct aws_s3_get_object_request *get_object_request = request->impl;
    struct aws_s3_get_object_result *get_object_result = &get_object_request->result;
    struct aws_s3_get_object_result_output *output = &get_object_result->output;

    for (size_t i = 0; i < headers_count; ++i) {
        const struct aws_byte_cursor *name = &headers[i].name;
        const struct aws_byte_cursor *value = &headers[i].value;

        if (aws_byte_cursor_eq_c_str(name, "Content-Type")) {
            output->content_type = aws_string_new_from_array(request->allocator, value->ptr, value->len);

            AWS_LOGF_INFO(AWS_LS_S3_GENERAL, "Content-Type: %s", (const char *)output->content_type->bytes);
        } else if (aws_byte_cursor_eq_c_str(name, "Content-Length")) {
            output->content_length = atoi((const char *)value->ptr);

            AWS_LOGF_INFO(AWS_LS_S3_GENERAL, "Content-Length: %" PRIu64, (uint64_t)output->content_length);
        }
    }

    return AWS_OP_SUCCESS;
}

static int s_s3_get_object_request_incoming_header_block_done(
    struct aws_s3_request *request,
    enum aws_http_header_block header_block) {
    AWS_PRECONDITION(request);
    (void)request;
    (void)header_block;
    return AWS_OP_SUCCESS;
}

static int s_s3_get_object_request_incoming_body(struct aws_s3_request *request, const struct aws_byte_cursor *data) {
    AWS_PRECONDITION(request);
    (void)request;
    (void)data;
    return AWS_OP_SUCCESS;
}

static void s_s3_get_object_request_stream_complete(struct aws_s3_request *request, int error_code) {
    AWS_PRECONDITION(request);
    (void)request;
    (void)error_code;
}

static void s_s3_get_object_request_finish(struct aws_s3_request *request, int error_code) {
    AWS_PRECONDITION(request);
    (void)request;
    (void)error_code;
}
