/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/model/s3_get_object_request.h"
#include "aws/s3/model/s3_get_object_result.h"
#include "aws/s3/private/s3_request_context.h"
#include "aws/s3/private/s3_request_impl.h"
#include "aws/s3/private/s3_request_result_impl.h"
#include "aws/s3/s3_client.h"
#include "aws/s3/s3_request.h"
#include "aws/s3/s3_request_result.h"

#include <aws/common/string.h>
#include <aws/http/request_response.h>
#include <aws/io/stream.h>
#include <inttypes.h>

struct aws_s3_client;

struct aws_s3_request_get_object {
    struct aws_s3_request s3_request;

    struct aws_string *key;

    aws_s3_request_get_object_body_callback *body_callback;
};

static struct aws_s3_request_result *s_s3_request_get_object_result_new(
    struct aws_s3_request *request,
    struct aws_allocator *allocator);
static int s_s3_request_get_object_build_http_request(
    struct aws_s3_request *request,
    struct aws_s3_request_context *context,
    struct aws_http_message *message);
static int s_s3_request_get_object_cancel(struct aws_s3_request *request);
static void s_s3_request_get_object_destroy(struct aws_s3_request *request);

static int s_s3_request_get_object_incoming_headers(
    struct aws_s3_request *request,
    struct aws_s3_request_context *context,
    enum aws_http_header_block header_block,
    const struct aws_http_header *headers,
    size_t headers_count);

static int s_s3_request_get_object_incoming_header_block_done(
    struct aws_s3_request *request,
    struct aws_s3_request_context *context,
    enum aws_http_header_block header_block);

static int s_s3_request_get_object_incoming_body(
    struct aws_s3_request *request,
    struct aws_s3_request_context *context,
    const struct aws_byte_cursor *data);

static void s_s3_request_get_object_stream_complete(
    struct aws_s3_request *request,
    struct aws_s3_request_context *context,
    int error_code);

static struct aws_s3_request_vtable s_s3_request_get_object_vtable = {
    .request_result_new = s_s3_request_get_object_result_new,
    .build_http_request = s_s3_request_get_object_build_http_request,
    .cancel = s_s3_request_get_object_cancel,
    .destroy = s_s3_request_get_object_destroy,
    .incoming_headers = s_s3_request_get_object_incoming_headers,
    .incoming_header_block_done = s_s3_request_get_object_incoming_header_block_done,
    .incoming_body = s_s3_request_get_object_incoming_body,
    .stream_complete = s_s3_request_get_object_stream_complete};

struct aws_s3_request *aws_s3_request_get_object_new(
    struct aws_allocator *allocator,
    struct aws_s3_request_get_object_options *options) {

    struct aws_s3_request_get_object *get_object =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_request_get_object));

    if (get_object == NULL) {
        return NULL;
    }

    struct aws_s3_request *request = &get_object->s3_request;

    if (aws_s3_request_init(
            request, &options->request_options, allocator, &s_s3_request_get_object_vtable, get_object)) {
        goto error_clean_up_request;
    }

    if (!aws_byte_cursor_is_valid(&options->key)) {
        goto error_clean_up_request;
    }

    get_object->key = aws_string_new_from_array(request->allocator, options->key.ptr, options->key.len);

    if (get_object->key == NULL) {
        goto error_clean_up_request;
    }

    get_object->body_callback = options->body_callback;

    return request;

error_clean_up_request:
    aws_s3_request_release(request);
    request = NULL;
    return NULL;
}

static int s_s3_request_get_object_build_http_request(
    struct aws_s3_request *request,
    struct aws_s3_request_context *context,
    struct aws_http_message *message) {

    (void)context;

    struct aws_s3_request_get_object *get_object = request->impl;

    if (aws_http_message_set_request_method(message, aws_http_method_get)) {
        return AWS_OP_ERR;
    }

    struct aws_string *key = get_object->key;

    if (!aws_string_is_valid(key)) {
        return AWS_OP_ERR;
    }

    if (aws_http_message_set_request_path(message, aws_byte_cursor_from_array(key->bytes, key->len))) {
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}

static struct aws_s3_request_result *s_s3_request_get_object_result_new(
    struct aws_s3_request *request,
    struct aws_allocator *allocator) {
    (void)request;
    return aws_s3_request_result_get_object_new(allocator);
}

static int s_s3_request_get_object_cancel(struct aws_s3_request *request) {
    (void)request;
    return AWS_OP_SUCCESS;
}

static void s_s3_request_get_object_destroy(struct aws_s3_request *request) {
    struct aws_s3_request_get_object *get_object = (struct aws_s3_request_get_object *)request->impl;

    if (get_object->key != NULL) {
        aws_string_destroy(get_object->key);
        get_object->key = NULL;
    }

    aws_mem_release(request->allocator, request);
}

static int s_s3_request_get_object_incoming_headers(
    struct aws_s3_request *request,
    struct aws_s3_request_context *context,
    enum aws_http_header_block header_block,
    const struct aws_http_header *headers,
    size_t headers_count) {

    (void)request;
    (void)header_block;

    struct aws_s3_request_result *result = aws_s3_request_context_get_request_result(context);
    struct aws_s3_request_result_get_object_output *output =
        (struct aws_s3_request_result_get_object_output *)aws_s3_request_result_get_output(result);

    for (size_t i = 0; i < headers_count; ++i) {
        const struct aws_byte_cursor *name = &headers[i].name;
        const struct aws_byte_cursor *value = &headers[i].value;

        if (aws_byte_cursor_eq_c_str(name, "Content-Type")) {
            output->content_type = aws_string_new_from_array(result->allocator, value->ptr, value->len);

            AWS_LOGF_INFO(AWS_LS_S3_GENERAL, "Content-Type: %s", (const char *)output->content_type->bytes);
        } else if (aws_byte_cursor_eq_c_str(name, "Content-Length")) {
            output->content_length = atoi((const char *)value->ptr);

            AWS_LOGF_INFO(AWS_LS_S3_GENERAL, "Content-Length: %" PRIu64, (uint64_t)output->content_length);
        }
    }

    return AWS_OP_SUCCESS;
}

static int s_s3_request_get_object_incoming_header_block_done(
    struct aws_s3_request *request,
    struct aws_s3_request_context *context,
    enum aws_http_header_block header_block) {
    (void)request;
    (void)context;
    (void)header_block;
    return AWS_OP_SUCCESS;
}

static int s_s3_request_get_object_incoming_body(
    struct aws_s3_request *request,
    struct aws_s3_request_context *context,
    const struct aws_byte_cursor *data) {

    struct aws_s3_request_get_object *get_object = (struct aws_s3_request_get_object *)request->impl;

    if (get_object->body_callback == NULL) {
        return AWS_OP_SUCCESS;
    }

    return get_object->body_callback(get_object, context->stream, data, request->user_data);
}

static void s_s3_request_get_object_stream_complete(
    struct aws_s3_request *request,
    struct aws_s3_request_context *context,
    int error_code) {
    (void)request;
    (void)context;
    (void)error_code;
}
