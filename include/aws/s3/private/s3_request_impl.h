#ifndef AWS_S3_REQUEST_IMPL_H
#define AWS_S3_REQUEST_IMPL_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/atomics.h>
#include <aws/http/request_response.h>

#include "aws/s3/private/s3_request_context.h"
#include "aws/s3/s3_request.h"

struct aws_s3_client;

struct aws_s3_request_vtable {
    struct aws_s3_request_result *(
        *request_result_new)(struct aws_s3_request *request, struct aws_allocator *allocator);

    int (*build_http_request)(
        struct aws_s3_request *request,
        struct aws_s3_request_context *context,
        struct aws_http_message *message);

    int (*cancel)(struct aws_s3_request *request);

    void (*destroy)(struct aws_s3_request *request);

    int (*incoming_headers)(
        struct aws_s3_request *request,
        struct aws_s3_request_context *context,
        enum aws_http_header_block header_block,
        const struct aws_http_header *headers,
        size_t headers_count);

    int (*incoming_header_block_done)(
        struct aws_s3_request *request,
        struct aws_s3_request_context *context,
        enum aws_http_header_block header_block);

    int (*incoming_body)(
        struct aws_s3_request *request,
        struct aws_s3_request_context *context,
        const struct aws_byte_cursor *data);

    void (*stream_complete)(struct aws_s3_request *request, struct aws_s3_request_context *context, int error_code);
};

struct aws_s3_request {
    struct aws_allocator *allocator;
    struct aws_s3_request_vtable *vtable;
    void *impl;
    struct aws_atomic_var ref_count;
    aws_s3_request_finish_callback *finish_callback;
    void *user_data;
};

int aws_s3_request_init(
    struct aws_s3_request *request,
    struct aws_s3_request_options *request_options,
    struct aws_allocator *allocator,
    struct aws_s3_request_vtable *vtable,
    void *impl);

struct aws_s3_request_result *aws_s3_request_result_new(
    struct aws_s3_request *request,
    struct aws_allocator *allocator);

int aws_s3_request_build_http_request(
    struct aws_s3_request *request,
    struct aws_s3_request_context *context,
    struct aws_http_message *message);

int aws_s3_request_incoming_header_block_done(
    struct aws_s3_request *request,
    struct aws_s3_request_context *context,
    enum aws_http_header_block header_block);

int aws_s3_request_incoming_headers(
    struct aws_s3_request *request,
    struct aws_s3_request_context *context,
    enum aws_http_header_block header_block,
    const struct aws_http_header *headers,
    size_t headers_count);

int aws_s3_request_incoming_body(
    struct aws_s3_request *request,
    struct aws_s3_request_context *context,
    const struct aws_byte_cursor *data);

void aws_s3_request_stream_complete(
    struct aws_s3_request *request,
    struct aws_s3_request_context *context,
    int error_code);

void aws_s3_request_finish(struct aws_s3_request *request, struct aws_s3_request_context *context, int error_code);

#endif
