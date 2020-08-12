#ifndef AWS_S3_REQUEST_H
#define AWS_S3_REQUEST_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/atomics.h>
#include <aws/common/linked_list.h>
#include <aws/http/request_response.h>
#include <aws/s3/s3.h>

struct aws_s3_client;
struct aws_s3_request;

struct aws_allocator;
struct aws_signable;
struct aws_http_message;
struct aws_http_stream;

typedef int(aws_s3_request_body_callback_fn)(
    struct aws_s3_request *request,
    struct aws_http_stream *stream,
    const struct aws_byte_cursor *body,
    void *user_data);

typedef void(aws_s3_request_finish_callback_fn)(struct aws_s3_request *request, int error_code, void *user_data);

/* VTable for s3 requests.  Right now, these are mostly HTTP handlers to allow different behavior per request type
 * during a request. */
struct aws_s3_request_vtable {

    void (*destroy)(struct aws_s3_request *request);

    int (*incoming_headers)(
        struct aws_s3_request *request,
        enum aws_http_header_block header_block,
        const struct aws_http_header *headers,
        size_t headers_count);

    int (*incoming_header_block_done)(struct aws_s3_request *request, enum aws_http_header_block header_block);

    int (*incoming_body)(struct aws_s3_request *request, const struct aws_byte_cursor *data);

    void (*stream_complete)(struct aws_s3_request *request, int error_code);

    void (*request_finish)(struct aws_s3_request *request, int error_code);
};

struct aws_s3_request_options {
    struct aws_http_message *message;
    aws_s3_request_body_callback_fn *body_callback;
    aws_s3_request_finish_callback_fn *finish_callback;
    void *user_data;
};

struct aws_s3_request_result {
    int32_t response_status;
};

/* Base type for an s3 request.  This holds onto state about a particular request, as well as some state that is mutable
 * throughout a requests lifetime, and also stores a request result state. */
struct aws_s3_request {
    struct aws_allocator *allocator;
    struct aws_s3_request_vtable *vtable;
    void *impl;

    struct aws_atomic_var ref_count;
    struct aws_linked_list_node node;

    struct aws_s3_meta_request *meta_request;
    struct aws_http_message *message;
    aws_s3_request_body_callback_fn *body_callback;
    aws_s3_request_finish_callback_fn *finish_callback;
    void *user_data;

    struct aws_signable *signable;
    struct aws_http_stream *stream;

    struct aws_s3_request_result result;
};

void aws_s3_request_acquire(struct aws_s3_request *request);

void aws_s3_request_release(struct aws_s3_request *request);

int aws_s3_request_init(
    struct aws_s3_request *request,
    struct aws_allocator *allocator,
    struct aws_s3_request_vtable *vtable,
    void *impl,
    const struct aws_s3_request_options *options);

void aws_s3_request_set_meta_request(struct aws_s3_request *request, struct aws_s3_meta_request *meta_request);

int aws_s3_request_incoming_header_block_done(struct aws_s3_request *request, enum aws_http_header_block header_block);

int aws_s3_request_incoming_headers(
    struct aws_s3_request *request,
    enum aws_http_header_block header_block,
    const struct aws_http_header *headers,
    size_t headers_count);

int aws_s3_request_incoming_body(struct aws_s3_request *request, const struct aws_byte_cursor *data);

void aws_s3_request_stream_complete(struct aws_s3_request *request, int error_code);

void aws_s3_request_finish(struct aws_s3_request *request, int error_code);

#endif /* AWS_S3_REQUEST_H */
