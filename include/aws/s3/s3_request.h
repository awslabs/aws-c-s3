#ifndef AWS_S3_REQUEST_H
#define AWS_S3_REQUEST_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/atomics.h>
#include <aws/http/request_response.h>
#include <aws/s3/s3.h>

struct aws_s3_request;
struct aws_s3_request_result;
struct aws_s3_request_context;

struct aws_http_message;

/* To be kept after the function is done executing, request and result need to have their reference count incremented
 * via their respective acquire functions.*/
typedef void(aws_s3_request_finish_callback)(
    struct aws_s3_request *request,
    struct aws_s3_request_result *result,
    void *user_data);

struct aws_s3_request_vtable {
    struct aws_s3_request_result *(
        *request_result_new)(struct aws_s3_request *request, struct aws_allocator *allocator);

    int (*build_http_request)(
        struct aws_s3_request *request,
        struct aws_s3_request_context *context,
        struct aws_http_message *message);

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

    void (*request_finish)(struct aws_s3_request *request, struct aws_s3_request_context *context, int error_code);
};

struct aws_s3_request_options {
    aws_s3_request_finish_callback *finish_callback;
    void *user_data;
};

struct aws_s3_request {
    struct aws_allocator *allocator;
    struct aws_s3_request_vtable *vtable;
    void *impl;
    aws_s3_request_finish_callback *finish_callback;
    void *user_data;
    struct aws_atomic_var ref_count;
};

AWS_EXTERN_C_BEGIN

AWS_S3_API
void aws_s3_request_acquire(struct aws_s3_request *request);

AWS_S3_API
void aws_s3_request_release(struct aws_s3_request *request);

AWS_S3_API
int aws_s3_request_init(
    struct aws_s3_request *request,
    const struct aws_s3_request_options *request_options,
    struct aws_allocator *allocator,
    struct aws_s3_request_vtable *vtable,
    void *impl);

AWS_EXTERN_C_END

#endif /* AWS_S3_REQUEST_H */
