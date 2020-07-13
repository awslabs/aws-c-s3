#ifndef AWS_S3_REQUEST_GET_OBJECT_H
#define AWS_S3_REQUEST_GET_OBJECT_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/byte_buf.h>
#include <aws/s3/s3_request.h>

struct aws_s3_request_get_object;
struct aws_http_stream;

typedef int(aws_s3_request_get_object_body_callback)(
    struct aws_s3_request_get_object *request,
    struct aws_http_stream *stream,
    const struct aws_byte_cursor *body,
    void *user_data);

struct aws_s3_request_get_object_options {
    struct aws_s3_request_options request_options;
    struct aws_byte_cursor key;
    aws_s3_request_get_object_body_callback *body_callback;
};

struct aws_s3_request *aws_s3_request_get_object_new(
    struct aws_allocator *allocator,
    const struct aws_s3_request_get_object_options *options);

#endif /* AWS_S3_REQUEST_GET_OBJECT_H */
