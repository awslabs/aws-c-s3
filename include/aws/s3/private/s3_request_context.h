#ifndef AWS_S3_REQUEST_CONTEXT_H
#define AWS_S3_REQUEST_CONTEXT_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

struct aws_allocator;
struct aws_s3_client;
struct aws_s3_request;
struct aws_s3_request_result;
struct aws_signable;
struct aws_http_message;
struct aws_http_stream;

struct aws_s3_request_context {
    struct aws_allocator *allocator;
    void *impl;
    struct aws_signable *signable;
    struct aws_http_message *message;
    struct aws_http_stream *stream;
};

struct aws_s3_request_context *aws_s3_request_context_new(
    struct aws_allocator *allocator,
    struct aws_s3_client *client,
    struct aws_s3_request *request,
    struct aws_s3_request_result *result);

struct aws_s3_client *aws_s3_request_context_get_client(struct aws_s3_request_context *context);

struct aws_s3_request *aws_s3_request_context_get_request(struct aws_s3_request_context *context);

struct aws_s3_request_result *aws_s3_request_context_get_request_result(struct aws_s3_request_context *context);

void aws_s3_request_context_destroy(struct aws_s3_request_context *context);

#endif
