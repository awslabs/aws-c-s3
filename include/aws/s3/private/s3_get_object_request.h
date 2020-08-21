#ifndef AWS_S3_GET_OBJECT_REQUEST_H
#define AWS_S3_GET_OBJECT_REQUEST_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

/* This type represents one individual get object request to S3. */

#include "aws/s3/private/s3_request.h"

struct aws_s3_get_object_request;

typedef int(aws_s3_get_object_headers_finished_fn)(
    struct aws_s3_get_object_request *request,
    struct aws_http_stream *stream,
    void *user_data);

/* Option for setting up a get object request that will be overlayed onto the existing headers. */
struct aws_s3_get_object_request_options {
    uint64_t range_start;
    uint64_t range_end;
    aws_s3_get_object_headers_finished_fn *headers_finished_callback;
    void *user_data;
};

/* Content range result of get object. */
struct aws_s3_get_object_content_result_range {
    uint64_t range_start;
    uint64_t range_end;
    uint64_t total_object_size;
};

/* Values retrieved from the get object request that we need inside the client. */
struct aws_s3_get_object_result {
    uint64_t content_length;
    struct aws_s3_get_object_content_result_range content_range;
};

/* A get-object request, derived from the type aws_s3_request. */
struct aws_s3_get_object_request {
    struct aws_s3_request s3_request;
    struct aws_s3_get_object_result result;

    /* The desired range of the object that we are trying to retrieve. */
    /* TODO try to factor these out of this structure--possibly relying on grabbing it from the message headers. */
    uint64_t range_start;
    uint64_t range_end;

    /* TODO we only need this for the initial ranged get object request where we discover the object's total size.  Try
     * to factor this out into just the use case for that request so we don't have to store these for each get-object
     * request. */
    aws_s3_get_object_headers_finished_fn *headers_finished_callback;
    void *user_data;
};

/* Allocate a new get object request. */
struct aws_s3_request *aws_s3_get_object_request_new(
    struct aws_allocator *allocator,
    const struct aws_s3_request_options *options,
    const struct aws_s3_get_object_request_options *get_object_request_options);

/* Exposed VTable so that it can be placed into a registry based on s3 request type. */
extern struct aws_s3_request_vtable g_aws_s3_request_get_object_vtable;

#endif /* AWS_S3_REQUEST_GET_OBJECT_H */
