#ifndef AWS_S3_GET_OBJECT_REQUEST_H
#define AWS_S3_GET_OBJECT_REQUEST_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_request.h"

struct aws_s3_get_object_request;

typedef int(aws_s3_get_object_headers_finished_fn)(struct aws_s3_get_object_request *request, void *user_data);

/* Option for setting up a get object request that will be overlayed onto the existing headers. */
struct aws_s3_get_object_request_options {
    uint64_t range_start;
    uint64_t range_end;
    aws_s3_get_object_headers_finished_fn *headers_finished_callback;
    void *user_data;
};

struct aws_s3_get_object_content_result_range {
    uint64_t range_start;
    uint64_t range_end;
    uint64_t object_size;
};

/* Values retrieved from the get object request that we need inside the client. */
struct aws_s3_get_object_result {
    uint64_t content_length;
    struct aws_s3_get_object_content_result_range content_range;
};

/* A get-object request, derived from the type aws_s3_request. */
struct aws_s3_get_object_request {
    struct aws_s3_request s3_request;

    uint64_t range_start;
    uint64_t range_end;
    aws_s3_get_object_headers_finished_fn *headers_finished_callback;
    void *user_data;

    struct aws_s3_get_object_result result;
};

/* Allocate a new get object request. */
struct aws_s3_request *aws_s3_get_object_request_new(
    struct aws_allocator *allocator,
    const struct aws_s3_request_options *options,
    const struct aws_s3_get_object_request_options *get_object_request_options);

#endif /* AWS_S3_REQUEST_GET_OBJECT_H */
