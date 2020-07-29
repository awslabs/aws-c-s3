#ifndef AWS_S3_ACCEL_CONTEXT_IMPL_H
#define AWS_S3_ACCEL_CONTEXT_IMPL_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/atomics.h>

#include "aws/s3/s3_accel_context.h"
#include "aws/s3/s3_client.h"

/* This represents one "accelerated" request, ie, file transfer.  Anything needed across different calls for an
 * acceleration of one particular S3 request will be stored here.  It doesn't currently track any aws_s3_request's, but
 * in the future it will likely keep track of any additional aws_s3_requests needed for accelerating a single request.
 */
struct aws_s3_accel_context {
    struct aws_allocator *allocator;
    struct aws_atomic_var ref_count;
    struct aws_s3_client *client;

    void *user_data;
    aws_s3_accel_receive_body_callback_fn *body_callback;
    aws_s3_accel_request_finish_fn *finish_callback;
};

struct aws_s3_accel_context *aws_s3_accel_context_new(
    struct aws_allocator *allocator,
    struct aws_s3_client *client,
    const struct aws_s3_accel_request_options *options);

#endif
