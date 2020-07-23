#ifndef AWS_S3_PUT_OBJECT_REQUEST_H
#define AWS_S3_PUT_OBJECT_REQUEST_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_request.h"

struct aws_s3_put_object_request {
    struct aws_s3_request s3_request;
};

struct aws_s3_request *aws_s3_put_object_request_new(
    struct aws_allocator *allocator,
    const struct aws_s3_request_options *options);

#endif /* AWS_S3_REQUEST_GET_OBJECT_H */
