#ifndef AWS_S3_REQUEST_H
#define AWS_S3_REQUEST_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/s3/s3.h>

struct aws_s3_request;
struct aws_s3_request_result;

typedef void(aws_s3_request_finish_callback)(
    struct aws_s3_request *request,
    struct aws_s3_request_result *result,
    int error_code,
    void *user_data);

struct aws_s3_request_options {
    aws_s3_request_finish_callback *finish_callback;
    void *user_data;
};

AWS_EXTERN_C_BEGIN

AWS_S3_API
int aws_s3_request_cancel(struct aws_s3_request *request);

AWS_S3_API
int aws_s3_request_acquire(struct aws_s3_request *request);

AWS_S3_API
void aws_s3_request_release(struct aws_s3_request *request);

AWS_EXTERN_C_END

#endif
