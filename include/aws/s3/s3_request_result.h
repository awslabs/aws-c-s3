#ifndef AWS_S3_REQUEST_RESULT_H
#define AWS_S3_REQUEST_RESULT_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/s3/s3.h>

struct aws_s3_request_result;

AWS_EXTERN_C_BEGIN

AWS_S3_API
int aws_s3_request_result_acquire(struct aws_s3_request_result *result);

AWS_S3_API
void aws_s3_request_result_release(struct aws_s3_request_result *result);

AWS_S3_API
int aws_s3_request_result_get_response_status(struct aws_s3_request_result *result);

AWS_S3_API
void *aws_s3_request_result_get_output(struct aws_s3_request_result *result);

AWS_EXTERN_C_END

#endif
