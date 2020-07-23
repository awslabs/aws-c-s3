#ifndef AWS_S3_ACCEL_REQUEST_CONTEXT_H
#define AWS_S3_ACCEL_REQUEST_CONTEXT_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/s3/s3.h>

struct aws_s3_accel_context;

AWS_EXTERN_C_BEGIN

AWS_S3_API
void aws_s3_accel_context_acquire(struct aws_s3_accel_context *context);

AWS_S3_API
void aws_s3_accel_context_release(struct aws_s3_accel_context *context);

AWS_EXTERN_C_END

#endif /* AWS_S3_ACCEL_CONTEXT_H */
