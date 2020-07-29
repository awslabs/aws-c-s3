#ifndef AWS_S3_META_REQUEST_H
#define AWS_S3_META_REQUEST_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/s3/s3.h>

struct aws_s3_meta_request;

AWS_EXTERN_C_BEGIN

AWS_S3_API
void aws_s3_meta_request_acquire(struct aws_s3_meta_request *meta_request);

AWS_S3_API
void aws_s3_meta_request_release(struct aws_s3_meta_request *meta_request);

AWS_EXTERN_C_END

#endif /* AWS_S3_META_REQUEST_H */
