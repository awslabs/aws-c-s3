#ifndef AWS_S3_PL_ALLOCATOR_H
#define AWS_S3_PL_ALLOCATOR_H

#include <aws/s3/s3.h>

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

/* Allocator that locks the pages of the memory allocated to keep sensitive memory from being saved to a swap file.*/

AWS_EXTERN_C_BEGIN

AWS_S3_API
struct aws_allocator *aws_s3_pl_allocator_new(struct aws_allocator *allocator);

AWS_S3_API
void aws_s3_pl_allocator_acquire(struct aws_allocator *allocator);

AWS_S3_API
void aws_s3_pl_allocator_release(struct aws_allocator *allocator);

AWS_EXTERN_C_END

#endif /* AWS_S3_PL_ALLOCATOR_H */
