#ifndef AWS_S3_PL_ALLOCATOR_H
#define AWS_S3_PL_ALLOCATOR_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

/* Allocator that locks the pages of the memory allocated so to keep sensitive memory from being saved to a swap file.*/

struct aws_allocator *aws_s3_pl_allocator_new(struct aws_allocator *allocator);

void aws_s3_pl_allocator_acquire(struct aws_allocator *allocator);

void aws_s3_pl_allocator_release(struct aws_allocator *allocator);

#endif /* AWS_S3_PL_ALLOCATOR_H */
