#ifndef AWS_S3_BUFFER_ALLOCATOR_H
#define AWS_S3_BUFFER_ALLOCATOR_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/s3/s3.h>

AWS_EXTERN_C_BEGIN

struct aws_s3_buffer_pool;

AWS_S3_API struct aws_s3_buffer_pool *aws_s3_buffer_pool_new(struct aws_allocator *allocator, 
    size_t block_size, size_t max_mem_usage);

AWS_S3_API void aws_s3_buffer_pool_destroy(struct aws_s3_buffer_pool *buffer_pool);

AWS_S3_API void *aws_s3_buffer_pool_acquire(struct aws_s3_buffer_pool *buffer_pool, size_t size);
AWS_S3_API void aws_s3_buffer_pool_release(struct aws_s3_buffer_pool *buffer_pool, void *ptr);

AWS_EXTERN_C_END

#endif /* AWS_S3_BUFFER_ALLOCATOR_H */
