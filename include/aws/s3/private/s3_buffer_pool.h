#ifndef AWS_S3_BUFFER_ALLOCATOR_H
#define AWS_S3_BUFFER_ALLOCATOR_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/s3/s3.h>

/*
 * S3 buffer pool.
 * Buffer pool used for pooling part sized buffers for Put/Get.
 * Provides additional functionally for limiting overall memory usage by setting
 * upper bound beyond which buffer acquisition will fail.
 */

AWS_EXTERN_C_BEGIN

struct aws_s3_buffer_pool;

struct aws_s3_pooled_buffer {
    size_t size;
    uint8_t *ptr;
};

struct aws_s3_buffer_pool_usage_stats {
    /* Max size limit. Same value as provided during creation. */
    size_t max_size;
    /* Approximate amount of memory used. Should be fairly close to actual
     * memory usage, but amount of remaining memory can vary based on
     * allocations being performed, ex. pool has several blocks with space
     * available to them, but allocation does not fit into any of them and new
     * block cannot be allocated.
     * Exercise caution when using this number and plan for allocations failing
     * even if pool seemingly has enough memory. */
    size_t approx_used;
};

/*
 * Create new buffer pool.
 * block_size - specifies size of the blocks in the primary storage. any acquires
 * bigger than block size are allocated off secondary storage.
 * max_mem_usage - limit on how much mem buffer pool can use. once limit is hit,
 * buffer pool will start return empty buffers.
 * Returns buffer pool pointer on success and NULL on failure.
 */
AWS_S3_API struct aws_s3_buffer_pool *aws_s3_buffer_pool_new(
    struct aws_allocator *allocator,
    size_t block_size,
    size_t max_mem_usage);

/*
 * Destroys buffer pool.
 * Does nothing if buffer_pool is NULL.
 */
AWS_S3_API void aws_s3_buffer_pool_destroy(struct aws_s3_buffer_pool *buffer_pool);

/*
 * Acquire buffer of specified size.
 * Returned buffer is empty (0 ptr and size) if buffer cannot be allocated.
 * Warning: Always release buffer using release api. Its fine to wrap pooled
 * buffer in another struct as long as it does not try to free underlying pointer
 * (ex. aws_byte_buf_from_pooled_buffer)
 */
AWS_S3_API struct aws_s3_pooled_buffer aws_s3_buffer_pool_acquire_buffer(
    struct aws_s3_buffer_pool *buffer_pool,
    size_t size);

/*
 * Release buffer back to the pool.
 */
AWS_S3_API void aws_s3_buffer_pool_release_buffer(
    struct aws_s3_buffer_pool *buffer_pool,
    struct aws_s3_pooled_buffer pooled_buffer);

/*
 * Get pool memory usage stats.
 */
AWS_S3_API struct aws_s3_buffer_pool_usage_stats aws_s3_buffer_pool_get_usage(struct aws_s3_buffer_pool *buffer_pool);

/*
 * Wraps pooled buffer in a static byte_buf.
 */
AWS_S3_API struct aws_byte_buf aws_byte_buf_from_pooled_buffer(struct aws_s3_pooled_buffer pooled_buffer);

/*
 * Trims all unused mem from the pool.
 * Warning: fairly slow operation, do not use in critical path.
 * TODO: partial trimming? ex. only trim down to 50% of max?
 */
AWS_S3_API void aws_s3_buffer_pool_trim(struct aws_s3_buffer_pool *buffer_pool);

AWS_EXTERN_C_END

#endif /* AWS_S3_BUFFER_ALLOCATOR_H */
