/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/s3/private/s3_buffer_pool.h>

#include <aws/common/array_list.h>
#include <aws/common/mutex.h>

/*
 * S3 Buffer Pool.
 * Fairly trivial implementation of "arena" style allocator.
 * Note: current implementation is not optimized and instead tries to be
 * as straightforward as possible. Given that pool manages a small number
 * of big allocations, performance impact is not that bad, but something we need
 * to look into on the next iteration.
 *
 * Basic approach is to keep a pool of blocks (block size provided by caller,
 * ex. 128 MB as a typical size). This is referred to as primary storage.
 * At high level, each block has offset ptr and allocation basically just means
 * bumping the offset ptr up and increasing blocks alloc count.
 * Deallocation decreases the block alloc count and if alloc count ever reaches
 * 0, the offset ptr is reset back to the start of the block.
 * For acquires less than a block size, the current approach is to go through
 * all blocks and see if any of them can fit the size and if not a new block is
 * created.
 * For acquires over block size, pool falls back to acquiring/releasing through
 * base allocator.
 * If memory limit is set, then acquires will return empty buffers when memory
 * limit is hit. For primary, that means acquires will fail when new block is
 * needed, but there is no mem for it. For secondary storage, it tracks mem used
 * by malloc.
 */

static size_t s_min_num_blocks = 5;

struct aws_s3_buffer_pool {
    struct aws_allocator *base_allocator;
    struct aws_mutex mutex;

    size_t block_size;
    size_t max_mem_usage;
    size_t current_mem_usage;

    struct aws_array_list blocks;
};

struct s3_buffer_pool_block {
    size_t block_size;
    uint8_t *block_ptr;
    uint8_t *offset_ptr;

    size_t alloc_count;
};

struct aws_s3_buffer_pool *aws_s3_buffer_pool_new(
    struct aws_allocator *allocator,
    size_t block_size,
    size_t max_mem_usage) {
    struct aws_s3_buffer_pool *buffer_pool = aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_buffer_pool));

    AWS_FATAL_ASSERT(buffer_pool != NULL);

    buffer_pool->base_allocator = allocator;
    buffer_pool->block_size = block_size;
    buffer_pool->current_mem_usage = 0;
    buffer_pool->max_mem_usage = max_mem_usage;
    if (aws_mutex_init(&buffer_pool->mutex)) {
        goto on_error;
    }

    aws_array_list_init_dynamic(&buffer_pool->blocks, allocator, s_min_num_blocks, sizeof(struct s3_buffer_pool_block));

    return buffer_pool;

on_error:
    aws_mem_release(allocator, buffer_pool);
    return NULL;
}

void aws_s3_buffer_pool_destroy(struct aws_s3_buffer_pool *buffer_pool) {
    if (buffer_pool == NULL) {
        return;
    }

    for (size_t i = 0; i < aws_array_list_length(&buffer_pool->blocks); ++i) {
        struct s3_buffer_pool_block *block;
        aws_array_list_get_at_ptr(&buffer_pool->blocks, (void **)&block, i);

        AWS_FATAL_ASSERT(block->alloc_count == 0 && "Allocator still has outstanding blocks");
        aws_mem_release(buffer_pool->base_allocator, block->block_ptr);
    }

    aws_array_list_clean_up(&buffer_pool->blocks);

    aws_mutex_clean_up(&buffer_pool->mutex);
    struct aws_allocator *base = buffer_pool->base_allocator;
    aws_mem_release(base, buffer_pool);
}

void aws_s3_buffer_pool_trim(struct aws_s3_buffer_pool *buffer_pool) {
    size_t swap_count = 0;

    for (size_t i = 0; i < aws_array_list_length(&buffer_pool->blocks); ++i) {
        struct s3_buffer_pool_block *block;
        aws_array_list_get_at_ptr(&buffer_pool->blocks, (void **)&block, i);

        if (block->alloc_count == 0) {
            size_t swap_loc = aws_array_list_length(&buffer_pool->blocks) - 1 - swap_count;
            if (swap_loc == i) {
                break;
            } else {
                aws_array_list_swap(&buffer_pool->blocks, i, swap_loc);
            }
        }
    }

    for (size_t i = 0; i < swap_count; ++i) {
        buffer_pool->current_mem_usage -= buffer_pool->block_size;
        aws_array_list_pop_back(&buffer_pool->blocks);
    }
}

static struct aws_s3_pooled_buffer s_primary_acquire(struct aws_s3_buffer_pool *buffer_pool, size_t size) {
    uint8_t *alloc_ptr = NULL;
    aws_mutex_lock(&buffer_pool->mutex);

    for (size_t i = 0; i < aws_array_list_length(&buffer_pool->blocks); ++i) {
        struct s3_buffer_pool_block *block;
        aws_array_list_get_at_ptr(&buffer_pool->blocks, (void **)&block, i);

        if (block->alloc_count == 0) {
            block->offset_ptr = block->block_ptr + size;
            alloc_ptr = block->block_ptr;
            block->alloc_count += 1;
            break;
        }

        if (block->offset_ptr + size < block->block_ptr + block->block_size) {
            alloc_ptr = block->offset_ptr;
            block->offset_ptr += size;
            block->alloc_count += 1;
            break;
        }
    }

    if (alloc_ptr == NULL && (buffer_pool->max_mem_usage == 0 ||
                              buffer_pool->current_mem_usage + buffer_pool->block_size <= buffer_pool->max_mem_usage)) {
        struct s3_buffer_pool_block block;
        block.alloc_count = 1;
        block.block_ptr = aws_mem_acquire(buffer_pool->base_allocator, buffer_pool->block_size);
        block.offset_ptr = block.block_ptr + size;
        block.block_size = buffer_pool->block_size;
        aws_array_list_push_back(&buffer_pool->blocks, &block);
        buffer_pool->current_mem_usage += buffer_pool->block_size;
        alloc_ptr = block.block_ptr;
    }

    aws_mutex_unlock(&buffer_pool->mutex);

    return (struct aws_s3_pooled_buffer){.ptr = alloc_ptr, .size = size};
}

struct aws_s3_pooled_buffer aws_s3_buffer_pool_acquire_buffer(struct aws_s3_buffer_pool *buffer_pool, size_t size) {
    AWS_PRECONDITION(buffer_pool);

    if (size < buffer_pool->block_size) {
        return s_primary_acquire(buffer_pool, size);
    }

    uint8_t *alloc_ptr = NULL;
    aws_mutex_lock(&buffer_pool->mutex);
    if (buffer_pool->max_mem_usage == 0 || buffer_pool->current_mem_usage + size <= buffer_pool->max_mem_usage) {
        buffer_pool->current_mem_usage += size;
        alloc_ptr = aws_mem_acquire(buffer_pool->base_allocator, size);
    } else {
        aws_s3_buffer_pool_trim(buffer_pool);
        if (buffer_pool->current_mem_usage + size <= buffer_pool->max_mem_usage) {
            buffer_pool->current_mem_usage += size;
            alloc_ptr = aws_mem_acquire(buffer_pool->base_allocator, size);
        }
    }
    aws_mutex_unlock(&buffer_pool->mutex);

    return (struct aws_s3_pooled_buffer){.ptr = alloc_ptr, .size = alloc_ptr == NULL ? 0 : size};
}

void aws_s3_buffer_pool_release_buffer(
    struct aws_s3_buffer_pool *buffer_pool,
    struct aws_s3_pooled_buffer pooled_buffer) {
    if (buffer_pool == NULL || pooled_buffer.ptr == NULL || pooled_buffer.size == 0) {
        return;
    }

    aws_mutex_lock(&buffer_pool->mutex);
    if (pooled_buffer.size < buffer_pool->block_size) {
        for (size_t i = 0; i < aws_array_list_length(&buffer_pool->blocks); ++i) {
            struct s3_buffer_pool_block *block;
            aws_array_list_get_at_ptr(&buffer_pool->blocks, (void **)&block, i);

            if (block->block_ptr <= pooled_buffer.ptr && block->block_ptr + block->block_size > pooled_buffer.ptr) {
                block->alloc_count -= 1;
                break;
            }
        }
    } else {
        aws_mem_release(buffer_pool->base_allocator, pooled_buffer.ptr);
    }

    buffer_pool->current_mem_usage -= pooled_buffer.size;

    aws_mutex_unlock(&buffer_pool->mutex);
}

struct aws_s3_buffer_pool_usage_stats aws_s3_buffer_pool_get_usage(struct aws_s3_buffer_pool *buffer_pool) {
    aws_mutex_lock(&buffer_pool->mutex);
    struct aws_s3_buffer_pool_usage_stats ret = (struct aws_s3_buffer_pool_usage_stats){
        .max_size = buffer_pool->max_mem_usage,
        .approx_used = buffer_pool->current_mem_usage,
    };
    aws_mutex_unlock(&buffer_pool->mutex);
    return ret;
}

struct aws_byte_buf aws_byte_buf_from_pooled_buffer(struct aws_s3_pooled_buffer pooled_buffer) {
    return aws_byte_buf_from_empty_array(pooled_buffer.ptr, pooled_buffer.size);
}
