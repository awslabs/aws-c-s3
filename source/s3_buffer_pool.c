/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/s3/private/s3_buffer_pool.h>

#include <aws/common/mutex.h>
#include <aws/common/array_list.h>

static size_t s_min_num_blocks = 2;

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

struct aws_s3_buffer_pool *aws_s3_buffer_pool_new(struct aws_allocator *allocator,
    size_t block_size, size_t max_mem_usage) {
    struct aws_s3_buffer_pool *buffer_pool = aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_buffer_pool));

    AWS_FATAL_ASSERT(buffer_pool != NULL);

    buffer_pool->base_allocator = allocator;
    buffer_pool->block_size = block_size;
    buffer_pool->current_mem_usage = 0;
    buffer_pool->max_mem_usage = max_mem_usage;
    if (aws_mutex_init(&buffer_pool->mutex)) {
        goto on_error;
    }

    aws_array_list_init_dynamic(&buffer_pool->blocks, allocator, 
            s_min_num_blocks, sizeof(struct s3_buffer_pool_block));

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

static void s_s3_buf_pool_trim(struct aws_s3_buffer_pool *impl) {
    size_t swap_count = 0;

    for (size_t i = 0; i < aws_array_list_length(&impl->blocks); ++i) {
        struct s3_buffer_pool_block *block;
        aws_array_list_get_at_ptr(&impl->blocks, (void **)&block, i);

        if (block->block_ptr == 0) {
            size_t swap_loc = aws_array_list_length(&impl->blocks) - 1 - swap_count;
            if (swap_loc == i) {
                break;
            } else {
                aws_array_list_swap(&impl->blocks, i, swap_loc);
            }
        }
    }

    for (size_t i = 0; i < swap_count; ++i) {
        impl->current_mem_usage -= impl->block_size;
        aws_array_list_pop_back(&impl->blocks);
    }
}

static void *s_primary_acquire(struct aws_s3_buffer_pool *buffer_pool, size_t size) {
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

    if (alloc_ptr == NULL && buffer_pool->current_mem_usage + buffer_pool->block_size < buffer_pool->max_mem_usage) {
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

    return alloc_ptr;
}

void *aws_s3_buffer_pool_acquire(struct aws_s3_buffer_pool *buffer_pool, size_t size) {
    AWS_PRECONDITION(buffer_pool);

    if (size < buffer_pool->block_size) {
        return s_primary_acquire(buffer_pool, size);
    }

    uint8_t *alloc_ptr = NULL;
    aws_mutex_lock(&buffer_pool->mutex);
    if (buffer_pool->current_mem_usage + size <= buffer_pool->max_mem_usage) {
        buffer_pool->current_mem_usage += size;
        alloc_ptr = aws_mem_acquire(buffer_pool->base_allocator, size);
    } else {
        s_s3_buf_pool_trim(buffer_pool);
        if (buffer_pool->current_mem_usage + size <= buffer_pool->max_mem_usage) {
            buffer_pool->current_mem_usage += size;
            alloc_ptr = aws_mem_acquire(buffer_pool->base_allocator, size);
        }
    }
    aws_mutex_unlock(&buffer_pool->mutex);

    return alloc_ptr;
}

void aws_s3_buffer_pool_release(struct aws_s3_buffer_pool *buffer_pool, void *ptr) {
    AWS_PRECONDITION(buffer_pool);

    if (ptr == NULL) {
        return;
    }

    bool found = false;
    aws_mutex_lock(&buffer_pool->mutex);
    for (size_t i = 0; i < aws_array_list_length(&buffer_pool->blocks); ++i) {
        struct s3_buffer_pool_block *block;
        aws_array_list_get_at_ptr(&buffer_pool->blocks, (void **)&block, i);

        if (block->block_ptr <= (uint8_t *)ptr && block->block_ptr + block->block_size > (uint8_t *)ptr) {
            found = true;
            block->alloc_count -= 1;
            break;
        }
    }

    if (!found) {
        aws_mem_release(buffer_pool->base_allocator, ptr);
    }
    aws_mutex_unlock(&buffer_pool->mutex);
}
