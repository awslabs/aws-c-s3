/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/s3/private/s3_buffer_pool.h>

#include <aws/common/array_list.h>
#include <aws/common/mutex.h>
#include <aws/s3/private/s3_util.h>

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

struct aws_s3_buffer_pool_ticket {
    size_t size;
    uint8_t *ptr;
};

/* Default size for blocks array. Note: this is just for meta info, blocks
 * themselves are not preallocated s*/
static size_t s_block_list_initial_capacity = 5;

/* Amount of mem reserved for use outside of buffer pool.
 * This is an optimistic upper bound on mem used as we dont track it.
 * Covers both usage outside of pool, i.e. all allocations done as part of s3
 * client as well as any allocations overruns due to memory waste in the pool. */
static const size_t s_buffer_pool_reserved_mem = MB_TO_BYTES(128);

static const size_t s_chunks_per_block = 16;

struct aws_s3_buffer_pool {
    struct aws_allocator *base_allocator;
    struct aws_mutex mutex;

    size_t block_size;
    size_t chunk_size;
    /* size at which allocations should go to secondary */
    size_t primary_size_cutoff;

    size_t mem_limit;

    bool has_reservation_hold;

    size_t primary_allocated;
    size_t primary_reserved;
    size_t primary_used;

    size_t secondary_reserved;
    size_t secondary_used;

    struct aws_array_list blocks;
};

struct s3_buffer_pool_block {
    size_t block_size;
    uint8_t *block_ptr;
    uint16_t alloc_bit_mask;
};

static inline uint16_t s_set_bit_n(uint16_t num, size_t position, size_t n) {
    uint16_t mask = ((uint16_t)0x00FF) >> (8 - n) ;
    return num | (mask << position) ;
}

static inline uint16_t s_clear_bit_n(uint16_t num, size_t position, size_t n) {
    uint16_t mask = ((uint16_t)0x00FF) >> (8 - n) ;
    return num & ~ (mask << position);
}

static inline bool s_check_bit_n(uint16_t num, size_t position, size_t n) {
    uint16_t mask = ((uint16_t)0x00FF) >> (8 - n) ;
    return (num >> position) & mask;
}

struct aws_s3_buffer_pool *aws_s3_buffer_pool_new(
    struct aws_allocator *allocator,
    size_t chunk_size,
    size_t mem_limit) {

    if (mem_limit < GB_TO_BYTES(1)) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_CLIENT, "Failed to initialize buffer pool. Min supported value for Memory Limit is 1GB.");
        aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        return NULL;
    }

    if (!(chunk_size == 0 || (chunk_size > 1024 * 1024 && chunk_size % 1024 == 0))) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_CLIENT,
            "Failed to initialize buffer pool. Chunk size must be either 0 or more than 1 mb and size must be 1 KB "
            "aligned.");
        aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        return NULL;
    }

    struct aws_s3_buffer_pool *buffer_pool = aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_buffer_pool));

    AWS_FATAL_ASSERT(buffer_pool != NULL);

    buffer_pool->base_allocator = allocator;
    buffer_pool->chunk_size = chunk_size;
    buffer_pool->block_size = chunk_size * s_chunks_per_block;
    /* Somewhat arbitrary number.
     * Tries to balance between how many allocations use buffer and buffer space
     * being wasted. */
    buffer_pool->primary_size_cutoff = chunk_size * 4;
    buffer_pool->mem_limit = mem_limit - s_buffer_pool_reserved_mem;
    int mutex_error = aws_mutex_init(&buffer_pool->mutex);
    AWS_FATAL_ASSERT(mutex_error == AWS_OP_SUCCESS);

    aws_array_list_init_dynamic(
        &buffer_pool->blocks, allocator, s_block_list_initial_capacity, sizeof(struct s3_buffer_pool_block));

    return buffer_pool;
}

void aws_s3_buffer_pool_destroy(struct aws_s3_buffer_pool *buffer_pool) {
    if (buffer_pool == NULL) {
        return;
    }

    for (size_t i = 0; i < aws_array_list_length(&buffer_pool->blocks); ++i) {
        struct s3_buffer_pool_block *block;
        aws_array_list_get_at_ptr(&buffer_pool->blocks, (void **)&block, i);

        AWS_FATAL_ASSERT(block->alloc_bit_mask == 0 && "Allocator still has outstanding blocks");
        aws_mem_release(buffer_pool->base_allocator, block->block_ptr);
    }

    aws_array_list_clean_up(&buffer_pool->blocks);

    aws_mutex_clean_up(&buffer_pool->mutex);
    struct aws_allocator *base = buffer_pool->base_allocator;
    aws_mem_release(base, buffer_pool);
}

void s_buffer_pool_trim_synced(struct aws_s3_buffer_pool *buffer_pool) {
    for (size_t i = 0; i < aws_array_list_length(&buffer_pool->blocks); ++i) {
        struct s3_buffer_pool_block *block;
        aws_array_list_get_at_ptr(&buffer_pool->blocks, (void **)&block, i);

        if (block->alloc_bit_mask == 0) {
            aws_mem_release(buffer_pool->base_allocator, block->block_ptr);
            aws_array_list_erase(&buffer_pool->blocks, i);
            --i;
        }
    }
}

void aws_s3_buffer_pool_trim(struct aws_s3_buffer_pool *buffer_pool) {
    aws_mutex_lock(&buffer_pool->mutex);
    s_buffer_pool_trim_synced(buffer_pool);
    aws_mutex_unlock(&buffer_pool->mutex);
}

struct aws_s3_buffer_pool_ticket *aws_s3_buffer_pool_reserve(struct aws_s3_buffer_pool *buffer_pool, size_t size) {
    AWS_PRECONDITION(buffer_pool);

    if (buffer_pool->has_reservation_hold) {
        return NULL;
    }

    if (size == 0) {
        AWS_LOGF_ERROR(AWS_LS_S3_CLIENT, "Could not reserve from buffer pool. 0 is not a valid size.");
        aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        return NULL;
    }

    struct aws_s3_buffer_pool_ticket *ticket = NULL;
    aws_mutex_lock(&buffer_pool->mutex);

    size_t overall_taken = buffer_pool->primary_used + buffer_pool->primary_reserved + buffer_pool->secondary_used +
                           buffer_pool->secondary_reserved;

    /*
     * If we are allocating from secondary and there is  unused space in
     * primary, trim the primary in hopes we can free up enough memory.
     * TODO: something smarter, like partial trim?
     */
    if (size > buffer_pool->primary_size_cutoff && (size + overall_taken) > buffer_pool->mem_limit &&
        (buffer_pool->primary_allocated >
         (buffer_pool->primary_used + buffer_pool->primary_reserved + buffer_pool->block_size))) {
        s_buffer_pool_trim_synced(buffer_pool);
        overall_taken = buffer_pool->primary_used + buffer_pool->primary_reserved + buffer_pool->secondary_used +
                        buffer_pool->secondary_reserved;
    }

    if ((size + overall_taken) <= buffer_pool->mem_limit) {
        ticket = aws_mem_calloc(buffer_pool->base_allocator, 1, sizeof(struct aws_s3_buffer_pool_ticket));
        ticket->size = size;
        if (size <= buffer_pool->primary_size_cutoff) {
            buffer_pool->primary_reserved += size;
        } else {
            buffer_pool->secondary_reserved += size;
        }
    } else {
        buffer_pool->has_reservation_hold = true;
    }

    aws_mutex_unlock(&buffer_pool->mutex);

    if (ticket == NULL) {
        AWS_LOGF_TRACE(
            AWS_LS_S3_CLIENT, "Failed to reserve buffer of size %zu. Consider increasing memory limit", size);
        aws_raise_error(AWS_ERROR_S3_EXCEEDS_MEMORY_LIMIT);
    }
    return ticket;
}

bool aws_s3_buffer_pool_has_reservation_hold(struct aws_s3_buffer_pool *buffer_pool) {
    AWS_PRECONDITION(buffer_pool);
    return buffer_pool->has_reservation_hold;
}

void aws_s3_buffer_pool_remove_reservation_hold(struct aws_s3_buffer_pool *buffer_pool) {
    AWS_PRECONDITION(buffer_pool);
    buffer_pool->has_reservation_hold = false;
}

static uint8_t *s_primary_acquire_synced(struct aws_s3_buffer_pool *buffer_pool, size_t size) {
    uint8_t *alloc_ptr = NULL;

    size_t chunks_needed = size / buffer_pool->chunk_size;
    if (size % buffer_pool->chunk_size != 0) {
        ++chunks_needed; /* round up */
    }

    for (size_t i = 0; i < aws_array_list_length(&buffer_pool->blocks); ++i) {
        struct s3_buffer_pool_block *block;
        aws_array_list_get_at_ptr(&buffer_pool->blocks, (void **)&block, i);

        for (size_t chunk_i = 0; chunk_i < s_chunks_per_block - chunks_needed + 1; ++chunk_i) {
            if (!s_check_bit_n(block->alloc_bit_mask, chunk_i, chunks_needed)) {
                alloc_ptr = block->block_ptr + chunk_i * buffer_pool->chunk_size;
                block->alloc_bit_mask = s_set_bit_n(block->alloc_bit_mask, chunk_i, chunks_needed);
                AWS_LOGF_DEBUG(0, "foo reuse %#010x", block->alloc_bit_mask);
                goto on_allocated;
            }
        }
    }

    struct s3_buffer_pool_block block;
    block.alloc_bit_mask = s_set_bit_n(block.alloc_bit_mask, 0, chunks_needed);
    AWS_LOGF_DEBUG(0, "foo new %#010x", block.alloc_bit_mask);
    block.block_ptr = aws_mem_acquire(buffer_pool->base_allocator, buffer_pool->block_size);
    block.block_size = buffer_pool->block_size;
    aws_array_list_push_back(&buffer_pool->blocks, &block);
    alloc_ptr = block.block_ptr;

    buffer_pool->primary_allocated += buffer_pool->block_size;

on_allocated:
    buffer_pool->primary_reserved -= size;
    buffer_pool->primary_used += size;

    return alloc_ptr;
}

struct aws_byte_buf aws_s3_buffer_pool_acquire_buffer(
    struct aws_s3_buffer_pool *buffer_pool,
    struct aws_s3_buffer_pool_ticket *ticket) {
    AWS_PRECONDITION(buffer_pool);
    AWS_PRECONDITION(ticket);

    if (ticket->ptr != NULL) {
        return aws_byte_buf_from_empty_array(ticket->ptr, ticket->size);
    }

    uint8_t *alloc_ptr = NULL;

    aws_mutex_lock(&buffer_pool->mutex);

    if (ticket->size <= buffer_pool->primary_size_cutoff) {
        alloc_ptr = s_primary_acquire_synced(buffer_pool, ticket->size);
    } else {
        alloc_ptr = aws_mem_acquire(buffer_pool->base_allocator, ticket->size);
        buffer_pool->secondary_reserved -= ticket->size;
        buffer_pool->secondary_used += ticket->size;
    }

    aws_mutex_unlock(&buffer_pool->mutex);
    ticket->ptr = alloc_ptr;

    return aws_byte_buf_from_empty_array(ticket->ptr, ticket->size);
}

void aws_s3_buffer_pool_release_ticket(
    struct aws_s3_buffer_pool *buffer_pool,
    struct aws_s3_buffer_pool_ticket *ticket) {

    if (buffer_pool == NULL || ticket == NULL) {
        return;
    }

    if (ticket->ptr == NULL) {
        /* Ticket was never used, make sure to clean up reserved count. */
        aws_mutex_lock(&buffer_pool->mutex);
        if (ticket->size <= buffer_pool->primary_size_cutoff) {
            buffer_pool->primary_reserved -= ticket->size;
        } else {
            buffer_pool->secondary_reserved -= ticket->size;
        }
        aws_mutex_unlock(&buffer_pool->mutex);
        aws_mem_release(buffer_pool->base_allocator, ticket);
        return;
    }

    aws_mutex_lock(&buffer_pool->mutex);
    if (ticket->size <= buffer_pool->primary_size_cutoff) {

        size_t chunks_used = ticket->size / buffer_pool->chunk_size;
        if (ticket->size % buffer_pool->chunk_size != 0) {
            ++chunks_used; /* round up */
        }

        bool found = false;
        for (size_t i = 0; i < aws_array_list_length(&buffer_pool->blocks); ++i) {
            struct s3_buffer_pool_block *block;
            aws_array_list_get_at_ptr(&buffer_pool->blocks, (void **)&block, i);

            if (block->block_ptr <= ticket->ptr && block->block_ptr + block->block_size > ticket->ptr) {
                size_t alloc_i = (ticket->ptr - block->block_ptr) / buffer_pool->chunk_size;

                block->alloc_bit_mask = s_clear_bit_n(block->alloc_bit_mask, alloc_i, chunks_used);
                AWS_LOGF_DEBUG(0, "foo clear %#010x", block->alloc_bit_mask);
                buffer_pool->primary_used -= ticket->size;

                found = true;
                break;
            }
        }

        AWS_FATAL_ASSERT(found);
    } else {
        aws_mem_release(buffer_pool->base_allocator, ticket->ptr);
        buffer_pool->secondary_used -= ticket->size;
    }

    aws_mem_release(buffer_pool->base_allocator, ticket);

    aws_mutex_unlock(&buffer_pool->mutex);
}

struct aws_s3_buffer_pool_usage_stats aws_s3_buffer_pool_get_usage(struct aws_s3_buffer_pool *buffer_pool) {
    aws_mutex_lock(&buffer_pool->mutex);

    struct aws_s3_buffer_pool_usage_stats ret = (struct aws_s3_buffer_pool_usage_stats){
        .max_size = buffer_pool->mem_limit,
        .primary_allocated = buffer_pool->primary_allocated,
        .primary_used = buffer_pool->primary_used,
        .primary_reserved = buffer_pool->primary_reserved,
        .primary_num_blocks = aws_array_list_length(&buffer_pool->blocks),
        .secondary_used = buffer_pool->secondary_used,
        .secondary_reserved = buffer_pool->secondary_reserved,
    };

    aws_mutex_unlock(&buffer_pool->mutex);
    return ret;
}
