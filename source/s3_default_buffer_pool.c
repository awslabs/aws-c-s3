/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/s3/private/s3_default_buffer_pool.h>

#include <aws/common/array_list.h>
#include <aws/common/mutex.h>
#include <aws/common/ref_count.h>
#include <aws/common/system_info.h>
#include <aws/io/future.h>
#include <aws/s3/private/s3_util.h>

/*
 * S3 Buffer Pool.
 * Fairly trivial implementation of "arena" style allocator.
 * Note: current implementation is not optimized and instead tries to be
 * as straightforward as possible. Given that pool manages a small number
 * of big allocations, performance impact is not that bad, but something we need
 * to look into on the next iteration.
 *
 * Basic approach is to divide acquires into primary and secondary.
 * User provides chunk size during construction. Acquires below 4 * chunks_size
 * are done from primary and the rest are from secondary.
 *
 * Primary storage consists of blocks that are each s_chunks_per_block *
 * chunk_size in size. blocks are created on demand as needed.
 * Acquire operation from primary basically works by determining how many chunks
 * are needed and then finding available space in existing blocks or creating a
 * new block. Acquire will always take over the whole chunk, so some space is
 * likely wasted.
 * Ex. say chunk_size is 8mb and s_chunks_per_block is 16, which makes block size 128mb.
 * acquires up to 32mb will be done from primary. So 1 block can hold 4 buffers
 * of 32mb (4 chunks) or 16 buffers of 8mb (1 chunk). If requested buffer size
 * is 12mb, 2 chunks are used for acquire and 4mb will be wasted.
 * Secondary storage delegates directly to system allocator.
 *
 * One complication is "forced" buffers. A forced buffer is one that
 * comes from primary or secondary storage as usual, but it is allowed to exceed
 * the memory limit. This is only used when we want to use memory from
 * the pool, but waiting for a normal ticket reservation could cause deadlock.
 */

struct aws_s3_default_buffer_pool;

struct aws_s3_default_buffer_ticket {
    size_t size;
    uint8_t *ptr;
    size_t chunks_used;
    bool forced;
    struct aws_s3_buffer_pool *pool;
};

/* Default size for blocks array. Note: this is just for meta info, blocks
 * themselves are not preallocated. */
static size_t s_block_list_initial_capacity = 5;

/* Amount of mem reserved for use outside of buffer pool.
 * This is an optimistic upper bound on mem used as we dont track it.
 * Covers both usage outside of pool, i.e. all allocations done as part of s3
 * client as well as any allocations overruns due to memory waste in the pool. */
static const size_t s_buffer_pool_reserved_mem = MB_TO_BYTES(128);

/*
 * How many chunks make up a block in primary storage.
 */
static const size_t s_chunks_per_block = 16;

/*
 * Max size of chunks in primary.
 * Effectively if client part size is above the following number, primary
 * storage along with buffer reuse is disabled and all buffers are allocated
 * directly using allocator.
 */
static const size_t s_max_chunk_size_for_buffer_reuse = MB_TO_BYTES(64);

/* Forced buffers only count against the memory limit up to a certain percent.
 * For example: if mem_limit is 10GiB, and forced_use is 11GiB, and THIS number is 90(%),
 * we still consider 1GiB available for normal buffer usage. */
static const size_t s_max_impact_of_forced_buffers_on_memory_limit_as_percentage = 80;

struct aws_s3_default_buffer_pool {
    struct aws_allocator *base_allocator;
    struct aws_mutex mutex;

    size_t block_size;
    size_t chunk_size;
    /* size at which allocations should go to secondary */
    size_t primary_size_cutoff;

    /* NOTE: See aws_s3_buffer_pool_usage_stats for descriptions of most fields */

    size_t mem_limit;

    size_t primary_allocated;
    size_t primary_reserved;
    size_t primary_used;

    size_t secondary_reserved;
    size_t secondary_used;

    size_t forced_used;

    struct aws_array_list blocks;

    struct aws_linked_list pending_reserves;
};

struct s3_pending_reserve {
    struct aws_linked_list_node node;
    struct aws_future_s3_buffer_ticket *ticket_future;
    struct aws_s3_default_buffer_ticket *ticket;
    struct aws_s3_buffer_pool_reserve_meta meta;
};

struct s3_buffer_pool_block {
    size_t block_size;
    uint8_t *block_ptr;
    uint16_t alloc_bit_mask;
};

/*
 * Sets n bits at position starting with LSB.
 * Note: n must be at most 8, but in practice will always be at most 4.
 * position + n should at most be 16
 */
static inline uint16_t s_set_bits(uint16_t num, size_t position, size_t n) {
    AWS_PRECONDITION(n <= 8);
    AWS_PRECONDITION(position + n <= 16);
    uint16_t mask = ((uint16_t)0x00FF) >> (8 - n);
    return num | (mask << position);
}

/*
 * Clears n bits at position starting with LSB.
 * Note: n must be at most 8, but in practice will always be at most 4.
 * position + n should at most be 16
 */
static inline uint16_t s_clear_bits(uint16_t num, size_t position, size_t n) {
    AWS_PRECONDITION(n <= 8);
    AWS_PRECONDITION(position + n <= 16);
    uint16_t mask = ((uint16_t)0x00FF) >> (8 - n);
    return num & ~(mask << position);
}

/*
 * Checks whether n bits are set at position starting with LSB.
 * Note: n must be at most 8, but in practice will always be at most 4.
 * position + n should at most be 16
 */
static inline bool s_check_bits(uint16_t num, size_t position, size_t n) {
    AWS_PRECONDITION(n <= 8);
    AWS_PRECONDITION(position + n <= 16);
    uint16_t mask = ((uint16_t)0x00FF) >> (8 - n);
    return (num >> position) & mask;
}

static void s_aws_ticket_wrapper_destroy(void *data);

struct aws_byte_buf s_default_ticket_claim(struct aws_s3_buffer_ticket *ticket_wrapper) {
    struct aws_s3_default_buffer_ticket *ticket = ticket_wrapper->impl;
    return aws_s3_default_buffer_pool_acquire_buffer(ticket->pool, ticket);
}

static struct aws_s3_buffer_ticket_vtable s_default_ticket_vtable = {.claim = s_default_ticket_claim};

struct aws_s3_buffer_ticket *s_wrap_default_ticket(struct aws_s3_default_buffer_ticket *ticket) {
    struct aws_s3_default_buffer_pool *pool = ticket->pool->impl;
    struct aws_s3_buffer_ticket *ticket_wrapper =
        aws_mem_calloc(pool->base_allocator, 1, sizeof(struct aws_s3_buffer_ticket));

    ticket_wrapper->impl = ticket;
    ticket_wrapper->vtable = &s_default_ticket_vtable;
    aws_ref_count_init(
        &ticket_wrapper->ref_count, ticket_wrapper, (aws_simple_completion_callback *)s_aws_ticket_wrapper_destroy);

    return ticket_wrapper;
}

struct aws_future_s3_buffer_ticket *s_default_pool_reserve(
    struct aws_s3_buffer_pool *pool,
    struct aws_s3_buffer_pool_reserve_meta meta) {

    return aws_s3_default_buffer_pool_reserve(pool, meta);
}

void s_default_pool_trim(struct aws_s3_buffer_pool *pool) {
    aws_s3_default_buffer_pool_trim(pool);
}

static struct aws_s3_buffer_pool_vtable s_default_pool_vtable = {
    .reserve = s_default_pool_reserve,
    .trim = s_default_pool_trim,
};

struct aws_s3_buffer_pool *aws_s3_default_buffer_pool_new(
    struct aws_allocator *allocator,
    struct aws_s3_buffer_pool_config config) {
    (void)allocator;

    size_t chunk_size = config.part_size;

    if (config.memory_limit < GB_TO_BYTES(1)) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_CLIENT,
            "Failed to initialize buffer pool. "
            "Minimum supported value for Memory Limit is 1GB.");
        aws_raise_error(AWS_ERROR_S3_INVALID_MEMORY_LIMIT_CONFIG);
        return NULL;
    }

    if (chunk_size < (1024) || chunk_size % (4 * 1024) != 0) {
        AWS_LOGF_WARN(
            AWS_LS_S3_CLIENT,
            "Part size specified on the client can lead to suboptimal performance. "
            "Consider specifying size in multiples of 4KiB. Ideal part size for most transfers is "
            "1MiB multiple between 8MiB and 16MiB. Note: the client will automatically scale part size "
            "if its not sufficient to transfer data within the maximum number of parts");
    }

    size_t adjusted_mem_lim = config.memory_limit - s_buffer_pool_reserved_mem;

    if (config.max_part_size > adjusted_mem_lim) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_CLIENT,
            "Cannot create client from client_config; configured max part size should not exceed memory limit."
            "size.");
        aws_raise_error(AWS_ERROR_S3_INVALID_MEMORY_LIMIT_CONFIG);
        return NULL;
    }

    /*
     * TODO: There is several things we can consider tweaking here:
     * - if chunk size is a weird number of bytes, force it to the closest page size?
     * - grow chunk size max based on overall mem lim (ex. for 4gb it might be
     *   64mb, but for 8gb it can be 128mb)
     * - align chunk size to better fill available mem? some chunk sizes can
     *   result in memory being wasted because overall limit does not divide
     *   nicely into chunks
     */
    if (chunk_size > s_max_chunk_size_for_buffer_reuse || chunk_size * s_chunks_per_block > adjusted_mem_lim) {
        AWS_LOGF_WARN(
            AWS_LS_S3_CLIENT,
            "Part size specified on the client is too large for automatic buffer reuse. "
            "Consider specifying a smaller part size to improve performance and memory utilization");
        chunk_size = 0;
    }

    size_t page_size = aws_system_info_page_size();
    /* TODO: if people override their allocator, this will not wrap their allocator at all. eg: mem-tracing allocator
     * will be ignored. */
    struct aws_allocator *aligned_allocator = aws_explicit_aligned_allocator_new(page_size);
    struct aws_s3_default_buffer_pool *buffer_pool =
        aws_mem_calloc(aligned_allocator, 1, sizeof(struct aws_s3_default_buffer_pool));

    AWS_FATAL_ASSERT(buffer_pool != NULL);

    buffer_pool->base_allocator = aligned_allocator;
    buffer_pool->chunk_size = chunk_size;
    buffer_pool->block_size = s_chunks_per_block * chunk_size;
    /* Somewhat arbitrary number.
     * Tries to balance between how many allocations use buffer and buffer space
     * being wasted. */
    buffer_pool->primary_size_cutoff = chunk_size * 4;
    buffer_pool->mem_limit = adjusted_mem_lim;
    int mutex_error = aws_mutex_init(&buffer_pool->mutex);
    AWS_FATAL_ASSERT(mutex_error == AWS_OP_SUCCESS);

    aws_array_list_init_dynamic(
        &buffer_pool->blocks,
        buffer_pool->base_allocator,
        s_block_list_initial_capacity,
        sizeof(struct s3_buffer_pool_block));

    aws_linked_list_init(&buffer_pool->pending_reserves);

    struct aws_s3_buffer_pool *pool = aws_mem_calloc(buffer_pool->base_allocator, 1, sizeof(struct aws_s3_buffer_pool));
    pool->impl = buffer_pool;
    pool->vtable = &s_default_pool_vtable;
    aws_ref_count_init(&pool->ref_count, pool, (aws_simple_completion_callback *)aws_s3_default_buffer_pool_destroy);

    return pool;
}

void aws_s3_default_buffer_pool_destroy(struct aws_s3_buffer_pool *buffer_pool_wrapper) {
    if (buffer_pool_wrapper == NULL) {
        return;
    }
    AWS_FATAL_ASSERT(buffer_pool_wrapper->impl);

    struct aws_s3_default_buffer_pool *buffer_pool = buffer_pool_wrapper->impl;
    aws_mem_release(buffer_pool->base_allocator, buffer_pool_wrapper);

    for (size_t i = 0; i < aws_array_list_length(&buffer_pool->blocks); ++i) {
        struct s3_buffer_pool_block *block;
        aws_array_list_get_at_ptr(&buffer_pool->blocks, (void **)&block, i);

        AWS_FATAL_ASSERT(block->alloc_bit_mask == 0 && "Allocator still has outstanding blocks");
        aws_mem_release(buffer_pool->base_allocator, block->block_ptr);
    }

    aws_array_list_clean_up(&buffer_pool->blocks);

    while (!aws_linked_list_empty(&buffer_pool->pending_reserves)) {
        struct aws_linked_list_node *node = aws_linked_list_front(&buffer_pool->pending_reserves);
        struct s3_pending_reserve *pending = AWS_CONTAINER_OF(node, struct s3_pending_reserve, node);
        AWS_FATAL_ASSERT(aws_future_s3_buffer_ticket_is_done(pending->ticket_future));
        aws_future_s3_buffer_ticket_release(pending->ticket_future);
        aws_linked_list_pop_front(&buffer_pool->pending_reserves);
        aws_mem_release(buffer_pool->base_allocator, pending);
    }

    aws_mutex_clean_up(&buffer_pool->mutex);
    struct aws_allocator *base = buffer_pool->base_allocator;
    aws_mem_release(base, buffer_pool);
    aws_explicit_aligned_allocator_destroy(base);
}

void s_buffer_pool_trim_synced(struct aws_s3_default_buffer_pool *buffer_pool) {
    for (size_t i = 0; i < aws_array_list_length(&buffer_pool->blocks);) {
        struct s3_buffer_pool_block *block;
        aws_array_list_get_at_ptr(&buffer_pool->blocks, (void **)&block, i);

        if (block->alloc_bit_mask == 0) {
            buffer_pool->primary_allocated -= block->block_size;
            aws_mem_release(buffer_pool->base_allocator, block->block_ptr);
            aws_array_list_erase(&buffer_pool->blocks, i);
            /* do not increment since we just released element */
        } else {
            ++i;
        }
    }
}

void aws_s3_default_buffer_pool_trim(struct aws_s3_buffer_pool *buffer_pool) {
    struct aws_s3_default_buffer_pool *pool = buffer_pool->impl;
    aws_mutex_lock(&pool->mutex);
    s_buffer_pool_trim_synced(pool);
    aws_mutex_unlock(&pool->mutex);
}

struct aws_s3_default_buffer_ticket *s_try_reserve(
    struct aws_s3_buffer_pool *buffer_pool,
    struct aws_s3_buffer_pool_reserve_meta meta);

static void s_aws_ticket_wrapper_destroy(void *data) {
    struct aws_s3_buffer_ticket *ticket_wrapper = data;
    struct aws_s3_default_buffer_ticket *ticket = ticket_wrapper->impl;
    struct aws_s3_buffer_pool *pool = ticket->pool;
    struct aws_s3_default_buffer_pool *buffer_pool = pool->impl;

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
        aws_mem_release(buffer_pool->base_allocator, ticket_wrapper);
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

                block->alloc_bit_mask = s_clear_bits(block->alloc_bit_mask, alloc_i, chunks_used);
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

    if (ticket->forced) {
        buffer_pool->forced_used -= ticket->size;
    }

    aws_mem_release(buffer_pool->base_allocator, ticket);
    aws_mem_release(buffer_pool->base_allocator, ticket_wrapper);

    struct aws_linked_list pending_reserves_to_remove;
    aws_linked_list_init(&pending_reserves_to_remove);

    struct aws_linked_list pending_reserves_to_complete;
    aws_linked_list_init(&pending_reserves_to_complete);

    /* Capture all the pending reserves that are done (currently can only happen when request is canceled, which cancels
     * pending futures) */
    struct aws_linked_list_node *node = aws_linked_list_begin(&buffer_pool->pending_reserves);
    while (node != aws_linked_list_end(&buffer_pool->pending_reserves)) {
        struct s3_pending_reserve *pending_reserve = AWS_CONTAINER_OF(node, struct s3_pending_reserve, node);
        struct aws_linked_list_node *current_node = node;
        node = aws_linked_list_next(node);
        if (aws_future_s3_buffer_ticket_is_done(pending_reserve->ticket_future)) {
            AWS_FATAL_ASSERT(aws_future_s3_buffer_ticket_get_error(pending_reserve->ticket_future) != AWS_OP_SUCCESS);
            aws_linked_list_remove(current_node);
            aws_linked_list_push_back(&pending_reserves_to_remove, current_node);
        }
    }

    /* Capture all the pending reserves that can be completed. They will actually be completed once outside the mutex.
     */
    while (!aws_linked_list_empty(&buffer_pool->pending_reserves)) {
        node = aws_linked_list_front(&buffer_pool->pending_reserves);
        struct s3_pending_reserve *pending_reserve = AWS_CONTAINER_OF(node, struct s3_pending_reserve, node);

        pending_reserve->ticket = s_try_reserve(pool, pending_reserve->meta);

        if (pending_reserve->ticket != NULL) {
            aws_linked_list_pop_front(&buffer_pool->pending_reserves);
            aws_linked_list_push_back(&pending_reserves_to_complete, node);
        } else {
            break;
        }
    }

    aws_mutex_unlock(&buffer_pool->mutex);

    /* release completed pending nodes outside of lock to avoid any deadlocks */
    while (!aws_linked_list_empty(&pending_reserves_to_remove)) {
        node = aws_linked_list_front(&pending_reserves_to_remove);
        struct s3_pending_reserve *pending = AWS_CONTAINER_OF(node, struct s3_pending_reserve, node);
        aws_future_s3_buffer_ticket_release(pending->ticket_future);
        aws_linked_list_pop_front(&pending_reserves_to_remove);
        aws_mem_release(buffer_pool->base_allocator, pending);
    }

    /* fill the next pending future */
    while (!aws_linked_list_empty(&pending_reserves_to_complete)) {
        node = aws_linked_list_front(&pending_reserves_to_complete);
        struct s3_pending_reserve *pending = AWS_CONTAINER_OF(node, struct s3_pending_reserve, node);

        struct aws_s3_buffer_ticket *new_ticket_wrapper = s_wrap_default_ticket(pending->ticket);
        aws_future_s3_buffer_ticket_set_result_by_move(pending->ticket_future, &new_ticket_wrapper);

        aws_future_s3_buffer_ticket_release(pending->ticket_future);
        aws_linked_list_pop_front(&pending_reserves_to_complete);
        aws_mem_release(buffer_pool->base_allocator, pending);
    }
}

struct aws_s3_default_buffer_ticket *s_try_reserve(
    struct aws_s3_buffer_pool *buffer_pool_wrapper,
    struct aws_s3_buffer_pool_reserve_meta meta) {
    struct aws_s3_default_buffer_ticket *ticket = NULL;
    struct aws_s3_default_buffer_pool *buffer_pool = buffer_pool_wrapper->impl;

    size_t overall_taken = buffer_pool->primary_used + buffer_pool->primary_reserved + buffer_pool->secondary_used +
                           buffer_pool->secondary_reserved;

    /*
     * If we are allocating from secondary and there is unused space in
     * primary, trim the primary in hopes we can free up enough memory.
     * TODO: something smarter, like partial trim?
     */
    if (meta.size > buffer_pool->primary_size_cutoff && (meta.size + overall_taken) > buffer_pool->mem_limit &&
        (buffer_pool->primary_allocated >
         (buffer_pool->primary_used + buffer_pool->primary_reserved + buffer_pool->block_size))) {
        s_buffer_pool_trim_synced(buffer_pool);
        overall_taken = buffer_pool->primary_used + buffer_pool->primary_reserved + buffer_pool->secondary_used +
                        buffer_pool->secondary_reserved;
    }

    /* Don't let forced buffers account for 100% of the memory limit */
    const size_t max_impact_of_forced_on_limit =
        (size_t)(buffer_pool->mem_limit * (s_max_impact_of_forced_buffers_on_memory_limit_as_percentage / 100.0));
    if (buffer_pool->forced_used > max_impact_of_forced_on_limit) {
        overall_taken -= buffer_pool->forced_used - max_impact_of_forced_on_limit;
    }

    if ((meta.size + overall_taken) <= buffer_pool->mem_limit) {
        ticket = aws_mem_calloc(buffer_pool->base_allocator, 1, sizeof(struct aws_s3_default_buffer_ticket));
        ticket->size = meta.size;
        ticket->pool = buffer_pool_wrapper;

        if (meta.size <= buffer_pool->primary_size_cutoff) {
            buffer_pool->primary_reserved += meta.size;
        } else {
            buffer_pool->secondary_reserved += meta.size;
        }
    }

    return ticket;
}

struct aws_future_s3_buffer_ticket *aws_s3_default_buffer_pool_reserve(
    struct aws_s3_buffer_pool *buffer_pool_wrapper,
    struct aws_s3_buffer_pool_reserve_meta meta) {
    AWS_PRECONDITION(buffer_pool_wrapper);

    struct aws_s3_default_buffer_pool *buffer_pool = buffer_pool_wrapper->impl;

    AWS_FATAL_ASSERT(meta.size != 0);
    AWS_FATAL_ASSERT(meta.size <= buffer_pool->mem_limit);

    aws_mutex_lock(&buffer_pool->mutex);

    struct aws_s3_default_buffer_ticket *ticket = NULL;
    if (meta.can_block) {
        ticket = aws_mem_calloc(buffer_pool->base_allocator, 1, sizeof(struct aws_s3_default_buffer_ticket));
        ticket->size = meta.size;
        ticket->forced = true;
        ticket->pool = buffer_pool_wrapper;

    } else {
        ticket = s_try_reserve(buffer_pool_wrapper, meta);
    }

    struct aws_future_s3_buffer_ticket *future = aws_future_s3_buffer_ticket_new(buffer_pool->base_allocator);
    if (ticket != NULL) {
        struct aws_s3_buffer_ticket *ticket_wrapper = s_wrap_default_ticket(ticket);
        aws_future_s3_buffer_ticket_set_result_by_move(future, &ticket_wrapper);
    } else {
        struct s3_pending_reserve *pending_reserve =
            aws_mem_calloc(buffer_pool->base_allocator, 1, sizeof(struct s3_pending_reserve));

        pending_reserve->meta = meta;
        pending_reserve->ticket_future = future;
        aws_future_s3_buffer_ticket_acquire(pending_reserve->ticket_future);

        aws_linked_list_push_back(&buffer_pool->pending_reserves, &pending_reserve->node);
    }

    aws_mutex_unlock(&buffer_pool->mutex);

    return future;
}

static uint8_t *s_primary_acquire_synced(
    struct aws_s3_default_buffer_pool *buffer_pool,
    struct aws_s3_default_buffer_ticket *ticket) {

    uint8_t *alloc_ptr = NULL;

    size_t chunks_needed = ticket->size / buffer_pool->chunk_size;
    if (ticket->size % buffer_pool->chunk_size != 0) {
        ++chunks_needed; /* round up */
    }
    ticket->chunks_used = chunks_needed;

    /* Look for space in existing blocks */
    for (size_t i = 0; i < aws_array_list_length(&buffer_pool->blocks); ++i) {
        struct s3_buffer_pool_block *block;
        aws_array_list_get_at_ptr(&buffer_pool->blocks, (void **)&block, i);

        for (size_t chunk_i = 0; chunk_i < s_chunks_per_block - chunks_needed + 1; ++chunk_i) {
            if (!s_check_bits(block->alloc_bit_mask, chunk_i, chunks_needed)) {
                alloc_ptr = block->block_ptr + chunk_i * buffer_pool->chunk_size;
                block->alloc_bit_mask = s_set_bits(block->alloc_bit_mask, chunk_i, chunks_needed);
                goto on_allocated;
            }
        }
    }

    /* No space available. Allocate new block. */
    struct s3_buffer_pool_block block;
    block.alloc_bit_mask = s_set_bits(0, 0, chunks_needed);
    block.block_ptr = aws_mem_acquire(buffer_pool->base_allocator, buffer_pool->block_size);
    block.block_size = buffer_pool->block_size;
    aws_array_list_push_back(&buffer_pool->blocks, &block);
    alloc_ptr = block.block_ptr;

    buffer_pool->primary_allocated += buffer_pool->block_size;

on_allocated:
    buffer_pool->primary_used += ticket->size;

    /* forced buffers acquire immediately, without reserving first */
    if (ticket->forced == false) {
        buffer_pool->primary_reserved -= ticket->size;
    }

    return alloc_ptr;
}

static struct aws_byte_buf s_acquire_buffer_synced(
    struct aws_s3_default_buffer_pool *buffer_pool_wrapper,
    struct aws_s3_default_buffer_ticket *ticket);

struct aws_byte_buf aws_s3_default_buffer_pool_acquire_buffer(
    struct aws_s3_buffer_pool *buffer_pool_wrapper,
    struct aws_s3_default_buffer_ticket *ticket) {

    AWS_PRECONDITION(buffer_pool_wrapper);
    AWS_PRECONDITION(ticket);

    struct aws_s3_default_buffer_pool *buffer_pool = buffer_pool_wrapper->impl;

    if (ticket->ptr != NULL) {
        return aws_byte_buf_from_empty_array(ticket->ptr, ticket->size);
    }

    aws_mutex_lock(&buffer_pool->mutex);
    struct aws_byte_buf buf = s_acquire_buffer_synced(buffer_pool, ticket);
    aws_mutex_unlock(&buffer_pool->mutex);

    return buf;
}

static struct aws_byte_buf s_acquire_buffer_synced(
    struct aws_s3_default_buffer_pool *buffer_pool,
    struct aws_s3_default_buffer_ticket *ticket) {

    AWS_PRECONDITION(ticket->ptr == NULL);

    if (ticket->size <= buffer_pool->primary_size_cutoff) {
        ticket->ptr = s_primary_acquire_synced(buffer_pool, ticket);
    } else {
        ticket->ptr = aws_mem_acquire(buffer_pool->base_allocator, ticket->size);
        buffer_pool->secondary_used += ticket->size;

        /* forced buffers acquire immediately, without reserving first */
        if (ticket->forced == false) {
            buffer_pool->secondary_reserved -= ticket->size;
        }
    }

    if (ticket->forced) {
        buffer_pool->forced_used += ticket->size;
    }

    return aws_byte_buf_from_empty_array(ticket->ptr, ticket->size);
}

struct aws_s3_default_buffer_pool_usage_stats aws_s3_default_buffer_pool_get_usage(
    struct aws_s3_buffer_pool *buffer_pool_wrapper) {

    struct aws_s3_default_buffer_pool *buffer_pool = buffer_pool_wrapper->impl;
    aws_mutex_lock(&buffer_pool->mutex);

    struct aws_s3_default_buffer_pool_usage_stats ret = (struct aws_s3_default_buffer_pool_usage_stats){
        .mem_limit = buffer_pool->mem_limit,
        .primary_cutoff = buffer_pool->primary_size_cutoff,
        .primary_allocated = buffer_pool->primary_allocated,
        .primary_used = buffer_pool->primary_used,
        .primary_reserved = buffer_pool->primary_reserved,
        .primary_num_blocks = aws_array_list_length(&buffer_pool->blocks),
        .secondary_used = buffer_pool->secondary_used,
        .secondary_reserved = buffer_pool->secondary_reserved,
        .forced_used = buffer_pool->forced_used,
    };

    aws_mutex_unlock(&buffer_pool->mutex);
    return ret;
}
