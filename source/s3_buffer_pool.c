/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/s3_buffer_pool.h"
#include "aws/s3/private/s3_default_buffer_pool.h"

AWS_FUTURE_T_POINTER_WITH_RELEASE_IMPLEMENTATION(
    aws_future_s3_buffer_ticket,
    struct aws_s3_buffer_ticket,
    aws_s3_buffer_ticket_release)

struct aws_s3_buffer_pool *aws_s3_buffer_pool_acquire(struct aws_s3_buffer_pool *buffer_pool) {
    if (buffer_pool != NULL) {
        if (buffer_pool->vtable->acquire) {
            buffer_pool->vtable->acquire(buffer_pool);
        } else {
            aws_ref_count_acquire(&buffer_pool->ref_count);
        }
    }
    return buffer_pool;
}

struct aws_s3_buffer_pool *aws_s3_buffer_pool_release(struct aws_s3_buffer_pool *buffer_pool) {
    if (buffer_pool != NULL) {
        if (buffer_pool->vtable->release) {
            buffer_pool->vtable->release(buffer_pool);
        } else {
            aws_ref_count_release(&buffer_pool->ref_count);
        }
    }
    return NULL;
}

struct aws_s3_buffer_ticket *aws_s3_buffer_ticket_acquire(struct aws_s3_buffer_ticket *ticket) {
    if (ticket != NULL) {
        if (ticket->vtable->acquire) {
            ticket->vtable->acquire(ticket);
        } else {
            aws_ref_count_acquire(&ticket->ref_count);
        }
    }
    return ticket;
}

struct aws_s3_buffer_ticket *aws_s3_buffer_ticket_release(struct aws_s3_buffer_ticket *ticket) {
    if (ticket != NULL) {
        if (ticket->vtable->release) {
            ticket->vtable->release(ticket);
        } else {
            aws_ref_count_release(&ticket->ref_count);
        }
    }
    return NULL;
}

struct aws_future_s3_buffer_ticket *s_default_pool_reserve(
    struct aws_s3_buffer_pool *pool,
    struct aws_s3_buffer_pool_reserve_meta meta) {
    struct aws_s3_default_buffer_pool *default_pool = (struct aws_s3_default_buffer_pool *)pool->impl;

    return aws_s3_default_buffer_pool_reserve(default_pool, meta);
}

void s_default_pool_trim(struct aws_s3_buffer_pool *pool) {
    struct aws_s3_default_buffer_pool *default_pool = (struct aws_s3_default_buffer_pool *)pool->impl;

    aws_s3_default_buffer_pool_trim(default_pool);
}

static struct aws_s3_buffer_pool_vtable s_default_tpool_vtable = {
    .reserve = s_default_pool_reserve,
    .trim = s_default_pool_trim 
};

/*TODO: error handling*/
struct aws_s3_buffer_pool *aws_s3_default_buffer_pool_factory(struct aws_allocator *allocator,
    struct aws_s3_buffer_pool_config config) {

    struct aws_s3_default_buffer_pool *impl = aws_s3_default_buffer_pool_new(allocator, config.part_size, config.memory_limit);

    if (impl == NULL) {
        return NULL;
    }

    struct aws_s3_default_buffer_pool_usage_stats pool_usage = aws_s3_default_buffer_pool_get_usage(impl);

    if (config.max_part_size > pool_usage.mem_limit) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_CLIENT,
            "Cannot create client from client_config; configured max part size should not exceed memory limit."
            "size.");
        aws_raise_error(AWS_ERROR_S3_INVALID_MEMORY_LIMIT_CONFIG);
        return NULL;
    }

    struct aws_s3_buffer_pool *pool = aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_buffer_pool));
    pool->impl = impl;
    pool->vtable = &s_default_tpool_vtable;
    aws_ref_count_init(&pool->ref_count, pool, (aws_simple_completion_callback *)aws_s3_default_buffer_pool_destroy);

    return pool;
}
