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

struct aws_future_s3_buffer_ticket *aws_s3_buffer_pool_reserve(
    struct aws_s3_buffer_pool *buffer_pool,
    struct aws_s3_buffer_pool_reserve_meta meta) {
    AWS_PRECONDITION(buffer_pool);

    return buffer_pool->vtable->reserve(buffer_pool, meta);
}

void aws_s3_buffer_pool_trim(struct aws_s3_buffer_pool *buffer_pool) {
    AWS_PRECONDITION(buffer_pool);

    buffer_pool->vtable->trim(buffer_pool);
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

struct aws_byte_buf aws_s3_buffer_ticket_claim(struct aws_s3_buffer_ticket *ticket) {
    AWS_PRECONDITION(ticket);

    return ticket->vtable->claim(ticket);
}
