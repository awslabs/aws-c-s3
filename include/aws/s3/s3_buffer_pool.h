#ifndef AWS_S3_BUFFER_POOL_H
#define AWS_S3_BUFFER_POOL_H

#include <aws/io/future.h>
#include <aws/s3/s3.h>

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */
AWS_PUSH_SANE_WARNING_LEVEL
AWS_EXTERN_C_BEGIN
struct aws_s3_buffer_ticket;

/**
 * aws_future<aws_s3_buffer_ticket*>
 */
AWS_FUTURE_T_POINTER_WITH_RELEASE_DECLARATION(aws_future_s3_buffer_ticket, struct aws_s3_buffer_ticket, AWS_S3_API)

struct aws_s3_buffer_pool_reserve_meta {
    struct aws_s3_client *client;
    struct aws_s3_meta_request *meta_request;
    size_t size;
    bool can_block;
};

struct aws_s3_buffer_pool {
    void (*destroy)(struct aws_s3_buffer_pool *pool);

    struct aws_future_s3_buffer_ticket *(*reserve)(struct aws_s3_buffer_pool *pool, 
        struct aws_s3_buffer_pool_reserve_meta meta);
    struct aws_byte_buf (*claim)(struct aws_s3_buffer_pool *pool, struct aws_s3_buffer_ticket *ticket);
    void (*release)(struct aws_s3_buffer_pool *pool, struct aws_s3_buffer_ticket *ticket);

    void (*trim)(struct aws_s3_buffer_pool *pool);

    struct aws_allocator *allocator;
    void *user_data;
};

typedef struct aws_s3_buffer_pool *(aws_s3_buffer_pool_factory_fn)(struct aws_allocator *allocator,
                                                                   uint64_t part_size,
                                                                   uint64_t mem_limit);

AWS_EXTERN_C_END
AWS_POP_SANE_WARNING_LEVEL
 
 #endif /* AWS_S3_BUFFER_POOL_H */
