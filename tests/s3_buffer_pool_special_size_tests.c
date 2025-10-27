/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/s3/private/s3_default_buffer_pool.h>
#include <aws/s3/private/s3_util.h>

#include <aws/testing/aws_test_harness.h>

/* Test basic functionality of adding a special size */
static int s_test_s3_buffer_pool_add_special_size_basic(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_buffer_pool *buffer_pool = aws_s3_default_buffer_pool_new(
        allocator, (struct aws_s3_buffer_pool_config){.part_size = MB_TO_BYTES(8), .memory_limit = GB_TO_BYTES(2)});
    ASSERT_NOT_NULL(buffer_pool);

    /* Add a special size larger than primary cutoff */
    uint64_t special_size = MB_TO_BYTES(64);
    int result = aws_s3_buffer_pool_add_special_size(buffer_pool, special_size);
    ASSERT_INT_EQUALS(AWS_OP_SUCCESS, result);

    /* Reserve and acquire a buffer of the special size */
    struct aws_future_s3_buffer_ticket *future =
        aws_s3_default_buffer_pool_reserve(buffer_pool, (struct aws_s3_buffer_pool_reserve_meta){.size = special_size});
    ASSERT_NOT_NULL(future);
    ASSERT_TRUE(aws_future_s3_buffer_ticket_is_done(future));
    ASSERT_INT_EQUALS(aws_future_s3_buffer_ticket_get_error(future), AWS_OP_SUCCESS);

    struct aws_s3_buffer_ticket *ticket = aws_future_s3_buffer_ticket_get_result_by_move(future);
    ASSERT_NOT_NULL(ticket);

    struct aws_byte_buf buf = aws_s3_buffer_ticket_claim(ticket);
    ASSERT_NOT_NULL(buf.buffer);
    ASSERT_UINT_EQUALS(special_size, buf.capacity);

    /* Verify special blocks statistics */
    struct aws_s3_default_buffer_pool_usage_stats stats = aws_s3_default_buffer_pool_get_usage(buffer_pool);
    ASSERT_UINT_EQUALS(special_size, stats.special_blocks_allocated);
    ASSERT_UINT_EQUALS(1, stats.special_blocks_num);
    ASSERT_UINT_EQUALS(0, stats.special_blocks_reserved);
    ASSERT_UINT_EQUALS(special_size, stats.special_blocks_used);

    /* Clean up */
    aws_s3_buffer_ticket_release(ticket);
    aws_future_s3_buffer_ticket_release(future);
    aws_s3_default_buffer_pool_destroy(buffer_pool);

    return 0;
}
AWS_TEST_CASE(test_s3_buffer_pool_add_special_size_basic, s_test_s3_buffer_pool_add_special_size_basic)

/* Test adding the same special size twice (should be idempotent) */
static int s_test_s3_buffer_pool_add_special_size_duplicate(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_buffer_pool *buffer_pool = aws_s3_default_buffer_pool_new(
        allocator, (struct aws_s3_buffer_pool_config){.part_size = MB_TO_BYTES(8), .memory_limit = GB_TO_BYTES(2)});
    ASSERT_NOT_NULL(buffer_pool);

    uint64_t special_size = MB_TO_BYTES(64);

    /* Add the same size twice */
    int result1 = aws_s3_buffer_pool_add_special_size(buffer_pool, special_size);
    ASSERT_INT_EQUALS(AWS_OP_SUCCESS, result1);

    int result2 = aws_s3_buffer_pool_add_special_size(buffer_pool, special_size);
    ASSERT_INT_EQUALS(AWS_OP_SUCCESS, result2);

    /* Verify we can still allocate from it */
    struct aws_future_s3_buffer_ticket *future =
        aws_s3_default_buffer_pool_reserve(buffer_pool, (struct aws_s3_buffer_pool_reserve_meta){.size = special_size});
    ASSERT_NOT_NULL(future);
    ASSERT_TRUE(aws_future_s3_buffer_ticket_is_done(future));
    ASSERT_INT_EQUALS(aws_future_s3_buffer_ticket_get_error(future), AWS_OP_SUCCESS);

    struct aws_s3_buffer_ticket *ticket = aws_future_s3_buffer_ticket_get_result_by_move(future);
    ASSERT_NOT_NULL(ticket);

    struct aws_byte_buf buf = aws_s3_buffer_ticket_claim(ticket);
    ASSERT_NOT_NULL(buf.buffer);

    /* Verify special blocks statistics */
    struct aws_s3_default_buffer_pool_usage_stats stats = aws_s3_default_buffer_pool_get_usage(buffer_pool);
    ASSERT_UINT_EQUALS(special_size, stats.special_blocks_allocated);
    ASSERT_UINT_EQUALS(1, stats.special_blocks_num);
    ASSERT_UINT_EQUALS(0, stats.special_blocks_reserved);
    ASSERT_UINT_EQUALS(special_size, stats.special_blocks_used);

    /* Clean up */
    aws_s3_buffer_ticket_release(ticket);
    aws_future_s3_buffer_ticket_release(future);
    aws_s3_default_buffer_pool_destroy(buffer_pool);

    return 0;
}
AWS_TEST_CASE(test_s3_buffer_pool_add_special_size_duplicate, s_test_s3_buffer_pool_add_special_size_duplicate)

/* Test adding multiple different special sizes */
static int s_test_s3_buffer_pool_add_special_size_multiple(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_buffer_pool *buffer_pool = aws_s3_default_buffer_pool_new(
        allocator, (struct aws_s3_buffer_pool_config){.part_size = MB_TO_BYTES(8), .memory_limit = GB_TO_BYTES(2)});
    ASSERT_NOT_NULL(buffer_pool);

    /* Add multiple special sizes */
    uint64_t size1 = MB_TO_BYTES(64);
    uint64_t size2 = MB_TO_BYTES(128);
    uint64_t size3 = MB_TO_BYTES(256);

    ASSERT_INT_EQUALS(AWS_OP_SUCCESS, aws_s3_buffer_pool_add_special_size(buffer_pool, size1));
    ASSERT_INT_EQUALS(AWS_OP_SUCCESS, aws_s3_buffer_pool_add_special_size(buffer_pool, size2));
    ASSERT_INT_EQUALS(AWS_OP_SUCCESS, aws_s3_buffer_pool_add_special_size(buffer_pool, size3));

    /* Allocate from each special size */
    struct aws_s3_buffer_ticket *tickets[3];
    struct aws_future_s3_buffer_ticket *futures[3];
    uint64_t sizes[] = {size1, size2, size3};
    uint64_t total_size = size1 + size2 + size3;

    for (size_t i = 0; i < 3; ++i) {
        futures[i] =
            aws_s3_default_buffer_pool_reserve(buffer_pool, (struct aws_s3_buffer_pool_reserve_meta){.size = sizes[i]});
        ASSERT_NOT_NULL(futures[i]);
        ASSERT_TRUE(aws_future_s3_buffer_ticket_is_done(futures[i]));
        ASSERT_INT_EQUALS(aws_future_s3_buffer_ticket_get_error(futures[i]), AWS_OP_SUCCESS);

        tickets[i] = aws_future_s3_buffer_ticket_get_result_by_move(futures[i]);
        ASSERT_NOT_NULL(tickets[i]);

        struct aws_byte_buf buf = aws_s3_buffer_ticket_claim(tickets[i]);
        ASSERT_NOT_NULL(buf.buffer);
        ASSERT_UINT_EQUALS(sizes[i], buf.capacity);
    }

    /* Verify special blocks statistics */
    struct aws_s3_default_buffer_pool_usage_stats stats = aws_s3_default_buffer_pool_get_usage(buffer_pool);
    ASSERT_UINT_EQUALS(total_size, stats.special_blocks_allocated);
    ASSERT_UINT_EQUALS(3, stats.special_blocks_num);
    ASSERT_UINT_EQUALS(0, stats.special_blocks_reserved);
    ASSERT_UINT_EQUALS(total_size, stats.special_blocks_used);

    /* Clean up */
    for (size_t i = 0; i < 3; ++i) {
        aws_s3_buffer_ticket_release(tickets[i]);
        aws_future_s3_buffer_ticket_release(futures[i]);
    }
    stats = aws_s3_default_buffer_pool_get_usage(buffer_pool);
    ASSERT_UINT_EQUALS(total_size, stats.special_blocks_allocated);
    ASSERT_UINT_EQUALS(3, stats.special_blocks_num);
    ASSERT_UINT_EQUALS(0, stats.special_blocks_reserved);
    ASSERT_UINT_EQUALS(0, stats.special_blocks_used);
    aws_s3_default_buffer_pool_destroy(buffer_pool);

    return 0;
}
AWS_TEST_CASE(test_s3_buffer_pool_add_special_size_multiple, s_test_s3_buffer_pool_add_special_size_multiple)

/* Test that special size below primary cutoff is handled gracefully */
static int s_test_s3_buffer_pool_add_special_size_below_cutoff(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_buffer_pool *buffer_pool = aws_s3_default_buffer_pool_new(
        allocator, (struct aws_s3_buffer_pool_config){.part_size = MB_TO_BYTES(8), .memory_limit = GB_TO_BYTES(2)});
    ASSERT_NOT_NULL(buffer_pool);

    struct aws_s3_default_buffer_pool_usage_stats stats = aws_s3_default_buffer_pool_get_usage(buffer_pool);
    uint64_t small_size = stats.primary_cutoff - 1;

    /* Should succeed but log a warning */
    int result = aws_s3_buffer_pool_add_special_size(buffer_pool, small_size);
    ASSERT_INT_EQUALS(AWS_OP_SUCCESS, result);

    /* Should still be able to allocate this size (from primary storage) */
    struct aws_future_s3_buffer_ticket *future =
        aws_s3_default_buffer_pool_reserve(buffer_pool, (struct aws_s3_buffer_pool_reserve_meta){.size = small_size});
    ASSERT_NOT_NULL(future);
    ASSERT_TRUE(aws_future_s3_buffer_ticket_is_done(future));
    ASSERT_INT_EQUALS(aws_future_s3_buffer_ticket_get_error(future), AWS_OP_SUCCESS);

    struct aws_s3_buffer_ticket *ticket = aws_future_s3_buffer_ticket_get_result_by_move(future);
    ASSERT_NOT_NULL(ticket);

    struct aws_byte_buf buf = aws_s3_buffer_ticket_claim(ticket);
    ASSERT_NOT_NULL(buf.buffer);

    /* Verify special blocks statistics - should be 0 since size is below cutoff */
    stats = aws_s3_default_buffer_pool_get_usage(buffer_pool);
    ASSERT_UINT_EQUALS(0, stats.special_blocks_allocated);
    ASSERT_UINT_EQUALS(0, stats.special_blocks_num);
    ASSERT_UINT_EQUALS(0, stats.special_blocks_reserved);
    ASSERT_UINT_EQUALS(0, stats.special_blocks_used);

    /* Clean up */
    aws_s3_buffer_ticket_release(ticket);
    aws_future_s3_buffer_ticket_release(future);
    aws_s3_default_buffer_pool_destroy(buffer_pool);

    return 0;
}
AWS_TEST_CASE(test_s3_buffer_pool_add_special_size_below_cutoff, s_test_s3_buffer_pool_add_special_size_below_cutoff)

/* Test buffer reuse with special sizes */
static int s_test_s3_buffer_pool_special_size_reuse(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_buffer_pool *buffer_pool = aws_s3_default_buffer_pool_new(
        allocator, (struct aws_s3_buffer_pool_config){.part_size = MB_TO_BYTES(8), .memory_limit = GB_TO_BYTES(2)});
    ASSERT_NOT_NULL(buffer_pool);

    uint64_t special_size = MB_TO_BYTES(64);
    ASSERT_INT_EQUALS(AWS_OP_SUCCESS, aws_s3_buffer_pool_add_special_size(buffer_pool, special_size));

    /* Allocate and release multiple times to test reuse */
    for (size_t iteration = 0; iteration < 5; ++iteration) {
        struct aws_future_s3_buffer_ticket *future = aws_s3_default_buffer_pool_reserve(
            buffer_pool, (struct aws_s3_buffer_pool_reserve_meta){.size = special_size});
        ASSERT_NOT_NULL(future);
        ASSERT_TRUE(aws_future_s3_buffer_ticket_is_done(future));
        ASSERT_INT_EQUALS(aws_future_s3_buffer_ticket_get_error(future), AWS_OP_SUCCESS);

        struct aws_s3_buffer_ticket *ticket = aws_future_s3_buffer_ticket_get_result_by_move(future);
        ASSERT_NOT_NULL(ticket);

        struct aws_byte_buf buf = aws_s3_buffer_ticket_claim(ticket);
        ASSERT_NOT_NULL(buf.buffer);
        ASSERT_UINT_EQUALS(special_size, buf.capacity);

        /* Write to buffer to ensure it's valid */
        memset(buf.buffer, (int)iteration, buf.capacity);

        aws_s3_buffer_ticket_release(ticket);
        aws_future_s3_buffer_ticket_release(future);
    }
    struct aws_s3_default_buffer_pool_usage_stats stats = aws_s3_default_buffer_pool_get_usage(buffer_pool);
    /* Only one block allocated */
    ASSERT_UINT_EQUALS(special_size, stats.special_blocks_allocated);
    ASSERT_UINT_EQUALS(1, stats.special_blocks_num);
    ASSERT_UINT_EQUALS(0, stats.special_blocks_reserved);
    ASSERT_UINT_EQUALS(0, stats.special_blocks_used);

    aws_s3_default_buffer_pool_destroy(buffer_pool);
    return 0;
}

/* Test mixing special size allocations with regular allocations */
static int s_test_s3_buffer_pool_special_size_mixed(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_buffer_pool *buffer_pool = aws_s3_default_buffer_pool_new(
        allocator, (struct aws_s3_buffer_pool_config){.part_size = MB_TO_BYTES(8), .memory_limit = GB_TO_BYTES(2)});
    ASSERT_NOT_NULL(buffer_pool);

    uint64_t special_size = MB_TO_BYTES(64);
    ASSERT_INT_EQUALS(AWS_OP_SUCCESS, aws_s3_buffer_pool_add_special_size(buffer_pool, special_size));

    /* Allocate mix of special and regular sizes */
    struct aws_s3_buffer_ticket *tickets[6];
    struct aws_future_s3_buffer_ticket *futures[6];
    size_t sizes[] = {
        MB_TO_BYTES(8),   /* regular primary */
        special_size,     /* special */
        MB_TO_BYTES(16),  /* regular primary */
        special_size,     /* special */
        MB_TO_BYTES(128), /* regular secondary */
        special_size      /* special */
    };

    for (size_t i = 0; i < 6; ++i) {
        futures[i] =
            aws_s3_default_buffer_pool_reserve(buffer_pool, (struct aws_s3_buffer_pool_reserve_meta){.size = sizes[i]});
        ASSERT_NOT_NULL(futures[i]);
        ASSERT_TRUE(aws_future_s3_buffer_ticket_is_done(futures[i]));
        ASSERT_INT_EQUALS(aws_future_s3_buffer_ticket_get_error(futures[i]), AWS_OP_SUCCESS);

        tickets[i] = aws_future_s3_buffer_ticket_get_result_by_move(futures[i]);
        ASSERT_NOT_NULL(tickets[i]);

        struct aws_byte_buf buf = aws_s3_buffer_ticket_claim(tickets[i]);
        ASSERT_NOT_NULL(buf.buffer);
        ASSERT_UINT_EQUALS(sizes[i], buf.capacity);
    }

    /* Verify special blocks statistics - 3 special buffers allocated */
    struct aws_s3_default_buffer_pool_usage_stats stats = aws_s3_default_buffer_pool_get_usage(buffer_pool);
    ASSERT_UINT_EQUALS(special_size * 3, stats.special_blocks_allocated);
    ASSERT_UINT_EQUALS(1, stats.special_blocks_num);
    ASSERT_UINT_EQUALS(0, stats.special_blocks_reserved);
    ASSERT_UINT_EQUALS(special_size * 3, stats.special_blocks_used);

    /* Clean up */
    for (size_t i = 0; i < 6; ++i) {
        aws_s3_buffer_ticket_release(tickets[i]);
        aws_future_s3_buffer_ticket_release(futures[i]);
    }
    aws_s3_default_buffer_pool_destroy(buffer_pool);

    return 0;
}
AWS_TEST_CASE(test_s3_buffer_pool_special_size_mixed, s_test_s3_buffer_pool_special_size_mixed)

/* Test special size with memory limit constraints. */
static int s_test_s3_buffer_pool_special_size_with_limits(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    /* Test that special sizes work correctly with memory limits */
    struct aws_s3_buffer_pool *buffer_pool = aws_s3_default_buffer_pool_new(
        allocator, (struct aws_s3_buffer_pool_config){.part_size = MB_TO_BYTES(8), .memory_limit = GB_TO_BYTES(1)});
    ASSERT_NOT_NULL(buffer_pool);

    uint64_t special_size = MB_TO_BYTES(300);
    ASSERT_INT_EQUALS(AWS_OP_SUCCESS, aws_s3_buffer_pool_add_special_size(buffer_pool, special_size));

    /* Allocate several buffers to verify special sizes respect memory limits */
    struct aws_s3_buffer_ticket *tickets[7];
    struct aws_future_s3_buffer_ticket *futures[7];
    size_t sizes[] = {
        MB_TO_BYTES(8),   /* regular primary */
        special_size,     /* special */
        special_size,     /* special */
        special_size,     /* special */
        special_size,     /* special. BLOCKED */
        MB_TO_BYTES(16),  /* regular primary */
        MB_TO_BYTES(256), /* regular second. BLOCKED */
    };

    for (size_t i = 0; i < 7; ++i) {
        futures[i] = aws_s3_default_buffer_pool_reserve(
            buffer_pool, (struct aws_s3_buffer_pool_reserve_meta){.size = special_size});
        ASSERT_NOT_NULL(futures[i]);
        if (i != 4 || i != 6) {
            /* The last special reserve should be blocked by the limit, but all the others should still go through. */
            ASSERT_TRUE(aws_future_s3_buffer_ticket_is_done(futures[i]));
            ASSERT_INT_EQUALS(aws_future_s3_buffer_ticket_get_error(futures[i]), AWS_OP_SUCCESS);

            tickets[i] = aws_future_s3_buffer_ticket_get_result_by_move(futures[i]);
            ASSERT_NOT_NULL(tickets[i]);

            struct aws_byte_buf buf = aws_s3_buffer_ticket_claim(tickets[i]);
            ASSERT_NOT_NULL(buf.buffer);
            ASSERT_UINT_EQUALS(sizes[i], buf.capacity);
        } else {
            ASSERT_FALSE(aws_future_s3_buffer_ticket_is_done(futures[i]));
            ASSERT_INT_EQUALS(
                aws_future_s3_buffer_ticket_get_error(futures[i]), AWS_ERROR_S3_BUFFER_POOL_OUT_OF_MEMORY);
        }
    }

    /* Verify usage stats show special size allocations */
    struct aws_s3_default_buffer_pool_usage_stats stats = aws_s3_default_buffer_pool_get_usage(buffer_pool);
    ASSERT_TRUE(stats.mem_limit > 0);
    ASSERT_UINT_EQUALS(special_size * 3, stats.special_blocks_allocated);
    ASSERT_UINT_EQUALS(1, stats.special_blocks_num);
    ASSERT_UINT_EQUALS(0, stats.special_blocks_reserved);
    ASSERT_UINT_EQUALS(special_size * NUM_LIMIT_TEST_BUFFERS, stats.special_blocks_used);

    /* Clean up */
    for (size_t i = 0; i < NUM_LIMIT_TEST_BUFFERS; ++i) {
        aws_s3_buffer_ticket_release(tickets[i]);
        aws_future_s3_buffer_ticket_release(futures[i]);
    }
    aws_s3_default_buffer_pool_destroy(buffer_pool);

    return 0;
}
AWS_TEST_CASE(test_s3_buffer_pool_special_size_with_limits, s_test_s3_buffer_pool_special_size_with_limits)

/* Test that special sizes work correctly with buffer release and reallocation. TODO */
static int s_test_s3_buffer_pool_special_size_trim(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_buffer_pool *buffer_pool = aws_s3_default_buffer_pool_new(
        allocator, (struct aws_s3_buffer_pool_config){.part_size = MB_TO_BYTES(8), .memory_limit = GB_TO_BYTES(2)});
    ASSERT_NOT_NULL(buffer_pool);

    uint64_t special_size = MB_TO_BYTES(64);
    ASSERT_INT_EQUALS(AWS_OP_SUCCESS, aws_s3_buffer_pool_add_special_size(buffer_pool, special_size));

    /* Allocate several special size buffers */
#define NUM_TRIM_TEST_BUFFERS 5
    struct aws_s3_buffer_ticket *tickets[NUM_TRIM_TEST_BUFFERS];
    struct aws_future_s3_buffer_ticket *futures[NUM_TRIM_TEST_BUFFERS];

    for (size_t i = 0; i < NUM_TRIM_TEST_BUFFERS; ++i) {
        futures[i] = aws_s3_default_buffer_pool_reserve(
            buffer_pool, (struct aws_s3_buffer_pool_reserve_meta){.size = special_size});
        ASSERT_NOT_NULL(futures[i]);
        ASSERT_TRUE(aws_future_s3_buffer_ticket_is_done(futures[i]));
        ASSERT_INT_EQUALS(aws_future_s3_buffer_ticket_get_error(futures[i]), AWS_OP_SUCCESS);

        tickets[i] = aws_future_s3_buffer_ticket_get_result_by_move(futures[i]);
        ASSERT_NOT_NULL(tickets[i]);

        struct aws_byte_buf buf = aws_s3_buffer_ticket_claim(tickets[i]);
        ASSERT_NOT_NULL(buf.buffer);
        ASSERT_UINT_EQUALS(special_size, buf.capacity);
    }

    /* Release half of them */
    for (size_t i = 0; i < NUM_TRIM_TEST_BUFFERS / 2; ++i) {
        aws_s3_buffer_ticket_release(tickets[i]);
        aws_future_s3_buffer_ticket_release(futures[i]);
    }

    /* Should still be able to allocate from special size after releasing some buffers */
    struct aws_future_s3_buffer_ticket *new_future =
        aws_s3_default_buffer_pool_reserve(buffer_pool, (struct aws_s3_buffer_pool_reserve_meta){.size = special_size});
    ASSERT_NOT_NULL(new_future);
    ASSERT_TRUE(aws_future_s3_buffer_ticket_is_done(new_future));
    ASSERT_INT_EQUALS(aws_future_s3_buffer_ticket_get_error(new_future), AWS_OP_SUCCESS);

    struct aws_s3_buffer_ticket *new_ticket = aws_future_s3_buffer_ticket_get_result_by_move(new_future);
    ASSERT_NOT_NULL(new_ticket);

    struct aws_byte_buf new_buf = aws_s3_buffer_ticket_claim(new_ticket);
    ASSERT_NOT_NULL(new_buf.buffer);
    ASSERT_UINT_EQUALS(special_size, new_buf.capacity);

    /* Verify special blocks statistics - 4 buffers still in use (3 from second half + 1 new) */
    struct aws_s3_default_buffer_pool_usage_stats stats = aws_s3_default_buffer_pool_get_usage(buffer_pool);
    ASSERT_UINT_EQUALS(special_size * NUM_TRIM_TEST_BUFFERS, stats.special_blocks_allocated);
    ASSERT_UINT_EQUALS(1, stats.special_blocks_num);
    ASSERT_UINT_EQUALS(0, stats.special_blocks_reserved);
    ASSERT_UINT_EQUALS(special_size * 4, stats.special_blocks_used); /* 4 buffers in use */

    /* Clean up remaining tickets */
    for (size_t i = NUM_TRIM_TEST_BUFFERS / 2; i < NUM_TRIM_TEST_BUFFERS; ++i) {
        aws_s3_buffer_ticket_release(tickets[i]);
        aws_future_s3_buffer_ticket_release(futures[i]);
    }

    /* Clean up new ticket */
    aws_s3_buffer_ticket_release(new_ticket);
    aws_future_s3_buffer_ticket_release(new_future);

    aws_s3_default_buffer_pool_destroy(buffer_pool);

#undef NUM_TRIM_TEST_BUFFERS
    return 0;
}
AWS_TEST_CASE(test_s3_buffer_pool_special_size_trim, s_test_s3_buffer_pool_special_size_trim)
