/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/s3/private/s3_default_buffer_pool.h>
#include <aws/s3/private/s3_util.h>

#include <aws/common/process.h>
#include <aws/common/thread.h>
#include <aws/testing/aws_test_harness.h>

#define NUM_TEST_ALLOCS 100
#define NUM_TEST_THREADS 8

struct pool_thread_test_data {
    struct aws_s3_default_buffer_pool *pool;
    uint32_t thread_idx;
};

static void s_thread_test(
    struct aws_allocator *allocator,
    void (*thread_fn)(void *),
    struct aws_s3_default_buffer_pool *pool) {
    const struct aws_thread_options *thread_options = aws_default_thread_options();
    struct aws_thread threads[NUM_TEST_THREADS];
    struct pool_thread_test_data thread_data[NUM_TEST_THREADS];
    AWS_ZERO_ARRAY(threads);
    AWS_ZERO_ARRAY(thread_data);
    for (size_t thread_idx = 0; thread_idx < AWS_ARRAY_SIZE(threads); ++thread_idx) {
        struct aws_thread *thread = &threads[thread_idx];
        aws_thread_init(thread, allocator);
        struct pool_thread_test_data *data = &thread_data[thread_idx];
        data->pool = pool;
        data->thread_idx = (uint32_t)thread_idx;
        aws_thread_launch(thread, thread_fn, data, thread_options);
    }

    for (size_t thread_idx = 0; thread_idx < AWS_ARRAY_SIZE(threads); ++thread_idx) {
        struct aws_thread *thread = &threads[thread_idx];
        aws_thread_join(thread);
    }
}

static void s_threaded_alloc_worker(void *user_data) {
    struct aws_s3_default_buffer_pool *pool = ((struct pool_thread_test_data *)user_data)->pool;

    struct aws_s3_buffer_ticket *tickets[NUM_TEST_ALLOCS];
    for (size_t count = 0; count < NUM_TEST_ALLOCS / NUM_TEST_THREADS; ++count) {
        struct aws_future_s3_buffer_ticket *future = aws_s3_default_buffer_pool_reserve(pool, 
            (struct aws_s3_buffer_pool_reserve_meta){.size = MB_TO_BYTES(8)});
        AWS_FATAL_ASSERT(future != NULL);
        AWS_FATAL_ASSERT(aws_future_s3_buffer_ticket_is_done(future));
        AWS_FATAL_ASSERT(aws_future_s3_buffer_ticket_get_error(future) == AWS_OP_SUCCESS);
        struct aws_s3_buffer_ticket *ticket = aws_future_s3_buffer_ticket_get_result(future);

        struct aws_byte_buf buf = aws_s3_default_buffer_pool_acquire_buffer(pool, ticket);
        AWS_FATAL_ASSERT(buf.buffer);
        memset(buf.buffer, 0, buf.capacity);
        tickets[count] = ticket;
    }

    for (size_t count = 0; count < NUM_TEST_ALLOCS / NUM_TEST_THREADS; ++count) {
        aws_s3_default_buffer_pool_release_ticket(tickets[count]);
    }
}

static int s_test_s3_buffer_pool_threaded_allocs_and_frees(struct aws_allocator *allocator, void *ctx) {
    (void)allocator;
    (void)ctx;

    struct aws_s3_default_buffer_pool *buffer_pool =
        aws_s3_default_buffer_pool_new(allocator, MB_TO_BYTES(8), GB_TO_BYTES(2));

    s_thread_test(allocator, s_threaded_alloc_worker, buffer_pool);

    aws_s3_default_buffer_pool_destroy(buffer_pool);

    return 0;
}
AWS_TEST_CASE(test_s3_buffer_pool_threaded_allocs_and_frees, s_test_s3_buffer_pool_threaded_allocs_and_frees)

static int s_test_s3_buffer_pool_large_chunk_threaded_allocs_and_frees(struct aws_allocator *allocator, void *ctx) {
    (void)allocator;
    (void)ctx;

    struct aws_s3_default_buffer_pool *buffer_pool =
        aws_s3_default_buffer_pool_new(allocator, MB_TO_BYTES(65), GB_TO_BYTES(2));

    struct aws_s3_default_buffer_pool_usage_stats stats = aws_s3_default_buffer_pool_get_usage(buffer_pool);
    ASSERT_INT_EQUALS(0, stats.primary_cutoff);

    s_thread_test(allocator, s_threaded_alloc_worker, buffer_pool);

    aws_s3_default_buffer_pool_destroy(buffer_pool);

    return 0;
}
AWS_TEST_CASE(
    test_s3_buffer_pool_large_chunk_threaded_allocs_and_frees,
    s_test_s3_buffer_pool_large_chunk_threaded_allocs_and_frees)

static int s_test_s3_buffer_pool_limits(struct aws_allocator *allocator, void *ctx) {
    (void)allocator;
    (void)ctx;

    struct aws_s3_default_buffer_pool *buffer_pool =
        aws_s3_default_buffer_pool_new(allocator, MB_TO_BYTES(8), GB_TO_BYTES(1));

    struct aws_s3_buffer_pool_reserve_meta meta = {.size = MB_TO_BYTES(64)};
    struct aws_future_s3_buffer_ticket *future1 = aws_s3_default_buffer_pool_reserve(buffer_pool, meta);
    ASSERT_NOT_NULL(future1);
    ASSERT_TRUE(aws_future_s3_buffer_ticket_is_done(future1));
    ASSERT_INT_EQUALS(aws_future_s3_buffer_ticket_get_error(future1), AWS_OP_SUCCESS);
    struct aws_s3_buffer_ticket *ticket1 = aws_future_s3_buffer_ticket_get_result(future1);
    ASSERT_NOT_NULL(ticket1);
    struct aws_byte_buf buf1 = aws_s3_default_buffer_pool_acquire_buffer(buffer_pool, ticket1);
    ASSERT_NOT_NULL(buf1.buffer);

    struct aws_s3_buffer_ticket *tickets[6];
    struct aws_future_s3_buffer_ticket *ticket_futures[6];
    for (size_t i = 0; i < 6; ++i) {
        ticket_futures[i] = aws_s3_default_buffer_pool_reserve(buffer_pool, 
            (struct aws_s3_buffer_pool_reserve_meta){.size = MB_TO_BYTES(128)});
        ASSERT_NOT_NULL(ticket_futures[i]);
        ASSERT_TRUE(aws_future_s3_buffer_ticket_is_done(ticket_futures[i]));
        ASSERT_INT_EQUALS(aws_future_s3_buffer_ticket_get_error(ticket_futures[i]), AWS_OP_SUCCESS);
        tickets[i] = aws_future_s3_buffer_ticket_get_result(ticket_futures[i]);
        ASSERT_NOT_NULL(tickets[i]);
        struct aws_byte_buf buf = aws_s3_default_buffer_pool_acquire_buffer(buffer_pool, tickets[i]);
        ASSERT_NOT_NULL(buf.buffer);
    }

    struct aws_future_s3_buffer_ticket *overallocated_future = aws_s3_default_buffer_pool_reserve(buffer_pool, 
        (struct aws_s3_buffer_pool_reserve_meta){.size = MB_TO_BYTES(128)});
    ASSERT_FALSE(aws_future_s3_buffer_ticket_is_done(overallocated_future));

    struct aws_future_s3_buffer_ticket *overallocated_future2 = aws_s3_default_buffer_pool_reserve(buffer_pool, 
        (struct aws_s3_buffer_pool_reserve_meta){.size = MB_TO_BYTES(96)});
    ASSERT_FALSE(aws_future_s3_buffer_ticket_is_done(overallocated_future2));

    struct aws_future_s3_buffer_ticket *future2 = aws_s3_default_buffer_pool_reserve(buffer_pool, 
        (struct aws_s3_buffer_pool_reserve_meta){.size = MB_TO_BYTES(32)});
    ASSERT_NOT_NULL(future2);
    ASSERT_TRUE(aws_future_s3_buffer_ticket_is_done(future2));
    ASSERT_INT_EQUALS(aws_future_s3_buffer_ticket_get_error(future2), AWS_OP_SUCCESS);
    struct aws_s3_buffer_ticket *ticket2 = aws_future_s3_buffer_ticket_get_result(future2);
    ASSERT_NOT_NULL(ticket2);
    struct aws_byte_buf buf2 = aws_s3_default_buffer_pool_acquire_buffer(buffer_pool, ticket2);
    ASSERT_NOT_NULL(buf2.buffer);

    for (size_t i = 0; i < 6; ++i) {
        aws_s3_default_buffer_pool_release_ticket(tickets[i]);
        aws_future_s3_buffer_ticket_release(ticket_futures[i]);
    }

    aws_s3_default_buffer_pool_release_ticket(ticket1);
    aws_future_s3_buffer_ticket_release(future1);
    aws_s3_default_buffer_pool_release_ticket(ticket2);
    aws_future_s3_buffer_ticket_release(future2);

    if (aws_future_s3_buffer_ticket_is_done(overallocated_future)) {
        aws_s3_default_buffer_pool_release_ticket(aws_future_s3_buffer_ticket_get_result(overallocated_future));
    }
    aws_future_s3_buffer_ticket_release(overallocated_future);
    if (aws_future_s3_buffer_ticket_is_done(overallocated_future2)) {
        aws_s3_default_buffer_pool_release_ticket(aws_future_s3_buffer_ticket_get_result(overallocated_future2));
    }
    aws_future_s3_buffer_ticket_release(overallocated_future2);

    aws_s3_default_buffer_pool_destroy(buffer_pool);

    return 0;
}
AWS_TEST_CASE(test_s3_buffer_pool_limits, s_test_s3_buffer_pool_limits)

static int s_test_s3_buffer_pool_trim(struct aws_allocator *allocator, void *ctx) {
    (void)allocator;
    (void)ctx;

    struct aws_s3_default_buffer_pool *buffer_pool =
        aws_s3_default_buffer_pool_new(allocator, MB_TO_BYTES(8), GB_TO_BYTES(1));

    struct aws_s3_buffer_ticket *tickets[40];
    struct aws_future_s3_buffer_ticket *ticket_futures[40];
    for (size_t i = 0; i < 40; ++i) {
        ticket_futures[i] = aws_s3_default_buffer_pool_reserve(buffer_pool, 
            (struct aws_s3_buffer_pool_reserve_meta){.size = MB_TO_BYTES(8)});
        ASSERT_NOT_NULL(ticket_futures[i]);
        ASSERT_TRUE(aws_future_s3_buffer_ticket_is_done(ticket_futures[i]));
        ASSERT_INT_EQUALS(aws_future_s3_buffer_ticket_get_error(ticket_futures[i]), AWS_OP_SUCCESS);
        tickets[i] = aws_future_s3_buffer_ticket_get_result(ticket_futures[i]);
        struct aws_byte_buf buf = aws_s3_default_buffer_pool_acquire_buffer(buffer_pool, tickets[i]);
        ASSERT_NOT_NULL(buf.buffer);
    }

    struct aws_s3_default_buffer_pool_usage_stats stats_before = aws_s3_default_buffer_pool_get_usage(buffer_pool);

    for (size_t i = 0; i < 20; ++i) {
        aws_s3_default_buffer_pool_release_ticket(tickets[i]);
    }

    aws_s3_default_buffer_pool_trim(buffer_pool);

    struct aws_s3_default_buffer_pool_usage_stats stats_after = aws_s3_default_buffer_pool_get_usage(buffer_pool);

    ASSERT_TRUE(stats_before.primary_num_blocks > stats_after.primary_num_blocks);
    ASSERT_TRUE(stats_before.primary_allocated > stats_after.primary_allocated);

    for (size_t i = 20; i < 40; ++i) {
        aws_s3_default_buffer_pool_release_ticket(tickets[i]);
    }

    aws_s3_default_buffer_pool_destroy(buffer_pool);

    return 0;
};
AWS_TEST_CASE(test_s3_buffer_pool_trim, s_test_s3_buffer_pool_trim)

struct s_reserve_state {
    struct aws_future_s3_buffer_ticket *future;
    struct aws_s3_buffer_ticket *ticket;
};

static void s_on_pool_buffer_reserved(void *user_data) {
    struct s_reserve_state *state = user_data;

    if (aws_future_s3_buffer_ticket_get_error(state->future) == AWS_OP_SUCCESS) {
        state->ticket = aws_future_s3_buffer_ticket_get_result(state->future);
    }
}

static int s_test_s3_buffer_pool_reserve_over_limit(struct aws_allocator *allocator, void *ctx) {
    (void)allocator;
    (void)ctx;

    struct aws_s3_default_buffer_pool *buffer_pool =
        aws_s3_default_buffer_pool_new(allocator, MB_TO_BYTES(8), GB_TO_BYTES(1));

    struct aws_s3_buffer_ticket *tickets[112];
    struct aws_future_s3_buffer_ticket *ticket_futures[112];
    for (size_t i = 0; i < 112; ++i) {
        ticket_futures[i] = aws_s3_default_buffer_pool_reserve(buffer_pool, 
            (struct aws_s3_buffer_pool_reserve_meta){.size = MB_TO_BYTES(8)});
        ASSERT_TRUE(aws_future_s3_buffer_ticket_is_done(ticket_futures[i]));
        ASSERT_INT_EQUALS(aws_future_s3_buffer_ticket_get_error(ticket_futures[i]), AWS_OP_SUCCESS);
        tickets[i] = aws_future_s3_buffer_ticket_get_result(ticket_futures[i]);
        struct aws_byte_buf buf = aws_s3_default_buffer_pool_acquire_buffer(buffer_pool, tickets[i]);
        ASSERT_NOT_NULL(buf.buffer);
    }

    struct aws_future_s3_buffer_ticket *over_future = aws_s3_default_buffer_pool_reserve(buffer_pool, 
        (struct aws_s3_buffer_pool_reserve_meta){.size = MB_TO_BYTES(8)});

    ASSERT_FALSE(aws_future_s3_buffer_ticket_is_done(over_future));

    struct s_reserve_state state = {
        .future = over_future
    };

    aws_future_s3_buffer_ticket_register_callback(over_future, s_on_pool_buffer_reserved, &state);

    for (size_t i = 0; i < 112; ++i) {
        aws_s3_default_buffer_pool_release_ticket(tickets[i]);
    }

    ASSERT_NOT_NULL(state.ticket);

    aws_s3_default_buffer_pool_release_ticket(state.ticket);

    aws_s3_default_buffer_pool_destroy(buffer_pool);

    return 0;
};
AWS_TEST_CASE(test_s3_buffer_pool_reserve_over_limit, s_test_s3_buffer_pool_reserve_over_limit)

static int s_test_s3_buffer_pool_too_small(struct aws_allocator *allocator, void *ctx) {
    (void)allocator;
    (void)ctx;

    struct aws_s3_default_buffer_pool *buffer_pool =
        aws_s3_default_buffer_pool_new(allocator, MB_TO_BYTES(8), MB_TO_BYTES(512));
    ASSERT_NULL(buffer_pool);
    ASSERT_INT_EQUALS(AWS_ERROR_S3_INVALID_MEMORY_LIMIT_CONFIG, aws_last_error());

    return 0;
};
AWS_TEST_CASE(test_s3_buffer_pool_too_small, s_test_s3_buffer_pool_too_small)

/* Sanity check that forced-buffer allocation works at all */
static int s_test_s3_buffer_pool_forced_buffer(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    const size_t chunk_size = MB_TO_BYTES(8);
    struct aws_s3_default_buffer_pool *buffer_pool =
        aws_s3_default_buffer_pool_new(allocator, chunk_size, GB_TO_BYTES(1));

    { /* Acquire forced buffer from primary storage */
        size_t acquire_size = chunk_size;
        struct aws_s3_buffer_ticket *forced_ticket = NULL;
        struct aws_future_s3_buffer_ticket *forced_future  = aws_s3_default_buffer_pool_reserve(buffer_pool, 
            (struct aws_s3_buffer_pool_reserve_meta){.size = MB_TO_BYTES(8), .can_block = true});
        ASSERT_TRUE(aws_future_s3_buffer_ticket_is_done(forced_future));
        ASSERT_INT_EQUALS(aws_future_s3_buffer_ticket_get_error(forced_future), AWS_OP_SUCCESS);
        forced_ticket = aws_future_s3_buffer_ticket_get_result(forced_future);
        struct aws_byte_buf forced_buf = aws_s3_default_buffer_pool_acquire_buffer(buffer_pool, forced_ticket);
        ASSERT_NOT_NULL(forced_buf.buffer);
        ASSERT_UINT_EQUALS(acquire_size, forced_buf.capacity);
        ASSERT_UINT_EQUALS(0, forced_buf.len);

        struct aws_s3_default_buffer_pool_usage_stats stats = aws_s3_default_buffer_pool_get_usage(buffer_pool);
        ASSERT_UINT_EQUALS(acquire_size, stats.forced_used);
        ASSERT_UINT_EQUALS(acquire_size, stats.primary_used);
        ASSERT_UINT_EQUALS(0, stats.primary_reserved);
        aws_s3_default_buffer_pool_release_ticket(forced_ticket);
    }

    { /* Acquire forced buffer from secondary storage */
        size_t acquire_size = aws_s3_default_buffer_pool_get_usage(buffer_pool).primary_cutoff + 1;
        struct aws_s3_buffer_ticket *forced_ticket = NULL;
        struct aws_future_s3_buffer_ticket *forced_future  = aws_s3_default_buffer_pool_reserve(buffer_pool, 
            (struct aws_s3_buffer_pool_reserve_meta){.size = MB_TO_BYTES(8), .can_block = true});
        ASSERT_TRUE(aws_future_s3_buffer_ticket_is_done(forced_future));
        ASSERT_INT_EQUALS(aws_future_s3_buffer_ticket_get_error(forced_future), AWS_OP_SUCCESS);
        forced_ticket = aws_future_s3_buffer_ticket_get_result(forced_future);
        struct aws_byte_buf forced_buf = aws_s3_default_buffer_pool_acquire_buffer(buffer_pool, forced_ticket);
        ASSERT_NOT_NULL(forced_buf.buffer);
        ASSERT_UINT_EQUALS(acquire_size, forced_buf.capacity);
        ASSERT_UINT_EQUALS(0, forced_buf.len);

        struct aws_s3_default_buffer_pool_usage_stats stats = aws_s3_default_buffer_pool_get_usage(buffer_pool);
        ASSERT_UINT_EQUALS(acquire_size, stats.forced_used);
        ASSERT_UINT_EQUALS(acquire_size, stats.secondary_used);
        ASSERT_UINT_EQUALS(0, stats.secondary_reserved);
        aws_s3_default_buffer_pool_release_ticket(forced_ticket);
    }

    /* Assert stats go back down after tickets released */
    struct aws_s3_default_buffer_pool_usage_stats stats = aws_s3_default_buffer_pool_get_usage(buffer_pool);
    ASSERT_UINT_EQUALS(0, stats.forced_used);
    ASSERT_UINT_EQUALS(0, stats.primary_used);
    ASSERT_UINT_EQUALS(0, stats.secondary_used);

    aws_s3_default_buffer_pool_destroy(buffer_pool);
    return 0;
}
AWS_TEST_CASE(test_s3_buffer_pool_forced_buffer, s_test_s3_buffer_pool_forced_buffer)

/* Test that we can still acquire forced buffers, even after pool has a reservation-hold */
static int s_test_s3_buffer_pool_forced_buffer_after_limit_hit(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    const size_t chunk_size = MB_TO_BYTES(8);
    struct aws_s3_default_buffer_pool *buffer_pool =
        aws_s3_default_buffer_pool_new(allocator, chunk_size, GB_TO_BYTES(1));

    /* Reserve normal tickets until pool has reservation-hold */
    struct aws_s3_buffer_ticket *tickets[112];
    struct aws_future_s3_buffer_ticket *ticket_futures[112];
    for (size_t i = 0; i < 112; ++i) {
        ticket_futures[i] = aws_s3_default_buffer_pool_reserve(buffer_pool, 
            (struct aws_s3_buffer_pool_reserve_meta){.size = MB_TO_BYTES(8)});
        ASSERT_TRUE(aws_future_s3_buffer_ticket_is_done(ticket_futures[i]));
        ASSERT_INT_EQUALS(aws_future_s3_buffer_ticket_get_error(ticket_futures[i]), AWS_OP_SUCCESS);
        tickets[i] = aws_future_s3_buffer_ticket_get_result(ticket_futures[i]);
    }

    /* Assert we can still get a forced-buffer */
    struct aws_s3_buffer_ticket *forced_ticket_1 = NULL;
    struct aws_future_s3_buffer_ticket *forced_future_1  = aws_s3_default_buffer_pool_reserve(buffer_pool, 
        (struct aws_s3_buffer_pool_reserve_meta){.size = MB_TO_BYTES(8), .can_block = true});
    ASSERT_TRUE(aws_future_s3_buffer_ticket_is_done(forced_future_1));
    ASSERT_INT_EQUALS(aws_future_s3_buffer_ticket_get_error(forced_future_1), AWS_OP_SUCCESS);
    forced_ticket_1 = aws_future_s3_buffer_ticket_get_result(forced_future_1);
    struct aws_byte_buf forced_buf_1 = aws_s3_default_buffer_pool_acquire_buffer(buffer_pool, forced_ticket_1);
    ASSERT_NOT_NULL(forced_ticket_1);
    ASSERT_UINT_EQUALS(chunk_size, forced_buf_1.capacity);

    /* Assert we can still acquire buffers for all those normal reservations */
    for (size_t i = 0; i < 112; ++i) {
        struct aws_byte_buf normal_buf = aws_s3_default_buffer_pool_acquire_buffer(buffer_pool, tickets[i]);
        ASSERT_UINT_EQUALS(chunk_size, normal_buf.capacity);
    }

    /* Assert we can still get a forced-buffer */
    struct aws_s3_buffer_ticket *forced_ticket_2 = NULL;
    struct aws_future_s3_buffer_ticket *forced_future_2  = aws_s3_default_buffer_pool_reserve(buffer_pool, 
        (struct aws_s3_buffer_pool_reserve_meta){.size = MB_TO_BYTES(8), .can_block = true});
    ASSERT_TRUE(aws_future_s3_buffer_ticket_is_done(forced_future_2));
    ASSERT_INT_EQUALS(aws_future_s3_buffer_ticket_get_error(forced_future_2), AWS_OP_SUCCESS);
    forced_ticket_2 = aws_future_s3_buffer_ticket_get_result(forced_future_2);
    struct aws_byte_buf forced_buf_2 = aws_s3_default_buffer_pool_acquire_buffer(buffer_pool, forced_ticket_2);
    ASSERT_NOT_NULL(forced_ticket_2);
    ASSERT_UINT_EQUALS(chunk_size, forced_buf_2.capacity);

    /* Cleanup */
    for (size_t i = 0; i < 112; ++i) {
        aws_s3_default_buffer_pool_release_ticket(tickets[i]);
    }

    aws_s3_default_buffer_pool_release_ticket(forced_ticket_1);
    aws_s3_default_buffer_pool_release_ticket(forced_ticket_2);
    aws_s3_default_buffer_pool_destroy(buffer_pool);
    return 0;
}
AWS_TEST_CASE(
    test_s3_buffer_pool_forced_buffer_after_limit_hit,
    s_test_s3_buffer_pool_forced_buffer_after_limit_hit)

/* Test that some normal tickets can still be reserved, even if forced-buffer usage is huge.
 * This is important because, if either system can stop the other from working, we risk deadlock. */
static int s_test_s3_buffer_pool_forced_buffer_wont_stop_reservations(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    const size_t chunk_size = MB_TO_BYTES(8);
    const size_t mem_limit = GB_TO_BYTES(1);
    struct aws_s3_default_buffer_pool *buffer_pool = aws_s3_default_buffer_pool_new(allocator, chunk_size, mem_limit);

    /* Skip test if this machine can't do enormous allocations */
    void *try_large_alloc = malloc(mem_limit);
    if (try_large_alloc == NULL) {
        aws_s3_default_buffer_pool_destroy(buffer_pool);
        return AWS_OP_SKIP;
    }
    free(try_large_alloc);

    /* Allocate enormous forced buffer */
    struct aws_s3_buffer_ticket *forced_ticket = NULL;
    struct aws_future_s3_buffer_ticket *forced_future  = aws_s3_default_buffer_pool_reserve(buffer_pool, 
        (struct aws_s3_buffer_pool_reserve_meta){.size = chunk_size, .can_block = true});
    ASSERT_TRUE(aws_future_s3_buffer_ticket_is_done(forced_future));
    ASSERT_INT_EQUALS(aws_future_s3_buffer_ticket_get_error(forced_future), AWS_OP_SUCCESS);
    forced_ticket = aws_future_s3_buffer_ticket_get_result(forced_future);
    struct aws_byte_buf forced_buf = aws_s3_default_buffer_pool_acquire_buffer(buffer_pool, forced_ticket);
    ASSERT_NOT_NULL(forced_ticket);
    ASSERT_UINT_EQUALS(chunk_size, forced_buf.capacity);

    /* Assert we can still reserve a normal ticket & allocate a normal buffer */
    struct aws_s3_buffer_ticket *normal_ticket = NULL;
    struct aws_future_s3_buffer_ticket *normal_future = aws_s3_default_buffer_pool_reserve(buffer_pool, 
        (struct aws_s3_buffer_pool_reserve_meta){.size =chunk_size});
    ASSERT_TRUE(aws_future_s3_buffer_ticket_is_done(normal_future));
    ASSERT_INT_EQUALS(aws_future_s3_buffer_ticket_get_error(normal_future), AWS_OP_SUCCESS);
    normal_ticket = aws_future_s3_buffer_ticket_get_result(normal_future);
    struct aws_byte_buf normal_buf = aws_s3_default_buffer_pool_acquire_buffer(buffer_pool, normal_ticket);
        ASSERT_UINT_EQUALS(chunk_size, normal_buf.capacity);
    aws_s3_default_buffer_pool_release_ticket(normal_ticket);

    /* Cleanup */
    aws_s3_default_buffer_pool_release_ticket(forced_ticket);
    aws_s3_default_buffer_pool_destroy(buffer_pool);
    return 0;
}
AWS_TEST_CASE(
    test_s3_buffer_pool_forced_buffer_wont_stop_reservations,
    s_test_s3_buffer_pool_forced_buffer_wont_stop_reservations)
