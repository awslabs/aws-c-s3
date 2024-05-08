/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/s3/private/s3_buffer_pool.h>
#include <aws/s3/private/s3_util.h>

#include <aws/common/process.h>
#include <aws/common/thread.h>
#include <aws/testing/aws_test_harness.h>

#define NUM_TEST_ALLOCS 100
#define NUM_TEST_THREADS 8

struct pool_thread_test_data {
    struct aws_s3_buffer_pool *pool;
    uint32_t thread_idx;
};

static void s_thread_test(struct aws_allocator *allocator, void (*thread_fn)(void *), struct aws_s3_buffer_pool *pool) {
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
    struct aws_s3_buffer_pool *pool = ((struct pool_thread_test_data *)user_data)->pool;

    struct aws_s3_buffer_pool_ticket *tickets[NUM_TEST_ALLOCS];
    for (size_t count = 0; count < NUM_TEST_ALLOCS / NUM_TEST_THREADS; ++count) {
        size_t size = 8 * 1024 * 1024;
        struct aws_s3_buffer_pool_ticket *ticket = aws_s3_buffer_pool_reserve(pool, size);
        AWS_FATAL_ASSERT(ticket);

        struct aws_byte_buf buf = aws_s3_buffer_pool_acquire_buffer(pool, ticket);
        AWS_FATAL_ASSERT(buf.buffer);
        memset(buf.buffer, 0, buf.capacity);
        tickets[count] = ticket;
    }

    for (size_t count = 0; count < NUM_TEST_ALLOCS / NUM_TEST_THREADS; ++count) {
        aws_s3_buffer_pool_release_ticket(pool, tickets[count]);
    }
}

static int s_test_s3_buffer_pool_threaded_allocs_and_frees(struct aws_allocator *allocator, void *ctx) {
    (void)allocator;
    (void)ctx;

    struct aws_s3_buffer_pool *buffer_pool = aws_s3_buffer_pool_new(allocator, MB_TO_BYTES(8), GB_TO_BYTES(2));

    s_thread_test(allocator, s_threaded_alloc_worker, buffer_pool);

    aws_s3_buffer_pool_destroy(buffer_pool);

    return 0;
}
AWS_TEST_CASE(test_s3_buffer_pool_threaded_allocs_and_frees, s_test_s3_buffer_pool_threaded_allocs_and_frees)

static int s_test_s3_buffer_pool_large_chunk_threaded_allocs_and_frees(struct aws_allocator *allocator, void *ctx) {
    (void)allocator;
    (void)ctx;

    struct aws_s3_buffer_pool *buffer_pool = aws_s3_buffer_pool_new(allocator, MB_TO_BYTES(65), GB_TO_BYTES(2));

    struct aws_s3_buffer_pool_usage_stats stats = aws_s3_buffer_pool_get_usage(buffer_pool);
    ASSERT_INT_EQUALS(0, stats.primary_cutoff);

    s_thread_test(allocator, s_threaded_alloc_worker, buffer_pool);

    aws_s3_buffer_pool_destroy(buffer_pool);

    return 0;
}
AWS_TEST_CASE(
    test_s3_buffer_pool_large_chunk_threaded_allocs_and_frees,
    s_test_s3_buffer_pool_large_chunk_threaded_allocs_and_frees)

static int s_test_s3_buffer_pool_limits(struct aws_allocator *allocator, void *ctx) {
    (void)allocator;
    (void)ctx;

    struct aws_s3_buffer_pool *buffer_pool = aws_s3_buffer_pool_new(allocator, MB_TO_BYTES(8), GB_TO_BYTES(1));

    struct aws_s3_buffer_pool_ticket *ticket1 = aws_s3_buffer_pool_reserve(buffer_pool, MB_TO_BYTES(64));
    ASSERT_NOT_NULL(ticket1);
    struct aws_byte_buf buf1 = aws_s3_buffer_pool_acquire_buffer(buffer_pool, ticket1);
    ASSERT_NOT_NULL(buf1.buffer);

    struct aws_s3_buffer_pool_ticket *tickets[6];
    for (size_t i = 0; i < 6; ++i) {
        tickets[i] = aws_s3_buffer_pool_reserve(buffer_pool, MB_TO_BYTES(128));
        ASSERT_NOT_NULL(tickets[i]);
        struct aws_byte_buf buf = aws_s3_buffer_pool_acquire_buffer(buffer_pool, tickets[i]);
        ASSERT_NOT_NULL(buf.buffer);
    }

    ASSERT_NULL(aws_s3_buffer_pool_reserve(buffer_pool, MB_TO_BYTES(128)));
    ASSERT_NULL(aws_s3_buffer_pool_reserve(buffer_pool, MB_TO_BYTES(96)));

    aws_s3_buffer_pool_remove_reservation_hold(buffer_pool);
    struct aws_s3_buffer_pool_ticket *ticket2 = aws_s3_buffer_pool_reserve(buffer_pool, MB_TO_BYTES(32));
    ASSERT_NOT_NULL(ticket2);
    struct aws_byte_buf buf2 = aws_s3_buffer_pool_acquire_buffer(buffer_pool, ticket2);
    ASSERT_NOT_NULL(buf2.buffer);

    for (size_t i = 0; i < 6; ++i) {
        aws_s3_buffer_pool_release_ticket(buffer_pool, tickets[i]);
    }

    aws_s3_buffer_pool_release_ticket(buffer_pool, ticket1);
    aws_s3_buffer_pool_release_ticket(buffer_pool, ticket2);

    aws_s3_buffer_pool_destroy(buffer_pool);

    return 0;
}
AWS_TEST_CASE(test_s3_buffer_pool_limits, s_test_s3_buffer_pool_limits)

static int s_test_s3_buffer_pool_trim(struct aws_allocator *allocator, void *ctx) {
    (void)allocator;
    (void)ctx;

    struct aws_s3_buffer_pool *buffer_pool = aws_s3_buffer_pool_new(allocator, MB_TO_BYTES(8), GB_TO_BYTES(1));

    struct aws_s3_buffer_pool_ticket *tickets[40];
    for (size_t i = 0; i < 40; ++i) {
        tickets[i] = aws_s3_buffer_pool_reserve(buffer_pool, MB_TO_BYTES(8));
        ASSERT_NOT_NULL(tickets[i]);
        struct aws_byte_buf buf = aws_s3_buffer_pool_acquire_buffer(buffer_pool, tickets[i]);
        ASSERT_NOT_NULL(buf.buffer);
    }

    struct aws_s3_buffer_pool_usage_stats stats_before = aws_s3_buffer_pool_get_usage(buffer_pool);

    for (size_t i = 0; i < 20; ++i) {
        aws_s3_buffer_pool_release_ticket(buffer_pool, tickets[i]);
    }

    aws_s3_buffer_pool_trim(buffer_pool);

    struct aws_s3_buffer_pool_usage_stats stats_after = aws_s3_buffer_pool_get_usage(buffer_pool);

    ASSERT_TRUE(stats_before.primary_num_blocks > stats_after.primary_num_blocks);

    for (size_t i = 20; i < 40; ++i) {
        aws_s3_buffer_pool_release_ticket(buffer_pool, tickets[i]);
    }

    aws_s3_buffer_pool_destroy(buffer_pool);

    return 0;
};
AWS_TEST_CASE(test_s3_buffer_pool_trim, s_test_s3_buffer_pool_trim)

static int s_test_s3_buffer_pool_reservation_hold(struct aws_allocator *allocator, void *ctx) {
    (void)allocator;
    (void)ctx;

    struct aws_s3_buffer_pool *buffer_pool = aws_s3_buffer_pool_new(allocator, MB_TO_BYTES(8), GB_TO_BYTES(1));

    struct aws_s3_buffer_pool_ticket *tickets[112];
    for (size_t i = 0; i < 112; ++i) {
        tickets[i] = aws_s3_buffer_pool_reserve(buffer_pool, MB_TO_BYTES(8));
        ASSERT_NOT_NULL(tickets[i]);
        struct aws_byte_buf buf = aws_s3_buffer_pool_acquire_buffer(buffer_pool, tickets[i]);
        ASSERT_NOT_NULL(buf.buffer);
    }

    ASSERT_NULL(aws_s3_buffer_pool_reserve(buffer_pool, MB_TO_BYTES(8)));

    ASSERT_TRUE(aws_s3_buffer_pool_has_reservation_hold(buffer_pool));

    for (size_t i = 0; i < 112; ++i) {
        aws_s3_buffer_pool_release_ticket(buffer_pool, tickets[i]);
    }

    ASSERT_NULL(aws_s3_buffer_pool_reserve(buffer_pool, MB_TO_BYTES(8)));

    aws_s3_buffer_pool_remove_reservation_hold(buffer_pool);

    struct aws_s3_buffer_pool_ticket *ticket = aws_s3_buffer_pool_reserve(buffer_pool, MB_TO_BYTES(8));
    ASSERT_NOT_NULL(ticket);

    aws_s3_buffer_pool_release_ticket(buffer_pool, ticket);

    aws_s3_buffer_pool_destroy(buffer_pool);

    return 0;
};
AWS_TEST_CASE(test_s3_buffer_pool_reservation_hold, s_test_s3_buffer_pool_reservation_hold)

static int s_test_s3_buffer_pool_too_small(struct aws_allocator *allocator, void *ctx) {
    (void)allocator;
    (void)ctx;

    struct aws_s3_buffer_pool *buffer_pool = aws_s3_buffer_pool_new(allocator, MB_TO_BYTES(8), MB_TO_BYTES(512));
    ASSERT_NULL(buffer_pool);
    ASSERT_INT_EQUALS(AWS_ERROR_S3_INVALID_MEMORY_LIMIT_CONFIG, aws_last_error());

    return 0;
};
AWS_TEST_CASE(test_s3_buffer_pool_too_small, s_test_s3_buffer_pool_too_small)

/* Sanity check that forced-buffer allocation works at all */
static int s_test_s3_buffer_pool_forced_buffer(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    const size_t chunk_size = MB_TO_BYTES(8);
    struct aws_s3_buffer_pool *buffer_pool = aws_s3_buffer_pool_new(allocator, chunk_size, GB_TO_BYTES(1));

    { /* Acquire forced buffer from primary storage */
        size_t acquire_size = chunk_size;
        struct aws_s3_buffer_pool_ticket *forced_ticket = NULL;
        struct aws_byte_buf forced_buf =
            aws_s3_buffer_pool_acquire_forced_buffer(buffer_pool, acquire_size, &forced_ticket);
        ASSERT_NOT_NULL(forced_ticket);
        ASSERT_UINT_EQUALS(acquire_size, forced_buf.capacity);
        ASSERT_UINT_EQUALS(0, forced_buf.len);

        struct aws_s3_buffer_pool_usage_stats stats = aws_s3_buffer_pool_get_usage(buffer_pool);
        ASSERT_UINT_EQUALS(acquire_size, stats.forced_used);
        ASSERT_UINT_EQUALS(acquire_size, stats.primary_used);
        ASSERT_UINT_EQUALS(0, stats.primary_reserved);
        aws_s3_buffer_pool_release_ticket(buffer_pool, forced_ticket);
    }

    { /* Acquire forced buffer from secondary storage */
        size_t acquire_size = aws_s3_buffer_pool_get_usage(buffer_pool).primary_cutoff + 1;
        struct aws_s3_buffer_pool_ticket *forced_ticket = NULL;
        struct aws_byte_buf forced_buf =
            aws_s3_buffer_pool_acquire_forced_buffer(buffer_pool, acquire_size, &forced_ticket);
        ASSERT_NOT_NULL(forced_ticket);
        ASSERT_UINT_EQUALS(acquire_size, forced_buf.capacity);
        ASSERT_UINT_EQUALS(0, forced_buf.len);

        struct aws_s3_buffer_pool_usage_stats stats = aws_s3_buffer_pool_get_usage(buffer_pool);
        ASSERT_UINT_EQUALS(acquire_size, stats.forced_used);
        ASSERT_UINT_EQUALS(acquire_size, stats.secondary_used);
        ASSERT_UINT_EQUALS(0, stats.secondary_reserved);
        aws_s3_buffer_pool_release_ticket(buffer_pool, forced_ticket);
    }

    /* Assert stats go back down after tickets released */
    struct aws_s3_buffer_pool_usage_stats stats = aws_s3_buffer_pool_get_usage(buffer_pool);
    ASSERT_UINT_EQUALS(0, stats.forced_used);
    ASSERT_UINT_EQUALS(0, stats.primary_used);
    ASSERT_UINT_EQUALS(0, stats.secondary_used);

    aws_s3_buffer_pool_destroy(buffer_pool);
    return 0;
}
AWS_TEST_CASE(test_s3_buffer_pool_forced_buffer, s_test_s3_buffer_pool_forced_buffer)

/* Test that we can still acquire forced buffers, even after pool has a reservation-hold */
static int s_test_s3_buffer_pool_forced_buffer_after_reservation_hold(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    const size_t chunk_size = MB_TO_BYTES(8);
    struct aws_s3_buffer_pool *buffer_pool = aws_s3_buffer_pool_new(allocator, chunk_size, GB_TO_BYTES(1));

    /* Reserve normal tickets until pool has reservation-hold */
    struct aws_array_list normal_tickets;
    aws_array_list_init_dynamic(&normal_tickets, allocator, 1, sizeof(struct aws_s3_buffer_pool_ticket *));
    while (aws_s3_buffer_pool_has_reservation_hold(buffer_pool) == false) {
        struct aws_s3_buffer_pool_ticket *normal_ticket = aws_s3_buffer_pool_reserve(buffer_pool, chunk_size);
        if (normal_ticket != NULL) {
            aws_array_list_push_back(&normal_tickets, &normal_ticket);
        }
    }

    /* Assert we can still get a forced-buffer */
    struct aws_s3_buffer_pool_ticket *forced_ticket_1 = NULL;
    struct aws_byte_buf forced_buf_1 =
        aws_s3_buffer_pool_acquire_forced_buffer(buffer_pool, chunk_size, &forced_ticket_1);
    ASSERT_NOT_NULL(forced_ticket_1);
    ASSERT_UINT_EQUALS(chunk_size, forced_buf_1.capacity);

    /* Assert we can still acquire buffers for all those normal reservations */
    for (size_t i = 0; i < aws_array_list_length(&normal_tickets); ++i) {
        struct aws_s3_buffer_pool_ticket *normal_ticket;
        aws_array_list_get_at(&normal_tickets, &normal_ticket, i);
        struct aws_byte_buf normal_buf = aws_s3_buffer_pool_acquire_buffer(buffer_pool, normal_ticket);
        ASSERT_UINT_EQUALS(chunk_size, normal_buf.capacity);
    }

    /* Assert we can still get a forced-buffer */
    struct aws_s3_buffer_pool_ticket *forced_ticket_2 = NULL;
    struct aws_byte_buf forced_buf_2 =
        aws_s3_buffer_pool_acquire_forced_buffer(buffer_pool, chunk_size, &forced_ticket_2);
    ASSERT_NOT_NULL(forced_ticket_2);
    ASSERT_UINT_EQUALS(chunk_size, forced_buf_2.capacity);

    /* Cleanup */
    for (size_t i = 0; i < aws_array_list_length(&normal_tickets); ++i) {
        struct aws_s3_buffer_pool_ticket *normal_ticket;
        aws_array_list_get_at(&normal_tickets, &normal_ticket, i);
        aws_s3_buffer_pool_release_ticket(buffer_pool, normal_ticket);
    }
    aws_array_list_clean_up(&normal_tickets);

    aws_s3_buffer_pool_release_ticket(buffer_pool, forced_ticket_1);
    aws_s3_buffer_pool_release_ticket(buffer_pool, forced_ticket_2);
    aws_s3_buffer_pool_destroy(buffer_pool);
    return 0;
}
AWS_TEST_CASE(
    test_s3_buffer_pool_forced_buffer_after_reservation_hold,
    s_test_s3_buffer_pool_forced_buffer_after_reservation_hold)

/* Test that some normal tickets can still be reserved, even if forced-buffer usage is huge.
 * This is important because, if either system can stop the other from working, we risk deadlock. */
static int s_test_s3_buffer_pool_forced_buffer_wont_stop_reservations(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    const size_t chunk_size = MB_TO_BYTES(8);
    const size_t mem_limit = GB_TO_BYTES(1);
    struct aws_s3_buffer_pool *buffer_pool = aws_s3_buffer_pool_new(allocator, chunk_size, mem_limit);

    /* Skip test if this machine can't do enormous allocations */
    void *try_large_alloc = malloc(mem_limit);
    if (try_large_alloc == NULL) {
        aws_s3_buffer_pool_destroy(buffer_pool);
        return AWS_OP_SKIP;
    }
    free(try_large_alloc);

    /* Allocate enormous forced buffer */
    struct aws_s3_buffer_pool_ticket *forced_ticket = NULL;
    struct aws_byte_buf forced_buf = aws_s3_buffer_pool_acquire_forced_buffer(buffer_pool, mem_limit, &forced_ticket);
    ASSERT_NOT_NULL(forced_ticket);
    ASSERT_UINT_EQUALS(mem_limit, forced_buf.capacity);

    /* Assert we can still reserve a normal ticket & allocate a normal buffer */
    struct aws_s3_buffer_pool_ticket *normal_ticket = aws_s3_buffer_pool_reserve(buffer_pool, chunk_size);
    ASSERT_NOT_NULL(normal_ticket);
    struct aws_byte_buf normal_buffer = aws_s3_buffer_pool_acquire_buffer(buffer_pool, normal_ticket);
    ASSERT_UINT_EQUALS(chunk_size, normal_buffer.capacity);
    aws_s3_buffer_pool_release_ticket(buffer_pool, normal_ticket);

    /* Cleanup */
    aws_s3_buffer_pool_release_ticket(buffer_pool, forced_ticket);
    aws_s3_buffer_pool_destroy(buffer_pool);
    return 0;
}
AWS_TEST_CASE(
    test_s3_buffer_pool_forced_buffer_wont_stop_reservations,
    s_test_s3_buffer_pool_forced_buffer_wont_stop_reservations)
