/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/s3/private/s3_buffer_pool.h>
#include <aws/s3/private/s3_util.h>

#include <aws/common/thread.h>
#include <aws/testing/aws_test_harness.h>

#define NUM_TEST_ALLOCS 100
#define NUM_TEST_THREADS 8

struct pool_thread_test_data {
    struct aws_s3_buffer_pool *pool;
    uint32_t thread_idx;
};

static void s_thread_test(
    struct aws_allocator *allocator,
    void (*thread_fn)(void *),
    struct aws_s3_buffer_pool *pool) {
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
    struct aws_s3_buffer_pool *test_allocator = ((struct pool_thread_test_data *)user_data)->pool;

    void *allocs[NUM_TEST_ALLOCS];
    for (size_t count = 0; count < NUM_TEST_ALLOCS / NUM_TEST_THREADS; ++count) {
        size_t size = 8 * 1024 * 1024;
        void *alloc = aws_s3_buffer_pool_acquire(test_allocator, size);
        AWS_FATAL_ASSERT(alloc);
        allocs[count] = alloc;
    }

    for (size_t count = 0; count < NUM_TEST_ALLOCS / NUM_TEST_THREADS; ++count) {
        void *alloc = allocs[count];
        aws_s3_buffer_pool_release(test_allocator, alloc);
    }
}

static int s_s3_buffer_pool_threaded_allocs_and_frees(struct aws_allocator *allocator, void *ctx) {
    (void)allocator;
    (void)ctx;

    struct aws_s3_buffer_pool *buffer_pool = aws_s3_buffer_pool_new(allocator, MB_TO_BYTES(128), GB_TO_BYTES(4));

    s_thread_test(allocator, s_threaded_alloc_worker, buffer_pool);

    aws_s3_buffer_pool_destroy(buffer_pool);

    return 0;
}
AWS_TEST_CASE(s3_buffer_pool_threaded_allocs_and_frees, s_s3_buffer_pool_threaded_allocs_and_frees)

static int s_s3_buffer_pool_limits(struct aws_allocator *allocator, void *ctx) {
    (void)allocator;
    (void)ctx;

    struct aws_s3_buffer_pool *buffer_pool = aws_s3_buffer_pool_new(allocator, MB_TO_BYTES(128), GB_TO_BYTES(1));

    void *ptr1 = aws_s3_buffer_pool_acquire(buffer_pool, MB_TO_BYTES(64));
    ASSERT_NOT_NULL(ptr1);

    void *ptrs[7];
    for(size_t i = 0; i < 7; ++i) {
        ptrs[i] = aws_s3_buffer_pool_acquire(buffer_pool, MB_TO_BYTES(128));
        ASSERT_NOT_NULL(ptrs[i]);
    }

    ASSERT_NULL(aws_s3_buffer_pool_acquire(buffer_pool, MB_TO_BYTES(128)));
    ASSERT_NULL(aws_s3_buffer_pool_acquire(buffer_pool, MB_TO_BYTES(96)));

    void *ptr2 = aws_s3_buffer_pool_acquire(buffer_pool, MB_TO_BYTES(32));
    ASSERT_NOT_NULL(ptr2);

    for(size_t i = 0; i < 7; ++i) {
        aws_s3_buffer_pool_release(buffer_pool, ptrs[i]);
    }

    aws_s3_buffer_pool_release(buffer_pool, ptr1);
    aws_s3_buffer_pool_release(buffer_pool, ptr2);

    aws_s3_buffer_pool_destroy(buffer_pool);

    return 0;
}
AWS_TEST_CASE(s3_buffer_pool_limits, s_s3_buffer_pool_limits)
