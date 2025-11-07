/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/allocator.h>
#include <aws/common/byte_buf.h>
#include <aws/common/logging.h>
#include <aws/s3/private/s3_default_buffer_pool.h>
#include <aws/s3/private/s3_util.h>
#include <aws/testing/aws_test_harness.h>

#include <inttypes.h>
#include <stdint.h>

/**
 * Fuzz test for buffer pool with special sizes - variant that keeps pending futures.
 * Similar to fuzz_buffer_pool_special_size.c but with key differences:
 * 1. When hitting memory limit, keeps the pending futures instead of releasing them
 * 2. Skips stats verification after reserve operations
 * 3. Ensures all futures and tickets are properly released at the end
 *
 * This variant tests the pool's ability to handle many pending reservations that
 * cannot complete due to memory limits.
 */

#define MAX_SPECIAL_SIZES 5
#define MAX_RESERVATIONS 10000

AWS_EXTERN_C_BEGIN

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    /* Setup allocator */
    struct aws_allocator *allocator = aws_mem_tracer_new(aws_default_allocator(), NULL, AWS_MEMTRACE_BYTES, 0);

    /* Init S3 library once (static to avoid repeated initialization) */
    static bool s3_library_initialized = false;
    if (!s3_library_initialized) {
        aws_s3_library_init(aws_default_allocator());
        s3_library_initialized = true;
    }

    /* Need at least 3 bytes of input:
     * - 1 byte for num_special_sizes
     * - 1 byte for num_reservations
     * - 1 byte for at least one size value
     */
    if (size < 3) {
        goto cleanup;
    }

    struct aws_byte_cursor input = aws_byte_cursor_from_array(data, size);

    /* Extract number of special sizes (0-5) */
    uint8_t num_special_sizes_raw = 0;
    aws_byte_cursor_read_u8(&input, &num_special_sizes_raw);
    size_t num_special_sizes = (num_special_sizes_raw % (MAX_SPECIAL_SIZES + 1));

    /* Extract number of reservations (1-20) */
    uint8_t num_reservations_raw = 0;
    aws_byte_cursor_read_u8(&input, &num_reservations_raw);
    size_t num_reservations = 1 + (num_reservations_raw % MAX_RESERVATIONS);

    /* Scale down counts based on available data for better fuzzing coverage */
    size_t available_for_special = input.len / 4;
    size_t available_for_reservations = input.len / 5;

    if (num_special_sizes > available_for_special) {
        num_special_sizes = available_for_special;
    }
    if (num_reservations > available_for_reservations) {
        num_reservations = available_for_reservations;
    }

    /* Create buffer pool with balanced memory limit to avoid OOM during fuzzing */
    size_t part_size = MB_TO_BYTES(8);
    size_t memory_limit = GB_TO_BYTES(1); /* 1GB - balanced between functionality and avoiding OOM */
    struct aws_s3_buffer_pool *buffer_pool = aws_s3_default_buffer_pool_new(
        allocator, (struct aws_s3_buffer_pool_config){.part_size = part_size, .memory_limit = memory_limit});

    AWS_FATAL_ASSERT(buffer_pool);
    /* Get initial stats */
    struct aws_s3_default_buffer_pool_usage_stats initial_stats = aws_s3_default_buffer_pool_get_usage(buffer_pool);
    size_t primary_cutoff = initial_stats.primary_cutoff;

    /* Add special sizes - consume as much input as available */
    size_t special_sizes[MAX_SPECIAL_SIZES] = {0};
    size_t valid_special_sizes = 0;
    size_t unique_special_sizes_added = 0;

    for (size_t i = 0; i < num_special_sizes && input.len >= 4; ++i) {
        uint32_t size_raw = 0;
        if (!aws_byte_cursor_read_be32(&input, &size_raw)) {
            break; /* Not enough bytes left */
        }

        /* Generate special size between primary_cutoff and 256MB */
        size_t special_size = primary_cutoff + 1 + (size_raw % MB_TO_BYTES(256));

        /* Check if this is a duplicate size */
        bool is_duplicate = false;
        for (size_t j = 0; j < valid_special_sizes; ++j) {
            if (special_sizes[j] == special_size) {
                is_duplicate = true;
                break;
            }
        }

        int result = aws_s3_buffer_pool_add_special_size(buffer_pool, special_size);
        AWS_FATAL_ASSERT(result == AWS_OP_SUCCESS);

        /* Track unique sizes (add_special_size is idempotent, so duplicates don't create new lists) */
        if (!is_duplicate) {
            unique_special_sizes_added++;
            special_sizes[valid_special_sizes++] = special_size;
        }
    }

    /* Skip test if no special sizes were added - this test is specifically for testing special sizes */
    if (valid_special_sizes == 0) {
        aws_s3_default_buffer_pool_destroy(buffer_pool);
        goto cleanup;
    }

    /* Verify special_blocks_num matches the number of unique special sizes we added */
    struct aws_s3_default_buffer_pool_usage_stats after_add_stats = aws_s3_default_buffer_pool_get_usage(buffer_pool);
    AWS_FATAL_ASSERT(after_add_stats.special_blocks_num == unique_special_sizes_added);

    /* Track reservations - keeping all futures (including pending ones) for cleanup */
    struct aws_future_s3_buffer_ticket *futures[MAX_RESERVATIONS] = {0};
    struct aws_s3_buffer_ticket *tickets[MAX_RESERVATIONS] = {0};
    size_t num_futures = 0;
    size_t num_tickets = 0;

    /*
     * Stress test: reserve/acquire/release cycle
     * KEY DIFFERENCE: Keep pending futures instead of releasing them immediately
     * This tests the pool's handling of many pending reservations
     */
    for (size_t i = 0; i < num_reservations && input.len >= 6; ++i) {
        /* Read 6 bytes: 1 for type, 4 for size, 1 for release decision */
        uint8_t reservation_type = 0;
        if (!aws_byte_cursor_read_u8(&input, &reservation_type)) {
            break;
        }

        uint32_t size_value = 0;
        if (!aws_byte_cursor_read_be32(&input, &size_value)) {
            break;
        }

        uint8_t release_immediately = 0;
        if (!aws_byte_cursor_read_u8(&input, &release_immediately)) {
            break;
        }

        /* Determine reservation size and type BEFORE reserving */
        size_t reservation_size = 0;
        uint8_t size_type = reservation_type % 3;

        if (size_type == 0) {
            /* Special size allocation - explicitly choose a special size */
            size_t special_idx = size_value % valid_special_sizes;
            reservation_size = special_sizes[special_idx];
        } else if (size_type == 1) {
            /* Primary storage allocation (below primary_cutoff) */
            reservation_size = 1024 + (size_value % (primary_cutoff - 1024));
        } else {
            /* Secondary storage allocation (above primary_cutoff, below smallest special size) */
            size_t secondary_range = special_sizes[0] - primary_cutoff - 1;
            if (secondary_range > 0) {
                reservation_size = primary_cutoff + 1 + (size_value % secondary_range);
            } else {
                /* Not enough space for secondary, use primary instead */
                reservation_size = 1024 + (size_value % (primary_cutoff - 1024));
            }
        }

        /* Reserve */
        struct aws_future_s3_buffer_ticket *future = aws_s3_default_buffer_pool_reserve(
            buffer_pool, (struct aws_s3_buffer_pool_reserve_meta){.size = reservation_size});

        AWS_FATAL_ASSERT(future);

        /* Check if reservation completed */
        if (!aws_future_s3_buffer_ticket_is_done(future)) {
            /*
             * KEY DIFFERENCE: Keep the pending future instead of releasing it
             * This allows us to test handling of many pending reservations
             */
            futures[num_futures++] = future;
            break; /* Stop trying to reserve more since we hit the limit */
        }

        /* Check for errors */
        int error = aws_future_s3_buffer_ticket_get_error(future);
        AWS_FATAL_ASSERT(error == AWS_OP_SUCCESS);

        /*
         * KEY DIFFERENCE: Skip stats verification after reserve
         * We're not tracking expected values in this variant
         */

        /* Acquire the ticket */
        struct aws_s3_buffer_ticket *ticket = aws_future_s3_buffer_ticket_get_result_by_move(future);
        AWS_FATAL_ASSERT(ticket != NULL);

        /* Claim the buffer to verify it works */
        struct aws_byte_buf buf = aws_s3_buffer_ticket_claim(ticket);
        AWS_FATAL_ASSERT(buf.buffer != NULL);
        AWS_FATAL_ASSERT(buf.capacity >= reservation_size);

        /* Store future for cleanup */
        futures[num_futures++] = future;

        /* Write to buffer to ensure it's valid */
        if (buf.capacity > 0) {
            buf.buffer[0] = (uint8_t)i;
            if (buf.capacity > 1) {
                buf.buffer[buf.capacity - 1] = (uint8_t)i;
            }
        }

        /* Decision: release immediately (50% chance) or keep for later */
        if ((release_immediately % 2) == 0) {
            /* Release immediately - creates churn */
            aws_s3_buffer_ticket_release(ticket);
        } else {
            /* Keep for later release */
            tickets[num_tickets++] = ticket;
        }
    }

    /* Release all tickets first */
    for (size_t i = 0; i < num_tickets; ++i) {
        if (tickets[i] != NULL) {
            aws_s3_buffer_ticket_release(tickets[i]);
            tickets[i] = NULL;
        }
    }

    /* Release all futures (including pending ones) */
    for (size_t i = 0; i < num_futures; ++i) {
        if (futures[i] != NULL) {
            aws_future_s3_buffer_ticket_release(futures[i]);
            futures[i] = NULL;
        }
    }

    /* Release special sizes */
    for (size_t j = 0; j < valid_special_sizes; ++j) {
        aws_s3_buffer_pool_release_special_size(buffer_pool, special_sizes[j]);
    }

    /* Force trim */
    aws_s3_buffer_pool_trim(buffer_pool);

    /* Check final stats after release */
    struct aws_s3_default_buffer_pool_usage_stats final_stats = aws_s3_default_buffer_pool_get_usage(buffer_pool);

    /* Verify everything is released */
    AWS_FATAL_ASSERT(final_stats.primary_allocated == 0);
    AWS_FATAL_ASSERT(final_stats.primary_used == 0);
    AWS_FATAL_ASSERT(final_stats.primary_reserved == 0);
    AWS_FATAL_ASSERT(final_stats.secondary_used == 0);
    AWS_FATAL_ASSERT(final_stats.secondary_reserved == 0);
    AWS_FATAL_ASSERT(final_stats.special_blocks_allocated == 0);
    AWS_FATAL_ASSERT(final_stats.special_blocks_used == 0);
    AWS_FATAL_ASSERT(final_stats.special_blocks_reserved == 0);
    AWS_FATAL_ASSERT(final_stats.forced_used == 0);

    /* Clean up buffer pool */
    aws_s3_default_buffer_pool_destroy(buffer_pool);

cleanup:
    /* Check for memory leaks */
    AWS_FATAL_ASSERT(aws_mem_tracer_count(allocator) == 0);
    allocator = aws_mem_tracer_destroy(allocator);
    return 0;
}

AWS_EXTERN_C_END
