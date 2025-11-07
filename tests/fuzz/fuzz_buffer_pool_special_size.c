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
 * Fuzz test for buffer pool with special sizes.
 * Tests the complete flow of:
 * 1. Adding special sizes to the buffer pool
 * 2. Reserving buffers with a mix of special sizes and random sizes
 * 3. Verifying usage statistics match expectations
 * 4. Acquiring buffers from tickets
 * 5. Releasing buffers and verifying cleanup
 */

#define MAX_SPECIAL_SIZES 5
#define MAX_RESERVATIONS 20

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

    /* Create buffer pool with reasonable defaults */
    size_t part_size = MB_TO_BYTES(8);
    size_t memory_limit = GB_TO_BYTES(2);
    struct aws_s3_buffer_pool *buffer_pool = aws_s3_default_buffer_pool_new(
        allocator, (struct aws_s3_buffer_pool_config){.part_size = part_size, .memory_limit = memory_limit});

    AWS_FATAL_ASSERT(buffer_pool);
    struct aws_s3_default_buffer_pool *pool = buffer_pool->impl;
    /* Hack to keep the special blocks alive from the trim, so that we can be sure tracking the stats correctly. */
    pool->force_keeping_special_blocks = true;
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

    /* Track reservations - some will be kept, some released immediately within the loop */
    struct aws_future_s3_buffer_ticket *futures[MAX_RESERVATIONS] = {0};
    struct aws_s3_buffer_ticket *tickets[MAX_RESERVATIONS] = {0};
    size_t num_kept = 0; /* Track how many we're keeping for later release */

    /* Track expected usage for validation */
    size_t expected_primary_reserved = 0;
    size_t expected_primary_used = 0;
    size_t expected_secondary_reserved = 0;
    size_t expected_secondary_used = 0;
    size_t expected_special_reserved = 0;
    size_t expected_special_used = 0;

    /*
     * Stress test: reserve/acquire/release cycle within loop
     * Pattern: reserve → acquire → maybe release immediately (50% chance)
     * This creates heavy churn and tests state management
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
        bool is_special = false;
        bool is_primary = false;
        bool is_secondary = false;

        uint8_t size_type = reservation_type % 3;

        if (size_type == 0) {
            /* Special size allocation - explicitly choose a special size */
            size_t special_idx = size_value % valid_special_sizes;
            reservation_size = special_sizes[special_idx];
            is_special = true;
        } else if (size_type == 1) {
            /* Primary storage allocation (below primary_cutoff) */
            reservation_size = 1024 + (size_value % (primary_cutoff - 1024));
            is_primary = true;
        } else {
            /* Secondary storage allocation (above primary_cutoff, below smallest special size) */
            size_t secondary_range = special_sizes[0] - primary_cutoff - 1;
            if (secondary_range > 0) {
                reservation_size = primary_cutoff + 1 + (size_value % secondary_range);
                is_secondary = true;
            } else {
                /* Not enough space for secondary, use primary instead */
                reservation_size = 1024 + (size_value % (primary_cutoff - 1024));
                is_primary = true;
            }
        }

        /*
         * CRITICAL: Check if randomly generated size accidentally matches a special size!
         * If so, the buffer pool will treat it as special, not secondary/primary.
         * We must update our flags to match what the pool will actually do.
         */
        if (!is_special) {
            for (size_t j = 0; j < valid_special_sizes; ++j) {
                if (reservation_size == special_sizes[j]) {
                    /* Randomly hit a special size - update flags to match buffer pool behavior */
                    is_special = true;
                    is_primary = false;
                    is_secondary = false;
                    break;
                }
            }
        }

        /* Reserve */
        struct aws_future_s3_buffer_ticket *future = aws_s3_default_buffer_pool_reserve(
            buffer_pool, (struct aws_s3_buffer_pool_reserve_meta){.size = reservation_size});

        AWS_FATAL_ASSERT(future);

        /* Check if reservation completed */
        if (!aws_future_s3_buffer_ticket_is_done(future)) {
            /* Hit memory limit - release the pending future to keep exact tracking */
            aws_future_s3_buffer_ticket_release(future);
            break;
        }

        /* Check for errors */
        int error = aws_future_s3_buffer_ticket_get_error(future);
        AWS_FATAL_ASSERT(error == AWS_OP_SUCCESS);

        /* Update expected reserved based on type we determined earlier (including accidental special size check) */
        if (is_special) {
            expected_special_reserved += reservation_size;
        } else if (is_primary) {
            expected_primary_reserved += reservation_size;
        } else if (is_secondary) {
            expected_secondary_reserved += reservation_size;
        }

        /* Check #1: Verify stats after reserve match expectations */
        struct aws_s3_default_buffer_pool_usage_stats after_reserve_stats =
            aws_s3_default_buffer_pool_get_usage(buffer_pool);

        if (is_special) {
            AWS_FATAL_ASSERT(after_reserve_stats.special_blocks_reserved == expected_special_reserved);
        } else if (is_primary) {
            AWS_FATAL_ASSERT(after_reserve_stats.primary_reserved == expected_primary_reserved);
        } else if (is_secondary) {
            AWS_FATAL_ASSERT(after_reserve_stats.secondary_reserved == expected_secondary_reserved);
        }

        /* Acquire the ticket */
        struct aws_s3_buffer_ticket *ticket = aws_future_s3_buffer_ticket_get_result_by_move(future);
        AWS_FATAL_ASSERT(ticket != NULL);

        /* Claim the buffer to verify it works */
        struct aws_byte_buf buf = aws_s3_buffer_ticket_claim(ticket);
        AWS_FATAL_ASSERT(buf.buffer != NULL);
        AWS_FATAL_ASSERT(buf.capacity >= reservation_size);

        /* Update expected used and reserved after acquire */
        if (is_special) {
            expected_special_used += reservation_size;
            expected_special_reserved -= reservation_size;
        } else if (is_primary) {
            expected_primary_used += reservation_size;
            expected_primary_reserved -= reservation_size;
        } else if (is_secondary) {
            expected_secondary_used += reservation_size;
            expected_secondary_reserved -= reservation_size;
        }

        /* Check #2: Verify stats after acquire match expectations */
        struct aws_s3_default_buffer_pool_usage_stats after_acquire_stats =
            aws_s3_default_buffer_pool_get_usage(buffer_pool);

        if (is_special) {
            AWS_FATAL_ASSERT(after_acquire_stats.special_blocks_used >= expected_special_used);
            AWS_FATAL_ASSERT(after_acquire_stats.special_blocks_reserved >= expected_special_reserved);
        } else if (is_primary) {
            AWS_FATAL_ASSERT(after_acquire_stats.primary_used >= expected_primary_used);
            AWS_FATAL_ASSERT(after_acquire_stats.primary_reserved >= expected_primary_reserved);
        } else if (is_secondary) {
            AWS_FATAL_ASSERT(after_acquire_stats.secondary_used >= expected_secondary_used);
            AWS_FATAL_ASSERT(after_acquire_stats.secondary_reserved >= expected_secondary_reserved);
        }

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

            /* Update expected used after release */
            if (is_special) {
                expected_special_used -= reservation_size;
            } else if (is_primary) {
                expected_primary_used -= reservation_size;
            } else if (is_secondary) {
                expected_secondary_used -= reservation_size;
            }

            /* Check #3: Verify stats after release match expectations */
            struct aws_s3_default_buffer_pool_usage_stats after_release_stats =
                aws_s3_default_buffer_pool_get_usage(buffer_pool);

            if (is_special) {
                AWS_FATAL_ASSERT(after_release_stats.special_blocks_used >= expected_special_used);
            } else if (is_primary) {
                AWS_FATAL_ASSERT(after_release_stats.primary_used >= expected_primary_used);
            } else if (is_secondary) {
                AWS_FATAL_ASSERT(after_release_stats.secondary_used >= expected_secondary_used);
            }
        } else {
            /* Keep for later release */
            tickets[num_kept++] = ticket;
        }
    }

    /* Release all tickets and futures */
    for (size_t i = 0; i < MAX_RESERVATIONS; ++i) {
        if (tickets[i] != NULL) {
            aws_s3_buffer_ticket_release(tickets[i]);
            tickets[i] = NULL;
        }
        if (futures[i] != NULL) {
            aws_future_s3_buffer_ticket_release(futures[i]);
            futures[i] = NULL;
        }
    }

    /* Check final stats after release */
    struct aws_s3_default_buffer_pool_usage_stats final_stats = aws_s3_default_buffer_pool_get_usage(buffer_pool);

    /* Verify everything is released */
    AWS_FATAL_ASSERT(final_stats.primary_used == 0);
    AWS_FATAL_ASSERT(final_stats.primary_reserved == 0);
    AWS_FATAL_ASSERT(final_stats.secondary_used == 0);
    AWS_FATAL_ASSERT(final_stats.secondary_reserved == 0);
    AWS_FATAL_ASSERT(final_stats.special_blocks_used == 0);
    AWS_FATAL_ASSERT(final_stats.special_blocks_reserved == 0);
    AWS_FATAL_ASSERT(final_stats.forced_used == 0);

    /*
     * Note: We don't assert special_blocks_allocated > 0 because the loop might not have
     * allocated any special sizes depending on the fuzzer input pattern
     */

    /* Clean up buffer pool */
    aws_s3_default_buffer_pool_destroy(buffer_pool);

cleanup:
    /* Check for memory leaks */
    AWS_FATAL_ASSERT(aws_mem_tracer_count(allocator) == 0);
    allocator = aws_mem_tracer_destroy(allocator);

    return 0;
}

AWS_EXTERN_C_END
