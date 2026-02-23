/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/allocator.h>
#include <aws/common/byte_buf.h>
#include <aws/common/logging.h>
#include <aws/s3/private/s3_util.h>
#include <aws/testing/aws_test_harness.h>

#include <inttypes.h>
#include <stdint.h>

/**
 * Combined fuzz test for optimal range size calculation functions:
 * 1. aws_s3_calculate_client_optimal_range_size
 * 2. aws_s3_calculate_request_optimal_range_size
 *
 * This tests the complete flow of calculating optimal range sizes for S3 operations.
 */

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

    /* Need at least 21 bytes of input:
     * - 8 bytes for memory_limit_in_bytes (uint64_t)
     * - 4 bytes for max_connections (uint32_t)
     * - 8 bytes for estimated_object_stored_part_size (uint64_t)
     * - 1 byte for is_express (bool)
     */
    if (size < 21) {
        goto cleanup;
    }

    struct aws_byte_cursor input = aws_byte_cursor_from_array(data, size);

    /* Extract inputs for aws_s3_calculate_client_optimal_range_size */
    uint64_t memory_limit_in_bytes = 0;
    aws_byte_cursor_read_be64(&input, &memory_limit_in_bytes);

    uint32_t max_connections = 0;
    aws_byte_cursor_read_be32(&input, &max_connections);

    /* Extract inputs for aws_s3_calculate_request_optimal_range_size */
    uint64_t estimated_object_stored_part_size = 0;
    aws_byte_cursor_read_be64(&input, &estimated_object_stored_part_size);

    uint8_t is_express_byte = 0;
    aws_byte_cursor_read_u8(&input, &is_express_byte);
    bool is_express = (is_express_byte % 2) == 1;

    /* Test 1: aws_s3_calculate_client_optimal_range_size */
    uint64_t client_optimal_range_size = 0;
    int result1 =
        aws_s3_calculate_client_optimal_range_size(memory_limit_in_bytes, max_connections, &client_optimal_range_size);

    if (memory_limit_in_bytes == 0 || max_connections == 0) {
        AWS_FATAL_ASSERT(result1 == AWS_OP_ERR);
        AWS_FATAL_ASSERT(aws_last_error() == AWS_ERROR_INVALID_ARGUMENT);
        goto cleanup; /* Can't proceed to test 2 without valid client_optimal_range_size */
    }

    AWS_FATAL_ASSERT(result1 == AWS_OP_SUCCESS);
    AWS_FATAL_ASSERT(client_optimal_range_size > 0);

    const uint64_t max_part_size = g_default_max_part_size;      /* 5 GiB */
    const uint64_t min_part_size = g_default_part_size_fallback; /* 8 MiB */

    AWS_FATAL_ASSERT(client_optimal_range_size >= min_part_size);
    AWS_FATAL_ASSERT(client_optimal_range_size <= max_part_size);
    /* Note: client_optimal_range_size may exceed memory_limit_in_bytes when the minimum
     * constraint (8 MiB) is applied. This is intentional to maintain S3's minimum part size. */

    /* Verify the calculation formula */
    uint64_t memory_constrained_size = memory_limit_in_bytes / max_connections / 3;
    if (memory_constrained_size < min_part_size) {
        AWS_FATAL_ASSERT(client_optimal_range_size == min_part_size);
    } else if (memory_constrained_size > max_part_size) {
        AWS_FATAL_ASSERT(client_optimal_range_size == max_part_size);
    } else {
        AWS_FATAL_ASSERT(client_optimal_range_size == memory_constrained_size);
    }

    /* Test 2: aws_s3_calculate_request_optimal_range_size */
    uint64_t request_optimal_range_size = 0;
    int result2 = aws_s3_calculate_request_optimal_range_size(
        client_optimal_range_size, estimated_object_stored_part_size, is_express, &request_optimal_range_size);

    AWS_FATAL_ASSERT(result2 == AWS_OP_SUCCESS);
    AWS_FATAL_ASSERT(request_optimal_range_size > 0);
    AWS_FATAL_ASSERT(request_optimal_range_size <= max_part_size);

    /* Verify S3 Express constraints */
    if (is_express) {
        const uint64_t express_max = 134217728ULL; /* 128 MiB */
        AWS_FATAL_ASSERT(request_optimal_range_size <= express_max);
    } else {
        const uint64_t standard_max = 2147483648ULL; /* 2 GiB */
        AWS_FATAL_ASSERT(request_optimal_range_size <= standard_max);
    }

    /* Verify relationship between client and request optimal sizes */
    const uint64_t default_part_size_fallback = g_default_part_size_fallback; /* 8 MiB */
    if (estimated_object_stored_part_size > 0 && estimated_object_stored_part_size < client_optimal_range_size &&
        client_optimal_range_size >= default_part_size_fallback) {
        /* When client size is >= fallback, output should be <= client_optimal_range_size */
        AWS_FATAL_ASSERT(request_optimal_range_size <= client_optimal_range_size);
    }

cleanup:
    /* Check for memory leaks */
    AWS_FATAL_ASSERT(aws_mem_tracer_count(allocator) == 0);
    allocator = aws_mem_tracer_destroy(allocator);

    return 0;
}

AWS_EXTERN_C_END
