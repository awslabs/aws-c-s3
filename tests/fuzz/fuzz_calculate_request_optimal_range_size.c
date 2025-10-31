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
 * Fuzz test for aws_s3_calculate_request_optimal_range_size function.
 *
 * This function calculates the optimal range size for S3 requests based on:
 * - client_optimal_range_size: The client's optimal range size
 * - estimated_object_stored_part_size: The estimated part size of the stored object
 * - is_express: Whether this is an S3 Express bucket
 *
 * The function should handle:
 * - Various combinations of input values
 * - Edge cases (0, very large values, etc.)
 * - Both S3 Express and standard S3 buckets
 * - Proper bounds checking and validation
 */

AWS_EXTERN_C_BEGIN

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    /* Setup allocator */
    struct aws_allocator *allocator = aws_mem_tracer_new(aws_default_allocator(), NULL, AWS_MEMTRACE_BYTES, 0);

    /* Enable logging for debugging */
    struct aws_logger logger;
    struct aws_logger_standard_options log_options = {
        .level = AWS_LL_TRACE,
        .file = stdout,
    };
    aws_logger_init_standard(&logger, allocator, &log_options);
    aws_logger_set(&logger);

    /* Init S3 library */
    aws_s3_library_init(aws_default_allocator());

    /* Need at least 17 bytes of input:
     * - 8 bytes for client_optimal_range_size (uint64_t)
     * - 8 bytes for estimated_object_stored_part_size (uint64_t)
     * - 1 byte for is_express (bool)
     */
    if (size < 17) {
        goto cleanup;
    }

    struct aws_byte_cursor input = aws_byte_cursor_from_array(data, size);

    /* Extract client_optimal_range_size (8 bytes) */
    uint64_t client_optimal_range_size = 0;
    aws_byte_cursor_read_be64(&input, &client_optimal_range_size);

    /* Extract estimated_object_stored_part_size (8 bytes) */
    uint64_t estimated_object_stored_part_size = 0;
    aws_byte_cursor_read_be64(&input, &estimated_object_stored_part_size);

    /* Extract is_express (1 byte) */
    uint8_t is_express_byte = 0;
    aws_byte_cursor_read_u8(&input, &is_express_byte);
    bool is_express = (is_express_byte % 2) == 1; /* Convert to boolean */

    /* Call the function under test */
    uint64_t out_request_optimal_range_size = 0;
    int result = aws_s3_calculate_request_optimal_range_size(
        client_optimal_range_size, estimated_object_stored_part_size, is_express, &out_request_optimal_range_size);

    /* Validate the result based on expected behavior */
    if (client_optimal_range_size == 0) {
        /* Should fail with invalid argument error */
        AWS_FATAL_ASSERT(result == AWS_OP_ERR);
        AWS_FATAL_ASSERT(aws_last_error() == AWS_ERROR_INVALID_ARGUMENT);
    } else {
        /* Should succeed */
        AWS_FATAL_ASSERT(result == AWS_OP_SUCCESS);

        /* Verify output is within reasonable bounds */
        AWS_FATAL_ASSERT(out_request_optimal_range_size > 0);

        /* Output should not exceed the maximum part size (5 GiB) */
        const uint64_t max_part_size = 5368709120ULL; /* 5 GiB */
        AWS_FATAL_ASSERT(out_request_optimal_range_size <= max_part_size);

        /* Output should be at least the minimum part size (5 MiB) when client_optimal_range_size is valid */
        const uint64_t min_part_size = 5242880ULL; /* 5 MiB */
        if (client_optimal_range_size >= min_part_size) {
            AWS_FATAL_ASSERT(out_request_optimal_range_size >= min_part_size);
        }

        /* For S3 Express, output should not exceed 128 MiB */
        if (is_express) {
            const uint64_t express_max = 134217728ULL; /* 128 MiB */
            AWS_FATAL_ASSERT(out_request_optimal_range_size <= express_max);
        } else {
            /* For standard S3, output should not exceed 2 GiB */
            const uint64_t standard_max = 2147483648ULL; /* 2 GiB */
            AWS_FATAL_ASSERT(out_request_optimal_range_size <= standard_max);
        }

        /* If estimated_object_stored_part_size is provided and less than client_optimal_range_size,
         * the output should be influenced by it (but still within bounds) */
        if (estimated_object_stored_part_size > 0 && estimated_object_stored_part_size < client_optimal_range_size) {
            /* Output should be <= client_optimal_range_size */
            AWS_FATAL_ASSERT(out_request_optimal_range_size <= client_optimal_range_size);
        }
    }

cleanup:
    /* Clean up */
    aws_logger_set(NULL);
    aws_logger_clean_up(&logger);

    atexit(aws_s3_library_clean_up);

    /* Check for memory leaks */
    AWS_FATAL_ASSERT(aws_mem_tracer_count(allocator) == 0);
    allocator = aws_mem_tracer_destroy(allocator);

    return 0;
}

AWS_EXTERN_C_END
