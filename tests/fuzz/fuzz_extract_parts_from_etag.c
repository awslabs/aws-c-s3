/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/s3/private/s3_util.h>

#include <aws/common/byte_buf.h>
#include <aws/common/common.h>
#include <aws/s3/s3.h>
#include <aws/testing/aws_test_harness.h>

#include <stddef.h>
#include <stdint.h>

/**
 * Fuzz test for aws_s3_extract_parts_from_etag function.
 *
 * This function parses S3 ETag headers to extract the number of parts in a multipart upload.
 * ETags can have two formats:
 * 1. Single-part: "abc123def456" (no dash) -> 1 part
 * 2. Multipart: "abc123def456-5" (hash-parts) -> 5 parts
 *
 * The fuzzer tests:
 * - Empty strings
 * - Strings with quotes and spaces
 * - Valid single-part ETags
 * - Valid multipart ETags
 * - Invalid formats (multiple dashes, non-numeric parts count, etc.)
 * - Edge cases (zero parts, exceeding max parts)
 */

/* Static initialization to avoid memory leaks in fuzzing */
static bool s_s3_library_initialized = false;

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    /* Initialize S3 library once */
    if (!s_s3_library_initialized) {
        struct aws_allocator *allocator = aws_default_allocator();
        aws_s3_library_init(allocator);
        s_s3_library_initialized = true;
    }

    /* Create a byte cursor from the fuzz input */
    struct aws_byte_cursor etag_cursor = aws_byte_cursor_from_array(data, size);

    uint32_t num_parts = 0;
    int result = aws_s3_extract_parts_from_etag(etag_cursor, &num_parts);

    if (result == AWS_OP_SUCCESS) {
        /* Validate successful results */
        AWS_FATAL_ASSERT(num_parts >= 1);
        AWS_FATAL_ASSERT(num_parts <= g_s3_max_num_upload_parts);

        /* Additional validation: if we successfully parsed, verify the format
         * Note: We don't validate the relationship between dash count and num_parts
         * because the function's parsing logic is more complex. For example:
         * - "-1" has one dash but may be treated as invalid or single-part
         * - "abc-" has one dash but the parts count is empty
         * The function handles these edge cases internally, so we just validate
         * that the returned num_parts is within valid bounds.
         */
    } else {
        /* Verify that failures set appropriate error codes */
        int error = aws_last_error();
        AWS_FATAL_ASSERT(error == AWS_ERROR_INVALID_ARGUMENT || error == AWS_ERROR_UNKNOWN);
    }

    return 0;
}
