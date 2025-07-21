#ifndef AWS_S3_CHECKSUM_CONTEXT_H
#define AWS_S3_CHECKSUM_CONTEXT_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "aws/s3/s3_client.h"
#include <aws/common/byte_buf.h>
#include <aws/common/ref_count.h>

struct aws_s3_meta_request_checksum_config_storage;

AWS_EXTERN_C_BEGIN

/**
 * Upload request checksum context that encapsulates all checksum-related state and behavior
 * for individual upload part requests. This replaces the complex tri-state buffer logic
 * with a cleaner approach. Uses reference counting for lifetime management since context
 * is transferred between functions.
 */
struct aws_s3_upload_request_checksum_context {
    struct aws_allocator *allocator;
    struct aws_ref_count ref_count;

    /* Configuration */
    enum aws_s3_checksum_algorithm algorithm;
    enum aws_s3_checksum_location location;

    struct aws_byte_buf base64_checksum;
    /* The checksum already be calculated or not. */
    bool checksum_calculated;

    /* Validation */
    size_t encoded_checksum_size;
};

/**
 * Create a new upload request checksum context from configuration and buffer parameters.
 * This function encapsulates all the complex logic for determining buffer state.
 * Returns with reference count of 1.
 *
 * @param allocator Memory allocator
 * @param checksum_config Meta request level checksum configuration (can be NULL)
 * @return New checksum context or NULL on error
 */
AWS_S3_API
struct aws_s3_upload_request_checksum_context *aws_s3_upload_request_checksum_context_new(
    struct aws_allocator *allocator,
    const struct aws_s3_meta_request_checksum_config_storage *checksum_config);

/**
 * Create a new upload request checksum context with an existing base64 encoded checksum value.
 * This is useful when resuming uploads or when the checksum is pre-calculated.
 * Returns with reference count of 1.
 *
 * @param allocator Memory allocator
 * @param checksum_config Meta request level checksum configuration (can be NULL)
 * @param existing_base64_checksum Pre-calculated checksum value as a byte cursor
 * @return New checksum context or NULL on error (e.g., if checksum size doesn't match algorithm)
 */
AWS_S3_API
struct aws_s3_upload_request_checksum_context *aws_s3_upload_request_checksum_context_new_with_existing_base64_checksum(
    struct aws_allocator *allocator,
    const struct aws_s3_meta_request_checksum_config_storage *checksum_config,
    struct aws_byte_cursor existing_base64_checksum);

/**
 * Acquire a reference to the upload request checksum context.
 * Use this when transferring ownership to another function/structure.
 *
 * @param context The checksum context to acquire
 * @return The same context pointer (for convenience)
 */
AWS_S3_API
struct aws_s3_upload_request_checksum_context *aws_s3_upload_request_checksum_context_acquire(
    struct aws_s3_upload_request_checksum_context *context);

/**
 * Release a reference to the upload request checksum context.
 * When the reference count reaches zero, the context will be destroyed.
 * Always returns NULL.
 *
 * @param context The checksum context to release
 */
AWS_S3_API
struct aws_s3_upload_request_checksum_context *aws_s3_upload_request_checksum_context_release(
    struct aws_s3_upload_request_checksum_context *context);

/**
 * Check if checksum calculation is needed based on context state.
 * Returns true if the context has a valid algorithm and the checksum has not been calculated yet.
 *
 * @param context The checksum context to check
 * @return true if checksum calculation is needed, false otherwise
 */
AWS_S3_API
bool aws_s3_upload_request_checksum_context_should_calculate(
    const struct aws_s3_upload_request_checksum_context *context);

/**
 * Check if checksum should be added to HTTP headers.
 * Returns true if the context has a valid algorithm and the location is set to header.
 *
 * @param context The checksum context to check
 * @return true if checksum should be added to headers, false otherwise
 */
AWS_S3_API
bool aws_s3_upload_request_checksum_context_should_add_header(
    const struct aws_s3_upload_request_checksum_context *context);

/**
 * Check if checksum should be added as trailer (aws-chunked encoding).
 * Returns true if the context has a valid algorithm and the location is set to trailer.
 *
 * @param context The checksum context to check
 * @return true if checksum should be added as trailer, false otherwise
 */
AWS_S3_API
bool aws_s3_upload_request_checksum_context_should_add_trailer(
    const struct aws_s3_upload_request_checksum_context *context);

/**
 * Get the checksum buffer to use for output.
 * Returns the internal buffer for storing the calculated checksum.
 *
 * @param context The checksum context
 * @return Pointer to the checksum buffer, or NULL if context is invalid
 */
AWS_S3_API
struct aws_byte_buf *aws_s3_upload_request_checksum_context_get_output_buffer(
    struct aws_s3_upload_request_checksum_context *context);

/**
 * Get a cursor to the current base64 encoded checksum value (for use in headers/trailers).
 * Returns an empty cursor if the checksum has not been calculated yet.
 *
 * @param context The checksum context
 * @return Byte cursor to the calculated checksum, or empty cursor if not available
 */
AWS_S3_API
struct aws_byte_cursor aws_s3_upload_request_checksum_context_get_checksum_cursor(
    const struct aws_s3_upload_request_checksum_context *context);

AWS_EXTERN_C_END

#endif /* AWS_S3_CHECKSUM_CONTEXT_H */
