#ifndef AWS_S3_CHECKSUMS_H
#define AWS_S3_CHECKSUMS_H
/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */
#include "aws/s3/s3_client.h"

/* TODO: consider moving the aws_checksum_stream to aws-c-checksum, and the rest about checksum headers and trailer to
 * aws-c-sdkutil. */

struct aws_s3_checksum;
struct aws_s3_upload_request_checksum_context;

/* List to check the checksum algorithm to use based on the priority. */
static const enum aws_s3_checksum_algorithm s_checksum_algo_priority_list[] = {
    AWS_SCA_CRC64NVME,
    AWS_SCA_CRC32C,
    AWS_SCA_CRC32,
    AWS_SCA_SHA1,
    AWS_SCA_SHA256,
};
AWS_STATIC_ASSERT(AWS_ARRAY_SIZE(s_checksum_algo_priority_list) == (AWS_SCA_END - AWS_SCA_INIT + 1));

struct aws_checksum_vtable {
    void (*destroy)(struct aws_s3_checksum *checksum);
    int (*update)(struct aws_s3_checksum *checksum, const struct aws_byte_cursor *buf);
    int (*finalize)(struct aws_s3_checksum *checksum, struct aws_byte_buf *out);
};

struct aws_s3_checksum {
    struct aws_allocator *allocator;
    struct aws_checksum_vtable *vtable;
    size_t digest_size;
    enum aws_s3_checksum_algorithm algorithm;
    bool good;
    union {
        struct aws_hash *hash;
        uint32_t crc_val_32bit;
        uint64_t crc_val_64bit;
    } impl;
};

struct aws_s3_meta_request_checksum_config_storage {
    struct aws_allocator *allocator;
    struct aws_byte_buf full_object_checksum;
    bool has_full_object_checksum;

    aws_s3_meta_request_full_object_checksum_fn *full_object_checksum_callback;
    void *user_data;

    enum aws_s3_checksum_location location;
    enum aws_s3_checksum_algorithm checksum_algorithm;
    bool validate_response_checksum;
    struct {
        bool crc64nvme;
        bool crc32c;
        bool crc32;
        bool sha1;
        bool sha256;
    } response_checksum_algorithms;
};

/**
 * Helper stream that takes in a stream and the checksum context to help finalize the checksum from the underlying
 * stream.
 * The context will be only finalized when the checksum stream has read to the end of stream.
 *
 * Note: seek this stream will immediately fail, as it would prevent an accurate calculation of the
 * checksum.
 *
 * @param allocator
 * @param existing_stream The real content to read from. Destroying the checksum stream destroys the existing stream.
 *                        outputs the checksum of existing stream to checksum_output upon destruction. Will be kept
 *                        alive by the checksum stream
 * @param context         Checksum context to keep and get checksum requirements from.
 */
AWS_S3_API
struct aws_input_stream *aws_checksum_stream_new_with_context(
    struct aws_allocator *allocator,
    struct aws_input_stream *existing_stream,
    struct aws_s3_upload_request_checksum_context *context);

/**
 * Helper stream that takes in a stream to keep track of the checksum of the underlying stream during read.
 * Invoke `aws_checksum_stream_finalize_checksum` to get the checksum of the data has been read so far.
 *
 * Note: seek this stream will immediately fail, as it would prevent an accurate calculation of the
 * checksum.
 *
 * @param allocator
 * @param existing_stream The real content to read from. Destroying the checksum stream destroys the existing stream.
 *                        outputs the checksum of existing stream to checksum_output upon destruction. Will be kept
 *                        alive by the checksum stream
 * @param algorithm       Checksum algorithm to use.
 */
AWS_S3_API
struct aws_input_stream *aws_checksum_stream_new(
    struct aws_allocator *allocator,
    struct aws_input_stream *existing_stream,
    enum aws_s3_checksum_algorithm algorithm);

/**
 * Finalize the checksum has read so far to the output checksum buf with base64 encoding.
 * Not thread safe.
 */
AWS_S3_API
int aws_checksum_stream_finalize_checksum(struct aws_input_stream *checksum_stream, struct aws_byte_buf *checksum_buf);

/**
 * TODO: properly support chunked encoding.
 * Creates a chunked encoding stream that wraps an existing stream and adds checksum trailers.
 *
 * This function creates a stream that:
 * 1. Encodes the input stream wraps the existing_stream with aws-chunked encoded.
 * 2. Calculates a checksum of the stream content (if not already calculated)
 * 3. Appends the checksum as a trailer at the end of the aws-chunked stream
 *
 * Note: This stream does not support seeking operations, as seeking would prevent
 * accurate checksum calculation and corrupt the chunked encoding format.
 *
 * @param allocator         Memory allocator to use for stream creation and internal buffers
 * @param existing_stream   The input stream to be chunked and checksummed. This stream
 *                          will be acquired by the chunk stream and released when the
 *                          chunk stream is destroyed. Must not be NULL.
 * @param checksum_context  Context containing checksum configuration and state. Must not be NULL.
 *                          The context contains:
 *                          - algorithm: The checksum algorithm to use (CRC32, CRC32C, etc.)
 *                          - base64_checksum: Buffer for the calculated checksum result
 *                          - checksum_calculated: Whether checksum is pre-calculated or needs calculation
 *                          - encoded_checksum_size: Expected size of the base64-encoded checksum
 *
 *                          If checksum_calculated is false, the stream will wrap existing_stream
 *                          with a checksum stream to calculate the checksum during reading.
 *                          If checksum_calculated is true, the existing checksum value will be used.
 *
 * @return A new input stream that provides chunked encoding with checksum trailers,
 *         or NULL if creation fails. The returned stream must be released with
 *         aws_input_stream_release() when no longer needed.
 *
 * @note The total length of the returned stream includes:
 *       - Chunk size header (hex representation + \r\n)
 *       - Original stream content
 *       - Final chunk marker (0\r\n or \r\n0\r\n)
 *       - Checksum trailer (header name + : + base64 checksum + \r\n\r\n)
 */
AWS_S3_API
struct aws_input_stream *aws_chunk_stream_new(
    struct aws_allocator *allocator,
    struct aws_input_stream *existing_stream,
    struct aws_s3_upload_request_checksum_context *context);

/**
 * Get the size of the checksum output corresponding to the aws_s3_checksum_algorithm enum value.
 */
AWS_S3_API
size_t aws_get_digest_size_from_checksum_algorithm(enum aws_s3_checksum_algorithm algorithm);

/**
 * Get header name to use for algorithm (e.g. "x-amz-checksum-crc32")
 */
AWS_S3_API
struct aws_byte_cursor aws_get_http_header_name_from_checksum_algorithm(enum aws_s3_checksum_algorithm algorithm);

/**
 * Get algorithm's name (e.g. "CRC32"), to be used as the value of headers like `x-amz-checksum-algorithm`
 */
AWS_S3_API
struct aws_byte_cursor aws_get_checksum_algorithm_name(enum aws_s3_checksum_algorithm algorithm);

/**
 * Get the name of checksum algorithm to be used as the details of the parts were uploaded. Referring to
 * https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompletedPart.html#AmazonS3-Type-CompletedPart
 */
AWS_S3_API
struct aws_byte_cursor aws_get_completed_part_name_from_checksum_algorithm(enum aws_s3_checksum_algorithm algorithm);

/**
 * create a new aws_checksum corresponding to the aws_s3_checksum_algorithm enum value.
 */
AWS_S3_API
struct aws_s3_checksum *aws_checksum_new(struct aws_allocator *allocator, enum aws_s3_checksum_algorithm algorithm);

/**
 * Compute an aws_checksum corresponding to the provided enum, passing a function pointer around instead of using a
 * conditional would be faster, but would be a negligible improvement compared to the cost of processing data twice
 * which would be the only time this function would be used, and would be harder to follow.
 */
AWS_S3_API
int aws_checksum_compute(
    struct aws_allocator *allocator,
    enum aws_s3_checksum_algorithm algorithm,
    const struct aws_byte_cursor *input,
    struct aws_byte_buf *output);

/**
 * Cleans up and deallocates checksum.
 */
AWS_S3_API
void aws_checksum_destroy(struct aws_s3_checksum *checksum);

/**
 * Updates the running checksum with to_checksum. this can be called multiple times.
 */
AWS_S3_API
int aws_checksum_update(struct aws_s3_checksum *checksum, const struct aws_byte_cursor *to_checksum);

/**
 * Completes the checksum computation and writes the final digest to output.
 * Allocation of output is the caller's responsibility.
 */
AWS_S3_API
int aws_checksum_finalize(struct aws_s3_checksum *checksum, struct aws_byte_buf *output);

AWS_S3_API
int aws_s3_meta_request_checksum_config_storage_init(
    struct aws_allocator *allocator,
    struct aws_s3_meta_request_checksum_config_storage *internal_config,
    const struct aws_s3_checksum_config *config,
    const struct aws_http_message *message,
    const void *log_id);

AWS_S3_API
void aws_s3_meta_request_checksum_config_storage_cleanup(
    struct aws_s3_meta_request_checksum_config_storage *internal_config);

#endif /* AWS_S3_CHECKSUMS_H */
