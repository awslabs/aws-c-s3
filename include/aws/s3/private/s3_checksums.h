#ifndef AWS_S3_Checksums_H
#define AWS_S3_Checksums_H
/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */
#include "aws/s3/s3_client.h"
#include <aws/cal/hash.h>
#include <aws/io/stream.h>

#define AWS_CRC32_LEN 4
#define AWS_CRC32C_LEN 4

struct aws_checksum;

struct aws_checksum_vtable {
    void (*destroy)(struct aws_checksum *checksum);
    int (*update)(struct aws_checksum *checksum, const struct aws_byte_cursor *buf);
    int (*finalize)(struct aws_checksum *checksum, struct aws_byte_buf *out, size_t truncate_to);
};

struct aws_checksum {
    struct aws_allocator *allocator;
    struct aws_checksum_vtable *vtable;
    void *impl;
    size_t digest_size;
    bool good;
};

struct aws_input_stream *aws_chunk_stream_new(
    struct aws_allocator *alloc,
    struct aws_input_stream *existing_stream,
    struct aws_checksum *checksum);

/**
 * Allocates and initializes a sha256 checksum instance.
 */
AWS_S3_API struct aws_checksum *aws_sha256_checksum_new(struct aws_allocator *allocator);
/**
 * Allocates and initializes a sha1 checksum instance.
 */
AWS_S3_API
struct aws_checksum *aws_sha1_checksum_new(struct aws_allocator *allocator);
/**
 * Allocates and initializes an md5 checksum instance.
 */
AWS_S3_API
struct aws_checksum *aws_md5_checksum_new(struct aws_allocator *allocator);
/**
 * Allocates and initializes an crc32 checksum instance.
 */
AWS_S3_API
struct aws_checksum *aws_crc32_checksum_new(struct aws_allocator *allocator);
/**
 * Allocates and initializes an crc32c checksum instance.
 */
AWS_S3_API
struct aws_checksum *aws_crc32c_checksum_new(struct aws_allocator *allocator);

/**
 * Compute an aws_checksum corresponding to the provided enum, passing a function pointer around instead of using a
 * conditional would be faster, but would be a negligble improvment compared to the cost of processing data twice which
 * would be the only time this function would be used, and would be harder to follow.
 */
AWS_S3_API
int aws_checksum_compute(
    struct aws_allocator *allocator,
    enum aws_s3_checksum_algorithm algorithm,
    const struct aws_byte_cursor *input,
    struct aws_byte_buf *output,
    size_t truncate_to);

/**
 * Cleans up and deallocates checksum.
 */
AWS_S3_API
void aws_checksum_destroy(struct aws_checksum *checksum);
/**
 * Updates the running checksum with to_checksum. this can be called multiple times.
 */
AWS_S3_API
int aws_checksum_update(struct aws_checksum *checksum, const struct aws_byte_cursor *to_checksum);
/**
 * Completes the checksum computation and writes the final digest to output.
 * Allocation of output is the caller's responsibility.
 */
AWS_S3_API
int aws_checksum_finalize(struct aws_checksum *checksum, struct aws_byte_buf *output, size_t truncate_to);
#endif
