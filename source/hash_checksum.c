/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/cal/hash.h>
#include <aws/s3/private/s3_checksum.h>
#include <aws/s3/s3_streaming_checksum.h>

static void s_destroy_hash(struct aws_checksum *checksum);
static int s_update_hash(struct aws_checksum *checksum, const struct aws_byte_cursor *buffer);
static int s_finalize_hash(struct aws_checksum *checksum, struct aws_byte_buf *output);

static struct aws_checksum_vtable s_sha1_vtable = {
    .destroy = s_destroy_hash,
    .update = s_update_hash,
    .finalize = s_finalize_hash,
    .alg_name = "SHA1",
};

static struct aws_checksum_vtable s_sha256_vtable = {
    .destroy = s_destroy_hash,
    .update = s_update_hash,
    .finalize = s_finalize_hash,
    .alg_name = "SHA256",
};

static void s_destroy_hash(struct aws_checksum *checksum) {
    struct aws_hash *hash = checksum->impl;
    aws_hash_destroy(hash);
    aws_mem_release(checksum->allocator, checksum);
}
static int s_update_hash(struct aws_checksum *checksum, const struct aws_byte_cursor *buffer) {
    if (!checksum->good) {
        return aws_raise_error(AWS_ERROR_INVALID_STATE);
    }
    struct aws_hash *hash = checksum->impl;
    return aws_hash_update(hash, buffer);
}

static int s_finalize_hash(struct aws_checksum *checksum, struct aws_byte_buf *output) {
    if (!checksum->good) {
        return aws_raise_error(AWS_ERROR_INVALID_STATE);
    }
    checksum->good = false;
    struct aws_hash *hash = checksum->impl;
    return hash->vtable->finalize(hash, output);
}

struct aws_checksum *aws_checksum_sha1_new(struct aws_allocator *allocator) {
    struct aws_checksum *checksum = aws_mem_calloc(allocator, 1, sizeof(struct aws_checksum));

    if (!checksum) {
        return NULL;
    }

    checksum->allocator = allocator;
    checksum->vtable = &s_sha1_vtable;
    checksum->digest_size = AWS_SHA1_LEN;
    checksum->good = true;
    checksum->impl = aws_sha1_new(allocator);
    return checksum;
}

struct aws_checksum *aws_checksum_sha256_new(struct aws_allocator *allocator) {
    struct aws_checksum *checksum = aws_mem_calloc(allocator, 1, sizeof(struct aws_checksum));

    if (!checksum) {
        return NULL;
    }

    checksum->allocator = allocator;
    checksum->vtable = &s_sha256_vtable;
    checksum->digest_size = AWS_SHA256_LEN;
    checksum->good = true;
    checksum->impl = aws_sha256_new(allocator);
    return checksum;
}

int aws_checksum_sha256_compute(
    struct aws_allocator *allocator,
    const struct aws_byte_cursor *input,
    struct aws_byte_buf *output,
    size_t truncate_to) {
    return compute_checksum(aws_checksum_sha256_new(allocator), input, output, truncate_to);
}

int aws_checksum_sha1_compute(
    struct aws_allocator *allocator,
    const struct aws_byte_cursor *input,
    struct aws_byte_buf *output,
    size_t truncate_to) {
    return compute_checksum(aws_checksum_sha1_new(allocator), input, output, truncate_to);
}
