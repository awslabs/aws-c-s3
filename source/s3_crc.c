/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/cal/hash.h>
#include <aws/checksums/crc.h>
#include <aws/common/byte_buf.h>
#include <aws/common/common.h>
#include <aws/s3/private/s3_crc.h>

#define AWS_CRC_LEN 4

static void s_destroy_crc(struct aws_hash *hash) {
    aws_mem_release(hash->allocator, hash);
}
static int s_update_crc32(struct aws_hash *hash, const struct aws_byte_cursor *buffer);
static int s_update_crc32c(struct aws_hash *hash, const struct aws_byte_cursor *buffer);
static int s_finalize_crc(struct aws_hash *hash, struct aws_byte_buf *output);

static struct aws_hash_vtable s_crc32_vtable = {
    .destroy = s_destroy_crc,
    .update = s_update_crc32,
    .finalize = s_finalize_crc,
    .alg_name = "CRC32",
};

static struct aws_hash_vtable s_crc32c_vtable = {
    .destroy = s_destroy_crc,
    .update = s_update_crc32c,
    .finalize = s_finalize_crc,
    .alg_name = "CRC32C",
};

static struct aws_hash *s_crc32_common_new(struct aws_allocator *allocator, struct aws_hash_vtable *vtable) {
    struct aws_hash *hash = aws_mem_calloc(allocator, 1, sizeof(struct aws_hash));

    if (!hash) {
        return NULL;
    }

    hash->allocator = allocator;
    hash->vtable = vtable;
    hash->impl = 0;
    hash->digest_size = AWS_CRC_LEN;
    hash->good = true;

    return hash;
}

struct aws_hash *aws_hash_crc32_new(struct aws_allocator *allocator) {
    return s_crc32_common_new(allocator, &s_crc32_vtable);
}

struct aws_hash *aws_hash_crc32c_new(struct aws_allocator *allocator) {
    return s_crc32_common_new(allocator, &s_crc32c_vtable);
}

static int s_crc32_common_update(
    struct aws_hash *hash,
    const struct aws_byte_cursor *to_hash,
    uint32_t (*checksum_fn)(const uint8_t *, int, uint32_t)) {

    if (!hash->good) {
        return aws_raise_error(AWS_ERROR_INVALID_STATE);
    }

    /* hash->impl has type (void *) to match the rest of the API, but we are storing as a uintptr, and using it as
     * an int to avoid mem allocation */
    uintptr_t crc_ptr = (uintptr_t)hash->impl;
    uint32_t crc = (uint32_t)crc_ptr;

    /* This function takes length as size_t, but actual CRC function takes length as int. * Consume the input in chunks
     * up to INT_MAX in length, just in case the input is larger than that. */
    struct aws_byte_cursor input_remainder = *to_hash;
    while (input_remainder.len > 0) {
        struct aws_byte_cursor input_chunk =
            aws_byte_cursor_advance(&input_remainder, aws_min_size(input_remainder.len, INT_MAX));
        uint32_t new_crc = checksum_fn(input_chunk.ptr, (int)input_chunk.len, crc);
        hash->impl = (void *)(uintptr_t)new_crc;
    }
    return AWS_OP_SUCCESS;
}

static int s_update_crc32(struct aws_hash *hash, const struct aws_byte_cursor *to_hash) {
    return s_crc32_common_update(hash, to_hash, aws_checksums_crc32);
}

static int s_update_crc32c(struct aws_hash *hash, const struct aws_byte_cursor *to_hash) {
    return s_crc32_common_update(hash, to_hash, aws_checksums_crc32c);
}

static int s_finalize_crc(struct aws_hash *hash, struct aws_byte_buf *output) {
    if (!hash->good) {
        return aws_raise_error(AWS_ERROR_INVALID_STATE);
    }

    size_t buffer_len = output->capacity - output->len;

    if (buffer_len < AWS_CRC_LEN) {
        return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
    }

    hash->good = false;
    /* hash->impl has type (void *) to match the rest of the API, but we are storing as a uintptr, and using it as
     * an int to avoid mem allocation */
    uintptr_t crc_ptr = (uintptr_t)hash->impl;
    const uint32_t crc = (uint32_t)crc_ptr;
    /* Write out bytes in big endian order */
    return aws_byte_buf_write_be32(output, crc) ? AWS_OP_SUCCESS : AWS_OP_ERR;
}
