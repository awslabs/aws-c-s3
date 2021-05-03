/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/checksums/crc.h>
#include <aws/common/byte_buf.h>
#include <aws/common/common.h>
#include <aws/s3/private/s3_checksum.h>
#include <aws/s3/s3_streaming_checksum.h>

static void s_destroy_crc(struct aws_checksum *checksum) {
    aws_mem_release(checksum->allocator, checksum);
}
static int s_update_crc32(struct aws_checksum *checksum, const struct aws_byte_cursor *buffer);
static int s_update_crc32c(struct aws_checksum *checksum, const struct aws_byte_cursor *buffer);
static int s_finalize_crc(struct aws_checksum *checksum, struct aws_byte_buf *output);

static struct aws_checksum_vtable s_crc32_vtable = {
    .destroy = s_destroy_crc,
    .update = s_update_crc32,
    .finalize = s_finalize_crc,
    .alg_name = "CRC32",
};

static struct aws_checksum_vtable s_crc32c_vtable = {
    .destroy = s_destroy_crc,
    .update = s_update_crc32c,
    .finalize = s_finalize_crc,
    .alg_name = "CRC32C",
};

static struct aws_checksum *s_crc32_common_new(struct aws_allocator *allocator, struct aws_checksum_vtable *vtable) {
    struct aws_checksum *checksum = aws_mem_calloc(allocator, 1, sizeof(struct aws_checksum));

    if (!checksum) {
        return NULL;
    }

    checksum->allocator = allocator;
    checksum->vtable = vtable;
    checksum->impl = 0;
    checksum->digest_size = AWS_CRC_LEN;
    checksum->good = true;

    return checksum;
}

struct aws_checksum *aws_checksum_crc32_new(struct aws_allocator *allocator) {
    return s_crc32_common_new(allocator, &s_crc32_vtable);
}

struct aws_checksum *aws_checksum_crc32c_new(struct aws_allocator *allocator) {
    return s_crc32_common_new(allocator, &s_crc32c_vtable);
}

static int s_crc32_common_update(
    struct aws_checksum *checksum,
    const struct aws_byte_cursor *to_checksum,
    uint32_t (*checksum_fn)(const uint8_t *, int, uint32_t)) {

    if (!checksum->good) {
        return aws_raise_error(AWS_ERROR_INVALID_STATE);
    }

    /* checksum->impl has type (void *) to match the rest of the API, but we are storing as a uintptr, and using it as
     * an int to avoid mem allocation */
    uintptr_t crc_ptr = (uintptr_t)checksum->impl;
    uint32_t crc = (uint32_t)crc_ptr;

    /* This function takes length as size_t, but actual CRC function takes length as int. * Consume the input in chunks
     * up to INT_MAX in length, just in case the input is larger than that. */
    struct aws_byte_cursor input_remainder = *to_checksum;
    while (input_remainder.len > 0) {
        struct aws_byte_cursor input_chunk =
            aws_byte_cursor_advance(&input_remainder, aws_min_size(input_remainder.len, INT_MAX));
        uint32_t new_crc = checksum_fn(input_chunk.ptr, (int)input_chunk.len, crc);
        checksum->impl = (void *)(uintptr_t)new_crc;
    }
    return AWS_OP_SUCCESS;
}

static int s_update_crc32(struct aws_checksum *checksum, const struct aws_byte_cursor *to_checksum) {
    return s_crc32_common_update(checksum, to_checksum, aws_checksums_crc32);
}

static int s_update_crc32c(struct aws_checksum *checksum, const struct aws_byte_cursor *to_checksum) {
    return s_crc32_common_update(checksum, to_checksum, aws_checksums_crc32c);
}

static int s_finalize_crc(struct aws_checksum *checksum, struct aws_byte_buf *output) {
    if (!checksum->good) {
        return aws_raise_error(AWS_ERROR_INVALID_STATE);
    }

    size_t buffer_len = output->capacity - output->len;

    if (buffer_len < AWS_CRC_LEN) {
        return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
    }

    checksum->good = false;
    /* checksum->impl has type (void *) to match the rest of the API, but we are storing as a uintptr, and using it as
     * an int to avoid mem allocation */
    uintptr_t crc_ptr = (uintptr_t)checksum->impl;
    const uint32_t crc = (uint32_t)crc_ptr;
    /* Write out bytes in big endian order */
    return aws_byte_buf_write_be32(output, crc) ? AWS_OP_SUCCESS : AWS_OP_ERR;
}

int aws_checksum_crc32_compute(
    struct aws_allocator *allocator,
    const struct aws_byte_cursor *input,
    struct aws_byte_buf *output,
    size_t truncate_to) {
    return compute_checksum(aws_checksum_crc32_new(allocator), input, output, truncate_to);
}

int aws_checksum_crc32c_compute(
    struct aws_allocator *allocator,
    const struct aws_byte_cursor *input,
    struct aws_byte_buf *output,
    size_t truncate_to) {
    return compute_checksum(aws_checksum_crc32c_new(allocator), input, output, truncate_to);
}
