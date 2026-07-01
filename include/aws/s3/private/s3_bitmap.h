#ifndef AWS_S3_BITMAP_H
#define AWS_S3_BITMAP_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

/* TODO: move to aws-c-common as a general-purpose aws_bitmap type
 * that wraps aws_byte_buf internally (hidden from callers). */

#include <aws/common/byte_buf.h>
#include <aws/s3/s3.h>

AWS_PUSH_SANE_WARNING_LEVEL

/**
 * Simple bitmap backed by aws_byte_buf.
 * Uses 1-based indexing to match S3 part numbering convention (parts start at 1).
 * Internally, bit 0 of byte 0 corresponds to index 1.
 */

AWS_STATIC_IMPL bool aws_s3_bitmap_get(const struct aws_byte_buf *bitmap, uint32_t index_1based) {
    if (bitmap->len == 0 || index_1based == 0) {
        return false;
    }
    uint32_t bit_index = index_1based - 1;
    size_t byte_index = bit_index / 8;
    uint8_t bit_mask = (uint8_t)(1u << (bit_index % 8));
    if (byte_index >= bitmap->len) {
        return false;
    }
    return (bitmap->buffer[byte_index] & bit_mask) != 0;
}

AWS_STATIC_IMPL void aws_s3_bitmap_set(struct aws_byte_buf *bitmap, uint32_t index_1based) {
    if (index_1based == 0) {
        return;
    }
    uint32_t bit_index = index_1based - 1;
    size_t byte_index = bit_index / 8;
    uint8_t bit_mask = (uint8_t)(1u << (bit_index % 8));
    if (byte_index < bitmap->len) {
        bitmap->buffer[byte_index] |= bit_mask;
    }
}

AWS_STATIC_IMPL int aws_s3_bitmap_init(struct aws_byte_buf *bitmap, struct aws_allocator *allocator, uint32_t capacity) {
    size_t bitmap_bytes = (capacity + 7) / 8;
    if (aws_byte_buf_init(bitmap, allocator, bitmap_bytes)) {
        return AWS_OP_ERR;
    }
    aws_byte_buf_write_u8_n(bitmap, 0, bitmap_bytes);
    return AWS_OP_SUCCESS;
}

AWS_POP_SANE_WARNING_LEVEL

#endif /* AWS_S3_BITMAP_H */
