/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <stdbool.h>
#include <stddef.h>

struct aws_checksum {
    struct aws_allocator *allocator;
    struct aws_checksum_vtable *vtable;
    size_t digest_size;
    bool good;
    void *impl;
};

struct aws_checksum_vtable {
    const char *alg_name;
    void (*destroy)(struct aws_checksum *checksum);
    int (*update)(struct aws_checksum *checksum, const struct aws_byte_cursor *buf);
    int (*finalize)(struct aws_checksum *checksum, struct aws_byte_buf *out);
};

int compute_checksum(
    struct aws_checksum *checksum,
    const struct aws_byte_cursor *input,
    struct aws_byte_buf *output,
    size_t truncate_to);
