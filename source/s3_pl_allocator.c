/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_pl_allocator.h"
#include <aws/common/ref_count.h>
#include <aws/s3/s3.h>

#if !defined(_WIN32) && !defined(WIN32)
#    include <sys/mman.h>
#else
#    include <windows.h>

#    include <memoryapi.h>
#endif

static void s_s3_pl_allocator_destroy(void *user_data);

static void s_lock_memory(void *mem, size_t mem_length) {
#if !defined(_WIN32) && !defined(WIN32)
    mlock(mem, mem_length);
#else
    VirtualLock(mem, mem_length);
#endif
}

static void s_unlock_memory(void *mem, size_t mem_length) {
#if !defined(_WIN32) && !defined(WIN32)
    munlock(mem, mem_length);
#else
    VirtualUnlock(mem, mem_length);
#endif
}

struct aws_s3_pl_allocation_header {
    size_t mem_length;
};

struct aws_s3_pl_allocator {
    struct aws_ref_count ref_count;
    struct aws_allocator *parent_allocator;
};

struct aws_s3_pl_allocation_header *s_s3_get_allocation_header(const void *addr) {
    return (struct aws_s3_pl_allocation_header *)((uint8_t *)(addr) - sizeof(struct aws_s3_pl_allocation_header));
}

static void *s_s3_pl_mem_acquire(struct aws_allocator *allocator, size_t size) {
    struct aws_s3_pl_allocator *s3_allocator = allocator->impl;

    struct aws_s3_pl_allocation_header *header = NULL;
    void *mem = NULL;

    aws_mem_acquire_many(
        s3_allocator->parent_allocator, 2, &header, sizeof(struct aws_s3_pl_allocation_header), &mem, size);

    AWS_ZERO_STRUCT(*header);
    header->mem_length = size;

    s_lock_memory(mem, size);

    return mem;
}

static void s_s3_pl_mem_release(struct aws_allocator *allocator, void *ptr) {
    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(ptr);

    struct aws_s3_pl_allocator *s3_allocator = allocator->impl;
    struct aws_s3_pl_allocation_header *header = s_s3_get_allocation_header(ptr);

    /* Scrub memory to erase values from cache. */
    memset(ptr, 0, header->mem_length);

    /* Unlock the memory so that it can be saved to a swap file again. */
    s_unlock_memory(ptr, header->mem_length);

    /* Release it back to the parent allocator. */
    aws_mem_release(s3_allocator->parent_allocator, header);
}

static void s_s3_pl_allocator_destroy(void *user_data) {
    AWS_PRECONDITION(user_data);

    AWS_LOGF_INFO(AWS_LS_S3_GENERAL, "Destroying s3 allocator.");

    struct aws_allocator *base_allocator = user_data;
    struct aws_s3_pl_allocator *s3_pl_allocator = base_allocator->impl;
    struct aws_allocator *parent_allocator = s3_pl_allocator->parent_allocator;

    aws_mem_release(parent_allocator, base_allocator);
}

struct aws_allocator *aws_s3_pl_allocator_new(struct aws_allocator *allocator) {
    AWS_PRECONDITION(allocator);

    struct aws_allocator *base_allocator = NULL;
    struct aws_s3_pl_allocator *s3_pl_allocator = NULL;

    aws_mem_acquire_many(
        allocator,
        2,
        &base_allocator,
        sizeof(struct aws_allocator),
        &s3_pl_allocator,
        sizeof(struct aws_s3_pl_allocator));

    AWS_ZERO_STRUCT(*base_allocator);
    AWS_ZERO_STRUCT(*s3_pl_allocator);

    base_allocator->mem_acquire = s_s3_pl_mem_acquire;
    base_allocator->mem_release = s_s3_pl_mem_release;
    base_allocator->impl = s3_pl_allocator;

    aws_ref_count_init(&s3_pl_allocator->ref_count, base_allocator, s_s3_pl_allocator_destroy);
    s3_pl_allocator->parent_allocator = allocator;

    return base_allocator;
}

void aws_s3_pl_allocator_acquire(struct aws_allocator *allocator) {
    AWS_PRECONDITION(allocator);

    AWS_LOGF_INFO(AWS_LS_S3_GENERAL, "Acquiring reference to s3 allocator.");

    struct aws_s3_pl_allocator *s3_pl_allocator = allocator->impl;
    AWS_PRECONDITION(s3_pl_allocator);

    aws_ref_count_acquire(&s3_pl_allocator->ref_count);
}

void aws_s3_pl_allocator_release(struct aws_allocator *allocator) {
    if (!allocator) {
        return;
    }

    struct aws_s3_pl_allocator *s3_pl_allocator = allocator->impl;

    if (!s3_pl_allocator) {
        return;
    }

    AWS_LOGF_INFO(AWS_LS_S3_GENERAL, "Releasing reference to s3 allocator.");
    aws_ref_count_release(&s3_pl_allocator->ref_count);
}
