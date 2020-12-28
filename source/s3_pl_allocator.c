/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_pl_allocator.h"
#include <aws/common/ref_count.h>
#include <aws/s3/s3.h>

#if !defined(_WIN32) && !defined(WIN32)
#    include <errno.h>
#    include <sys/mman.h>
#else
#    include <windows.h>

#    include <memoryapi.h>
#endif

static void s_s3_pl_allocator_destroy(void *user_data);

static int s_lock_memory(void *mem, size_t mem_length) {
    AWS_PRECONDITION(mem);

    int platform_error_code = 0;

#if !defined(_WIN32) && !defined(WIN32)
    int result = mlock(mem, mem_length);

    if (result == -1) {
        platform_error_code = errno;
        goto error_result;
    }
#else
    if (!VirtualLock(mem, mem_length)) {
        platform_error_code = GetLastError();
        goto error_result;
    }
#endif

    return AWS_OP_SUCCESS;

error_result:
    AWS_LOGF_ERROR(AWS_LS_S3_PL_ALLOCATOR, "Could not lock memory (platform error code: %d)", platform_error_code);
    aws_raise_error(AWS_ERROR_S3_LOCK_MEM_FAILED);
    return AWS_OP_ERR;
}

static int s_unlock_memory(void *mem, size_t mem_length) {
    AWS_PRECONDITION(mem);
    int platform_error_code = 0;

#if !defined(_WIN32) && !defined(WIN32)
    int result = munlock(mem, mem_length);

    if (result == -1) {
        goto error_result;
    }
#else
    if (!VirtualUnlock(mem, mem_length)) {
        platform_error_code = GetLastError();
        goto error_result;
    }
#endif
    return AWS_OP_SUCCESS;

error_result:
    AWS_LOGF_ERROR(AWS_LS_S3_PL_ALLOCATOR, "Could not unlock memory (platform error code: %d)", platform_error_code);
    aws_raise_error(AWS_ERROR_S3_UNLOCK_MEM_FAILED);
    return AWS_OP_ERR;
}

struct aws_s3_pl_allocation_header {
    size_t mem_length;
};

struct aws_s3_pl_allocator {
    struct aws_ref_count ref_count;
    struct aws_allocator *parent_allocator;
};

/* TODO: expose this logic from aws-c-common or find an alternate approach to tagging allocations. */
#define AWS_ALIGN_ROUND_UP(value, alignment) (((value) + ((alignment)-1)) & ~((alignment)-1))

struct aws_s3_pl_allocation_header *s_s3_get_allocation_header(const void *addr) {
    enum { S_ALIGNMENT = sizeof(intmax_t) };
    const size_t offset = AWS_ALIGN_ROUND_UP(sizeof(struct aws_s3_pl_allocation_header), S_ALIGNMENT);
    return (struct aws_s3_pl_allocation_header *)((uint8_t *)(addr)-offset);
}

#undef AWS_ALIGN_ROUND_UP

static void *s_s3_pl_mem_acquire(struct aws_allocator *allocator, size_t size) {
    AWS_PRECONDITION(allocator);

    struct aws_s3_pl_allocator *s3_allocator = allocator->impl;

    struct aws_s3_pl_allocation_header *header = NULL;
    void *mem = NULL;

    aws_mem_acquire_many(
        s3_allocator->parent_allocator, 2, &header, sizeof(struct aws_s3_pl_allocation_header), &mem, size);

    AWS_ZERO_STRUCT(*header);
    header->mem_length = size;

    int lock_mem_result = s_lock_memory(mem, size);

    AWS_FATAL_ASSERT(lock_mem_result == AWS_OP_SUCCESS);

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

    AWS_LOGF_INFO(AWS_LS_S3_PL_ALLOCATOR, "id=%p Destroying s3_pl_allocator", user_data);

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

    struct aws_s3_pl_allocator *s3_pl_allocator = allocator->impl;
    AWS_PRECONDITION(s3_pl_allocator);

    AWS_LOGF_INFO(AWS_LS_S3_PL_ALLOCATOR, "id=%p Acquiring reference to s3_pl_allocator", (void *)s3_pl_allocator);

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

    AWS_LOGF_INFO(AWS_LS_S3_PL_ALLOCATOR, "id=%p Releasing reference to s3_pl_allocator", (void *)s3_pl_allocator);
    aws_ref_count_release(&s3_pl_allocator->ref_count);
}
