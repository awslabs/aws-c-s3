
/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/aws_mmap.h"
#include <aws/common/allocator.h>

#ifdef _WIN32
#    include <windows.h>
#else
#    include <errno.h>
#    include <sys/mman.h>
#    include <sys/stat.h>
#    include <unistd.h>
#endif /* _WIN32 */

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>

struct aws_mmap_context {
    void *impl;
};

#ifdef _WIN32
struct aws_mmap_context_win_impl {
    struct aws_allocator *allocator;

    HANDLE file_handler;
    HANDLE mapping_handler;
};

struct aws_mmap_context *s_mmap_context_destroy(struct aws_mmap_context *context) {
    if (!context) {
        return NULL;
    }
    struct aws_mmap_context_win_impl *impl = context->impl;

    if (impl->mapping_handler) {
        CloseHandle(impl->mapping_handler);
    }
    if (impl->file_handler) {
        CloseHandle(impl->file_handler);
    }
    aws_mem_release(impl->allocator, context);
    return NULL;
}

struct aws_mmap_context *aws_mmap_context_release(struct aws_mmap_context *context) {
    return s_mmap_context_destroy(context);
}

void *aws_mmap_context_map_content(
    struct aws_mmap_context *context,
    size_t length,
    size_t offset,
    void **out_start_addr) {
    //     AWS_PRECONDITION(context);
    //     struct aws_mmap_context_win_impl *impl = context->impl;

    //     void *mapped_address = MapViewOfFile(impl->mapping_handler, FILE_MAP_READ, 0, 0, 0);
    //     if (mapped_address == NULL) {
    //         goto error;
    //     }
    //     return mapped_address;
    // error:
    /* TODO: LOG and raise AWS ERRORs */
    aws_raise_error(AWS_ERROR_SYS_CALL_FAILURE);
    return NULL;
}

int aws_mmap_context_unmap_content(void *mapped_addr, size_t len) {
    (void)len;
    if (mapped_addr) {
        UnmapViewOfFile(mapped_addr);
    }
    return AWS_OP_SUCCESS;
}

struct aws_mmap_context *aws_mmap_context_new(struct aws_allocator *allocator, const char *file_name) {
    struct aws_mmap_context *context = NULL;
    struct aws_mmap_context_win_impl *impl = NULL;
    aws_mem_acquire_many(
        allocator, 2, &context, sizeof(struct aws_mmap_context), &impl, sizeof(struct aws_mmap_context_win_impl));
    AWS_ZERO_STRUCT(*context);
    AWS_ZERO_STRUCT(*impl);

    context->impl = impl;

    impl->allocator = allocator;
    impl->file_handler = CreateFile(
        file_name,
        GENERIC_READ,
        FILE_SHARE_READ,
        NULL /*SecurityAttributes*/,
        OPEN_EXISTING,
        FILE_ATTRIBUTE_NORMAL,
        NULL /*TemplateFile*/);
    if (impl->file_handler == INVALID_HANDLE_VALUE) {
        goto error;
    }
    impl->mapping_handler = CreateFileMapping(impl->file_handler, NULL, PAGE_READONLY, 0, 0, NULL /*Name*/);
    if (impl->mapping_handler == NULL) {
        goto error;
    }
    return context;
error:
    /* TODO: LOG and raise AWS ERRORs */
    int error = GetLastError();
    if (error == ERROR_FILE_NOT_FOUND) {
        aws_raise_error(AWS_ERROR_FILE_INVALID_PATH);
    }

    if (error == ERROR_ACCESS_DENIED) {
        aws_raise_error(AWS_ERROR_NO_PERMISSION);
    }

    /* TODO: more errors and maybe same interface as `aws_translate_and_raise_io_error` */
    return s_mmap_context_destroy(context);
}
#else

struct aws_mmap_context_posix_impl {
    struct aws_allocator *allocator;
    int fd;
};

struct aws_mmap_context *s_mmap_context_destroy(struct aws_mmap_context *context) {
    if (!context) {
        return NULL;
    }
    struct aws_mmap_context_posix_impl *impl = context->impl;
    if (impl->fd) {
        close(impl->fd);
    }
    aws_mem_release(impl->allocator, context);
    return NULL;
}

struct aws_mmap_context *aws_mmap_context_release(struct aws_mmap_context *context) {
    return s_mmap_context_destroy(context);
}

void *aws_mmap_context_map_content(
    struct aws_mmap_context *context,
    size_t length,
    size_t offset,
    void **out_start_addr) {
    AWS_PRECONDITION(context);
    struct aws_mmap_context_posix_impl *impl = context->impl;

    long page_size = sysconf(_SC_PAGE_SIZE);
    uint64_t number_pages = offset / page_size;
    uint64_t page_starts_offset = page_size * number_pages;
    uint64_t in_page_offset = offset - page_starts_offset;

    void *mapped_data = mmap(NULL, length + in_page_offset, PROT_READ, MAP_SHARED, impl->fd, page_starts_offset);
    if (mapped_data == MAP_FAILED) {
        goto error;
    }
    *out_start_addr = mapped_data;
    return (char *)mapped_data + in_page_offset;
error:
    /* TODO: LOG and raise AWS ERRORs */
    aws_translate_and_raise_io_error(errno);
    return NULL;
}

int aws_mmap_context_unmap_content(void *mapped_addr, size_t len) {
    if (!mapped_addr) {
        return AWS_OP_SUCCESS;
    }
    if (munmap(mapped_addr, len)) {

        return aws_translate_and_raise_io_error(errno);
    }
    return AWS_OP_SUCCESS;
}

struct aws_mmap_context *aws_mmap_context_new(struct aws_allocator *allocator, const char *file_name) {
    struct aws_mmap_context *context = NULL;
    struct aws_mmap_context_posix_impl *impl = NULL;
    aws_mem_acquire_many(
        allocator, 2, &context, sizeof(struct aws_mmap_context), &impl, sizeof(struct aws_mmap_context_posix_impl));
    AWS_ZERO_STRUCT(*context);
    AWS_ZERO_STRUCT(*impl);

    impl->allocator = allocator;
    context->impl = impl;

    impl->fd = open(file_name, O_RDONLY);
    if (impl->fd == -1) {
        goto error;
    }

    return context;
error:
    /* TODO: LOG and raise AWS ERRORs */
    aws_translate_and_raise_io_error(errno);
    return s_mmap_context_destroy(context);
}
#endif /* _WIN32 */
