
/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/aws_mmap.h"
#include <aws/common/allocator.h>
#include <aws/common/logging.h>
#include <aws/common/ref_count.h>

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
    struct aws_ref_count ref_count;
};

#ifdef _WIN32
struct aws_mmap_context_win_impl {
    struct aws_allocator *allocator;

    HANDLE file_handler;
    HANDLE mapping_handler;
};

static void s_mmap_context_destroy_callback(void *context) {
    struct aws_mmap_context *mmap_context = context;
    struct aws_mmap_context_win_impl *impl = mmap_context->impl;

    if (impl->mapping_handler) {
        CloseHandle(impl->mapping_handler);
    }
    if (impl->file_handler) {
        CloseHandle(impl->file_handler);
    }
    aws_mem_release(impl->allocator, mmap_context);
}

struct aws_mmap_context *s_mmap_context_destroy(struct aws_mmap_context *context) {
    if (!context) {
        return NULL;
    }
    s_mmap_context_destroy_callback(context);
    return NULL;
}

struct aws_mmap_context *aws_mmap_context_release(struct aws_mmap_context *context) {
    if (context == NULL) {
        return NULL;
    }

    aws_ref_count_release(&context->ref_count);
    return NULL;
}

void *aws_mmap_context_map_content(
    struct aws_mmap_context *context,
    size_t length,
    size_t offset,
    void **out_start_addr) {

    AWS_PRECONDITION(context);
    struct aws_mmap_context_win_impl *impl = context->impl;

    SYSTEM_INFO si;
    GetSystemInfo(&si);
    uint64_t page_size = si.dwAllocationGranularity;
    uint64_t number_pages = offset / page_size;
    uint64_t page_starts_offset = page_size * number_pages;
    uint64_t in_page_offset = offset - page_starts_offset;
    DWORD page_starts_offset_high = (DWORD)((page_starts_offset >> 32) & 0xFFFFFFFF);
    DWORD page_starts_offset_low = (DWORD)(page_starts_offset & 0xFFFFFFFF);

    void *mapped_address = MapViewOfFile(
        impl->mapping_handler,
        FILE_MAP_READ,
        page_starts_offset_high,
        page_starts_offset_low,
        (SIZE_T)(length + in_page_offset));
    if (mapped_address == NULL) {
        goto error;
    }
    *out_start_addr = mapped_address;
    return (char *)mapped_address + in_page_offset;
error:
    DWORD error_code = GetLastError();
    AWS_LOGF_ERROR(AWS_LS_S3_GENERAL, "Failed to map file content with error code %d", error_code);
    aws_raise_error(AWS_ERROR_SYS_CALL_FAILURE);
    return NULL;
}

int aws_mmap_context_unmap_content(void *mapped_addr, size_t len) {
    (void)len;
    if (mapped_addr) {
        if (!UnmapViewOfFile(mapped_addr)) {
            DWORD error_code = GetLastError();
            AWS_LOGF_ERROR(AWS_LS_S3_GENERAL, "Failed to unmap memory with error code %d", error_code);
            aws_raise_error(AWS_ERROR_SYS_CALL_FAILURE);
            return AWS_OP_ERR;
        }
    }
    return AWS_OP_SUCCESS;
}

struct aws_mmap_context *aws_mmap_context_acquire(struct aws_mmap_context *context) {
    if (context == NULL) {
        return NULL;
    }

    aws_ref_count_acquire(&context->ref_count);
    return context;
}

struct aws_mmap_context *aws_mmap_context_new(struct aws_allocator *allocator, struct aws_string *file_name) {
    struct aws_mmap_context *context = NULL;
    struct aws_mmap_context_win_impl *impl = NULL;
    aws_mem_acquire_many(
        allocator, 2, &context, sizeof(struct aws_mmap_context), &impl, sizeof(struct aws_mmap_context_win_impl));
    AWS_ZERO_STRUCT(*context);
    AWS_ZERO_STRUCT(*impl);

    aws_ref_count_init(&context->ref_count, context, s_mmap_context_destroy_callback);
    context->impl = impl;
    struct aws_wstring *w_file_name = aws_string_convert_to_wstring(aws_default_allocator(), file_name);

    impl->allocator = allocator;
    impl->file_handler = CreateFileW(
        aws_wstring_c_str(w_file_name),
        GENERIC_READ,
        FILE_SHARE_READ,
        NULL /*SecurityAttributes*/,
        OPEN_EXISTING,
        FILE_ATTRIBUTE_NORMAL,
        NULL /*TemplateFile*/);
    aws_wstring_destroy(w_file_name);
    if (impl->file_handler == INVALID_HANDLE_VALUE) {
        goto error;
    }
    impl->mapping_handler = CreateFileMapping(impl->file_handler, NULL, PAGE_READONLY, 0, 0, NULL /*Name*/);
    if (impl->mapping_handler == NULL) {
        goto error;
    }
    return context;
error:
    DWORD error_code = GetLastError();
    AWS_LOGF_ERROR(
        AWS_LS_S3_GENERAL,
        "Failed to create mmap context for file %s with error code %d",
        aws_string_c_str(file_name),
        error_code);

    if (error_code == ERROR_FILE_NOT_FOUND) {
        AWS_LOGF_ERROR(AWS_LS_S3_GENERAL, "File not found: %s", aws_string_c_str(file_name));
        aws_raise_error(AWS_ERROR_FILE_INVALID_PATH);
    } else if (error_code == ERROR_ACCESS_DENIED) {
        AWS_LOGF_ERROR(AWS_LS_S3_GENERAL, "Access denied to file: %s", aws_string_c_str(file_name));
        aws_raise_error(AWS_ERROR_NO_PERMISSION);
    } else if (error_code == ERROR_SHARING_VIOLATION) {
        AWS_LOGF_ERROR(AWS_LS_S3_GENERAL, "Sharing violation for file: %s", aws_string_c_str(file_name));
        aws_raise_error(AWS_ERROR_FILE_PERMISSION_DENIED);
    } else if (error_code == ERROR_LOCK_VIOLATION) {
        AWS_LOGF_ERROR(AWS_LS_S3_GENERAL, "Lock violation for file: %s", aws_string_c_str(file_name));
        aws_raise_error(AWS_ERROR_FILE_PERMISSION_DENIED);
    } else {
        aws_raise_error(AWS_ERROR_SYS_CALL_FAILURE);
    }

    return s_mmap_context_destroy(context);
}
#else

struct aws_mmap_context_posix_impl {
    struct aws_allocator *allocator;
    int fd;
};

static void s_mmap_context_destroy_callback_posix(void *context) {
    struct aws_mmap_context *mmap_context = context;
    struct aws_mmap_context_posix_impl *impl = mmap_context->impl;
    if (impl->fd) {
        close(impl->fd);
    }
    aws_mem_release(impl->allocator, mmap_context);
}

struct aws_mmap_context *s_mmap_context_destroy(struct aws_mmap_context *context) {
    if (!context) {
        return NULL;
    }
    s_mmap_context_destroy_callback_posix(context);
    return NULL;
}

struct aws_mmap_context *aws_mmap_context_release(struct aws_mmap_context *context) {
    if (context == NULL) {
        return NULL;
    }

    aws_ref_count_release(&context->ref_count);
    return NULL;
}

struct aws_mmap_context *aws_mmap_context_acquire(struct aws_mmap_context *context) {
    if (context == NULL) {
        return NULL;
    }

    aws_ref_count_acquire(&context->ref_count);
    return context;
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
    int error_code = errno;
    AWS_LOGF_ERROR(AWS_LS_S3_GENERAL, "Failed to map file content with error code %d", error_code);
    aws_translate_and_raise_io_error(error_code);
    return NULL;
}

int aws_mmap_context_get_fd(struct aws_mmap_context *context) {
    AWS_PRECONDITION(context);
    struct aws_mmap_context_posix_impl *impl = context->impl;
    return impl->fd;
}

int aws_mmap_context_unmap_content(void *mapped_addr, size_t len) {
    if (!mapped_addr) {
        return AWS_OP_SUCCESS;
    }
    if (munmap(mapped_addr, len)) {
        /**
         * Remove any mappings for those entire pages containing any part of the address space of the process starting
         * at addr and continuing for len bytes.
         *
         * So, even if the len is not the exact match of the length of bytes we mapped, we will still free the number of
         * pages we allocated.
         **/
        int error_code = errno;
        AWS_LOGF_ERROR(AWS_LS_S3_GENERAL, "Failed to unmap memory with error code %d", error_code);
        return aws_translate_and_raise_io_error(error_code);
    }
    return AWS_OP_SUCCESS;
}

struct aws_mmap_context *aws_mmap_context_new(struct aws_allocator *allocator, struct aws_string *file_name) {
    struct aws_mmap_context *context = NULL;
    struct aws_mmap_context_posix_impl *impl = NULL;
    aws_mem_acquire_many(
        allocator, 2, &context, sizeof(struct aws_mmap_context), &impl, sizeof(struct aws_mmap_context_posix_impl));
    AWS_ZERO_STRUCT(*context);
    AWS_ZERO_STRUCT(*impl);

    aws_ref_count_init(&context->ref_count, context, s_mmap_context_destroy_callback_posix);
    impl->allocator = allocator;
    context->impl = impl;

    impl->fd = open(aws_string_c_str(file_name), O_RDONLY);
    if (impl->fd == -1) {
        goto error;
    }

    return context;
error:
    int error_code = errno;
    AWS_LOGF_ERROR(
        AWS_LS_S3_GENERAL,
        "Failed to create mmap context for file %s with error code %d",
        aws_string_c_str(file_name),
        error_code);

    if (error_code == ENOENT) {
        AWS_LOGF_ERROR(AWS_LS_S3_GENERAL, "File not found: %s", aws_string_c_str(file_name));
    } else if (error_code == EACCES) {
        AWS_LOGF_ERROR(AWS_LS_S3_GENERAL, "Permission denied for file: %s", aws_string_c_str(file_name));
    } else if (error_code == EMFILE) {
        AWS_LOGF_ERROR(AWS_LS_S3_GENERAL, "Too many open files when opening: %s", aws_string_c_str(file_name));
    } else if (error_code == ENFILE) {
        AWS_LOGF_ERROR(AWS_LS_S3_GENERAL, "System file table overflow when opening: %s", aws_string_c_str(file_name));
    }

    aws_translate_and_raise_io_error(error_code);
    return s_mmap_context_destroy(context);
}
#endif /* _WIN32 */
