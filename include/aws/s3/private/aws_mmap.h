
#ifndef AWS_MMAP_H
#define AWS_MMAP_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

/**
 * TODO: move this to part of <aws/common/file.h> instead
 */

#include <aws/s3/s3.h>

struct aws_allocator;

AWS_EXTERN_C_BEGIN

struct aws_mmap_context;

/**
 * Create a READ-ONLY mmap context.
 */
AWS_S3_API struct aws_mmap_context *aws_mmap_context_new(struct aws_allocator *allocator, struct aws_string *file_name);

/**
 * Acquires a reference to an aws_mmap_context.
 */
AWS_S3_API struct aws_mmap_context *aws_mmap_context_acquire(struct aws_mmap_context *context);

/**
 * Releases a reference to an aws_mmap_context. When the reference count reaches zero, the context is destroyed.
 */
AWS_S3_API struct aws_mmap_context *aws_mmap_context_release(struct aws_mmap_context *context);

/**
 * Get mapped content. OS will load the content into memory when the content is accessed.
 * You need to invoke `aws_mmap_context_unmap_content` on the `out_start_addr` once read from the memory is done,
 * otherwise, leak will happen
 *
 * @param context           The mmap context
 * @param length            The length of memory to map
 * @param offset            The offset starting at the file to map.
 * @param out_start_addr    The start address for the mapped memory, it will be a multiple
 *                          of the page size as returned by sysconf(_SC_PAGE_SIZE)
 * @return                  The mapped address starts from `offset` of the file
 **/
AWS_S3_API void *aws_mmap_context_map_content(
    struct aws_mmap_context *context,
    size_t length,
    size_t offset,
    void **out_start_addr);

/**
 * Remove any mappings for those entire pages containing any part of the address space of the process starting at addr
 * and continuing for len bytes
 *
 * @param mapped_addr   The start address of the mapped content
 * @param len           The length to free
 * @return AWS_OP_SUCCESS on success, AWS_OP_ERR on error.
 **/
AWS_S3_API int aws_mmap_context_unmap_content(void *mapped_addr, size_t len);

AWS_EXTERN_C_END
#endif /* AWS_MMAP_H */
