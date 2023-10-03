
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
 * TODO: supports different mode and extra.
 * Create a READ-ONLY mmap context.
 */
AWS_S3_API struct aws_mmap_context *aws_mmap_context_new(struct aws_allocator *allocator, const char *file_name);

/**
 * TODO: real refcount.
 */
AWS_S3_API struct aws_mmap_context *aws_mmap_context_release(struct aws_mmap_context *context);

/**
 * Get mapped content. OS will load the content into memory when the content is accessed.
 * You need to invoke `aws_mmap_context_unmap_content` once read from the memory, otherwise, leak will happen
 *
 * @param context       The mmap context
 * @return              The mapped address
 **/
AWS_S3_API void *aws_mmap_context_map_content(struct aws_mmap_context *context);

/**
 * Remove any mappings for those entire pages containing any part of the address space of the process starting at addr
 * and continuing for len bytes
 *
 * @param mapped_addr   The start address of the mapped content
 * @param len           The length to free
 * @return AWS_OP_SUCCESS on succes, AWS_OP_ERR on error.
 **/
AWS_S3_API int aws_mmap_context_unmap_content(void *mapped_addr, size_t len);

AWS_EXTERN_C_END
#endif /* AWS_MMAP_H */
