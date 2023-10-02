
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

struct aws_mmap_context {
    void *impl;
    const char *content;
};

/**
 * TODO: supports different mode and extra.
 * Create a READ-ONLY mmap context.
 */
AWS_S3_API struct aws_mmap_context *aws_mmap_context_new(struct aws_allocator *allocator, const char *file_name);

/**
 * TODO: real refcount.
 */
AWS_S3_API struct aws_mmap_context *aws_mmap_context_release(struct aws_mmap_context *context);

AWS_EXTERN_C_END
#endif /* AWS_MMAP_H */
