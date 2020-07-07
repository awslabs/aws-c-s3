#ifndef AWS_S3_REQUEST_RESULT_IMPL_H
#define AWS_S3_REQUEST_RESULT_IMPL_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/atomics.h>

struct aws_s3_request_result;

struct aws_s3_request_result_vtable {
    void *(*get_output)(struct aws_s3_request_result *result);
    void (*destroy)(struct aws_s3_request_result *result);
};

struct aws_s3_request_result {
    struct aws_allocator *allocator;
    struct aws_s3_request_result_vtable *vtable;
    void *impl;
    struct aws_atomic_var ref_count;

    int response_status;
};

int aws_s3_request_result_init(
    struct aws_s3_request_result *result,
    struct aws_allocator *allocator,
    struct aws_s3_request_result_vtable *vtable,
    void *impl);

#endif
