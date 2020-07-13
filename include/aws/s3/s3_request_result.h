#ifndef AWS_S3_REQUEST_RESULT_H
#define AWS_S3_REQUEST_RESULT_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/atomics.h>
#include <aws/s3/s3.h>

struct aws_s3_request_result;

struct aws_s3_request_result_vtable {
    void (*destroy)(struct aws_s3_request_result *result);
};

struct aws_s3_request_result {
    struct aws_allocator *allocator;
    struct aws_s3_request_result_vtable *vtable;
    void *impl;
    struct aws_atomic_var ref_count;
    int32_t error_code;
    int32_t response_status;
};

AWS_EXTERN_C_BEGIN

AWS_S3_API
void aws_s3_request_result_acquire(struct aws_s3_request_result *result);

AWS_S3_API
void aws_s3_request_result_release(struct aws_s3_request_result *result);

AWS_S3_API
int aws_s3_request_result_get_response_status(const struct aws_s3_request_result *result);

AWS_S3_API
int aws_s3_request_result_get_error_code(const struct aws_s3_request_result *result);

AWS_S3_API
void aws_s3_request_result_set_error_code(struct aws_s3_request_result *result, int32_t error_code);

AWS_S3_API
int aws_s3_request_result_init(
    struct aws_s3_request_result *result,
    struct aws_allocator *allocator,
    struct aws_s3_request_result_vtable *vtable,
    void *impl);

AWS_EXTERN_C_END

#endif /* AWS_S3_REQUEST_RESULT_H */
