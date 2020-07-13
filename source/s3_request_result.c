/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/s3_request_result.h"

int aws_s3_request_result_init(
    struct aws_s3_request_result *result,
    struct aws_allocator *allocator,
    struct aws_s3_request_result_vtable *vtable,
    void *impl) {

    result->allocator = allocator;
    result->vtable = vtable;
    result->impl = impl;

    aws_atomic_init_int(&result->ref_count, 1);

    return AWS_OP_SUCCESS;
}

void aws_s3_request_result_acquire(struct aws_s3_request_result *result) {
    aws_atomic_fetch_add(&result->ref_count, 1);
}

void aws_s3_request_result_release(struct aws_s3_request_result *result) {

    size_t new_ref_count = aws_atomic_fetch_sub(&result->ref_count, 1) - 1;

    if (new_ref_count > 0) {
        return;
    }

    result->vtable->destroy(result);
}

AWS_S3_API
int aws_s3_request_result_get_response_status(const struct aws_s3_request_result *result) {
    return result->response_status;
}

AWS_S3_API
int aws_s3_request_result_get_error_code(const struct aws_s3_request_result *result) {
    return result->error_code;
}

AWS_S3_API
void aws_s3_request_result_set_error_code(struct aws_s3_request_result *result, int32_t error_code) {
    result->error_code = error_code;
}
