/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_accel_context_impl.h"

struct aws_s3_accel_context *aws_s3_accel_context_new(
    struct aws_allocator *allocator,
    struct aws_s3_client *client,
    const struct aws_s3_accel_request_options *options) {

    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(options);

    struct aws_s3_accel_context *context = aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_accel_context));

    if (context == NULL) {
        AWS_LOGF_ERROR(AWS_LS_S3_CLIENT, "Could not create accel context for client %p", (void *)client);
        return NULL;
    }

    context->allocator = allocator;
    aws_atomic_init_int(&context->ref_count, 1);

    aws_s3_client_acquire(client);
    context->client = client;

    context->user_data = options->user_data;
    context->body_callback = options->body_callback;
    context->finish_callback = options->finish_callback;

    return context;
}

void aws_s3_accel_context_acquire(struct aws_s3_accel_context *context) {
    AWS_PRECONDITION(context);

    aws_atomic_fetch_add(&context->ref_count, 1);
}

void aws_s3_accel_context_release(struct aws_s3_accel_context *context) {
    AWS_PRECONDITION(context);

    size_t new_ref_count = aws_atomic_fetch_sub(&context->ref_count, 1) - 1;

    if (new_ref_count > 0) {
        return;
    }

    if (context->client != NULL) {
        aws_s3_client_release(context->client);
        context->client = NULL;
    }

    aws_mem_release(context->allocator, context);
}
