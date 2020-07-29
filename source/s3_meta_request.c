/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_meta_request_impl.h"

struct aws_s3_meta_request *aws_s3_meta_request_new(
    struct aws_allocator *allocator,
    struct aws_s3_client *client,
    const struct aws_s3_meta_request_options *options) {

    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(options);

    struct aws_s3_meta_request *meta_request = aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_meta_request));

    if (meta_request == NULL) {
        AWS_LOGF_ERROR(AWS_LS_S3_CLIENT, "Could not create accel meta_request for client %p", (void *)client);
        return NULL;
    }

    meta_request->allocator = allocator;

    /* The caller gets one initial reference */
    aws_atomic_init_int(&meta_request->ref_count, 1);

    /* Grab a reference to passed in client and store it*/
    aws_s3_client_acquire(client);
    meta_request->client = client;

    meta_request->user_data = options->user_data;
    meta_request->body_callback = options->body_callback;
    meta_request->finish_callback = options->finish_callback;

    return meta_request;
}

void aws_s3_meta_request_acquire(struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);

    aws_atomic_fetch_add(&meta_request->ref_count, 1);
}

void aws_s3_meta_request_release(struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);

    size_t new_ref_count = aws_atomic_fetch_sub(&meta_request->ref_count, 1) - 1;

    if (new_ref_count > 0) {
        return;
    }

    if (meta_request->client != NULL) {
        aws_s3_client_release(meta_request->client);
        meta_request->client = NULL;
    }

    aws_mem_release(meta_request->allocator, meta_request);
}
