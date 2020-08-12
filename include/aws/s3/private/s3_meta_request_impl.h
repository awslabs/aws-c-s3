#ifndef AWS_S3_META_REQUEST_IMPL_H
#define AWS_S3_META_REQUEST_IMPL_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/atomics.h>
#include <aws/common/linked_list.h>
#include <aws/common/mutex.h>

#include "aws/s3/s3_client.h"

struct aws_s3_meta_request_internal_options {
    const struct aws_s3_meta_request_options *options;
    void *user_data;
    aws_s3_meta_request_request_finish_fn *finish_callback;
};

/* This represents one meta request, ie, one accelerated file transfer.  Anything needed across different calls for an
 * acceleration of one particular S3 request will be stored here.
 */
struct aws_s3_meta_request {
    struct aws_allocator *allocator;
    struct aws_atomic_var ref_count;

    /* Initial HTTP Message that this meta request is based on. Immutable after creation until destruction. */
    struct aws_http_message *initial_request_message;

    /* Part size to use for uploads and downloads.  Immutable after creation. */
    uint64_t part_size;

    /* Timestamp at which this meta request was initiated. Immutable after creation. */
    uint64_t initiated_timestamp;

    struct aws_mutex lock;

    /* S3 Requests for this file transfer. This can change as the meta request progresses.  Once there are no other
     * requests to push, queue_finished_populating must be set. Requires lock. */
    struct aws_linked_list request_queue;

    /* Flag that is set when no other s3 requests will be queued. Requires lock. */
    bool queue_finished_populating;

    /* Number of requests popped from queue but not finished yet.*/
    uint32_t outstanding_requests;

    /* User data to be apssed to each callback.*/
    void *user_data;

    /* Customer specified callbacks. */
    aws_s3_meta_request_receive_body_callback_fn *body_callback;
    aws_s3_meta_request_request_finish_fn *finish_callback;

    /* Internal user data and callbacks specified by the aws-c-s3 library.*/
    void *internal_user_data;
    aws_s3_meta_request_request_finish_fn *internal_finish_callback;
};

/* Create a new s3 meta request given a client and options. */
struct aws_s3_meta_request *aws_s3_meta_request_new(
    struct aws_allocator *allocator,
    struct aws_s3_client *client,
    const struct aws_s3_meta_request_internal_options *options);

/* Pop a request from the queue. */
struct aws_s3_request *aws_s3_meta_request_pop_request(struct aws_s3_meta_request *meta_request);

/* Push a request back onto the queue.  It is not valid to push a request from different meta request. */
int aws_s3_meta_request_retry_request(struct aws_s3_meta_request *meta_request, struct aws_s3_request *request);

/* Get the time at which the request was initiated. */
uint64_t aws_s3_meta_request_get_initiated_timestamp(const struct aws_s3_meta_request *meta_request);

#endif /* AWS_S3_META_REQUEST_IMPL_H */
