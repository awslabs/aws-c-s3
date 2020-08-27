#ifndef AWS_S3_META_REQUEST_IMPL_H
#define AWS_S3_META_REQUEST_IMPL_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/atomics.h>
#include <aws/common/linked_list.h>
#include <aws/common/mutex.h>
#include <aws/common/task_scheduler.h>

#include "aws/s3/private/s3_request.h"
#include "aws/s3/s3_client.h"

struct aws_s3_client;
struct aws_s3_meta_request;
struct aws_s3_request_options;

enum aws_s3_meta_request_gen_ranged_flag {
    AWS_S3_META_REQUEST_GEN_RANGED_FLAG_SKIP_FIRST = 0x00000001,
    AWS_S3_META_REQUEST_GEN_RANGED_FLAG_QUEUING_DONE = 0x00000002
};

enum s3_push_new_request_flag { S3_PUSH_NEW_REQUEST_FLAG_QUEUING_DONE = 0x00000001 };

struct aws_s3_meta_request_internal_options {
    const struct aws_s3_meta_request_options *options;
    void *user_data;

    aws_s3_meta_request_finish_fn *finish_callback;
};

/* This represents one meta request, ie, one accelerated file transfer.  Anything needed across different calls for an
 * acceleration of one particular S3 request will be stored here.
 */
struct aws_s3_meta_request {
    struct aws_allocator *allocator;
    struct aws_atomic_var ref_count;

    struct aws_s3_client *client;

    /* Initial HTTP Message that this meta request is based on. Immutable after creation until destruction. */
    struct aws_http_message *initial_request_message;

    /* Part size to use for uploads and downloads.  This is passed down by the creating client. Immutable after
     * creation. */
    uint64_t part_size;

    struct aws_string *upload_id;

    /* User data to be passed to each callback.*/
    void *user_data;

    /* Customer specified callbacks. */
    aws_s3_meta_request_receive_body_callback_fn *body_callback;
    aws_s3_meta_request_finish_fn *finish_callback;
    aws_s3_meta_request_shutdown_fn *shutdown_callback;

    /* Internal user data and callbacks so that the owning client can generically listen for finish without working
     * around the customer specified callbacks. */
    void *internal_user_data;
    aws_s3_meta_request_finish_fn *internal_finish_callback;

    struct {
        struct aws_mutex lock;

        /* Flag that is set when no other s3 requests will be queued. */
        uint32_t queue_finished_populating : 1;

        uint32_t is_writing_to_caller : 1;

        uint32_t cleaning_up : 1;

        /* Number of requests popped from queue but not finished yet.*/
        uint32_t in_flight_requests;

        /* S3 Requests for this file transfer. This can change as the meta request progresses.  Once there are no other
         * requests to push, queue_finished_populating must be set. */
        struct aws_linked_list pending_request_queue;

        struct aws_linked_list finished_requests;

        struct aws_linked_list write_queue;

        struct aws_task write_to_caller_task;

        struct aws_input_stream *initial_body_stream;

    } synced_data;
};

/* Create a new s3 meta request given a client and options. */
struct aws_s3_meta_request *aws_s3_meta_request_new(
    struct aws_allocator *allocator,
    struct aws_s3_client *client,
    const struct aws_s3_meta_request_internal_options *options);

int aws_s3_meta_requests_get_total_object_size(
    struct aws_s3_meta_request *meta_request,
    int64_t *out_total_object_size);

int aws_s3_meta_request_set_upload_id(
    struct aws_s3_meta_request *meta_request,
    struct aws_byte_cursor *upload_id_cursor);

struct aws_string *aws_s3_meta_request_get_upload_id(struct aws_s3_meta_request *meta_request);

int aws_s3_meta_request_copy_part_to_part_buffer(
    struct aws_s3_meta_request *meta_request,
    uint32_t part_number,
    struct aws_s3_part_buffer *dest_part_buffer);

void aws_s3_meta_request_write_part_buffer_to_caller(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_part_buffer *part_buffer);

/* Pop a request from the queue onto the given pipeline.. */
struct aws_s3_request *aws_s3_meta_request_pop_request(struct aws_s3_meta_request *meta_request);

int aws_s3_meta_request_generate_ranged_requests(
    struct aws_s3_meta_request *meta_request,
    enum aws_s3_request_type request_type,
    uint64_t range_start,
    uint64_t range_end,
    uint32_t flags);

int aws_s3_meta_request_push_new_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request_options *request_options,
    uint32_t flags);

void aws_s3_meta_request_finish_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    int error_code);

typedef void(aws_s3_iterate_finished_requests_callback_fn)(struct aws_s3_request *request, void *user_data);

void aws_s3_meta_request_iterate_finished_requests(
    struct aws_s3_meta_request *meta_request,
    aws_s3_iterate_finished_requests_callback_fn *callback,
    void *user_data);

#endif /* AWS_S3_META_REQUEST_IMPL_H */
