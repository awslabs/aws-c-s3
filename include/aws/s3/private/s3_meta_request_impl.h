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
#include <aws/http/request_response.h>

#include "aws/s3/s3_client.h"

struct aws_s3_client;
struct aws_s3_meta_request;
struct aws_s3_request_options;

typedef void(
    aws_s3_meta_request_stopped_callback_fn)(struct aws_s3_meta_request *meta_request, int error_code, void *user_data);

typedef void(aws_s3_request_finished_callback_fn)(void *user_data);

struct aws_s3_request_desc {
    struct aws_linked_list_node node;
    uint32_t part_number;
    uint32_t request_tag;
};

struct aws_s3_request {
    struct aws_http_message *message;
    struct aws_s3_part_buffer *part_buffer;
};

struct aws_s3_send_request_options {
    struct aws_s3_client *client;
    struct aws_s3_vip_connection *vip_connection;
    aws_s3_request_finished_callback_fn *finished_callback;
    void *user_data;
};

struct aws_s3_send_request_work {
    struct aws_s3_client *client;
    struct aws_s3_vip_connection *vip_connection;
    struct aws_s3_meta_request *meta_request;
    struct aws_s3_request_desc *request_desc;
    struct aws_s3_request *request;

    aws_s3_request_finished_callback_fn *finished_callback;
    void *user_data;
};

struct aws_s3_meta_request_internal_options {
    const struct aws_s3_meta_request_options *options;
    void *user_data;

    struct aws_s3_client *client;
    aws_s3_meta_request_stopped_callback_fn *stopped_callback;
    aws_s3_meta_request_finish_fn *finish_callback;
};

struct aws_s3_meta_request_vtable {

    int (*next_request)(struct aws_s3_meta_request *meta_request, struct aws_s3_request_desc **request_desc);

    struct aws_s3_request *(*request_factory)(
        struct aws_s3_meta_request *meta_request,
        struct aws_s3_client *client,
        struct aws_s3_request_desc *request_desc);

    aws_http_on_incoming_headers_fn *incoming_headers;
    aws_http_on_incoming_header_block_done_fn *incoming_headers_block_done;
    aws_http_on_incoming_body_fn *incoming_body;

    void (*stream_complete)(struct aws_http_stream *stream, int error_code, void *user_data);

    void (*destroy)(struct aws_s3_meta_request *);
};

/* This represents one meta request, ie, one accelerated file transfer.  Anything needed across different calls for an
 * acceleration of one particular S3 request will be stored here.
 */
struct aws_s3_meta_request {
    struct aws_allocator *allocator;
    struct aws_atomic_var ref_count;
    void *impl;
    struct aws_s3_meta_request_vtable *vtable;

    /* Initial HTTP Message that this meta request is based on. Immutable after creation until destruction. */
    struct aws_http_message *initial_request_message;

    /* Part size to use for uploads and downloads.  This is passed down by the creating client. Immutable after
     * creation. */
    uint64_t part_size;

    struct aws_event_loop *event_loop;

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
    aws_s3_meta_request_stopped_callback_fn *internal_stopped_callback;

    struct aws_atomic_var work_ref_count;
    struct aws_atomic_var finished_error_code;
    struct aws_atomic_var issued_finish_callback;

    struct {
        struct aws_mutex lock;

        uint32_t stopped : 1;
        uint32_t processing_write_queue : 1;

        struct aws_linked_list retry_queue;
        struct aws_input_stream *initial_body_stream;
        struct aws_linked_list write_queue;

    } synced_data;
};

struct aws_s3_meta_request *aws_s3_meta_request_auto_ranged_get_new(
    struct aws_allocator *allocator,
    const struct aws_s3_meta_request_internal_options *options);

struct aws_s3_meta_request *aws_s3_meta_request_auto_ranged_put_new(
    struct aws_allocator *allocator,
    const struct aws_s3_meta_request_internal_options *options);

int aws_s3_meta_request_send_next_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_send_request_options *options,
    bool *out_found_work);

/* BEGIN - Should only be called by derived types */
int aws_s3_meta_request_init_base(
    struct aws_allocator *allocator,
    const struct aws_s3_meta_request_internal_options *options,
    void *impl,
    struct aws_s3_meta_request_vtable *vtable,
    struct aws_s3_meta_request *base_type);

int aws_s3_meta_request_write_part_buffer_to_caller(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_part_buffer *part_buffer);

struct aws_s3_request_desc *aws_s3_request_desc_new(
    struct aws_s3_meta_request *meta_request,
    uint32_t tag,
    uint32_t part_number);

void aws_s3_request_desc_destroy(struct aws_s3_meta_request *meta_request, struct aws_s3_request_desc *request_desc);

struct aws_s3_request *aws_s3_request_new(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request_desc *request_desc,
    struct aws_http_message *message);

void aws_s3_request_destroy(struct aws_s3_meta_request *meta_request, struct aws_s3_request *request);

int aws_s3_meta_request_queue_retry(struct aws_s3_meta_request *meta_request, struct aws_s3_request_desc **in_out_desc);

void aws_s3_meta_request_finish(struct aws_s3_meta_request *meta_request, int error_code);
/* END - Should only be called by derived types */

#endif /* AWS_S3_META_REQUEST_IMPL_H */
