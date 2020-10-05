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

typedef void(aws_s3_meta_request_work_available_fn)(struct aws_s3_meta_request *meta_request, void *user_data);

typedef void(aws_s3_request_finished_callback_fn)(void *user_data);

enum aws_s3_meta_request_state { AWS_S3_META_REQUEST_STATE_ACTIVE, AWS_S3_META_REQUEST_STATE_FINISHED };

/* Represents a "description" of a request, ie, enough information to create an actual message.  This is meant to be
 * queueable for retries. */
struct aws_s3_request_desc {

    /* Linked list node used for queuing. */
    struct aws_linked_list_node node;

    /* Part number that this request first to.  If this is not a part, this can be 0.  (S3 Part Numbers start at 1.) */
    uint32_t part_number;

    /* Tag that defines what the built request will actually consist of.  Request tags are different per meta request
     * type, and do not necessarily map 1:1 with actual S3 API requests.  For example, they can be more contextual, like
     * "first part" instead of just "part".) */
    int request_tag;
};

/* Represents an in-flight active request.  Does not persist past a the execution of the request. */
struct aws_s3_request {

    /* The HTTP message to send for this request. */
    struct aws_http_message *message;

    /* Optional part buffer to be used with this request. */
    struct aws_s3_part_buffer *part_buffer;
};

/* Meta requests control the flow (when signing, sending, etc. takes place) of their underlying requests.  This options
 * structure is used to configure that flow when triggering it. */
struct aws_s3_send_request_options {
    struct aws_s3_vip_connection *vip_connection;
    aws_s3_request_finished_callback_fn *finished_callback;
    void *user_data;
};

/* Represents the state for the sending of a single request.  This data is passed through the chain of function calls
 * that prepare/send a request. Visible in the header file for usage by derived meta requests types. */
struct aws_s3_send_request_work {
    struct aws_s3_vip_connection *vip_connection;
    struct aws_s3_meta_request *meta_request;
    struct aws_s3_request_desc *request_desc;
    struct aws_s3_request *request;

    aws_s3_request_finished_callback_fn *finished_callback;
    void *user_data;
};

/* Additional options that can be used internally (ie: by the client) without having to interfere with any user
 * specified options. */
struct aws_s3_meta_request_internal_options {
    const struct aws_s3_meta_request_options *options;

    void *user_data;

    struct aws_s3_client *client;

    /* Callback for when the meta request has more work to do.  */
    aws_s3_meta_request_work_available_fn *work_available_callback;

    /* Callback for when the meta request has finished, and has no more work in flight. */
    aws_s3_meta_request_finish_fn *finish_callback;
};

struct aws_s3_meta_request_vtable {
    bool (*has_work)(const struct aws_s3_meta_request *meta_request);

    /* Pass back a request description of the next request that should be created.  If no work is available, this should
     * pass back a NULL pointer. */
    int (*next_request)(struct aws_s3_meta_request *meta_request, struct aws_s3_request_desc **request_desc);

    /* Given a request description, create a new in-flight request. */
    struct aws_s3_request *(*request_factory)(
        struct aws_s3_meta_request *meta_request,
        struct aws_s3_client *client,
        struct aws_s3_request_desc *request_desc);

    /* Callbacks for all HTTP messages being processed by this meta request. */
    aws_http_on_incoming_headers_fn *incoming_headers;
    aws_http_on_incoming_header_block_done_fn *incoming_headers_block_done;
    aws_http_on_incoming_body_fn *incoming_body;
    aws_http_on_stream_complete_fn *stream_complete;

    /* Handle de-allocation of the meta request. */
    void (*destroy)(struct aws_s3_meta_request *);
};

/* This represents one meta request, ie, one accelerated file transfer.  One S3 meta request can represent multiple S3
 * requests.
 */
struct aws_s3_meta_request {
    struct aws_allocator *allocator;
    struct aws_ref_count ref_count;
    void *impl;
    struct aws_s3_meta_request_vtable *vtable;

    /* Initial HTTP Message that this meta request is based on. */
    struct aws_http_message *initial_request_message;

    /* Part size to use for uploads and downloads.  Passed down by the creating client. */
    const uint64_t part_size;

    /* Event loop used for scheduling.  Passed down by the creating client. */
    struct aws_event_loop *event_loop;

    /* User data to be passed to each customer specified callback.*/
    void *user_data;

    /* Customer specified callbacks. */
    aws_s3_meta_request_receive_body_callback_fn *body_callback;
    aws_s3_meta_request_finish_fn *finish_callback;
    aws_s3_meta_request_shutdown_fn *shutdown_callback;

    /* Internal user data and callbacks so that the owning client can generically listen for without working
     * around the customer specified callbacks. */
    void *internal_user_data;
    aws_s3_meta_request_work_available_fn *internal_work_available_callback;
    aws_s3_meta_request_finish_fn *internal_finish_callback;

    /* Internal reference count.  This does not keep the meta request alive, but does delay the finish callback from
     * taking place. Like a normal reference count, this should be incremented from a place that already owns an
     * internal ref count.
     */
    struct aws_ref_count internal_ref_count;

    struct {

        struct aws_linked_list_node node;

    } client_data;

    struct {
        struct aws_mutex lock;

        enum aws_s3_meta_request_state state;

        /* Queue of aws_s3_request_desc structures that will be retried. */
        struct aws_linked_list retry_queue;

        /* Body of stream of the initial_request_message.  We store this here so that parts can take turns seeking to
         * their own specific position (which should be in close proximity of one another). */
        struct aws_input_stream *initial_body_stream;

    } synced_data;
};

bool aws_s3_meta_request_has_work(const struct aws_s3_meta_request *meta_request);

/* Tells the meta request to start sending another request, if there is one currently to send.  This is used the client.
 */
void aws_s3_meta_request_send_next_request(
    struct aws_s3_meta_request *meta_request,
    const struct aws_s3_send_request_options *options);

/* BEGIN - Meant only for use by derived types. */

/* Initialize the base meta request structure. */
int aws_s3_meta_request_init_base(
    struct aws_allocator *allocator,
    const struct aws_s3_meta_request_internal_options *options,
    void *impl,
    struct aws_s3_meta_request_vtable *vtable,
    struct aws_s3_meta_request *base_type);

typedef void(aws_write_part_buffer_callback_fn)(void *user_data);

/* Pass back this part buffer to the customer specified callback.  This assumes ownership of the part buffer passed in.
 */
int aws_s3_meta_request_write_part_buffer_to_caller(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_part_buffer **part_buffer,
    aws_write_part_buffer_callback_fn callback,
    void *user_data);

/* Allocate a new request description with the given options. */
struct aws_s3_request_desc *aws_s3_request_desc_new(
    struct aws_s3_meta_request *meta_request,
    int request_tag,
    uint32_t part_number);

void aws_s3_request_desc_destroy(struct aws_s3_meta_request *meta_request, struct aws_s3_request_desc *request_desc);

/* Create a new s3 request structure with the given options. */
struct aws_s3_request *aws_s3_request_new(struct aws_s3_meta_request *meta_request, struct aws_http_message *message);

void aws_s3_request_destroy(struct aws_s3_meta_request *meta_request, struct aws_s3_request *request);

/* Push a request description into the retry queue.  This assumes ownership of the request desc, and will NULL out the
 * passed in pointer-to-pointer to help enforce this. */
int aws_s3_meta_request_queue_retry(struct aws_s3_meta_request *meta_request, struct aws_s3_request_desc **in_out_desc);

/* Tells the meta request to stop, with an error code for indicating failure when necessary. */
void aws_s3_meta_request_finish(struct aws_s3_meta_request *meta_request, int error_code);

void aws_s3_meta_request_notify_work_available(struct aws_s3_meta_request *meta_request);

/* END - Meant only for use by derived types.  */

#endif /* AWS_S3_META_REQUEST_IMPL_H */
