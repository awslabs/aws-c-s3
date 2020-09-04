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

/* Represents a "description" of a request, ie, enough information to create an actual message.  This is meant to be
 * queuable for retries. */
struct aws_s3_request_desc {

    /* Linked list node used for queuing. */
    struct aws_linked_list_node node;

    /* Part number that this request first to.  If this is not a part, this can be 0.  (S3 Part Numbers start at 1.) */
    uint32_t part_number;

    /* Tag that defines what the built request will actually consist of.  Request tags are different per meta request
     * type, and do not necessarily map 1:1 with actual S3 API requests.  For example, they can be more contextual, like
     * "first part" instead of just "part".) */
    uint32_t request_tag;
};

/* Represents an in-flight active request.  Does not persist past a the execution of the request. */
struct aws_s3_request {

    /* The HTTP message to send for this request. */
    struct aws_http_message *message;

    /* Optional part buffer to be used with this request. */
    struct aws_s3_part_buffer *part_buffer;
};

/* Meta requests control the flow (when signing, sending, etc. takes place) of their underlying requests.  This options
 * structure is used to configur that flow when triggering it. */
struct aws_s3_send_request_options {
    struct aws_s3_client *client;
    struct aws_s3_vip_connection *vip_connection;
    aws_s3_request_finished_callback_fn *finished_callback;
    void *user_data;
};

/* Represents the state for the sending of a single request.  This data is passed through the chain of function calls
 * that prepare/send a request. Visible in the header file for usage by derived meta requests types. */
struct aws_s3_send_request_work {
    struct aws_s3_client *client;
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

    /* Callback for when the meta request has stopped and will not send additional requests, but may have still have
     * work in flight.*/
    aws_s3_meta_request_stopped_callback_fn *stopped_callback;

    /* Callback for when the meta request has finished, and has no more work in flight. */
    aws_s3_meta_request_finish_fn *finish_callback;
};

struct aws_s3_meta_request_vtable {

    /* Pass back a request description of the next request that should be created.  If no work is available, this should
     * pass back a NULL pointer. */
    int (*next_request)(struct aws_s3_meta_request *meta_request, struct aws_s3_request_desc **request_desc);

    /* Given a reqeust description, create a new in-flight request. */
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
    struct aws_atomic_var ref_count;
    void *impl;
    struct aws_s3_meta_request_vtable *vtable;

    /* Initial HTTP Message that this meta request is based on. */
    struct aws_http_message *initial_request_message;

    /* Part size to use for uploads and downloads.  Passed down by the creating client. */
    uint64_t part_size;

    /* Event loop used for scheduling.  Passed down by the creating client. */
    struct aws_event_loop *event_loop;

    /* User data to be passed to each customer specified callback.*/
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

    /* Work reference ount.  This does not keep the meta request alive, but does delay the finish callback from taking
     * place. Like a normal reference count, this should be incremented from a place that already owns a work ref count.
     */
    struct aws_atomic_var work_ref_count;

    /* Error code that we finished the request with. */
    struct aws_atomic_var finished_error_code;

    /* Thread-safe flag for us to know if we have issued the finish callback already or not. */
    struct aws_atomic_var issued_finish_callback;

    struct {
        struct aws_mutex lock;

        /* True when the request is stopped, ie, isn't sending any more requests.  Other work at this point can still be
         * in flight via the work_ref_count, but no additional work should be started. */
        uint32_t stopped : 1;

        /* True if we're processing our "write" queue via tasks.  This is for passing buffers back to the customer. */
        uint32_t processing_write_queue : 1;

        /* Queue of aws_s3_request_desc structures that will be retried. */
        struct aws_linked_list retry_queue;

        /* Body of stream of the initial_request_message.  We store this here so that parts can take turns seeking to
         * their own specific position (which should be in close proximity of one another). */
        struct aws_input_stream *initial_body_stream;

        /* Queue of part buffers that we are sending back to the client. */
        /* TODO "write" queue probably isn't the best name. */
        struct aws_linked_list write_queue;

    } synced_data;
};

/* Creates a new auto-ranged get meta request.  This will do multiple parallel ranged-gets when appropriate. */
struct aws_s3_meta_request *aws_s3_meta_request_auto_ranged_get_new(
    struct aws_allocator *allocator,
    const struct aws_s3_meta_request_internal_options *options);

/* Creates a new auto-ranged put meta request.  This will do a multipart upload in parallel when appropriate. */
struct aws_s3_meta_request *aws_s3_meta_request_auto_ranged_put_new(
    struct aws_allocator *allocator,
    const struct aws_s3_meta_request_internal_options *options);

/* Tells the meta request to start sending another request, if there is one currently to send.  This is used the client.
 */
int aws_s3_meta_request_send_next_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_send_request_options *options,
    bool *out_found_work);

/* BEGIN - Meant only for use by derived types. */

/* Initialize the base meta request structure. */
int aws_s3_meta_request_init_base(
    struct aws_allocator *allocator,
    const struct aws_s3_meta_request_internal_options *options,
    void *impl,
    struct aws_s3_meta_request_vtable *vtable,
    struct aws_s3_meta_request *base_type);

/* Pass back this part buffer to the customer specified callback.  This assumes ownership of the part buffer passed in.
 */
int aws_s3_meta_request_write_part_buffer_to_caller(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_part_buffer *part_buffer);

/* Allocate a new reqeust description with the given options. */
struct aws_s3_request_desc *aws_s3_request_desc_new(
    struct aws_s3_meta_request *meta_request,
    uint32_t tag,
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

/* END - Meant only for use by derived types.  */

#endif /* AWS_S3_META_REQUEST_IMPL_H */
