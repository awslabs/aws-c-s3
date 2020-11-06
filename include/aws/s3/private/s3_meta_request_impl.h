#ifndef AWS_S3_META_REQUEST_IMPL_H
#define AWS_S3_META_REQUEST_IMPL_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/atomics.h>
#include <aws/common/linked_list.h>
#include <aws/common/mutex.h>
#include <aws/common/ref_count.h>
#include <aws/common/task_scheduler.h>
#include <aws/http/request_response.h>

#include "aws/s3/private/s3_part_buffer.h"
#include "aws/s3/s3_client.h"

struct aws_s3_client;
struct aws_s3_meta_request;
struct aws_s3_request;
struct aws_s3_request_options;
struct aws_http_headers;

typedef void(aws_s3_meta_request_work_available_fn)(struct aws_s3_meta_request *meta_request, void *user_data);

typedef void(aws_s3_meta_request_write_body_callback_fn)(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request);

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

    struct aws_ref_count ref_count;

    /* Owning meta request. */
    struct aws_s3_meta_request *meta_request;

    /* The HTTP message to send for this request. */
    struct aws_http_message *message;

    /* Recorded response headers for the request. */
    struct aws_http_headers *response_headers;

    /* Optional part buffer to be used with this request. */
    struct aws_s3_part_buffer *part_buffer;

    /* TODO put these in a struct? */
    /* Callback used for aws_s3_meta_request_write_body_to_caller. */
    aws_s3_meta_request_write_body_callback_fn *write_body_callback;

    /* Task used for aws_s3_meta_request_write_body_to_caller. */
    struct aws_task write_body_task;
};

/* Additional options that can be used internally (ie: by the client) without having to interfere with any user
 * specified options. */
struct aws_s3_meta_request_internal_options {
    const struct aws_s3_meta_request_options *options;

    struct aws_s3_client *client;
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
    int (*incoming_headers)(
        struct aws_http_stream *stream,
        enum aws_http_header_block header_block,
        const struct aws_http_header *headers,
        size_t headers_count,
        struct aws_s3_vip_connection *vip_connection);

    int (*incoming_headers_block_done)(
        struct aws_http_stream *stream,
        enum aws_http_header_block header_block,
        struct aws_s3_vip_connection *vip_connection);

    int (*incoming_body)(
        struct aws_http_stream *stream,
        const struct aws_byte_cursor *data,
        struct aws_s3_vip_connection *vip_connection);

    void (
        *stream_complete)(struct aws_http_stream *stream, int error_code, struct aws_s3_vip_connection *vip_connection);

    /* Handle de-allocation of the meta request. */
    void (*destroy)(struct aws_s3_meta_request *);
};

/* This represents one meta request, ie, one accelerated file transfer.  One S3 meta request can represent multiple S3
 * requests.
 */
struct aws_s3_meta_request {
    struct aws_allocator *allocator;

    struct aws_ref_count ref_count;

    /* Internal reference count.  This does not keep the meta request alive, but does delay the finish callback from
     * taking place. Like a normal reference count, this should be incremented from a place that already owns an
     * internal ref count.
     */
    struct aws_ref_count internal_ref_count;

    void *impl;
    struct aws_s3_meta_request_vtable *vtable;

    /* Client that created this meta request which also processes this request.  After the meta request is finished,
     * this reference is removed. */
    struct aws_s3_client *client;

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

    struct {

        /* Linked list node for the meta requests linked list in the client. */
        /* Note: this needs to be first for using AWS_CONTAINER_OF with the nested structure. */
        struct aws_linked_list_node node;

        /* List of VIP connections currently processing this meta request. */
        struct aws_linked_list referenced_vip_connections;

        /* True when this meta request has already been added to the client. */
        bool added_to_client;

    } threaded_data;

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

/* Creates a new auto-ranged get meta request.  This will do multiple parallel ranged-gets when appropriate. */
struct aws_s3_meta_request *aws_s3_meta_request_auto_ranged_get_new(
    struct aws_allocator *allocator,
    const struct aws_s3_meta_request_internal_options *options);

/* Creates a new auto-ranged put meta request.  This will do a multipart upload in parallel when appropriate. */
struct aws_s3_meta_request *aws_s3_meta_request_auto_ranged_put_new(
    struct aws_allocator *allocator,
    const struct aws_s3_meta_request_internal_options *options);

/* Tells the meta request to start sending another request, if there is one currently to send.  This is used by the
 * client.
 */
void aws_s3_meta_request_send_next_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_vip_connection *vip_connection,
    aws_s3_request_finished_callback_fn *finished_callback,
    void *user_data);

/* BEGIN - Meant only for use by derived types. */

/* Initialize the base meta request structure. */
int aws_s3_meta_request_init_base(
    struct aws_allocator *allocator,
    const struct aws_s3_meta_request_internal_options *options,
    void *impl,
    struct aws_s3_meta_request_vtable *vtable,
    struct aws_s3_meta_request *base_type);

/* Pass back the part buffer of the request as a response body to the user */
void aws_s3_meta_request_write_body_to_caller(
    struct aws_s3_request *request,
    aws_s3_meta_request_write_body_callback_fn *callback);

/* Allocate a new request description with the given options. */
struct aws_s3_request_desc *aws_s3_request_desc_new(
    struct aws_s3_meta_request *meta_request,
    int request_tag,
    uint32_t part_number);

void aws_s3_request_desc_destroy(struct aws_s3_meta_request *meta_request, struct aws_s3_request_desc *request_desc);

/* Create a new s3 request structure with the given options. */
struct aws_s3_request *aws_s3_request_new(struct aws_s3_meta_request *meta_request, struct aws_http_message *message);

void aws_s3_request_acquire(struct aws_s3_request *request);

void aws_s3_request_release(struct aws_s3_request *request);

/* Push a request description into the retry queue.  This assumes ownership of the request desc, */
void aws_s3_meta_request_queue_retry(struct aws_s3_meta_request *meta_request, struct aws_s3_request_desc *desc);

/* Tells the meta request to stop, with an error code for indicating failure when necessary. */
void aws_s3_meta_request_finish(struct aws_s3_meta_request *meta_request, int error_code);

void aws_s3_meta_request_internal_acquire(struct aws_s3_meta_request *meta_request);

void aws_s3_meta_request_internal_release(struct aws_s3_meta_request *meta_request);

/* END - Meant only for use by derived types.  */

#endif /* AWS_S3_META_REQUEST_IMPL_H */
