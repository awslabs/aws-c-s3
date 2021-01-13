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

#include "aws/s3/private/s3_client_impl.h"

struct aws_s3_client;
struct aws_s3_vip_connection;
struct aws_s3_meta_request;
struct aws_s3_request;
struct aws_s3_request_options;
struct aws_http_headers;
struct aws_http_make_request_options;
struct aws_retry_strategy;
struct aws_byte_buffer;

typedef void(aws_s3_meta_request_work_available_fn)(struct aws_s3_meta_request *meta_request, void *user_data);

typedef void(aws_s3_meta_request_write_body_finished_callback_fn)(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request);

typedef void(aws_s3_request_finished_callback_fn)(void *user_data);

enum aws_s3_meta_request_state {
    AWS_S3_META_REQUEST_STATE_ACTIVE,
    AWS_S3_META_REQUEST_STATE_CANCELLING,
    AWS_S3_META_REQUEST_STATE_FINISHED,
};

enum aws_s3_request_desc_flags {
    AWS_S3_REQUEST_DESC_RECORD_RESPONSE_HEADERS = 0x00000001,
    AWS_S3_REQUEST_DESC_STREAM_RESPONSE_BODY = 0x00000002,
    AWS_S3_REQUEST_DESC_PART_SIZE_RESPONSE_BODY = 0x0000004,
};

/* Represents an in-flight active request.  Does not persist past a the execution of the request. */
struct aws_s3_request {

    /* Linked list node used for queuing. */
    struct aws_linked_list_node node;

    /* TODO Ref count on the request is no longer needed--only one part of code should ever be holding onto a request,
     * and we can just transfer ownership.*/
    struct aws_ref_count ref_count;

    struct aws_allocator *allocator;

    /* Owning meta request. */
    struct aws_s3_meta_request *meta_request;

    /* Request body to use when sending the request. The contents of this body will be re-used if a request is
     * retried.*/
    struct aws_byte_buf request_body;

    /* Part number that this request refers to.  If this is not a part, this can be 0.  (S3 Part Numbers start at 1.)
     * However, must currently be a valid part number (ie: greater than 0) if the response body is to be streamed to the
     * caller.
     */
    uint32_t part_number;

    /* Tag that defines what the built request will actually consist of.  This is meant to be space for an enum defined
     * by the derived type.  Request tags do not necessarily map 1:1 with actual S3 API requests.  For example, they can
     * be more contextual, like "first part" instead of just "part".) */
    /* TODO we could potentially combine these with the bitfields below. */
    int request_tag;

    /* When true, response headers from the request will be stored in the request's response_headers variable. */
    uint32_t record_response_headers : 1;

    /* When true, the response body will be streamed back to the caller. */
    uint32_t stream_response_body : 1;

    /* When true, the response body buffer will be allocated in the size of a part. */
    uint32_t part_size_response_body : 1;

    /* Members of this structure will be repopulated each time the request is sent.  For example, If the request fails,
     * and needs to be retried, then the members of this structure will be cleaned up and re-populated on the next send.
     */
    struct {

        /* The HTTP message to send for this request. */
        struct aws_http_message *message;

        /* Signable created for the above message. */
        struct aws_signable *signable;

        /* Recorded response headers for the request. Set only when the request desc has record_response_headers set to
         * true or when this response indicates an error. */
        struct aws_http_headers *response_headers;

        /* Recorded response body of the request. */
        struct aws_byte_buf response_body;

        /* Returned response status of this request. */
        int response_status;

    } send_data;
};

struct aws_s3_meta_request_vtable {
    /* Pass back a request with a populated description.  If no work is available, this is allowed to pass back a NULL
     * pointer. */
    int (*next_request)(struct aws_s3_meta_request *meta_request, struct aws_s3_request **out_request);

    /* Called when sending of the request has finished. */
    void (*send_request_finish)(
        struct aws_s3_vip_connection *vip_connection,
        struct aws_http_stream *stream,
        int error_code);

    /* Given a request, prepare it for sending based on its description. Should call aws_s3_request_setup_send_data
     * before exitting. */
    int (*prepare_request)(
        struct aws_s3_meta_request *meta_request,
        struct aws_s3_client *client,
        struct aws_s3_vip_connection *vip_connection,
        bool is_initial_prepare);

    void (*init_signing_date_time)(struct aws_s3_meta_request *meta_request, struct aws_date_time *date_time);

    /* Sign the request on the given VIP Connection. */
    int (*sign_request)(struct aws_s3_meta_request *meta_request, struct aws_s3_vip_connection *vip_connection);

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

    int (*stream_complete)(struct aws_http_stream *stream, struct aws_s3_vip_connection *vip_connection);

    /* Called when an aws_s3_request created by this meta request has been destroyed. */
    void (*notify_request_destroyed)(struct aws_s3_meta_request *meta_request, struct aws_s3_request *request);

    /* Finish the meta request either succeed or failed. */
    void (*finish)(
        struct aws_s3_meta_request *,
        struct aws_s3_request *failed_request,
        int response_status,
        int error_code);

    /* Handle de-allocation of the meta request. */
    void (*destroy)(struct aws_s3_meta_request *);
};

/**
 * This represents one meta request, ie, one accelerated file transfer.  One S3 meta request can represent multiple S3
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
    const size_t part_size;

    struct aws_cached_signing_config_aws *cached_signing_config;

    /* User data to be passed to each customer specified callback.*/
    void *user_data;

    /* Customer specified callbacks. */
    aws_s3_meta_request_headers_callback_fn *headers_callback;
    aws_s3_meta_request_receive_body_callback_fn *body_callback;
    aws_s3_meta_request_finish_fn *finish_callback;
    aws_s3_meta_request_shutdown_fn *shutdown_callback;

    struct {
        struct aws_mutex lock;

        /* Client that created this meta request which also processes this request.  After the meta request is finished,
         * this reference is removed. */
        struct aws_s3_client *client;

        /* Body of stream of the initial_request_message.  We store this here so that parts can take turns seeking to
         * their own specific position (which should be in close proximity of one another). */
        struct aws_input_stream *initial_body_stream;

        /* Priority queue for pending streaming requests.  We use a priority queue to keep parts in order so that we
         * can stream them to the caller in order. */
        struct aws_priority_queue pending_body_streaming_requests;

        /* Current state of the meta request. */
        enum aws_s3_meta_request_state state;

        /* The next expected streaming part number needed to continue streaming part bodies.  (For example, this will
         * initially be 1 for part 1, and after that part is received, it will be 2, then 3, etc.. */
        uint32_t next_streaming_part;

    } synced_data;

    /* Anything in this structure should only ever be accessed by the client. */
    struct {
        /* Event loop to be used for streaming the response bodies for this meta request.*/
        struct aws_event_loop *body_streaming_event_loop;
    } client_data;

    /* Anything in this structure should only ever be accessed by the client on its process work event loop task. */
    struct {

        /* Linked list node for the meta requests linked list in the client. */
        /* Note: this needs to be first for using AWS_CONTAINER_OF with the nested structure. */
        struct aws_linked_list_node node;

        /* True if this meta request is currently in the client's list. */
        bool scheduled;

    } client_process_work_threaded_data;
};

/* Creates a new auto-ranged get meta request.  This will do multiple parallel ranged-gets when appropriate. */
struct aws_s3_meta_request *aws_s3_meta_request_auto_ranged_get_new(
    struct aws_allocator *allocator,
    struct aws_s3_client *client,
    size_t part_size,
    const struct aws_s3_meta_request_options *options);

/* Creates a new auto-ranged put meta request.  This will do a multipart upload in parallel when appropriate. */
struct aws_s3_meta_request *aws_s3_meta_request_auto_ranged_put_new(
    struct aws_allocator *allocator,
    struct aws_s3_client *client,
    size_t part_size,
    uint32_t num_parts,
    const struct aws_s3_meta_request_options *options);

/* Creates a new default meta request. This will send the request as is and pass back the response. */
struct aws_s3_meta_request *aws_s3_meta_request_default_new(
    struct aws_allocator *allocator,
    struct aws_s3_client *client,
    uint64_t content_length,
    const struct aws_s3_meta_request_options *options);

struct aws_s3_request *aws_s3_meta_request_next_request(struct aws_s3_meta_request *meta_request);

/* lock will be acquired and release by this function */
bool aws_s3_meta_request_check_active(struct aws_s3_meta_request *meta_request);

int aws_s3_meta_request_make_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_client *client,
    struct aws_s3_vip_connection *vip_connection);

/* Tells the meta request to stop, with an error code for indicating failure when necessary. */
void aws_s3_meta_request_finish(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *failed_request,
    int response_status,
    int error_code);

AWS_EXTERN_C_BEGIN

AWS_S3_API
bool aws_s3_meta_request_is_finished(struct aws_s3_meta_request *meta_request);

/* ******************************************** */
/* BEGIN - Meant only for use by derived types. */
/* ******************************************** */

/* Initialize the base meta request structure. */
AWS_S3_API
int aws_s3_meta_request_init_base(
    struct aws_allocator *allocator,
    struct aws_s3_client *client,
    size_t part_size,
    const struct aws_s3_meta_request_options *options,
    void *impl,
    struct aws_s3_meta_request_vtable *vtable,
    struct aws_s3_meta_request *base_type);

/* Create a new s3 request structure with the given options. */
AWS_S3_API
struct aws_s3_request *aws_s3_request_new(
    struct aws_s3_meta_request *meta_request,
    int request_tag,
    uint32_t part_number,
    uint32_t flags);

/* Set up the request to be sent. Called each time before the request is sent. Will initially call
 * aws_s3_request_clean_up_send_data to clear out anything previously existing in send_data. */
AWS_S3_API
void aws_s3_request_setup_send_data(struct aws_s3_request *request, struct aws_http_message *message);

/* Clear out send_data members so that they can be repopulated before the next send. */
AWS_S3_API
void aws_s3_request_clean_up_send_data(struct aws_s3_request *request);

AWS_S3_API
void aws_s3_request_acquire(struct aws_s3_request *request);

AWS_S3_API
void aws_s3_request_release(struct aws_s3_request *request);

AWS_S3_API
void aws_s3_meta_request_lock_synced_data(struct aws_s3_meta_request *meta_request);

AWS_S3_API
void aws_s3_meta_request_unlock_synced_data(struct aws_s3_meta_request *meta_request);

/* Call to have the meta request notify the owning client (if one exists) that there is more work to be done. */
void aws_s3_meta_request_push_to_client(struct aws_s3_meta_request *meta_request);

/* Gets the client reference in the meta request synced_data, acquiring a reference to it if it exists. After calling
 * this function, it is necessary to release that reference. */
AWS_S3_API
struct aws_s3_client *aws_s3_meta_request_acquire_client(struct aws_s3_meta_request *meta_request);

AWS_S3_API
void aws_s3_meta_request_init_signing_date_time_default(
    struct aws_s3_meta_request *meta_request,
    struct aws_date_time *date_time);

AWS_S3_API
int aws_s3_meta_request_sign_request_default(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_vip_connection *vip_connection);

AWS_S3_API
void aws_s3_meta_request_finish_default(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *failed_request,
    int response_status,
    int error_code);

AWS_S3_API
void aws_s3_meta_request_send_request_finish_default(
    struct aws_s3_vip_connection *vip_connection,
    struct aws_http_stream *stream,
    int error_code);

int aws_s3_meta_request_read_body(struct aws_s3_meta_request *meta_request, struct aws_byte_buf *buffer);

int aws_s3_meta_request_read_body_synced(struct aws_s3_meta_request *meta_request, struct aws_byte_buf *buffer);
/* ******************************************** */
/* END - Meant only for use by derived types.  */
/* ******************************************** */

/* ******************************************** */
/* BEGIN - Exposed only for use in tests */
/* ******************************************** */
AWS_S3_API
void aws_s3_meta_request_body_streaming_push_synced(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request);

AWS_S3_API
struct aws_s3_request *aws_s3_meta_request_body_streaming_pop_synced(struct aws_s3_meta_request *meta_request);

AWS_EXTERN_C_END
/* ******************************************** */
/* END - Exposed only for use in tests */
/* ******************************************** */

#endif /* AWS_S3_META_REQUEST_IMPL_H */
