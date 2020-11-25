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
#include "aws/s3/private/s3_part_buffer.h"

struct aws_s3_client;
struct aws_s3_vip_connection;
struct aws_s3_meta_request;
struct aws_s3_request;
struct aws_s3_request_options;
struct aws_http_headers;
struct aws_retry_strategy;

typedef void(aws_s3_meta_request_work_available_fn)(struct aws_s3_meta_request *meta_request, void *user_data);

typedef void(aws_s3_meta_request_write_body_finished_callback_fn)(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request);

typedef void(aws_s3_request_finished_callback_fn)(void *user_data);

enum aws_s3_meta_request_state { AWS_S3_META_REQUEST_STATE_ACTIVE, AWS_S3_META_REQUEST_STATE_FINISHED };

enum aws_s3_request_desc_flags { AWS_S3_REQUEST_DESC_RECORD_RESPONSE_HEADERS = 0x00000001 };

/* Represents an in-flight active request.  Does not persist past a the execution of the request. */
struct aws_s3_request {

    /* Linked list node used for queuing. */
    struct aws_linked_list_node node;

    struct aws_ref_count ref_count;

    struct aws_allocator *allocator;

    /* Owning meta request. */
    struct aws_s3_meta_request *meta_request;

    /* Current retry token for the request. If it has never been retried, this will be NULL. */
    struct aws_retry_token *retry_token;

    /* Members of this structure describes the request, making it possible to generate anything needed to send the
     * request. */
    struct {
        /* Part number that this request first to.  If this is not a part, this can be 0.  (S3 Part Numbers start at 1.)
         */
        uint32_t part_number;

        /* Tag that defines what the built request will actually consist of.  Request tags are different per meta
         * request type, and do not necessarily map 1:1 with actual S3 API requests.  For example, they can be more
         * contextual, like "first part" instead of just "part".) */
        int request_tag;

        /* When true, response headers from the request will be stored in the request's response_headers variable. */
        uint32_t record_response_headers : 1;

    } desc_data;

    /* Members of this structure will be repopulated each time the request is sent.  For example, If the request fails,
     * and needs to be retried, then the members of this structure will be cleaned up and re-populated on the next send.
     */
    struct {

        /* The HTTP message to send for this request. */
        struct aws_http_message *message;

        /* Recorded response headers for the request. Set only when the request desc has record_response_headers set to
         * true. */
        struct aws_http_headers *response_headers;

        /* Part buffer to be used with this request. */
        struct aws_s3_part_buffer *part_buffer;

        /* If the request receives an error, this byte buffer will be allocated and will hold the body of that error.*/
        struct aws_byte_buf response_body_error;

        /* Returned response status of this request. */
        int response_status;

        /* Error code result for this sending of the request. */
        int error_code;

        /* Callback for when the current sending of the request has finished.  */
        aws_s3_request_finished_callback_fn *finished_callback;

        /* User data for the finish callback. */
        void *user_data;

    } send_data;

    /* Data intended to be only be used by aws_s3_meta_request_write_body_to_caller functionality. */
    struct {
        /* Callback used for aws_s3_meta_request_write_body_to_caller. */
        aws_s3_meta_request_write_body_finished_callback_fn *finished_callback;

        /* Task used for aws_s3_meta_request_write_body_to_caller. */
        struct aws_task task;
    } write_body_data;
};

struct aws_s3_meta_request_vtable {
    bool (*has_work)(const struct aws_s3_meta_request *meta_request);

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
        struct aws_s3_request *request);

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

    /* Initial HTTP Message that this meta request is based on. */
    struct aws_http_message *initial_request_message;

    /* Part size to use for uploads and downloads.  Passed down by the creating client. */
    const uint64_t part_size;

    struct aws_cached_signing_config_aws *cached_signing_config;

    /* Event loop used for scheduling.  Passed down by the creating client. */
    struct aws_event_loop *event_loop;

    /* User data to be passed to each customer specified callback.*/
    void *user_data;

    /* Customer specified callbacks. */
    aws_s3_meta_request_headers_callback_fn *headers_callback;
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

        /* Client that created this meta request which also processes this request.  After the meta request is finished,
         * this reference is removed. */
        struct aws_s3_client *client;

        /* Queue of aws_s3_request structures that will be retried. */
        struct aws_linked_list retry_queue;

        /* Body of stream of the initial_request_message.  We store this here so that parts can take turns seeking to
         * their own specific position (which should be in close proximity of one another). */
        struct aws_input_stream *initial_body_stream;

        enum aws_s3_meta_request_state state;

    } synced_data;
};

AWS_EXTERN_C_BEGIN

bool aws_s3_meta_request_has_work(const struct aws_s3_meta_request *meta_request);

/* Creates a new auto-ranged get meta request.  This will do multiple parallel ranged-gets when appropriate. */
struct aws_s3_meta_request *aws_s3_meta_request_auto_ranged_get_new(
    struct aws_allocator *allocator,
    struct aws_s3_client *client,
    const struct aws_s3_meta_request_options *options);

/* Creates a new auto-ranged put meta request.  This will do a multipart upload in parallel when appropriate. */
struct aws_s3_meta_request *aws_s3_meta_request_auto_ranged_put_new(
    struct aws_allocator *allocator,
    struct aws_s3_client *client,
    const struct aws_s3_meta_request_options *options);

/* Creates a new default meta request. This will send the request as is and pass back the response. */
struct aws_s3_meta_request *aws_s3_meta_request_default_new(
    struct aws_allocator *allocator,
    struct aws_s3_client *client,
    const struct aws_s3_meta_request_options *options);

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
AWS_S3_API
int aws_s3_meta_request_init_base(
    struct aws_allocator *allocator,
    struct aws_s3_client *client,
    const struct aws_s3_meta_request_options *options,
    void *impl,
    struct aws_s3_meta_request_vtable *vtable,
    struct aws_s3_meta_request *base_type);

/* Pass back the part buffer of the request as a response body to the user */
void aws_s3_meta_request_write_body_to_caller(
    struct aws_s3_request *request,
    aws_s3_meta_request_write_body_finished_callback_fn *callback);

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

/* Tells the meta request to stop, with an error code for indicating failure when necessary. */
void aws_s3_meta_request_finish(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *failed_request,
    int response_status,
    int error_code);

void aws_s3_meta_request_internal_acquire(struct aws_s3_meta_request *meta_request);

void aws_s3_meta_request_internal_release(struct aws_s3_meta_request *meta_request);

AWS_S3_API
void aws_s3_meta_request_lock_synced_data(struct aws_s3_meta_request *meta_request);

AWS_S3_API
void aws_s3_meta_request_unlock_synced_data(struct aws_s3_meta_request *meta_request);

/* Call to have the meta request notify the owning client (if one exists) that there is more work to be done. */
void aws_s3_meta_request_schedule_work(struct aws_s3_meta_request *meta_request);

/* Gets the client reference in the meta request synced_data, acquiring a reference to it if it exists. After calling
 * this function, it is necessary to release that reference. */
AWS_S3_API
struct aws_s3_client *aws_s3_meta_request_acquire_client(struct aws_s3_meta_request *meta_request);

/* END - Meant only for use by derived types.  */

/* BEGIN - Exposed only for use in tests */

AWS_S3_API
void aws_s3_meta_request_handle_error(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    int error_code);

AWS_S3_API
void aws_s3_meta_request_retry_queue_push(struct aws_s3_meta_request *meta_request, struct aws_s3_request *request);

AWS_S3_API
struct aws_s3_request *aws_s3_meta_request_retry_queue_pop_synced(struct aws_s3_meta_request *meta_request);

AWS_EXTERN_C_END

/* END - Exposed only for use in tests */

#endif /* AWS_S3_META_REQUEST_IMPL_H */
