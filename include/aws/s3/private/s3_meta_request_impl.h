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
#include "aws/s3/private/s3_request.h"

struct aws_s3_client;
struct aws_s3_vip_connection;
struct aws_s3_meta_request;
struct aws_s3_request;
struct aws_s3_request_options;
struct aws_http_headers;
struct aws_http_make_request_options;
struct aws_retry_strategy;
struct aws_byte_buffer;

enum aws_s3_meta_request_state {
    AWS_S3_META_REQUEST_STATE_ACTIVE,
    AWS_S3_META_REQUEST_STATE_FINISHED,
};

enum aws_s3_meta_request_next_request_flags {
    AWS_S3_META_REQUEST_NEXT_REQUEST_FLAG_NO_ENDPOINT_CONNECTIONS = 0x00000001,
};

struct aws_s3_meta_request_vtable {
    /* Pass back a request with a populated description.  If no work is available, this is allowed to pass back a NULL
     * pointer. */
    void (*next_request)(struct aws_s3_meta_request *meta_request, struct aws_s3_request **out_request, uint32_t flags);

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

    /* Called when any sending of the request is finished, including for each retry. */
    void (*send_request_finish)(
        struct aws_s3_vip_connection *vip_connection,
        struct aws_http_stream *stream,
        int error_code);

    /* Called when the request is done being sent, and will not be retried/sent again. */
    int (*finished_request)(struct aws_s3_meta_request *meta_request, struct aws_s3_request *request, int error_code);

    /* Called when response bodies have either been delivered or failed to have been delivered to the caller. */
    void (*delivered_requests)(
        struct aws_s3_meta_request *meta_request,
        int error_code,
        uint32_t num_failed,
        uint32_t num_successful);

    /* Called when the meta request is completely finished. */
    void (*finish)(struct aws_s3_meta_request *meta_request);

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

        /* Priority queue for pending streaming requests.  We use a priority queue to keep parts in order so that we
         * can stream them to the caller in order. */
        struct aws_priority_queue pending_body_streaming_requests;

        /* Current state of the meta request. */
        enum aws_s3_meta_request_state state;

        /* The next expected streaming part number needed to continue streaming part bodies.  (For example, this will
         * initially be 1 for part 1, and after that part is received, it will be 2, then 3, etc.. */
        uint32_t next_streaming_part;

        /* Number of parts scheduled for delivery. */
        uint32_t num_parts_delivery_sent;

        /* Total number of parts that have been attempted to be delivered. (Will equal the sum of succeeded and
         * failed.)*/
        uint32_t num_parts_delivery_completed;

        /* Number of parts that have been successfully delivered to the caller. */
        uint32_t num_parts_delivery_succeeded;

        /* Number of parts that have failed while trying to be delivered to the caller. */
        uint32_t num_parts_delivery_failed;

        /* The end finish result of the meta request. */
        struct aws_s3_meta_request_result finish_result;

        /* True if the finish result has been set. */
        uint32_t finish_result_set : 1;

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

AWS_EXTERN_C_BEGIN

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

/* Returns true if the meta request is still in the "active" state. */
AWS_S3_API
bool aws_s3_meta_request_is_active(struct aws_s3_meta_request *meta_request);

/* Retruns true if the meta request is in the "finished" state. */
AWS_S3_API
bool aws_s3_meta_request_is_finished(struct aws_s3_meta_request *meta_request);

AWS_S3_API
bool aws_s3_meta_request_is_finishing(struct aws_s3_meta_request *meta_request);

AWS_S3_API
void aws_s3_meta_request_lock_synced_data(struct aws_s3_meta_request *meta_request);

AWS_S3_API
void aws_s3_meta_request_unlock_synced_data(struct aws_s3_meta_request *meta_request);

/* Gets the client reference in the meta request synced_data, acquiring a reference to it if it exists. After calling
 * this function, it is necessary to release that reference. */
AWS_S3_API
struct aws_s3_client *aws_s3_meta_request_acquire_client(struct aws_s3_meta_request *meta_request);

AWS_S3_API
void aws_s3_meta_request_next_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request **out_request,
    uint32_t flags);

AWS_S3_API
int aws_s3_meta_request_make_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_client *client,
    struct aws_s3_vip_connection *vip_connection);

AWS_S3_API
void aws_s3_meta_request_init_signing_date_time_default(
    struct aws_s3_meta_request *meta_request,
    struct aws_date_time *date_time);

AWS_S3_API
int aws_s3_meta_request_sign_request_default(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_vip_connection *vip_connection);

AWS_S3_API
void aws_s3_meta_request_send_request_finish_default(
    struct aws_s3_vip_connection *vip_connection,
    struct aws_http_stream *stream,
    int error_code);

AWS_S3_API
int aws_s3_meta_request_finished_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    int error_code);

AWS_S3_API
void aws_s3_meta_request_stream_response_body_synced(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request);

AWS_S3_API
void aws_s3_meta_request_delivered_requests_default(
    struct aws_s3_meta_request *meta_request,
    int error_code,
    uint32_t num_failed,
    uint32_t num_successful);

AWS_S3_API
int aws_s3_meta_request_read_body(struct aws_s3_meta_request *meta_request, struct aws_byte_buf *buffer);

/* Set that the meta request has failed. This is meant to be called sometime before aws_s3_meta_request_finish.
 * Subsequent calls this function or to aws_s3_meta_request_set_success_synced will not overwrite the end result of the
 * meta request. */
AWS_S3_API
void aws_s3_meta_request_set_fail_synced(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *failed_request,
    int error_code);

/* Set that the meta request has failed. This is meant to be called sometime before aws_s3_meta_request_finish.
 * Subsequent calls this function or to aws_s3_meta_request_set_fail_synced will not overwrite the end result of the
 * meta request. */
AWS_S3_API
void aws_s3_meta_request_set_success_synced(struct aws_s3_meta_request *meta_request, int response_status);

/* Returns true if either aws_s3_meta_request_set_fail_synced or aws_s3_meta_request_set_success_synced have been
 * called. */
AWS_S3_API
bool aws_s3_meta_request_is_finishing_synced(struct aws_s3_meta_request *meta_request);

/* Virtual function called by the meta request derived type when it's completely finished and there is no other work to
 * be done. */
AWS_S3_API
void aws_s3_meta_request_finish(struct aws_s3_meta_request *meta_request);

/* Default implementation of the meta request finish functino. */
AWS_S3_API
void aws_s3_meta_request_finish_default(struct aws_s3_meta_request *meta_request);

AWS_S3_API
void aws_s3_meta_request_body_streaming_push_synced(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request);

AWS_S3_API
struct aws_s3_request *aws_s3_meta_request_body_streaming_pop_synced(struct aws_s3_meta_request *meta_request);

AWS_S3_API
void aws_s3_meta_request_result_setup(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_meta_request_result *result,
    struct aws_s3_request *request,
    int response_status,
    int error_code);

AWS_S3_API
void aws_s3_meta_request_result_clean_up(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_meta_request_result *result);

AWS_EXTERN_C_END

#endif /* AWS_S3_META_REQUEST_IMPL_H */
