#ifndef AWS_S3_CLIENT_IMPL_H
#define AWS_S3_CLIENT_IMPL_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/s3_client.h"

#include <aws/common/atomics.h>
#include <aws/common/byte_buf.h>
#include <aws/common/linked_list.h>
#include <aws/common/mutex.h>
#include <aws/common/ref_count.h>
#include <aws/common/task_scheduler.h>
#include <aws/http/connection_manager.h>

#define S3_NUM_HTTP_CONNECTIONS 2

struct aws_http_connection;
struct aws_http_connection_manager;

typedef void(aws_s3_client_sign_callback)(int error_code, void *user_data);

enum aws_s3_vip_connection_finish_code {
    AWS_S3_VIP_CONNECTION_FINISH_CODE_SUCCESS,
    AWS_S3_VIP_CONNECTION_FINISH_CODE_FAILED,
    AWS_S3_VIP_CONNECTION_FINISH_CODE_RETRY,
};

/* Represents one connection on a particular VIP. */
struct aws_s3_vip_connection {
    struct aws_linked_list_node node;

    struct aws_s3_client *owning_client;

    struct aws_http_connection_manager *connection_manager;

    uint32_t num_connections;

    uint32_t num_pending_acquisition_count;

    struct aws_http_connection *connections[S3_NUM_HTTP_CONNECTIONS];

    struct aws_http_connection *active_connection;

    struct aws_atomic_var waiting_for_active_connection;

    /* Request currently being processed on the VIP connection. */
    struct aws_s3_request *request;

    /* Current retry token for the request. If it has never been retried, this will be NULL. */
    struct aws_retry_token *retry_token;

    /* True if the connection is currently retrying to process the request. */
    bool is_retry;

    struct {
        struct aws_http_connection *new_connections[S3_NUM_HTTP_CONNECTIONS];

        uint32_t num_new_connections;
    } synced_data;
};

struct aws_s3_client_vtable {

    struct aws_s3_meta_request *(
        *meta_request_factory)(struct aws_s3_client *client, const struct aws_s3_meta_request_options *options);

    void (*schedule_process_work_synced)(struct aws_s3_client *client);

    void (*process_work)(struct aws_s3_client *client);

    void (*on_acquire_http_connection)(
        struct aws_http_connection *incoming_http_connection,
        int error_code,
        void *user_data);
};

/* Represents the state of the S3 client. */
struct aws_s3_client {
    struct aws_allocator *allocator;

    /* Small block allocator for our small allocations. */
    struct aws_allocator *sba_allocator;

    struct aws_s3_client_vtable *vtable;

    struct aws_ref_count ref_count;

    /* Client bootstrap for setting up connection managers. */
    struct aws_client_bootstrap *client_bootstrap;

    /* Event loop on the client bootstrap ELG for processing work/dispatching requests. */
    struct aws_event_loop *process_work_event_loop;

    /* Event loop group for streaming request bodies back to the user. */
    struct aws_event_loop_group *body_streaming_elg;

    /* Region of the S3 bucket. */
    struct aws_string *region;

    /* Size of parts for files when doing gets or puts.  This exists on the client as configurable option that is passed
     * to meta requests for use. */
    const size_t part_size;

    /* Size of parts for files when doing gets or puts.  This exists on the client as configurable option that is passed
     * to meta requests for use. */
    const size_t max_part_size;

    /* TLS Options to be used for each connection. */
    struct aws_tls_connection_options *tls_connection_options;

    /* Cached signing config. Can be NULL if no signing config was specified. */
    struct aws_cached_signing_config_aws *cached_signing_config;

    /* Throughput target in Gbps that we are trying to reach. */
    const double throughput_target_gbps;

    /* The calculated ideal number of VIP's based on throughput target and throughput per vip. */
    const uint32_t ideal_vip_count;

    /* Retry strategy used for scheduling request retries. */
    struct aws_retry_strategy *retry_strategy;

    /* Shutdown callbacks to notify when the client is completely cleaned up. */
    aws_s3_client_shutdown_complete_callback_fn *shutdown_callback;
    void *shutdown_callback_user_data;

    struct {
        struct aws_mutex lock;

        /* Endpoint to use for the bucket. */
        struct aws_string *endpoint;

        struct aws_http_connection_manager *connection_manager;

        /* Meta requests that need added in the work event loop. */
        struct aws_linked_list pending_meta_request_work;

        struct aws_linked_list pending_connections;

        /* Task for processing requests from meta requests on vip connections. */
        struct aws_task process_work_task;

        /* Counter for number of requests that have been finished/released, allowing us to create new requests. */
        uint32_t pending_request_count;

        /* Whether or not the client has started cleaning up all of its resources */
        uint32_t active : 1;

        /* Whether or not work processing is currently scheduled. */
        uint32_t process_work_task_scheduled : 1;

        /* Whether or not work process is currently in progress. */
        uint32_t process_work_task_in_progress : 1;

        /* Whether or not the body streaming ELG is allocated. If the body streaming ELG is NULL, but this is true, the
         * shutdown callback has not yet been called.*/
        uint32_t body_streaming_elg_allocated : 1;

        uint32_t connection_manager_active : 1;

        /* True if the host resolver couldn't find the endpoint.*/
        uint32_t invalid_endpoint : 1;

        /* True if client has been flagged to finish destroying itself. Used to catch double-destroy bugs.*/
        uint32_t finish_destroy : 1;

    } synced_data;

    struct {

        /* Client list of on going meta requests. */
        struct aws_linked_list meta_requests;

        struct aws_linked_list connections;

        /* Number of requests being processed, either still being sent/received or being streamed to the caller. */
        uint32_t num_requests_in_flight;

        struct aws_array_list open_conn_timestamps_millis;

    } threaded_data;
};

int aws_s3_client_make_request(struct aws_s3_client *client, struct aws_s3_vip_connection *vip_connection);

void aws_s3_client_notify_connection_finished(
    struct aws_s3_client *client,
    struct aws_s3_vip_connection *vip_connection,
    int error_code,
    enum aws_s3_vip_connection_finish_code finish_code);

void aws_s3_client_notify_request_destroyed(struct aws_s3_client *client, struct aws_s3_request *request);

typedef void(aws_s3_client_stream_response_body_callback_fn)(
    int error_code,
    uint32_t num_failed,
    uint32_t num_successful,
    void *user_data);

void aws_s3_client_stream_response_body(
    struct aws_s3_client *client,
    struct aws_s3_meta_request *meta_request,
    struct aws_linked_list *requests,
    aws_s3_client_stream_response_body_callback_fn callback,
    void *user_data);

AWS_EXTERN_C_BEGIN

AWS_S3_API
void aws_s3_set_dns_ttl(size_t ttl);

AWS_S3_API
void aws_s3_client_lock_synced_data(struct aws_s3_client *client);

AWS_S3_API
void aws_s3_client_unlock_synced_data(struct aws_s3_client *client);

AWS_EXTERN_C_END

#endif /* AWS_S3_CLIENT_IMPL_H */
