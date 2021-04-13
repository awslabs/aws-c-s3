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

/* TODO automate this value in the future to prevent it from becoming out-of-sync. */
#define AWS_S3_CLIENT_VERSION "0.1.x"

struct aws_http_connection;
struct aws_http_connection_manager;

typedef void(aws_s3_client_acquire_http_connection_callback)(
    struct aws_http_connection *http_connection,
    int error_code,
    void *user_data);

typedef void(aws_s3_client_sign_callback)(int error_code, void *user_data);

enum aws_s3_vip_connection_finish_code {
    AWS_S3_VIP_CONNECTION_FINISH_CODE_SUCCESS,
    AWS_S3_VIP_CONNECTION_FINISH_CODE_FAILED,
    AWS_S3_VIP_CONNECTION_FINISH_CODE_RETRY,
};

typedef void(aws_s3_vip_shutdown_callback_fn)(void *user_data);

/* Represents one Virtual IP (VIP) in S3, including a connection manager that points directly at that VIP. */
struct aws_s3_vip {
    struct aws_linked_list_node node;

    /* True if this VIP is in use. */
    struct aws_atomic_var active;

    /* S3 Client that owns this vip. */
    struct aws_s3_client *owning_client;

    /* Connection manager shared by all VIP connections. */
    struct aws_http_connection_manager *http_connection_manager;

    /* Address this VIP represents. */
    struct aws_string *host_address;

    /* Callback used when this vip has completely shutdown, which happens when all associated connections and the
     * connection manager are shutdown. */
    aws_s3_vip_shutdown_callback_fn *shutdown_callback;

    /* User data for the shutdown callback. */
    void *shutdown_user_data;

    struct {
        /* How many aws_s3_vip_connection structures are allocated for this vip. This structure will not finish cleaning
         * up until this counter is 0.*/
        uint32_t num_vip_connections;

        /* Whether or not the connection manager is allocated. If the connection manager is NULL, but this is true, the
         * shutdown callback for the connection manager has not yet been called. */
        uint32_t http_connection_manager_active;
    } synced_data;
};

/* Represents one connection on a particular VIP. */
struct aws_s3_vip_connection {

    struct aws_linked_list_node node;

    /* The VIP that this connection belongs to. */
    struct aws_s3_vip *owning_vip;

    /* The underlying, currently in-use HTTP connection. */
    struct aws_http_connection *http_connection;

    /* Number of requests we have made on this particular connection. Important for the request service limit. */
    uint32_t request_count;

    /* Maximum number of requests this connection will do before using a different connection. */
    uint32_t max_request_count;

    /* Request currently being processed on the VIP connection. */
    struct aws_s3_request *request;

    /* Current retry token for the request. If it has never been retried, this will be NULL. */
    struct aws_retry_token *retry_token;

    /* True if the connection is currently retrying to process the request. */
    uint32_t is_retry : 1;

    /* True if the connection has sent at least one request. */
    uint32_t is_warm : 1;

    /* True if the connection is currently sending a request. */
    uint32_t is_active : 1;
};

struct aws_s3_client_vtable {

    struct aws_s3_meta_request *(
        *meta_request_factory)(struct aws_s3_client *client, const struct aws_s3_meta_request_options *options);

    void (*acquire_http_connection)(
        struct aws_s3_client *client,
        struct aws_s3_vip_connection *vip_connection,
        aws_http_connection_manager_on_connection_setup_fn *on_connection_acquired_callback);

    int (*add_vips)(struct aws_s3_client *client, const struct aws_array_list *host_addresses);

    void (*remove_vips)(struct aws_s3_client *client, const struct aws_array_list *host_addresses);

    bool (*http_connection_is_open)(const struct aws_http_connection *http_connection);

    void (*vip_connection_destroy)(struct aws_s3_client *client, struct aws_s3_vip_connection *vip_connection);

    void (*schedule_process_work_synced)(struct aws_s3_client *client);

    void (*process_work)(struct aws_s3_client *client);

    void (
        *setup_vip_connection_retry_token)(struct aws_s3_client *client, struct aws_s3_vip_connection *vip_connection);

    void (*finish_destroy)(struct aws_s3_client *client);
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

    /* Hard limit on max connections set through the client config. */
    const uint32_t max_active_connections_override;

    /* Retry strategy used for scheduling request retries. */
    struct aws_retry_strategy *retry_strategy;

    /* Shutdown callbacks to notify when the client is completely cleaned up. */
    aws_s3_client_shutdown_complete_callback_fn *shutdown_callback;
    void *shutdown_callback_user_data;

    struct {
        /* Number of overall requests currently being processed by the client. */
        struct aws_atomic_var num_requests_in_flight;

        /* Number of requests being sent/received over network. */
        struct aws_atomic_var num_requests_network_io;

        /* Number of requests sitting in their meta request priority queue, waiting to be streamed. */
        struct aws_atomic_var num_requests_stream_queued_waiting;

        /* Number of requests currently scheduled to be streamed or are actively being streamed. */
        struct aws_atomic_var num_requests_streaming;

        /* Number of allocated VIP connnections. */
        struct aws_atomic_var num_allocated_vip_connections;

        /* Number of aws_s3_vip_connections currently processing requests. */
        struct aws_atomic_var num_active_vip_connections;

        /* Number of aws_s3_vip_connections that have already processed one request. */
        struct aws_atomic_var num_warm_vip_connections;
    } stats;

    struct {
        struct aws_mutex lock;

        /* How many vips are being actively used. */
        uint32_t active_vip_count;

        /* How many vips are allocated. (This number includes vips that are in the process of cleaning up) */
        uint32_t allocated_vip_count;

        /* How many requests failed to be prepared. */
        uint32_t num_failed_prepare_requests;

        /* Endpoint to use for the bucket. */
        struct aws_string *endpoint;

        /* Linked list of active VIP's. */
        struct aws_linked_list vips;

        /* VIP Connections that need added or updated in the work event loop. */
        struct aws_linked_list pending_vip_connection_updates;

        /* Meta requests that need added in the work event loop. */
        struct aws_linked_list pending_meta_request_work;

        /* Requests that are prepared and ready to be put in the threaded_data request queue. */
        struct aws_linked_list prepared_requests;

        /* Task for processing requests from meta requests on vip connections. */
        struct aws_task process_work_task;

        /* Host listener to get new IP addresses. */
        struct aws_host_listener *host_listener;

        /* Whether or not the client has started cleaning up all of its resources */
        uint32_t active : 1;

        /* True if the start_destroy function is still executing, which blocks shutdown from completing. */
        uint32_t start_destroy_executing : 1;

        /* True if the client has called aws_host_resolver_resolve_host but hasn't received a callback yet. There isn't
         * a way to cancel this first callback, so this will block shutdown from completing. */
        uint32_t waiting_for_first_host_resolve_callback : 1;

        /* Whether or not work processing is currently scheduled. */
        uint32_t process_work_task_scheduled : 1;

        /* Whether or not work process is currently in progress. */
        uint32_t process_work_task_in_progress : 1;

        /* Whether or not the body streaming ELG is allocated. If the body streaming ELG is NULL, but this is true, the
         * shutdown callback has not yet been called.*/
        uint32_t body_streaming_elg_allocated : 1;

        /* Whether or not the host listener is allocated. If the host listener is NULL, but this is true, the shutdown
         * callback for the listener has not yet been called. */
        uint32_t host_listener_allocated : 1;

        /* True if client has been flagged to finish destroying itself. Used to catch double-destroy bugs.*/
        uint32_t finish_destroy : 1;

        /* True if the host resolver couldn't find the endpoint.*/
        uint32_t invalid_endpoint : 1;

    } synced_data;

    struct {
        /* List of all VIP Connections for each VIP. */
        struct aws_linked_list idle_vip_connections;

        /* Queue of prepared requests that are waiting to be assigned to a VIP connection. */
        struct aws_linked_list request_queue;

        /* Client list of on going meta requests. */
        struct aws_linked_list meta_requests;

        /* Number of requests in the request_queue linked_list. */
        uint32_t request_queue_size;

        /* Number of requests currently being prepared. */
        uint32_t num_requests_being_prepared;

    } threaded_data;
};

void aws_s3_client_notify_connection_finished(
    struct aws_s3_client *client,
    struct aws_s3_vip_connection *vip_connection,
    int error_code,
    enum aws_s3_vip_connection_finish_code finish_code);

void aws_s3_client_notify_request_destroyed(struct aws_s3_client *client, struct aws_s3_request *request);

AWS_EXTERN_C_BEGIN

AWS_S3_API
void aws_s3_set_dns_ttl(size_t ttl);

AWS_S3_API
uint32_t aws_s3_client_get_max_requests_prepare(struct aws_s3_client *client);

AWS_S3_API
uint32_t aws_s3_client_get_max_active_connections(struct aws_s3_client *client, uint32_t num_connections_per_vip);

AWS_S3_API
uint32_t aws_s3_client_get_max_requests_in_flight(struct aws_s3_client *client);

AWS_S3_API
uint32_t aws_s3_client_get_max_allocated_vip_count(struct aws_s3_client *client);

AWS_S3_API
struct aws_s3_vip *aws_s3_vip_new(
    struct aws_s3_client *client,
    const struct aws_byte_cursor *host_address,
    const struct aws_byte_cursor *server_name,
    uint32_t num_vip_connections,
    struct aws_linked_list *out_vip_connections_list,
    aws_s3_vip_shutdown_callback_fn *shutdown_callback,
    void *shutdown_user_data);

AWS_S3_API
void aws_s3_vip_start_destroy(struct aws_s3_vip *vip);

AWS_S3_API
struct aws_s3_vip *aws_s3_find_vip(const struct aws_linked_list *vip_list, const struct aws_byte_cursor *host_address);

AWS_S3_API
void aws_s3_client_set_vip_connection_warm(
    struct aws_s3_client *client,
    struct aws_s3_vip_connection *vip_connection,
    bool is_warm);

AWS_S3_API
void aws_s3_client_set_vip_connection_active(
    struct aws_s3_client *client,
    struct aws_s3_vip_connection *vip_connection,
    bool is_active);

AWS_S3_API
uint32_t aws_s3_client_queue_requests_threaded(
    struct aws_s3_client *client,
    struct aws_linked_list *request_list,
    bool queue_front);

AWS_S3_API
struct aws_s3_request *aws_s3_client_dequeue_request_threaded(struct aws_s3_client *client);

AWS_S3_API
void aws_s3_vip_connection_destroy(struct aws_s3_client *client, struct aws_s3_vip_connection *vip_connection);

/* Sets up vips for each of the given host addresses as long as they are not already in use by other vip structures. */
AWS_S3_API
int aws_s3_client_add_vips(struct aws_s3_client *client, const struct aws_array_list *host_addresses);

/* Removes vips associated with each of the given host addresses. */
AWS_S3_API
void aws_s3_client_remove_vips(struct aws_s3_client *client, const struct aws_array_list *host_addresses);

AWS_S3_API
void aws_s3_client_schedule_process_work(struct aws_s3_client *client);

AWS_S3_API
void aws_s3_client_update_meta_requests_threaded(struct aws_s3_client *client, uint32_t meta_request_update_flags);

AWS_S3_API
void aws_s3_client_update_connections_threaded(struct aws_s3_client *client, bool client_active);

AWS_S3_API
void aws_s3_client_lock_synced_data(struct aws_s3_client *client);

AWS_S3_API
void aws_s3_client_unlock_synced_data(struct aws_s3_client *client);

AWS_S3_API
extern const uint32_t g_max_num_connections_per_vip;
AWS_EXTERN_C_END

#endif /* AWS_S3_CLIENT_IMPL_H */
