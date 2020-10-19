#ifndef AWS_S3_CLIENT_IMPL_H
#define AWS_S3_CLIENT_IMPL_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_part_buffer.h"
#include "aws/s3/s3_client.h"

#include <aws/common/atomics.h>
#include <aws/common/byte_buf.h>
#include <aws/common/linked_list.h>
#include <aws/common/mutex.h>
#include <aws/common/ref_count.h>
#include <aws/common/task_scheduler.h>

struct aws_http_connection_manager;

/* Represents one Virtual IP (VIP) in S3, including a connection manager that points directly at that VIP. */
struct aws_s3_vip {
    struct aws_linked_list_node node;

    struct aws_ref_count internal_ref_count;

    /* S3 Client that owns this vip. */
    struct aws_s3_client *owning_client;

    /* Address this VIP represents. */
    struct aws_string *host_address;

    /* Connection manager shared by all VIP connections. */
    struct aws_http_connection_manager *http_connection_manager;

    struct {
        /* When true, the VIP connection should destroy itself as soon as possible. */
        uint32_t pending_destruction : 1;
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

    /* Current meta request being processed by this VIP connection. */
    struct aws_s3_meta_request *meta_request;

    /* Task for processing meta requests on this VIP connection. */
    struct aws_task process_meta_requests_task;
};

/* Represents the state of the S3 client. */
struct aws_s3_client {
    struct aws_allocator *allocator;

    struct aws_ref_count ref_count;

    /* Internal ref count is used for tracking the lifetime of resources owned by the client that have asynchronous
     * clean up.  In those cases, we don't want to prevent clean up from being initiated (which is what would happen
     * with a normal reference), but we do want to know when we can completely clean up (ie: regular ref count and
     * internal ref count are both 0). */
    struct aws_ref_count internal_ref_count;

    struct aws_client_bootstrap *client_bootstrap;

    struct aws_event_loop *event_loop;

    struct aws_credentials_provider *credentials_provider;

    /* Region of the S3 bucket. */
    struct aws_string *region;

    /* Endpoint to use for the bucket. */
    struct aws_string *endpoint;

    /* Size of parts for files when doing gets or puts.  This exists on the client as configurable option that is passed
     * to meta requests for use. */
    const uint64_t part_size;

    /* TLS Options to be used for each connection.  Specify NULL to not use TLS. */
    struct aws_tls_connection_options *tls_connection_options;

    /* Timeout value, in milliseconds, used for each connection. */
    const uint32_t connection_timeout_ms;

    /* Throughput target in Gbps that we are trying to reach. */
    const double throughput_target_gbps;

    /* Amount of throughput in Gbps to designate to each VIP. */
    const double throughput_per_vip_gbps;

    /* The number of connections that each VIP will have. */
    const uint32_t num_connections_per_vip;

    /* The calculated ideal number of VIP's based on throughput target and throughput per vip. */
    const uint32_t ideal_vip_count;

    /* Atomic used for switching host resolution on/off in a thread safe way. */
    struct aws_atomic_var resolving_hosts;

    /* Shutdown callbacks to notify when the client is completely cleaned up. */
    aws_s3_client_shutdown_complete_callback_fn *shutdown_callback;
    void *shutdown_callback_user_data;

    struct {
        struct aws_mutex lock;

        uint32_t vip_count;

        /* Linked list of active VIP's. */
        struct aws_linked_list vips;

        /* List of all idle VIP Connections for each VIP. */
        struct aws_linked_list idle_vip_connections;

        /* Client list of on going meta requests. */
        struct aws_linked_list meta_requests;

        /* Our pool of parts to be used by file transfers as needed. */
        struct aws_s3_part_buffer_pool part_buffer_pool;

    } synced_data;
};

typedef void(aws_s3_client_get_http_connection_callback)(
    struct aws_http_connection *http_connection,
    int error_code,
    void *user_data);

typedef void(aws_s3_client_sign_callback)(int error_code, void *user_data);

int aws_s3_client_sign_message(
    struct aws_s3_client *client,
    struct aws_http_message *message,
    aws_s3_client_sign_callback *callback,
    void *user_data);

int aws_s3_client_get_http_connection(
    struct aws_s3_client *client,
    struct aws_s3_vip_connection *vip_connection,
    aws_s3_client_get_http_connection_callback *callback,
    void *user_data);

/* Gets the next part buffer from the pool.  Returns NULL if the pool is empty. */
struct aws_s3_part_buffer *aws_s3_client_get_part_buffer(struct aws_s3_client *client, uint32_t part_number);

void aws_s3_part_buffer_release(struct aws_s3_part_buffer *part_buffer);

#endif /* AWS_S3_CLIENT_IMPL_H */
