#ifndef AWS_S3_CLIENT_IMPL_H
#define AWS_S3_CLIENT_IMPL_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_request.h"
#include "aws/s3/s3_client.h"

#include <aws/common/array_list.h>
#include <aws/common/atomics.h>
#include <aws/common/byte_buf.h>
#include <aws/common/mutex.h>

struct aws_http_connection_manager;
struct aws_htttp_connection;

struct aws_s3_part_buffer {
    struct aws_linked_list_node node;

    uint64_t range_start;

    uint64_t range_end;

    struct aws_byte_buf buffer;
};

struct aws_s3_part_buffer_pool {
    int32_t num_allocated;
    struct aws_linked_list free_list;
};

struct aws_s3_vip {
    /* Address this VIP represents. */
    struct aws_string *host_address;

    /* Connection manager shared by all VIP connections. */
    struct aws_http_connection_manager *http_connection_manager;
};

/* We need a payload of data to pass through a series of callbacks for setting up/making request which knows about the
 * vip connection and client. This is that payload.  It should not be touched outside of the processing of a request
 * while processing is taking place.  This data does not need a lock as long as that rule is abided by. */
struct aws_s3_vip_connection_make_request_state {
    uint32_t request_count;

    struct aws_s3_client *client;

    struct aws_s3_vip_connection *vip_connection;

    struct aws_s3_meta_request *meta_request;

    struct aws_s3_request *request;

    /* HTTP connection currently in use by this pipeline.  A single connection is re-used until
     * connection_request_count is hit. */
    struct aws_http_connection *http_connection;

    struct aws_http_stream *http_stream;

    struct aws_signable *signable;
};

struct aws_s3_vip_connection {
    struct aws_linked_list_node node;

    /* Used to group this VIP connection with other VIP connections belonging to the same VIP. */
    void *vip_id;

    struct aws_http_connection_manager *http_connection_manager;

    /* Next meta request to be used.  We try to keep this up always pointing to the next meta request, even when
     * meta requests are removed/added, so that mutations of the meta request list do not cause any unintentional
     * favoring of certain files.  (Might be overkill.)*/
    size_t next_meta_request_index;

    uint32_t pending_destruction : 1;

    struct aws_s3_vip_connection_make_request_state make_request_state;
};

/* Stores state for an instance of a high performance s3 client */
struct aws_s3_client {
    struct aws_allocator *allocator;

    struct aws_atomic_var ref_count;

    struct aws_client_bootstrap *client_bootstrap;

    struct aws_event_loop *event_loop;

    struct aws_credentials_provider *credentials_provider;

    /* Region of the S3 bucket. */
    struct aws_string *region;

    /* Endpoint to use for the bucket. */
    struct aws_string *endpoint;

    /* Size of parts that files will be transfered in.  This exists on the client as configurable option that is passed
     * to meta requests for use. */
    uint64_t part_size;

    /* Amount of time a VIP address stays in the host resolver. */
    size_t dns_host_address_ttl;

    /* Throughput target in Gbps that we are trying to reach. */
    double throughput_target_gbps;

    /* Amount of throughput in Gbps to designate to each VIP. */
    double throughput_per_vip;

    /* The number of connections that each VIP will have. */
    uint32_t num_connections_per_vip;

    /* The calculated ideal number of VIP's based on throughput target and throughput per vip. */
    uint32_t ideal_vip_count;

    /* Utility used that tries to simplify task creation and provides an off-switch/shutdown path for tasks issued. */
    struct aws_s3_task_manager *task_manager;

    /* Shutdown callbacks to notify when the client is completely cleaned up. */
    aws_s3_client_shutdown_complete_callback_fn *shutdown_callback;
    void *shutdown_callback_user_data;

    /* Number of connection managers that are still allocated by the client's VIP's. */
    struct aws_atomic_var num_http_conn_managers_allocated;

    /* Number of VIP connections that are still alocated by the client's VIP's.*/
    struct aws_atomic_var num_vip_connections_allocated;

    struct aws_atomic_var resolving_hosts;

    struct {
        struct aws_mutex lock;

        /* Array list of active VIP's. */
        struct aws_array_list vips;

        /* List of all active VIP Connections for each VIP. */
        struct aws_linked_list active_vip_connections;

        /* List of all idle VIP Connections for each VIP. */
        struct aws_linked_list idle_vip_connections;

        /* Client list of on going meta requests. */
        struct aws_array_list meta_requests;

        struct aws_s3_part_buffer_pool part_buffer_pool;

    } synced_data;
};

struct aws_s3_part_buffer *aws_s3_client_get_part_buffer(struct aws_s3_client *client);

void aws_s3_client_release_part_buffer(struct aws_s3_client *client, struct aws_s3_part_buffer *part_buffer);

#endif /* AWS_S3_CLIENT_IMPL_H */
