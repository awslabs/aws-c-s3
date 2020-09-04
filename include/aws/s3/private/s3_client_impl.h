#ifndef AWS_S3_CLIENT_IMPL_H
#define AWS_S3_CLIENT_IMPL_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/s3_client.h"

#include <aws/common/array_list.h>
#include <aws/common/atomics.h>
#include <aws/common/byte_buf.h>
#include <aws/common/mutex.h>
#include <aws/common/task_scheduler.h>

struct aws_http_connection_manager;
struct aws_htttp_connection;

/* Pre-allocated buffer that is the size of a single part.*/
struct aws_s3_part_buffer {
    struct aws_linked_list_node node;

    /* Reference to the owning client so that it can easily be released back. */
    struct aws_s3_client *client;

    /* What part of the overall file transfer this part is currently designated to. */
    uint64_t range_start;

    uint64_t range_end;

    /* Re-usable byte buffer. */
    struct aws_byte_buf buffer;
};

/* Pool of pre-allocated part buffers. */
struct aws_s3_part_buffer_pool {
    int32_t num_allocated;
    struct aws_linked_list free_list;
};

/* Represents one VIP in S3, including a connection manager that points directly at that VIP. */
struct aws_s3_vip {
    /* Address this VIP represents. */
    struct aws_string *host_address;

    /* Connection manager shared by all VIP connections. */
    struct aws_http_connection_manager *http_connection_manager;
};

/* Represents one connection on a particular VIP. */
struct aws_s3_vip_connection {
    struct aws_linked_list_node node;

    /* Used to group this VIP connection with other VIP connections belonging to the same VIP. */
    void *vip_id;

    /* Connection manager reference.  We store one on the VIP connection so that we don't have to worry about release
     * order with the associated VIP. */
    struct aws_http_connection_manager *http_connection_manager;

    /* The underlying, currently in-use HTTP connection. */
    struct aws_http_connection *http_connection;

    /* Next meta request to be used.  We try to keep this up always pointing to the next meta request, even when
     * meta requests are removed/added, so that mutations of the meta request list do not cause any unintentional
     * favoring of certain files.  (Might be overkill.)*/
    size_t next_meta_request_index;

    /* When true, the VIP connection should destroy itself as soon as possible. */
    uint32_t pending_destruction : 1;

    /* Number of requests we have mde on this particular connection. Important for the request service limit. */
    uint32_t request_count;

    /* This is transient used during the processing of meta requests, placed here just to prevent constant allocation,
     * but not meant to be used outside of that flow. */
    struct {

        struct aws_s3_client *client;

    } transient_active_request_args;
};

/* Represents the state of the S3 client. */
struct aws_s3_client {
    struct aws_allocator *allocator;

    struct aws_atomic_var ref_count;

    /* Internal ref count is used for tracking the lifetime of resources owned by the client that have asynchronous
     * clean up.  In those cases, we don't want to prevent clean up from being initiated (which is what would happen
     * with a normal reference), but we do want to know when we can completely clean up (ie: regular ref count and
     * internal ref count are both 0). */
    struct aws_atomic_var internal_ref_count;

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

    /* Atomic used for switching host resolution on/off in a thread safe way. */
    struct aws_atomic_var resolving_hosts;

    /* Shutdown callbacks to notify when the client is completely cleaned up. */
    aws_s3_client_shutdown_complete_callback_fn *shutdown_callback;
    void *shutdown_callback_user_data;

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

struct aws_s3_part_buffer *aws_s3_client_get_part_buffer(struct aws_s3_client *client, uint32_t part_number);

void aws_s3_part_buffer_release(struct aws_s3_part_buffer *part_buffer);

#endif /* AWS_S3_CLIENT_IMPL_H */
