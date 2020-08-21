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

enum aws_s3_client_state {

    /* Client is allocated and ready for operations. */
    AWS_S3_CLIENT_STATE_ACTIVE,

    /* Clean up states to make sure everything shuts down correctly. */
    AWS_S3_CLIENT_STATE_CLEAN_UP,
    AWS_S3_CLIENT_STATE_CLEAN_UP_RESOLVE,
    AWS_S3_CLIENT_STATE_CLEAN_UP_RESOLVE_FINISHED,
    AWS_S3_CLIENT_STATE_CLEAN_UP_VIPS,
    AWS_S3_CLIENT_STATE_CLEAN_UP_VIPS_FINISHED,
    AWS_S3_CLIENT_STATE_CLEAN_UP_TASK_UTIL,
    AWS_S3_CLIENT_STATE_CLEAN_UP_TASK_UTIL_FINISHED,
    AWS_S3_CLIENT_STATE_CLEAN_UP_FINISH_RELEASE
};

struct aws_s3_vip {
    /* Address this VIP represents. */
    struct aws_string *host_address;

    /* Connection manager shared by all VIP connections. */
    struct aws_http_connection_manager *http_connection_manager;
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

    struct {

        struct aws_mutex lock;

        /* Current place in the client's state machine.*/
        enum aws_s3_client_state state;

        /* Number of connection managers that are still allocated by the client's VIP's. */
        uint32_t num_http_conn_managers_allocated;

        /* Number of VIP connections that are still alocated by the client's VIP's.*/
        uint32_t num_vip_connections_allocated;

        /* Array list of active VIP's. */
        struct aws_array_list vips;

        /* Array list of all VIP Connections for each VIP. */
        struct aws_array_list vip_connections;

        /* Client list of on going meta requests. */
        struct aws_array_list meta_requests;

    } synced_data;

    /* Utility used that tries to simplify task creation and provides an off-switch/shutdown path for tasks issued. */
    struct aws_s3_task_util *task_util;

    /* Shutdown callbacks to notify when the client is completely cleaned up. */
    aws_s3_client_shutdown_complete_callback_fn *shutdown_callback;
    void *shutdown_callback_user_data;
};

#endif /* AWS_S3_CLIENT_IMPL_H */
