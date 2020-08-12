#ifndef AWS_S3_CLIENT_IMPL_H
#define AWS_S3_CLIENT_IMPL_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_work_util.h"
#include "aws/s3/s3_client.h"

#include <aws/common/atomics.h>
#include <aws/common/hash_table.h>
#include <aws/common/linked_list.h>
#include <aws/common/mutex.h>
#include <aws/common/task_scheduler.h>

struct aws_s3_vip;
struct aws_s3_vip_connection;
struct aws_s3_meta_request;
struct aws_s3_async_work;

enum aws_s3_client_state {

    /* Client is allocated. */
    AWS_S3_CLIENT_STATE_INIT,

    /* Client is allocated and ready for operations.  Once the user gets the client, it should be in this state. */
    AWS_S3_CLIENT_STATE_ACTIVE,

    /* Clean up states to make sure everything shuts down correctly. */
    AWS_S3_CLIENT_STATE_CLEAN_UP,
    AWS_S3_CLIENT_STATE_CLEAN_UP_RESOLVE,
    AWS_S3_CLIENT_STATE_CLEAN_UP_RESOLVE_FINISHED,
    AWS_S3_CLIENT_STATE_CLEAN_UP_VIPS,
    AWS_S3_CLIENT_STATE_CLEAN_UP_VIPS_FINISHED,
    AWS_S3_CLIENT_STATE_CLEAN_UP_WORK_CONTROLLER,
    AWS_S3_CLIENT_STATE_CLEAN_UP_WORK_CONTROLLER_FINISHED,
    AWS_S3_CLIENT_STATE_CLEAN_UP_FINISH_RELEASE
};

/* Action Id's for actions that can be done asynchronously. */
enum aws_s3_client_async_action {
    AWS_S3_CLIENT_ASYNC_ACTION_PUSH_META_REQUEST,
    AWS_S3_CLIENT_ASYNC_ACTION_REMOVE_META_REQUEST,
    AWS_S3_CLIENT_ASYNC_ACTION_ADD_VIP,
    AWS_S3_CLIENT_ASYNC_ACTION_REMOVE_VIP,
    AWS_S3_CLIENT_ASYNC_ACTION_HANDLE_VIP_CONNECTION_SHUTDOWN,
    AWS_S3_CLIENT_ASYNC_ACTION_HANDLE_CONN_MANAGER_SHUTDOWN,
    AWS_S3_CLIENT_ASYNC_ACTION_CLEAN_UP
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

    /* Size of parts the files will be downloaded or uploaded in. */
    uint64_t part_size;

    /* Amount of time a VIP address stays in the host resolver. */
    size_t dns_host_address_ttl;

    /* Throughput target in Gbps that we are trying to reach. */
    double throughput_target_gbps;

    /* Amount of throughput in Gbps to designate to each VIP. */
    double throughput_per_vip;

    /* The number of connections that each VIP will have. */
    uint32_t num_connections_per_vip;

    /* The calculate ideal number of VIP's based on throughput target and throughput per vip. */
    uint32_t ideal_vip_count;

    /* Lock for any state that is mutable across threads. */
    struct aws_mutex lock;

    /* Current place in the client's state machine. Requires lock.*/
    enum aws_s3_client_state state;

    /* Number of vips available. Requires Lock. */
    uint32_t num_active_vips;

    /* Number of vips allocated.  This mainly used for clean up time so that we know we cleaned all VIP's that we
     * allocated. Requires lock. */
    uint32_t num_vips_allocated;

    /* Linked list of active VIP's. Requires lock. */
    struct aws_linked_list vips;

    /* Client list of on going meta requests. Requires lock. */
    struct aws_array_list meta_requests;

    /* Look up table of Address -> VIP structure. Requires lock. */
    struct aws_hash_table vips_table;

    /* Async work controller for facilitating async actions. */
    struct aws_s3_async_work_controller async_work_controller;

    /* Shutdown callbacks to notify when the client is completely cleaned up. */
    aws_s3_client_shutdown_complete_callback_fn *shutdown_callback;
    void *shutdown_callback_user_data;
};

/* Poll the current number of active VIP's. */
uint32_t aws_s3_client_get_num_active_vips(struct aws_s3_client *client);

/* Set the client's current state.  Exposed in the header file to be reachable by s3_client.c and s3_vip.c. */
void aws_s3_client_set_state(struct aws_s3_client *client, enum aws_s3_client_state state);

/* Trigger an asyn client action. Exposed in the header file to be reachable by s3_client.c and s3_vip.c. */
int aws_s3_client_async_action(
    struct aws_s3_client *client,
    enum aws_s3_client_async_action action,
    struct aws_s3_vip *vip,
    struct aws_s3_meta_request *meta_request,
    void *extra);

#endif /* AWS_S3_CLIENT_IMPL_H */
