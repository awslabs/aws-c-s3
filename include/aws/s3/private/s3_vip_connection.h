#ifndef AWS_S3_VIP_CONNECTION_H
#define AWS_S3_VIP_CONNECTION_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_work_util.h"

#include <aws/common/atomics.h>
#include <aws/common/byte_buf.h>
#include <aws/common/linked_list.h>
#include <aws/common/mutex.h>
#include <aws/common/task_scheduler.h>

struct aws_s3_meta_request;
struct aws_s3_request;
struct aws_s3_vip_connection;
struct aws_http_connection;

/* State machine states for the vip connection. */
enum aws_s3_vip_connection_state {
    AWS_S3_VIP_CONNECTION_STATE_ALIVE,

    AWS_S3_VIP_CONNECTION_STATE_CLEAN_UP,
    AWS_S3_VIP_CONNECTION_STATE_CLEAN_UP_WAIT_FOR_IDLE,
    AWS_S3_VIP_CONNECTION_STATE_CLEAN_UP_IDLE_FINISHED,
    AWS_S3_VIP_CONNECTION_STATE_CLEAN_UP_WAIT_FOR_WORK_CONTROLLER,
    AWS_S3_VIP_CONNECTION_STATE_CLEAN_UP_WORK_CONTROLLER_FINISHED,
    AWS_S3_VIP_CONNECTION_STATE_CLEAN_UP_FINISH
};

/* Processing state of a VIP connection, ie, if it is currently not processing meta requests, or it is processing meta
 * requests.*/
enum aws_s3_vip_connection_processing_state {
    AWS_S3_VIP_CONNECTION_PROCESSING_STATE_IDLE,
    AWS_S3_VIP_CONNECTION_PROCESSING_STATE_ACTIVE
};

typedef void(
    aws_s3_vip_connection_on_active_callback_fn)(struct aws_s3_vip_connection *vip_connection, void *user_data);
typedef void(aws_s3_vip_connection_on_idle_callback_fn)(struct aws_s3_vip_connection *vip_connection, void *user_data);
typedef void(aws_s3_vip_connection_shutdown_complete_callback_fn)(void *user_data);

struct aws_s3_vip_connection_options {
    struct aws_allocator *allocator;
    struct aws_event_loop *event_loop;
    struct aws_credentials_provider *credentials_provider;

    /* HTTP Connection manager of the owning VIP. */
    struct aws_http_connection_manager *http_connection_manager;

    /* Region that transfers will be done in. */
    struct aws_byte_cursor region;

    /* Called when a vip conenction enters the 'active' processing state. */
    aws_s3_vip_connection_on_active_callback_fn *on_active_callback;

    /* Called when a vip conenction enters the 'idle' processing state. */
    aws_s3_vip_connection_on_idle_callback_fn *on_idle_callback;

    /* Called when a vip conenction has finished shutting down. */
    aws_s3_vip_connection_shutdown_complete_callback_fn *shutdown_callback;

    /* User data to use for the above callbacks. */
    void *user_data;
};

struct aws_s3_vip_connection {
    struct aws_allocator *allocator;
    struct aws_atomic_var ref_count;

    struct aws_event_loop *event_loop;
    struct aws_credentials_provider *credentials_provider;
    struct aws_http_connection_manager *http_connection_manager;
    struct aws_string *region;

    struct aws_mutex lock;

    /* Current place in the VIP connection's state machine. Requires Lock */
    enum aws_s3_vip_connection_state state;

    /* Current processing state (doing work or not doing work) for the VIP connection. Requires Lock. */
    enum aws_s3_vip_connection_processing_state processing_state;

    /* Local list of meta requests.  Changes to this list are all pushed by the owning client. Requires Lock */
    struct aws_array_list meta_requests;

    /* List of meta request removals that arrived before their meta-request-push happened due to task ordering. Requires
     * Lock */
    struct aws_array_list meta_request_ooo_removals;

    /* Used to facilitate async operations on the VIP connection. */
    struct aws_s3_async_work_controller async_work_controller;

    /* Current HTTP connection for this transfer. This is only used during processing of work for a connection,
     * which should not have any multithreaded overlap, not requiring a lock. */
    struct aws_http_connection *http_connection;

    /* Current request that is being processed. This is only used during processing of work for a connection,
     * which should not have any multithreaded overlap, not requiring a lock. */
    struct aws_s3_request *request;

    /* Next meta request to be used.  We try to keep this up always pointing to the next meta request, even when meta
     * requests are removed/added, so that mutations of the meta request list do not cause any unintentional favoring of
     * certain files.  (Might be overkill.)*/
    size_t next_meta_request_index;
    int32_t connection_request_count;

    /* Called when this connection's processing state is active. */
    aws_s3_vip_connection_on_active_callback_fn *on_active_callback;

    /* Called when this connection's processing state is idle. */
    aws_s3_vip_connection_on_idle_callback_fn *on_idle_callback;

    /* Called when this connection is done cleaning up. */
    aws_s3_vip_connection_shutdown_complete_callback_fn *shutdown_callback;
    void *user_data;
};

struct aws_s3_vip_connection *aws_s3_vip_connection_new(
    struct aws_allocator *allocator,
    const struct aws_s3_vip_connection_options *options);

void aws_s3_vip_connection_acquire(struct aws_s3_vip_connection *vip_connection);

void aws_s3_vip_connection_release(struct aws_s3_vip_connection *vip_connection);

/* Asynchronously push a meta request onto this VIP connection. */
int aws_s3_vip_connection_push_meta_request(
    struct aws_s3_vip_connection *vip_connection,
    struct aws_s3_meta_request *meta_request);

/* Asynchronously push a meta request onto this VIP connection. */
int aws_s3_vip_connection_remove_meta_request(
    struct aws_s3_vip_connection *vip_connection,
    struct aws_s3_meta_request *meta_request);

#endif /* AWS_S3_VIP_CONNECTION_H */
