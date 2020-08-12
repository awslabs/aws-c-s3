#ifndef AWS_S3_VIP_H
#define AWS_S3_VIP_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/array_list.h>
#include <aws/common/atomics.h>
#include <aws/common/byte_buf.h>
#include <aws/common/linked_list.h>
#include <aws/common/mutex.h>
#include <aws/common/task_scheduler.h>

struct aws_s3_client;
struct aws_s3_request;
struct aws_s3_vip;
struct aws_host_address;
struct aws_http_connection_manager;
struct aws_http_connection;

enum aws_s3_vip_state {
    AWS_S3_VIP_STATE_INIT,
    AWS_S3_VIP_STATE_ACTIVE,

    AWS_S3_VIP_STATE_CLEAN_UP,
    AWS_S3_VIP_STATE_CLEAN_UP_CONNS,
    AWS_S3_VIP_STATE_CLEAN_UP_CONNS_FINISHED,
    AWS_S3_VIP_STATE_CLEAN_UP_CONN_MANAGER,
    AWS_S3_VIP_STATE_CLEAN_UP_CONN_MANAGER_FINISHED,
    AWS_S3_VIP_STATE_CLEAN_UP_FINISH_RELEASE
};

/* This represents one S3 VIP. This structure is meant to be an extension of the client, ie, it's not meant to be
 * thought of as a standalone object that operates separtely from the client.  For example, all VIP's
 * share the client's lock, and do not have their reference count.  In an attempt to enforce this relationship, all VIP
 * operations also take in the owning client, and do not store a reference to the owning client. (The latter might be
 * revisited to remove the awkward context structure 'aws_s3_client_vip_pair'.) */
struct aws_s3_vip {
    struct aws_allocator *allocator;

    /* This VIP's place in the client's VIP linked list. */
    struct aws_linked_list_node node;

    /* Current place in the VIP's state machine. */
    enum aws_s3_vip_state state;

    /* Address this VIP represents. */
    struct aws_string *host_address;

    /* Connection manager shared by all VIP connections. */
    struct aws_http_connection_manager *http_connection_manager;

    /* Array list of all VIP Connections. */
    struct aws_array_list vip_connections;

    /* Number of VIP connections that have been allocated.  Used for ensuring that we have de-allocated all of our VIP
     * connections. */
    size_t num_allocated_vip_connections;
};

/* This may be factored out in the future.  Because we intentionally do not store a reference to the owning client on
 * the VIP itself (see comment for aws_s3_vip structure), and VIP's do not have their own reference count, we need this
 * structure in certain cases to provide context tying a VIP to a client. */
struct aws_s3_client_vip_pair {
    struct aws_allocator *allocator;
    struct aws_s3_client *client;
    struct aws_s3_vip *vip;
};

/* Poll if a VIP is in the process of cleaning up. */
bool aws_s3_client_vip_is_cleaning_up(struct aws_s3_client *client, const struct aws_s3_vip *vip);

/* Logic for adding a VIP to the client.   Should NOT be called outside of the client. Exposed here to make it visible
 * to s3_client.c and s3_vip.c. */
int aws_s3_client_add_vip(struct aws_s3_client *client, struct aws_byte_cursor host_address);

/* Logic for removing a VIP from the client.   Should NOT be called outside of the client. Exposed here to make it
 * visible to s3_client.c and s3_vip.c. */
int aws_s3_client_remove_vip(struct aws_s3_client *client, struct aws_s3_vip *vip);

/* Updates a vip's place in its state machine.  Should NOT be called outside of the client. Exposed here to make it
 * visible to s3_client.c and s3_vip.c */
void aws_s3_client_set_vip_state(struct aws_s3_client *client, struct aws_s3_vip *vip, enum aws_s3_vip_state state);

/* Allocate a client VIP pair structure. */
struct aws_s3_client_vip_pair *aws_s3_client_vip_pair_new(
    struct aws_allocator *allocator,
    struct aws_s3_client *client,
    struct aws_s3_vip *vip);

/* Destroy a client VIP pair structure. */
void aws_s3_client_vip_pair_destroy(struct aws_s3_client_vip_pair *aws_s3_client_vip_pair);

#endif /* AWS_S3_VIP_H */
