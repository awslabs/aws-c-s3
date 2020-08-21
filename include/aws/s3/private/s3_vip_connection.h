#ifndef AWS_S3_VIP_CONNECTION_H
#define AWS_S3_VIP_CONNECTION_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/atomics.h>
#include <aws/common/byte_buf.h>
#include <aws/common/mutex.h>

struct aws_s3_meta_request;
struct aws_s3_request;
struct aws_s3_vip_connection;
struct aws_http_connection;

typedef void(aws_s3_vip_connection_shutdown_complete_callback_fn)(void *user_data);

struct aws_s3_vip_connection_options {
    struct aws_allocator *allocator;
    struct aws_event_loop *event_loop;
    struct aws_credentials_provider *credentials_provider;

    /* HTTP Connection manager of the owning VIP. */
    struct aws_http_connection_manager *http_connection_manager;

    /* Region that transfers will be done in. */
    struct aws_byte_cursor region;

    /* Called when a VIP connection has finished shutting down. */
    aws_s3_vip_connection_shutdown_complete_callback_fn *shutdown_callback;

    /* Identifier for being able to know what VIP connections belong to which VIP.  (This is primarily for the client's
     * use when doing things like cleaning up a specific VIP, so that it knows what connections to remove.) */
    void *vip_identifier;

    /* User data to use for the above callbacks. */
    void *user_data;
};

struct aws_s3_vip_connection *aws_s3_vip_connection_new(
    struct aws_allocator *allocator,
    const struct aws_s3_vip_connection_options *options);

void aws_s3_vip_connection_acquire(struct aws_s3_vip_connection *vip_connection);

void aws_s3_vip_connection_release(struct aws_s3_vip_connection *vip_connection);

void *aws_s3_vip_connection_get_vip_identifier(struct aws_s3_vip_connection *vip_connection);

/* Asynchronously push a meta request onto this VIP connection. */
int aws_s3_vip_connection_push_meta_request(
    struct aws_s3_vip_connection *vip_connection,
    struct aws_s3_meta_request *meta_request);

/* Asynchronously push a meta request onto this VIP connection. */
int aws_s3_vip_connection_remove_meta_request(
    struct aws_s3_vip_connection *vip_connection,
    struct aws_s3_meta_request *meta_request);

#endif /* AWS_S3_VIP_CONNECTION_H */
