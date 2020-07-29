#ifndef AWS_S3_CLIENT_H
#define AWS_S3_CLIENT_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/s3/s3.h>
#include <aws/s3/s3_accel_context.h>

struct aws_allocator;

struct aws_http_stream;
struct aws_http_message;

struct aws_s3_client;
struct aws_s3_request;
struct aws_s3_accel_context;

typedef int(aws_s3_accel_receive_body_callback_fn)(
    struct aws_s3_accel_context *context,
    struct aws_http_stream *stream,
    const struct aws_byte_cursor *body,
    void *user_data);
typedef void(
    aws_s3_accel_request_finish_fn)(const struct aws_s3_accel_context *context, int error_code, void *user_data);
typedef void(aws_s3_client_shutdown_complete_callback_fn)(void *user_data);

/* Options for a new client. */
struct aws_s3_client_config {
    struct aws_event_loop_group *el_group;
    struct aws_host_resolver *host_resolver;
    struct aws_byte_cursor region;
    struct aws_byte_cursor bucket_name;
    struct aws_byte_cursor endpoint;

    aws_s3_client_shutdown_complete_callback_fn *shutdown_callback;
    void *shutdown_callback_user_data;
};

struct aws_s3_accel_request_options {
    struct aws_http_message *message;
    void *user_data;
    aws_s3_accel_receive_body_callback_fn *body_callback;
    aws_s3_accel_request_finish_fn *finish_callback;
};

AWS_EXTERN_C_BEGIN

AWS_S3_API
struct aws_s3_client *aws_s3_client_new(
    struct aws_allocator *allocator,
    const struct aws_s3_client_config *client_config);

AWS_S3_API
void aws_s3_client_acquire(struct aws_s3_client *client);

AWS_S3_API
void aws_s3_client_release(struct aws_s3_client *client);

AWS_S3_API
struct aws_s3_accel_context *aws_s3_client_accel_request(
    struct aws_s3_client *client,
    const struct aws_s3_accel_request_options *options);

AWS_EXTERN_C_END

#endif /* AWS_S3_CLIENT_H */
