#ifndef AWS_S3_CLIENT_H
#define AWS_S3_CLIENT_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/s3/s3.h>

struct aws_allocator;

struct aws_s3_client;
struct aws_s3_request;

typedef void(aws_s3_client_shutdown_complete_callback)(void *user_data);

struct aws_s3_client_config {
    struct aws_event_loop_group *el_group;
    struct aws_host_resolver *host_resolver;
    struct aws_byte_cursor region;
    struct aws_byte_cursor bucket_name;

    aws_s3_client_shutdown_complete_callback *shutdown_callback;
    void *shutdown_callback_user_data;
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
int aws_s3_client_execute_request(struct aws_s3_client *client, struct aws_s3_request *request);

AWS_EXTERN_C_END

#endif /* AWS_S3_CLIENT_H */
