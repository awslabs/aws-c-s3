#ifndef AWS_S3_CLIENT_CONFIG_H
#define AWS_S3_CLIENT_CONFIG_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/byte_buf.h>

struct aws_event_loop_group;
struct aws_host_resolver;

typedef void(aws_s3_client_shutdown_complete_callback)(void *user_data);

struct aws_s3_client_config {
    struct aws_event_loop_group *el_group;
    struct aws_host_resolver *host_resolver;
    struct aws_byte_cursor region;
    struct aws_byte_cursor bucket_name; // TODO likely needs to be more robust in the future.

    aws_s3_client_shutdown_complete_callback *shutdown_callback;
    void *shutdown_callback_user_data;

    // TODO encryption options
    // TODO connection options
};

#endif
