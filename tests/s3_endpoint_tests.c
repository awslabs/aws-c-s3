/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_client_impl.h"
#include "s3_tester.h"

/*
struct aws_s3_endpoint_options {
    struct aws_string *host_name;

    aws_s3_endpoint_ref_zero_fn *ref_count_zero_callback;

    aws_s3_endpoint_shutdown_fn *shutdown_callback;

    struct aws_client_bootstrap *client_bootstrap;

    const struct aws_tls_connection_options *tls_connection_options;

    void *user_data;

    uint32_t max_connections;
};
*/

struct aws_s3_endpoint *aws_s3_client_endpoint_new(
    struct aws_allocator *allocator,
    const struct aws_s3_endpoint_options *options);

struct aws_s3_endpoint *aws_s3_endpoint_acquire(struct aws_s3_endpoint *endpoint);

void aws_s3_endpoint_release(struct aws_s3_endpoint *endpoint);
