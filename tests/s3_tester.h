#ifndef AWS_S3_TESTER_H
#define AWS_S3_TESTER_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/s3/s3.h>
#include <aws/s3/s3_client.h>

#include <aws/common/common.h>
#include <aws/common/condition_variable.h>
#include <aws/common/logging.h>
#include <aws/common/mutex.h>
#include <aws/common/string.h>

struct aws_client_bootstrap;
struct aws_credentials_provider;
struct aws_event_loop_group;
struct aws_host_resolver;

/* Utility for setting up commonly needed resources for tests. */
struct aws_s3_tester {
    struct aws_allocator *allocator;

    struct aws_mutex lock;
    struct aws_condition_variable signal;

    struct aws_event_loop_group *el_group;
    struct aws_host_resolver *host_resolver;
    struct aws_client_bootstrap *client_bootstrap;
    struct aws_credentials_provider *credentials_provider;

    struct aws_string *region;

    int finish_error_code;

    bool received_finish_callback;
    bool bound_to_client_shutdown;
    bool clean_up_flag;
};

struct aws_s3_client_config;

int aws_s3_tester_init(
    struct aws_allocator *allocator,
    struct aws_s3_tester *tester,
    const struct aws_byte_cursor region,
    const struct aws_byte_cursor bucket_name);

/* Wait for aws_s3_tester_notify_finished to be called */
void aws_s3_tester_wait_for_finish(struct aws_s3_tester *tester);

/* Notify the tester that an operation has finished and that anyway waiting with aws_s3_tester_wait_for_finish can
 * continue */
void aws_s3_tester_notify_finished(struct aws_s3_tester *tester, int error_code);

/* Set up the aws_s3_client's shutdown callbacks to be used by the tester.  This allows the tester to wait for the
 * client to clean up. */
void aws_s3_tester_bind_client_shutdown(struct aws_s3_tester *tester, struct aws_s3_client_config *config);

/* Handle cleaning up the tester.  If aws_s3_tester_bind_client_shutdown was used, then it will wait for the client to
 * finish shutting down before releasing any resources. */
void aws_s3_tester_clean_up(struct aws_s3_tester *tester);

void aws_s3_create_test_buffer(struct aws_allocator *allocator, size_t buffer_size, struct aws_byte_buf *out_buf);

#endif /* AWS_S3_TESTER_H */
