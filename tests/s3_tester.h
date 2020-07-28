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

#include <aws/io/event_loop.h>
#include <aws/io/host_resolver.h>

/* Utility for setting up commonly needed resources for tests. */
struct aws_s3_tester {
    struct aws_logger logger;

    struct aws_mutex lock;
    struct aws_condition_variable signal;

    struct aws_event_loop_group el_group;
    struct aws_host_resolver host_resolver;

    struct aws_string *bucket_name;
    struct aws_string *region;
    struct aws_string *endpoint;

    bool received_finish_callback;
    bool clean_up_finished;
    int finish_error_code;
};

struct aws_s3_client_config;

int aws_s3_tester_init(
    struct aws_allocator *allocator,
    struct aws_s3_tester *tester,
    const struct aws_byte_cursor region,
    const struct aws_byte_cursor bucket_name);

void aws_s3_tester_wait_for_finish(struct aws_s3_tester *tester);

void aws_s3_tester_wait_for_clean_up(struct aws_s3_tester *tester);

void aws_s3_tester_notify_finished(struct aws_s3_tester *tester, int error_code);

void aws_s3_tester_clean_up(struct aws_s3_tester *tester);

void aws_s3_tester_bind_client_shutdown(struct aws_s3_tester *tester, struct aws_s3_client_config *config);

#endif /* AWS_S3_TESTER_H */
