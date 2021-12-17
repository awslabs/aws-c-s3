/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/condition_variable.h>
#include <aws/common/mutex.h>
#include <aws/s3/s3.h>
#include <aws/s3/s3_client.h>

struct app_ctx {
    struct aws_allocator *allocator;
    struct aws_s3_client *client;
    struct aws_credentials_provider *credentials_provider;
    struct aws_client_bootstrap *client_bootstrap;
    struct aws_logger logger;
    struct aws_mutex mutex;
    struct aws_condition_variable c_var;
    bool execution_completed;
    struct aws_signing_config_aws signing_config;
    const char *region;
    enum aws_log_level log_level;
    bool help_requested;
    void *sub_command_data;
};
