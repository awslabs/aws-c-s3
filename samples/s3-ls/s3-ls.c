/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/auth/credentials.h>
#include <aws/common/condition_variable.h>
#include <aws/common/mutex.h>
#include <aws/common/zero.h>
#include <aws/io/channel_bootstrap.h>
#include <aws/io/event_loop.h>
#include <aws/io/logging.h>
#include <aws/s3/private/s3_file_system_support.h>
#include <aws/s3/s3.h>
#include <aws/s3/s3_client.h>

#include <inttypes.h>
#include <stdio.h>

struct app_ctx {
    struct aws_allocator *allocator;
    struct aws_logger logger;
    struct aws_mutex mutex;
    struct aws_condition_variable c_var;
    bool execution_completed;
    struct aws_signing_config_aws signing_config;
};

/**
 * Predicate used to decide if the application is ready to exit.
 * The corresponding condition variable is set when the last
 * page of ListObjects is received.
 */
static bool s_app_completion_predicate(void *arg) {
    struct app_ctx *app_ctx = arg;
    return app_ctx->execution_completed;
}

/**
 * Called once for each object returned in the ListObjectsV2 responses.
 */
bool s_on_object(const struct aws_s3_object_file_system_info *info, void *user_data) {
    (void) user_data;

    printf("%-18" PRIu64 " %.*s\n", info->size, (int) info->key.len, info->key.ptr);
    return true;
}

/**
 * Called once for each ListObjectsV2 response received.
 * If the response contains a continuation token indicating there are more results to be fetched,
 * requests the next page using aws_s3_paginator_continue.
 */
void s_on_list_finished(struct aws_s3_paginator *paginator, int error_code, void *user_data) {
    struct app_ctx *app_ctx = user_data;

    if (error_code == 0) {
        bool has_more_results = aws_s3_paginator_has_more_results(paginator);

        printf("______ has_more_results=%d\n", has_more_results);
        if (has_more_results) {
            /* get next page */
            int result = aws_s3_paginator_continue(paginator, &app_ctx->signing_config);
            if (result) {
                printf("ERROR returned by aws_s3_paginator_continue from s_on_list_finished: %d\n", result);
            }
            return;
        }
    }

    /* all pages received. triggers the condition variable to exit the application. */
    aws_mutex_lock(&app_ctx->mutex);
    app_ctx->execution_completed = true;
    aws_mutex_unlock(&app_ctx->mutex);
    aws_condition_variable_notify_one(&app_ctx->c_var);
}

/**
 * optionally called to enable logs.
 */
void setup_logger(struct app_ctx *app_ctx) {
    struct aws_logger_standard_options logger_options = {
            .level = AWS_LOG_LEVEL_INFO,
            .file = stderr,
    };

    aws_logger_init_standard(&app_ctx->logger, app_ctx->allocator, &logger_options);
    aws_logger_set(&app_ctx->logger);
}

/**
 * gets the bucket name and prefix from the command line parameter.
 */
int get_bucket_and_prefix_from_command_line(const char *input, struct aws_byte_cursor *bucket,
                                            struct aws_byte_cursor *prefix) {
    AWS_PRECONDITION(input);
    AWS_PRECONDITION(bucket);
    AWS_PRECONDITION(prefix);

    const char *delimiter_pos = strchr(input, '/');
    if (delimiter_pos == NULL) {
        bucket->ptr = (uint8_t *) input;
        bucket->len = strlen(input);
        prefix->ptr = NULL;
        prefix->len = 0;
    } else {
        bucket->ptr = (uint8_t *) input;
        bucket->len = delimiter_pos - input;
        prefix->ptr = (uint8_t *) (delimiter_pos + 1);
        prefix->len = strlen(input) - bucket->len - 1;
    }

    return 0;
}

int main(int argc, char *argv[]) {

    if (argc < 3) {
        printf("Usage: s3-ls {bucket} region\n");
        printf("       s3-ls {bucket}/{prefix} {region}\n");
        printf("\n");
        printf("Example:");
        printf("\n");
        printf("       s3-ls bucket us-west-2\n");

        return -1;
    }

    const char *input = argv[1];
    struct aws_byte_cursor bucket;
    struct aws_byte_cursor prefix;
    get_bucket_and_prefix_from_command_line(input, &bucket, &prefix);

    const char *region = argv[2];
    const bool enable_logs = false;

    struct aws_allocator *allocator = aws_default_allocator();
    aws_s3_library_init(allocator);

    struct app_ctx app_ctx;
    AWS_ZERO_STRUCT(app_ctx);
    app_ctx.allocator = allocator;
    app_ctx.c_var = (struct aws_condition_variable) AWS_CONDITION_VARIABLE_INIT;
    aws_mutex_init(&app_ctx.mutex);

    if (enable_logs) {
        setup_logger(&app_ctx);
    }

    /* event loop */
    struct aws_event_loop_group *event_loop_group = aws_event_loop_group_new_default(allocator, 0, NULL);

    /* resolver */
    struct aws_host_resolver_default_options resolver_options = {
            .el_group = event_loop_group,
            .max_entries = 8,
    };
    struct aws_host_resolver *resolver = aws_host_resolver_new_default(allocator, &resolver_options);

    /* client bootstrap */
    struct aws_client_bootstrap_options bootstrap_options = {
            .event_loop_group = event_loop_group,
            .host_resolver = resolver,
    };
    struct aws_client_bootstrap *client_bootstrap = aws_client_bootstrap_new(allocator, &bootstrap_options);
    if (client_bootstrap == NULL) {
        printf("ERROR initializing client bootstrap\n");
        return -1;
    }

    /* credentials */
    struct aws_credentials_provider_chain_default_options credentials_provider_options;
    AWS_ZERO_STRUCT(credentials_provider_options);
    credentials_provider_options.bootstrap = client_bootstrap;
    struct aws_credentials_provider *credentials_provider = aws_credentials_provider_new_chain_default(allocator, &credentials_provider_options);

    /* signing config */
    aws_s3_init_default_signing_config(&app_ctx.signing_config, aws_byte_cursor_from_c_str(region),
                                       credentials_provider);
    app_ctx.signing_config.flags.use_double_uri_encode = false;

    /* s3 client */
    struct aws_s3_client_config client_config;
    AWS_ZERO_STRUCT(client_config);
    client_config.client_bootstrap = client_bootstrap;
    client_config.region = aws_byte_cursor_from_c_str(region);
    client_config.signing_config = &app_ctx.signing_config;
    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    /* listObjects */
    struct aws_s3_list_bucket_v2_params params;
    AWS_ZERO_STRUCT(params);
    params.client = client;
    params.bucket_name = bucket;
    params.prefix = prefix;

    char endpoint[1024];
    snprintf(endpoint, sizeof(endpoint), "s3.%s.amazonaws.com", region);
    params.endpoint = aws_byte_cursor_from_c_str(endpoint);
    params.user_data = &app_ctx;
    params.on_object = &s_on_object;
    params.on_list_finished = &s_on_list_finished;

    struct aws_s3_paginator *paginator = aws_s3_initiate_list_bucket(allocator, &params);
    int paginator_result = aws_s3_paginator_continue(paginator, &app_ctx.signing_config);
    if (paginator_result) {
        printf("ERROR returned from initial call to aws_s3_paginator_continue: %d \n", paginator_result);
    }

    aws_s3_paginator_release(paginator);

    /* wait completion of last page */
    aws_mutex_lock(&app_ctx.mutex);
    aws_condition_variable_wait_pred(&app_ctx.c_var, &app_ctx.mutex, s_app_completion_predicate, &app_ctx);
    aws_mutex_unlock(&app_ctx.mutex);

    /* release resources */
    aws_s3_client_release(client);
    aws_credentials_provider_release(credentials_provider);
    aws_client_bootstrap_release(client_bootstrap);
    aws_host_resolver_release(resolver);
    aws_event_loop_group_release(event_loop_group);
    aws_mutex_clean_up(&app_ctx.mutex);
    aws_s3_library_clean_up();

    return 0;
}
