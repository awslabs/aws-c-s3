/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/auth/credentials.h>
#include <aws/common/command_line_parser.h>
#include <aws/common/condition_variable.h>
#include <aws/common/mutex.h>
#include <aws/common/zero.h>
#include <aws/io/channel_bootstrap.h>
#include <aws/io/event_loop.h>
#include <aws/io/logging.h>
#include <aws/io/uri.h>
#include <aws/s3/private/s3_list_objects.h>
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
    const char *region;
    enum aws_log_level log_level;
    struct aws_uri uri;
};

static void s_usage(int exit_code) {

    fprintf(stderr, "usage: s3-ls [options] s3://{bucket}[/prefix]\n");
    fprintf(stderr, " bucket: the S3 bucket to list objects\n");
    fprintf(stderr, " prefix: the prefix to filter\n");
    fprintf(stderr, "\n Options:\n\n");
    fprintf(stderr, "  -r, --region: the AWS region where the bucket is located. Default is us-west-2.\n");
    fprintf(stderr, "  -v, --verbose: ERROR|INFO|DEBUG|TRACE: log level to configure. Default is none.\n");
    fprintf(stderr, "  -h, --help\n");
    fprintf(stderr, "            Display this message and quit.\n");
    exit(exit_code);
}

static struct aws_cli_option s_long_options[] = {
    {"region", AWS_CLI_OPTIONS_REQUIRED_ARGUMENT, NULL, 'r'},
    {"verbose", AWS_CLI_OPTIONS_REQUIRED_ARGUMENT, NULL, 'v'},
    {"help", AWS_CLI_OPTIONS_NO_ARGUMENT, NULL, 'h'},
    /* Per getopt(3) the last element of the array has to be filled with all zeros */
    {NULL, AWS_CLI_OPTIONS_NO_ARGUMENT, NULL, 0},
};

static void s_parse_options(int argc, char **argv, struct app_ctx *ctx) {
    while (true) {
        int option_index = 0;
        int c = aws_cli_getopt_long(argc, argv, "r:v:h", s_long_options, &option_index);
        if (c == -1) {
            break;
        }

        switch (c) {
            case 0:
                /* getopt_long() returns 0 if an option.flag is non-null */
                break;
            case 'r':
                ctx->region = aws_cli_optarg;
                break;
            case 'v':
                if (!strcmp(aws_cli_optarg, "TRACE")) {
                    ctx->log_level = AWS_LL_TRACE;
                } else if (!strcmp(aws_cli_optarg, "INFO")) {
                    ctx->log_level = AWS_LL_INFO;
                } else if (!strcmp(aws_cli_optarg, "DEBUG")) {
                    ctx->log_level = AWS_LL_DEBUG;
                } else if (!strcmp(aws_cli_optarg, "ERROR")) {
                    ctx->log_level = AWS_LL_ERROR;
                } else {
                    fprintf(stderr, "unsupported log level %s.\n", aws_cli_optarg);
                    s_usage(1);
                }
                break;
            case 'h':
                s_usage(0);
                break;
            default:
                fprintf(stderr, "Unknown option\n");
                s_usage(1);
        }
    }

    if (aws_cli_optind < argc) {
        struct aws_byte_cursor uri_cursor = aws_byte_cursor_from_c_str(argv[aws_cli_optind++]);

        if (aws_uri_init_parse(&ctx->uri, ctx->allocator, &uri_cursor)) {
            fprintf(
                stderr,
                "Failed to parse uri %s with error %s\n",
                (char *)uri_cursor.ptr,
                aws_error_debug_str(aws_last_error()));
            s_usage(1);
        };
    } else {
        fprintf(stderr, "A URI for the request must be supplied.\n");
        s_usage(1);
    }
}

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
bool s_on_object(const struct aws_s3_object_info *info, void *user_data) {
    (void)user_data;

    printf("%-18" PRIu64 " %.*s\n", info->size, (int)info->key.len, info->key.ptr);
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
        if (has_more_results) {
            /* get next page */
            int result = aws_s3_paginator_continue(paginator, &app_ctx->signing_config);
            if (result) {
                fprintf(stderr, "ERROR returned by aws_s3_paginator_continue from s_on_list_finished: %d\n", result);
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

void setup_logger(struct app_ctx *app_ctx) {
    struct aws_logger_standard_options logger_options = {
        .level = app_ctx->log_level,
        .file = stderr,
    };

    aws_logger_init_standard(&app_ctx->logger, app_ctx->allocator, &logger_options);
    aws_logger_set(&app_ctx->logger);
}

int main(int argc, char *argv[]) {

    struct aws_allocator *allocator = aws_default_allocator();
    aws_s3_library_init(allocator);

    struct app_ctx app_ctx;
    AWS_ZERO_STRUCT(app_ctx);
    app_ctx.allocator = allocator;
    app_ctx.c_var = (struct aws_condition_variable)AWS_CONDITION_VARIABLE_INIT;
    aws_mutex_init(&app_ctx.mutex);

    s_parse_options(argc, argv, &app_ctx);

    struct aws_byte_cursor bucket = app_ctx.uri.host_name;
    struct aws_byte_cursor prefix;
    if (app_ctx.uri.path.len == 0 || (app_ctx.uri.path.len == 1 && app_ctx.uri.path.ptr[0] == '/')) {
        prefix.len = 0;
        prefix.ptr = NULL;
    } else {
        /* skips the initial / in the path */
        prefix.len = app_ctx.uri.path.len - 1;
        prefix.ptr = app_ctx.uri.path.ptr + 1;
    }

    if (app_ctx.log_level != AWS_LOG_LEVEL_NONE) {
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
    struct aws_credentials_provider *credentials_provider =
        aws_credentials_provider_new_chain_default(allocator, &credentials_provider_options);

    /* signing config */
    aws_s3_init_default_signing_config(
        &app_ctx.signing_config, aws_byte_cursor_from_c_str(app_ctx.region), credentials_provider);
    app_ctx.signing_config.flags.use_double_uri_encode = false;

    /* s3 client */
    struct aws_s3_client_config client_config;
    AWS_ZERO_STRUCT(client_config);
    client_config.client_bootstrap = client_bootstrap;
    client_config.region = aws_byte_cursor_from_c_str(app_ctx.region);
    client_config.signing_config = &app_ctx.signing_config;
    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    /* listObjects */
    struct aws_s3_list_objects_params params = {.client = client, .bucket_name = bucket, .prefix = prefix};

    char endpoint[1024];
    snprintf(endpoint, sizeof(endpoint), "s3.%s.amazonaws.com", app_ctx.region);
    params.endpoint = aws_byte_cursor_from_c_str(endpoint);
    params.user_data = &app_ctx;
    params.on_object = &s_on_object;
    params.on_list_finished = &s_on_list_finished;

    struct aws_s3_paginator *paginator = aws_s3_initiate_list_objects(allocator, &params);
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
