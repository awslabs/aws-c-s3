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

#include "app_ctx.h"

#include <stdio.h>

int s3_ls_main(int argc, char *const argv[], const char *command_name, void *user_data);
int s3_cp_main(int argc, char *const argv[], const char *command_name, void *user_data);
int s3_compute_platform_info_main(int argc, char *const argv[], const char *command_name, void *user_data);

static struct aws_cli_subcommand_dispatch s_dispatch_table[] = {
    {
        .command_name = "ls",
        .subcommand_fn = s3_ls_main,
    },
    {
        .command_name = "cp",
        .subcommand_fn = s3_cp_main,
    },
    {
        .command_name = "platform-info",
        .subcommand_fn = s3_compute_platform_info_main,
    },
};

static void s_usage(int exit_code) {

    FILE *output = exit_code == 0 ? stdout : stderr;
    fprintf(output, "usage: s3 <command> <options>\n");
    fprintf(output, " available commands:\n");

    for (size_t i = 0; i < AWS_ARRAY_SIZE(s_dispatch_table); ++i) {
        fprintf(output, " %s\n", s_dispatch_table[i].command_name);
    }

    fflush(output);
    exit(exit_code);
}

static void s_setup_logger(struct app_ctx *app_ctx) {
    struct aws_logger_standard_options logger_options = {
        .level = app_ctx->log_level,
        .file = stderr,
    };

    aws_logger_init_standard(&app_ctx->logger, app_ctx->allocator, &logger_options);
    aws_logger_set(&app_ctx->logger);
}

static struct aws_cli_option s_long_options[] = {
    {"region", AWS_CLI_OPTIONS_REQUIRED_ARGUMENT, NULL, 'r'},
    {"verbose", AWS_CLI_OPTIONS_REQUIRED_ARGUMENT, NULL, 'v'},
    {"help", AWS_CLI_OPTIONS_NO_ARGUMENT, NULL, 'h'},
    /* Per getopt(3) the last element of the array has to be filled with all zeros */
    {NULL, AWS_CLI_OPTIONS_NO_ARGUMENT, NULL, 0},
};

static void s_parse_app_ctx(int argc, char *const argv[], struct app_ctx *app_ctx) {

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
                app_ctx->region = aws_cli_optarg;
                break;
            case 'v':
                if (!strcmp(aws_cli_optarg, "TRACE")) {
                    app_ctx->log_level = AWS_LL_TRACE;
                } else if (!strcmp(aws_cli_optarg, "INFO")) {
                    app_ctx->log_level = AWS_LL_INFO;
                } else if (!strcmp(aws_cli_optarg, "DEBUG")) {
                    app_ctx->log_level = AWS_LL_DEBUG;
                } else if (!strcmp(aws_cli_optarg, "ERROR")) {
                    app_ctx->log_level = AWS_LL_ERROR;
                } else {
                    fprintf(stderr, "unsupported log level %s.\n", aws_cli_optarg);
                    s_usage(1);
                }
                break;
            case 'h':
                app_ctx->help_requested = true;
                break;
            default:
                break;
        }
    }

    if (!app_ctx->help_requested) {
        if (app_ctx->log_level != AWS_LOG_LEVEL_NONE) {
            s_setup_logger(app_ctx);
        }
    }

    /* reset for the next parser */
    aws_cli_reset_state();

    /* signing config */
    aws_s3_init_default_signing_config(
        &app_ctx->signing_config, aws_byte_cursor_from_c_str(app_ctx->region), app_ctx->credentials_provider);
    app_ctx->signing_config.flags.use_double_uri_encode = false;

    /* s3 client */
    struct aws_s3_client_config client_config;
    AWS_ZERO_STRUCT(client_config);
    client_config.client_bootstrap = app_ctx->client_bootstrap;
    client_config.region = aws_byte_cursor_from_c_str(app_ctx->region);
    client_config.signing_config = &app_ctx->signing_config;
    app_ctx->client = aws_s3_client_new(app_ctx->allocator, &client_config);
}

int main(int argc, char *argv[]) {

    struct aws_allocator *allocator = aws_default_allocator();
    aws_s3_library_init(allocator);

    struct app_ctx app_ctx;
    AWS_ZERO_STRUCT(app_ctx);
    app_ctx.allocator = allocator;
    app_ctx.c_var = (struct aws_condition_variable)AWS_CONDITION_VARIABLE_INIT;
    aws_mutex_init(&app_ctx.mutex);

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
    app_ctx.client_bootstrap = aws_client_bootstrap_new(allocator, &bootstrap_options);
    if (app_ctx.client_bootstrap == NULL) {
        printf("ERROR initializing client bootstrap\n");
        return -1;
    }

    /* credentials */
    struct aws_credentials_provider_chain_default_options credentials_provider_options;
    AWS_ZERO_STRUCT(credentials_provider_options);
    credentials_provider_options.bootstrap = app_ctx.client_bootstrap;
    app_ctx.credentials_provider = aws_credentials_provider_new_chain_default(allocator, &credentials_provider_options);

    s_parse_app_ctx(argc, argv, &app_ctx);
    int dispatch_return_code =
        aws_cli_dispatch_on_subcommand(argc, argv, s_dispatch_table, AWS_ARRAY_SIZE(s_dispatch_table), &app_ctx);

    if (dispatch_return_code &&
        (aws_last_error() == AWS_ERROR_INVALID_ARGUMENT || aws_last_error() == AWS_ERROR_UNIMPLEMENTED)) {
        s_usage(app_ctx.help_requested == true ? 0 : 1);
    }

    /* release resources */
    aws_s3_client_release(app_ctx.client);
    aws_credentials_provider_release(app_ctx.credentials_provider);
    aws_client_bootstrap_release(app_ctx.client_bootstrap);
    aws_host_resolver_release(resolver);
    aws_event_loop_group_release(event_loop_group);
    aws_mutex_clean_up(&app_ctx.mutex);
    aws_s3_library_clean_up();

    return dispatch_return_code;
}
