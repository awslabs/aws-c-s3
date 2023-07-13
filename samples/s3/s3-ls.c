/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/command_line_parser.h>
#include <aws/common/condition_variable.h>
#include <aws/common/mutex.h>
#include <aws/common/zero.h>
#include <aws/io/uri.h>
#include <aws/s3/private/s3_list_objects.h>

#include "app_ctx.h"

#include <inttypes.h>
#include <stdio.h>

struct s3_ls_app_data {
    struct aws_uri uri;
    struct app_ctx *app_ctx;
    struct aws_mutex mutex;
    struct aws_condition_variable cvar;
    bool execution_completed;
    bool long_format;
};

static void s_usage(int exit_code) {
    FILE *output = exit_code == 0 ? stdout : stderr;
    fprintf(output, "usage: s3 ls [options] s3://{bucket}[/prefix]\n");
    fprintf(output, " bucket: the S3 bucket to list objects\n");
    fprintf(output, " prefix: the prefix to filter\n");
    fprintf(output, "  -l, List in long format\n");
    fprintf(output, "  -h, --help\n");
    fprintf(output, "            Display this message and quit.\n");
    exit(exit_code);
}

static struct aws_cli_option s_long_options[] = {
    {"long-format", AWS_CLI_OPTIONS_NO_ARGUMENT, NULL, 'l'},
    /* Per getopt(3) the last element of the array has to be filled with all zeros */
    {NULL, AWS_CLI_OPTIONS_NO_ARGUMENT, NULL, 0},
};

static void s_parse_options(int argc, char **argv, struct s3_ls_app_data *ctx) {
    int option_index = 0;

    int opt_val = 0;
    bool uri_found = false;
    do {
        opt_val = aws_cli_getopt_long(argc, argv, "l", s_long_options, &option_index);
        /* START_OF_TEXT means our positional argument */
        if (opt_val == 'l') {
            ctx->long_format = true;
        }
        if (opt_val == 0x02) {
            struct aws_byte_cursor uri_cursor = aws_byte_cursor_from_c_str(aws_cli_positional_arg);

            if (aws_uri_init_parse(&ctx->uri, ctx->app_ctx->allocator, &uri_cursor)) {
                fprintf(
                    stderr,
                    "Failed to parse uri %s with error %s\n",
                    (char *)uri_cursor.ptr,
                    aws_error_debug_str(aws_last_error()));
                s_usage(1);
            }
            uri_found = true;
        }
    } while (opt_val != -1);

    if (!uri_found) {
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
    struct s3_ls_app_data *app_ctx = arg;
    return app_ctx->execution_completed;
}

/**
 * Called once for each object returned in the ListObjectsV2 responses.
 */
int s_on_object(const struct aws_s3_object_info *info, void *user_data) {
    struct s3_ls_app_data *app_ctx = user_data;

    if (app_ctx->long_format) {
        printf("%-18" PRIu64 " ", info->size);
    }
    printf("%.*s\n", (int)info->key.len, info->key.ptr);
    return AWS_OP_SUCCESS;
}

/**
 * Called once for each ListObjectsV2 response received.
 * If the response contains a continuation token indicating there are more results to be fetched,
 * requests the next page using aws_s3_paginator_continue.
 */
void s_on_list_finished(struct aws_s3_paginator *paginator, int error_code, void *user_data) {
    struct s3_ls_app_data *app_ctx = user_data;

    if (error_code == 0) {
        bool has_more_results = aws_s3_paginator_has_more_results(paginator);
        if (has_more_results) {
            /* get next page */
            int result = aws_s3_paginator_continue(paginator, &app_ctx->app_ctx->signing_config);
            if (result) {
                fprintf(stderr, "ERROR returned by aws_s3_paginator_continue from s_on_list_finished: %d\n", result);
            }
            return;
        }
    } else {
        fprintf(
            stderr,
            "Failure while listing objects. Please check if you have valid credentials and s3 path is correct. "
            "Error: "
            "%s\n",
            aws_error_debug_str(error_code));
    }

    /* all pages received. triggers the condition variable to exit the application. */
    aws_mutex_lock(&app_ctx->mutex);
    app_ctx->execution_completed = true;
    aws_mutex_unlock(&app_ctx->mutex);
    aws_condition_variable_notify_one(&app_ctx->cvar);
}

int s3_ls_main(int argc, char *argv[], const char *command_name, void *user_data) {
    (void)command_name;
    struct app_ctx *app_ctx = user_data;

    if (app_ctx->help_requested) {
        s_usage(0);
    }

    struct s3_ls_app_data impl_data = {
        .app_ctx = app_ctx,
        .mutex = AWS_MUTEX_INIT,
        .cvar = AWS_CONDITION_VARIABLE_INIT,
    };

    app_ctx->sub_command_data = &impl_data;

    s_parse_options(argc, argv, &impl_data);

    struct aws_byte_cursor bucket = impl_data.uri.host_name;
    struct aws_byte_cursor prefix;
    if (impl_data.uri.path.len == 0 || (impl_data.uri.path.len == 1 && impl_data.uri.path.ptr[0] == '/')) {
        prefix.len = 0;
        prefix.ptr = NULL;
    } else {
        /* skips the initial / in the path */
        prefix.len = impl_data.uri.path.len - 1;
        prefix.ptr = impl_data.uri.path.ptr + 1;
    }

    /* listObjects */
    struct aws_s3_list_objects_params params = {.client = app_ctx->client, .bucket_name = bucket, .prefix = prefix};

    char endpoint[1024];
    snprintf(endpoint, sizeof(endpoint), "s3.%s.amazonaws.com", app_ctx->region);
    params.endpoint = aws_byte_cursor_from_c_str(endpoint);
    params.user_data = &impl_data;
    params.on_object = &s_on_object;
    params.on_list_finished = &s_on_list_finished;

    struct aws_s3_paginator *paginator = aws_s3_initiate_list_objects(app_ctx->allocator, &params);
    int paginator_result = aws_s3_paginator_continue(paginator, &app_ctx->signing_config);
    if (paginator_result) {
        printf("ERROR returned from initial call to aws_s3_paginator_continue: %d \n", paginator_result);
    }

    aws_s3_paginator_release(paginator);

    /* wait completion of last page */
    aws_mutex_lock(&impl_data.mutex);
    aws_condition_variable_wait_pred(&impl_data.cvar, &impl_data.mutex, s_app_completion_predicate, &impl_data);
    aws_mutex_unlock(&impl_data.mutex);

    return 0;
}
