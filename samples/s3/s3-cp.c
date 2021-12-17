/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/command_line_parser.h>
#include <aws/io/uri.h>
#include <aws/s3/private/s3_copy_object.h>

#include "app_ctx.h"
#include "cli_progress_bar.h"

#include <inttypes.h>
#include <stdio.h>

struct cp_app_ctx {
    struct app_ctx *app_ctx;
    struct aws_uri source_uri;
    struct aws_uri destination_uri;
    struct progress_listener_group *listener_group;
    struct aws_mutex mutex;
    struct aws_condition_variable c_var;
};

struct single_transfer_ctx {
    struct cp_app_ctx *cp_app_ctx;
    struct progress_listener *listener;
};

static void s_usage(int exit_code) {
    FILE *sink = exit_code == 0 ? stdout : stderr;

    fprintf(
        sink,
        "usage: s3-cp [options] s3://{source_bucket/source_object_key} "
        "s3://{destination_bucket/destination_object_key}\n");
    fprintf(sink, " source_bucket: the S3 bucket containing the object to copy\n");
    fprintf(sink, " source_object_key: the key of the S3 Object to copy\n");
    fprintf(sink, " destination_bucket: the S3 bucket the object will be copied to\n");
    fprintf(sink, " destination_object_key: the key to be used for the new S3 object\n");
    exit(exit_code);
}

static struct aws_cli_option s_long_options[] = {
    /* Per getopt(3) the last element of the array has to be filled with all zeros */
    {NULL, AWS_CLI_OPTIONS_NO_ARGUMENT, NULL, 0},
};

static void s_parse_options(int argc, char **argv, struct cp_app_ctx *ctx) {
    bool src_uri_found = false;
    bool dest_uri_found = false;
    int option_index = 0;
    int opt_val = -1;

    do {
        opt_val = aws_cli_getopt_long(argc, argv, "", s_long_options, &option_index);
        /* START_OF_TEXT means our positional argument */
        if (opt_val == 0x02) {
            struct aws_byte_cursor uri_cursor = aws_byte_cursor_from_c_str(aws_cli_positional_arg);

            struct aws_uri *uri_to_parse = src_uri_found ? &ctx->source_uri : &ctx->destination_uri;

            if (aws_uri_init_parse(uri_to_parse, ctx->app_ctx->allocator, &uri_cursor)) {
                fprintf(
                    stderr,
                    "Failed to parse uri %s with error %s\n",
                    (char *)uri_cursor.ptr,
                    aws_error_debug_str(aws_last_error()));
                s_usage(1);
            }

            if (uri_to_parse == &ctx->source_uri) {
                src_uri_found = true;
            } else {
                dest_uri_found = true;
            }
        }
    } while (opt_val != -1);

    if (!(src_uri_found && dest_uri_found)) {
        fprintf(stderr, "An URI for the source and destination must be provided.\n");
        s_usage(1);
    }
}

static bool s_app_completion_predicate(void *arg) {
    struct app_ctx *app_ctx = arg;
    return app_ctx->execution_completed;
}

static void s_meta_request_finish(
    struct aws_s3_meta_request *meta_request,
    const struct aws_s3_meta_request_result *meta_request_result,
    void *user_data) {

    (void)meta_request;

    if (meta_request_result->error_code != AWS_ERROR_SUCCESS) {
        fprintf(
            stderr,
            "Meta request failed with error '%s', response status code: %d\n",
            aws_error_debug_str(meta_request_result->error_code),
            meta_request_result->response_status);
    }

    /* copy finished. triggers the condition variable to exit the application. */
    struct app_ctx *app_ctx = user_data;
    aws_mutex_lock(&app_ctx->mutex);
    app_ctx->execution_completed = true;
    aws_mutex_unlock(&app_ctx->mutex);
    aws_condition_variable_notify_one(&app_ctx->c_var);
}

static const struct aws_byte_cursor g_host_header_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Host");
static const struct aws_byte_cursor g_x_amz_copy_source_name =
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-copy-source");

struct aws_http_message *copy_object_request_new(
    struct aws_allocator *allocator,
    struct aws_byte_cursor source_bucket,
    struct aws_byte_cursor source_key,
    struct aws_byte_cursor endpoint,
    struct aws_byte_cursor destination_key) {

    AWS_PRECONDITION(allocator);

    struct aws_http_message *message = aws_http_message_new_request(allocator);

    if (message == NULL) {
        return NULL;
    }

    /* the URI path is / followed by the key */
    char destination_path[1024];
    snprintf(destination_path, sizeof(destination_path), "/%.*s", (int)destination_key.len, destination_key.ptr);

    if (aws_http_message_set_request_path(message, aws_byte_cursor_from_c_str(destination_path))) {
        goto error_clean_up_message;
    }

    struct aws_http_header host_header = {.name = g_host_header_name, .value = endpoint};
    if (aws_http_message_add_header(message, host_header)) {
        goto error_clean_up_message;
    }

    char copy_source_value[1024];
    snprintf(
        copy_source_value,
        sizeof(copy_source_value),
        "%.*s/%.*s",
        (int)source_bucket.len,
        source_bucket.ptr,
        (int)source_key.len,
        source_key.ptr);

    struct aws_byte_cursor copy_source_cursor = aws_byte_cursor_from_c_str(copy_source_value);
    struct aws_byte_buf copy_source_value_encoded;
    aws_byte_buf_init(&copy_source_value_encoded, allocator, 1024);
    aws_byte_buf_append_encoding_uri_param(&copy_source_value_encoded, &copy_source_cursor);

    struct aws_http_header copy_source_header = {
        .name = g_x_amz_copy_source_name,
        .value = aws_byte_cursor_from_buf(&copy_source_value_encoded),
    };
    if (aws_http_message_add_header(message, copy_source_header)) {
        goto error_clean_up_message;
    }

    if (aws_http_message_set_request_method(message, aws_http_method_put)) {
        goto error_clean_up_message;
    }

    aws_byte_buf_clean_up(&copy_source_value_encoded);
    return message;

error_clean_up_message:

    aws_byte_buf_clean_up(&copy_source_value_encoded);
    if (message != NULL) {
        aws_http_message_release(message);
        message = NULL;
    }

    return NULL;
}

int s3_cp_main(int argc, char *argv[], const char *command_name, void *user_data) {
    (void)command_name;

    struct app_ctx *app_ctx = user_data;

    if (app_ctx->help_requested) {
        s_usage(0);
    }

    struct cp_app_ctx cp_app_ctx = {
        .app_ctx = app_ctx,
        .mutex = AWS_MUTEX_INIT,
        .c_var = AWS_CONDITION_VARIABLE_INIT,
    };
    app_ctx->sub_command_data = &cp_app_ctx;

    s_parse_options(argc, argv, &cp_app_ctx);

    cp_app_ctx.listener_group = progress_listener_group_new(app_ctx->allocator);

    struct aws_byte_cursor source_bucket = cp_app_ctx.source_uri.host_name;
    struct aws_byte_cursor source_key = {
        .ptr = cp_app_ctx.source_uri.path.ptr + 1, /* skips the initial / */
        .len = cp_app_ctx.source_uri.path.len - 1,
    };

    struct aws_byte_cursor destination_bucket = cp_app_ctx.destination_uri.host_name;
    struct aws_byte_cursor destination_key = {
        .ptr = cp_app_ctx.destination_uri.path.ptr + 1, /* skips the initial / */
        .len = cp_app_ctx.destination_uri.path.len - 1,
    };

    /* TODO: remove debug messages below */
    printf("source bucket: %.*s\n", (int)source_bucket.len, source_bucket.ptr);
    printf("source key: %.*s\n", (int)source_key.len, source_key.ptr);

    printf("destination bucket: %.*s\n", (int)destination_bucket.len, destination_bucket.ptr);
    printf("destination key: %.*s\n", (int)destination_key.len, destination_key.ptr);

    char endpoint[1024];
    snprintf(
        endpoint,
        sizeof(endpoint),
        "%.*s.s3.%s.amazonaws.com",
        (int)destination_bucket.len,
        destination_bucket.ptr,
        app_ctx->region);

    /* creates a CopyObject request */
    struct aws_http_message *message = copy_object_request_new(
        app_ctx->allocator, source_bucket, source_key, aws_byte_cursor_from_c_str(endpoint), destination_key);

    struct aws_s3_meta_request_options meta_request_options = {
        .user_data = &app_ctx,
        .body_callback = NULL,
        .signing_config = &app_ctx->signing_config,
        .finish_callback = s_meta_request_finish,
        .headers_callback = NULL,
        .message = message,
        .shutdown_callback = NULL,
        .type = AWS_S3_META_REQUEST_TYPE_COPY_OBJECT,
    };

    struct aws_s3_meta_request *meta_request = aws_s3_client_make_meta_request(app_ctx->client, &meta_request_options);
    if (meta_request == NULL) {
        printf("*** meta_request IS NULL\n");
    }

    /* wait completion of the meta request */
    aws_mutex_lock(&cp_app_ctx.mutex);
    aws_condition_variable_wait_pred(&cp_app_ctx.c_var, &cp_app_ctx.mutex, s_app_completion_predicate, &app_ctx);
    aws_mutex_unlock(&cp_app_ctx.mutex);

    /* release resources */
    aws_s3_meta_request_release(meta_request);

    aws_condition_variable_clean_up(&cp_app_ctx.c_var);
    aws_mutex_clean_up(&cp_app_ctx.mutex);

    return 0;
}
