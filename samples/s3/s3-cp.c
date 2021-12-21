/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/command_line_parser.h>
#include <aws/common/file.h>
#include <aws/io/stream.h>
#include <aws/io/uri.h>
#include <aws/s3/private/s3_auto_ranged_put.h>
#include <aws/s3/private/s3_copy_object.h>
#include <aws/s3/private/s3_list_objects.h>

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
    const char *source_endpoint;
    const char *dest_endpoint;
    size_t expected_transfers;
    size_t completed_transfers;
    bool list_objects_completed;
    bool source_s3;
    bool source_file_system;
    bool dest_s3;
    bool dest_file_system;
    bool source_is_directory_or_prefix;
};

struct single_transfer_ctx {
    struct cp_app_ctx *cp_app_ctx;
    struct progress_listener *listener;
    struct aws_s3_meta_request *meta_request;
    FILE *output_sink;
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

            struct aws_uri *uri_to_parse = !src_uri_found ? &ctx->source_uri : &ctx->destination_uri;

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

static void s_dispatch_and_run_transfers(struct cp_app_ctx *cp_app_ctx);

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
    progress_listener_group_run_background_render_thread(cp_app_ctx.listener_group);

    char source_endpoint[1024];
    AWS_ZERO_ARRAY(source_endpoint);
    char dest_endpoint[1024];
    AWS_ZERO_ARRAY(dest_endpoint);

    struct aws_byte_cursor s3_scheme = aws_byte_cursor_from_c_str("s3");
    struct aws_byte_cursor file_scheme = aws_byte_cursor_from_c_str("file");

    if (aws_byte_cursor_eq_ignore_case(&cp_app_ctx.source_uri.scheme, &s3_scheme)) {
        cp_app_ctx.source_s3 = true;
        cp_app_ctx.source_is_directory_or_prefix = true;

        struct aws_byte_cursor source_bucket = cp_app_ctx.source_uri.host_name;

        snprintf(
            source_endpoint,
            sizeof(source_endpoint),
            "%.*s.s3.%s.amazonaws.com",
            (int)source_bucket.len,
            source_bucket.ptr,
            app_ctx->region);

        cp_app_ctx.source_endpoint = source_endpoint;
    } else if (
        aws_byte_cursor_eq_ignore_case(&cp_app_ctx.source_uri.scheme, &file_scheme) ||
        cp_app_ctx.source_uri.scheme.len == 0) {
        cp_app_ctx.source_file_system = true;

        struct aws_string *path_str = aws_string_new_from_buf(app_ctx->allocator, &cp_app_ctx.source_uri.uri_str);
        struct aws_string *path_open_mode = aws_string_new_from_c_str(app_ctx->allocator, "r");

        FILE *file_open_check = NULL;
        if (aws_directory_exists(path_str)) {
            cp_app_ctx.source_is_directory_or_prefix = true;
        } else if ((file_open_check = aws_fopen_safe(path_str, path_open_mode))) {
            cp_app_ctx.source_is_directory_or_prefix = false;
            fclose(file_open_check);
        } else {
            fprintf(stderr, "Source path does not exist\n");
            s_usage(1);
        }

        aws_string_destroy(path_open_mode);
        aws_string_destroy(path_str);
    } else {
        fprintf(stderr, "Source URI type is unsupported. s3://, file://, or / are currently supported\n");
        s_usage(1);
    }

    if (aws_byte_cursor_eq_ignore_case(&cp_app_ctx.destination_uri.scheme, &s3_scheme)) {
        cp_app_ctx.dest_s3 = true;

        struct aws_byte_cursor destination_bucket = cp_app_ctx.destination_uri.host_name;

        snprintf(
            dest_endpoint,
            sizeof(dest_endpoint),
            "%.*s.s3.%s.amazonaws.com",
            (int)destination_bucket.len,
            destination_bucket.ptr,
            app_ctx->region);

        cp_app_ctx.dest_endpoint = dest_endpoint;

    } else if (
        aws_byte_cursor_eq_ignore_case(&cp_app_ctx.destination_uri.scheme, &file_scheme) ||
        cp_app_ctx.destination_uri.scheme.len == 0) {
        cp_app_ctx.dest_file_system = true;
    } else {
        fprintf(stderr, "Destination URI type is unsupported. s3://, file://, or / are currently supported\n");
        s_usage(1);
    }

    s_dispatch_and_run_transfers(&cp_app_ctx);

    /* come back to all of this once it's moved

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

    // wait completion of the meta request
    aws_mutex_lock(&cp_app_ctx.mutex);
    aws_condition_variable_wait_pred(&cp_app_ctx.c_var, &cp_app_ctx.mutex, s_app_completion_predicate, &app_ctx);
    aws_mutex_unlock(&cp_app_ctx.mutex);

    // release resources
    aws_s3_meta_request_release(meta_request);
    */

    aws_condition_variable_clean_up(&cp_app_ctx.c_var);
    aws_mutex_clean_up(&cp_app_ctx.mutex);

    return 0;
}

struct progress_update_stream {
    struct aws_input_stream input_stream;
    struct single_transfer_ctx *transfer;
    struct aws_input_stream *wrapped_stream;
};

int s_input_seek(struct aws_input_stream *stream, int64_t offset, enum aws_stream_seek_basis basis) {
    struct progress_update_stream *update_stream = stream->impl;

    if (basis == AWS_SSB_BEGIN && offset == 0) {
        progress_listener_reset_progress(update_stream->transfer->listener);
        struct aws_string *state =
            aws_string_new_from_c_str(update_stream->transfer->cp_app_ctx->app_ctx->allocator, "In Progress");
        progress_listener_update_state(update_stream->transfer->listener, state);
        aws_string_destroy(state);
    }

    return update_stream->wrapped_stream->vtable->seek(stream, offset, basis);
}

int s_input_read(struct aws_input_stream *stream, struct aws_byte_buf *dest) {
    struct progress_update_stream *update_stream = stream->impl;

    size_t current_len = dest->len;
    int val = update_stream->wrapped_stream->vtable->read(update_stream->wrapped_stream, dest);
    size_t progress = dest->len - current_len;
    progress_listener_update_progress(update_stream->transfer->listener, progress);
    return val;
}

static int s_input_get_status(struct aws_input_stream *stream, struct aws_stream_status *status) {
    struct progress_update_stream *update_stream = stream->impl;

    return update_stream->wrapped_stream->vtable->get_status(update_stream->wrapped_stream, status);
}

static int s_input_get_length(struct aws_input_stream *stream, int64_t *out_length) {
    struct progress_update_stream *update_stream = stream->impl;
    return update_stream->wrapped_stream->vtable->get_length(update_stream->wrapped_stream, out_length);
}

static void s_input_destroy(struct aws_input_stream *stream) {
    struct progress_update_stream *update_stream = stream->impl;
    update_stream->wrapped_stream->vtable->destroy(update_stream->wrapped_stream);
    aws_mem_release(stream->allocator, update_stream);
}

static struct aws_input_stream_vtable s_update_input_stream_vtable = {
    .get_length = s_input_get_length,
    .seek = s_input_seek,
    .read = s_input_read,
    .get_status = s_input_get_status,
    .destroy = s_input_destroy,
};

void s_put_request_finished(
    struct aws_s3_meta_request *meta_request,
    const struct aws_s3_meta_request_result *meta_request_result,
    void *user_data) {
    (void)meta_request;

    struct single_transfer_ctx *transfer_ctx = user_data;

    struct aws_string *state = NULL;
    if (meta_request_result->error_code == AWS_ERROR_SUCCESS) {
        state = aws_string_new_from_c_str(transfer_ctx->cp_app_ctx->app_ctx->allocator, "Completed");
    } else {
        state = aws_string_new_from_c_str(transfer_ctx->cp_app_ctx->app_ctx->allocator, "Failed");
    }

    progress_listener_update_state(transfer_ctx->listener, state);
    aws_string_destroy(state);

    aws_mutex_lock(&transfer_ctx->cp_app_ctx->mutex);
    transfer_ctx->cp_app_ctx->completed_transfers++;
    aws_mutex_unlock(&transfer_ctx->cp_app_ctx->mutex);
    aws_condition_variable_notify_one(&transfer_ctx->cp_app_ctx->c_var);
    aws_s3_meta_request_release(transfer_ctx->meta_request);
    aws_mem_release(transfer_ctx->cp_app_ctx->app_ctx->allocator, transfer_ctx);
}

static bool s_are_all_transfers_done(void *arg) {
    struct cp_app_ctx *cp_app_ctx = arg;
    return cp_app_ctx->expected_transfers == cp_app_ctx->completed_transfers;
}

static bool s_on_directory_entry(const struct aws_directory_entry *entry, void *user_data) {
    struct cp_app_ctx *cp_app_ctx = user_data;

    if (entry->file_type & AWS_FILE_TYPE_FILE) {
        struct single_transfer_ctx *transfer_ctx =
            aws_mem_calloc(cp_app_ctx->app_ctx->allocator, 1, sizeof(struct single_transfer_ctx));
        transfer_ctx->cp_app_ctx = cp_app_ctx;

        struct aws_byte_buf uri_path;
        struct aws_byte_cursor destination_path = entry->relative_path;

        if (destination_path.len >= 2 && destination_path.ptr[0] == '.') {
            aws_byte_cursor_advance(&destination_path, 1);
        }
        aws_byte_buf_init_copy_from_cursor(&uri_path, cp_app_ctx->app_ctx->allocator, cp_app_ctx->destination_uri.path);
        aws_byte_buf_append_dynamic(&uri_path, &destination_path);

        for (size_t i = 0; i < uri_path.len; ++i) {
            if (uri_path.buffer[i] == '\\') {
                uri_path.buffer[i] = '/';
            }
        }

        struct aws_byte_cursor full_path = aws_byte_cursor_from_buf(&uri_path);

        if (uri_path.buffer[0] == '.') {
            aws_byte_cursor_advance(&full_path, 1);
        }

        struct aws_byte_buf label_buf;
        struct aws_byte_cursor operation_name_cur = aws_byte_cursor_from_c_str("upload: ");
        aws_byte_buf_init_copy_from_cursor(&label_buf, cp_app_ctx->app_ctx->allocator, operation_name_cur);
        aws_byte_buf_append_dynamic(&label_buf, &entry->relative_path);
        struct aws_byte_cursor to_cur = aws_byte_cursor_from_c_str(" to s3://");
        aws_byte_buf_append_dynamic(&label_buf, &to_cur);
        aws_byte_buf_append_dynamic(&label_buf, &cp_app_ctx->destination_uri.authority);
        aws_byte_buf_append_dynamic(&label_buf, &full_path);

        struct aws_string *label = aws_string_new_from_buf(cp_app_ctx->app_ctx->allocator, &label_buf);
        aws_byte_buf_clean_up(&label_buf);

        struct aws_string *state = aws_string_new_from_c_str(cp_app_ctx->app_ctx->allocator, "In Progress");

        transfer_ctx->listener = progress_listener_new(cp_app_ctx->listener_group, label, state, entry->file_size);
        aws_string_destroy(state);
        aws_string_destroy(label);
        aws_byte_buf_clean_up(&label_buf);

        struct aws_s3_meta_request_options request_options = {
            .user_data = transfer_ctx,
            .signing_config = &cp_app_ctx->app_ctx->signing_config,
            .type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
            .finish_callback = s_put_request_finished,
        };

        struct aws_http_header host_header = {
            .name = aws_byte_cursor_from_c_str("host"),
            .value = aws_byte_cursor_from_c_str(cp_app_ctx->dest_endpoint),
        };

        char content_length[256];
        AWS_ZERO_ARRAY(content_length);
        snprintf(content_length, AWS_ARRAY_SIZE(content_length), "%" PRIu64, entry->file_size);

        struct aws_http_header content_length_header = {
            .name = aws_byte_cursor_from_c_str("content-length"),
            .value = aws_byte_cursor_from_c_str(content_length),
        };

        request_options.message = aws_http_message_new_request(cp_app_ctx->app_ctx->allocator);
        aws_http_message_add_header(request_options.message, host_header);
        aws_http_message_add_header(request_options.message, content_length_header);
        aws_http_message_set_request_method(request_options.message, aws_http_method_put);
        aws_http_message_set_request_path(request_options.message, full_path);

        struct aws_input_stream *body_input =
            aws_input_stream_new_from_file(cp_app_ctx->app_ctx->allocator, (const char *)entry->path.ptr);
        if (body_input) {

            struct progress_update_stream *update_stream =
                aws_mem_calloc(cp_app_ctx->app_ctx->allocator, 1, sizeof(struct progress_update_stream));
            update_stream->transfer = transfer_ctx;
            update_stream->wrapped_stream = body_input;
            update_stream->input_stream.vtable = &s_update_input_stream_vtable;
            update_stream->input_stream.impl = update_stream;
            update_stream->input_stream.allocator = cp_app_ctx->app_ctx->allocator;
            aws_http_message_set_body_stream(request_options.message, &update_stream->input_stream);

            transfer_ctx->meta_request = aws_s3_client_make_meta_request(cp_app_ctx->app_ctx->client, &request_options);

            aws_mutex_lock(&transfer_ctx->cp_app_ctx->mutex);
            transfer_ctx->cp_app_ctx->expected_transfers++;
            aws_mutex_unlock(&transfer_ctx->cp_app_ctx->mutex);
        }
    }

    return true;
}

int s_get_body_callback(
    struct aws_s3_meta_request *meta_request,
    const struct aws_byte_cursor *body,
    uint64_t range_start,
    void *user_data) {
    (void)meta_request;
    (void)range_start;

    struct single_transfer_ctx *transfer_ctx = user_data;

    fwrite(body->ptr, sizeof(uint8_t), body->len, transfer_ctx->output_sink);
    progress_listener_update_progress(transfer_ctx->listener, body->len);

    return AWS_OP_SUCCESS;
}

void s_get_request_finished(
    struct aws_s3_meta_request *meta_request,
    const struct aws_s3_meta_request_result *meta_request_result,
    void *user_data) {
    (void)meta_request;

    struct single_transfer_ctx *transfer_ctx = user_data;

    struct aws_string *state = NULL;
    if (meta_request_result->error_code == AWS_ERROR_SUCCESS) {
        state = aws_string_new_from_c_str(transfer_ctx->cp_app_ctx->app_ctx->allocator, "Completed");
    } else {
        state = aws_string_new_from_c_str(transfer_ctx->cp_app_ctx->app_ctx->allocator, "Failed");
    }

    progress_listener_update_state(transfer_ctx->listener, state);
    aws_string_destroy(state);

    fclose(transfer_ctx->output_sink);
    aws_mutex_lock(&transfer_ctx->cp_app_ctx->mutex);
    transfer_ctx->cp_app_ctx->completed_transfers++;
    aws_mutex_unlock(&transfer_ctx->cp_app_ctx->mutex);
    aws_condition_variable_notify_one(&transfer_ctx->cp_app_ctx->c_var);
    aws_s3_meta_request_release(transfer_ctx->meta_request);
    aws_mem_release(transfer_ctx->cp_app_ctx->app_ctx->allocator, transfer_ctx);
}

static bool s_on_list_object(const struct aws_s3_object_info *info, void *user_data) {
    struct cp_app_ctx *cp_app_ctx = user_data;

    /* size greater than zero means it's an actual object. */
    if (info->key.len > 0) {
        struct aws_byte_buf dest_directory;
        aws_byte_buf_init_copy(&dest_directory, cp_app_ctx->app_ctx->allocator, &cp_app_ctx->destination_uri.uri_str);

        struct aws_string *dir_path = aws_string_new_from_buf(cp_app_ctx->app_ctx->allocator, &dest_directory);
        if (!aws_directory_exists(dir_path)) {
            aws_directory_create(dir_path);
        }
        aws_string_destroy(dir_path);

        struct aws_byte_cursor trimmed_key = info->key;

        if (info->prefix.len) {
            aws_byte_cursor_advance(&trimmed_key, info->prefix.len);
        }

        struct aws_array_list splits;
        aws_array_list_init_dynamic(&splits, cp_app_ctx->app_ctx->allocator, 8, sizeof(struct aws_byte_cursor));
        aws_byte_cursor_split_on_char(&trimmed_key, '/', &splits);

        for (size_t i = 0; i < aws_array_list_length(&splits); ++i) {
            struct aws_byte_cursor path_component;
            aws_array_list_get_at(&splits, &path_component, i);

            if (path_component.len > 0) {
                if (dest_directory.buffer[dest_directory.len - 1] != AWS_PATH_DELIM) {
                    struct aws_byte_cursor slash_cur = aws_byte_cursor_from_c_str(AWS_PATH_DELIM_STR);
                    aws_byte_buf_append_dynamic(&dest_directory, &slash_cur);
                }
                aws_byte_buf_append_dynamic(&dest_directory, &path_component);

                dir_path = aws_string_new_from_buf(cp_app_ctx->app_ctx->allocator, &dest_directory);
                if (i < aws_array_list_length(&splits) - 1 && !aws_directory_exists(dir_path)) {
                    aws_directory_create(dir_path);
                }
                aws_string_destroy(dir_path);
            }
        }

        struct single_transfer_ctx *transfer_ctx =
            aws_mem_calloc(cp_app_ctx->app_ctx->allocator, 1, sizeof(struct single_transfer_ctx));
        transfer_ctx->cp_app_ctx = cp_app_ctx;

        struct aws_byte_buf label_buf;
        struct aws_byte_cursor operation_name_cur = aws_byte_cursor_from_c_str("download: s3://");
        aws_byte_buf_init_copy_from_cursor(&label_buf, cp_app_ctx->app_ctx->allocator, operation_name_cur);
        aws_byte_buf_append_dynamic(&label_buf, &cp_app_ctx->source_uri.host_name);
        struct aws_byte_cursor slash_cur = aws_byte_cursor_from_c_str("/");
        aws_byte_buf_append_dynamic(&label_buf, &slash_cur);
        aws_byte_buf_append_dynamic(&label_buf, &info->key);
        struct aws_byte_cursor to_cur = aws_byte_cursor_from_c_str(" to ");
        aws_byte_buf_append_dynamic(&label_buf, &to_cur);
        struct aws_byte_cursor dest_dir_cur = aws_byte_cursor_from_buf(&dest_directory);
        aws_byte_buf_append_dynamic(&label_buf, &dest_dir_cur);

        struct aws_string *label = aws_string_new_from_buf(cp_app_ctx->app_ctx->allocator, &label_buf);
        aws_byte_buf_clean_up(&label_buf);

        struct aws_string *state = aws_string_new_from_c_str(cp_app_ctx->app_ctx->allocator, "In Progress");

        transfer_ctx->listener = progress_listener_new(cp_app_ctx->listener_group, label, state, info->size);
        aws_string_destroy(state);
        aws_string_destroy(label);
        aws_byte_buf_clean_up(&label_buf);

        struct aws_string *file_path = aws_string_new_from_buf(cp_app_ctx->app_ctx->allocator, &dest_directory);
        aws_byte_buf_clean_up(&dest_directory);
        struct aws_string *mode = aws_string_new_from_c_str(cp_app_ctx->app_ctx->allocator, "wb");
        transfer_ctx->output_sink = aws_fopen_safe(file_path, mode);
        aws_string_destroy(mode);
        aws_string_destroy(file_path);

        struct aws_s3_meta_request_options request_options = {
            .user_data = transfer_ctx,
            .signing_config = &cp_app_ctx->app_ctx->signing_config,
            .type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
            .finish_callback = s_get_request_finished,
            .body_callback = s_get_body_callback,
        };

        struct aws_http_header host_header = {
            .name = aws_byte_cursor_from_c_str("host"),
            .value = aws_byte_cursor_from_c_str(cp_app_ctx->source_endpoint),
        };

        struct aws_http_header accept_header = {
            .name = aws_byte_cursor_from_c_str("accept"),
            .value = aws_byte_cursor_from_c_str("*/*"),
        };

        struct aws_http_header user_agent_header = {
            .name = aws_byte_cursor_from_c_str("user-agent"),
            .value = aws_byte_cursor_from_c_str("AWS common runtime command-line client"),
        };

        request_options.message = aws_http_message_new_request(cp_app_ctx->app_ctx->allocator);
        aws_http_message_add_header(request_options.message, host_header);
        aws_http_message_add_header(request_options.message, accept_header);
        aws_http_message_add_header(request_options.message, user_agent_header);
        aws_http_message_set_request_method(request_options.message, aws_http_method_get);

        struct aws_byte_buf path_buf;
        aws_byte_buf_init(&path_buf, cp_app_ctx->app_ctx->allocator, info->key.len + 1);
        aws_byte_buf_append_dynamic(&path_buf, &slash_cur);
        aws_byte_buf_append_dynamic(&path_buf, &info->key);
        struct aws_byte_cursor path_cur = aws_byte_cursor_from_buf(&path_buf);
        aws_http_message_set_request_path(request_options.message, path_cur);
        aws_byte_buf_clean_up(&path_buf);

        transfer_ctx->meta_request = aws_s3_client_make_meta_request(cp_app_ctx->app_ctx->client, &request_options);

        aws_mutex_lock(&transfer_ctx->cp_app_ctx->mutex);
        transfer_ctx->cp_app_ctx->expected_transfers++;
        aws_mutex_unlock(&transfer_ctx->cp_app_ctx->mutex);
    }

    return true;
}

static bool s_are_all_transfers_and_listings_done(void *arg) {
    struct cp_app_ctx *cp_app_ctx = arg;
    return cp_app_ctx->expected_transfers == cp_app_ctx->completed_transfers && cp_app_ctx->list_objects_completed;
}

void s_on_object_list_finished(struct aws_s3_paginator *paginator, int error_code, void *user_data) {
    (void)error_code;

    struct cp_app_ctx *cp_app_ctx = user_data;

    if (aws_s3_paginator_has_more_results(paginator)) {
        aws_s3_paginator_continue(paginator, &cp_app_ctx->app_ctx->signing_config);
    } else {
        aws_mutex_lock(&cp_app_ctx->mutex);
        cp_app_ctx->list_objects_completed = true;
        aws_mutex_unlock(&cp_app_ctx->mutex);
    }
}

void s_dispatch_and_run_transfers(struct cp_app_ctx *cp_app_ctx) {

    struct aws_s3_paginator *paginator = NULL;

    if (cp_app_ctx->source_is_directory_or_prefix) {
        if (cp_app_ctx->source_file_system) {
            struct aws_string *path =
                aws_string_new_from_buf(cp_app_ctx->app_ctx->allocator, &cp_app_ctx->source_uri.uri_str);

            if (aws_directory_traverse(cp_app_ctx->app_ctx->allocator, path, true, s_on_directory_entry, cp_app_ctx)) {
                fprintf(
                    stderr, "Failure while traversing directory. Error %s\n", aws_error_debug_str(aws_last_error()));
                exit(1);
            }

            aws_string_destroy(path);
            aws_mutex_lock(&cp_app_ctx->mutex);
            aws_condition_variable_wait_pred(
                &cp_app_ctx->c_var, &cp_app_ctx->mutex, s_are_all_transfers_done, cp_app_ctx);
            aws_mutex_unlock(&cp_app_ctx->mutex);

        } else {
            char main_endpoint[1024];
            AWS_ZERO_ARRAY(main_endpoint);
            snprintf(main_endpoint, sizeof(main_endpoint), "s3.%s.amazonaws.com", cp_app_ctx->app_ctx->region);
            struct aws_byte_cursor prefix_cur = cp_app_ctx->source_uri.path;
            aws_byte_cursor_advance(&prefix_cur, 1);
            struct aws_s3_list_objects_params list_objects_params = {
                .user_data = cp_app_ctx,
                .endpoint = aws_byte_cursor_from_c_str(main_endpoint),
                .client = cp_app_ctx->app_ctx->client,
                .prefix = prefix_cur,
                .bucket_name = cp_app_ctx->source_uri.host_name,
                .on_object = s_on_list_object,
                .on_list_finished = s_on_object_list_finished,
            };

            paginator = aws_s3_initiate_list_objects(cp_app_ctx->app_ctx->allocator, &list_objects_params);
            aws_s3_paginator_continue(paginator, &cp_app_ctx->app_ctx->signing_config);

            aws_mutex_lock(&cp_app_ctx->mutex);
            aws_condition_variable_wait_pred(
                &cp_app_ctx->c_var, &cp_app_ctx->mutex, s_are_all_transfers_and_listings_done, cp_app_ctx);
            aws_mutex_unlock(&cp_app_ctx->mutex);

            aws_s3_paginator_release(paginator);
        }
    }
}
