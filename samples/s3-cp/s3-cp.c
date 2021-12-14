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
#include <aws/s3/private/s3_copy_object.h>
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
    struct aws_uri source_uri;
    struct aws_uri destination_uri;
};

static void s_usage(int exit_code) {

    fprintf(
        stderr,
        "usage: s3-cp [options] s3://{source_bucket/source_object_key} "
        "s3://{destination_bucket/destination_object_key}\n");
    fprintf(stderr, " source_bucket: the S3 bucket containing the object to copy\n");
    fprintf(stderr, " source_object_key: the key of the S3 Object to copy\n");
    fprintf(stderr, " destination_bucket: the S3 bucket the object will be copied to\n");
    fprintf(stderr, " destination_object_key: the key to be used for the new S3 object\n");
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
        struct aws_byte_cursor source_uri_cursor = aws_byte_cursor_from_c_str(argv[aws_cli_optind++]);

        if (aws_uri_init_parse(&ctx->source_uri, ctx->allocator, &source_uri_cursor)) {
            fprintf(
                stderr,
                "Failed to parse uri %s with error %s\n",
                (char *)source_uri_cursor.ptr,
                aws_error_debug_str(aws_last_error()));
            s_usage(1);
        };
    } else {
        fprintf(stderr, "An URI for the source object must be supplied.\n");
        s_usage(1);
    }

    if (aws_cli_optind < argc) {
        struct aws_byte_cursor destination_uri_cursor = aws_byte_cursor_from_c_str(argv[aws_cli_optind++]);

        if (aws_uri_init_parse(&ctx->destination_uri, ctx->allocator, &destination_uri_cursor)) {
            fprintf(
                stderr,
                "Failed to parse uri %s with error %s\n",
                (char *)destination_uri_cursor.ptr,
                aws_error_debug_str(aws_last_error()));
            s_usage(1);
        };
    } else {
        fprintf(stderr, "An URI for the destination object must be supplied.\n");
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

void setup_logger(struct app_ctx *app_ctx) {
    struct aws_logger_standard_options logger_options = {
        .level = app_ctx->log_level,
        .file = stderr,
    };

    aws_logger_init_standard(&app_ctx->logger, app_ctx->allocator, &logger_options);
    aws_logger_set(&app_ctx->logger);
}

static void s_meta_request_finish(
    struct aws_s3_meta_request *meta_request,
    const struct aws_s3_meta_request_result *meta_request_result,
    void *user_data) {

    (void)meta_request;

    /* copy finished. triggers the condition variable to exit the application. */
    struct app_ctx *app_ctx = user_data;
    aws_mutex_lock(&app_ctx->mutex);
    app_ctx->execution_completed = true;
    aws_mutex_unlock(&app_ctx->mutex);
    aws_condition_variable_notify_one(&app_ctx->c_var);
}

static int s_meta_request_headers(
    struct aws_s3_meta_request *meta_request,
    const struct aws_http_headers *headers,
    int response_status,
    void *user_data) {

    (void)meta_request;
    (void)headers;
    (void)user_data;

    return 0;
}

static void s_meta_request_shutdown(void *user_data) {

    (void)user_data;
}

static int s_meta_request_receive_body(
    struct aws_s3_meta_request *meta_request,
    const struct aws_byte_cursor *body,
    uint64_t range_start,
    void *user_data) {

    (void)meta_request;
    (void)body;
    (void)range_start;
    (void)user_data;

    return 0;
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

    // the URI path is / followed by the key
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
        .name = g_x_amz_copy_source_name, .value = aws_byte_cursor_from_buf(&copy_source_value_encoded)};
    if (aws_http_message_add_header(message, copy_source_header)) {
        goto error_clean_up_message;
    }

    if (aws_http_message_set_request_method(message, aws_http_method_put)) {
        goto error_clean_up_message;
    }

    return message;

error_clean_up_message:

    if (message != NULL) {
        aws_http_message_release(message);
        message = NULL;
    }

    return NULL;
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

    struct aws_byte_cursor source_bucket = app_ctx.source_uri.host_name;
    struct aws_byte_cursor source_key = {
        .ptr = app_ctx.source_uri.path.ptr + 1, /* skips the initial / */
        .len = app_ctx.source_uri.path.len - 1};

    struct aws_byte_cursor destination_bucket = app_ctx.destination_uri.host_name;
    struct aws_byte_cursor destination_key = {
        .ptr = app_ctx.destination_uri.path.ptr + 1, /* skips the initial / */
        .len = app_ctx.destination_uri.path.len - 1};

    /* TODO: remove debug messages below */
    printf("source bucket: %.*s\n", (int)source_bucket.len, source_bucket.ptr);
    printf("source key: %.*s\n", (int)source_key.len, source_key.ptr);

    printf("destination bucket: %.*s\n", (int)destination_bucket.len, destination_bucket.ptr);
    printf("destination key: %.*s\n", (int)destination_key.len, destination_key.ptr);

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

    char endpoint[1024];
    snprintf(
        endpoint,
        sizeof(endpoint),
        "%.*s.s3.%s.amazonaws.com",
        (int)destination_bucket.len,
        destination_bucket.ptr,
        app_ctx.region);

    /* creates a CopyObject request */
    struct aws_http_message *message = copy_object_request_new(
        allocator, source_bucket, source_key, aws_byte_cursor_from_c_str(endpoint), destination_key);

    struct aws_s3_meta_request_options meta_request_options = {
        .user_data = &app_ctx,
        .body_callback = s_meta_request_receive_body,
        .signing_config = &app_ctx.signing_config,
        .finish_callback = s_meta_request_finish,
        .headers_callback = s_meta_request_headers,
        .message = message,
        .shutdown_callback = s_meta_request_shutdown,
        .type = AWS_S3_META_REQUEST_TYPE_COPY_OBJECT};

    struct aws_s3_meta_request *meta_request = aws_s3_client_make_meta_request(client, &meta_request_options);
    if (meta_request == NULL) {
        printf("*** meta_request IS NULL\n");
    }

    /* wait completion of last page */
    aws_mutex_lock(&app_ctx.mutex);
    aws_condition_variable_wait_pred(&app_ctx.c_var, &app_ctx.mutex, s_app_completion_predicate, &app_ctx);
    aws_mutex_unlock(&app_ctx.mutex);

    /* release resources */
    aws_s3_meta_request_release(meta_request);
    aws_s3_client_release(client);
    aws_credentials_provider_release(credentials_provider);
    aws_client_bootstrap_release(client_bootstrap);
    aws_host_resolver_release(resolver);
    aws_event_loop_group_release(event_loop_group);
    aws_mutex_clean_up(&app_ctx.mutex);
    aws_s3_library_clean_up();

    return 0;
}
