/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_client_impl.h"
#include "aws/s3/private/s3_request_context.h"
#include "aws/s3/private/s3_request_impl.h"
#include "aws/s3/s3_client_config.h"
#include "aws/s3/s3_request.h"
#include "aws/s3/s3_request_result.h"

#include <aws/common/clock.h>
#include <aws/common/string.h>

#include <aws/auth/credentials.h>
#include <aws/auth/signable.h>
#include <aws/auth/signing.h>
#include <aws/auth/signing_config.h>
#include <aws/auth/signing_result.h>

#include <aws/http/connection.h>
#include <aws/http/connection_manager.h>

#include <aws/common/atomics.h>
#include <aws/io/channel_bootstrap.h>
#include <aws/io/socket.h>

static int s_s3_client_setup_endpoint(struct aws_s3_client *client);
static int s_s3_client_add_required_headers(struct aws_s3_client *client, struct aws_http_message *message);

static void s_s3_client_dec_shutdown_wait_count(struct aws_s3_client *client);
static void s_s3_client_client_bootstrap_shutdown_callback(void *user_data);
static void s_s3_client_credentials_provider_shutdown_callback(void *user_data);
static void s_s3_client_connection_manager_shutdown_callback(void *user_data);

static void s_s3_client_signing_complete(struct aws_signing_result *result, int error_code, void *user_data);
static void s_s3_client_on_acquire_connection(struct aws_http_connection *connection, int error_code, void *user_data);

static void s_s3_client_stream_complete(struct aws_http_stream *stream, int error_code, void *user_data);
static int s_s3_client_incoming_headers(
    struct aws_http_stream *stream,
    enum aws_http_header_block header_block,
    const struct aws_http_header *headers,
    size_t num_headers,
    void *user_data);
static int s_s3_client_incoming_header_block_done(
    struct aws_http_stream *stream,
    enum aws_http_header_block header_block,
    void *user_data);
static int s_s3_client_incoming_body(
    struct aws_http_stream *stream,
    const struct aws_byte_cursor *data,
    void *user_data);
static void s_s3_client_stream_complete(struct aws_http_stream *stream, int error_code, void *user_data);

struct aws_s3_client *aws_s3_client_new(struct aws_allocator *allocator, struct aws_s3_client_config *client_config) {

    struct aws_s3_client *client = aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_client));

    if (client == NULL) {
        return NULL;
    }

    client->allocator = allocator;
    aws_atomic_init_int(&client->ref_count, 1);

    client->shutdown_callback = client_config->shutdown_callback;
    client->shutdown_callback_user_data = client_config->shutdown_callback_user_data;
    aws_atomic_init_int(&client->shutdown_wait_count, 0);

    client->region = aws_string_new_from_array(allocator, client_config->region.ptr, client_config->region.len);

    if (client->region == NULL) {
        goto error_clean_up;
    }

    client->bucket_name =
        aws_string_new_from_array(allocator, client_config->bucket_name.ptr, client_config->bucket_name.len);

    if (client->bucket_name == NULL) {
        goto error_clean_up;
    }

    if (s_s3_client_setup_endpoint(client)) {
        goto error_clean_up;
    }

    /* Client Bootstrap */
    {
        struct aws_client_bootstrap_options bootstrap_options;
        AWS_ZERO_STRUCT(bootstrap_options);
        bootstrap_options.event_loop_group = client_config->el_group;
        bootstrap_options.host_resolver = client_config->host_resolver;
        bootstrap_options.on_shutdown_complete = s_s3_client_client_bootstrap_shutdown_callback;
        bootstrap_options.user_data = client;

        client->client_bootstrap = aws_client_bootstrap_new(allocator, &bootstrap_options);

        if (client->client_bootstrap == NULL) {
            goto error_clean_up;
        }

        aws_atomic_fetch_add(&client->shutdown_wait_count, 1);
    }

    /* Connection Manager */
    {
        const int port = 80;            // TODO
        const int max_connections = 10; // TODO
        const int timeout_seconds = 10; // TODO

        struct aws_socket_options socket_options = {
            .type = AWS_SOCKET_STREAM,
            .domain = AWS_SOCKET_IPV4,
            .connect_timeout_ms =
                (uint32_t)aws_timestamp_convert(timeout_seconds, AWS_TIMESTAMP_SECS, AWS_TIMESTAMP_MILLIS, NULL),
        };

        struct aws_http_connection_manager_options manager_options = {
            .bootstrap = client->client_bootstrap,
            .initial_window_size = SIZE_MAX,
            .socket_options = &socket_options,
            .tls_connection_options = NULL,
            .proxy_options = NULL,
            .host = aws_byte_cursor_from_string(client->endpoint),
            .port = port,
            .max_connections = max_connections,
            .shutdown_complete_callback = s_s3_client_connection_manager_shutdown_callback,
            .shutdown_complete_user_data = client};

        client->connection_manager = aws_http_connection_manager_new(allocator, &manager_options);

        if (client->connection_manager == NULL) {
            goto error_clean_up;
        }

        aws_atomic_fetch_add(&client->shutdown_wait_count, 1);
    }

    /* Credentials Provider */
    {
        struct aws_credentials_provider_chain_default_options credentials_config;
        AWS_ZERO_STRUCT(credentials_config);
        credentials_config.bootstrap = client->client_bootstrap;
        credentials_config.shutdown_options.shutdown_callback = s_s3_client_credentials_provider_shutdown_callback;
        credentials_config.shutdown_options.shutdown_user_data = client;
        client->credentials_provider = aws_credentials_provider_new_chain_default(allocator, &credentials_config);

        if (client->credentials_provider == NULL) {
            goto error_clean_up;
        }

        aws_atomic_fetch_add(&client->shutdown_wait_count, 1);
    }

    AWS_LOGF_INFO(
        AWS_LS_S3_CLIENT,
        "Initiating client with bucket %s in region %s at endpoint %s",
        (const char *)client->bucket_name->bytes,
        (const char *)client->region->bytes,
        (const char *)client->endpoint->bytes);

    return client;

error_clean_up:

    if (client != NULL) {
        aws_s3_client_release(client);
        client = NULL;
    }

    return NULL;
}

int aws_s3_client_acquire(struct aws_s3_client *client) {
    aws_atomic_fetch_add(&client->ref_count, 1);
    return AWS_OP_SUCCESS;
}

void aws_s3_client_release(struct aws_s3_client *client) {
    size_t new_ref_count = aws_atomic_fetch_sub(&client->ref_count, 1) - 1;

    if (new_ref_count > 0) {
        return;
    }

    if (client->credentials_provider != NULL) {
        aws_credentials_provider_release(client->credentials_provider);
        client->credentials_provider = NULL;
    }

    if (client->connection_manager != NULL) {
        aws_http_connection_manager_release(client->connection_manager);
        client->connection_manager = NULL;
    }

    if (client->client_bootstrap != NULL) {
        aws_client_bootstrap_release(
            client
                ->client_bootstrap); // TODO should we be waiting until the connection manager is shutdown to call this?
        client->client_bootstrap = NULL;
    }

    if (client->region != NULL) {
        aws_string_destroy(client->region);
        client->region = NULL;
    }

    if (client->endpoint != NULL) {
        aws_string_destroy(client->endpoint);
        client->endpoint = NULL;
    }

    if (client->bucket_name != NULL) {
        aws_string_destroy(client->bucket_name);
        client->bucket_name = NULL;
    }
}

static void s_s3_client_dec_shutdown_wait_count(struct aws_s3_client *client) {
    size_t new_count = aws_atomic_fetch_sub(&client->shutdown_wait_count, 1) - 1;

    if (new_count > 0) {
        return;
    }

    aws_s3_client_shutdown_complete_callback *shutdown_callback = client->shutdown_callback;
    void *shutdown_user_data = client->shutdown_callback_user_data;

    aws_mem_release(client->allocator, client);
    client = NULL;

    shutdown_callback(shutdown_user_data);
}

static void s_s3_client_client_bootstrap_shutdown_callback(void *user_data) {
    s_s3_client_dec_shutdown_wait_count(user_data);
}

static void s_s3_client_credentials_provider_shutdown_callback(void *user_data) {
    s_s3_client_dec_shutdown_wait_count(user_data);
}

static void s_s3_client_connection_manager_shutdown_callback(void *user_data) {
    s_s3_client_dec_shutdown_wait_count(user_data);
}

static int s_s3_client_setup_endpoint(struct aws_s3_client *client) {
    struct aws_byte_cursor endpoint_url_part0 = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL(".s3.");
    struct aws_byte_cursor endpoint_url_part1 = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL(".amazonaws.com");
    size_t endpoint_buffer_len =
        (client->bucket_name->len) + endpoint_url_part0.len + (client->region->len) + endpoint_url_part1.len + 1;
    char *endpoint_buffer = aws_mem_acquire(client->allocator, endpoint_buffer_len);

    if (endpoint_buffer == NULL) {
        return AWS_OP_ERR;
    }

    endpoint_buffer[0] = '\0';

    strncat(endpoint_buffer, aws_string_c_str(client->bucket_name), client->bucket_name->len);
    strncat(endpoint_buffer, (const char *)endpoint_url_part0.ptr, endpoint_url_part0.len);
    strncat(endpoint_buffer, aws_string_c_str(client->region), client->region->len);
    strncat(endpoint_buffer, (const char *)endpoint_url_part1.ptr, endpoint_url_part1.len);

    client->endpoint = aws_string_new_from_c_str(client->allocator, endpoint_buffer);

    int result = AWS_OP_SUCCESS;

    if (client->endpoint == NULL) {
        result = AWS_OP_ERR;
    }

    aws_mem_release(client->allocator, endpoint_buffer);

    return result;
}

int s_s3_client_add_required_headers(struct aws_s3_client *client, struct aws_http_message *message) {

    struct aws_http_header host_header = {.name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("host"),
                                          .value = aws_byte_cursor_from_string(client->endpoint)};

    return aws_http_message_add_header(message, host_header);
}

int aws_s3_client_execute_request(struct aws_s3_client *client, struct aws_s3_request *request) {

    /* Setup request result */
    struct aws_s3_request_result *result = aws_s3_request_result_new(request, client->allocator);

    if (result == NULL) {
        return AWS_OP_ERR;
    }

    /* Setup request context */
    struct aws_s3_request_context *context = aws_s3_request_context_new(client->allocator, client, request, result);

    aws_s3_request_result_release(result);

    if (context == NULL) {
        goto error_clean_up;
    }

    /* Setup HTTP Request Message */
    {
        context->message = aws_http_message_new_request(client->allocator);

        if (context->message == NULL) {
            goto error_clean_up;
        }

        if (s_s3_client_add_required_headers(client, context->message)) {
            goto error_clean_up;
        }

        if (aws_s3_request_build_http_request(request, context, context->message)) {
            goto error_clean_up;
        }
    }

    /* Setup signing of request */
    {
        context->signable = aws_signable_new_http_request(client->allocator, context->message);

        if (context->signable == NULL) {
            goto error_clean_up;
        }

        struct aws_date_time now;
        aws_date_time_init_now(&now);

        struct aws_signing_config_aws signing_config = {
            .config_type = AWS_SIGNING_CONFIG_AWS,
            .algorithm = AWS_SIGNING_ALGORITHM_V4,
            .credentials_provider = client->credentials_provider,
            .region = aws_byte_cursor_from_array(client->region->bytes, client->region->len),
            .service = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("s3"),
            .date = now,
            .signed_body_value = AWS_SBVT_UNSIGNED_PAYLOAD,
            .signed_body_header = AWS_SBHT_X_AMZ_CONTENT_SHA256};

        if (aws_sign_request_aws(
                client->allocator,
                context->signable,
                (struct aws_signing_config_base *)&signing_config,
                s_s3_client_signing_complete,
                context)) {
            goto error_clean_up;
        }
    }

    return AWS_OP_SUCCESS;

error_clean_up:

    if (context != NULL) {
        aws_s3_request_context_destroy(context);
        context = NULL;
    }

    return AWS_OP_ERR;
}

static void s_s3_client_signing_complete(struct aws_signing_result *result, int error_code, void *user_data) {
    struct aws_s3_request_context *context = (struct aws_s3_request_context *)user_data;
    struct aws_s3_request *request = aws_s3_request_context_get_request(context);

    if (error_code != AWS_OP_SUCCESS) {
        goto error_clean_up;
    }

    struct aws_s3_client *client = aws_s3_request_context_get_client(context);

    if (aws_apply_signing_result_to_http_request(context->message, client->allocator, result)) {
        error_code = aws_last_error();
        goto error_clean_up;
    }

    aws_http_connection_manager_acquire_connection(
        client->connection_manager, s_s3_client_on_acquire_connection, context);

    return;

error_clean_up:
    aws_s3_request_finish(request, context, error_code);

    aws_s3_request_context_destroy(context);
    context = NULL;
}

static void s_s3_client_on_acquire_connection(struct aws_http_connection *connection, int error_code, void *user_data) {

    struct aws_s3_request_context *context = (struct aws_s3_request_context *)user_data;
    struct aws_s3_request *request = aws_s3_request_context_get_request(context);

    if (error_code != AWS_ERROR_SUCCESS) {
        goto error_clean_up;
    }

    struct aws_http_make_request_options options;
    AWS_ZERO_STRUCT(options);
    options.self_size = sizeof(struct aws_http_make_request_options);
    options.request = context->message;
    options.user_data = context;
    options.on_response_headers = s_s3_client_incoming_headers;
    options.on_response_header_block_done = s_s3_client_incoming_header_block_done;
    options.on_response_body = s_s3_client_incoming_body;
    options.on_complete = s_s3_client_stream_complete;

    context->stream = aws_http_connection_make_request(connection, &options);

    if (context->stream == NULL) {
        error_code = aws_last_error();
        goto error_clean_up;
    }

    if (aws_http_stream_activate(context->stream) != AWS_OP_SUCCESS) {
        error_code = aws_last_error();
        goto error_clean_up;
    }

    return;

error_clean_up:
    aws_s3_request_finish(request, context, error_code);

    aws_s3_request_context_destroy(context);
    context = NULL;
}

static int s_s3_client_incoming_headers(
    struct aws_http_stream *stream,
    enum aws_http_header_block header_block,
    const struct aws_http_header *headers,
    size_t num_headers,
    void *user_data) {

    (void)stream;
    struct aws_s3_request_context *context = (struct aws_s3_request_context *)user_data;
    struct aws_s3_request *request = aws_s3_request_context_get_request(context);

    return aws_s3_request_incoming_headers(request, context, header_block, headers, num_headers);
}

static int s_s3_client_incoming_header_block_done(
    struct aws_http_stream *stream,
    enum aws_http_header_block header_block,
    void *user_data) {

    (void)stream;

    struct aws_s3_request_context *context = (struct aws_s3_request_context *)user_data;
    struct aws_s3_request *request = aws_s3_request_context_get_request(context);

    return aws_s3_request_incoming_header_block_done(request, context, header_block);
}

static int s_s3_client_incoming_body(
    struct aws_http_stream *stream,
    const struct aws_byte_cursor *data,
    void *user_data) {

    (void)stream;

    struct aws_s3_request_context *context = (struct aws_s3_request_context *)user_data;
    struct aws_s3_request *request = aws_s3_request_context_get_request(context);

    return aws_s3_request_incoming_body(request, context, data);
}

static void s_s3_client_stream_complete(struct aws_http_stream *stream, int error_code, void *user_data) {
    (void)stream;

    struct aws_s3_request_context *context = (struct aws_s3_request_context *)user_data;
    struct aws_s3_request *request = aws_s3_request_context_get_request(context);

    aws_s3_request_stream_complete(request, context, error_code);

    aws_s3_request_context_destroy(context);
    context = NULL;
}
