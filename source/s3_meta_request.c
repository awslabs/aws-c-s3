/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_client_impl.h"
#include "aws/s3/private/s3_meta_request_impl.h"
#include "aws/s3/private/s3_util.h"
#include <aws/auth/signable.h>
#include <aws/auth/signing.h>
#include <aws/auth/signing_config.h>
#include <aws/auth/signing_result.h>
#include <aws/common/string.h>
#include <aws/io/event_loop.h>
#include <aws/io/retry_strategy.h>
#include <aws/io/stream.h>
#include <inttypes.h>

static const size_t s_dynamic_body_initial_buf_size = KB_TO_BYTES(1);
static const size_t s_default_body_streaming_priority_queue_size = 16;

static int s_s3_request_priority_queue_pred(const void *a, const void *b);
static void s_s3_request_destroy(void *user_data);

static void s_s3_meta_request_destroy(void *user_data);

static void s_s3_meta_request_send_request(struct aws_s3_client *client, struct aws_s3_vip_connection *vip_connection);

static void s_s3_meta_request_init_signing_date_time(
    struct aws_s3_meta_request *meta_request,
    struct aws_date_time *date_time);

static int s_s3_meta_request_sign_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_vip_connection *vip_connection);

static void s_s3_meta_request_request_on_signed(
    struct aws_signing_result *signing_result,
    int error_code,
    void *user_data);

static int s_s3_meta_request_headers_block_done(
    struct aws_http_stream *stream,
    enum aws_http_header_block header_block,
    void *user_data);

static int s_s3_meta_request_incoming_body(
    struct aws_http_stream *stream,
    const struct aws_byte_cursor *data,
    void *user_data);

static int s_s3_meta_request_incoming_headers(
    struct aws_http_stream *stream,
    enum aws_http_header_block header_block,
    const struct aws_http_header *headers,
    size_t headers_count,
    void *user_data);

static void s_s3_meta_request_stream_complete(struct aws_http_stream *stream, int error_code, void *user_data);

static void s_s3_meta_request_send_request_finish(
    struct aws_s3_vip_connection *vip_connection,
    struct aws_http_stream *stream,
    int error_code);

static void s_s3_meta_request_notify_request_destroyed(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request);

void aws_s3_meta_request_lock_synced_data(struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);

    aws_mutex_lock(&meta_request->synced_data.lock);
}

void aws_s3_meta_request_unlock_synced_data(struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);

    aws_mutex_unlock(&meta_request->synced_data.lock);
}

struct aws_s3_client *aws_s3_meta_request_acquire_client(struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);

    struct aws_s3_client *client = NULL;

    aws_s3_meta_request_lock_synced_data(meta_request);

    client = meta_request->synced_data.client;

    if (client != NULL) {
        aws_s3_client_acquire(client);
    } else {
        AWS_LOGF_DEBUG(
            AWS_LS_S3_META_REQUEST,
            "id=%p Meta request trying to get reference to client but client is null.",
            (void *)meta_request);
    }

    aws_s3_meta_request_unlock_synced_data(meta_request);

    return client;
}

void aws_s3_meta_request_push_to_client(struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);

    struct aws_s3_client *client = aws_s3_meta_request_acquire_client(meta_request);

    if (client != NULL) {
        aws_s3_client_push_meta_request(client, meta_request);
    } else {
        AWS_LOGF_DEBUG(
            AWS_LS_S3_META_REQUEST,
            "id=%p Meta request trying to schedule work but client is null.",
            (void *)meta_request);
    }

    aws_s3_client_release(client);
}

void aws_s3_meta_request_remove_from_client(struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);

    struct aws_s3_client *client = aws_s3_meta_request_acquire_client(meta_request);

    if (client != NULL) {
        aws_s3_client_remove_meta_request(client, meta_request);
    } else {
        AWS_LOGF_DEBUG(
            AWS_LS_S3_META_REQUEST,
            "id=%p Meta request trying to schedule work but client is null.",
            (void *)meta_request);
    }

    aws_s3_client_release(client);
}

int aws_s3_meta_request_init_base(
    struct aws_allocator *allocator,
    struct aws_s3_client *client,
    size_t part_size,
    const struct aws_s3_meta_request_options *options,
    void *impl,
    struct aws_s3_meta_request_vtable *vtable,
    struct aws_s3_meta_request *meta_request) {

    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(options);
    AWS_PRECONDITION(options->message);
    AWS_PRECONDITION(impl);
    AWS_PRECONDITION(meta_request);

    AWS_ZERO_STRUCT(*meta_request);
    meta_request->impl = impl;
    meta_request->vtable = vtable;

    AWS_ASSERT(vtable->next_request);
    AWS_ASSERT(vtable->prepare_request);
    AWS_ASSERT(vtable->destroy);
    AWS_ASSERT(vtable->sign_request);
    AWS_ASSERT(vtable->init_signing_date_time);
    AWS_ASSERT(vtable->send_request_finish);

    meta_request->allocator = allocator;

    /* Set up reference count. */
    aws_ref_count_init(&meta_request->ref_count, meta_request, s_s3_meta_request_destroy);

    *((size_t *)&meta_request->part_size) = part_size;

    if (options->signing_config) {
        meta_request->cached_signing_config = aws_cached_signing_config_new(allocator, options->signing_config);
    }

    /* Keep a reference to the original message structure passed in. */
    meta_request->initial_request_message = options->message;
    aws_http_message_acquire(options->message);

    /* Store a copy of the original message's initial body stream in our synced data, so that concurrent requests can
     * safely take turns reading from it when needed. */
    meta_request->synced_data.initial_body_stream = aws_http_message_get_body_stream(options->message);

    if (aws_mutex_init(&meta_request->synced_data.lock)) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST, "id=%p Could not initialize mutex for meta request", (void *)meta_request);
        return AWS_OP_ERR;
    }

    aws_priority_queue_init_dynamic(
        &meta_request->synced_data.pending_body_streaming_requests,
        meta_request->allocator,
        s_default_body_streaming_priority_queue_size,
        sizeof(struct aws_s3_request *),
        s_s3_request_priority_queue_pred);

    /* Client is currently optional to allow spining up a meta_request without a client in a test. */
    if (client != NULL) {
        aws_s3_client_acquire(client);
        meta_request->synced_data.client = client;
    }

    meta_request->synced_data.next_streaming_part = 1;

    meta_request->user_data = options->user_data;
    meta_request->headers_callback = options->headers_callback;
    meta_request->body_callback = options->body_callback;
    meta_request->finish_callback = options->finish_callback;
    meta_request->shutdown_callback = options->shutdown_callback;

    return AWS_OP_SUCCESS;
}

void aws_s3_meta_request_cancel(struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);

    aws_s3_meta_request_finish(meta_request, NULL, 0, AWS_ERROR_S3_CANCELED);
}

void aws_s3_meta_request_acquire(struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);

    aws_ref_count_acquire(&meta_request->ref_count);
}

void aws_s3_meta_request_release(struct aws_s3_meta_request *meta_request) {
    if (meta_request == NULL) {
        return;
    }

    aws_ref_count_release(&meta_request->ref_count);
}

void aws_s3_default_signing_config(
    struct aws_signing_config_aws *signing_config,
    const struct aws_byte_cursor region,
    struct aws_credentials_provider *credentials_provider) {
    AWS_PRECONDITION(signing_config);
    AWS_PRECONDITION(credentials_provider);

    AWS_ZERO_STRUCT(*signing_config);

    signing_config->config_type = AWS_SIGNING_CONFIG_AWS;
    signing_config->algorithm = AWS_SIGNING_ALGORITHM_V4;
    signing_config->credentials_provider = credentials_provider;
    signing_config->region = region;
    signing_config->service = aws_byte_cursor_from_c_str("s3");
    signing_config->signed_body_header = AWS_SBHT_X_AMZ_CONTENT_SHA256;
    signing_config->signed_body_value = g_aws_signed_body_value_unsigned_payload;
}

static void s_s3_meta_request_destroy(void *user_data) {
    struct aws_s3_meta_request *meta_request = user_data;
    AWS_PRECONDITION(meta_request);

    /* Clean up our initial http message */
    if (meta_request->initial_request_message != NULL) {
        aws_http_message_release(meta_request->initial_request_message);
        meta_request->initial_request_message = NULL;
    }

    void *meta_request_user_data = meta_request->user_data;
    aws_s3_meta_request_shutdown_fn *shutdown_callback = meta_request->shutdown_callback;

    aws_cached_signing_config_destroy(meta_request->cached_signing_config);
    aws_mutex_clean_up(&meta_request->synced_data.lock);
    aws_s3_client_release(meta_request->synced_data.client);

    AWS_ASSERT(aws_priority_queue_size(&meta_request->synced_data.pending_body_streaming_requests) == 0);
    aws_priority_queue_clean_up(&meta_request->synced_data.pending_body_streaming_requests);

    meta_request->vtable->destroy(meta_request);

    if (shutdown_callback != NULL) {
        shutdown_callback(meta_request_user_data);
    }
}

struct aws_s3_request *aws_s3_request_new(
    struct aws_s3_meta_request *meta_request,
    int request_tag,
    uint32_t part_number,
    uint32_t flags) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(meta_request->allocator);

    struct aws_s3_request *request = aws_mem_calloc(meta_request->allocator, 1, sizeof(struct aws_s3_request));

    aws_ref_count_init(&request->ref_count, request, (aws_simple_completion_callback *)s_s3_request_destroy);

    request->allocator = meta_request->allocator;
    request->meta_request = meta_request;
    aws_s3_meta_request_acquire(meta_request);

    request->request_tag = request_tag;
    request->part_number = part_number;
    request->record_response_headers = (flags & AWS_S3_REQUEST_DESC_RECORD_RESPONSE_HEADERS) != 0;
    request->stream_response_body = (flags & AWS_S3_REQUEST_DESC_STREAM_RESPONSE_BODY) != 0;
    request->part_size_response_body = (flags & AWS_S3_REQUEST_DESC_PART_SIZE_RESPONSE_BODY) != 0;

    return request;
}

void aws_s3_request_setup_send_data(struct aws_s3_request *request, struct aws_http_message *message) {
    AWS_PRECONDITION(request);
    AWS_PRECONDITION(message);

    aws_s3_request_clean_up_send_data(request);

    request->send_data.message = message;
    aws_http_message_acquire(message);
}

void s_s3_request_clean_up_send_data_message(struct aws_s3_request *request) {
    AWS_PRECONDITION(request);

    struct aws_http_message *message = request->send_data.message;

    if (message == NULL) {
        return;
    }

    request->send_data.message = NULL;

    struct aws_input_stream *input_stream = aws_http_message_get_body_stream(message);
    aws_input_stream_destroy(input_stream);
    input_stream = NULL;

    aws_http_message_set_body_stream(message, NULL);
    aws_http_message_release(message);
}

void aws_s3_request_clean_up_send_data(struct aws_s3_request *request) {
    AWS_PRECONDITION(request);

    s_s3_request_clean_up_send_data_message(request);

    aws_signable_destroy(request->send_data.signable);
    request->send_data.signable = NULL;

    aws_http_headers_release(request->send_data.response_headers);
    request->send_data.response_headers = NULL;

    aws_byte_buf_clean_up(&request->send_data.response_body);

    AWS_ZERO_STRUCT(request->send_data);
}

void aws_s3_request_acquire(struct aws_s3_request *request) {
    AWS_PRECONDITION(request);

    aws_ref_count_acquire(&request->ref_count);
}

void aws_s3_request_release(struct aws_s3_request *request) {
    if (request == NULL) {
        return;
    }

    aws_ref_count_release(&request->ref_count);
}

static int s_s3_request_priority_queue_pred(const void *a, const void *b) {
    const struct aws_s3_request **request_a = (const struct aws_s3_request **)a;
    AWS_PRECONDITION(request_a);
    AWS_PRECONDITION(*request_a);

    const struct aws_s3_request **request_b = (const struct aws_s3_request **)b;
    AWS_PRECONDITION(request_b);
    AWS_PRECONDITION(*request_b);

    return (*request_a)->part_number > (*request_b)->part_number;
}

static void s_s3_request_destroy(void *user_data) {
    struct aws_s3_request *request = user_data;

    if (request == NULL) {
        return;
    }

    struct aws_s3_meta_request *meta_request = request->meta_request;

    if (meta_request != NULL) {
        struct aws_s3_client *client = aws_s3_meta_request_acquire_client(meta_request);

        if (client != NULL) {
            aws_s3_client_notify_request_destroyed(client);
            aws_s3_client_release(client);
            client = NULL;
        }

        s_s3_meta_request_notify_request_destroyed(meta_request, request);
    }

    aws_s3_request_clean_up_send_data(request);
    aws_byte_buf_clean_up(&request->request_body);
    aws_mem_release(request->allocator, request);
    aws_s3_meta_request_release(meta_request);
}

struct aws_s3_request *aws_s3_meta_request_next_request(struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);

    struct aws_s3_meta_request_vtable *vtable = meta_request->vtable;
    AWS_FATAL_ASSERT(vtable);

    if (aws_s3_meta_request_is_finished(meta_request)) {
        return NULL;
    }

    struct aws_s3_request *request = NULL;

    if (vtable->next_request(meta_request, &request)) {
        aws_s3_meta_request_finish(meta_request, NULL, 0, aws_last_error_or_unknown());
        return NULL;
    }

    return request;
}

bool aws_s3_meta_request_is_finished(struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);

    aws_s3_meta_request_lock_synced_data(meta_request);
    bool is_finished = meta_request->synced_data.state == AWS_S3_META_REQUEST_STATE_FINISHED;
    aws_s3_meta_request_unlock_synced_data(meta_request);

    return is_finished;
}

bool aws_s3_meta_request_check_active(struct aws_s3_meta_request *meta_request) {
    aws_s3_meta_request_lock_synced_data(meta_request);
    bool active = meta_request->synced_data.state == AWS_S3_META_REQUEST_STATE_ACTIVE;
    aws_s3_meta_request_unlock_synced_data(meta_request);
    return active;
}

int aws_s3_meta_request_make_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_client *client,
    struct aws_s3_vip_connection *vip_connection) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(vip_connection);

    struct aws_s3_request *request = vip_connection->request;
    AWS_PRECONDITION(request);

    struct aws_s3_meta_request_vtable *vtable = meta_request->vtable;
    AWS_PRECONDITION(vtable);

    if (vtable->prepare_request(meta_request, client, vip_connection, !vip_connection->is_retry)) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST, "id=%p Could not prepare request %p", (void *)meta_request, (void *)request);

        goto call_finished_callback;
    }

    /* Sign the newly created message. */
    if (s_s3_meta_request_sign_request(meta_request, vip_connection)) {

        goto call_finished_callback;
    }

    return AWS_OP_SUCCESS;

call_finished_callback:

    s_s3_meta_request_send_request_finish(vip_connection, NULL, aws_last_error_or_unknown());

    return AWS_OP_ERR;
}

static void s_s3_meta_request_init_signing_date_time(
    struct aws_s3_meta_request *meta_request,
    struct aws_date_time *date_time) {
    AWS_PRECONDITION(meta_request);

    struct aws_s3_meta_request_vtable *vtable = meta_request->vtable;
    AWS_PRECONDITION(vtable);
    AWS_PRECONDITION(vtable->init_signing_date_time);

    vtable->init_signing_date_time(meta_request, date_time);
}

void aws_s3_meta_request_init_signing_date_time_default(
    struct aws_s3_meta_request *meta_request,
    struct aws_date_time *date_time) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(date_time);
    (void)meta_request;

    aws_date_time_init_now(date_time);
}

static int s_s3_meta_request_sign_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_vip_connection *vip_connection) {
    AWS_PRECONDITION(meta_request);

    struct aws_s3_meta_request_vtable *vtable = meta_request->vtable;
    AWS_PRECONDITION(vtable);
    AWS_PRECONDITION(vtable->send_request_finish);

    return vtable->sign_request(meta_request, vip_connection);
}

/* Handles signing a message for the caller. */
int aws_s3_meta_request_sign_request_default(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_vip_connection *vip_connection) {
    AWS_PRECONDITION(meta_request)

    AWS_PRECONDITION(vip_connection);
    AWS_PRECONDITION(vip_connection->owning_vip);

    struct aws_s3_client *client = vip_connection->owning_vip->owning_client;
    AWS_PRECONDITION(client);

    struct aws_s3_request *request = vip_connection->request;
    AWS_PRECONDITION(request);

    struct aws_signing_config_aws signing_config;

    if (meta_request->cached_signing_config != NULL) {
        signing_config = meta_request->cached_signing_config->config;
    } else if (client->cached_signing_config != NULL) {
        signing_config = client->cached_signing_config->config;
    } else {
        AWS_LOGF_DEBUG(
            AWS_LS_S3_META_REQUEST,
            "id=%p: No signing config present. Not signing request %p.",
            (void *)meta_request,
            (void *)request);

        s_s3_meta_request_request_on_signed(NULL, AWS_ERROR_SUCCESS, vip_connection);
        return AWS_OP_SUCCESS;
    }

    s_s3_meta_request_init_signing_date_time(meta_request, &signing_config.date);

    int result = AWS_OP_ERR;
    request->send_data.signable = aws_signable_new_http_request(meta_request->allocator, request->send_data.message);

    AWS_LOGF_DEBUG(
        AWS_LS_S3_META_REQUEST,
        "id=%p Created signable %p for request %p with message %p",
        (void *)meta_request,
        (void *)request->send_data.signable,
        (void *)request,
        (void *)request->send_data.message);

    if (request->send_data.signable == NULL) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST,
            "id=%p: Could not allocate signable for request %p",
            (void *)meta_request,
            (void *)request);

        goto done;
    }

    if (aws_sign_request_aws(
            meta_request->allocator,
            request->send_data.signable,
            (struct aws_signing_config_base *)&signing_config,
            s_s3_meta_request_request_on_signed,
            vip_connection)) {

        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST, "id=%p: Could not sign request %p", (void *)meta_request, (void *)request);

        goto done;
    }

    result = AWS_OP_SUCCESS;

done:
    return result;
}

/* Handle the signing result, getting an HTTP connection for the request if signing succeeded. */
static void s_s3_meta_request_request_on_signed(
    struct aws_signing_result *signing_result,
    int error_code,
    void *user_data) {

    struct aws_s3_vip_connection *vip_connection = user_data;
    AWS_PRECONDITION(vip_connection);

    struct aws_s3_client *client = vip_connection->owning_vip->owning_client;
    AWS_PRECONDITION(client);

    struct aws_s3_request *request = vip_connection->request;
    AWS_PRECONDITION(request);

    struct aws_s3_meta_request *meta_request = request->meta_request;
    AWS_PRECONDITION(meta_request);

    if (error_code != AWS_ERROR_SUCCESS) {
        goto error_finish;
    }

    if (signing_result != NULL &&
        aws_apply_signing_result_to_http_request(request->send_data.message, meta_request->allocator, signing_result)) {
        goto error_finish;
    }

    AWS_LOGF_DEBUG(
        AWS_LS_S3_META_REQUEST, "id=%p Getting HTTP connection for request %p", (void *)meta_request, (void *)request);

    s_s3_meta_request_send_request(client, vip_connection);

    return;

error_finish:

    s_s3_meta_request_send_request_finish(vip_connection, NULL, aws_last_error_or_unknown());
}

static void s_s3_meta_request_send_request(struct aws_s3_client *client, struct aws_s3_vip_connection *vip_connection) {
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(vip_connection);
    AWS_PRECONDITION(vip_connection->http_connection);
    (void)client;

    struct aws_s3_request *request = vip_connection->request;
    AWS_PRECONDITION(request);

    struct aws_s3_meta_request *meta_request = request->meta_request;
    AWS_PRECONDITION(meta_request);

    /* Now that we have a signed request and a connection, go ahead and issue the request. */
    struct aws_http_make_request_options options;
    AWS_ZERO_STRUCT(options);

    options.self_size = sizeof(struct aws_http_make_request_options);
    options.request = request->send_data.message;
    options.user_data = vip_connection;
    options.on_response_headers = s_s3_meta_request_incoming_headers;
    options.on_response_header_block_done = s_s3_meta_request_headers_block_done;
    options.on_response_body = s_s3_meta_request_incoming_body;
    options.on_complete = s_s3_meta_request_stream_complete;

    struct aws_http_stream *stream = aws_http_connection_make_request(vip_connection->http_connection, &options);

    if (stream == NULL) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST, "id=%p: Could not make HTTP request %p", (void *)meta_request, (void *)request);

        goto error_finish;
    }

    AWS_LOGF_DEBUG(AWS_LS_S3_META_REQUEST, "id=%p: Sending request %p", (void *)meta_request, (void *)request);

    if (aws_http_stream_activate(stream) != AWS_OP_SUCCESS) {
        aws_http_stream_release(stream);
        stream = NULL;

        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST, "id=%p: Could not activate HTTP stream %p", (void *)meta_request, (void *)request);

        goto error_finish;
    }

    return;

error_finish:

    s_s3_meta_request_send_request_finish(vip_connection, NULL, aws_last_error_or_unknown());
}

static int s_s3_meta_request_error_code_from_response_status(int response_status) {
    int error_code = AWS_ERROR_UNKNOWN;

    switch (response_status) {
        case AWS_S3_RESPONSE_STATUS_SUCCESS:
        case AWS_S3_RESPONSE_STATUS_RANGE_SUCCESS:
        case AWS_S3_RESPONSE_STATUS_NO_CONTENT_SUCCESS:
            error_code = AWS_ERROR_SUCCESS;
            break;
        case AWS_S3_RESPONSE_STATUS_INTERNAL_ERROR:
            error_code = AWS_ERROR_S3_INTERNAL_ERROR;
            break;
        case AWS_S3_RESPONSE_STATUS_SLOW_DOWN:
            error_code = AWS_ERROR_S3_SLOW_DOWN;
            break;
        default:
            error_code = AWS_ERROR_S3_INVALID_RESPONSE_STATUS;
            break;
    }

    return error_code;
}

static int s_s3_meta_request_incoming_headers(
    struct aws_http_stream *stream,
    enum aws_http_header_block header_block,
    const struct aws_http_header *headers,
    size_t headers_count,
    void *user_data) {

    (void)header_block;

    AWS_PRECONDITION(stream);

    struct aws_s3_vip_connection *vip_connection = user_data;
    AWS_PRECONDITION(vip_connection);

    struct aws_s3_request *request = vip_connection->request;
    AWS_PRECONDITION(request);

    struct aws_s3_meta_request *meta_request = request->meta_request;
    AWS_PRECONDITION(meta_request);

    AWS_LOGF_DEBUG(
        AWS_LS_S3_META_REQUEST,
        "id=%p Incoming headers for request %p. VIP connection: %p.",
        (void *)meta_request,
        (void *)request,
        (void *)vip_connection);

    if (aws_http_stream_get_incoming_response_status(stream, &request->send_data.response_status)) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST,
            "id=%p Could not get incoming response status for request %p",
            (void *)meta_request,
            (void *)request);
    }

    bool successful_response =
        s_s3_meta_request_error_code_from_response_status(request->send_data.response_status) == AWS_ERROR_SUCCESS;

    /* Only record headers if an error has taken place, or if the reqest_desc has asked for them. */
    bool should_record_headers = !successful_response || request->record_response_headers;

    if (should_record_headers) {
        if (request->send_data.response_headers == NULL) {
            request->send_data.response_headers = aws_http_headers_new(meta_request->allocator);
        }

        for (size_t i = 0; i < headers_count; ++i) {
            const struct aws_byte_cursor *name = &headers[i].name;
            const struct aws_byte_cursor *value = &headers[i].value;

            aws_http_headers_add(request->send_data.response_headers, *name, *value);
        }
    }

    /* Failed requests are handled inside of the meta request base type, so only pass through to virtual function on
     * success. */
    if (successful_response && meta_request->vtable->incoming_headers) {
        return meta_request->vtable->incoming_headers(stream, header_block, headers, headers_count, vip_connection);
    }

    return AWS_OP_SUCCESS;
}

static int s_s3_meta_request_headers_block_done(
    struct aws_http_stream *stream,
    enum aws_http_header_block header_block,
    void *user_data) {

    struct aws_s3_vip_connection *vip_connection = user_data;
    AWS_PRECONDITION(vip_connection);

    struct aws_s3_request *request = vip_connection->request;
    AWS_PRECONDITION(request);

    struct aws_s3_meta_request *meta_request = request->meta_request;
    AWS_PRECONDITION(meta_request && meta_request->vtable);

    AWS_LOGF_DEBUG(
        AWS_LS_S3_META_REQUEST,
        "id=%p Header block done for request %p. Response status: %d. VIP connection: %p.",
        (void *)meta_request,
        (void *)request,
        request->send_data.response_status,
        (void *)vip_connection);

    bool successful_response =
        s_s3_meta_request_error_code_from_response_status(request->send_data.response_status) == AWS_ERROR_SUCCESS;

    /* Failed requests are handled inside of the meta request base type, so only pass through to virtual function on
     * success. */
    if (successful_response) {
        if (meta_request->vtable->incoming_headers_block_done) {
            return meta_request->vtable->incoming_headers_block_done(stream, header_block, vip_connection);
        }
    }

    return AWS_OP_SUCCESS;
}

static int s_s3_meta_request_incoming_body(
    struct aws_http_stream *stream,
    const struct aws_byte_cursor *data,
    void *user_data) {

    struct aws_s3_vip_connection *vip_connection = user_data;
    AWS_PRECONDITION(vip_connection);

    struct aws_s3_request *request = vip_connection->request;
    AWS_PRECONDITION(request);

    struct aws_s3_meta_request *meta_request = request->meta_request;
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(meta_request->vtable);

    AWS_LOGF_DEBUG(
        AWS_LS_S3_META_REQUEST,
        "id=%p Incoming body for request %p. Response status: %d. Data Size: %" PRIu64 ". VIP connection: %p.",
        (void *)meta_request,
        (void *)request,
        request->send_data.response_status,
        (uint64_t)data->len,
        (void *)vip_connection);

    bool successful_response =
        s_s3_meta_request_error_code_from_response_status(request->send_data.response_status) == AWS_ERROR_SUCCESS;

    if (request->send_data.response_body.capacity == 0) {
        size_t buffer_size = s_dynamic_body_initial_buf_size;

        if (request->part_size_response_body) {
            buffer_size = meta_request->part_size;
        }

        aws_byte_buf_init(&request->send_data.response_body, meta_request->allocator, buffer_size);
    }

    if (aws_byte_buf_append_dynamic(&request->send_data.response_body, data)) {

        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST,
            "id=%p: Request %p could not append to response body due to error %d (%s)",
            (void *)meta_request,
            (void *)request,
            aws_last_error_or_unknown(),
            aws_error_str(aws_last_error_or_unknown()));

        return AWS_OP_ERR;
    }

    /* Failed requests are handled inside of the meta request base type, so only pass through to virtual function on
     * success. */
    if (successful_response) {
        if (meta_request->vtable->incoming_body) {
            return meta_request->vtable->incoming_body(stream, data, vip_connection);
        }
    }

    return AWS_OP_SUCCESS;
}

/* Finish up the processing of the request work. */
static void s_s3_meta_request_stream_complete(struct aws_http_stream *stream, int error_code, void *user_data) {

    struct aws_s3_vip_connection *vip_connection = user_data;
    AWS_PRECONDITION(vip_connection);

    s_s3_meta_request_send_request_finish(vip_connection, stream, error_code);
}

static void s_s3_meta_request_send_request_finish(
    struct aws_s3_vip_connection *vip_connection,
    struct aws_http_stream *stream,
    int error_code) {
    AWS_PRECONDITION(vip_connection);

    struct aws_s3_request *request = vip_connection->request;
    AWS_PRECONDITION(request);

    struct aws_s3_meta_request *meta_request = request->meta_request;
    AWS_PRECONDITION(meta_request);

    struct aws_s3_meta_request_vtable *vtable = meta_request->vtable;
    AWS_PRECONDITION(vtable);

    vtable->send_request_finish(vip_connection, stream, error_code);
}

void aws_s3_meta_request_finish(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *failed_request,
    int response_status,
    int error_code) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(meta_request->vtable);
    AWS_PRECONDITION(meta_request->vtable->finish);

    meta_request->vtable->finish(meta_request, failed_request, response_status, error_code);
}

void aws_s3_meta_request_send_request_finish_default(
    struct aws_s3_vip_connection *vip_connection,
    struct aws_http_stream *stream,
    int error_code) {
    AWS_PRECONDITION(vip_connection);
    AWS_PRECONDITION(vip_connection->owning_vip);

    struct aws_s3_client *client = vip_connection->owning_vip->owning_client;
    AWS_PRECONDITION(client);

    struct aws_s3_request *request = vip_connection->request;
    AWS_PRECONDITION(request);

    struct aws_s3_meta_request *meta_request = request->meta_request;
    AWS_PRECONDITION(meta_request);

    struct aws_s3_meta_request_vtable *vtable = meta_request->vtable;
    AWS_PRECONDITION(vtable);

    int response_status = request->send_data.response_status;

    /* If our error code is currently success, then we have some other calls to make that could still indicate a
     * failure. */
    if (error_code == AWS_ERROR_SUCCESS) {

        /* Check if the response code indicates an error occurred. */
        error_code = s_s3_meta_request_error_code_from_response_status(response_status);

        if (error_code == AWS_ERROR_SUCCESS) {
            /* Call the derived type, having it handle what to do with the current success. */
            if (vtable->stream_complete && vtable->stream_complete(stream, vip_connection)) {

                error_code = aws_last_error_or_unknown();
            }
        } else {
            aws_raise_error(error_code);
        }
    }

    AWS_LOGF_DEBUG(
        AWS_LS_S3_META_REQUEST,
        "id=%p: Request %p finished with error code %d and response status %d",
        (void *)meta_request,
        (void *)request,
        error_code,
        response_status);

    enum aws_s3_vip_connection_finish_code finish_code = AWS_S3_VIP_CONNECTION_FINISH_CODE_FAILED;

    /* If the request was entirely successful...*/
    if (error_code == AWS_ERROR_SUCCESS) {

        /* If this request body is meant to be streamed to the caller, set that up now. */
        if (request->stream_response_body) {
            AWS_ASSERT(request->part_number > 0);

            struct aws_linked_list streaming_requests;
            aws_linked_list_init(&streaming_requests);

            aws_s3_meta_request_lock_synced_data(meta_request);

            /* Push it into the priority queue. */
            aws_s3_meta_request_body_streaming_push_synced(meta_request, request);

            /* Grab the next request that can be streamed back to the caller. */
            struct aws_s3_request *next_streaming_request = aws_s3_meta_request_body_streaming_pop_synced(meta_request);

            /* Grab any additional requests that could be streamed to the caller. */
            while (next_streaming_request != NULL) {
                aws_linked_list_push_back(&streaming_requests, &next_streaming_request->node);
                next_streaming_request = aws_s3_meta_request_body_streaming_pop_synced(meta_request);
            }

            if (!aws_linked_list_empty(&streaming_requests)) {
                aws_s3_client_stream_response_body(client, meta_request, &streaming_requests);
            }

            aws_s3_meta_request_unlock_synced_data(meta_request);
        }

        finish_code = AWS_S3_VIP_CONNECTION_FINISH_CODE_SUCCESS;

    } else {
        /* If the request failed due to an invalid, ie, unrecoverable, response status, then finish the meta request
         * with that request as the failing request. */
        if (error_code == AWS_ERROR_S3_INVALID_RESPONSE_STATUS) {

            AWS_LOGF_ERROR(
                AWS_LS_S3_META_REQUEST,
                "id=%p Meta request cannot recover from error %d (%s). (request=%p, response status=%d)",
                (void *)meta_request,
                error_code,
                aws_error_str(error_code),
                (void *)request,
                response_status);

            aws_s3_meta_request_finish(meta_request, request, response_status, error_code);
        } else {
            /* Otherwise, set this up for a retry. */
            finish_code = AWS_S3_VIP_CONNECTION_FINISH_CODE_RETRY;
        }
    }

    if (stream != NULL) {
        aws_http_stream_release(stream);
        stream = NULL;
    }

    aws_s3_client_notify_connection_finished(client, vip_connection, error_code, finish_code);
}

static void s_s3_meta_request_notify_request_destroyed(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request) {
    AWS_PRECONDITION(meta_request);

    struct aws_s3_meta_request_vtable *vtable = meta_request->vtable;
    AWS_PRECONDITION(vtable);
    AWS_PRECONDITION(request);

    if (vtable->notify_request_destroyed) {
        vtable->notify_request_destroyed(meta_request, request);
    }
}

void aws_s3_meta_request_body_streaming_push_synced(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request) {
    ASSERT_SYNCED_DATA_LOCK_HELD(meta_request);
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(request);

    AWS_ASSERT(request->meta_request == meta_request);

    aws_s3_request_acquire(request);

    aws_priority_queue_push(&meta_request->synced_data.pending_body_streaming_requests, &request);
}

struct aws_s3_request *aws_s3_meta_request_body_streaming_pop_synced(struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);
    ASSERT_SYNCED_DATA_LOCK_HELD(meta_request);

    struct aws_s3_request **top_request = NULL;

    aws_priority_queue_top(&meta_request->synced_data.pending_body_streaming_requests, (void **)&top_request);

    if (top_request == NULL) {
        return NULL;
    }

    AWS_FATAL_ASSERT(*top_request);

    if ((*top_request)->part_number != meta_request->synced_data.next_streaming_part) {
        return NULL;
    }

    struct aws_s3_request *request = NULL;
    aws_priority_queue_pop(&meta_request->synced_data.pending_body_streaming_requests, (void **)&request);

    ++meta_request->synced_data.next_streaming_part;

    return request;
}

void aws_s3_meta_request_finish_default(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *failed_request,
    int response_status,
    int error_code) {
    AWS_PRECONDITION(meta_request);

    bool already_finished = false;
    struct aws_s3_client *client = NULL;
    struct aws_linked_list release_request_list;
    aws_linked_list_init(&release_request_list);

    aws_s3_meta_request_lock_synced_data(meta_request);

    if (meta_request->synced_data.state == AWS_S3_META_REQUEST_STATE_FINISHED) {
        already_finished = true;
        goto unlock;
    }

    meta_request->synced_data.state = AWS_S3_META_REQUEST_STATE_FINISHED;

    /* Get rid of our client reference. Release it outside of the lock. */
    client = meta_request->synced_data.client;
    meta_request->synced_data.client = NULL;

    /* Cleaning out the pending-stream-to-caller priority queue*/
    struct aws_s3_request *request = aws_s3_meta_request_body_streaming_pop_synced(meta_request);

    while (request != NULL) {
        aws_linked_list_push_back(&release_request_list, &request->node);
        request = aws_s3_meta_request_body_streaming_pop_synced(meta_request);
    }

unlock:
    aws_s3_meta_request_unlock_synced_data(meta_request);

    if (already_finished) {
        return;
    }

    while (!aws_linked_list_empty(&release_request_list)) {
        struct aws_linked_list_node *request_node = aws_linked_list_pop_front(&release_request_list);
        struct aws_s3_request *release_request = AWS_CONTAINER_OF(request_node, struct aws_s3_request, node);
        AWS_FATAL_ASSERT(release_request != NULL);
        aws_s3_request_release(release_request);
    }

    aws_s3_client_release(client);

    AWS_LOGF_DEBUG(
        AWS_LS_S3_META_REQUEST,
        "id=%p Meta request finished with error code %d (%s)",
        (void *)meta_request,
        error_code,
        aws_error_str(error_code));

    if (meta_request->finish_callback != NULL) {

        struct aws_s3_meta_request_result meta_request_result = {
            .error_response_headers = NULL,
            .error_response_body = NULL,
            .error_code = error_code,
            .response_status = response_status,
        };

        if (error_code == AWS_ERROR_S3_INVALID_RESPONSE_STATUS && failed_request != NULL) {
            meta_request_result.error_response_headers = failed_request->send_data.response_headers;
            meta_request_result.error_response_body = &failed_request->send_data.response_body;
        }

        meta_request->finish_callback(meta_request, &meta_request_result, meta_request->user_data);
    }
}

int aws_s3_meta_request_read_body(struct aws_s3_meta_request *meta_request, struct aws_byte_buf *buffer) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(buffer);

    aws_s3_meta_request_lock_synced_data(meta_request);

    int result = aws_s3_meta_request_read_body_synced(meta_request, buffer);

    aws_s3_meta_request_unlock_synced_data(meta_request);

    return result;
}

int aws_s3_meta_request_read_body_synced(struct aws_s3_meta_request *meta_request, struct aws_byte_buf *buffer) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(buffer);
    ASSERT_SYNCED_DATA_LOCK_HELD(meta_request);

    struct aws_input_stream *initial_body_stream = meta_request->synced_data.initial_body_stream;
    AWS_FATAL_ASSERT(initial_body_stream);

    /* Copy it into our buffer. */
    if (aws_input_stream_read(initial_body_stream, buffer)) {
        AWS_LOGF_ERROR(AWS_LS_S3_META_REQUEST, "id=%p Could not read from body stream.", (void *)meta_request);
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}
