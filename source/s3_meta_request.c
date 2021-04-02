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
#include <aws/common/system_info.h>
#include <aws/io/event_loop.h>
#include <aws/io/retry_strategy.h>
#include <aws/io/stream.h>
#include <inttypes.h>

static const size_t s_dynamic_body_initial_buf_size = KB_TO_BYTES(1);
static const size_t s_default_body_streaming_priority_queue_size = 16;

static int s_s3_request_priority_queue_pred(const void *a, const void *b);
static void s_s3_meta_request_destroy(void *user_data);

static void s_s3_meta_request_init_signing_date_time(
    struct aws_s3_meta_request *meta_request,
    struct aws_date_time *date_time);

static void s_s3_meta_request_sign_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    aws_signing_complete_fn *on_signing_complete,
    void *user_data);

static void s_s3_meta_request_request_on_signed(
    struct aws_signing_result *signing_result,
    int error_code,
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

void aws_s3_meta_request_lock_synced_data(struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);

    aws_mutex_lock(&meta_request->synced_data.lock);
}

void aws_s3_meta_request_unlock_synced_data(struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);

    aws_mutex_unlock(&meta_request->synced_data.lock);
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

    AWS_ASSERT(vtable->update);
    AWS_ASSERT(vtable->prepare_request);
    AWS_ASSERT(vtable->destroy);
    AWS_ASSERT(vtable->sign_request);
    AWS_ASSERT(vtable->init_signing_date_time);
    AWS_ASSERT(vtable->finished_request);
    AWS_ASSERT(vtable->send_request_finish);

    meta_request->allocator = allocator;
    meta_request->type = options->type;

    /* Set up reference count. */
    aws_ref_count_init(&meta_request->ref_count, meta_request, s_s3_meta_request_destroy);

    *((size_t *)&meta_request->part_size) = part_size;

    if (options->signing_config) {
        meta_request->cached_signing_config = aws_cached_signing_config_new(allocator, options->signing_config);
    }

    /* Keep a reference to the original message structure passed in. */
    meta_request->initial_request_message = options->message;
    aws_http_message_acquire(options->message);

    aws_s3_add_user_agent_header(meta_request->allocator, meta_request->initial_request_message);

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

    /* Client is currently optional to allow spinning up a meta_request without a client in a test. */
    if (client != NULL) {
        aws_s3_client_acquire(client);
        meta_request->client = client;
        meta_request->io_event_loop = aws_event_loop_group_get_next_loop(client->body_streaming_elg);
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
    aws_s3_meta_request_lock_synced_data(meta_request);
    aws_s3_meta_request_set_fail_synced(meta_request, NULL, AWS_ERROR_S3_CANCELED);
    aws_s3_meta_request_unlock_synced_data(meta_request);
}

void aws_s3_meta_request_set_fail_synced(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *failed_request,
    int error_code) {
    AWS_PRECONDITION(meta_request);
    ASSERT_SYNCED_DATA_LOCK_HELD(meta_request);

    if (meta_request->synced_data.finish_result_set) {
        return;
    }

    meta_request->synced_data.finish_result_set = true;

    if (error_code == AWS_ERROR_S3_INVALID_RESPONSE_STATUS && failed_request != NULL) {
        aws_s3_meta_request_result_setup(
            meta_request,
            &meta_request->synced_data.finish_result,
            failed_request,
            failed_request->send_data.response_status,
            error_code);
    } else {
        AWS_ASSERT(error_code != AWS_ERROR_S3_INVALID_RESPONSE_STATUS);

        aws_s3_meta_request_result_setup(meta_request, &meta_request->synced_data.finish_result, NULL, 0, error_code);
    }
}

void aws_s3_meta_request_set_success_synced(struct aws_s3_meta_request *meta_request, int response_status) {
    AWS_PRECONDITION(meta_request);
    ASSERT_SYNCED_DATA_LOCK_HELD(meta_request);

    if (meta_request->synced_data.finish_result_set) {
        return;
    }

    meta_request->synced_data.finish_result_set = true;

    aws_s3_meta_request_result_setup(
        meta_request, &meta_request->synced_data.finish_result, NULL, response_status, AWS_ERROR_SUCCESS);
}

bool aws_s3_meta_request_has_finish_result(struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);

    aws_s3_meta_request_lock_synced_data(meta_request);
    bool is_finishing = aws_s3_meta_request_has_finish_result_synced(meta_request);
    aws_s3_meta_request_unlock_synced_data(meta_request);

    return is_finishing;
}

bool aws_s3_meta_request_has_finish_result_synced(struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);
    ASSERT_SYNCED_DATA_LOCK_HELD(meta_request);

    if (!meta_request->synced_data.finish_result_set) {
        return false;
    }

    return true;
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

    AWS_LOGF_DEBUG(AWS_LS_S3_META_REQUEST, "id=%p Cleaning up meta request", (void *)meta_request);

    /* Clean up our initial http message */
    if (meta_request->initial_request_message != NULL) {
        aws_http_message_release(meta_request->initial_request_message);
        meta_request->initial_request_message = NULL;
    }

    void *meta_request_user_data = meta_request->user_data;
    aws_s3_meta_request_shutdown_fn *shutdown_callback = meta_request->shutdown_callback;

    aws_cached_signing_config_destroy(meta_request->cached_signing_config);
    aws_mutex_clean_up(&meta_request->synced_data.lock);
    aws_s3_client_release(meta_request->client);

    AWS_ASSERT(aws_priority_queue_size(&meta_request->synced_data.pending_body_streaming_requests) == 0);
    aws_priority_queue_clean_up(&meta_request->synced_data.pending_body_streaming_requests);
    aws_s3_meta_request_result_clean_up(meta_request, &meta_request->synced_data.finish_result);

    AWS_LOGF_DEBUG(
        AWS_LS_S3_META_REQUEST, "id=%p Calling virtual meta request destroy function.", (void *)meta_request);

    meta_request->vtable->destroy(meta_request);
    meta_request = NULL;

    AWS_LOGF_DEBUG(AWS_LS_S3_META_REQUEST, "id=%p Calling meta request shutdown callback.", (void *)meta_request);

    if (shutdown_callback != NULL) {
        shutdown_callback(meta_request_user_data);
    }

    AWS_LOGF_DEBUG(AWS_LS_S3_META_REQUEST, "id=%p Meta request clean up finished.", (void *)meta_request);
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

bool aws_s3_meta_request_update(
    struct aws_s3_meta_request *meta_request,
    uint32_t flags,
    struct aws_s3_request **out_request) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(meta_request->vtable);
    AWS_PRECONDITION(meta_request->vtable->update);

    return meta_request->vtable->update(meta_request, flags, out_request);
}

bool aws_s3_meta_request_is_active(struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);

    aws_s3_meta_request_lock_synced_data(meta_request);
    bool active = meta_request->synced_data.state == AWS_S3_META_REQUEST_STATE_ACTIVE;
    aws_s3_meta_request_unlock_synced_data(meta_request);

    return active;
}

bool aws_s3_meta_request_is_finished(struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);

    aws_s3_meta_request_lock_synced_data(meta_request);
    bool is_finished = meta_request->synced_data.state == AWS_S3_META_REQUEST_STATE_FINISHED;
    aws_s3_meta_request_unlock_synced_data(meta_request);

    return is_finished;
}

static void s_s3_meta_request_prepare_request_task(struct aws_task *task, void *arg, enum aws_task_status task_status);

static void s_s3_prepare_request_payload_callback_and_destroy(
    struct aws_s3_prepare_request_payload *payload,
    int error_code) {
    AWS_PRECONDITION(payload);
    AWS_PRECONDITION(payload->request);

    struct aws_s3_meta_request *meta_request = payload->request->meta_request;
    AWS_PRECONDITION(meta_request);

    struct aws_s3_client *client = meta_request->client;
    AWS_PRECONDITION(client);

    struct aws_allocator *sba_allocator = client->sba_allocator;
    AWS_PRECONDITION(sba_allocator);

    /* Acquire a reference to the client pointer just be to safe, so that we know the callback won't trigger a clean up
     * of the client and cause the sba_allocator to go away. */
    aws_s3_client_acquire(client);

    if (payload->callback != NULL) {
        payload->callback(meta_request, payload->request, error_code, payload->user_data);
    }

    aws_mem_release(sba_allocator, payload);
    aws_s3_client_release(client);
}

static void s_s3_meta_request_schedule_prepare_request_default(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    aws_s3_meta_request_prepare_request_callback_fn *callback,
    void *user_data);

void aws_s3_meta_request_prepare_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    aws_s3_meta_request_prepare_request_callback_fn *callback,
    void *user_data) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(meta_request->vtable);

    if (meta_request->vtable->schedule_prepare_request) {
        meta_request->vtable->schedule_prepare_request(meta_request, request, callback, user_data);
    } else {
        s_s3_meta_request_schedule_prepare_request_default(meta_request, request, callback, user_data);
    }
}

static void s_s3_meta_request_schedule_prepare_request_default(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    aws_s3_meta_request_prepare_request_callback_fn *callback,
    void *user_data) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(request);

    struct aws_s3_client *client = meta_request->client;
    AWS_PRECONDITION(client);

    struct aws_allocator *sba_allocator = client->sba_allocator;
    AWS_PRECONDITION(sba_allocator);

    struct aws_s3_prepare_request_payload *payload =
        aws_mem_calloc(sba_allocator, 1, sizeof(struct aws_s3_prepare_request_payload));

    payload->request = request;
    payload->callback = callback;
    payload->user_data = user_data;

    aws_task_init(
        &payload->task, s_s3_meta_request_prepare_request_task, payload, "s3_meta_request_prepare_request_task");
    aws_event_loop_schedule_task_now(meta_request->io_event_loop, &payload->task);
}

static void s_s3_meta_request_prepare_request_task(struct aws_task *task, void *arg, enum aws_task_status task_status) {
    (void)task;
    (void)task_status;

    struct aws_s3_prepare_request_payload *payload = arg;
    AWS_PRECONDITION(payload);

    struct aws_s3_request *request = payload->request;
    AWS_PRECONDITION(request);

    struct aws_s3_meta_request *meta_request = request->meta_request;
    AWS_PRECONDITION(meta_request);

    struct aws_s3_meta_request_vtable *vtable = meta_request->vtable;
    AWS_PRECONDITION(vtable);

    /* Client owns this event loop group. A cancel should not be possible. */
    AWS_ASSERT(task_status == AWS_TASK_STATUS_RUN_READY);

    int prepare_result = vtable->prepare_request(meta_request, request);

    ++request->num_times_prepared;

    if (prepare_result == AWS_OP_ERR) {
        int error_code = aws_last_error_or_unknown();

        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST,
            "id=%p Could not prepare request %p due to error %d (%s).",
            (void *)meta_request,
            (void *)request,
            error_code,
            aws_error_str(error_code));

        aws_s3_meta_request_lock_synced_data(meta_request);
        aws_s3_meta_request_set_fail_synced(meta_request, request, error_code);
        aws_s3_meta_request_unlock_synced_data(meta_request);

        s_s3_prepare_request_payload_callback_and_destroy(payload, error_code);
        return;
    }

    /* Sign the newly created message. */
    s_s3_meta_request_sign_request(meta_request, request, s_s3_meta_request_request_on_signed, payload);
}

static void s_s3_meta_request_init_signing_date_time(
    struct aws_s3_meta_request *meta_request,
    struct aws_date_time *date_time) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(meta_request->vtable);
    AWS_PRECONDITION(meta_request->vtable->init_signing_date_time);

    meta_request->vtable->init_signing_date_time(meta_request, date_time);
}

void aws_s3_meta_request_init_signing_date_time_default(
    struct aws_s3_meta_request *meta_request,
    struct aws_date_time *date_time) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(date_time);
    (void)meta_request;

    aws_date_time_init_now(date_time);
}

static void s_s3_meta_request_sign_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    aws_signing_complete_fn *on_signing_complete,
    void *user_data) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(meta_request->vtable);
    AWS_PRECONDITION(meta_request->vtable->send_request_finish);

    meta_request->vtable->sign_request(meta_request, request, on_signing_complete, user_data);
}

/* Handles signing a message for the caller. */
void aws_s3_meta_request_sign_request_default(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    aws_signing_complete_fn *on_signing_complete,
    void *user_data) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(request);
    AWS_PRECONDITION(on_signing_complete);

    struct aws_s3_client *client = meta_request->client;
    AWS_ASSERT(client);

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

        on_signing_complete(NULL, AWS_ERROR_SUCCESS, user_data);
        return;
    }

    s_s3_meta_request_init_signing_date_time(meta_request, &signing_config.date);

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

        on_signing_complete(NULL, aws_last_error_or_unknown(), user_data);
        return;
    }

    if (aws_sign_request_aws(
            meta_request->allocator,
            request->send_data.signable,
            (struct aws_signing_config_base *)&signing_config,
            on_signing_complete,
            user_data)) {

        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST, "id=%p: Could not sign request %p", (void *)meta_request, (void *)request);

        on_signing_complete(NULL, aws_last_error_or_unknown(), user_data);
        return;
    }
}

/* Handle the signing result, getting an HTTP connection for the request if signing succeeded. */
static void s_s3_meta_request_request_on_signed(
    struct aws_signing_result *signing_result,
    int error_code,
    void *user_data) {

    struct aws_s3_prepare_request_payload *payload = user_data;
    AWS_PRECONDITION(payload);

    struct aws_s3_request *request = payload->request;
    AWS_PRECONDITION(request);

    struct aws_s3_meta_request *meta_request = request->meta_request;
    AWS_PRECONDITION(meta_request);

    if (error_code != AWS_ERROR_SUCCESS) {
        goto finish;
    }

    if (signing_result != NULL &&
        aws_apply_signing_result_to_http_request(request->send_data.message, meta_request->allocator, signing_result)) {

        error_code = aws_last_error_or_unknown();

        goto finish;
    }

finish:

    if (error_code != AWS_ERROR_SUCCESS) {

        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST,
            "id=%p Meta request could not sign TTP request due to error code %d (%s)",
            (void *)meta_request,
            error_code,
            aws_error_str(error_code));

        aws_s3_meta_request_lock_synced_data(meta_request);
        aws_s3_meta_request_set_fail_synced(meta_request, request, error_code);
        aws_s3_meta_request_unlock_synced_data(meta_request);
    }

    s_s3_prepare_request_payload_callback_and_destroy(payload, error_code);
}

void aws_s3_meta_request_send_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_vip_connection *vip_connection) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(vip_connection);
    AWS_PRECONDITION(vip_connection->http_connection);

    struct aws_s3_request *request = vip_connection->request;
    AWS_PRECONDITION(request);

    /* Now that we have a signed request and a connection, go ahead and issue the request. */
    struct aws_http_make_request_options options;
    AWS_ZERO_STRUCT(options);

    options.self_size = sizeof(struct aws_http_make_request_options);
    options.request = request->send_data.message;
    options.user_data = vip_connection;
    options.on_response_headers = s_s3_meta_request_incoming_headers;
    options.on_response_header_block_done = NULL;
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

    return AWS_OP_SUCCESS;
}

static int s_s3_meta_request_incoming_body(
    struct aws_http_stream *stream,
    const struct aws_byte_cursor *data,
    void *user_data) {
    (void)stream;

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

    int response_status = request->send_data.response_status;

    /* If our error code is currently success, then we have some other calls to make that could still indicate a
     * failure. */
    if (error_code == AWS_ERROR_SUCCESS) {
        /* Check if the response code indicates an error occurred. */
        error_code = s_s3_meta_request_error_code_from_response_status(response_status);

        if (error_code != AWS_ERROR_SUCCESS) {
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

    if (error_code == AWS_ERROR_SUCCESS) {

        finish_code = AWS_S3_VIP_CONNECTION_FINISH_CODE_SUCCESS;

    } else {
        aws_s3_meta_request_lock_synced_data(meta_request);
        bool meta_request_finishing = aws_s3_meta_request_has_finish_result_synced(meta_request);
        aws_s3_meta_request_unlock_synced_data(meta_request);

        /* If the request failed due to an invalid (ie: unrecoverable) response status, or the meta request already has
         * a result, then make sure that this request isn't retried. */
        if (error_code == AWS_ERROR_S3_INVALID_RESPONSE_STATUS || meta_request_finishing) {
            finish_code = AWS_S3_VIP_CONNECTION_FINISH_CODE_FAILED;

            AWS_LOGF_ERROR(
                AWS_LS_S3_META_REQUEST,
                "id=%p Meta request cannot recover from error %d (%s). (request=%p, response status=%d)",
                (void *)meta_request,
                error_code,
                aws_error_str(error_code),
                (void *)request,
                response_status);

        } else {
            /* Otherwise, set this up for a retry if the meta request is active. */
            finish_code = AWS_S3_VIP_CONNECTION_FINISH_CODE_RETRY;
        }
    }

    if (stream != NULL) {
        aws_http_stream_release(stream);
        stream = NULL;
    }

    aws_s3_client_notify_connection_finished(client, vip_connection, error_code, finish_code);
}

void aws_s3_meta_request_finished_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    int error_code) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(meta_request->vtable);
    AWS_PRECONDITION(meta_request->vtable->finished_request);

    meta_request->vtable->finished_request(meta_request, request, error_code);
}

struct s3_stream_response_body_payload {
    struct aws_s3_meta_request *meta_request;
    struct aws_linked_list requests;
    struct aws_task task;
};

/* Pushes a request into the body streaming priority queue. Derived meta request types should not call this--they should
 * instead call aws_s3_meta_request_stream_response_body_synced.*/
static void s_s3_meta_request_body_streaming_push_synced(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request);

/* Pops the next available request from the body streaming priority queue. If the parts previous the next request in the
 * priority queue have not been placed in the priority queue yet, the priority queue will remain the same, and NULL will
 * be returned. (Should not be needed to be called by derived types.) */
static struct aws_s3_request *s_s3_meta_request_body_streaming_pop_next_synced(
    struct aws_s3_meta_request *meta_request);

static void s_s3_meta_request_body_streaming_task(struct aws_task *task, void *arg, enum aws_task_status task_status);

void aws_s3_meta_request_stream_response_body_synced(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request) {
    ASSERT_SYNCED_DATA_LOCK_HELD(meta_request);
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(request);
    AWS_PRECONDITION(request->part_number > 0);

    struct aws_linked_list streaming_requests;
    aws_linked_list_init(&streaming_requests);

    /* Push it into the priority queue. */
    s_s3_meta_request_body_streaming_push_synced(meta_request, request);

    struct aws_s3_client *client = meta_request->client;
    AWS_PRECONDITION(client);
    aws_atomic_fetch_add(&client->stats.num_requests_stream_queued_waiting, 1);

    /* Grab the next request that can be streamed back to the caller. */
    struct aws_s3_request *next_streaming_request = s_s3_meta_request_body_streaming_pop_next_synced(meta_request);
    uint32_t num_streaming_requests = 0;

    /* Grab any additional requests that could be streamed to the caller. */
    while (next_streaming_request != NULL) {
        aws_atomic_fetch_sub(&client->stats.num_requests_stream_queued_waiting, 1);

        aws_linked_list_push_back(&streaming_requests, &next_streaming_request->node);
        ++num_streaming_requests;
        next_streaming_request = s_s3_meta_request_body_streaming_pop_next_synced(meta_request);
    }

    if (aws_linked_list_empty(&streaming_requests)) {
        return;
    }

    aws_atomic_fetch_add(&client->stats.num_requests_streaming, num_streaming_requests);

    meta_request->synced_data.num_parts_delivery_sent += num_streaming_requests;

    struct s3_stream_response_body_payload *payload =
        aws_mem_calloc(client->sba_allocator, 1, sizeof(struct s3_stream_response_body_payload));

    aws_s3_meta_request_acquire(meta_request);
    payload->meta_request = meta_request;

    aws_linked_list_init(&payload->requests);
    aws_linked_list_swap_contents(&payload->requests, &streaming_requests);

    aws_task_init(
        &payload->task, s_s3_meta_request_body_streaming_task, payload, "s_s3_meta_request_body_streaming_task");
    aws_event_loop_schedule_task_now(meta_request->io_event_loop, &payload->task);
}

static void s_s3_meta_request_body_streaming_task(struct aws_task *task, void *arg, enum aws_task_status task_status) {
    (void)task;
    (void)task_status;

    struct s3_stream_response_body_payload *payload = arg;
    AWS_PRECONDITION(payload);

    struct aws_s3_meta_request *meta_request = payload->meta_request;
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(meta_request->vtable);

    struct aws_s3_client *client = meta_request->client;
    AWS_PRECONDITION(client);

    /* Client owns this event loop group. A cancel should not be possible. */
    AWS_ASSERT(task_status == AWS_TASK_STATUS_RUN_READY);

    struct aws_linked_list completed_requests;
    aws_linked_list_init(&completed_requests);

    int error_code = AWS_ERROR_SUCCESS;
    uint32_t num_successful = 0;
    uint32_t num_failed = 0;

    while (!aws_linked_list_empty(&payload->requests)) {
        struct aws_linked_list_node *request_node = aws_linked_list_pop_front(&payload->requests);
        struct aws_s3_request *request = AWS_CONTAINER_OF(request_node, struct aws_s3_request, node);
        AWS_ASSERT(meta_request == request->meta_request);

        struct aws_byte_cursor body_buffer_byte_cursor = aws_byte_cursor_from_buf(&request->send_data.response_body);

        AWS_ASSERT(request->part_number >= 1);

        uint64_t range_start = (request->part_number - 1) * meta_request->part_size;

        if (aws_s3_meta_request_has_finish_result(meta_request)) {
            ++num_failed;
        } else {
            if (error_code == AWS_ERROR_SUCCESS && meta_request->body_callback &&
                meta_request->body_callback(
                    meta_request, &body_buffer_byte_cursor, range_start, meta_request->user_data)) {
                error_code = aws_last_error_or_unknown();
            }

            if (error_code == AWS_ERROR_SUCCESS) {
                ++num_successful;
            } else {
                ++num_failed;
            }
        }

        aws_atomic_fetch_sub(&client->stats.num_requests_streaming, 1);
        aws_s3_request_release(request);
    }

    aws_s3_meta_request_lock_synced_data(meta_request);
    if (error_code != AWS_ERROR_SUCCESS) {
        aws_s3_meta_request_set_fail_synced(meta_request, NULL, error_code);
    }

    meta_request->synced_data.num_parts_delivery_completed += (num_failed + num_successful);
    meta_request->synced_data.num_parts_delivery_failed += num_failed;
    meta_request->synced_data.num_parts_delivery_succeeded += num_successful;
    aws_s3_meta_request_unlock_synced_data(meta_request);

    aws_mem_release(client->sba_allocator, payload);
    payload = NULL;

    aws_s3_client_schedule_process_work(client);
    aws_s3_meta_request_release(meta_request);
}

static void s_s3_meta_request_body_streaming_push_synced(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request) {
    ASSERT_SYNCED_DATA_LOCK_HELD(meta_request);
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(request);

    AWS_ASSERT(request->meta_request == meta_request);

    aws_s3_request_acquire(request);

    aws_priority_queue_push(&meta_request->synced_data.pending_body_streaming_requests, &request);
}

static struct aws_s3_request *s_s3_meta_request_body_streaming_pop_next_synced(
    struct aws_s3_meta_request *meta_request) {
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

void aws_s3_meta_request_finish(struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(meta_request->vtable);
    AWS_PRECONDITION(meta_request->vtable->finish);

    meta_request->vtable->finish(meta_request);
}

void aws_s3_meta_request_finish_default(struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);

    bool already_finished = false;
    struct aws_linked_list release_request_list;
    aws_linked_list_init(&release_request_list);

    struct aws_s3_meta_request_result finish_result;
    AWS_ZERO_STRUCT(finish_result);

    aws_s3_meta_request_lock_synced_data(meta_request);

    if (meta_request->synced_data.state == AWS_S3_META_REQUEST_STATE_FINISHED) {
        already_finished = true;
        goto unlock;
    }

    meta_request->synced_data.state = AWS_S3_META_REQUEST_STATE_FINISHED;

    /* Clean out the pending-stream-to-caller priority queue*/
    while (aws_priority_queue_size(&meta_request->synced_data.pending_body_streaming_requests) > 0) {
        struct aws_s3_request *request = NULL;
        aws_priority_queue_pop(&meta_request->synced_data.pending_body_streaming_requests, (void **)&request);
        AWS_FATAL_ASSERT(request != NULL);

        aws_linked_list_push_back(&release_request_list, &request->node);
    }

    finish_result = meta_request->synced_data.finish_result;
    AWS_ZERO_STRUCT(meta_request->synced_data.finish_result);

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

    AWS_LOGF_DEBUG(
        AWS_LS_S3_META_REQUEST,
        "id=%p Meta request finished with error code %d (%s)",
        (void *)meta_request,
        finish_result.error_code,
        aws_error_str(finish_result.error_code));

    if (meta_request->finish_callback != NULL) {
        meta_request->finish_callback(meta_request, &finish_result, meta_request->user_data);
    }

    aws_s3_meta_request_result_clean_up(meta_request, &finish_result);

    aws_s3_client_release(meta_request->client);
    meta_request->client = NULL;
    meta_request->io_event_loop = NULL;
}

int aws_s3_meta_request_read_body(struct aws_s3_meta_request *meta_request, struct aws_byte_buf *buffer) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(buffer);

    struct aws_input_stream *initial_body_stream =
        aws_http_message_get_body_stream(meta_request->initial_request_message);
    AWS_FATAL_ASSERT(initial_body_stream);

    /* Copy it into our buffer. */
    if (aws_input_stream_read(initial_body_stream, buffer)) {
        AWS_LOGF_ERROR(AWS_LS_S3_META_REQUEST, "id=%p Could not read from body stream.", (void *)meta_request);
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}

void aws_s3_meta_request_result_setup(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_meta_request_result *result,
    struct aws_s3_request *request,
    int response_status,
    int error_code) {

    if (request != NULL) {
        if (request->send_data.response_headers != NULL) {
            result->error_response_headers = request->send_data.response_headers;
            aws_http_headers_acquire(result->error_response_headers);
        }

        if (request->send_data.response_body.capacity > 0) {
            result->error_response_body = aws_mem_calloc(meta_request->allocator, 1, sizeof(struct aws_byte_buf));

            aws_byte_buf_init_copy(
                result->error_response_body, meta_request->allocator, &request->send_data.response_body);
        }
    }

    result->response_status = response_status;
    result->error_code = error_code;
}

void aws_s3_meta_request_result_clean_up(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_meta_request_result *result) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(result);

    aws_http_headers_release(result->error_response_headers);

    if (result->error_response_body != NULL) {
        aws_byte_buf_clean_up(result->error_response_body);
        aws_mem_release(meta_request->allocator, result->error_response_body);
    }

    AWS_ZERO_STRUCT(*result);
}
