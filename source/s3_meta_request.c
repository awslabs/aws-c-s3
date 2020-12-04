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
#include <aws/io/stream.h>
#include <inttypes.h>

static const uint64_t s_response_body_error_buf_size = KB_TO_BYTES(1);

static void s_s3_request_destroy(void *user_data);

static void s_s3_meta_request_start_destroy(void *user_data);
static void s_s3_meta_request_finish_destroy(void *user_data);

static int s_s3_meta_request_set_state(struct aws_s3_meta_request *meta_request, enum aws_s3_meta_request_state state);

static void s_s3_meta_request_process_write_body_task(
    struct aws_task *task,
    void *arg,
    enum aws_task_status task_status);

static int s_s3_meta_request_sign_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_vip_connection *vip_connection);

static int s_s3_meta_request_sign_request_default(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_vip_connection *vip_connection);

static void s_s3_meta_request_request_on_signed(
    struct aws_signing_result *signing_result,
    int error_code,
    void *user_data);

static void s_s3_meta_request_send_http_request(
    struct aws_http_connection *http_connection,
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

static void s_s3_meta_request_send_request_finish_default(
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

struct aws_s3_client *aws_s3_meta_request_acquire_client(struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);

    struct aws_s3_client *client = NULL;

    aws_s3_meta_request_lock_synced_data(meta_request);

    client = meta_request->synced_data.client;

    if (client != NULL) {
        AWS_LOGF_TRACE(
            AWS_LS_S3_META_REQUEST,
            "id=%p Meta request trying to get reference to client, but client is null.",
            (void *)meta_request);
        aws_s3_client_acquire(client);
    } else {
        AWS_LOGF_TRACE(
            AWS_LS_S3_META_REQUEST,
            "id=%p Meta request trying to get reference to client but client is null.",
            (void *)meta_request);
    }

    aws_s3_meta_request_unlock_synced_data(meta_request);

    return client;
}

void aws_s3_meta_request_schedule_work(struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);

    struct aws_s3_client *client = aws_s3_meta_request_acquire_client(meta_request);

    if (client != NULL) {
        aws_s3_client_schedule_meta_request_work(client, meta_request);
    } else {
        AWS_LOGF_TRACE(
            AWS_LS_S3_META_REQUEST,
            "id=%p Meta request trying to schedule work but client is null.",
            (void *)meta_request);
    }

    aws_s3_client_release(client);
}

int aws_s3_meta_request_init_base(
    struct aws_allocator *allocator,
    struct aws_s3_client *client,
    const struct aws_s3_meta_request_options *options,
    void *impl,
    struct aws_s3_meta_request_vtable *vtable,
    struct aws_s3_meta_request *meta_request) {

    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(client);
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

    if (vtable->sign_request == NULL) {
        vtable->sign_request = s_s3_meta_request_sign_request_default;
    }

    if (vtable->send_request_finish == NULL) {
        vtable->send_request_finish = s_s3_meta_request_send_request_finish_default;
    }

    meta_request->allocator = allocator;

    /* Set up reference count. */
    aws_ref_count_init(&meta_request->ref_count, meta_request, s_s3_meta_request_start_destroy);
    aws_ref_count_init(&meta_request->internal_ref_count, meta_request, s_s3_meta_request_finish_destroy);

    *((uint64_t *)&meta_request->part_size) = client->part_size;

    if (options->signing_config) {
        meta_request->cached_signing_config = aws_cached_signing_config_new(allocator, options->signing_config);
    }

    meta_request->event_loop = client->event_loop;

    /* Keep a reference to the original message structure passed in. */
    meta_request->initial_request_message = options->message;
    aws_http_message_acquire(options->message);

    aws_linked_list_init(&meta_request->threaded_data.referenced_vip_connections);
    aws_linked_list_init(&meta_request->synced_data.retry_queue);

    /* Store a copy of the original message's initial body stream in our synced data, so that concurrent requests can
     * safely take turns reading from it when needed. */
    meta_request->synced_data.initial_body_stream = aws_http_message_get_body_stream(options->message);

    if (aws_mutex_init(&meta_request->synced_data.lock)) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST, "id=%p Could not initialize mutex for meta request", (void *)meta_request);
        return AWS_OP_ERR;
    }

    aws_s3_client_acquire(client);
    meta_request->synced_data.client = client;

    meta_request->user_data = options->user_data;
    meta_request->headers_callback = options->headers_callback;
    meta_request->body_callback = options->body_callback;
    meta_request->finish_callback = options->finish_callback;
    meta_request->shutdown_callback = options->shutdown_callback;

    return AWS_OP_SUCCESS;
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

static void s_s3_meta_request_start_destroy(void *user_data) {
    struct aws_s3_meta_request *meta_request = user_data;
    AWS_PRECONDITION(meta_request);

    aws_s3_meta_request_internal_release(meta_request);
}

static void s_s3_meta_request_finish_destroy(void *user_data) {
    struct aws_s3_meta_request *meta_request = user_data;
    AWS_PRECONDITION(meta_request);

    /* Clean out the retry queue*/
    while (!aws_linked_list_empty(&meta_request->synced_data.retry_queue)) {
        struct aws_linked_list_node *request_node = aws_linked_list_pop_front(&meta_request->synced_data.retry_queue);

        struct aws_s3_request *request = AWS_CONTAINER_OF(request_node, struct aws_s3_request, node);
        AWS_FATAL_ASSERT(request != NULL);

        aws_s3_request_release(request);
    }

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

    meta_request->vtable->destroy(meta_request);

    if (shutdown_callback != NULL) {
        shutdown_callback(meta_request_user_data);
    }
}

void aws_s3_meta_request_internal_acquire(struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);

    aws_ref_count_acquire(&meta_request->internal_ref_count);
}

void aws_s3_meta_request_internal_release(struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);

    aws_ref_count_release(&meta_request->internal_ref_count);
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

    request->desc_data.request_tag = request_tag;
    request->desc_data.part_number = part_number;
    request->desc_data.record_response_headers = (flags & AWS_S3_REQUEST_DESC_RECORD_RESPONSE_HEADERS) != 0;
    request->desc_data.use_initial_body_stream = (flags & AWS_S3_REQUEST_DESC_USE_INITIAL_BODY_STREAM) != 0;

    return request;
}

void aws_s3_request_setup_send_data(
    struct aws_s3_request *request,
    struct aws_http_message *message,
    struct aws_s3_part_buffer *part_buffer) {
    AWS_PRECONDITION(request);
    AWS_PRECONDITION(message);

    struct aws_s3_meta_request *meta_request = request->meta_request;
    AWS_PRECONDITION(meta_request);

    aws_s3_request_clean_up_send_data(request);

    request->send_data.message = message;
    aws_http_message_acquire(message);

    if (request->desc_data.use_initial_body_stream) {
        aws_s3_meta_request_lock_synced_data(meta_request);

        struct aws_input_stream *input_stream = meta_request->synced_data.initial_body_stream;

        if (input_stream != NULL) {
            aws_http_message_set_body_stream(message, input_stream);
            meta_request->synced_data.initial_body_stream = NULL;
        }

        aws_s3_meta_request_unlock_synced_data(meta_request);

        /* TODO don't assume that the stream is seekable or that it started at the beginning. */
        if (input_stream != NULL) {
            aws_input_stream_seek(input_stream, 0, AWS_SSB_BEGIN);
        }
    }

    request->send_data.part_buffer = part_buffer;
}

void s_s3_request_clean_up_send_data_message(struct aws_s3_request *request) {
    AWS_PRECONDITION(request);

    struct aws_http_message *message = request->send_data.message;

    if (message == NULL) {
        return;
    }

    request->send_data.message = NULL;

    struct aws_input_stream *input_stream = aws_http_message_get_body_stream(message);

    if (input_stream != NULL) {
        if (request->desc_data.use_initial_body_stream) {
            struct aws_s3_meta_request *meta_request = request->meta_request;
            AWS_ASSERT(meta_request);

            aws_s3_meta_request_lock_synced_data(meta_request);
            meta_request->synced_data.initial_body_stream = input_stream;
            aws_s3_meta_request_unlock_synced_data(meta_request);
        } else {
            aws_input_stream_destroy(input_stream);
        }

        input_stream = NULL;
    }

    aws_http_message_set_body_stream(message, NULL);
    aws_http_message_release(message);
}

void aws_s3_request_clean_up_send_data(struct aws_s3_request *request) {
    AWS_PRECONDITION(request);

    s_s3_request_clean_up_send_data_message(request);

    aws_http_headers_release(request->send_data.response_headers);
    request->send_data.response_headers = NULL;

    aws_byte_buf_clean_up(&request->send_data.response_body_error);

    aws_s3_part_buffer_release(request->send_data.part_buffer);
    request->send_data.part_buffer = NULL;

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

void s_s3_request_destroy(void *user_data) {
    struct aws_s3_request *request = user_data;

    if (request == NULL) {
        return;
    }

    aws_retry_token_release(request->retry_token);
    aws_s3_request_clean_up_send_data(request);
    aws_s3_meta_request_release(request->meta_request);
    aws_mem_release(request->allocator, request);
}

static void s_s3_meta_request_clean_up_work_data(struct aws_s3_vip_connection *vip_connection) {
    AWS_PRECONDITION(vip_connection);

    aws_s3_request_release(vip_connection->work_data.request);
    vip_connection->work_data.request = NULL;
}

bool aws_s3_meta_request_has_work(const struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);

    bool has_result = false;
    bool result = false;

    aws_s3_meta_request_lock_synced_data((struct aws_s3_meta_request *)meta_request);

    if (meta_request->synced_data.state == AWS_S3_META_REQUEST_STATE_FINISHED) {
        has_result = true;
        result = false;
    } else if (!aws_linked_list_empty(&meta_request->synced_data.retry_queue)) {
        has_result = true;
        result = true;
    }

    aws_s3_meta_request_unlock_synced_data((struct aws_s3_meta_request *)meta_request);

    if (has_result) {
        return result;
    }

    struct aws_s3_meta_request_vtable *vtable = meta_request->vtable;
    AWS_FATAL_ASSERT(vtable);

    return vtable->has_work(meta_request);
}

void aws_s3_meta_request_send_next_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_vip_connection *vip_connection,
    aws_s3_request_finished_callback_fn *finished_callback,
    void *user_data) {

    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(vip_connection);

    struct aws_s3_meta_request_vtable *vtable = meta_request->vtable;
    AWS_FATAL_ASSERT(vtable);

    struct aws_s3_request *request = NULL;
    bool meta_request_already_finished = false;

    aws_s3_meta_request_lock_synced_data(meta_request);

    /* If the meta request has been finished, don't initiate any additional work. */
    if (meta_request->synced_data.state == AWS_S3_META_REQUEST_STATE_FINISHED) {
        meta_request_already_finished = true;
        goto unlock;
    }

    request = aws_s3_meta_request_retry_queue_pop_synced(meta_request);

unlock:

    aws_s3_meta_request_unlock_synced_data(meta_request);

    if (meta_request_already_finished) {
        goto call_finished_callback;
    }

    /* If we didn't find something in the retry queue, call the derived request type, asking what the next request
     * should be. */
    if (request == NULL) {
        if (vtable->next_request(meta_request, &request)) {

            aws_s3_meta_request_handle_error(meta_request, NULL, aws_last_error());

            goto call_finished_callback;
        }
    }

    /* If it returned NULL, return success.  This is valid in cases where the meta request is waiting on a particular
     * request finishing before it can continue.  (ie: a create-multipart-upload, or an initial ranged get to discover
     * file size, etc.) */
    if (request == NULL) {
        goto call_finished_callback;
    }

    struct aws_s3_vip *vip = vip_connection->owning_vip;
    struct aws_s3_client *client = vip->owning_client;

    if (vtable->prepare_request(meta_request, client, request)) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST, "id=%p Could not prepare request %p", (void *)meta_request, (void *)request);

        aws_s3_meta_request_handle_error(meta_request, request, aws_last_error());

        aws_s3_request_release(request);

        goto call_finished_callback;
    }

    AWS_LOGF_TRACE(
        AWS_LS_S3_META_REQUEST, "id=%p Initiating work for request %p", (void *)meta_request, (void *)request);

    request->send_data.finished_callback = finished_callback;
    request->send_data.user_data = user_data;

    aws_s3_request_acquire(request);
    vip_connection->work_data.request = request;

    /* Release initial reference of request to give complete ownership to the work_data. */
    aws_s3_request_release(vip_connection->work_data.request);

    AWS_LOGF_TRACE(AWS_LS_S3_META_REQUEST, "id=%p Signing request %p", (void *)meta_request, (void *)request);

    /* Sign the newly created message. */
    if (s_s3_meta_request_sign_request(meta_request, vip_connection)) {
        aws_s3_meta_request_handle_error(meta_request, request, aws_last_error());
        goto call_finished_callback;
    }

    return;

call_finished_callback:

    s_s3_meta_request_clean_up_work_data(vip_connection);

    if (finished_callback != NULL) {
        finished_callback(user_data);
    }
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
static int s_s3_meta_request_sign_request_default(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_vip_connection *vip_connection) {
    AWS_PRECONDITION(meta_request)
    AWS_PRECONDITION(vip_connection);
    AWS_PRECONDITION(vip_connection->owning_vip);

    struct aws_s3_client *client = vip_connection->owning_vip->owning_client;
    AWS_PRECONDITION(client);

    struct aws_s3_request *request = vip_connection->work_data.request;
    AWS_PRECONDITION(request);

    struct aws_signing_config_aws signing_config;

    if (meta_request->cached_signing_config != NULL) {
        signing_config = meta_request->cached_signing_config->config;
    } else if (client->cached_signing_config != NULL) {
        signing_config = client->cached_signing_config->config;
    } else {
        AWS_LOGF_TRACE(
            AWS_LS_S3_META_REQUEST,
            "id=%p: No signing config present. Not signing request %p.",
            (void *)meta_request,
            (void *)request);

        s_s3_meta_request_request_on_signed(NULL, AWS_ERROR_SUCCESS, vip_connection);
        return AWS_OP_SUCCESS;
    }

    aws_date_time_init_now(&signing_config.date);

    int result = AWS_OP_SUCCESS;
    struct aws_signable *signable = aws_signable_new_http_request(meta_request->allocator, request->send_data.message);

    if (signable == NULL) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST,
            "id=%p: Could not allocate signable for request %p",
            (void *)meta_request,
            (void *)request);

        result = AWS_OP_ERR;
        goto clean_up;
    }

    if (aws_sign_request_aws(
            meta_request->allocator,
            signable,
            (struct aws_signing_config_base *)&signing_config,
            s_s3_meta_request_request_on_signed,
            vip_connection)) {

        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST, "id=%p: Could not sign request %p", (void *)meta_request, (void *)request);

        result = AWS_OP_ERR;
        goto clean_up;
    }

clean_up:

    aws_signable_destroy(signable);

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

    struct aws_s3_request *request = vip_connection->work_data.request;
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

    AWS_LOGF_TRACE(
        AWS_LS_S3_META_REQUEST, "id=%p Getting HTTP connection for request %p", (void *)meta_request, (void *)request);

    /* Get a connection for this request, finishing if we failed immediately trying to get the connection. */
    if (aws_s3_client_get_http_connection(
            client, vip_connection, s_s3_meta_request_send_http_request, vip_connection)) {

        goto error_finish;
    }

    return;

error_finish:

    s_s3_meta_request_send_request_finish(vip_connection, NULL, aws_last_error());
}

/* Set up a stream to actually process the request. */
static void s_s3_meta_request_send_http_request(
    struct aws_http_connection *http_connection,
    int error_code,
    void *user_data) {

    struct aws_s3_vip_connection *vip_connection = user_data;
    AWS_PRECONDITION(vip_connection);

    struct aws_s3_request *request = vip_connection->work_data.request;
    AWS_PRECONDITION(request && request->send_data.message);

    struct aws_s3_meta_request *meta_request = request->meta_request;
    AWS_PRECONDITION(meta_request);

    struct aws_s3_meta_request_vtable *vtable = meta_request->vtable;
    AWS_FATAL_ASSERT(vtable);

    if (error_code != AWS_ERROR_SUCCESS) {
        s_s3_meta_request_send_request_finish(vip_connection, NULL, error_code);
        return;
    }

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

    struct aws_http_stream *stream = aws_http_connection_make_request(http_connection, &options);

    if (stream == NULL) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST, "id=%p: Could not make HTTP request %p", (void *)meta_request, (void *)request);

        s_s3_meta_request_send_request_finish(vip_connection, NULL, aws_last_error());
        return;
    }

    AWS_LOGF_TRACE(AWS_LS_S3_META_REQUEST, "id=%p: Sending request %p", (void *)meta_request, (void *)request);

    if (aws_http_stream_activate(stream) != AWS_OP_SUCCESS) {
        aws_http_stream_release(stream);
        stream = NULL;

        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST, "id=%p: Could not activate HTTP stream %p", (void *)meta_request, (void *)request);

        s_s3_meta_request_send_request_finish(vip_connection, NULL, aws_last_error());
        return;
    }
}

static int s_s3_meta_request_error_code_from_response_status(int response_status) {
    int error_code = AWS_ERROR_UNKNOWN;

    if (response_status == AWS_S3_RESPONSE_STATUS_SUCCESS || response_status == AWS_S3_RESPONSE_STATUS_RANGE_SUCCESS) {
        error_code = AWS_ERROR_SUCCESS;
    } else if (response_status == AWS_S3_RESPONSE_STATUS_INTERNAL_ERROR) {
        error_code = AWS_ERROR_S3_INTERNAL_ERROR;
    } else if (response_status == AWS_S3_RESPONSE_STATUS_SLOW_DOWN) {
        error_code = AWS_ERROR_S3_SLOW_DOWN;
    } else {
        error_code = AWS_ERROR_S3_INVALID_RESPONSE_STATUS;
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

    struct aws_s3_request *request = vip_connection->work_data.request;
    AWS_PRECONDITION(request);

    struct aws_s3_meta_request *meta_request = request->meta_request;
    AWS_PRECONDITION(meta_request);

    AWS_LOGF_TRACE(
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
    bool should_record_headers = !successful_response || request->desc_data.record_response_headers;

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

    struct aws_s3_request *request = vip_connection->work_data.request;
    AWS_PRECONDITION(request);

    struct aws_s3_meta_request *meta_request = request->meta_request;
    AWS_PRECONDITION(meta_request && meta_request->vtable);

    AWS_LOGF_TRACE(
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
    } else {
        /* We may have an error body coming soon, so allocate a buffer for that error. */
        aws_byte_buf_init(
            &request->send_data.response_body_error, meta_request->allocator, s_response_body_error_buf_size);
    }

    return AWS_OP_SUCCESS;
}

static int s_s3_meta_request_incoming_body(
    struct aws_http_stream *stream,
    const struct aws_byte_cursor *data,
    void *user_data) {

    struct aws_s3_vip_connection *vip_connection = user_data;
    AWS_PRECONDITION(vip_connection);

    struct aws_s3_request *request = vip_connection->work_data.request;
    AWS_PRECONDITION(request);

    struct aws_s3_meta_request *meta_request = request->meta_request;
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(meta_request->vtable);

    AWS_LOGF_TRACE(
        AWS_LS_S3_META_REQUEST,
        "id=%p Incoming body for request %p. Response status: %d. Data Size: %" PRIu64 ". VIP connection: %p.",
        (void *)meta_request,
        (void *)request,
        request->send_data.response_status,
        (uint64_t)data->len,
        (void *)vip_connection);

    bool successful_response =
        s_s3_meta_request_error_code_from_response_status(request->send_data.response_status) == AWS_ERROR_SUCCESS;

    /* Failed requests are handled inside of the meta request base type, so only pass through to virtual function on
     * success. */
    if (successful_response) {

        if (meta_request->vtable->incoming_body) {
            return meta_request->vtable->incoming_body(stream, data, vip_connection);
        }

    } else {

        /* Append the error to the response_body_error buffer, allowing the buffer to grow if necessary. */
        if (aws_byte_buf_append_dynamic(&request->send_data.response_body_error, data)) {
            return AWS_OP_ERR;
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

    struct aws_s3_request *request = vip_connection->work_data.request;
    AWS_PRECONDITION(request);

    struct aws_s3_meta_request *meta_request = request->meta_request;
    AWS_PRECONDITION(meta_request);

    struct aws_s3_meta_request_vtable *vtable = meta_request->vtable;
    AWS_PRECONDITION(vtable);

    vtable->send_request_finish(vip_connection, stream, error_code);
}

static void s_s3_meta_request_send_request_finish_default(
    struct aws_s3_vip_connection *vip_connection,
    struct aws_http_stream *stream,
    int error_code) {
    AWS_PRECONDITION(vip_connection);

    struct aws_s3_request *request = vip_connection->work_data.request;
    AWS_PRECONDITION(request);

    struct aws_s3_meta_request *meta_request = request->meta_request;
    AWS_PRECONDITION(meta_request);

    struct aws_s3_meta_request_vtable *vtable = meta_request->vtable;
    AWS_PRECONDITION(vtable);

    int response_status = request->send_data.response_status;

    if (error_code == AWS_ERROR_SUCCESS) {
        error_code = s_s3_meta_request_error_code_from_response_status(response_status);

        if (error_code == AWS_ERROR_SUCCESS) {
            if (vtable->stream_complete) {
                /* Call the derived type, having it handle what to do with the current success. */
                if (vtable->stream_complete(stream, vip_connection)) {
                    error_code = aws_last_error();
                }
            }
        } else {
            aws_raise_error(error_code);
        }
    }

    AWS_LOGF_TRACE(
        AWS_LS_S3_META_REQUEST,
        "id=%p: Request %p finished with error code %d and response status %d",
        (void *)meta_request,
        (void *)request,
        error_code,
        response_status);

    if (error_code == AWS_ERROR_SUCCESS) {
        if (request->retry_token != NULL) {
            aws_retry_token_record_success(request->retry_token);
        }
    } else {
        aws_s3_meta_request_handle_error(meta_request, request, error_code);
    }

    if (stream != NULL) {
        aws_http_stream_release(stream);
        stream = NULL;
    }

    aws_s3_request_finished_callback_fn *finished_callback = request->send_data.finished_callback;

    /* Clean up anything held by the vip connection's work_data structure. */
    s_s3_meta_request_clean_up_work_data(vip_connection);

    /* Tell the caller that we finished processing this particular request. */
    if (finished_callback != NULL) {
        finished_callback(vip_connection);
    }
}

static void s_s3_meta_request_retry_ready(struct aws_retry_token *token, int error_code, void *user_data) {
    AWS_PRECONDITION(token);
    (void)token;

    struct aws_s3_request *request = user_data;
    AWS_PRECONDITION(request);

    struct aws_s3_meta_request *meta_request = request->meta_request;
    AWS_PRECONDITION(meta_request);

    /* If we couldn't retry this request, then bail on the entire meta request. */
    if (error_code != AWS_ERROR_SUCCESS) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST,
            "id=%p Meta request could not retry request %p due to error %d (%s)",
            (void *)meta_request,
            (void *)request,
            error_code,
            aws_error_str(error_code));

        aws_s3_meta_request_finish(meta_request, NULL, 0, error_code);
        goto clean_up;
    }

    AWS_LOGF_DEBUG(
        AWS_LS_S3_META_REQUEST,
        "id=%p Meta request pushing request %p with token %p into retry queue",
        (void *)meta_request,
        (void *)request,
        (void *)request->retry_token);

    /* Push the request into the retry queue so that it can actually be retried. */
    aws_s3_meta_request_retry_queue_push(meta_request, request);

    /* Tell the client we have additional work that can be done. */
    aws_s3_meta_request_schedule_work(meta_request);

clean_up:

    aws_s3_request_release(request);
    aws_s3_meta_request_release(meta_request);
}

static void s_s3_meta_request_queue_retry_with_token(
    struct aws_retry_strategy *retry_strategy,
    int error_code,
    struct aws_retry_token *token,
    void *user_data) {

    AWS_PRECONDITION(retry_strategy);
    (void)retry_strategy;

    struct aws_s3_request *request = user_data;
    AWS_PRECONDITION(request);

    struct aws_s3_meta_request *meta_request = request->meta_request;
    AWS_PRECONDITION(meta_request);

    if (error_code != AWS_ERROR_SUCCESS) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST,
            "id=%p Meta request could not get retry token request %p due to error %d (%s)",
            (void *)meta_request,
            (void *)request,
            error_code,
            aws_error_str(error_code));

        goto error_ref_clean_up;
    }

    AWS_ASSERT(token);

    enum aws_retry_error_type error_type = AWS_RETRY_ERROR_TYPE_TRANSIENT;

    if (error_code == AWS_ERROR_S3_NO_PART_BUFFER) {
        error_type = AWS_RETRY_ERROR_TYPE_CLIENT_ERROR;
    } else if (error_code == AWS_ERROR_S3_INTERNAL_ERROR) {
        error_type = AWS_RETRY_ERROR_TYPE_SERVER_ERROR;
    } else if (error_code == AWS_ERROR_S3_SLOW_DOWN) {
        error_type = AWS_RETRY_ERROR_TYPE_THROTTLING;
    } else {
        error_type = AWS_RETRY_ERROR_TYPE_TRANSIENT;
    }

    request->retry_token = token;

    AWS_LOGF_DEBUG(
        AWS_LS_S3_META_REQUEST,
        "id=%p Meta request scheduling retry of request %p with token %p",
        (void *)meta_request,
        (void *)request,
        (void *)token);

    /* Ask the retry strategy to schedule a retry of the request. */
    if (aws_retry_strategy_schedule_retry(request->retry_token, error_type, s_s3_meta_request_retry_ready, request)) {
        error_code = aws_last_error();

        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST,
            "id=%p Meta request could not retry request %p with token %p due to error %d (%s)",
            (void *)meta_request,
            (void *)request,
            (void *)token,
            error_code,
            aws_error_str(error_code));

        goto error_ref_clean_up;
    }

    return;

error_ref_clean_up:

    aws_s3_request_release(request);
    aws_s3_meta_request_release(meta_request);

    aws_s3_meta_request_finish(meta_request, NULL, 0, aws_last_error());
}

static void s_s3_meta_request_acquire_retry_token(
    struct aws_retry_strategy *retry_strategy,
    struct aws_s3_request *request) {
    AWS_PRECONDITION(retry_strategy);
    AWS_PRECONDITION(request);

    struct aws_s3_meta_request *meta_request = request->meta_request;
    AWS_PRECONDITION(meta_request);

    struct aws_http_message *message = meta_request->initial_request_message;
    AWS_PRECONDITION(message);

    struct aws_byte_cursor host_header_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Host");

    struct aws_http_headers *message_headers = aws_http_message_get_headers(message);
    AWS_ASSERT(message_headers);

    struct aws_byte_cursor host_header_value;

    bool host_header_exists =
        aws_http_headers_get(message_headers, host_header_name, &host_header_value) == AWS_ERROR_SUCCESS;
    AWS_ASSERT(host_header_exists);
    (void)host_header_exists;

    /* Try to acquire a token so that we can schedule a retry. */
    if (aws_retry_strategy_acquire_retry_token(
            retry_strategy, &host_header_value, s_s3_meta_request_queue_retry_with_token, request, 0)) {

        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST,
            "id=%p Meta request could not acquire retry token for request %p due to error %d (%s)",
            (void *)request->meta_request,
            (void *)request,
            aws_last_error(),
            aws_error_str(aws_last_error()));

        goto error_ref_clean_up;
    }

    return;

error_ref_clean_up:

    aws_s3_request_release(request);
    aws_s3_meta_request_finish(meta_request, NULL, 0, aws_last_error());
    aws_s3_meta_request_release(meta_request);
}

void aws_s3_meta_request_handle_error(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    int error_code) {
    AWS_PRECONDITION(meta_request);

    AWS_LOGF_ERROR(
        AWS_LS_S3_META_REQUEST,
        "id=%p Meta request handling error %d (%s). (request=%p)",
        (void *)meta_request,
        error_code,
        aws_error_str(error_code),
        (void *)request);

    int response_status = 0;

    if (request != NULL) {
        request->send_data.error_code = error_code;
        response_status = request->send_data.response_status;
    }

    /* If the request is NULL or we have a response that indicates retrying would be futile, then bail on the entire
     * meta request. */
    if (request == NULL || error_code == AWS_ERROR_S3_INVALID_RESPONSE_STATUS) {

        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST,
            "id=%p Meta request cannot recover from error %d (%s). (request=%p, response status=%d)",
            (void *)meta_request,
            error_code,
            aws_error_str(error_code),
            (void *)request,
            response_status);

        aws_s3_meta_request_finish(meta_request, request, response_status, error_code);
        return;
    }

    struct aws_s3_client *client = aws_s3_meta_request_acquire_client(meta_request);

    /* If we were able to get the point of handling an error, the client should still be around. */
    AWS_ASSERT(client);
    AWS_ASSERT(request);
    AWS_ASSERT(request->meta_request == meta_request);

    aws_s3_request_acquire(request);
    aws_s3_meta_request_acquire(meta_request);

    /* If the retry token is NULL, try to grab one, otherwise, just re-use the token from the request's last retry. */
    if (request->retry_token == NULL) {
        s_s3_meta_request_acquire_retry_token(client->retry_strategy, request);
    } else {
        s_s3_meta_request_queue_retry_with_token(
            client->retry_strategy, AWS_ERROR_SUCCESS, request->retry_token, request);
    }

    /* Release the reference to the client that we got with aws_s3_meta_request_get_client. */
    aws_s3_client_release(client);
}

void aws_s3_meta_request_retry_queue_push(struct aws_s3_meta_request *meta_request, struct aws_s3_request *request) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(request->meta_request == meta_request);

    aws_s3_request_acquire(request);

    aws_s3_meta_request_lock_synced_data(meta_request);

    aws_s3_meta_request_release(request->meta_request);
    request->meta_request = NULL;

    aws_linked_list_push_back(&meta_request->synced_data.retry_queue, &request->node);

    aws_s3_meta_request_unlock_synced_data(meta_request);
}

struct aws_s3_request *aws_s3_meta_request_retry_queue_pop_synced(struct aws_s3_meta_request *meta_request) {
    ASSERT_SYNCED_DATA_LOCK_HELD(meta_request);
    AWS_PRECONDITION(meta_request);

    struct aws_s3_request *request = NULL;

    /* Try grabbing a request from the retry queue. */
    if (!aws_linked_list_empty(&meta_request->synced_data.retry_queue)) {
        struct aws_linked_list_node *request_node = aws_linked_list_pop_front(&meta_request->synced_data.retry_queue);

        request = AWS_CONTAINER_OF(request_node, struct aws_s3_request, node);
        AWS_FATAL_ASSERT(request != NULL);

        request->meta_request = meta_request;
        aws_s3_meta_request_acquire(meta_request);
    }

    return request;
}

/* Flag the meta request as finished, immediately triggering any on-finished callbacks. */
void aws_s3_meta_request_finish(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *failed_request,
    int response_status,
    int error_code) {
    AWS_PRECONDITION(meta_request);

    if (s_s3_meta_request_set_state(meta_request, AWS_S3_META_REQUEST_STATE_FINISHED)) {
        return;
    }

    /* Failed requests should only be specified for the AWS_ERROR_S3_INVALID_RESPONSE_STATUS error code. */
    AWS_ASSERT(error_code != AWS_ERROR_S3_INVALID_RESPONSE_STATUS || failed_request != NULL);

    struct aws_s3_client *client = NULL;

    aws_s3_meta_request_lock_synced_data(meta_request);
    client = meta_request->synced_data.client;
    meta_request->synced_data.client = NULL;
    aws_s3_meta_request_unlock_synced_data(meta_request);

    aws_s3_client_release(client);

    AWS_LOGF_INFO(
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

        if (failed_request != NULL) {
            meta_request_result.error_response_headers = failed_request->send_data.response_headers;
            meta_request_result.error_response_body = &failed_request->send_data.response_body_error;
        }

        meta_request->finish_callback(meta_request, &meta_request_result, meta_request->user_data);
    }
}

static int s_s3_meta_request_set_state(struct aws_s3_meta_request *meta_request, enum aws_s3_meta_request_state state) {
    AWS_PRECONDITION(meta_request);

    int result = AWS_OP_SUCCESS;

    aws_s3_meta_request_lock_synced_data(meta_request);

    if (meta_request->synced_data.state == state) {
        result = AWS_OP_ERR;
    }

    meta_request->synced_data.state = state;

    aws_s3_meta_request_unlock_synced_data(meta_request);

    return result;
}

/* Asynchronously queue a part to send back to the caller.  This is for situations such as a parallel get, where we
 * don't necessarily want the handling of that data (which may include file IO) to happen at the same time we're
 * downloading it. */
void aws_s3_meta_request_write_body_to_caller(
    struct aws_s3_request *request,
    aws_s3_meta_request_write_body_finished_callback_fn *callback) {
    AWS_PRECONDITION(request && request->send_data.part_buffer);

    struct aws_s3_meta_request *meta_request = request->meta_request;
    AWS_PRECONDITION(meta_request);

    aws_s3_meta_request_internal_acquire(meta_request);

    aws_s3_request_acquire(request);

    AWS_ASSERT(request->write_body_data.finished_callback == NULL);
    request->write_body_data.finished_callback = callback;

    aws_task_init(
        &request->write_body_data.task,
        s_s3_meta_request_process_write_body_task,
        request,
        "s3_meta_request_process_write_body_task");

    aws_event_loop_schedule_task_now(meta_request->event_loop, &request->write_body_data.task);
}

static void s_s3_meta_request_process_write_body_task(
    struct aws_task *task,
    void *arg,
    enum aws_task_status task_status) {

    (void)task;
    (void)task_status;

    struct aws_s3_request *request = arg;
    AWS_PRECONDITION(request);

    struct aws_s3_part_buffer *part_buffer = request->send_data.part_buffer;
    AWS_PRECONDITION(part_buffer);

    struct aws_s3_meta_request *meta_request = request->meta_request;
    AWS_PRECONDITION(meta_request);

    aws_s3_meta_request_write_body_finished_callback_fn *callback = request->write_body_data.finished_callback;
    request->write_body_data.finished_callback = NULL;

    if (task_status == AWS_TASK_STATUS_CANCELED) {
        goto clean_up;
    }

    AWS_ASSERT(task_status == AWS_TASK_STATUS_RUN_READY);

    /* Send the data to the user through the body callback. */
    if (meta_request->body_callback != NULL) {
        struct aws_byte_cursor buf_byte_cursor = aws_byte_cursor_from_buf(&part_buffer->buffer);

        meta_request->body_callback(meta_request, &buf_byte_cursor, part_buffer->range_start, meta_request->user_data);
    }

clean_up:

    if (task_status == AWS_TASK_STATUS_RUN_READY && callback != NULL) {
        callback(meta_request, request);
    }

    aws_s3_request_release(request);
    aws_s3_meta_request_internal_release(meta_request);
}
