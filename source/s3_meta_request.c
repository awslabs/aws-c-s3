/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_client_impl.h"
#include "aws/s3/private/s3_meta_request_impl.h"
#include "aws/s3/private/s3_util.h"
#include <aws/common/string.h>
#include <aws/io/event_loop.h>
#include <aws/io/stream.h>
#include <inttypes.h>

static void s_s3_meta_request_lock_synced_data(struct aws_s3_meta_request *meta_request);
static void s_s3_meta_request_unlock_synced_data(struct aws_s3_meta_request *meta_request);

static void s_s3_meta_request_start_destroy(void *user_data);
static void s_s3_meta_request_finish_destroy(void *user_data);

static int s_s3_meta_request_set_state(struct aws_s3_meta_request *meta_request, enum aws_s3_meta_request_state state);

static void s_s3_meta_request_process_write_queue_task(
    struct aws_task *task,
    void *arg,
    enum aws_task_status task_status);

static struct aws_s3_send_request_work *s_s3_meta_request_send_request_work_new(
    struct aws_s3_meta_request *meta_request,
    const struct aws_s3_send_request_options *options,
    struct aws_s3_request_desc *request_desc);

static void s_s3_meta_request_send_request_work_destroy(struct aws_s3_send_request_work *work);

static int s_s3_meta_request_process_work(struct aws_s3_send_request_work *work);

static void s_s3_meta_request_request_on_signed(int error_code, void *user_data);
static void s_s3_meta_request_send_http_request(
    struct aws_http_connection *http_connection,
    int error_code,
    void *user_data);

static void s_s3_meta_request_stream_complete(struct aws_http_stream *stream, int error_code, void *user_data);
static void s_s3_meta_request_send_request_work_finish(
    struct aws_s3_send_request_work *work,
    struct aws_http_stream *stream,
    int error_code);

static void s_s3_meta_request_lock_synced_data(struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);

    aws_mutex_lock(&meta_request->synced_data.lock);
}

static void s_s3_meta_request_unlock_synced_data(struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);

    aws_mutex_unlock(&meta_request->synced_data.lock);
}

int aws_s3_meta_request_init_base(
    struct aws_allocator *allocator,
    const struct aws_s3_meta_request_internal_options *internal_options,
    void *impl,
    struct aws_s3_meta_request_vtable *vtable,
    struct aws_s3_meta_request *meta_request) {

    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(internal_options);
    AWS_PRECONDITION(impl);
    AWS_PRECONDITION(internal_options->options);
    AWS_PRECONDITION(internal_options->client);
    AWS_PRECONDITION(meta_request);

    AWS_ZERO_STRUCT(*meta_request);
    meta_request->impl = impl;
    meta_request->vtable = vtable;

    AWS_ASSERT(vtable->next_request);
    AWS_ASSERT(vtable->request_factory);
    AWS_ASSERT(vtable->stream_complete);
    AWS_ASSERT(vtable->destroy);

    const struct aws_s3_meta_request_options *options = internal_options->options;
    AWS_PRECONDITION(options->message);

    meta_request->allocator = allocator;

    /* Set up reference count. */
    aws_ref_count_init(&meta_request->ref_count, meta_request, s_s3_meta_request_start_destroy);
    aws_ref_count_init(&meta_request->internal_ref_count, meta_request, s_s3_meta_request_finish_destroy);

    *((uint64_t *)&meta_request->part_size) = internal_options->client->part_size;
    meta_request->event_loop = internal_options->client->event_loop;

    /* Keep a reference to the original message structure passed in. */
    meta_request->initial_request_message = options->message;
    aws_http_message_acquire(options->message);

    aws_linked_list_init(&meta_request->synced_data.retry_queue);

    /* Store a copy of the original message's initial body stream in our synced data, so that concurrent requests can
     * safely take turns reading from it when needed. */
    meta_request->synced_data.initial_body_stream = aws_http_message_get_body_stream(options->message);

    if (aws_mutex_init(&meta_request->synced_data.lock)) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST, "id=%p Could not initialize mutex for meta request", (void *)meta_request);
        return AWS_OP_ERR;
    }

    meta_request->user_data = options->user_data;
    meta_request->body_callback = options->body_callback;
    meta_request->finish_callback = options->finish_callback;
    meta_request->shutdown_callback = options->shutdown_callback;

    meta_request->internal_user_data = internal_options->user_data;
    meta_request->internal_work_available_callback = internal_options->work_available_callback;
    meta_request->internal_finish_callback = internal_options->finish_callback;

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

    /* Clean up our initial http message */
    if (meta_request->initial_request_message != NULL) {
        aws_http_message_release(meta_request->initial_request_message);
        meta_request->initial_request_message = NULL;
    }

    void *meta_request_user_data = meta_request->user_data;
    aws_s3_meta_request_shutdown_fn *shutdown_callback = meta_request->shutdown_callback;

    aws_mutex_clean_up(&meta_request->synced_data.lock);

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

struct aws_s3_request_desc *aws_s3_request_desc_new(
    struct aws_s3_meta_request *meta_request,
    int request_tag,
    uint32_t part_number) {
    AWS_PRECONDITION(meta_request);

    struct aws_s3_request_desc *desc = aws_mem_calloc(meta_request->allocator, 1, sizeof(struct aws_s3_request_desc));

    desc->request_tag = request_tag;
    desc->part_number = part_number;

    return desc;
}

void aws_s3_request_desc_destroy(struct aws_s3_meta_request *meta_request, struct aws_s3_request_desc *request_desc) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(request_desc);

    aws_mem_release(meta_request->allocator, request_desc);
}

struct aws_s3_request *aws_s3_request_new(struct aws_s3_meta_request *meta_request, struct aws_http_message *message) {

    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(meta_request->allocator);
    AWS_PRECONDITION(message);

    struct aws_s3_request *request = aws_mem_calloc(meta_request->allocator, 1, sizeof(struct aws_s3_request));

    request->message = message;
    aws_http_message_acquire(message);

    return request;
}

void aws_s3_request_destroy(struct aws_s3_meta_request *meta_request, struct aws_s3_request *request) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(meta_request->allocator);
    AWS_PRECONDITION(meta_request->initial_request_message);
    AWS_PRECONDITION(request);

    if (request->message != NULL) {
        struct aws_input_stream *input_stream = aws_http_message_get_body_stream(request->message);
        struct aws_input_stream *initial_input_stream = NULL;

        initial_input_stream = aws_http_message_get_body_stream(meta_request->initial_request_message);

        if (input_stream != initial_input_stream) {
            aws_input_stream_destroy(input_stream);
            input_stream = NULL;
            aws_http_message_set_body_stream(request->message, NULL);
        }

        aws_http_message_release(request->message);
        request->message = NULL;
    }

    aws_s3_part_buffer_release(request->part_buffer);
    aws_mem_release(meta_request->allocator, request);
}

static struct aws_s3_send_request_work *s_s3_meta_request_send_request_work_new(
    struct aws_s3_meta_request *meta_request,
    const struct aws_s3_send_request_options *options,
    struct aws_s3_request_desc *request_desc) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(options);
    AWS_PRECONDITION(options->vip_connection);
    AWS_PRECONDITION(request_desc);

    struct aws_s3_send_request_work *work =
        aws_mem_calloc(meta_request->allocator, 1, sizeof(struct aws_s3_send_request_work));

    work->vip_connection = options->vip_connection;

    work->meta_request = meta_request;
    aws_s3_meta_request_internal_acquire(meta_request);
    aws_s3_meta_request_acquire(meta_request);

    work->request_desc = request_desc;
    work->finished_callback = options->finished_callback;
    work->user_data = options->user_data;

    return work;
}

static void s_s3_meta_request_send_request_work_destroy(struct aws_s3_send_request_work *work) {

    if(work == NULL) {
        return;
    }

    AWS_PRECONDITION(work->meta_request);
    AWS_PRECONDITION(work->vip_connection);

    struct aws_s3_meta_request *meta_request = work->meta_request;

    aws_s3_request_destroy(work->meta_request, work->request);
    work->request = NULL;

    aws_s3_request_desc_destroy(meta_request, work->request_desc);
    work->request_desc = NULL;

    aws_mem_release(meta_request->allocator, work);

    aws_s3_meta_request_internal_release(meta_request);
    aws_s3_meta_request_release(meta_request);
}

void aws_s3_meta_request_notify_work_available(struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);

    if (meta_request->internal_work_available_callback) {
        meta_request->internal_work_available_callback(meta_request, meta_request->internal_user_data);
    }
}

bool aws_s3_meta_request_has_work(const struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);

    s_s3_meta_request_lock_synced_data((struct aws_s3_meta_request *)meta_request);
    if (meta_request->synced_data.state == AWS_S3_META_REQUEST_STATE_FINISHED) {
        s_s3_meta_request_unlock_synced_data((struct aws_s3_meta_request *)meta_request);
        return false;
    }

    if (!aws_linked_list_empty(&meta_request->synced_data.retry_queue)) {
        s_s3_meta_request_unlock_synced_data((struct aws_s3_meta_request *)meta_request);
        return true;
    }

    s_s3_meta_request_unlock_synced_data((struct aws_s3_meta_request *)meta_request);

    struct aws_s3_meta_request_vtable *vtable = meta_request->vtable;
    AWS_FATAL_ASSERT(vtable);

    return vtable->has_work(meta_request);
}

void aws_s3_meta_request_send_next_request(
    struct aws_s3_meta_request *meta_request,
    const struct aws_s3_send_request_options *options) {

    AWS_PRECONDITION(meta_request);

    struct aws_s3_request_desc *request_desc = NULL;

    s_s3_meta_request_lock_synced_data(meta_request);

    /* If the meta request has been finished, don't initiate any additional work. */
    if (meta_request->synced_data.state == AWS_S3_META_REQUEST_STATE_FINISHED) {
        s_s3_meta_request_unlock_synced_data(meta_request);

        goto call_finished_callback;
    }

    /* Try grabbing a request_desc from the retry queue. */
    if (!aws_linked_list_empty(&meta_request->synced_data.retry_queue)) {
        struct aws_linked_list_node *request_desc_node =
            aws_linked_list_pop_front(&meta_request->synced_data.retry_queue);

        request_desc = AWS_CONTAINER_OF(request_desc_node, struct aws_s3_request_desc, node);
        AWS_FATAL_ASSERT(request_desc != NULL);
    }

    s_s3_meta_request_unlock_synced_data(meta_request);

    /* If we didn't find something in the retry queue, call the derived request type, asking what the next request
     * should be. */
    if (request_desc == NULL) {
        struct aws_s3_meta_request_vtable *vtable = meta_request->vtable;
        AWS_FATAL_ASSERT(vtable);

        if (vtable->next_request(meta_request, &request_desc)) {
            goto error_finish;
        }
    }

    /* If it returned NULL, return success.  This is valid in cases where the meta request is waiting on a particular
     * request finishing before it can continue.  (ie: a create-multipart-upload, or an initial ranged get to discover
     * file size, etc.) */
    if (request_desc == NULL) {
        goto call_finished_callback;
    }

    AWS_LOGF_TRACE(
        AWS_LS_S3_META_REQUEST,
        "id=%p Initiating send for request with request tag %d, request desc %p",
        (void *)meta_request,
        request_desc->request_tag,
        (void *)request_desc);

    /* Allocate a work structure that we will keep alive for the duration of processing this request. */
    struct aws_s3_send_request_work *work =
        s_s3_meta_request_send_request_work_new(meta_request, options, request_desc);

    if (work == NULL) {
        AWS_LOGF_ERROR(AWS_LS_S3_META_REQUEST, "id=%p Could not allocate work for meta request.", (void *)meta_request);
        goto error_finish;
    }

    /* Kick off processing the actual work for this request. */
    if (s_s3_meta_request_process_work(work)) {
        goto error_process_work_failed;
    }

    return;

error_process_work_failed:

    s_s3_meta_request_send_request_work_destroy(work);
    work = NULL;

error_finish:

    aws_s3_meta_request_finish(meta_request, aws_last_error());

call_finished_callback:

    if (options->finished_callback != NULL) {
        options->finished_callback(options->user_data);
    }
}

static int s_s3_meta_request_process_work(struct aws_s3_send_request_work *work) {
    AWS_PRECONDITION(work);

    AWS_PRECONDITION(work->meta_request);
    AWS_PRECONDITION(work->request_desc);

    AWS_LOGF_TRACE(
        AWS_LS_S3_META_REQUEST,
        "id=%p Creating request with tag %d, request desc %p",
        (void *)work->meta_request,
        work->request_desc->request_tag,
        (void *)work->request_desc);

    struct aws_s3_meta_request_vtable *vtable = work->meta_request->vtable;
    AWS_PRECONDITION(vtable);

    struct aws_s3_vip_connection *vip_connection = work->vip_connection;
    struct aws_s3_vip *vip = vip_connection->owning_vip;
    struct aws_s3_client *client = vip->owning_client;

    /* Ask the factory to create a new request based on the request description. */
    work->request = vtable->request_factory(work->meta_request, client, work->request_desc);

    /* Finish the work for this request immediately if we couldn't allocate any work for it. */
    if (work->request == NULL) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST,
            "id=%p Could not create request with tag %d, request desc %p",
            (void *)work->meta_request,
            work->request_desc->request_tag,
            (void *)work->request_desc);

        return AWS_OP_ERR;
    }

    AWS_FATAL_ASSERT(work->request->message != NULL);

    AWS_LOGF_TRACE(
        AWS_LS_S3_META_REQUEST, "id=%p Signing request %p", (void *)work->meta_request, (void *)work->request);

    /* Sign the newly created message. */
    if (aws_s3_client_sign_message(client, work->request->message, s_s3_meta_request_request_on_signed, work)) {
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}

/* Handle the signing result, getting an HTTP connection for the request if signing succeeded. */
static void s_s3_meta_request_request_on_signed(int error_code, void *user_data) {
    AWS_PRECONDITION(user_data);

    struct aws_s3_send_request_work *work = user_data;

    AWS_PRECONDITION(work);
    AWS_PRECONDITION(work->meta_request);
    AWS_PRECONDITION(work->request);
    AWS_PRECONDITION(work->vip_connection);

    /* If we couldn't sign, finish the request with an error. */
    if (error_code != AWS_ERROR_SUCCESS) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST,
            "id=%p Unable to sign request %p",
            (void *)work->meta_request,
            (void *)work->request);

        s_s3_meta_request_send_request_work_finish(work, NULL, error_code);
        return;
    }

    AWS_LOGF_TRACE(
        AWS_LS_S3_META_REQUEST,
        "id=%p Getting HTTP connection for request %p",
        (void *)work->meta_request,
        (void *)work->request);

    struct aws_s3_vip_connection *vip_connection = work->vip_connection;
    struct aws_s3_vip *vip = vip_connection->owning_vip;
    struct aws_s3_client *client = vip->owning_client;

    /* Get a connection for this request, finishing if we failed immediately trying to get the connection. */
    if (aws_s3_client_get_http_connection(client, work->vip_connection, s_s3_meta_request_send_http_request, work)) {

        s_s3_meta_request_send_request_work_finish(work, NULL, aws_last_error());
        return;
    }
}

/* Set up a stream to actually process the request. */
static void s_s3_meta_request_send_http_request(
    struct aws_http_connection *http_connection,
    int error_code,
    void *user_data) {

    struct aws_s3_send_request_work *work = user_data;
    AWS_PRECONDITION(work);
    AWS_PRECONDITION(work->meta_request);
    AWS_PRECONDITION(work->request);

    if (error_code != AWS_ERROR_SUCCESS) {
        s_s3_meta_request_send_request_work_finish(work, NULL, error_code);
        return;
    }

    struct aws_s3_meta_request_vtable *vtable = work->meta_request->vtable;
    AWS_FATAL_ASSERT(vtable);

    /* Now that we have a signed request and a connection, go ahead and issue the request. */
    struct aws_http_make_request_options options;
    AWS_ZERO_STRUCT(options);
    options.self_size = sizeof(struct aws_http_make_request_options);
    options.request = work->request->message;
    options.user_data = work;
    options.on_response_headers = vtable->incoming_headers;
    options.on_response_header_block_done = vtable->incoming_headers_block_done;
    options.on_response_body = vtable->incoming_body;
    options.on_complete = s_s3_meta_request_stream_complete;

    struct aws_http_stream *stream = aws_http_connection_make_request(http_connection, &options);

    if (stream == NULL) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST,
            "id=%p: Could not make HTTP request %p",
            (void *)work->meta_request,
            (void *)work->request);

        s_s3_meta_request_send_request_work_finish(work, NULL, aws_last_error());
        return;
    }

    AWS_LOGF_TRACE(
        AWS_LS_S3_META_REQUEST, "id=%p: Sending request %p", (void *)work->meta_request, (void *)work->request);

    if (aws_http_stream_activate(stream) != AWS_OP_SUCCESS) {
        aws_http_stream_release(stream);
        stream = NULL;

        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST,
            "id=%p: Could not activate HTTP stream %p",
            (void *)work->meta_request,
            (void *)work->request);

        s_s3_meta_request_send_request_work_finish(work, NULL, aws_last_error());
        return;
    }
}

/* Finish up the processing of the request work. */
static void s_s3_meta_request_stream_complete(struct aws_http_stream *stream, int error_code, void *user_data) {
    (void)stream;

    struct aws_s3_send_request_work *work = user_data;
    AWS_PRECONDITION(work);

    s_s3_meta_request_send_request_work_finish(work, stream, error_code);
}

static void s_s3_meta_request_send_request_work_finish(
    struct aws_s3_send_request_work *work,
    struct aws_http_stream *stream,
    int error_code) {
    AWS_PRECONDITION(work);
    AWS_PRECONDITION(work->meta_request);
    AWS_PRECONDITION(work->request_desc);

    struct aws_s3_meta_request *meta_request = work->meta_request;

    struct aws_s3_meta_request_vtable *vtable = meta_request->vtable;
    AWS_PRECONDITION(vtable);
    AWS_PRECONDITION(vtable->stream_complete);

    AWS_LOGF_TRACE(
        AWS_LS_S3_META_REQUEST,
        "id=%p: Sending of request desc %p finished with error code %d",
        (void *)work->meta_request,
        (void *)work->request_desc,
        error_code);

    int response_status = 0;

    /* If there isn't already an error code, look for other failures.*/
    if (error_code == AWS_ERROR_SUCCESS) {
        if (stream == NULL) {
            error_code = AWS_ERROR_UNKNOWN;
            aws_raise_error(error_code);
        } else if (aws_http_stream_get_incoming_response_status(stream, &response_status)) {
            error_code = aws_last_error();
        } else if (
            response_status == AWS_S3_RESPONSE_STATUS_SUCCESS ||
            response_status == AWS_S3_RESPONSE_STATUS_RANGE_SUCCESS) {
            error_code = AWS_ERROR_SUCCESS;
        } else if (response_status == AWS_S3_RESPONSE_STATUS_INTERNAL_ERROR) {
            error_code = AWS_ERROR_S3_INTERNAL_ERROR;
        } else {
            error_code = AWS_ERROR_S3_INVALID_RESPONSE_STATUS;
            AWS_LOGF_ERROR(
                AWS_LS_S3_META_REQUEST,
                "id=%p Meta request received response status %d for request with tag %d",
                (void *)work->meta_request,
                response_status,
                work->request_desc->request_tag);
        }
    }

    /* Call the derived type, having it handle what to do with the current failure or success. */
    vtable->stream_complete(stream, error_code, work);

    if (stream != NULL) {
        aws_http_stream_release(stream);
        stream = NULL;
    }

    /* Tell the caller that we finished processing this particular request. */
    if (work->finished_callback != NULL) {
        work->finished_callback(work->user_data);
    }

    /* Release our work structure. */
    s_s3_meta_request_send_request_work_destroy(work);
}

/* Push a request description into the retry queue.  This assumes ownership of the request desc, and will NULL out the
 * passed in pointer-to-pointer to help enforce this. */
int aws_s3_meta_request_queue_retry(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request_desc **in_out_desc) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(in_out_desc && *in_out_desc);

    s_s3_meta_request_lock_synced_data(meta_request);

    aws_linked_list_push_back(&meta_request->synced_data.retry_queue, &(*in_out_desc)->node);
    *in_out_desc = NULL;

    s_s3_meta_request_unlock_synced_data(meta_request);

    return AWS_OP_SUCCESS;
}

/* Flag the meta request as finished, immediately triggering any on-finished callbacks. */
void aws_s3_meta_request_finish(struct aws_s3_meta_request *meta_request, int error_code) {
    AWS_PRECONDITION(meta_request);

    if (s_s3_meta_request_set_state(meta_request, AWS_S3_META_REQUEST_STATE_FINISHED)) {
        return;
    }

    AWS_LOGF_INFO(
        AWS_LS_S3_META_REQUEST,
        "id=%p Meta request finished with error code %d (%s)",
        (void *)meta_request,
        error_code,
        aws_error_str(error_code));

    if (meta_request->finish_callback != NULL) {
        meta_request->finish_callback(meta_request, error_code, meta_request->user_data);
    }

    if (meta_request->internal_finish_callback != NULL) {
        meta_request->internal_finish_callback(meta_request, error_code, meta_request->internal_user_data);
    }
}

static int s_s3_meta_request_set_state(struct aws_s3_meta_request *meta_request, enum aws_s3_meta_request_state state) {
    AWS_PRECONDITION(meta_request);

    s_s3_meta_request_lock_synced_data(meta_request);

    if (meta_request->synced_data.state == state) {
        s_s3_meta_request_unlock_synced_data(meta_request);
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}

/* Asynchronously queue a part to send back to the caller.  This is for situations such as a parallel get, where we
 * don't necessarily want the handling of that data (which may include file IO) to happen at the same time we're
 * downloading it. */
int aws_s3_meta_request_write_part_buffer_to_caller(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_part_buffer **in_out_part_buffer,
    aws_write_part_buffer_callback_fn *callback,
    void *user_data) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(in_out_part_buffer && *in_out_part_buffer);

    struct aws_s3_part_buffer *part_buffer = *in_out_part_buffer;

    aws_s3_meta_request_internal_acquire(meta_request);
    part_buffer->write_part_buffer_data.meta_request = meta_request;
    part_buffer->write_part_buffer_data.write_part_buffer_callback = callback;
    part_buffer->write_part_buffer_data.user_data = user_data;

    aws_task_init(
        &part_buffer->write_part_buffer_data.task,
        s_s3_meta_request_process_write_queue_task,
        part_buffer,
        "s3_write_part_buffer_to_caller");

    aws_event_loop_schedule_task_now(meta_request->event_loop, &part_buffer->write_part_buffer_data.task);

    *in_out_part_buffer = NULL;

    return AWS_OP_SUCCESS;
}

static void s_s3_meta_request_process_write_queue_task(
    struct aws_task *task,
    void *arg,
    enum aws_task_status task_status) {

    AWS_ASSERT(task_status == AWS_TASK_STATUS_RUN_READY);

    (void)task;
    (void)task_status;

    struct aws_s3_part_buffer *part_buffer = arg;
    AWS_PRECONDITION(part_buffer);

    struct aws_s3_meta_request *meta_request = part_buffer->write_part_buffer_data.meta_request;
    AWS_PRECONDITION(meta_request);

    aws_write_part_buffer_callback_fn *callback = part_buffer->write_part_buffer_data.write_part_buffer_callback;
    void *callback_user_data = part_buffer->write_part_buffer_data.user_data;

    /* Send the data to the user through the body callback. */
    if (meta_request->body_callback != NULL) {
        struct aws_byte_cursor buf_byte_cursor = aws_byte_cursor_from_buf(&part_buffer->buffer);

        meta_request->body_callback(
            meta_request, &buf_byte_cursor, part_buffer->range_start, part_buffer->range_end, meta_request->user_data);
    }

    part_buffer->write_part_buffer_data.meta_request = NULL;
    part_buffer->write_part_buffer_data.write_part_buffer_callback = NULL;
    part_buffer->write_part_buffer_data.user_data = NULL;
    aws_s3_meta_request_internal_release(meta_request);

    /* Release the part buffer back to the client. */
    aws_s3_part_buffer_release(part_buffer);

    if (callback != NULL) {
        callback(callback_user_data);
    }
}
