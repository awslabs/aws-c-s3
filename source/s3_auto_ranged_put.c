
#include "aws/s3/private/s3_auto_ranged_put.h"
#include "aws/s3/private/s3_request_messages.h"
#include <aws/common/string.h>
#include <aws/io/stream.h>

static const size_t s_etags_initial_capacity = 16;
static const struct aws_byte_cursor s_upload_id = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("UploadId");
static const size_t s_complete_multipart_upload_init_body_size_bytes = 512;

static const struct aws_byte_cursor s_create_multipart_upload_copy_headers[] = {
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-server-side-encryption-customer-algorithm"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-server-side-encryption-customer-key-MD5"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-server-side-encryption-context"),
};

static void s_s3_auto_ranged_put_lock_synced_data(struct aws_s3_auto_ranged_put *auto_ranged_put);
static void s_s3_auto_ranged_put_unlock_synced_data(struct aws_s3_auto_ranged_put *auto_ranged_put);

static void s_s3_auto_ranged_put_cancel_finished(struct aws_s3_meta_request *meta_request);

static void s_s3_meta_request_auto_ranged_put_destroy(struct aws_s3_meta_request *meta_request);

static int s_s3_auto_ranged_put_next_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request **out_request);

static int s_s3_auto_ranged_put_prepare_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_client *client,
    struct aws_s3_vip_connection *vip_connection,
    bool is_initial_prepare);

static int s_s3_auto_ranged_put_header_block_done(
    struct aws_http_stream *stream,
    enum aws_http_header_block header_block,
    struct aws_s3_vip_connection *vip_connection);

static int s_s3_auto_ranged_put_stream_complete(
    struct aws_http_stream *stream,
    struct aws_s3_vip_connection *vip_connection);

static void s_s3_auto_ranged_put_finish(
    struct aws_s3_meta_request *meta_request,
    const struct aws_s3_meta_request_finish_options *finish_options);

static void s_s3_auto_ranged_put_notify_request_destroyed(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request);

static struct aws_s3_meta_request_vtable s_s3_auto_ranged_put_vtable = {
    .next_request = s_s3_auto_ranged_put_next_request,
    .send_request_finish = aws_s3_meta_request_send_request_finish_default,
    .prepare_request = s_s3_auto_ranged_put_prepare_request,
    .init_signing_date_time = aws_s3_meta_request_init_signing_date_time_default,
    .sign_request = aws_s3_meta_request_sign_request_default,
    .incoming_headers = NULL,
    .incoming_headers_block_done = s_s3_auto_ranged_put_header_block_done,
    .incoming_body = NULL,
    .stream_complete = s_s3_auto_ranged_put_stream_complete,
    .notify_request_destroyed = s_s3_auto_ranged_put_notify_request_destroyed,
    .destroy = s_s3_meta_request_auto_ranged_put_destroy,
    .finish = s_s3_auto_ranged_put_finish,
};

static void s_s3_auto_ranged_put_lock_synced_data(struct aws_s3_auto_ranged_put *auto_ranged_put) {
    AWS_PRECONDITION(auto_ranged_put);

    aws_mutex_lock(&auto_ranged_put->base.synced_data.lock);
}

static void s_s3_auto_ranged_put_unlock_synced_data(struct aws_s3_auto_ranged_put *auto_ranged_put) {
    AWS_PRECONDITION(auto_ranged_put);

    aws_mutex_unlock(&auto_ranged_put->base.synced_data.lock);
}

static struct aws_s3_meta_request_finish_options *s_copy_finish_options(
    struct aws_allocator *allocator,
    const struct aws_s3_meta_request_finish_options *options) {
    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(options);

    struct aws_s3_meta_request_finish_options *options_copy =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_meta_request_finish_options));

    options_copy->error_code = options->error_code;
    options_copy->response_status = options->response_status;

    if (options->error_response_headers != NULL) {
        options_copy->error_response_headers = options->error_response_headers;
        aws_http_headers_acquire(options->error_response_headers);
    }

    if (options->error_response_body != NULL) {
        options_copy->error_response_body = aws_mem_calloc(allocator, 1, sizeof(struct aws_byte_buf));
        aws_byte_buf_init_copy(options_copy->error_response_body, allocator, options->error_response_body);
    }

    return options_copy;
}

static void s_destroy_finish_options_copy(
    struct aws_allocator *allocator,
    struct aws_s3_meta_request_finish_options *options) {
    AWS_PRECONDITION(allocator);

    if (options == NULL) {
        return;
    }
    aws_http_headers_release(options->error_response_headers);

    if (options->error_response_body != NULL) {
        aws_byte_buf_clean_up(options->error_response_body);
        aws_mem_release(allocator, options->error_response_body);
    }

    aws_mem_release(allocator, options);
}

/* Allocate a new auto-ranged put meta request */
struct aws_s3_meta_request *aws_s3_meta_request_auto_ranged_put_new(
    struct aws_allocator *allocator,
    struct aws_s3_client *client,
    size_t part_size,
    uint32_t num_parts,
    const struct aws_s3_meta_request_options *options) {

    /* These should already have been validated by the caller. */
    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(options);
    AWS_PRECONDITION(options->message);
    AWS_PRECONDITION(aws_http_message_get_body_stream(options->message));

    struct aws_s3_auto_ranged_put *auto_ranged_put =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_auto_ranged_put));

    if (aws_s3_meta_request_init_base(
            allocator,
            client,
            part_size,
            options,
            auto_ranged_put,
            &s_s3_auto_ranged_put_vtable,
            &auto_ranged_put->base)) {
        goto error_clean_up;
    }

    if (aws_array_list_init_dynamic(
            &auto_ranged_put->synced_data.etag_list,
            allocator,
            s_etags_initial_capacity,
            sizeof(struct aws_string *))) {
        goto error_clean_up;
    }

    auto_ranged_put->synced_data.total_num_parts = num_parts;
    auto_ranged_put->threaded_next_request_data.next_part_number = 1;

    AWS_LOGF_DEBUG(
        AWS_LS_S3_META_REQUEST, "id=%p Created new Auto-Ranged Put Meta Request.", (void *)&auto_ranged_put->base);

    return &auto_ranged_put->base;

error_clean_up:

    aws_mem_release(allocator, auto_ranged_put);
    return NULL;
}

/* Destroy our auto-ranged put meta request */
static void s_s3_meta_request_auto_ranged_put_destroy(struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(meta_request->impl);

    struct aws_s3_auto_ranged_put *auto_ranged_put = meta_request->impl;

    aws_string_destroy(auto_ranged_put->upload_id);
    auto_ranged_put->upload_id = NULL;

    for (size_t etag_index = 0; etag_index < aws_array_list_length(&auto_ranged_put->synced_data.etag_list);
         ++etag_index) {
        struct aws_string *etag = NULL;

        aws_array_list_get_at(&auto_ranged_put->synced_data.etag_list, &etag, etag_index);
        aws_string_destroy(etag);
    }

    aws_array_list_clean_up(&auto_ranged_put->synced_data.etag_list);
    aws_http_headers_release(auto_ranged_put->synced_data.needed_response_headers);
    s_destroy_finish_options_copy(meta_request->allocator, auto_ranged_put->synced_data.cached_finish_options);

    aws_mem_release(meta_request->allocator, auto_ranged_put);
}

static void s_s3_auto_ranged_put_finish(
    struct aws_s3_meta_request *meta_request,
    const struct aws_s3_meta_request_finish_options *options) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(options);

    struct aws_s3_auto_ranged_put *auto_ranged_put = meta_request->impl;
    AWS_PRECONDITION(auto_ranged_put);

    bool not_active = false;
    bool cancelling = false;

    aws_s3_meta_request_lock_synced_data(meta_request);

    if (meta_request->synced_data.state != AWS_S3_META_REQUEST_STATE_ACTIVE) {
        not_active = true;
        goto unlock;
    }

    /* If this is a successful finish, then don't try to cancel. */
    if (options->error_code == AWS_ERROR_SUCCESS) {
        goto unlock;
    }

    /* If the complete message has already been sent, then cancelling is not possible. */
    if (auto_ranged_put->synced_data.state == AWS_S3_AUTO_RANGED_PUT_STATE_WAITING_FOR_COMPLETE) {
        goto unlock;
    }

    meta_request->synced_data.state = AWS_S3_META_REQUEST_STATE_CANCELING;

    AWS_ASSERT(auto_ranged_put->synced_data.cached_finish_options == NULL);
    auto_ranged_put->synced_data.cached_finish_options = s_copy_finish_options(meta_request->allocator, options);

    cancelling = true;

unlock:
    aws_s3_meta_request_unlock_synced_data(meta_request);

    /* If meta request is not in the active state, then it's already being cancelled or finished. */
    if (not_active) {
        return;
    }

    /* If cancelling, we may have an abort message to send, so re-push the meta request to the client for processing. */
    if (cancelling) {
        aws_s3_meta_request_push_to_client(meta_request);
    } else {
        aws_s3_meta_request_finish_default(meta_request, options);
    }
}

static void s_s3_auto_ranged_put_cancel_finished(struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);

    struct aws_s3_auto_ranged_put *auto_ranged_put = meta_request->impl;
    AWS_PRECONDITION(auto_ranged_put);

    bool already_finished_canceling = false;
    struct aws_s3_meta_request_finish_options *finish_options = NULL;

    s_s3_auto_ranged_put_lock_synced_data(auto_ranged_put);

    already_finished_canceling = meta_request->synced_data.state == AWS_S3_META_REQUEST_STATE_CANCELED;

    if (already_finished_canceling) {
        AWS_ASSERT(auto_ranged_put->synced_data.cached_finish_options == NULL);
        goto unlock;
    }

    meta_request->synced_data.state = AWS_S3_META_REQUEST_STATE_CANCELED;

    finish_options = auto_ranged_put->synced_data.cached_finish_options;
    auto_ranged_put->synced_data.cached_finish_options = NULL;
    AWS_ASSERT(finish_options != NULL);

unlock:
    s_s3_auto_ranged_put_unlock_synced_data(auto_ranged_put);

    if (!already_finished_canceling) {
        aws_s3_meta_request_finish_default(meta_request, finish_options);

        s_destroy_finish_options_copy(meta_request->allocator, finish_options);
        meta_request->cached_signing_config = NULL;
    }
}

static int s_s3_auto_ranged_put_next_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request **out_request) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(out_request);

    struct aws_s3_request *request = NULL;
    struct aws_s3_auto_ranged_put *auto_ranged_put = meta_request->impl;

    bool finish_canceling = false;

    s_s3_auto_ranged_put_lock_synced_data(auto_ranged_put);
    bool canceling = meta_request->synced_data.state == AWS_S3_META_REQUEST_STATE_CANCELING;

    switch (auto_ranged_put->synced_data.state) {
        case AWS_S3_AUTO_RANGED_PUT_STATE_START: {

            if (canceling) {
                /* If we are canceling, then at this point, we haven't sent anything yet, so go ahead and finish
                 * canceling. */
                finish_canceling = true;
            } else {
                /* Setup for a create-multipart upload */
                request = aws_s3_request_new(
                    meta_request,
                    AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_CREATE_MULTIPART_UPLOAD,
                    0,
                    AWS_S3_REQUEST_DESC_RECORD_RESPONSE_HEADERS);

                /* We'll need to wait for the initial create to get back so that we can get the upload-id. */
                auto_ranged_put->synced_data.state = AWS_S3_AUTO_RANGED_PUT_STATE_WAITING_FOR_CREATE;
            }

            break;
        }
        case AWS_S3_AUTO_RANGED_PUT_STATE_WAITING_FOR_CREATE: {
            break;
        }
        case AWS_S3_AUTO_RANGED_PUT_STATE_SENDING_PARTS: {

            if (canceling) {
                if (!auto_ranged_put->synced_data.create_multipart_upload_successful) {
                    finish_canceling = true;
                } else if (
                    auto_ranged_put->synced_data.num_parts_completed == auto_ranged_put->synced_data.num_parts_sent) {

                    request = aws_s3_request_new(
                        meta_request,
                        AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_ABORT_MULTIPART_UPLOAD,
                        0,
                        AWS_S3_REQUEST_DESC_RECORD_RESPONSE_HEADERS);
                    auto_ranged_put->synced_data.state = AWS_S3_AUTO_RANGED_PUT_STATE_WAITING_FOR_CANCEL;
                } else {
                    auto_ranged_put->synced_data.state = AWS_S3_AUTO_RANGED_PUT_STATE_WAITING_FOR_PARTS;
                }

            } else if (auto_ranged_put->synced_data.num_parts_sent < auto_ranged_put->synced_data.total_num_parts) {
                /* Keep setting up to send parts until we've sent all of them at least once. */
                request = aws_s3_request_new(
                    meta_request,
                    AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_PART,
                    0,
                    AWS_S3_REQUEST_DESC_RECORD_RESPONSE_HEADERS);

                aws_byte_buf_init(&request->request_body, meta_request->allocator, meta_request->part_size);
                request->part_number = auto_ranged_put->threaded_next_request_data.next_part_number;

                ++auto_ranged_put->threaded_next_request_data.next_part_number;
            }

            break;
        }
        case AWS_S3_AUTO_RANGED_PUT_STATE_WAITING_FOR_PARTS: {
            break;
        }
        case AWS_S3_AUTO_RANGED_PUT_STATE_SEND_COMPLETE: {
            if (canceling) {
                request = aws_s3_request_new(
                    meta_request,
                    AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_ABORT_MULTIPART_UPLOAD,
                    0,
                    AWS_S3_REQUEST_DESC_RECORD_RESPONSE_HEADERS);
                auto_ranged_put->synced_data.state = AWS_S3_AUTO_RANGED_PUT_STATE_WAITING_FOR_CANCEL;

            } else {
                /* If all parts have been completed, set up to send a complete-multipart-upload request. */
                request = aws_s3_request_new(
                    meta_request,
                    AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_COMPLETE_MULTIPART_UPLOAD,
                    0,
                    AWS_S3_REQUEST_DESC_RECORD_RESPONSE_HEADERS);

                auto_ranged_put->synced_data.state = AWS_S3_AUTO_RANGED_PUT_STATE_WAITING_FOR_COMPLETE;

                aws_byte_buf_init(
                    &request->request_body, meta_request->allocator, s_complete_multipart_upload_init_body_size_bytes);
            }
            break;
        }
        case AWS_S3_AUTO_RANGED_PUT_STATE_WAITING_FOR_COMPLETE: {
            break;
        }
        case AWS_S3_AUTO_RANGED_PUT_STATE_WAITING_FOR_CANCEL:
            break;

        default:
            AWS_FATAL_ASSERT(false);
            break;
    }

    s_s3_auto_ranged_put_unlock_synced_data(auto_ranged_put);

    if (finish_canceling) {
        AWS_ASSERT(request == NULL);
        s_s3_auto_ranged_put_cancel_finished(meta_request);
        return AWS_OP_SUCCESS;
    }

    if (request != NULL && request->request_tag == AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_PART) {

        if (aws_s3_meta_request_read_body(meta_request, &request->request_body)) {
            aws_s3_request_release(request);
            return AWS_OP_ERR;
        }

        bool no_longer_active = false;

        /* Now we know that we're going to return the request, increment our counter that it has been sent.*/
        /* TODO having to do this active state check here is awkward. Basically, we need to cover the case that failure
         * happened in between reading the request and needing to increment the synced_data.num_parts_sent variable. */
        s_s3_auto_ranged_put_lock_synced_data(auto_ranged_put);
        if (meta_request->synced_data.state == AWS_S3_META_REQUEST_STATE_ACTIVE) {
            ++auto_ranged_put->synced_data.num_parts_sent;
        } else {
            no_longer_active = true;
        }
        s_s3_auto_ranged_put_unlock_synced_data(auto_ranged_put);

        if (no_longer_active) {
            aws_s3_request_release(request);
            return AWS_OP_SUCCESS;
        }

        AWS_LOGF_DEBUG(
            AWS_LS_S3_META_REQUEST,
            "id=%p: Returning request %p for part %d",
            (void *)meta_request,
            (void *)request,
            request->part_number);
    }

    *out_request = request;
    return AWS_OP_SUCCESS;
}

/* Given a request, prepare it for sending based on its description. */
static int s_s3_auto_ranged_put_prepare_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_client *client,
    struct aws_s3_vip_connection *vip_connection,
    bool is_initial_prepare) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(client);
    (void)client;
    (void)is_initial_prepare;

    struct aws_s3_request *request = vip_connection->request;
    AWS_PRECONDITION(request);

    struct aws_s3_auto_ranged_put *auto_ranged_put = meta_request->impl;
    AWS_PRECONDITION(auto_ranged_put);

    struct aws_http_message *message = NULL;

    switch (request->request_tag) {

        case AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_PART: {
            /* Create a new put-object message to upload a part. */
            message = aws_s3_put_object_message_new(
                meta_request->allocator,
                meta_request->initial_request_message,
                &request->request_body,
                request->part_number,
                auto_ranged_put->upload_id);

        } break;
        case AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_CREATE_MULTIPART_UPLOAD: {

            /* Create the message to create a new multipart upload. */
            message = aws_s3_create_multipart_upload_message_new(
                meta_request->allocator, meta_request->initial_request_message);

            break;
        }
        case AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_COMPLETE_MULTIPART_UPLOAD: {
            s_s3_auto_ranged_put_lock_synced_data(auto_ranged_put);

            AWS_FATAL_ASSERT(auto_ranged_put->upload_id);
            AWS_ASSERT(request->request_body.capacity > 0);
            aws_byte_buf_reset(&request->request_body, false);

            /* Build the message to complete our multipart upload, which includes a payload describing all of our
             * completed parts. */
            message = aws_s3_complete_multipart_message_new(
                meta_request->allocator,
                meta_request->initial_request_message,
                &request->request_body,
                auto_ranged_put->upload_id,
                &auto_ranged_put->synced_data.etag_list);

            s_s3_auto_ranged_put_unlock_synced_data(auto_ranged_put);

            break;
        }
        case AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_ABORT_MULTIPART_UPLOAD: {
            s_s3_auto_ranged_put_lock_synced_data(auto_ranged_put);

            AWS_FATAL_ASSERT(auto_ranged_put->upload_id);
            AWS_LOGF_DEBUG(
                AWS_LS_S3_META_REQUEST,
                "id=%p Abort multipart upload request for upload id %s.",
                (void *)meta_request,
                aws_string_c_str(auto_ranged_put->upload_id));

            /* Build the message to abort our multipart upload */
            message = aws_s3_abort_multipart_upload_message_new(
                meta_request->allocator, meta_request->initial_request_message, auto_ranged_put->upload_id);

            s_s3_auto_ranged_put_unlock_synced_data(auto_ranged_put);

            break;
        }
    }

    if (message == NULL) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST,
            "id=%p Could not allocate message for request with tag %d for auto-ranged-put meta request.",
            (void *)meta_request,
            request->request_tag);
        goto message_create_failed;
    }

    aws_s3_request_setup_send_data(request, message);

    aws_http_message_release(message);

    AWS_LOGF_DEBUG(
        AWS_LS_S3_META_REQUEST,
        "id=%p: Prepared request %p for part %d",
        (void *)meta_request,
        (void *)request,
        request->part_number);

    return AWS_OP_SUCCESS;

message_create_failed:

    return AWS_OP_ERR;
}

static int s_s3_auto_ranged_put_header_block_done(
    struct aws_http_stream *stream,
    enum aws_http_header_block header_block,
    struct aws_s3_vip_connection *vip_connection) {

    (void)stream;
    (void)header_block;

    AWS_PRECONDITION(stream);

    AWS_PRECONDITION(vip_connection);

    struct aws_s3_request *request = vip_connection->request;
    AWS_PRECONDITION(request);

    struct aws_s3_meta_request *meta_request = request->meta_request;
    AWS_PRECONDITION(meta_request);

    struct aws_s3_auto_ranged_put *auto_ranged_put = meta_request->impl;
    AWS_PRECONDITION(auto_ranged_put);

    struct aws_allocator *allocator = meta_request->allocator;
    AWS_PRECONDITION(allocator);

    if (request->request_tag == AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_CREATE_MULTIPART_UPLOAD) {
        AWS_ASSERT(request->send_data.response_headers);

        struct aws_http_headers *needed_response_headers = aws_http_headers_new(allocator);
        const size_t copy_header_count =
            sizeof(s_create_multipart_upload_copy_headers) / sizeof(struct aws_byte_cursor);

        /* Copy any headers now that we'll need for the final, transformed headers later. */
        for (size_t header_index = 0; header_index < copy_header_count; ++header_index) {
            const struct aws_byte_cursor *header_name = &s_create_multipart_upload_copy_headers[header_index];
            struct aws_byte_cursor header_value;
            AWS_ZERO_STRUCT(header_value);

            if (!aws_http_headers_get(request->send_data.response_headers, *header_name, &header_value)) {
                aws_http_headers_set(needed_response_headers, *header_name, header_value);
            }
        }

        /* Copy those headers into our needed_response_headers. */
        s_s3_auto_ranged_put_lock_synced_data(auto_ranged_put);
        aws_http_headers_release(auto_ranged_put->synced_data.needed_response_headers);
        auto_ranged_put->synced_data.needed_response_headers = needed_response_headers;
        s_s3_auto_ranged_put_unlock_synced_data(auto_ranged_put);

        return AWS_OP_SUCCESS;

    } else if (request->request_tag == AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_PART) {

        size_t part_number = request->part_number;
        AWS_FATAL_ASSERT(part_number > 0);
        size_t part_index = part_number - 1;

        int result = AWS_OP_SUCCESS;

        /* Find the ETag header if it exists and cache it. */
        struct aws_byte_cursor etag_within_quotes;

        AWS_ASSERT(request->send_data.response_headers);

        if (aws_http_headers_get(request->send_data.response_headers, g_etag_header_name, &etag_within_quotes)) {
            AWS_LOGF_ERROR(
                AWS_LS_S3_META_REQUEST,
                "id=%p Could not find ETag header for request %p",
                (void *)meta_request,
                (void *)request);
            aws_raise_error(AWS_ERROR_S3_MISSING_ETAG);
            return AWS_OP_ERR;
        }

        /* The ETag value arrives in quotes, but we don't want it in quotes when we send it back up later, so just
         * get rid of the quotes now. */
        if (etag_within_quotes.len >= 2 && etag_within_quotes.ptr[0] == '"' &&
            etag_within_quotes.ptr[etag_within_quotes.len - 1] == '"') {

            aws_byte_cursor_advance(&etag_within_quotes, 1);
            --etag_within_quotes.len;
        }

        struct aws_string *etag = aws_string_new_from_cursor(allocator, &etag_within_quotes);
        struct aws_string *null_etag = NULL;

        s_s3_auto_ranged_put_lock_synced_data(auto_ranged_put);

        /* ETags need to be associated with their part number, so we keep the etag indices consistent with part
         * numbers. This means we may have to add padding to the list in the case that parts finish out of order. */
        while (aws_array_list_length(&auto_ranged_put->synced_data.etag_list) < part_number) {
            if (aws_array_list_push_back(&auto_ranged_put->synced_data.etag_list, &null_etag)) {
                result = AWS_OP_ERR;
                goto unlock;
            }
        }

        aws_array_list_set_at(&auto_ranged_put->synced_data.etag_list, &etag, part_index);
    unlock:
        s_s3_auto_ranged_put_unlock_synced_data(auto_ranged_put);

        return result;
    } else if (request->request_tag == AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_COMPLETE_MULTIPART_UPLOAD) {
        return AWS_OP_SUCCESS;
    }

    return AWS_OP_SUCCESS;
}

static int s_s3_auto_ranged_put_stream_complete(
    struct aws_http_stream *stream,
    struct aws_s3_vip_connection *vip_connection) {
    AWS_PRECONDITION(stream);
    (void)stream;

    AWS_PRECONDITION(vip_connection);

    struct aws_s3_request *request = vip_connection->request;
    AWS_PRECONDITION(request);

    struct aws_s3_meta_request *meta_request = request->meta_request;
    AWS_PRECONDITION(meta_request);

    struct aws_s3_auto_ranged_put *auto_ranged_put = meta_request->impl;
    AWS_PRECONDITION(auto_ranged_put);

    switch (request->request_tag) {
        case AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_CREATE_MULTIPART_UPLOAD: {
            struct aws_byte_cursor buffer_byte_cursor = aws_byte_cursor_from_buf(&request->send_data.response_body);

            /* Find the upload id for this multipart upload. */
            struct aws_string *upload_id =
                get_top_level_xml_tag_value(meta_request->allocator, &s_upload_id, &buffer_byte_cursor);

            if (upload_id == NULL) {
                AWS_LOGF_ERROR(
                    AWS_LS_S3_META_REQUEST,
                    "id=%p Could not find upload-id in create-multipart-upload response",
                    (void *)meta_request);

                aws_raise_error(AWS_ERROR_S3_MISSING_UPLOAD_ID);
                return AWS_OP_ERR;
            }

            /* Store the multipart upload id and set that we are ready for sending parts. */
            auto_ranged_put->upload_id = upload_id;

            /* Record success of the create multipart upload. Wait until the request cleans up entirely for advancing
             * the state. */
            s_s3_auto_ranged_put_lock_synced_data(auto_ranged_put);
            auto_ranged_put->synced_data.create_multipart_upload_successful = true;
            s_s3_auto_ranged_put_unlock_synced_data(auto_ranged_put);
            break;
        }
        case AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_PART: {
            break;
        }
        case AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_COMPLETE_MULTIPART_UPLOAD: {
            int finish_error_code = AWS_ERROR_SUCCESS;
            if (meta_request->headers_callback != NULL) {
                struct aws_http_headers *final_response_headers = aws_http_headers_new(meta_request->allocator);

                /* Copy all the response headers from this request. */
                copy_http_headers(request->send_data.response_headers, final_response_headers);

                /* Copy over any response headers that we've previously determined are needed for this final response.
                 */
                s_s3_auto_ranged_put_lock_synced_data(auto_ranged_put);
                copy_http_headers(
                    request->send_data.response_headers, auto_ranged_put->synced_data.needed_response_headers);
                s_s3_auto_ranged_put_unlock_synced_data(auto_ranged_put);

                struct aws_byte_cursor response_body_cursor =
                    aws_byte_cursor_from_buf(&request->send_data.response_body);

                /* Grab the ETag for the entire object, and set it as a header. */
                struct aws_string *etag_header_value =
                    get_top_level_xml_tag_value(meta_request->allocator, &g_etag_header_name, &response_body_cursor);

                if (etag_header_value != NULL) {
                    struct aws_byte_buf etag_header_value_byte_buf;
                    AWS_ZERO_STRUCT(etag_header_value_byte_buf);

                    replace_quote_entities(meta_request->allocator, etag_header_value, &etag_header_value_byte_buf);

                    aws_http_headers_set(
                        final_response_headers,
                        g_etag_header_name,
                        aws_byte_cursor_from_buf(&etag_header_value_byte_buf));

                    aws_string_destroy(etag_header_value);
                    aws_byte_buf_clean_up(&etag_header_value_byte_buf);
                }

                /* Notify the user of the headers. */
                if (meta_request->headers_callback(
                        meta_request,
                        final_response_headers,
                        request->send_data.response_status,
                        meta_request->user_data)) {

                    finish_error_code = aws_last_error_or_unknown();
                }

                aws_http_headers_release(final_response_headers);
            }

            aws_s3_meta_request_finish(meta_request, NULL, AWS_S3_RESPONSE_STATUS_SUCCESS, finish_error_code);
            break;
        }
        case AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_ABORT_MULTIPART_UPLOAD: {
            break;
        }
        default:
            AWS_FATAL_ASSERT(false);
    }

    return AWS_OP_SUCCESS;
}

/* TODO: make this callback into a notify_request_finished function, and move all stream complete logic (which currently
 * only happens on success) into here. */
static void s_s3_auto_ranged_put_notify_request_destroyed(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(meta_request->impl);

    (void)request;

    struct aws_s3_auto_ranged_put *auto_ranged_put = meta_request->impl;

    if (!request->request_was_sent) {
        return;
    }

    if (request->request_tag == AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_CREATE_MULTIPART_UPLOAD) {

        /* Any time a create multipart upload request has finished, be it success or failure, advance to the sending
         * parts state, which will immediately cancel if there has been failure with the create. */
        /* TODO branch on the success/failure of the request here and go to a different state that makes this logic more
         * clear.*/
        s_s3_auto_ranged_put_lock_synced_data(auto_ranged_put);
        auto_ranged_put->synced_data.state = AWS_S3_AUTO_RANGED_PUT_STATE_SENDING_PARTS;
        s_s3_auto_ranged_put_unlock_synced_data(auto_ranged_put);

        aws_s3_meta_request_push_to_client(meta_request);

    } else if (request->request_tag == AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_PART) {

        bool notify_work_available = false;

        s_s3_auto_ranged_put_lock_synced_data(auto_ranged_put);
        bool cancelling = meta_request->synced_data.state == AWS_S3_META_REQUEST_STATE_CANCELING;

        /* TODO This part is confusing/unclear and should be slightly refactored. This function can get called on
         * success OR failure of the request.  This really only works because we're currently assuming that the meta
         * request will fail entirely when a request completely fails (initiating a cancel), and that that logic will
         * happen before this function is called.  All of that is client detail, which makes this a bit hacky right
         * now.*/
        ++auto_ranged_put->synced_data.num_parts_completed;

        if (cancelling) {
            /* Request is canceling, if all sent parts completed, we can send abort now */
            if (auto_ranged_put->synced_data.num_parts_completed == auto_ranged_put->synced_data.num_parts_sent) {
                auto_ranged_put->synced_data.state = AWS_S3_AUTO_RANGED_PUT_STATE_SEND_COMPLETE;
                notify_work_available = true;
            }
        } else if (auto_ranged_put->synced_data.num_parts_completed == auto_ranged_put->synced_data.total_num_parts) {
            auto_ranged_put->synced_data.state = AWS_S3_AUTO_RANGED_PUT_STATE_SEND_COMPLETE;
            notify_work_available = true;
        }

        AWS_LOGF_DEBUG(
            AWS_LS_S3_META_REQUEST,
            "id=%p: %d out of %d parts have completed.",
            (void *)meta_request,
            auto_ranged_put->synced_data.num_parts_completed,
            auto_ranged_put->synced_data.total_num_parts);

        s_s3_auto_ranged_put_unlock_synced_data(auto_ranged_put);

        if (notify_work_available) {
            aws_s3_meta_request_push_to_client(meta_request);
        }

    } else if (request->request_tag == AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_ABORT_MULTIPART_UPLOAD) {
        s_s3_auto_ranged_put_cancel_finished(meta_request);
    }
}
