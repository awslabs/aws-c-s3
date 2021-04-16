/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_auto_ranged_put.h"
#include "aws/s3/private/s3_request_messages.h"
#include "aws/s3/private/s3_util.h"
#include <aws/common/string.h>
#include <aws/io/stream.h>

static const size_t s_etags_initial_capacity = 16;
static const struct aws_byte_cursor s_upload_id = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("UploadId");
static const size_t s_complete_multipart_upload_init_body_size_bytes = 512;
static const size_t s_abort_multipart_upload_init_body_size_bytes = 512;

static const struct aws_byte_cursor s_create_multipart_upload_copy_headers[] = {
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-server-side-encryption-customer-algorithm"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-server-side-encryption-customer-key-MD5"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-server-side-encryption-context"),
};

static void s_s3_meta_request_auto_ranged_put_destroy(struct aws_s3_meta_request *meta_request);

static bool s_s3_auto_ranged_put_update(
    struct aws_s3_meta_request *meta_request,
    uint32_t flags,
    struct aws_s3_request **out_request);

static int s_s3_auto_ranged_put_prepare_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request);

static void s_s3_auto_ranged_put_request_finished(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    int error_code);

static struct aws_s3_meta_request_vtable s_s3_auto_ranged_put_vtable = {
    .update = s_s3_auto_ranged_put_update,
    .send_request_finish = aws_s3_meta_request_send_request_finish_default,
    .prepare_request = s_s3_auto_ranged_put_prepare_request,
    .init_signing_date_time = aws_s3_meta_request_init_signing_date_time_default,
    .sign_request = aws_s3_meta_request_sign_request_default,
    .finished_request = s_s3_auto_ranged_put_request_finished,
    .destroy = s_s3_meta_request_auto_ranged_put_destroy,
    .finish = aws_s3_meta_request_finish_default,
};

/* Allocate a new auto-ranged put meta request */
struct aws_s3_meta_request *aws_s3_meta_request_auto_ranged_put_new(
    struct aws_allocator *allocator,
    struct aws_s3_client *client,
    size_t part_size,
    uint64_t content_length,
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

    auto_ranged_put->content_length = content_length;
    auto_ranged_put->synced_data.total_num_parts = num_parts;
    auto_ranged_put->threaded_update_data.next_part_number = 1;

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
    aws_mem_release(meta_request->allocator, auto_ranged_put);
}

static bool s_s3_auto_ranged_put_update(
    struct aws_s3_meta_request *meta_request,
    uint32_t flags,
    struct aws_s3_request **out_request) {
    AWS_PRECONDITION(meta_request);

    struct aws_s3_request *request = NULL;
    bool work_remaining = false;

    struct aws_s3_auto_ranged_put *auto_ranged_put = meta_request->impl;

    aws_s3_meta_request_lock_synced_data(meta_request);

    if ((flags & AWS_S3_META_REQUEST_UPDATE_FLAG_NO_ENDPOINT_CONNECTIONS) > 0) {
        /* If there isn't a connection, then fail the meta request now if it hasn't been already. */
        aws_s3_meta_request_set_fail_synced(meta_request, NULL, AWS_ERROR_S3_NO_ENDPOINT_CONNECTIONS);
    }

    if (!aws_s3_meta_request_has_finish_result_synced(meta_request)) {

        /* If we haven't already sent a create-multipart-upload message, do so now. */
        if (!auto_ranged_put->synced_data.create_multipart_upload_sent) {

            if (out_request == NULL) {
                goto has_work_remaining;
            }

            request = aws_s3_request_new(
                meta_request,
                AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_CREATE_MULTIPART_UPLOAD,
                0,
                AWS_S3_REQUEST_FLAG_RECORD_RESPONSE_HEADERS);

            auto_ranged_put->synced_data.create_multipart_upload_sent = true;

            goto has_work_remaining;
        }

        /* If the create-multipart-upload message hasn't been completed, then there is still additional work to do, but
         * it can't be done yet. */
        if (!auto_ranged_put->synced_data.create_multipart_upload_completed) {
            goto has_work_remaining;
        }

        /* If we haven't sent all of the parts yet, then set up to send a new part now. */
        if (auto_ranged_put->synced_data.num_parts_sent < auto_ranged_put->synced_data.total_num_parts) {

            if (out_request == NULL) {
                goto has_work_remaining;
            }

            if ((flags & AWS_S3_META_REQUEST_UPDATE_FLAG_CONSERVATIVE) != 0) {
                uint32_t num_parts_in_flight =
                    (auto_ranged_put->synced_data.num_parts_sent - auto_ranged_put->synced_data.num_parts_completed);

                /* Because uploads must read from their streams serially, we try to limit the amount of in flight
                 * requests for a given multipart upload if we can. */
                if (num_parts_in_flight > 0) {
                    goto has_work_remaining;
                }
            }

            /* Allocate a request for another part. */
            request = aws_s3_request_new(
                meta_request, AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_PART, 0, AWS_S3_REQUEST_FLAG_RECORD_RESPONSE_HEADERS);

            request->part_number = auto_ranged_put->threaded_update_data.next_part_number;

            ++auto_ranged_put->threaded_update_data.next_part_number;
            ++auto_ranged_put->synced_data.num_parts_sent;

            AWS_LOGF_DEBUG(
                AWS_LS_S3_META_REQUEST,
                "id=%p: Returning request %p for part %d",
                (void *)meta_request,
                (void *)request,
                request->part_number);

            goto has_work_remaining;
        }

        /* There is one more request to send after all of the parts (the complete-multipart-upload) but it can't be done
         * until all of the parts have been completed.*/
        if (auto_ranged_put->synced_data.num_parts_completed != auto_ranged_put->synced_data.total_num_parts) {
            goto has_work_remaining;
        }

        /* If the complete-multipart-upload request hasn't been set yet, then send it now. */
        if (!auto_ranged_put->synced_data.complete_multipart_upload_sent) {

            if (out_request == NULL) {
                goto has_work_remaining;
            }

            request = aws_s3_request_new(
                meta_request,
                AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_COMPLETE_MULTIPART_UPLOAD,
                0,
                AWS_S3_REQUEST_FLAG_RECORD_RESPONSE_HEADERS);

            auto_ranged_put->synced_data.complete_multipart_upload_sent = true;

            goto has_work_remaining;
        }

        /* Wait for the complete-multipart-upload request to finish. */
        if (!auto_ranged_put->synced_data.complete_multipart_upload_completed) {
            goto has_work_remaining;
        }

        goto no_work_remaining;
    } else {

        /* If the create multipart upload hasn't been sent, then there is nothing left to do when canceling. */
        if (!auto_ranged_put->synced_data.create_multipart_upload_sent) {
            goto no_work_remaining;
        }

        /* If the create-multipart-upload request is still in flight, wait for it to finish. */
        if (!auto_ranged_put->synced_data.create_multipart_upload_completed) {
            goto has_work_remaining;
        }

        /* If the number of parts completed is less than the number of parts sent, then we need to wait until all of
         * those parts are done sending before aborting. */
        if (auto_ranged_put->synced_data.num_parts_completed < auto_ranged_put->synced_data.num_parts_sent) {
            goto has_work_remaining;
        }

        /* If the complete-multipart-upload is already in flight, then we can't necessarily send an abort. */
        if (auto_ranged_put->synced_data.complete_multipart_upload_sent &&
            !auto_ranged_put->synced_data.complete_multipart_upload_completed) {
            goto has_work_remaining;
        }

        /* If the complete-multipart-upload completed successfully, then there is nothing to abort since the transfer
         * has already finished. */
        if (auto_ranged_put->synced_data.complete_multipart_upload_completed &&
            auto_ranged_put->synced_data.complete_multipart_upload_error_code == AWS_ERROR_SUCCESS) {
            goto no_work_remaining;
        }

        /* If there are no endpoint connections to send requests on, then we can't send an abort message, so there is no
         * work remaining. */
        if ((flags & AWS_S3_META_REQUEST_UPDATE_FLAG_NO_ENDPOINT_CONNECTIONS) > 0) {
            goto no_work_remaining;
        }

        /* If we made it here, and the abort-multipart-upload message hasn't been sent yet, then do so now. */
        if (!auto_ranged_put->synced_data.abort_multipart_upload_sent) {
            if (auto_ranged_put->upload_id == NULL) {
                goto no_work_remaining;
            }

            if (out_request == NULL) {
                goto has_work_remaining;
            }

            request = aws_s3_request_new(
                meta_request,
                AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_ABORT_MULTIPART_UPLOAD,
                0,
                AWS_S3_REQUEST_FLAG_RECORD_RESPONSE_HEADERS | AWS_S3_REQUEST_FLAG_ALWAYS_SEND);

            auto_ranged_put->synced_data.abort_multipart_upload_sent = true;

            goto has_work_remaining;
        }

        /* Wait for the multipart upload to be completed. */
        if (!auto_ranged_put->synced_data.abort_multipart_upload_completed) {
            goto has_work_remaining;
        }

        goto no_work_remaining;
    }

has_work_remaining:
    work_remaining = true;

no_work_remaining:

    if (!work_remaining) {
        aws_s3_meta_request_set_success_synced(meta_request, AWS_S3_RESPONSE_STATUS_SUCCESS);
    }

    aws_s3_meta_request_unlock_synced_data(meta_request);

    if (work_remaining) {
        if (request != NULL) {
            AWS_ASSERT(out_request);
            *out_request = request;
        }
    } else {
        AWS_ASSERT(request == NULL);

        aws_s3_meta_request_finish(meta_request);
    }

    return work_remaining;
}

/* Given a request, prepare it for sending based on its description. */
static int s_s3_auto_ranged_put_prepare_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request) {
    AWS_PRECONDITION(meta_request);

    struct aws_s3_auto_ranged_put *auto_ranged_put = meta_request->impl;
    AWS_PRECONDITION(auto_ranged_put);

    struct aws_http_message *message = NULL;

    switch (request->request_tag) {
        case AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_CREATE_MULTIPART_UPLOAD: {

            /* Create the message to create a new multipart upload. */
            message = aws_s3_create_multipart_upload_message_new(
                meta_request->allocator, meta_request->initial_request_message);

            break;
        }
        case AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_PART: {

            size_t request_body_size = meta_request->part_size;

            /* Last part--adjust size to match remaining content length. */
            if (request->part_number == auto_ranged_put->synced_data.total_num_parts) {
                size_t content_remainder =
                    (size_t)(auto_ranged_put->content_length % (uint64_t)meta_request->part_size);

                if (content_remainder > 0) {
                    request_body_size = content_remainder;
                }
            }

            if (request->num_times_prepared == 0) {
                aws_byte_buf_init(&request->request_body, meta_request->allocator, request_body_size);

                if (aws_s3_meta_request_read_body(meta_request, &request->request_body)) {
                    goto message_create_failed;
                }
            }

            /* Create a new put-object message to upload a part. */
            message = aws_s3_upload_part_message_new(
                meta_request->allocator,
                meta_request->initial_request_message,
                &request->request_body,
                request->part_number,
                auto_ranged_put->upload_id);
            break;
        }
        case AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_COMPLETE_MULTIPART_UPLOAD: {

            if (request->num_times_prepared == 0) {
                aws_byte_buf_init(
                    &request->request_body, meta_request->allocator, s_complete_multipart_upload_init_body_size_bytes);
            } else {
                aws_byte_buf_reset(&request->request_body, false);
            }

            aws_s3_meta_request_lock_synced_data(meta_request);

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

            aws_s3_meta_request_unlock_synced_data(meta_request);

            break;
        }
        case AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_ABORT_MULTIPART_UPLOAD: {
            AWS_FATAL_ASSERT(auto_ranged_put->upload_id);
            AWS_LOGF_DEBUG(
                AWS_LS_S3_META_REQUEST,
                "id=%p Abort multipart upload request for upload id %s.",
                (void *)meta_request,
                aws_string_c_str(auto_ranged_put->upload_id));

            if (request->num_times_prepared == 0) {
                aws_byte_buf_init(
                    &request->request_body, meta_request->allocator, s_abort_multipart_upload_init_body_size_bytes);
            } else {
                aws_byte_buf_reset(&request->request_body, false);
            }

            /* Build the message to abort our multipart upload */
            message = aws_s3_abort_multipart_upload_message_new(
                meta_request->allocator, meta_request->initial_request_message, auto_ranged_put->upload_id);

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

static void s_s3_auto_ranged_put_request_finished(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    int error_code) {

    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(meta_request->impl);
    AWS_PRECONDITION(request);

    struct aws_s3_auto_ranged_put *auto_ranged_put = meta_request->impl;

    switch (request->request_tag) {

        case AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_CREATE_MULTIPART_UPLOAD: {
            struct aws_http_headers *needed_response_headers = NULL;

            if (error_code == AWS_ERROR_SUCCESS) {
                needed_response_headers = aws_http_headers_new(meta_request->allocator);
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
                    error_code = AWS_ERROR_S3_MISSING_UPLOAD_ID;
                } else {
                    /* Store the multipart upload id. */
                    auto_ranged_put->upload_id = upload_id;
                }
            }

            aws_s3_meta_request_lock_synced_data(meta_request);

            AWS_ASSERT(auto_ranged_put->synced_data.needed_response_headers == NULL)
            auto_ranged_put->synced_data.needed_response_headers = needed_response_headers;

            auto_ranged_put->synced_data.create_multipart_upload_completed = true;
            auto_ranged_put->synced_data.create_multipart_upload_error_code = error_code;

            if (error_code != AWS_ERROR_SUCCESS) {
                aws_s3_meta_request_set_fail_synced(meta_request, request, error_code);
            }

            aws_s3_meta_request_unlock_synced_data(meta_request);
            break;
        }

        case AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_PART: {
            size_t part_number = request->part_number;
            AWS_FATAL_ASSERT(part_number > 0);
            size_t part_index = part_number - 1;
            struct aws_string *etag = NULL;

            if (error_code == AWS_ERROR_SUCCESS) {
                /* Find the ETag header if it exists and cache it. */
                struct aws_byte_cursor etag_within_quotes;

                AWS_ASSERT(request->send_data.response_headers);

                if (aws_http_headers_get(
                        request->send_data.response_headers, g_etag_header_name, &etag_within_quotes)) {
                    AWS_LOGF_ERROR(
                        AWS_LS_S3_META_REQUEST,
                        "id=%p Could not find ETag header for request %p",
                        (void *)meta_request,
                        (void *)request);

                    error_code = AWS_ERROR_S3_MISSING_UPLOAD_ID;
                } else {
                    /* The ETag value arrives in quotes, but we don't want it in quotes when we send it back up
                     * later, so just get rid of the quotes now. */
                    if (etag_within_quotes.len >= 2 && etag_within_quotes.ptr[0] == '"' &&
                        etag_within_quotes.ptr[etag_within_quotes.len - 1] == '"') {

                        aws_byte_cursor_advance(&etag_within_quotes, 1);
                        --etag_within_quotes.len;
                    }

                    etag = aws_string_new_from_cursor(meta_request->allocator, &etag_within_quotes);
                }
            }

            aws_s3_meta_request_lock_synced_data(meta_request);

            ++auto_ranged_put->synced_data.num_parts_completed;

            AWS_LOGF_DEBUG(
                AWS_LS_S3_META_REQUEST,
                "id=%p: %d out of %d parts have completed.",
                (void *)meta_request,
                auto_ranged_put->synced_data.num_parts_completed,
                auto_ranged_put->synced_data.total_num_parts);

            if (error_code == AWS_ERROR_SUCCESS) {
                AWS_ASSERT(etag != NULL);

                ++auto_ranged_put->synced_data.num_parts_successful;

                struct aws_string *null_etag = NULL;

                /* ETags need to be associated with their part number, so we keep the etag indices consistent with
                 * part numbers. This means we may have to add padding to the list in the case that parts finish out
                 * of order. */
                while (aws_array_list_length(&auto_ranged_put->synced_data.etag_list) < part_number) {
                    int push_back_result =
                        aws_array_list_push_back(&auto_ranged_put->synced_data.etag_list, &null_etag);
                    AWS_FATAL_ASSERT(push_back_result == AWS_OP_SUCCESS);
                }

                aws_array_list_set_at(&auto_ranged_put->synced_data.etag_list, &etag, part_index);
            } else {
                ++auto_ranged_put->synced_data.num_parts_failed;
                aws_s3_meta_request_set_fail_synced(meta_request, request, error_code);
            }

            aws_s3_meta_request_unlock_synced_data(meta_request);

            break;
        }

        case AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_COMPLETE_MULTIPART_UPLOAD: {
            if (error_code == AWS_ERROR_SUCCESS && meta_request->headers_callback != NULL) {
                struct aws_http_headers *final_response_headers = aws_http_headers_new(meta_request->allocator);

                /* Copy all the response headers from this request. */
                copy_http_headers(request->send_data.response_headers, final_response_headers);

                /* Copy over any response headers that we've previously determined are needed for this final
                 * response.
                 */
                aws_s3_meta_request_lock_synced_data(meta_request);
                copy_http_headers(auto_ranged_put->synced_data.needed_response_headers, final_response_headers);
                aws_s3_meta_request_unlock_synced_data(meta_request);

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

                    error_code = aws_last_error_or_unknown();
                }

                aws_http_headers_release(final_response_headers);
            }

            aws_s3_meta_request_lock_synced_data(meta_request);
            auto_ranged_put->synced_data.complete_multipart_upload_completed = true;
            auto_ranged_put->synced_data.complete_multipart_upload_error_code = error_code;

            if (error_code != AWS_ERROR_SUCCESS) {
                aws_s3_meta_request_set_fail_synced(meta_request, request, error_code);
            }
            aws_s3_meta_request_unlock_synced_data(meta_request);

            break;
        }
        case AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_ABORT_MULTIPART_UPLOAD: {
            aws_s3_meta_request_lock_synced_data(meta_request);
            auto_ranged_put->synced_data.abort_multipart_upload_error_code = error_code;
            auto_ranged_put->synced_data.abort_multipart_upload_completed = true;
            aws_s3_meta_request_unlock_synced_data(meta_request);
            break;
        }
    }
}
