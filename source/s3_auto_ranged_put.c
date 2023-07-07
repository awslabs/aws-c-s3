/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_auto_ranged_put.h"
#include "aws/s3/private/s3_checksums.h"
#include "aws/s3/private/s3_list_parts.h"
#include "aws/s3/private/s3_request_messages.h"
#include "aws/s3/private/s3_util.h"
#include <aws/common/encoding.h>
#include <aws/common/string.h>
#include <aws/io/stream.h>

/* TODO: better logging of steps */

static const struct aws_byte_cursor s_upload_id = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("UploadId");
static const size_t s_complete_multipart_upload_init_body_size_bytes = 512;
static const size_t s_abort_multipart_upload_init_body_size_bytes = 512;
/* For unknown length body we no longer know the number of parts. to avoid
 * resizing arrays for etags/checksums too much, those array start out with
 * capacity specified by the constant below. Note: constant has been arbitrary
 * picked to avoid using allocations and using too much memory. might change in future.
 */
static const uint32_t s_unknown_length_default_num_parts = 32;

/* Max number of parts (per meta-request) that can be: "started, but not done reading from stream".
 * Though reads are serial (only 1 part can be reading from stream at a time)
 * we may queue up more to minimize delays between each read.
 *
 * If this number is too low, there could be an avoidable delay between each read
 * (meta-request ready for more work, but client hasn't run update and given it more work yet)
 *
 * If this number is too high, early meta-requests could hog all the "work tokens"
 * (1st meta-request as queue of 100 "work tokens" that it needs to read
 * the stream for, while later meta-requests are doing nothing waiting for work tokens)
 *
 * TODO: this value needs further benchmarking. */
static const uint32_t s_max_parts_pending_read = 5;

static const struct aws_byte_cursor s_create_multipart_upload_copy_headers[] = {
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-server-side-encryption-customer-algorithm"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-server-side-encryption-customer-key-MD5"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-server-side-encryption-context"),
};

/* Data for aws_s3_auto_ranged_put's async vtable->prepare_request() job */
struct aws_s3_auto_ranged_put_prepare_request_job {
    struct aws_allocator *allocator;
    struct aws_s3_request *request;
    /* async step: prepare type-specific message */
    struct aws_future_http_message *asyncstep_prepare_message;
    /* future to set when this job completes */
    struct aws_future_void *on_complete;
};

/* Data for async preparation of an UploadPart request */
struct aws_s3_prepare_upload_part_job {
    struct aws_allocator *allocator;
    struct aws_s3_request *request;
    /* async step: skip parts from input stream that were previously uploaded */
    struct aws_future_void *asyncstep1_skip_prev_parts;
    /* async step: read this part from input stream */
    struct aws_future_bool *asyncstep2_read_part;
    /* future to set when this job completes */
    struct aws_future_http_message *on_complete;
};

/* Data for async preparation of a CompleteMultipartUpload request */
struct aws_s3_prepare_complete_multipart_upload_job {
    struct aws_allocator *allocator;
    struct aws_s3_request *request;
    /* async step: skip remaining parts from input stream that were previously uploaded */
    struct aws_future_void *asyncstep_skip_remaining_parts;
    /* future to set when this job completes */
    struct aws_future_http_message *on_complete;
};

static void s_s3_meta_request_auto_ranged_put_destroy(struct aws_s3_meta_request *meta_request);

static bool s_s3_auto_ranged_put_update(
    struct aws_s3_meta_request *meta_request,
    uint32_t flags,
    struct aws_s3_request **out_request);

static void s_skip_parts_from_stream_loop(void *user_data);

static struct aws_future_void *s_s3_auto_ranged_put_prepare_request(struct aws_s3_request *request);
static void s_s3_auto_ranged_put_prepare_request_finish(void *user_data);

static struct aws_future_http_message *s_s3_prepare_list_parts(struct aws_s3_request *request);

static struct aws_future_http_message *s_s3_prepare_create_multipart_upload(struct aws_s3_request *request);

static struct aws_future_http_message *s_s3_prepare_upload_part(struct aws_s3_request *request);
static void s_s3_prepare_upload_part_on_skipping_done(void *user_data);
static void s_s3_prepare_upload_part_on_read_done(void *user_data);
static void s_s3_prepare_upload_part_finish(struct aws_s3_prepare_upload_part_job *part_prep, int error_code);

static struct aws_future_http_message *s_s3_prepare_complete_multipart_upload(struct aws_s3_request *request);
static void s_s3_prepare_complete_multipart_upload_on_skipping_done(void *user_data);
static void s_s3_prepare_complete_multipart_upload_finish(
    struct aws_s3_prepare_complete_multipart_upload_job *complete_mpu_prep,
    int error_code);

static struct aws_future_http_message *s_s3_prepare_abort_multipart_upload(struct aws_s3_request *request);

static void s_s3_auto_ranged_put_request_finished(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    int error_code);

static void s_s3_auto_ranged_put_send_request_finish(
    struct aws_s3_connection *connection,
    struct aws_http_stream *stream,
    int error_code);

static int s_s3_auto_ranged_put_pause(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_meta_request_resume_token **resume_token);

static bool s_process_part_info_synced(const struct aws_s3_part_info *info, void *user_data) {
    struct aws_s3_auto_ranged_put *auto_ranged_put = user_data;
    struct aws_s3_meta_request *meta_request = &auto_ranged_put->base;

    ASSERT_SYNCED_DATA_LOCK_HELD(&auto_ranged_put->base);

    struct aws_s3_mpu_part_info *part = aws_mem_calloc(meta_request->allocator, 1, sizeof(struct aws_s3_mpu_part_info));
    part->size = info->size;
    part->etag = aws_strip_quotes(meta_request->allocator, info->e_tag);

    const struct aws_byte_cursor *checksum_cur = NULL;
    switch (auto_ranged_put->base.checksum_config.checksum_algorithm) {
        case AWS_SCA_CRC32:
            checksum_cur = &info->checksumCRC32;
            break;
        case AWS_SCA_CRC32C:
            checksum_cur = &info->checksumCRC32C;
            break;
        case AWS_SCA_SHA1:
            checksum_cur = &info->checksumSHA1;
            break;
        case AWS_SCA_SHA256:
            checksum_cur = &info->checksumSHA256;
            break;
        case AWS_SCA_NONE:
            break;
        default:
            AWS_ASSERT(false);
            break;
    }

    if ((checksum_cur != NULL) && (checksum_cur->len > 0)) {
        aws_byte_buf_init_copy_from_cursor(&part->checksum_base64, auto_ranged_put->base.allocator, *checksum_cur);
    }

    /* Parts might be out of order or have gaps in them.
     * Resize array-list to be long enough to hold this part,
     * filling any intermediate slots with NULL. */
    aws_array_list_ensure_capacity(&auto_ranged_put->synced_data.part_list, info->part_number);
    while (aws_array_list_length(&auto_ranged_put->synced_data.part_list) < info->part_number) {
        struct aws_s3_mpu_part_info *null_part = NULL;
        aws_array_list_push_back(&auto_ranged_put->synced_data.part_list, &null_part);
    }

    /* Add this part */
    aws_array_list_set_at(&auto_ranged_put->synced_data.part_list, &part, info->part_number - 1);

    return true;
}

/*
 * Validates token and updates part variables. Noop if token is null.
 */
static int s_try_update_part_info_from_resume_token(
    uint64_t content_length,
    const struct aws_s3_meta_request_resume_token *resume_token,
    size_t *out_part_size,
    uint32_t *out_total_num_parts) {

    if (!resume_token) {
        return AWS_OP_SUCCESS;
    }

    if (resume_token->type != AWS_S3_META_REQUEST_TYPE_PUT_OBJECT) {
        AWS_LOGF_ERROR(AWS_LS_S3_META_REQUEST, "Could not load persisted state. Invalid token type.");
        goto invalid_argument_cleanup;
    }

    if (resume_token->multipart_upload_id == NULL) {
        AWS_LOGF_ERROR(AWS_LS_S3_META_REQUEST, "Could not load persisted state. Multipart upload id missing.");
        goto invalid_argument_cleanup;
    }

    if (resume_token->part_size < g_s3_min_upload_part_size) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST,
            "Could not create resume auto-ranged-put meta request; part size of %" PRIu64
            " specified in the token is below minimum threshold for multi-part.",
            (uint64_t)resume_token->part_size);

        goto invalid_argument_cleanup;
    }

    if ((uint32_t)resume_token->total_num_parts > g_s3_max_num_upload_parts) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST,
            "Could not create resume auto-ranged-put meta request; total number of parts %" PRIu32
            " specified in the token is too large for platform.",
            (uint32_t)resume_token->total_num_parts);

        goto invalid_argument_cleanup;
    }

    uint32_t num_parts = (uint32_t)(content_length / resume_token->part_size);

    if ((content_length % resume_token->part_size) > 0) {
        ++num_parts;
    }

    if (resume_token->total_num_parts != num_parts) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST,
            "Could not create auto-ranged-put meta request; persisted number of parts %zu"
            " does not match expected number of parts based on length of the body.",
            resume_token->total_num_parts);

        return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
    }

    *out_part_size = resume_token->part_size;
    *out_total_num_parts = (uint32_t)resume_token->total_num_parts;

    return AWS_OP_SUCCESS;

invalid_argument_cleanup:
    return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
}

/**
 * Initializes state necessary to resume upload. Noop if token is null.
 */
static int s_try_init_resume_state_from_persisted_data(
    struct aws_allocator *allocator,
    struct aws_s3_auto_ranged_put *auto_ranged_put,
    const struct aws_s3_meta_request_resume_token *resume_token) {

    if (resume_token == NULL) {
        auto_ranged_put->synced_data.list_parts_operation = NULL;
        auto_ranged_put->synced_data.list_parts_state.completed = true;
        auto_ranged_put->synced_data.list_parts_state.started = true;
        return AWS_OP_SUCCESS;
    }

    AWS_FATAL_ASSERT(auto_ranged_put->has_content_length);

    struct aws_byte_cursor request_path;
    if (aws_http_message_get_request_path(auto_ranged_put->base.initial_request_message, &request_path)) {
        AWS_LOGF_ERROR(AWS_LS_S3_META_REQUEST, "Could not load persisted state. Request path could not be read.");
        return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
    }

    auto_ranged_put->synced_data.num_parts_started = 0;
    auto_ranged_put->synced_data.num_parts_completed = 0;
    auto_ranged_put->synced_data.num_parts_noop = 0;
    auto_ranged_put->synced_data.create_multipart_upload_sent = true;
    auto_ranged_put->synced_data.create_multipart_upload_completed = true;
    auto_ranged_put->upload_id = aws_string_clone_or_reuse(allocator, resume_token->multipart_upload_id);

    struct aws_s3_list_parts_params list_parts_params = {
        .key = request_path,
        .upload_id = aws_byte_cursor_from_string(auto_ranged_put->upload_id),
        .on_part = s_process_part_info_synced,
        .user_data = auto_ranged_put,
    };

    auto_ranged_put->synced_data.list_parts_operation = aws_s3_list_parts_operation_new(allocator, &list_parts_params);

    struct aws_http_headers *needed_response_headers = aws_http_headers_new(allocator);
    const size_t copy_header_count = AWS_ARRAY_SIZE(s_create_multipart_upload_copy_headers);
    const struct aws_http_headers *initial_headers =
        aws_http_message_get_headers(auto_ranged_put->base.initial_request_message);

    /* Copy headers that would have been used for create multipart from initial message, since create will never be
     * called in this flow */
    for (size_t header_index = 0; header_index < copy_header_count; ++header_index) {
        const struct aws_byte_cursor *header_name = &s_create_multipart_upload_copy_headers[header_index];
        struct aws_byte_cursor header_value;
        AWS_ZERO_STRUCT(header_value);

        if (aws_http_headers_get(initial_headers, *header_name, &header_value) == AWS_OP_SUCCESS) {
            aws_http_headers_set(needed_response_headers, *header_name, header_value);
        }
    }

    auto_ranged_put->synced_data.needed_response_headers = needed_response_headers;

    return AWS_OP_SUCCESS;
}

static int s_s3_auto_ranged_put_request_type(struct aws_s3_request *request) {
    switch (request->request_tag) {
        case AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_LIST_PARTS:
            return AWS_S3_REQUEST_TYPE_LIST_PARTS;
        case AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_CREATE_MULTIPART_UPLOAD:
            return AWS_S3_REQUEST_TYPE_CREATE_MULTIPART_UPLOAD;
        case AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_PART:
            return AWS_S3_REQUEST_TYPE_UPLOAD_PART;
        case AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_ABORT_MULTIPART_UPLOAD:
            return AWS_S3_REQUEST_TYPE_ABORT_MULTIPART_UPLOAD;
        case AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_COMPLETE_MULTIPART_UPLOAD:
            return AWS_S3_REQUEST_TYPE_COMPLETE_MULTIPART_UPLOAD;
    }
    AWS_ASSERT(false);
    return AWS_S3_REQUEST_TYPE_MAX;
}

static struct aws_s3_meta_request_vtable s_s3_auto_ranged_put_vtable = {
    .update = s_s3_auto_ranged_put_update,
    .send_request_finish = s_s3_auto_ranged_put_send_request_finish,
    .prepare_request = s_s3_auto_ranged_put_prepare_request,
    .init_signing_date_time = aws_s3_meta_request_init_signing_date_time_default,
    .sign_request = aws_s3_meta_request_sign_request_default,
    .finished_request = s_s3_auto_ranged_put_request_finished,
    .destroy = s_s3_meta_request_auto_ranged_put_destroy,
    .finish = aws_s3_meta_request_finish_default,
    .pause = s_s3_auto_ranged_put_pause,
    .get_request_type = s_s3_auto_ranged_put_request_type,
};

/* Allocate a new auto-ranged put meta request */
struct aws_s3_meta_request *aws_s3_meta_request_auto_ranged_put_new(
    struct aws_allocator *allocator,
    struct aws_s3_client *client,
    size_t part_size,
    bool has_content_length,
    uint64_t content_length,
    uint32_t num_parts,
    const struct aws_s3_meta_request_options *options) {

    /* These should already have been validated by the caller. */
    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(options);
    AWS_PRECONDITION(options->message);

    if (s_try_update_part_info_from_resume_token(content_length, options->resume_token, &part_size, &num_parts)) {
        return NULL;
    }

    struct aws_s3_auto_ranged_put *auto_ranged_put =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_auto_ranged_put));

    if (aws_s3_meta_request_init_base(
            allocator,
            client,
            part_size,
            client->compute_content_md5 == AWS_MR_CONTENT_MD5_ENABLED ||
                aws_http_headers_has(aws_http_message_get_headers(options->message), g_content_md5_header_name),
            options,
            auto_ranged_put,
            &s_s3_auto_ranged_put_vtable,
            &auto_ranged_put->base)) {
        aws_mem_release(allocator, auto_ranged_put);
        return NULL;
    }

    auto_ranged_put->has_content_length = has_content_length;
    auto_ranged_put->content_length = has_content_length ? content_length : 0;
    auto_ranged_put->total_num_parts_from_content_length = has_content_length ? num_parts : 0;
    auto_ranged_put->upload_id = NULL;
    auto_ranged_put->resume_token = options->resume_token;

    aws_s3_meta_request_resume_token_acquire(auto_ranged_put->resume_token);

    auto_ranged_put->threaded_update_data.next_part_number = 1;
    auto_ranged_put->prepare_data.part_index_for_skipping = 0;
    auto_ranged_put->synced_data.is_body_stream_at_end = false;

    uint32_t initial_num_parts = auto_ranged_put->has_content_length ? num_parts : s_unknown_length_default_num_parts;

    aws_array_list_init_dynamic(
        &auto_ranged_put->synced_data.part_list, allocator, initial_num_parts, sizeof(struct aws_s3_mpu_part_info *));

    if (s_try_init_resume_state_from_persisted_data(allocator, auto_ranged_put, options->resume_token)) {
        goto error_clean_up;
    }

    AWS_LOGF_DEBUG(
        AWS_LS_S3_META_REQUEST, "id=%p Created new Auto-Ranged Put Meta Request.", (void *)&auto_ranged_put->base);

    return &auto_ranged_put->base;

error_clean_up:
    aws_s3_meta_request_release(&auto_ranged_put->base);
    return NULL;
}

/* Destroy our auto-ranged put meta request */
static void s_s3_meta_request_auto_ranged_put_destroy(struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(meta_request->impl);

    struct aws_s3_auto_ranged_put *auto_ranged_put = meta_request->impl;

    aws_string_destroy(auto_ranged_put->upload_id);
    auto_ranged_put->upload_id = NULL;

    auto_ranged_put->resume_token = aws_s3_meta_request_resume_token_release(auto_ranged_put->resume_token);

    aws_s3_paginated_operation_release(auto_ranged_put->synced_data.list_parts_operation);

    for (size_t part_index = 0; part_index < aws_array_list_length(&auto_ranged_put->synced_data.part_list);
         ++part_index) {

        struct aws_s3_mpu_part_info *part;
        aws_array_list_get_at(&auto_ranged_put->synced_data.part_list, &part, part_index);
        if (part != NULL) {
            aws_byte_buf_clean_up(&part->checksum_base64);
            aws_string_destroy(part->etag);
            aws_mem_release(auto_ranged_put->base.allocator, part);
        }
    }
    aws_array_list_clean_up(&auto_ranged_put->synced_data.part_list);

    aws_string_destroy(auto_ranged_put->synced_data.list_parts_continuation_token);

    aws_http_headers_release(auto_ranged_put->synced_data.needed_response_headers);
    aws_mem_release(meta_request->allocator, auto_ranged_put);
}

/* Check flags and corresponding conditions to see if any more parts can be
 * scheduled during this pass. */
static bool s_should_skip_scheduling_more_parts_based_on_flags(
    const struct aws_s3_auto_ranged_put *auto_ranged_put,
    uint32_t flags) {

    /* If the stream is actually async, only allow 1 pending-read.
     * We need to wait for async read() to complete before calling it again. */
    if (auto_ranged_put->base.request_body_async_stream != NULL) {
        return auto_ranged_put->synced_data.num_parts_pending_read > 0;
    }

    /* If this is the conservative pass, only allow 1 pending-read.
     * Reads are serial anyway, so queuing up a whole bunch isn't necessarily a speedup. */
    if ((flags & AWS_S3_META_REQUEST_UPDATE_FLAG_CONSERVATIVE) != 0) {
        return auto_ranged_put->synced_data.num_parts_pending_read > 0;
    }

    /* In all other cases, cap the number of pending-reads to something reasonable */
    return auto_ranged_put->synced_data.num_parts_pending_read >= s_max_parts_pending_read;
}

static bool s_s3_auto_ranged_put_update(
    struct aws_s3_meta_request *meta_request,
    uint32_t flags,
    struct aws_s3_request **out_request) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(out_request);

    struct aws_s3_request *request = NULL;
    bool work_remaining = false;

    struct aws_s3_auto_ranged_put *auto_ranged_put = meta_request->impl;

    /* BEGIN CRITICAL SECTION */
    {
        aws_s3_meta_request_lock_synced_data(meta_request);

        if (!aws_s3_meta_request_has_finish_result_synced(meta_request)) {
            /* If resuming and list part has not been sent, do it now. */
            if (!auto_ranged_put->synced_data.list_parts_state.started) {
                request = aws_s3_request_new(
                    meta_request,
                    AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_LIST_PARTS,
                    0,
                    AWS_S3_REQUEST_FLAG_RECORD_RESPONSE_HEADERS);

                auto_ranged_put->synced_data.list_parts_state.started = true;

                goto has_work_remaining;
            }

            if (auto_ranged_put->synced_data.list_parts_state.continues) {
                /* If list parts need to continue, send another list parts request. */
                AWS_ASSERT(auto_ranged_put->synced_data.list_parts_continuation_token != NULL);
                request = aws_s3_request_new(
                    meta_request,
                    AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_LIST_PARTS,
                    0,
                    AWS_S3_REQUEST_FLAG_RECORD_RESPONSE_HEADERS);
                auto_ranged_put->synced_data.list_parts_state.continues = false;
                goto has_work_remaining;
            }

            if (!auto_ranged_put->synced_data.list_parts_state.completed) {
                /* waiting on list parts to finish. */
                goto has_work_remaining;
            }

            /* If we haven't already sent a create-multipart-upload message, do so now. */
            if (!auto_ranged_put->synced_data.create_multipart_upload_sent) {
                request = aws_s3_request_new(
                    meta_request,
                    AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_CREATE_MULTIPART_UPLOAD,
                    0,
                    AWS_S3_REQUEST_FLAG_RECORD_RESPONSE_HEADERS);

                auto_ranged_put->synced_data.create_multipart_upload_sent = true;

                goto has_work_remaining;
            }

            /* If the create-multipart-upload message hasn't been completed, then there is still additional work to do,
             * but it can't be done yet. */
            if (!auto_ranged_put->synced_data.create_multipart_upload_completed) {
                goto has_work_remaining;
            }

            bool should_create_next_part_request = false;
            if (auto_ranged_put->has_content_length && (auto_ranged_put->synced_data.num_parts_started <
                                                        auto_ranged_put->total_num_parts_from_content_length)) {

                /* Check if the etag/checksum list has the result already */
                for (size_t part_index = auto_ranged_put->threaded_update_data.next_part_number - 1;
                     part_index < aws_array_list_length(&auto_ranged_put->synced_data.part_list);
                     ++part_index) {

                    struct aws_s3_mpu_part_info *part;
                    aws_array_list_get_at(&auto_ranged_put->synced_data.part_list, &part, part_index);
                    if (part != NULL) {
                        /* part already downloaded, skip it here and prepare will take care of adjusting the buffer */
                        ++auto_ranged_put->threaded_update_data.next_part_number;

                    } else {
                        // incomplete part found. break out and create request for it.
                        break;
                    }
                }

                // Something went really wrong. we still have parts to send, but have etags for all parts
                AWS_FATAL_ASSERT(
                    auto_ranged_put->threaded_update_data.next_part_number <=
                    auto_ranged_put->total_num_parts_from_content_length);

                if (s_should_skip_scheduling_more_parts_based_on_flags(auto_ranged_put, flags)) {
                    goto has_work_remaining;
                }

                should_create_next_part_request = true;

            } else if (!auto_ranged_put->has_content_length && !auto_ranged_put->synced_data.is_body_stream_at_end) {

                if (s_should_skip_scheduling_more_parts_based_on_flags(auto_ranged_put, flags)) {
                    goto has_work_remaining;
                }

                should_create_next_part_request = true;
            }

            if (should_create_next_part_request) {

                /* Allocate a request for another part. */
                request = aws_s3_request_new(
                    meta_request,
                    AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_PART,
                    0,
                    AWS_S3_REQUEST_FLAG_RECORD_RESPONSE_HEADERS);

                request->part_number = auto_ranged_put->threaded_update_data.next_part_number;

                ++auto_ranged_put->threaded_update_data.next_part_number;
                ++auto_ranged_put->synced_data.num_parts_started;
                ++auto_ranged_put->synced_data.num_parts_pending_read;

                AWS_LOGF_DEBUG(
                    AWS_LS_S3_META_REQUEST,
                    "id=%p: Returning request %p for part %d",
                    (void *)meta_request,
                    (void *)request,
                    request->part_number);

                goto has_work_remaining;
            }

            /* There is one more request to send after all the parts (the complete-multipart-upload) but it can't be
             * done until all the parts have been completed.*/
            if (auto_ranged_put->has_content_length) {
                if (auto_ranged_put->synced_data.num_parts_completed !=
                    auto_ranged_put->total_num_parts_from_content_length) {
                    goto has_work_remaining;
                }
            } else {
                if ((!auto_ranged_put->synced_data.is_body_stream_at_end) ||
                    auto_ranged_put->synced_data.num_parts_completed !=
                        auto_ranged_put->synced_data.num_parts_started) {
                    goto has_work_remaining;
                }
            }

            /* If the complete-multipart-upload request hasn't been set yet, then send it now. */
            if (!auto_ranged_put->synced_data.complete_multipart_upload_sent) {
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
            if (auto_ranged_put->synced_data.num_parts_completed < auto_ranged_put->synced_data.num_parts_started) {
                goto has_work_remaining;
            }

            /* If the complete-multipart-upload is already in flight, then we can't necessarily send an abort. */
            if (auto_ranged_put->synced_data.complete_multipart_upload_sent &&
                !auto_ranged_put->synced_data.complete_multipart_upload_completed) {
                goto has_work_remaining;
            }

            /* If the upload was paused or resume failed, we don't abort the multipart upload. */
            if (meta_request->synced_data.finish_result.error_code == AWS_ERROR_S3_PAUSED ||
                meta_request->synced_data.finish_result.error_code == AWS_ERROR_S3_RESUME_FAILED) {
                goto no_work_remaining;
            }

            /* If the complete-multipart-upload completed successfully, then there is nothing to abort since the
             * transfer has already finished. */
            if (auto_ranged_put->synced_data.complete_multipart_upload_completed &&
                auto_ranged_put->synced_data.complete_multipart_upload_error_code == AWS_ERROR_SUCCESS) {
                goto no_work_remaining;
            }

            /* If we made it here, and the abort-multipart-upload message hasn't been sent yet, then do so now. */
            if (!auto_ranged_put->synced_data.abort_multipart_upload_sent) {
                if (auto_ranged_put->upload_id == NULL) {
                    goto no_work_remaining;
                }
                if (auto_ranged_put->base.synced_data.finish_result.error_code == AWS_ERROR_SUCCESS) {
                    /* Not sending abort when success even if we haven't sent complete MPU, in case we resume after MPU
                     * already completed. */
                    goto no_work_remaining;
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
    }
    /* END CRITICAL SECTION */

    if (work_remaining) {
        *out_request = request;
    } else {
        AWS_ASSERT(request == NULL);

        aws_s3_meta_request_finish(meta_request);
    }

    return work_remaining;
}

/**
 * Helper to compute request body size.
 * Basically returns either part size or if content is not equally divisible into parts, the size of the remaining last
 * part.
 */
static size_t s_compute_request_body_size(const struct aws_s3_meta_request *meta_request, uint32_t part_number) {
    AWS_PRECONDITION(meta_request);

    const struct aws_s3_auto_ranged_put *auto_ranged_put = meta_request->impl;

    size_t request_body_size = meta_request->part_size;
    /* Last part--adjust size to match remaining content length. */
    if (auto_ranged_put->has_content_length && part_number == auto_ranged_put->total_num_parts_from_content_length) {
        size_t content_remainder = (size_t)(auto_ranged_put->content_length % (uint64_t)meta_request->part_size);

        if (content_remainder > 0) {
            request_body_size = content_remainder;
        }
    }

    return request_body_size;
}

static int s_verify_part_matches_checksum(
    struct aws_allocator *allocator,
    struct aws_byte_cursor body_cur,
    enum aws_s3_checksum_algorithm algorithm,
    struct aws_byte_cursor part_checksum) {
    AWS_PRECONDITION(allocator);

    if (algorithm == AWS_SCA_NONE) {
        return AWS_OP_SUCCESS;
    }

    struct aws_byte_buf checksum;
    if (aws_byte_buf_init(&checksum, allocator, aws_get_digest_size_from_algorithm(algorithm))) {
        return AWS_OP_ERR;
    }

    struct aws_byte_buf encoded_checksum = {0};

    int return_status = AWS_OP_SUCCESS;

    size_t encoded_len = 0;
    if (aws_base64_compute_encoded_len(aws_get_digest_size_from_algorithm(algorithm), &encoded_len)) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST, "Failed to resume upload. Unable to determine length of encoded checksum.");
        return_status = aws_raise_error(AWS_ERROR_S3_RESUME_FAILED);
        goto on_done;
    }

    if (aws_checksum_compute(allocator, algorithm, &body_cur, &checksum, 0)) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST, "Failed to resume upload. Unable to compute checksum for the skipped part.");
        return_status = aws_raise_error(AWS_ERROR_S3_RESUME_FAILED);
        goto on_done;
    }

    if (aws_byte_buf_init(&encoded_checksum, allocator, encoded_len)) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST, "Failed to resume upload. Unable to allocate buffer for encoded checksum.");
        return_status = aws_raise_error(AWS_ERROR_S3_RESUME_FAILED);
        goto on_done;
    }

    struct aws_byte_cursor checksum_cur = aws_byte_cursor_from_buf(&checksum);
    if (aws_base64_encode(&checksum_cur, &encoded_checksum)) {
        AWS_LOGF_ERROR(AWS_LS_S3_META_REQUEST, "Failed to resume upload. Unable to encode checksum.");
        return_status = aws_raise_error(AWS_ERROR_S3_RESUME_FAILED);
        goto on_done;
    }

    if (!aws_byte_cursor_eq_byte_buf(&part_checksum, &encoded_checksum)) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST, "Failed to resume upload. Checksum for previously uploaded part does not match");
        return_status = aws_raise_error(AWS_ERROR_S3_RESUMED_PART_CHECKSUM_MISMATCH);
        goto on_done;
    }

on_done:
    aws_byte_buf_clean_up(&checksum);
    aws_byte_buf_clean_up(&encoded_checksum);
    return return_status;
}

/* Data for the async skip-parts-from-stream job */
struct aws_s3_skip_parts_from_stream_job {
    struct aws_allocator *allocator;
    struct aws_s3_meta_request *meta_request;
    uint32_t part_index;
    uint32_t skip_until_part_number;
    struct aws_byte_buf temp_body_buf;
    /* non-owning reference to info about part we're currently skipping */
    struct aws_s3_mpu_part_info *part_being_skipped;
    /* repeated async step: read each part we're skipping */
    struct aws_future_bool *asyncstep_read_each_part;
    /* future to set when this job completes */
    struct aws_future_void *on_complete;
};

/**
 * Async function that skips parts from input stream that were previously uploaded.
 * Assumes input stream has part_index_for_skipping specifying which part stream is on
 * and will read into temp buffer until it gets to skip_until_part_number (i.e. skipping does include
 * that part). If checksum is set on the request and parts with checksums were uploaded before, checksum will be
 * verified.
 *
 * Note: If there's no content_length, pause/resume is not supported, so skipping parts will be noop.
 */
static struct aws_future_void *s_skip_parts_from_stream(
    struct aws_s3_meta_request *meta_request,
    uint32_t num_parts_read_from_stream,
    uint32_t skip_until_part_number) {

    AWS_PRECONDITION(meta_request);

    const struct aws_s3_auto_ranged_put *auto_ranged_put = meta_request->impl;
    (void)auto_ranged_put;

    struct aws_future_void *skip_future = aws_future_void_new(meta_request->allocator);
    if (num_parts_read_from_stream == skip_until_part_number || !auto_ranged_put->has_content_length) {
        aws_future_void_set_result(skip_future);
        return skip_future;
    }

    AWS_PRECONDITION(num_parts_read_from_stream <= skip_until_part_number);
    AWS_PRECONDITION(skip_until_part_number <= auto_ranged_put->total_num_parts_from_content_length);

    /* Store data for async job */
    struct aws_s3_skip_parts_from_stream_job *skip_job =
        aws_mem_calloc(meta_request->allocator, 1, sizeof(struct aws_s3_skip_parts_from_stream_job));
    skip_job->allocator = meta_request->allocator;
    skip_job->meta_request = meta_request;
    skip_job->part_index = num_parts_read_from_stream;
    skip_job->skip_until_part_number = skip_until_part_number;
    skip_job->on_complete = aws_future_void_acquire(skip_future);

    AWS_LOGF_DEBUG(
        AWS_LS_S3_META_REQUEST,
        "id=%p: Skipping parts %d through %d",
        (void *)meta_request,
        num_parts_read_from_stream,
        skip_until_part_number);

    /* Kick off async work loop */
    s_skip_parts_from_stream_loop(skip_job);

    return skip_future;
}

/* Async work loop for reading over skipped parts.
 * In a loop, try to read each part. If the read completes immediately then keep looping.
 * Otherwise, register this same function as the completion callback and bail out,
 * we'll resume the loop when the callback fires.
 *
 * It would have been simpler to always set a completion callback,
 * but this risks the call stack growing very large (if there are many parts
 * to skip and the reads all complete immediately). */
static void s_skip_parts_from_stream_loop(void *user_data) {
    struct aws_s3_skip_parts_from_stream_job *skip_job = user_data;
    struct aws_s3_meta_request *meta_request = skip_job->meta_request;
    struct aws_s3_auto_ranged_put *auto_ranged_put = meta_request->impl;
    struct aws_byte_buf *temp_body_buf = &skip_job->temp_body_buf;

    int error_code = AWS_ERROR_SUCCESS;

    for (; skip_job->part_index < skip_job->skip_until_part_number; ++skip_job->part_index) {

        /* kick off an async read if none are pending */
        if (skip_job->asyncstep_read_each_part == NULL) {

            /* Get info about part being skipped.
             * (Yes it's weird that we keep a pointer to the part_info even after
             * releasing the lock that protects part_list. But it's the resizable
             * part_list that needs lock protection. A previously uploaded part_info is const,
             * and it's on the heap, so it's safe to keep the pointer around) */
            /* BEGIN CRITICAL SECTION */
            aws_s3_meta_request_lock_synced_data(meta_request);
            aws_array_list_get_at(
                &auto_ranged_put->synced_data.part_list, &skip_job->part_being_skipped, skip_job->part_index);
            AWS_ASSERT(skip_job->part_being_skipped != NULL);
            aws_s3_meta_request_unlock_synced_data(meta_request);
            /* END CRITICAL SECTION */

            size_t request_body_size = s_compute_request_body_size(meta_request, skip_job->part_index + 1);
            if (request_body_size != skip_job->part_being_skipped->size) {
                error_code = AWS_ERROR_S3_RESUME_FAILED;
                AWS_LOGF_ERROR(
                    AWS_LS_S3_META_REQUEST,
                    "id=%p: Failed resuming upload, previous upload used different part size.",
                    (void *)meta_request);
                goto on_done;
            }

            if (temp_body_buf->capacity != request_body_size) {
                // reinit with correct size
                aws_byte_buf_clean_up(temp_body_buf);
                aws_byte_buf_init(temp_body_buf, meta_request->allocator, request_body_size);
            } else {
                // reuse buffer
                aws_byte_buf_reset(temp_body_buf, false);
            }

            skip_job->asyncstep_read_each_part = aws_s3_meta_request_read_body(skip_job->meta_request, temp_body_buf);

            /* the read may or may not complete immediately */
            if (aws_future_bool_register_callback_if_not_done(
                    skip_job->asyncstep_read_each_part, s_skip_parts_from_stream_loop, skip_job)) {

                /* read is pending, we'll resume this loop when callback fires */
                return;
            }
        }

        /* read is complete, check results */
        error_code = aws_future_bool_get_error(skip_job->asyncstep_read_each_part);
        skip_job->asyncstep_read_each_part =
            aws_future_bool_release(skip_job->asyncstep_read_each_part); /* release and set NULL */

        if (error_code != AWS_ERROR_SUCCESS) {
            AWS_LOGF_ERROR(
                AWS_LS_S3_META_REQUEST,
                "id=%p: Failed resuming upload, error reading request body %d (%s)",
                (void *)meta_request,
                error_code,
                aws_error_str(error_code));
            goto on_done;
        }

        if (temp_body_buf->len < temp_body_buf->capacity) {
            AWS_LOGF_ERROR(
                AWS_LS_S3_META_REQUEST,
                "id=%p: Failed resuming upload, request body smaller than it was during previous upload.",
                (void *)meta_request);
            error_code = AWS_ERROR_S3_INCORRECT_CONTENT_LENGTH;
            goto on_done;
        }

        // if previously uploaded part had a checksum, compare it to what we just skipped
        if (skip_job->part_being_skipped->checksum_base64.len > 0 &&
            s_verify_part_matches_checksum(
                meta_request->allocator,
                aws_byte_cursor_from_buf(temp_body_buf),
                meta_request->checksum_config.checksum_algorithm,
                aws_byte_cursor_from_buf(&skip_job->part_being_skipped->checksum_base64))) {
            error_code = aws_last_error_or_unknown();
            goto on_done;
        }
    }

on_done:
    aws_byte_buf_clean_up(&skip_job->temp_body_buf);
    if (error_code == AWS_ERROR_SUCCESS) {
        aws_future_void_set_result(skip_job->on_complete);
    } else {
        aws_future_void_set_error(skip_job->on_complete, error_code);
    }
    aws_future_void_release(skip_job->on_complete);
    aws_mem_release(skip_job->allocator, skip_job);
}

/* Given a request, prepare it for sending based on its description. */
static struct aws_future_void *s_s3_auto_ranged_put_prepare_request(struct aws_s3_request *request) {

    struct aws_future_void *asyncstep_prepare_request = aws_future_void_new(request->allocator);

    /* Store data for async job */
    struct aws_s3_auto_ranged_put_prepare_request_job *request_prep =
        aws_mem_calloc(request->allocator, 1, sizeof(struct aws_s3_auto_ranged_put_prepare_request_job));
    request_prep->allocator = request->allocator;
    request_prep->on_complete = aws_future_void_acquire(asyncstep_prepare_request);
    request_prep->request = request;

    /* Each type of request prepares an aws_http_message in its own way, which maybe require async substeps */
    switch (request->request_tag) {
        case AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_LIST_PARTS:
            request_prep->asyncstep_prepare_message = s_s3_prepare_list_parts(request);
            break;
        case AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_CREATE_MULTIPART_UPLOAD:
            request_prep->asyncstep_prepare_message = s_s3_prepare_create_multipart_upload(request);
            break;
        case AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_PART:
            request_prep->asyncstep_prepare_message = s_s3_prepare_upload_part(request);
            break;
        case AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_COMPLETE_MULTIPART_UPLOAD:
            request_prep->asyncstep_prepare_message = s_s3_prepare_complete_multipart_upload(request);
            break;
        case AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_ABORT_MULTIPART_UPLOAD:
            request_prep->asyncstep_prepare_message = s_s3_prepare_abort_multipart_upload(request);
            break;
        default:
            AWS_FATAL_ASSERT(0);
            break;
    }

    /* When the specific type of message is ready, finish common preparation steps */
    aws_future_http_message_register_callback(
        request_prep->asyncstep_prepare_message, s_s3_auto_ranged_put_prepare_request_finish, request_prep);

    return asyncstep_prepare_request;
}

/* Prepare a ListParts request.
 * Currently, this is actually synchronous. */
static struct aws_future_http_message *s_s3_prepare_list_parts(struct aws_s3_request *request) {
    struct aws_s3_meta_request *meta_request = request->meta_request;
    struct aws_s3_auto_ranged_put *auto_ranged_put = meta_request->impl;

    struct aws_http_message *message = NULL;
    int message_creation_result = AWS_OP_ERR;
    /* BEGIN CRITICAL SECTION */
    {
        aws_s3_meta_request_lock_synced_data(meta_request);

        if (auto_ranged_put->synced_data.list_parts_continuation_token) {
            AWS_LOGF_DEBUG(
                AWS_LS_S3_META_REQUEST,
                "id=%p ListParts for Multi-part Upload, with ID:%s, continues with token:%s.",
                (void *)meta_request,
                aws_string_c_str(auto_ranged_put->upload_id),
                aws_string_c_str(auto_ranged_put->synced_data.list_parts_continuation_token));
            struct aws_byte_cursor continuation_cur =
                aws_byte_cursor_from_string(auto_ranged_put->synced_data.list_parts_continuation_token);
            message_creation_result = aws_s3_construct_next_paginated_request_http_message(
                auto_ranged_put->synced_data.list_parts_operation, &continuation_cur, &message);
        } else {
            message_creation_result = aws_s3_construct_next_paginated_request_http_message(
                auto_ranged_put->synced_data.list_parts_operation, NULL, &message);
        }

        aws_s3_meta_request_unlock_synced_data(meta_request);
    }
    /* END CRITICAL SECTION */
    /* ListPart will not fail to create the next message `s_construct_next_request_http_message` */
    AWS_FATAL_ASSERT(message_creation_result == AWS_OP_SUCCESS);
    if (meta_request->checksum_config.checksum_algorithm == AWS_SCA_NONE) {
        /* We don't need to worry about the pre-calculated checksum from user as for multipart upload, only way
         * to calculate checksum for multipart upload is from client. */
        aws_s3_message_util_copy_headers(
            meta_request->initial_request_message,
            message,
            g_s3_list_parts_excluded_headers,
            g_s3_list_parts_excluded_headers_count,
            true);
    } else {
        aws_s3_message_util_copy_headers(
            meta_request->initial_request_message,
            message,
            g_s3_list_parts_with_checksum_excluded_headers,
            g_s3_list_parts_with_checksum_excluded_headers_count,
            true);
    }
    AWS_ASSERT(message);
    struct aws_future_http_message *future = aws_future_http_message_new(request->allocator);
    aws_future_http_message_set_result_by_move(future, &message);

    return future;
}

/* Prepare a CreateMultipartUpload request.
 * Currently, this is actually synchronous. */
struct aws_future_http_message *s_s3_prepare_create_multipart_upload(struct aws_s3_request *request) {
    struct aws_s3_meta_request *meta_request = request->meta_request;

    /* Create the message to create a new multipart upload. */
    struct aws_http_message *message = aws_s3_create_multipart_upload_message_new(
        meta_request->allocator,
        meta_request->initial_request_message,
        meta_request->checksum_config.checksum_algorithm);

    struct aws_future_http_message *future = aws_future_http_message_new(request->allocator);
    if (message != NULL) {
        aws_future_http_message_set_result_by_move(future, &message);
    } else {
        aws_future_http_message_set_error(future, aws_last_error_or_unknown());
    }
    return future;
}
/* Prepare an UploadPart request */
struct aws_future_http_message *s_s3_prepare_upload_part(struct aws_s3_request *request) {
    struct aws_s3_meta_request *meta_request = request->meta_request;
    struct aws_s3_auto_ranged_put *auto_ranged_put = meta_request->impl;
    struct aws_allocator *allocator = request->allocator;

    struct aws_future_http_message *message_future = aws_future_http_message_new(allocator);

    struct aws_s3_prepare_upload_part_job *part_prep =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_prepare_upload_part_job));
    part_prep->allocator = allocator;
    part_prep->request = request;
    part_prep->on_complete = aws_future_http_message_acquire(message_future);

    if (request->num_times_prepared == 0) {
        /* Preparing request for the first time.
         * Next async step: read through the body stream until we've
         * skipped over parts that were already uploaded (in case we're resuming
         * from an upload that had been paused) */
        part_prep->asyncstep1_skip_prev_parts = s_skip_parts_from_stream(
            meta_request, auto_ranged_put->prepare_data.part_index_for_skipping, request->part_number - 1);
        aws_future_void_register_callback(
            part_prep->asyncstep1_skip_prev_parts, s_s3_prepare_upload_part_on_skipping_done, part_prep);
    } else {
        /* Not the first time preparing request (e.g. retry).
         * We can skip over the async steps that read the body stream */
        s_s3_prepare_upload_part_finish(part_prep, AWS_ERROR_SUCCESS);
    }

    return message_future;
}

/* Completion callback for skipping over parts that were previously uploaded. */
static void s_s3_prepare_upload_part_on_skipping_done(void *user_data) {
    struct aws_s3_prepare_upload_part_job *part_prep = user_data;
    struct aws_s3_request *request = part_prep->request;
    struct aws_s3_meta_request *meta_request = request->meta_request;

    int error_code = aws_future_void_get_error(part_prep->asyncstep1_skip_prev_parts);

    /* If skipping failed, the prepare-upload-part job has failed. */
    if (error_code) {
        s_s3_prepare_upload_part_finish(part_prep, error_code);
        return;
    }

    /* Skipping succeeded.
     * Next async step: read body stream for this part into a buffer */

    size_t request_body_size = s_compute_request_body_size(meta_request, request->part_number);
    aws_byte_buf_init(&request->request_body, meta_request->allocator, request_body_size);

    part_prep->asyncstep2_read_part = aws_s3_meta_request_read_body(meta_request, &request->request_body);
    aws_future_bool_register_callback(
        part_prep->asyncstep2_read_part, s_s3_prepare_upload_part_on_read_done, part_prep);
}

/* Completion callback for reading this part's chunk of the body stream */
static void s_s3_prepare_upload_part_on_read_done(void *user_data) {
    struct aws_s3_prepare_upload_part_job *part_prep = user_data;
    struct aws_s3_request *request = part_prep->request;
    struct aws_s3_meta_request *meta_request = request->meta_request;
    struct aws_s3_auto_ranged_put *auto_ranged_put = meta_request->impl;
    bool has_content_length = auto_ranged_put->has_content_length != 0;

    int error_code = aws_future_bool_get_error(part_prep->asyncstep2_read_part);

    /* If reading failed, the prepare-upload-part job has failed */
    if (error_code != AWS_ERROR_SUCCESS) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST,
            "id=%p: Failed reading request body, error %d (%s)",
            (void *)meta_request,
            error_code,
            aws_error_str(error_code));
        goto on_done;
    }

    /* Reading succeeded. */
    bool is_body_stream_at_end = aws_future_bool_get_result(part_prep->asyncstep2_read_part);
    request->is_noop = request->part_number >
                           1 && /* allow first part to have 0 length to support empty unknown content length objects. */
                       request->request_body.len == 0;

    /* If Content-Length is defined, check that we read the expected amount */
    if (has_content_length && (request->request_body.len < request->request_body.capacity)) {
        error_code = AWS_ERROR_S3_INCORRECT_CONTENT_LENGTH;
        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST,
            "id=%p: Request body is smaller than 'Content-Length' header said it would be",
            (void *)meta_request);
        goto on_done;
    }

    /* BEGIN CRITICAL SECTION */
    {
        aws_s3_meta_request_lock_synced_data(meta_request);

        --auto_ranged_put->synced_data.num_parts_pending_read;

        auto_ranged_put->synced_data.is_body_stream_at_end = is_body_stream_at_end;

        if (!request->is_noop) {
            auto_ranged_put->prepare_data.part_index_for_skipping = request->part_number;

            /* Add part to array-list */
            struct aws_s3_mpu_part_info *part =
                aws_mem_calloc(meta_request->allocator, 1, sizeof(struct aws_s3_mpu_part_info));
            part->size = request->request_body.len;
            aws_array_list_set_at(&auto_ranged_put->synced_data.part_list, &part, request->part_number - 1);
        }

        aws_s3_meta_request_unlock_synced_data(meta_request);
    }
    /* END CRITICAL SECTION */

    /* We throttle the number of parts that can be "pending read"
     * (e.g. only 1 at a time if reading from async-stream).
     * Now that read is complete, poke the client to see if it can give us more work.
     *
     * Poking now gives measurable speedup (1%) for async streaming,
     * vs waiting until all the part-prep steps are complete (still need to sign, etc) */
    aws_s3_client_schedule_process_work(meta_request->client);

on_done:
    s_s3_prepare_upload_part_finish(part_prep, error_code);
}

/* Finish async preparation of an UploadPart request */
static void s_s3_prepare_upload_part_finish(struct aws_s3_prepare_upload_part_job *part_prep, int error_code) {
    struct aws_s3_request *request = part_prep->request;
    struct aws_s3_meta_request *meta_request = request->meta_request;
    struct aws_s3_auto_ranged_put *auto_ranged_put = meta_request->impl;

    if (error_code != AWS_ERROR_SUCCESS) {
        aws_future_http_message_set_error(part_prep->on_complete, error_code);
        goto on_done;
    }

    struct aws_byte_buf *checksum_buf = NULL;
    if (request->is_noop) {
        AWS_LOGF_DEBUG(
            AWS_LS_S3_META_REQUEST,
            "id=%p UploadPart with part num %u for Multi-part Upload, with ID:%s"
            "is noop due to encountering end of stream",
            (void *)meta_request,
            request->part_number,
            aws_string_c_str(auto_ranged_put->upload_id));

    } else {

        /* BEGIN CRITICAL SECTION */
        {
            aws_s3_meta_request_lock_synced_data(meta_request);
            struct aws_s3_mpu_part_info *part = NULL;
            aws_array_list_get_at(&auto_ranged_put->synced_data.part_list, &part, request->part_number - 1);
            AWS_ASSERT(part != NULL);
            checksum_buf = &part->checksum_base64;
            /* Clean up the buffer in case of it's initialized before and retry happens. */
            aws_byte_buf_clean_up(checksum_buf);
            aws_s3_meta_request_unlock_synced_data(meta_request);
        }
        /* END CRITICAL SECTION */

        AWS_LOGF_DEBUG(
            AWS_LS_S3_META_REQUEST,
            "id=%p UploadPart for Multi-part Upload, with ID:%s",
            (void *)meta_request,
            aws_string_c_str(auto_ranged_put->upload_id));
    }

    /* Create a new put-object message to upload a part. */
    struct aws_http_message *message = aws_s3_upload_part_message_new(
        meta_request->allocator,
        meta_request->initial_request_message,
        &request->request_body,
        request->part_number,
        auto_ranged_put->upload_id,
        meta_request->should_compute_content_md5,
        &meta_request->checksum_config,
        checksum_buf);
    if (message == NULL) {
        aws_future_http_message_set_error(part_prep->on_complete, aws_last_error());
        goto on_done;
    }

    /* Success! */
    aws_future_http_message_set_result_by_move(part_prep->on_complete, &message);

on_done:
    AWS_FATAL_ASSERT(aws_future_http_message_is_done(part_prep->on_complete));
    aws_future_void_release(part_prep->asyncstep1_skip_prev_parts);
    aws_future_bool_release(part_prep->asyncstep2_read_part);
    aws_future_http_message_release(part_prep->on_complete);
    aws_mem_release(part_prep->allocator, part_prep);
}

/* Prepare a CompleteMultipartUpload request. */
static struct aws_future_http_message *s_s3_prepare_complete_multipart_upload(struct aws_s3_request *request) {
    struct aws_s3_meta_request *meta_request = request->meta_request;
    struct aws_s3_auto_ranged_put *auto_ranged_put = meta_request->impl;
    struct aws_allocator *allocator = request->allocator;

    struct aws_future_http_message *message_future = aws_future_http_message_new(allocator);

    struct aws_s3_prepare_complete_multipart_upload_job *complete_mpu_prep =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_prepare_complete_multipart_upload_job));
    complete_mpu_prep->allocator = allocator;
    complete_mpu_prep->request = request;
    complete_mpu_prep->on_complete = aws_future_http_message_acquire(message_future);

    if (request->num_times_prepared == 0) {

        /* Corner case of last part being previously uploaded during resume.
         * Read it from input stream and potentially verify checksum */
        complete_mpu_prep->asyncstep_skip_remaining_parts = s_skip_parts_from_stream(
            meta_request,
            auto_ranged_put->prepare_data.part_index_for_skipping,
            auto_ranged_put->total_num_parts_from_content_length);

        aws_future_void_register_callback(
            complete_mpu_prep->asyncstep_skip_remaining_parts,
            s_s3_prepare_complete_multipart_upload_on_skipping_done,
            complete_mpu_prep);
    } else {
        /* Not the first time preparing request (e.g. retry).
         * We can skip over the async steps. */
        s_s3_prepare_complete_multipart_upload_finish(complete_mpu_prep, AWS_ERROR_SUCCESS);
    }

    return message_future;
}

/* Completion callback for skipping over parts that were previously uploaded. */
static void s_s3_prepare_complete_multipart_upload_on_skipping_done(void *user_data) {

    struct aws_s3_prepare_complete_multipart_upload_job *complete_mpu_prep = user_data;

    int error_code = aws_future_void_get_error(complete_mpu_prep->asyncstep_skip_remaining_parts);
    if (error_code != AWS_ERROR_SUCCESS) {
        s_s3_prepare_complete_multipart_upload_finish(complete_mpu_prep, error_code);
        return;
    }

    /* Async steps complete, finish up job */
    s_s3_prepare_complete_multipart_upload_finish(complete_mpu_prep, AWS_ERROR_SUCCESS);
}

/* Allow user to review what we've uploaded, and fail the meta-request if they don't approve. */
static int s_s3_review_multipart_upload(struct aws_s3_request *request) {
    struct aws_s3_meta_request *meta_request = request->meta_request;
    struct aws_s3_auto_ranged_put *auto_ranged_put = meta_request->impl;

    /* If user registered no callback, then success! */
    if (meta_request->upload_review_callback == NULL) {
        return AWS_OP_SUCCESS;
    }

    /* Prepare review info */
    struct aws_s3_upload_review_info review = {
        .checksum_algorithm = meta_request->checksum_config.checksum_algorithm,
    };

    /* BEGIN CRITICAL SECTION */
    aws_s3_meta_request_lock_synced_data(meta_request);

    review.part_count = aws_array_list_length(&auto_ranged_put->synced_data.part_list);

    if (review.part_count > 0) {
        review.part_array =
            aws_mem_calloc(meta_request->allocator, review.part_count, sizeof(struct aws_s3_upload_review_part_info));

        for (size_t part_index = 0; part_index < review.part_count; ++part_index) {
            struct aws_s3_mpu_part_info *part;
            aws_array_list_get_at(&auto_ranged_put->synced_data.part_list, &part, part_index);

            struct aws_s3_upload_review_part_info *part_review = &review.part_array[part_index];
            part_review->size = part->size;
            part_review->checksum = aws_byte_cursor_from_buf(&part->checksum_base64);
        }
    }

    aws_s3_meta_request_unlock_synced_data(meta_request);
    /* END CRITICAL SECTION */

    /* Invoke callback */
    int error_code = AWS_ERROR_SUCCESS;
    if (meta_request->upload_review_callback(meta_request, &review, meta_request->user_data) != AWS_OP_SUCCESS) {
        error_code = aws_last_error_or_unknown();
    }

    /* Clean up review info */
    aws_mem_release(meta_request->allocator, review.part_array);

    if (error_code == AWS_ERROR_SUCCESS) {
        return AWS_OP_SUCCESS;
    } else {
        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST,
            "id=%p: Upload review callback raised error %d (%s)",
            (void *)meta_request,
            error_code,
            aws_error_str(error_code));
        return aws_raise_error(error_code);
    }
}

/* Finish async preparation of a CompleteMultipartUpload request */
static void s_s3_prepare_complete_multipart_upload_finish(
    struct aws_s3_prepare_complete_multipart_upload_job *complete_mpu_prep,
    int error_code) {

    struct aws_s3_request *request = complete_mpu_prep->request;
    struct aws_s3_meta_request *meta_request = request->meta_request;
    struct aws_s3_auto_ranged_put *auto_ranged_put = meta_request->impl;

    if (error_code != AWS_ERROR_SUCCESS) {
        aws_future_http_message_set_error(complete_mpu_prep->on_complete, error_code);
        goto on_done;
    }

    AWS_FATAL_ASSERT(auto_ranged_put->upload_id);

    if (request->num_times_prepared == 0) {
        /* Invoke upload_review_callback, and fail meta-request if user raises an error */
        if (s_s3_review_multipart_upload(request) != AWS_OP_SUCCESS) {
            error_code = aws_last_error();
            aws_future_http_message_set_error(complete_mpu_prep->on_complete, error_code);
            goto on_done;
        }

        /* Allocate request body */
        aws_byte_buf_init(
            &request->request_body, meta_request->allocator, s_complete_multipart_upload_init_body_size_bytes);

    } else {
        /* This is a retry, reset request body */
        aws_byte_buf_reset(&request->request_body, false);
    }

    /* BEGIN CRITICAL SECTION */
    aws_s3_meta_request_lock_synced_data(meta_request);

    /* Build the message to complete our multipart upload, which includes a payload describing all of
     * our completed parts. */
    struct aws_http_message *message = aws_s3_complete_multipart_message_new(
        meta_request->allocator,
        meta_request->initial_request_message,
        &request->request_body,
        auto_ranged_put->upload_id,
        &auto_ranged_put->synced_data.part_list,
        meta_request->checksum_config.checksum_algorithm);

    aws_s3_meta_request_unlock_synced_data(meta_request);
    /* END CRITICAL SECTION */

    if (message == NULL) {
        aws_future_http_message_set_error(complete_mpu_prep->on_complete, aws_last_error());
        goto on_done;
    }

    /* Success! */
    aws_future_http_message_set_result_by_move(complete_mpu_prep->on_complete, &message);

on_done:
    AWS_FATAL_ASSERT(aws_future_http_message_is_done(complete_mpu_prep->on_complete));
    aws_future_void_release(complete_mpu_prep->asyncstep_skip_remaining_parts);
    aws_future_http_message_release(complete_mpu_prep->on_complete);
    aws_mem_release(complete_mpu_prep->allocator, complete_mpu_prep);
}

/* Prepare an AbortMultipartUpload request.
 * Currently, this is actually synchronous. */
struct aws_future_http_message *s_s3_prepare_abort_multipart_upload(struct aws_s3_request *request) {
    struct aws_s3_meta_request *meta_request = request->meta_request;
    struct aws_s3_auto_ranged_put *auto_ranged_put = meta_request->impl;

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
    struct aws_http_message *message = aws_s3_abort_multipart_upload_message_new(
        meta_request->allocator, meta_request->initial_request_message, auto_ranged_put->upload_id);

    struct aws_future_http_message *future = aws_future_http_message_new(request->allocator);
    if (message != NULL) {
        aws_future_http_message_set_result_by_move(future, &message);
    } else {
        aws_future_http_message_set_error(future, aws_last_error_or_unknown());
    }
    return future;
}

/* Finish the vtable->prepare_request() job */
static void s_s3_auto_ranged_put_prepare_request_finish(void *user_data) {
    struct aws_s3_auto_ranged_put_prepare_request_job *request_prep = user_data;
    struct aws_s3_request *request = request_prep->request;
    struct aws_s3_meta_request *meta_request = request->meta_request;

    /* Did we successfully create the type-specific HTTP message? */
    int error_code = aws_future_http_message_get_error(request_prep->asyncstep_prepare_message);
    if (error_code != AWS_ERROR_SUCCESS) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST,
            "id=%p Could not allocate message for request with tag %d for auto-ranged-put meta request.",
            (void *)meta_request,
            request->request_tag);

        goto on_done;
    }

    /* Success! Apply aws_http_message to aws_s3_request */
    struct aws_http_message *message =
        aws_future_http_message_get_result_by_move(request_prep->asyncstep_prepare_message);
    aws_s3_request_setup_send_data(request, message);
    aws_http_message_release(message);

    AWS_LOGF_DEBUG(
        AWS_LS_S3_META_REQUEST,
        "id=%p: Prepared request %p for part %d",
        (void *)meta_request,
        (void *)request,
        request->part_number);

on_done:
    if (error_code == AWS_ERROR_SUCCESS) {
        aws_future_void_set_result(request_prep->on_complete);
    } else {
        aws_future_void_set_error(request_prep->on_complete, error_code);
    }

    aws_future_http_message_release(request_prep->asyncstep_prepare_message);
    aws_future_void_release(request_prep->on_complete);
    aws_mem_release(request_prep->allocator, request_prep);
}

/* Invoked before retry */
static void s_s3_auto_ranged_put_send_request_finish(
    struct aws_s3_connection *connection,
    struct aws_http_stream *stream,
    int error_code) {

    const struct aws_s3_request *request = connection->request;
    AWS_PRECONDITION(request);

    /* Request tag is different from different type of meta requests */
    switch (request->request_tag) {

        case AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_COMPLETE_MULTIPART_UPLOAD: {
            /* For complete multipart upload, the server may return async error. */
            aws_s3_meta_request_send_request_finish_handle_async_error(connection, stream, error_code);
            break;
        }

        default:
            aws_s3_meta_request_send_request_finish_default(connection, stream, error_code);
            break;
    }
}

/* Invoked when no-retry will happen */
static void s_s3_auto_ranged_put_request_finished(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    int error_code) {

    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(meta_request->impl);
    AWS_PRECONDITION(request);

    struct aws_s3_auto_ranged_put *auto_ranged_put = meta_request->impl;

    switch (request->request_tag) {

        case AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_LIST_PARTS: {
            /* BEGIN CRITICAL SECTION */
            {
                aws_s3_meta_request_lock_synced_data(meta_request);

                bool has_more_results = false;

                if (error_code == AWS_ERROR_SUCCESS) {

                    struct aws_byte_cursor body_cursor = aws_byte_cursor_from_buf(&request->send_data.response_body);
                    /* Clear the token before */
                    aws_string_destroy(auto_ranged_put->synced_data.list_parts_continuation_token);
                    auto_ranged_put->synced_data.list_parts_continuation_token = NULL;
                    if (aws_s3_paginated_operation_on_response(
                            auto_ranged_put->synced_data.list_parts_operation,
                            &body_cursor,
                            &auto_ranged_put->synced_data.list_parts_continuation_token,
                            &has_more_results)) {
                        AWS_LOGF_ERROR(
                            AWS_LS_S3_META_REQUEST, "id=%p Failed to parse list parts response.", (void *)meta_request);
                        error_code = AWS_ERROR_S3_LIST_PARTS_PARSE_FAILED;
                    } else if (!has_more_results) {
                        for (size_t part_index = 0;
                             part_index < aws_array_list_length(&auto_ranged_put->synced_data.part_list);
                             part_index++) {
                            struct aws_s3_mpu_part_info *part = NULL;
                            aws_array_list_get_at(&auto_ranged_put->synced_data.part_list, &part, part_index);
                            if (part != NULL) {
                                /* Update the number of parts sent/completed previously */
                                ++auto_ranged_put->synced_data.num_parts_started;
                                ++auto_ranged_put->synced_data.num_parts_completed;
                            }
                        }

                        AWS_LOGF_DEBUG(
                            AWS_LS_S3_META_REQUEST,
                            "id=%p: Resuming PutObject. %d out of %d parts have completed during previous request.",
                            (void *)meta_request,
                            auto_ranged_put->synced_data.num_parts_completed,
                            auto_ranged_put->total_num_parts_from_content_length);
                    }
                }

                if (has_more_results) {
                    /* If list parts has more result, make sure list parts continues */
                    auto_ranged_put->synced_data.list_parts_state.continues = true;
                    auto_ranged_put->synced_data.list_parts_state.completed = false;
                } else {
                    /* No more result, complete the list parts */
                    auto_ranged_put->synced_data.list_parts_state.continues = false;
                    auto_ranged_put->synced_data.list_parts_state.completed = true;
                }
                auto_ranged_put->synced_data.list_parts_error_code = error_code;

                if (error_code != AWS_ERROR_SUCCESS) {
                    if (request->send_data.response_status == AWS_HTTP_STATUS_CODE_404_NOT_FOUND &&
                        auto_ranged_put->resume_token->num_parts_completed ==
                            auto_ranged_put->resume_token->total_num_parts) {
                        AWS_LOGF_DEBUG(
                            AWS_LS_S3_META_REQUEST,
                            "id=%p: Resuming PutObject ended early, since there is nothing to resume"
                            "(request finished prior to being paused?)",
                            (void *)meta_request);

                        aws_s3_meta_request_set_success_synced(meta_request, AWS_S3_RESPONSE_STATUS_SUCCESS);
                    } else {
                        aws_s3_meta_request_set_fail_synced(meta_request, request, error_code);
                    }
                }

                aws_s3_meta_request_unlock_synced_data(meta_request);
            }
            /* END CRITICAL SECTION */
            break;
        }

        case AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_CREATE_MULTIPART_UPLOAD: {
            struct aws_http_headers *needed_response_headers = NULL;

            if (error_code == AWS_ERROR_SUCCESS) {
                needed_response_headers = aws_http_headers_new(meta_request->allocator);
                const size_t copy_header_count = AWS_ARRAY_SIZE(s_create_multipart_upload_copy_headers);

                /* Copy any headers now that we'll need for the final, transformed headers later. */
                for (size_t header_index = 0; header_index < copy_header_count; ++header_index) {
                    const struct aws_byte_cursor *header_name = &s_create_multipart_upload_copy_headers[header_index];
                    struct aws_byte_cursor header_value;
                    AWS_ZERO_STRUCT(header_value);

                    if (aws_http_headers_get(request->send_data.response_headers, *header_name, &header_value) ==
                        AWS_OP_SUCCESS) {
                        aws_http_headers_set(needed_response_headers, *header_name, header_value);
                    }
                }

                struct aws_byte_cursor buffer_byte_cursor = aws_byte_cursor_from_buf(&request->send_data.response_body);

                /* Find the upload id for this multipart upload. */
                struct aws_string *upload_id =
                    aws_xml_get_top_level_tag(meta_request->allocator, &s_upload_id, &buffer_byte_cursor);

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

            /* BEGIN CRITICAL SECTION */
            {
                aws_s3_meta_request_lock_synced_data(meta_request);

                AWS_ASSERT(auto_ranged_put->synced_data.needed_response_headers == NULL);
                auto_ranged_put->synced_data.needed_response_headers = needed_response_headers;

                auto_ranged_put->synced_data.create_multipart_upload_completed = true;
                auto_ranged_put->synced_data.list_parts_error_code = error_code;

                if (error_code != AWS_ERROR_SUCCESS) {
                    aws_s3_meta_request_set_fail_synced(meta_request, request, error_code);
                }

                aws_s3_meta_request_unlock_synced_data(meta_request);
            }
            /* END CRITICAL SECTION */
            break;
        }

        case AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_PART: {
            size_t part_number = request->part_number;
            AWS_FATAL_ASSERT(part_number > 0);
            size_t part_index = part_number - 1;
            struct aws_string *etag = NULL;
            bool request_is_noop = request->is_noop != 0;

            if (!request_is_noop) {
                if (error_code == AWS_ERROR_SUCCESS) {
                    /* Find the ETag header if it exists and cache it. */
                    struct aws_byte_cursor etag_within_quotes;

                    AWS_ASSERT(request->send_data.response_headers);

                    if (aws_http_headers_get(
                            request->send_data.response_headers, g_etag_header_name, &etag_within_quotes) !=
                        AWS_OP_SUCCESS) {
                        AWS_LOGF_ERROR(
                            AWS_LS_S3_META_REQUEST,
                            "id=%p Could not find ETag header for request %p",
                            (void *)meta_request,
                            (void *)request);

                        error_code = AWS_ERROR_S3_MISSING_ETAG;
                    } else {
                        /* The ETag value arrives in quotes, but we don't want it in quotes when we send it back up
                         * later, so just get rid of the quotes now. */
                        etag = aws_strip_quotes(meta_request->allocator, etag_within_quotes);
                    }
                }
                if (error_code == AWS_ERROR_SUCCESS && meta_request->progress_callback != NULL) {
                    struct aws_s3_meta_request_progress progress = {
                        .bytes_transferred = request->request_body.len,
                        .content_length = auto_ranged_put->content_length,
                    };
                    meta_request->progress_callback(meta_request, &progress, meta_request->user_data);
                }
            }

            /* BEGIN CRITICAL SECTION */
            {
                aws_s3_meta_request_lock_synced_data(meta_request);

                ++auto_ranged_put->synced_data.num_parts_completed;

                if (request_is_noop) {
                    ++auto_ranged_put->synced_data.num_parts_noop;
                }

                if (auto_ranged_put->has_content_length) {
                    AWS_LOGF_DEBUG(
                        AWS_LS_S3_META_REQUEST,
                        "id=%p: %d out of %d parts have completed.",
                        (void *)meta_request,
                        auto_ranged_put->synced_data.num_parts_completed,
                        auto_ranged_put->total_num_parts_from_content_length);
                } else {
                    AWS_LOGF_DEBUG(
                        AWS_LS_S3_META_REQUEST,
                        "id=%p: %d parts have completed.",
                        (void *)meta_request,
                        auto_ranged_put->synced_data.num_parts_completed);
                }

                if (!request_is_noop) {
                    if (error_code == AWS_ERROR_SUCCESS) {
                        AWS_ASSERT(etag != NULL);

                        ++auto_ranged_put->synced_data.num_parts_successful;

                        /* Store part's ETag */
                        struct aws_s3_mpu_part_info *part = NULL;
                        aws_array_list_get_at(&auto_ranged_put->synced_data.part_list, &part, part_index);
                        AWS_ASSERT(part != NULL);
                        AWS_ASSERT(part->etag == NULL);
                        part->etag = etag;
                    } else {
                        ++auto_ranged_put->synced_data.num_parts_failed;
                        aws_s3_meta_request_set_fail_synced(meta_request, request, error_code);
                    }
                }

                aws_s3_meta_request_unlock_synced_data(meta_request);
            }
            /* END CRITICAL SECTION */

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

                /* BEGIN CRITICAL SECTION */
                {
                    aws_s3_meta_request_lock_synced_data(meta_request);
                    copy_http_headers(auto_ranged_put->synced_data.needed_response_headers, final_response_headers);
                    aws_s3_meta_request_unlock_synced_data(meta_request);
                }
                /* END CRITICAL SECTION */

                struct aws_byte_cursor response_body_cursor =
                    aws_byte_cursor_from_buf(&request->send_data.response_body);

                /**
                 * TODO: The body of the response can be ERROR, check Error specified in body part from
                 * https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html#AmazonS3-CompleteMultipartUpload-response-CompleteMultipartUploadOutput
                 * We need to handle this case.
                 * TODO: the checksum returned within the response of complete multipart upload need to be exposed?
                 */

                /* Grab the ETag for the entire object, and set it as a header. */
                struct aws_string *etag_header_value =
                    aws_xml_get_top_level_tag(meta_request->allocator, &g_etag_header_name, &response_body_cursor);

                if (etag_header_value != NULL) {
                    struct aws_byte_buf etag_header_value_byte_buf;
                    AWS_ZERO_STRUCT(etag_header_value_byte_buf);

                    aws_replace_quote_entities(meta_request->allocator, etag_header_value, &etag_header_value_byte_buf);

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
                meta_request->headers_callback = NULL;

                aws_http_headers_release(final_response_headers);
            }

            /* BEGIN CRITICAL SECTION */
            {
                aws_s3_meta_request_lock_synced_data(meta_request);
                auto_ranged_put->synced_data.complete_multipart_upload_completed = true;
                auto_ranged_put->synced_data.complete_multipart_upload_error_code = error_code;

                if (error_code != AWS_ERROR_SUCCESS) {
                    aws_s3_meta_request_set_fail_synced(meta_request, request, error_code);
                }
                aws_s3_meta_request_unlock_synced_data(meta_request);
            }
            /* END CRITICAL SECTION */

            break;
        }
        case AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_ABORT_MULTIPART_UPLOAD: {
            /* BEGIN CRITICAL SECTION */
            {
                aws_s3_meta_request_lock_synced_data(meta_request);
                auto_ranged_put->synced_data.abort_multipart_upload_error_code = error_code;
                auto_ranged_put->synced_data.abort_multipart_upload_completed = true;
                aws_s3_meta_request_unlock_synced_data(meta_request);
            }
            /* END CRITICAL SECTION */
            break;
        }
    }
}

static int s_s3_auto_ranged_put_pause(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_meta_request_resume_token **out_resume_token) {

    *out_resume_token = NULL;

    struct aws_s3_auto_ranged_put *auto_ranged_put = meta_request->impl;
    if (!auto_ranged_put->has_content_length) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST, "id=%p: Failed to pause request with unknown content length", (void *)meta_request);
        return aws_raise_error(AWS_ERROR_UNSUPPORTED_OPERATION);
    }

    /* lock */
    aws_s3_meta_request_lock_synced_data(meta_request);

    AWS_LOGF_DEBUG(
        AWS_LS_S3_META_REQUEST,
        "id=%p: Pausing request with %u out of %u parts have completed.",
        (void *)meta_request,
        auto_ranged_put->synced_data.num_parts_completed,
        auto_ranged_put->total_num_parts_from_content_length);

    /* upload can be in one of several states:
     * - not started, i.e. we didn't even call crete mpu yet - return success,
     *   token is NULL and cancel the upload
     * - in the middle of upload - return success, create token and cancel
     *     upload
     * - complete MPU started - return success, generate token and try to cancel
     *   complete MPU
     */
    if (auto_ranged_put->synced_data.create_multipart_upload_completed) {

        *out_resume_token = aws_s3_meta_request_resume_token_new(meta_request->allocator);

        (*out_resume_token)->type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT;
        (*out_resume_token)->multipart_upload_id =
            aws_string_clone_or_reuse(meta_request->allocator, auto_ranged_put->upload_id);
        (*out_resume_token)->part_size = meta_request->part_size;
        (*out_resume_token)->total_num_parts = auto_ranged_put->total_num_parts_from_content_length;
        (*out_resume_token)->num_parts_completed = auto_ranged_put->synced_data.num_parts_completed;
    }

    /**
     * Cancels the meta request using the PAUSED flag to avoid deletion of uploaded parts.
     * This allows the client to resume the upload later, setting the persistable state in the meta request options.
     */
    aws_s3_meta_request_set_fail_synced(meta_request, NULL, AWS_ERROR_S3_PAUSED);

    /* unlock */
    aws_s3_meta_request_unlock_synced_data(meta_request);

    return AWS_OP_SUCCESS;
}
