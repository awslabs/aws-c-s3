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

static const struct aws_byte_cursor s_upload_id = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("UploadId");
static const size_t s_complete_multipart_upload_init_body_size_bytes = 512;
static const size_t s_abort_multipart_upload_init_body_size_bytes = 512;
/* For unknown length body we no longer know the number of parts. to avoid
 * resizing arrays for etags/checksums too much, those array start out with
 * capacity specified by the constant below. Note: constant has been arbitrary
 * picked to avoid using allocations and using too much memory. might change in future.
 */
static const uint32_t s_unknown_length_default_num_parts = 32;

/* For unknown length body we dont know how many parts we'll end up with.
 * We optimistically schedule upload requests, but cap the number of requests
 * being prepared by bellow number to avoid unknown length request dominating the
 * queue.
 * TODO: this value needs further benchmarking.*/
static const uint32_t s_unknown_length_max_optimistic_prepared_parts = 5;

static const struct aws_byte_cursor s_create_multipart_upload_copy_headers[] = {
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-server-side-encryption-customer-algorithm"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-server-side-encryption-customer-key-MD5"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-server-side-encryption-context"),
};

/* Context for aws_s3_auto_ranged_put's async vtable->prepare_request() operation */
struct aws_s3_auto_ranged_put_prepare_request_async_ctx {
    struct aws_allocator *allocator;
    struct aws_s3_request *request;
    struct aws_future *message_future; /* aws_future<aws_http_message *> for type-specific prep step */
    struct aws_future *my_future;      /* aws_future<void> to set when this operation completes */
};

/* Context for async preparation of an UploadPart request */
struct aws_s3_prepare_upload_part_async_ctx {
    struct aws_allocator *allocator;
    struct aws_s3_request *request;
    struct aws_future *skipping_future;  /* aws_future<bool> for skipping step */
    struct aws_future *read_part_future; /* aws_future<void> for reading step */
    struct aws_future *my_future;        /* aws_future<aws_http_message *> to set when this operation completes */
};

/* Context for async preparation of a CompleteMultipartUpload request */
struct aws_s3_prepare_complete_multipart_upload_async_ctx {
    struct aws_allocator *allocator;
    struct aws_s3_request *request;
    struct aws_future *skipping_future; /* aws_future<bool> for skipping step */
    struct aws_future *my_future;       // aws_future<aws_http_message *> to set when this operation completes */
};

static void s_s3_meta_request_auto_ranged_put_destroy(struct aws_s3_meta_request *meta_request);

static bool s_s3_auto_ranged_put_update(
    struct aws_s3_meta_request *meta_request,
    uint32_t flags,
    struct aws_s3_request **out_request);

static void s_skip_parts_from_stream_loop(void *user_data);

static struct aws_future *s_s3_auto_ranged_put_prepare_request(struct aws_s3_request *request);
static void s_s3_auto_ranged_put_prepare_request_finish(void *user_data);

static struct aws_future *s_s3_prepare_list_parts(struct aws_s3_request *request);

static struct aws_future *s_s3_prepare_create_multipart_upload(struct aws_s3_request *request);

static struct aws_future *s_s3_prepare_upload_part(struct aws_s3_request *request);
static void s_s3_prepare_upload_part_on_skipping_done(void *user_data);
static void s_s3_prepare_upload_part_on_read_done(void *user_data);
static void s_s3_prepare_upload_part_finish(struct aws_s3_prepare_upload_part_async_ctx *part_prep, int error_code);

static struct aws_future *s_s3_prepare_complete_multipart_upload(struct aws_s3_request *request);
static void s_s3_prepare_complete_multipart_upload_on_skipping_done(void *user_data);
static void s_s3_prepare_complete_multipart_upload_finish(
    struct aws_s3_prepare_complete_multipart_upload_async_ctx *complete_mpu_prep,
    int error_code);

static struct aws_future *s_s3_prepare_abort_multipart_upload(struct aws_s3_request *request);

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

static bool s_process_part_info(const struct aws_s3_part_info *info, void *user_data) {
    struct aws_s3_auto_ranged_put *auto_ranged_put = user_data;

    struct aws_string *etag = aws_strip_quotes(auto_ranged_put->base.allocator, info->e_tag);

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

    /* Parts might be out of order or have gaps in them. array list allows
     * setting elements outside of current length, but values between old length
     * and new value will be uninitialized. as a workaround when processing
     * element make sure to init any new elements to zeroed values. */
    size_t current_num_parts = aws_array_list_length(&auto_ranged_put->synced_data.etag_list);
    if (info->part_number > current_num_parts) {
        struct aws_byte_buf empty_buf = {0};
        struct aws_string *null_etag = NULL;

        /* Note: using 1 based part nums here to avoid dealing with underflow of
        unsigned number in case part 1 was not completed, but other parts were. */
        for (size_t part_num = info->part_number; part_num > current_num_parts; --part_num) {
            aws_array_list_set_at(&auto_ranged_put->synced_data.encoded_checksum_list, &empty_buf, part_num - 1);
            aws_array_list_set_at(&auto_ranged_put->synced_data.etag_list, &null_etag, part_num - 1);
        }
    }

    if (checksum_cur) {
        struct aws_byte_buf checksum_buf;
        aws_byte_buf_init_copy_from_cursor(&checksum_buf, auto_ranged_put->base.allocator, *checksum_cur);
        aws_array_list_set_at(
            &auto_ranged_put->synced_data.encoded_checksum_list, &checksum_buf, info->part_number - 1);
    }

    aws_array_list_set_at(&auto_ranged_put->synced_data.etag_list, &etag, info->part_number - 1);

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

    auto_ranged_put->synced_data.num_parts_sent = 0;
    auto_ranged_put->synced_data.num_parts_completed = 0;
    auto_ranged_put->synced_data.num_parts_noop = 0;
    auto_ranged_put->synced_data.create_multipart_upload_sent = true;
    auto_ranged_put->synced_data.create_multipart_upload_completed = true;
    auto_ranged_put->upload_id = aws_string_clone_or_reuse(allocator, resume_token->multipart_upload_id);

    struct aws_s3_list_parts_params list_parts_params = {
        .key = request_path,
        .upload_id = aws_byte_cursor_from_string(auto_ranged_put->upload_id),
        .on_part = s_process_part_info,
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
    auto_ranged_put->synced_data.total_num_parts = has_content_length ? num_parts : 0;
    auto_ranged_put->upload_id = NULL;
    auto_ranged_put->resume_token = options->resume_token;

    aws_s3_meta_request_resume_token_acquire(auto_ranged_put->resume_token);

    auto_ranged_put->threaded_update_data.next_part_number = 1;
    auto_ranged_put->prepare_data.num_parts_read_from_stream = 0;
    auto_ranged_put->synced_data.is_body_stream_at_end = false;

    uint32_t initial_num_parts = auto_ranged_put->has_content_length ? num_parts : s_unknown_length_default_num_parts;

    aws_array_list_init_dynamic(
        &auto_ranged_put->synced_data.etag_list, allocator, initial_num_parts, sizeof(struct aws_string *));
    aws_array_list_init_dynamic(
        &auto_ranged_put->synced_data.encoded_checksum_list, allocator, initial_num_parts, sizeof(struct aws_byte_buf));

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

    for (size_t etag_index = 0; etag_index < aws_array_list_length(&auto_ranged_put->synced_data.etag_list);
         ++etag_index) {
        struct aws_string *etag = NULL;

        aws_array_list_get_at(&auto_ranged_put->synced_data.etag_list, &etag, etag_index);
        aws_string_destroy(etag);
    }

    aws_string_destroy(auto_ranged_put->synced_data.list_parts_continuation_token);

    for (size_t checksum_index = 0;
         checksum_index < aws_array_list_length(&auto_ranged_put->synced_data.encoded_checksum_list);
         ++checksum_index) {

        struct aws_byte_buf checksum_buf;
        aws_array_list_get_at(&auto_ranged_put->synced_data.encoded_checksum_list, &checksum_buf, checksum_index);

        aws_byte_buf_clean_up(&checksum_buf);
    }
    aws_array_list_clean_up(&auto_ranged_put->synced_data.etag_list);
    aws_array_list_clean_up(&auto_ranged_put->synced_data.encoded_checksum_list);
    aws_http_headers_release(auto_ranged_put->synced_data.needed_response_headers);
    aws_mem_release(meta_request->allocator, auto_ranged_put);
}

/* Check flags and corresponding conditions to see if any more parts can be
 * scheduled during this pass. */
static bool s_should_skip_scheduling_more_parts_based_on_flags(
    const struct aws_s3_auto_ranged_put *auto_ranged_put,
    uint32_t flags) {
    if ((flags & AWS_S3_META_REQUEST_UPDATE_FLAG_CONSERVATIVE) != 0) {
        uint32_t num_parts_in_flight =
            (auto_ranged_put->synced_data.num_parts_sent - auto_ranged_put->synced_data.num_parts_completed);

        /* Because uploads must read from their streams serially, we try to limit the amount of in flight
         * requests for a given multipart upload if we can. */
        return num_parts_in_flight > 0;
    }

    return false;
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
            if (auto_ranged_put->has_content_length &&
                (auto_ranged_put->synced_data.num_parts_sent < auto_ranged_put->synced_data.total_num_parts)) {

                /* Check if the etag/checksum list has the result already */
                int part_index = auto_ranged_put->threaded_update_data.next_part_number - 1;
                for (size_t etag_index = part_index;
                     etag_index < aws_array_list_length(&auto_ranged_put->synced_data.etag_list);
                     ++etag_index) {
                    struct aws_string *etag = NULL;

                    if (!aws_array_list_get_at(&auto_ranged_put->synced_data.etag_list, &etag, etag_index) && etag) {
                        /* part already downloaded, skip it here and prepare will take care of adjusting the buffer
                         */
                        ++auto_ranged_put->threaded_update_data.next_part_number;

                    } else {
                        // incomplete part found. break out and create request for it.
                        break;
                    }
                }

                // Something went really wrong. we still have parts to send, but have etags for all parts
                AWS_FATAL_ASSERT(
                    auto_ranged_put->threaded_update_data.next_part_number <=
                    auto_ranged_put->synced_data.total_num_parts);

                if (s_should_skip_scheduling_more_parts_based_on_flags(auto_ranged_put, flags)) {
                    goto has_work_remaining;
                }

                should_create_next_part_request = true;

            } else if (!auto_ranged_put->has_content_length && !auto_ranged_put->synced_data.is_body_stream_at_end) {

                if (s_should_skip_scheduling_more_parts_based_on_flags(auto_ranged_put, flags)) {
                    goto has_work_remaining;
                }

                uint32_t num_parts_not_read =
                    (auto_ranged_put->synced_data.num_parts_sent - auto_ranged_put->synced_data.num_parts_read);

                /* Because uploads must read from their streams serially, we try to limit the amount of in flight
                 * requests for a given multipart upload if we can. */
                if (num_parts_not_read >= s_unknown_length_max_optimistic_prepared_parts) {
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
                ++auto_ranged_put->synced_data.num_parts_sent;

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
                if (auto_ranged_put->synced_data.num_parts_completed != auto_ranged_put->synced_data.total_num_parts) {
                    goto has_work_remaining;
                }
            } else {
                if ((!auto_ranged_put->synced_data.is_body_stream_at_end) ||
                    auto_ranged_put->synced_data.num_parts_completed != auto_ranged_put->synced_data.num_parts_sent) {
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
            if (auto_ranged_put->synced_data.num_parts_completed < auto_ranged_put->synced_data.num_parts_sent) {
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
    if (auto_ranged_put->has_content_length && part_number == auto_ranged_put->synced_data.total_num_parts) {
        size_t content_remainder = (size_t)(auto_ranged_put->content_length % (uint64_t)meta_request->part_size);

        if (content_remainder > 0) {
            request_body_size = content_remainder;
        }
    }

    return request_body_size;
}

static int s_verify_part_matches_checksum(
    struct aws_allocator *allocator,
    struct aws_byte_buf part_body,
    enum aws_s3_checksum_algorithm algorithm,
    struct aws_byte_buf part_checksum) {
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
    struct aws_byte_cursor body_cur = aws_byte_cursor_from_buf(&part_body);

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

    if (!aws_byte_buf_eq(&encoded_checksum, &part_checksum)) {
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

/* Context for the async skip-parts-from-stream operation */
struct aws_s3_skip_parts_from_stream_async_ctx {
    struct aws_allocator *allocator;
    struct aws_s3_meta_request *meta_request;
    uint32_t part_index;
    uint32_t skip_until_part_number;
    struct aws_byte_buf temp_body_buf;
    struct aws_future *read_part_future; /* aws_future<bool> for each read-part step */
    struct aws_future *my_future;        /* aws_future<void> to set when this operation completes */
};

/**
 * Async operation that skips parts from input stream that were previously uploaded.
 * Assumes input stream has num_parts_read_from_stream specifying which part stream is on
 * and will read into temp buffer until it gets to skip_until_part_number (i.e. skipping does include
 * that part). If checksum is set on the request and parts with checksums were uploaded before, checksum will be
 * verified.
 *
 * Returns an aws_future<bool> indicating whether any parts were skipped.
 */
static struct aws_future *s_skip_parts_from_stream(
    struct aws_s3_meta_request *meta_request,
    uint32_t num_parts_read_from_stream,
    uint32_t skip_until_part_number) {

    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(num_parts_read_from_stream <= skip_until_part_number);

    const struct aws_s3_auto_ranged_put *auto_ranged_put = meta_request->impl;
    AWS_PRECONDITION(skip_until_part_number <= auto_ranged_put->synced_data.total_num_parts);

    struct aws_future *skip_future = aws_future_new(meta_request->allocator, AWS_FUTURE_BOOL);

    if (num_parts_read_from_stream == skip_until_part_number) {
        aws_future_set_bool(skip_future, false);
        return skip_future;
    }

    /* Store context for async operation */
    struct aws_s3_skip_parts_from_stream_async_ctx *skip_ctx =
        aws_mem_calloc(meta_request->allocator, 1, sizeof(struct aws_s3_skip_parts_from_stream_async_ctx));
    skip_ctx->allocator = meta_request->allocator;
    skip_ctx->meta_request = meta_request;
    skip_ctx->part_index = num_parts_read_from_stream;
    skip_ctx->skip_until_part_number = skip_until_part_number;
    skip_ctx->my_future = aws_future_acquire(skip_future);

    AWS_LOGF_DEBUG(
        AWS_LS_S3_META_REQUEST,
        "id=%p: Skipping parts %d through %d",
        (void *)meta_request,
        num_parts_read_from_stream,
        skip_until_part_number);

    /* Kick off async work loop */
    s_skip_parts_from_stream_loop(skip_ctx);

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
    struct aws_s3_skip_parts_from_stream_async_ctx *skip_ctx = user_data;
    struct aws_s3_meta_request *meta_request = skip_ctx->meta_request;
    struct aws_s3_auto_ranged_put *auto_ranged_put = meta_request->impl;
    struct aws_byte_buf *temp_body_buf = &skip_ctx->temp_body_buf;

    int error_code = AWS_ERROR_SUCCESS;

    for (; skip_ctx->part_index < skip_ctx->skip_until_part_number; ++skip_ctx->part_index) {

        /* unless we just re-entered loop due to async read completion, kick off a new async read */
        if (skip_ctx->read_part_future == NULL) {
            size_t request_body_size = s_compute_request_body_size(meta_request, skip_ctx->part_index + 1);

            if (temp_body_buf->capacity != request_body_size) {
                // reinit with correct size
                aws_byte_buf_clean_up(temp_body_buf);
                aws_byte_buf_init(temp_body_buf, meta_request->allocator, request_body_size);
            } else {
                // reuse buffer
                aws_byte_buf_reset(temp_body_buf, false);
            }

            skip_ctx->read_part_future = aws_s3_meta_request_read_body(skip_ctx->meta_request, temp_body_buf);

            /* the read may or may not complete immediately */
            if (aws_future_register_callback_if_not_done(
                    skip_ctx->read_part_future, s_skip_parts_from_stream_loop, skip_ctx)) {

                /* read is pending, we'll resume this loop when callback fires */
                return;
            }
        }

        /* read is complete, check results */
        error_code = aws_future_get_error(skip_ctx->read_part_future);
        skip_ctx->read_part_future = aws_future_release(skip_ctx->read_part_future); /* release and set NULL */

        if (error_code != AWS_ERROR_SUCCESS) {
            AWS_LOGF_ERROR(
                AWS_LS_S3_META_REQUEST,
                "id=%p: Failed to resume upload. Input stream read error %d (%s)",
                (void *)meta_request,
                error_code,
                aws_error_str(error_code));
            error_code = AWS_ERROR_S3_RESUME_FAILED;
            goto on_done;
        }

        if (temp_body_buf->len < temp_body_buf->capacity) {
            AWS_LOGF_ERROR(
                AWS_LS_S3_META_REQUEST,
                "id=%p: Failed to resume upload. Input steam ended prematurely.",
                (void *)meta_request);
            error_code = AWS_ERROR_S3_RESUME_FAILED;
            goto on_done;
        }

        struct aws_byte_buf checksum_buf;
        aws_array_list_get_at(&auto_ranged_put->synced_data.encoded_checksum_list, &checksum_buf, skip_ctx->part_index);

        // compare skipped checksum to previously uploaded checksum
        if (checksum_buf.len > 0 && s_verify_part_matches_checksum(
                                        meta_request->allocator,
                                        *temp_body_buf,
                                        meta_request->checksum_config.checksum_algorithm,
                                        checksum_buf)) {
            error_code = aws_last_error_or_unknown();
            goto on_done;
        }
    }

on_done:
    aws_byte_buf_clean_up(&skip_ctx->temp_body_buf);
    if (error_code == AWS_ERROR_SUCCESS) {
        aws_future_set_bool(skip_ctx->my_future, true);
    } else {
        aws_future_set_error(skip_ctx->my_future, error_code);
    }
    aws_future_release(skip_ctx->my_future);
    aws_mem_release(skip_ctx->allocator, skip_ctx);
}

/* Destructor for pointer within aws_future<aws_http_message *> */
static void s_destroy_message_pointer(void *user_data) {
    struct aws_http_message *message = user_data;
    aws_http_message_release(message);
}

/* Given a request, prepare it for sending based on its description. */
static struct aws_future *s_s3_auto_ranged_put_prepare_request(struct aws_s3_request *request) {

    struct aws_future *preparation_future = aws_future_new(request->allocator, AWS_FUTURE_VALUELESS);

    /* Store context for async operation */
    struct aws_s3_auto_ranged_put_prepare_request_async_ctx *request_prep =
        aws_mem_calloc(request->allocator, 1, sizeof(struct aws_s3_auto_ranged_put_prepare_request_async_ctx));
    request_prep->allocator = request->allocator;
    request_prep->my_future = aws_future_acquire(preparation_future);
    request_prep->request = request;

    /* Each type of request prepares an aws_http_message in its own way, which maybe require async substeps */
    switch (request->request_tag) {
        case AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_LIST_PARTS:
            request_prep->message_future = s_s3_prepare_list_parts(request);
            break;
        case AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_CREATE_MULTIPART_UPLOAD:
            request_prep->message_future = s_s3_prepare_create_multipart_upload(request);
            break;
        case AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_PART:
            request_prep->message_future = s_s3_prepare_upload_part(request);
            break;
        case AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_COMPLETE_MULTIPART_UPLOAD:
            request_prep->message_future = s_s3_prepare_complete_multipart_upload(request);
            break;
        case AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_ABORT_MULTIPART_UPLOAD:
            request_prep->message_future = s_s3_prepare_abort_multipart_upload(request);
            break;
        default:
            AWS_FATAL_ASSERT(0);
            break;
    }

    /* When the specific type of message is ready, finish common preparation steps */
    aws_future_register_callback(
        request_prep->message_future, s_s3_auto_ranged_put_prepare_request_finish, request_prep);

    return preparation_future;
}

/* Prepare a ListParts request.
 * Currently, this is actually synchronous. */
static struct aws_future *s_s3_prepare_list_parts(struct aws_s3_request *request) {
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

    if (message_creation_result) {
        goto on_done;
    }
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

on_done:;
    struct aws_future *future = aws_future_new(request->allocator, AWS_FUTURE_POINTER);
    if (message == NULL) {
        aws_future_set_error(future, aws_last_error_or_unknown());
    } else {
        aws_future_set_pointer(future, message, s_destroy_message_pointer);
    }
    return future;
}

/* Prepare a CreateMultipartUpload request.
 * Currently, this is actually synchronous. */
struct aws_future *s_s3_prepare_create_multipart_upload(struct aws_s3_request *request) {
    struct aws_s3_meta_request *meta_request = request->meta_request;

    /* Create the message to create a new multipart upload. */
    struct aws_http_message *message = aws_s3_create_multipart_upload_message_new(
        meta_request->allocator,
        meta_request->initial_request_message,
        meta_request->checksum_config.checksum_algorithm);

    struct aws_future *future = aws_future_new(request->allocator, AWS_FUTURE_POINTER);
    if (message != NULL) {
        aws_future_set_pointer(future, message, s_destroy_message_pointer);
    } else {
        aws_future_set_error(future, aws_last_error_or_unknown());
    }
    return future;
}

/* Prepare an UploadPart request */
struct aws_future *s_s3_prepare_upload_part(struct aws_s3_request *request) {
    struct aws_s3_meta_request *meta_request = request->meta_request;
    struct aws_s3_auto_ranged_put *auto_ranged_put = meta_request->impl;
    struct aws_allocator *allocator = request->allocator;

    struct aws_future *message_future = aws_future_new(allocator, AWS_FUTURE_POINTER);

    struct aws_s3_prepare_upload_part_async_ctx *part_prep =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_prepare_upload_part_async_ctx));
    part_prep->allocator = allocator;
    part_prep->request = request;
    part_prep->my_future = aws_future_acquire(message_future);

    if (request->num_times_prepared == 0) {
        /* Preparing request for the first time.
         * Next async step: read through the body stream until we've
         * skipped over parts that were already uploaded (in case we're resuming
         * from an upload that had been paused) */
        if (auto_ranged_put->has_content_length) {
            part_prep->skipping_future = s_skip_parts_from_stream(
                meta_request, auto_ranged_put->prepare_data.num_parts_read_from_stream, request->part_number - 1);
            aws_future_register_callback(
                part_prep->skipping_future, s_s3_prepare_upload_part_on_skipping_done, part_prep);
        } else {
            /* Skip skipping parts if content_length is not available as we don't support pause/resume for now */
            ++auto_ranged_put->synced_data.total_num_parts;
            part_prep->skipping_future = aws_future_new(allocator, AWS_FUTURE_BOOL);
            aws_future_set_bool(part_prep->skipping_future, false);
            s_s3_prepare_upload_part_on_skipping_done(part_prep);
        }
    } else {
        /* Not the first time preparing request (e.g. retry).
         * We can skip over the async steps that read the body stream */
        s_s3_prepare_upload_part_finish(part_prep, AWS_ERROR_SUCCESS);
    }

    return message_future;
}

/* Completion callback for skipping over parts that were previously uploaded. */
static void s_s3_prepare_upload_part_on_skipping_done(void *user_data) {
    struct aws_s3_prepare_upload_part_async_ctx *part_prep = user_data;
    struct aws_s3_request *request = part_prep->request;
    struct aws_s3_meta_request *meta_request = request->meta_request;
    struct aws_s3_auto_ranged_put *auto_ranged_put = meta_request->impl;

    int error_code = aws_future_get_error(part_prep->skipping_future);

    /* If skipping failed, the prepare-upload-part operation has failed. */
    if (error_code) {
        s_s3_prepare_upload_part_finish(part_prep, error_code);
        return;
    }
    if (aws_future_get_bool(part_prep->skipping_future)) {
        /* Skipping succeeded */
        auto_ranged_put->prepare_data.num_parts_read_from_stream = request->part_number - 1;
    }

    /* Next async step: read body stream for this part into a buffer */
    size_t request_body_size = s_compute_request_body_size(meta_request, request->part_number);

    aws_byte_buf_init(&request->request_body, meta_request->allocator, request_body_size);

    part_prep->read_part_future = aws_s3_meta_request_read_body(meta_request, &request->request_body);
    aws_future_register_callback(part_prep->read_part_future, s_s3_prepare_upload_part_on_read_done, part_prep);
}

/* Completion callback for reading this part's chunk of the body stream */
static void s_s3_prepare_upload_part_on_read_done(void *user_data) {
    struct aws_s3_prepare_upload_part_async_ctx *part_prep = user_data;
    struct aws_s3_request *request = part_prep->request;
    struct aws_s3_meta_request *meta_request = request->meta_request;
    struct aws_s3_auto_ranged_put *auto_ranged_put = meta_request->impl;

    int error_code = aws_future_get_error(part_prep->read_part_future);

    if (error_code != AWS_ERROR_SUCCESS) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST,
            "id=%p: Failed reading request body, error %d (%s)",
            (void *)meta_request,
            error_code,
            aws_error_str(error_code));
        goto on_done;
    }

    if (auto_ranged_put->has_content_length && request->request_body.len < request->request_body.capacity) {
        error_code = aws_raise_error(AWS_ERROR_S3_INCORRECT_CONTENT_LENGTH_HEADER);
        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST,
            "id=%p: Request body is smaller than 'Content-Length' header said it would be",
            (void *)meta_request);
        goto on_done;
    }

    if (!auto_ranged_put->has_content_length) {
        /* BEGIN CRITICAL SECTION */
        {
            aws_s3_meta_request_lock_synced_data(meta_request);

            if (request->request_body.len == 0) {
                request->is_noop = true;
                ++auto_ranged_put->synced_data.num_parts_read;
                auto_ranged_put->synced_data.is_body_stream_at_end = true;

                AWS_LOGF_DEBUG(
                    AWS_LS_S3_META_REQUEST,
                    "id=%p UploadPart with part num %u for Multi-part Upload, with ID:%s"
                    "is noop due to encountering end of stream",
                    (void *)meta_request,
                    request->part_number,
                    aws_string_c_str(auto_ranged_put->upload_id));
            } else if (request->request_body.len < request->request_body.capacity) {
                auto_ranged_put->synced_data.is_body_stream_at_end = true;
            }
            aws_s3_meta_request_unlock_synced_data(meta_request);
        }
        /* END CRITICAL SECTION */
    }

    if (!request->is_noop) {
        /* Reset values for corresponding checksum and etag.
         * Note: During resume flow this might cause the values to be
         * reset twice (if we are preparing part in between
         * previously completed parts). */
        struct aws_byte_buf checksum_buf = {0};
        aws_array_list_set_at(
            &auto_ranged_put->synced_data.encoded_checksum_list, &checksum_buf, request->part_number - 1);

        struct aws_string *null_etag = NULL;
        aws_array_list_set_at(&auto_ranged_put->synced_data.etag_list, &null_etag, request->part_number - 1);

        aws_s3_meta_request_unlock_synced_data(meta_request);
        ++auto_ranged_put->prepare_data.num_parts_read_from_stream;
    }

on_done:
    s_s3_prepare_upload_part_finish(part_prep, error_code);
}

/* Finish async preparation of an UploadPart request */
static void s_s3_prepare_upload_part_finish(struct aws_s3_prepare_upload_part_async_ctx *part_prep, int error_code) {
    struct aws_s3_request *request = part_prep->request;
    struct aws_s3_meta_request *meta_request = request->meta_request;
    struct aws_s3_auto_ranged_put *auto_ranged_put = meta_request->impl;

    if (error_code != AWS_ERROR_SUCCESS) {
        aws_future_set_error(part_prep->my_future, error_code);
        goto on_done;
    }

    struct aws_byte_buf *checksum_buf = NULL;
    if (!request->is_noop) {
        aws_array_list_get_at_ptr(
            &auto_ranged_put->synced_data.encoded_checksum_list, (void **)&checksum_buf, request->part_number - 1);
        /* Clean up the buffer in case of it's initialized before and retry happens. */
        aws_byte_buf_clean_up(checksum_buf);
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
        aws_future_set_error(part_prep->my_future, aws_last_error());
        goto on_done;
    }

    /* Success! */
    aws_future_set_pointer(part_prep->my_future, message, s_destroy_message_pointer);

on_done:
    AWS_FATAL_ASSERT(aws_future_is_done(part_prep->my_future));
    aws_future_release(part_prep->skipping_future);
    aws_future_release(part_prep->read_part_future);
    aws_future_release(part_prep->my_future);
    aws_mem_release(part_prep->allocator, part_prep);
}

/* Prepare a CompleteMultipartUpload request. */
static struct aws_future *s_s3_prepare_complete_multipart_upload(struct aws_s3_request *request) {
    struct aws_s3_meta_request *meta_request = request->meta_request;
    struct aws_s3_auto_ranged_put *auto_ranged_put = meta_request->impl;
    struct aws_allocator *allocator = request->allocator;

    struct aws_future *message_future = aws_future_new(allocator, AWS_FUTURE_POINTER);
    /* Note: completeMPU fails if no parts are provided. We could
     * workaround it by uploading an empty part at the cost of
     * complicating flow logic for dealing with noop parts, but that
     * arguably adds a lot of complexity for little benefit.
     * Pre-buffering parts to determine whether mpu is needed will
     * resolve this issue.
     */
    if (!auto_ranged_put->has_content_length && auto_ranged_put->prepare_data.num_parts_read_from_stream == 0) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST,
            "id=%p 0 byte meta requests without Content-Length header are currently not supported. Set "
            "Content-Length header to 0 to upload empty object",
            (void *)meta_request);
        aws_future_set_error(message_future, AWS_ERROR_UNSUPPORTED_OPERATION);
        return message_future;
    }
    struct aws_s3_prepare_complete_multipart_upload_async_ctx *complete_mpu_prep =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_prepare_complete_multipart_upload_async_ctx));
    complete_mpu_prep->allocator = allocator;
    complete_mpu_prep->request = request;
    complete_mpu_prep->my_future = aws_future_acquire(message_future);

    if (request->num_times_prepared == 0) {

        /* Corner case of last part being previously uploaded during resume.
         * Read it from input stream and potentially verify checksum */

        if (auto_ranged_put->has_content_length) {
            complete_mpu_prep->skipping_future = s_skip_parts_from_stream(
                meta_request,
                auto_ranged_put->prepare_data.num_parts_read_from_stream,
                auto_ranged_put->synced_data.total_num_parts);
            aws_future_register_callback(
                complete_mpu_prep->skipping_future,
                s_s3_prepare_complete_multipart_upload_on_skipping_done,
                complete_mpu_prep);
        } else {
            /* Skip skipping parts if content_length is not available as we don't support pause/resume for now */
            complete_mpu_prep->skipping_future = aws_future_new(allocator, AWS_FUTURE_BOOL);
            aws_future_set_bool(complete_mpu_prep->skipping_future, false);
            s_s3_prepare_complete_multipart_upload_on_skipping_done(complete_mpu_prep);
        }

    } else {
        /* Not the first time preparing request (e.g. retry).
         * We can skip over the async steps. */
        s_s3_prepare_complete_multipart_upload_finish(complete_mpu_prep, AWS_ERROR_SUCCESS);
    }

    return message_future;
}

/* Completion callback for skipping over parts that were previously uploaded. */
static void s_s3_prepare_complete_multipart_upload_on_skipping_done(void *user_data) {

    struct aws_s3_prepare_complete_multipart_upload_async_ctx *complete_mpu_prep = user_data;
    struct aws_s3_request *request = complete_mpu_prep->request;
    struct aws_s3_meta_request *meta_request = request->meta_request;
    struct aws_s3_auto_ranged_put *auto_ranged_put = meta_request->impl;

    int error_code = aws_future_get_error(complete_mpu_prep->skipping_future);
    if (error_code != AWS_ERROR_SUCCESS) {
        s_s3_prepare_complete_multipart_upload_finish(complete_mpu_prep, error_code);
        return;
    }

    /* Skipping was successful */
    auto_ranged_put->prepare_data.num_parts_read_from_stream = auto_ranged_put->synced_data.total_num_parts;

    aws_byte_buf_init(
        &request->request_body, meta_request->allocator, s_complete_multipart_upload_init_body_size_bytes);

    /* Async steps complete, finish up operation */
    s_s3_prepare_complete_multipart_upload_finish(complete_mpu_prep, AWS_ERROR_SUCCESS);
}

/* Finish async preparation of a CompleteMultipartUpload request */
static void s_s3_prepare_complete_multipart_upload_finish(
    struct aws_s3_prepare_complete_multipart_upload_async_ctx *complete_mpu_prep,
    int error_code) {

    struct aws_s3_request *request = complete_mpu_prep->request;
    struct aws_s3_meta_request *meta_request = request->meta_request;
    struct aws_s3_auto_ranged_put *auto_ranged_put = meta_request->impl;

    if (error_code != AWS_ERROR_SUCCESS) {
        aws_future_set_error(complete_mpu_prep->my_future, error_code);
        goto on_done;
    }

    AWS_FATAL_ASSERT(auto_ranged_put->upload_id);
    AWS_ASSERT(request->request_body.capacity > 0);
    aws_byte_buf_reset(&request->request_body, false);

    /* BEGIN CRITICAL SECTION */
    aws_s3_meta_request_lock_synced_data(meta_request);

    /* Build the message to complete our multipart upload, which includes a payload describing all of
     * our completed parts. */
    struct aws_http_message *message = aws_s3_complete_multipart_message_new(
        meta_request->allocator,
        meta_request->initial_request_message,
        &request->request_body,
        auto_ranged_put->upload_id,
        &auto_ranged_put->synced_data.etag_list,
        &auto_ranged_put->synced_data.encoded_checksum_list,
        meta_request->checksum_config.checksum_algorithm);

    aws_s3_meta_request_unlock_synced_data(meta_request);
    /* END CRITICAL SECTION */

    if (message == NULL) {
        aws_future_set_error(complete_mpu_prep->my_future, aws_last_error());
        goto on_done;
    }

    /* Success! */
    aws_future_set_pointer(complete_mpu_prep->my_future, message, s_destroy_message_pointer);

on_done:
    AWS_FATAL_ASSERT(aws_future_is_done(complete_mpu_prep->my_future));
    aws_future_release(complete_mpu_prep->skipping_future);
    aws_future_release(complete_mpu_prep->my_future);
    aws_mem_release(complete_mpu_prep->allocator, complete_mpu_prep);
}

/* Prepare an AbortMultipartUpload request.
 * Currently, this is actually synchronous. */
struct aws_future *s_s3_prepare_abort_multipart_upload(struct aws_s3_request *request) {
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

    struct aws_future *future = aws_future_new(request->allocator, AWS_FUTURE_POINTER);
    if (message != NULL) {
        aws_future_set_pointer(future, message, s_destroy_message_pointer);
    } else {
        aws_future_set_error(future, aws_last_error_or_unknown());
    }
    return future;
}

/* Finish the vtable->prepare_request() operation */
static void s_s3_auto_ranged_put_prepare_request_finish(void *user_data) {
    struct aws_s3_auto_ranged_put_prepare_request_async_ctx *request_prep = user_data;
    struct aws_s3_request *request = request_prep->request;
    struct aws_s3_meta_request *meta_request = request->meta_request;

    /* Did we successfully create the type-specific HTTP message? */
    int error_code = aws_future_get_error(request_prep->message_future);
    if (error_code != AWS_ERROR_SUCCESS) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST,
            "id=%p Could not allocate message for request with tag %d for auto-ranged-put meta request.",
            (void *)meta_request,
            request->request_tag);

        goto on_done;
    }

    /* Success! Apply aws_http_message to aws_s3_request */
    struct aws_http_message *message = aws_future_get_pointer(request_prep->message_future);
    aws_s3_request_setup_send_data(request, message);

    AWS_LOGF_DEBUG(
        AWS_LS_S3_META_REQUEST,
        "id=%p: Prepared request %p for part %d",
        (void *)meta_request,
        (void *)request,
        request->part_number);

on_done:
    if (error_code == AWS_ERROR_SUCCESS) {
        aws_future_set_valueless(request_prep->my_future);
    } else {
        aws_future_set_error(request_prep->my_future, error_code);
    }

    aws_future_release(request_prep->message_future);
    aws_future_release(request_prep->my_future);
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
                        for (size_t etag_index = 0;
                             etag_index < aws_array_list_length(&auto_ranged_put->synced_data.etag_list);
                             etag_index++) {
                            struct aws_string *etag = NULL;
                            aws_array_list_get_at(&auto_ranged_put->synced_data.etag_list, &etag, etag_index);
                            if (etag != NULL) {
                                /* Update the number of parts sent/completed previously */
                                ++auto_ranged_put->synced_data.num_parts_sent;
                                ++auto_ranged_put->synced_data.num_parts_completed;
                            }
                        }

                        AWS_LOGF_DEBUG(
                            AWS_LS_S3_META_REQUEST,
                            "id=%p: Resuming PutObject. %d out of %d parts have completed during previous request.",
                            (void *)meta_request,
                            auto_ranged_put->synced_data.num_parts_completed,
                            auto_ranged_put->synced_data.total_num_parts);
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
                        .bytes_transferred = meta_request->part_size,
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

                AWS_LOGF_DEBUG(
                    AWS_LS_S3_META_REQUEST,
                    "id=%p: %d out of %d parts have completed.",
                    (void *)meta_request,
                    auto_ranged_put->synced_data.num_parts_completed,
                    auto_ranged_put->synced_data.total_num_parts);

                if (!request_is_noop) {
                    if (error_code == AWS_ERROR_SUCCESS) {
                        AWS_ASSERT(etag != NULL);

                        ++auto_ranged_put->synced_data.num_parts_successful;

                        /* ETags need to be associated with their part number, so we keep the etag indices consistent
                         * with part numbers. This means we may have to add padding to the list in the case that parts
                         * finish out of order. */
                        aws_array_list_set_at(&auto_ranged_put->synced_data.etag_list, &etag, part_index);
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
        auto_ranged_put->synced_data.total_num_parts);

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
        (*out_resume_token)->total_num_parts = auto_ranged_put->synced_data.total_num_parts;
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
