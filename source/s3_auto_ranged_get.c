/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_auto_ranged_get.h"
#include "aws/s3/private/s3_client_impl.h"
#include "aws/s3/private/s3_meta_request_impl.h"
#include "aws/s3/private/s3_request_messages.h"
#include "aws/s3/private/s3_util.h"
#include <aws/common/string.h>
#include <inttypes.h>

#ifdef _MSC_VER
/* sscanf warning (not currently scanning for strings) */
#    pragma warning(disable : 4996)
#endif

const uint32_t s_conservative_max_requests_in_flight = 8;
const struct aws_byte_cursor g_application_xml_value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("application/xml");
const struct aws_byte_cursor g_object_size_value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("ActualObjectSize");

static void s_s3_meta_request_auto_ranged_get_destroy(struct aws_s3_meta_request *meta_request);

static bool s_s3_auto_ranged_get_update(
    struct aws_s3_meta_request *meta_request,
    uint32_t flags,
    struct aws_s3_request **out_request);

static int s_s3_auto_ranged_get_prepare_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request);

static void s_s3_auto_ranged_get_request_finished(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    int error_code);

static struct aws_s3_meta_request_vtable s_s3_auto_ranged_get_vtable = {
    .update = s_s3_auto_ranged_get_update,
    .send_request_finish = aws_s3_meta_request_send_request_finish_default,
    .prepare_request = s_s3_auto_ranged_get_prepare_request,
    .init_signing_date_time = aws_s3_meta_request_init_signing_date_time_default,
    .sign_request = aws_s3_meta_request_sign_request_default,
    .finished_request = s_s3_auto_ranged_get_request_finished,
    .destroy = s_s3_meta_request_auto_ranged_get_destroy,
    .finish = aws_s3_meta_request_finish_default,
};

/* Allocate a new auto-ranged-get meta request. */
struct aws_s3_meta_request *aws_s3_meta_request_auto_ranged_get_new(
    struct aws_allocator *allocator,
    struct aws_s3_client *client,
    size_t part_size,
    const struct aws_s3_meta_request_options *options) {
    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(options);
    AWS_PRECONDITION(options->message);

    struct aws_s3_auto_ranged_get *auto_ranged_get =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_auto_ranged_get));

    /* Try to initialize the base type. */
    if (aws_s3_meta_request_init_base(
            allocator,
            client,
            part_size,
            options,
            auto_ranged_get,
            &s_s3_auto_ranged_get_vtable,
            &auto_ranged_get->base)) {

        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST,
            "id=%p Could not initialize base type for Auto-Ranged-Get Meta Request.",
            (void *)auto_ranged_get);
        goto error_clean_up;
    }

    AWS_LOGF_DEBUG(
        AWS_LS_S3_META_REQUEST, "id=%p Created new Auto-Ranged Get Meta Request.", (void *)&auto_ranged_get->base);

    return &auto_ranged_get->base;

error_clean_up:

    aws_s3_meta_request_release(&auto_ranged_get->base);
    auto_ranged_get = NULL;

    return NULL;
}

static void s_s3_meta_request_auto_ranged_get_destroy(struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(meta_request->impl);

    struct aws_s3_auto_ranged_get *auto_ranged_get = meta_request->impl;
    aws_mem_release(meta_request->allocator, auto_ranged_get);
}

/* Check the finish result of meta request, in case of the request failed because of downloading an empty file */
static bool s_check_empty_file_download_error(struct aws_s3_request *failed_request) {
    struct aws_http_headers *failed_headers = failed_request->send_data.response_headers;
    struct aws_byte_buf failed_body = failed_request->send_data.response_body;
    if (failed_headers && failed_body.capacity > 0) {
        struct aws_byte_cursor content_type;
        AWS_ZERO_STRUCT(content_type);
        if (!aws_http_headers_get(failed_headers, g_content_type_header_name, &content_type)) {
            /* Content type found */
            if (aws_byte_cursor_eq_ignore_case(&content_type, &g_application_xml_value)) {
                /* XML response */
                struct aws_byte_cursor body_cursor = aws_byte_cursor_from_buf(&failed_body);
                struct aws_string *size =
                    get_top_level_xml_tag_value(failed_request->allocator, &g_object_size_value, &body_cursor);
                bool check_size = aws_string_eq_c_str(size, "0");
                aws_string_destroy(size);
                if (check_size) {
                    return true;
                }
            }
        }
    }
    return false;
}

static bool s_s3_auto_ranged_get_update(
    struct aws_s3_meta_request *meta_request,
    uint32_t flags,
    struct aws_s3_request **out_request) {
    AWS_PRECONDITION(meta_request);

    struct aws_s3_auto_ranged_get *auto_ranged_get = meta_request->impl;
    struct aws_s3_request *request = NULL;
    bool work_remaining = false;

    aws_s3_meta_request_lock_synced_data(meta_request);

    if ((flags & AWS_S3_META_REQUEST_UPDATE_FLAG_NO_ENDPOINT_CONNECTIONS) > 0) {
        /* If we have haven't already requested all of the parts, then we need to fail now. Note: total_num_parts is
         * not populated until after we get the respones from the first request, so the initial num_parts_requested == 0
         * check is necessary. */
        if (auto_ranged_get->synced_data.num_parts_requested == 0 ||
            (auto_ranged_get->synced_data.total_num_parts > 0 &&
             auto_ranged_get->synced_data.num_parts_requested < auto_ranged_get->synced_data.total_num_parts)) {
            aws_s3_meta_request_set_fail_synced(meta_request, NULL, AWS_ERROR_S3_NO_ENDPOINT_CONNECTIONS);
        }
    }

    if (!aws_s3_meta_request_has_finish_result_synced(meta_request)) {

        if ((flags & AWS_S3_META_REQUEST_UPDATE_FLAG_CONSERVATIVE) != 0) {
            uint32_t num_requests_in_flight =
                (auto_ranged_get->synced_data.num_parts_requested - auto_ranged_get->synced_data.num_parts_completed) +
                (uint32_t)aws_priority_queue_size(&meta_request->synced_data.pending_body_streaming_requests);

            /* auto-ranged-gets make use of body streaming, which will hold onto response bodies if parts earlier in the
             * file haven't arrived yet. This can potentially create a lot of backed up requests, causing us to hit our
             * global request limit. To help mitigate this, when the "conservative" flag is passed in, we only allow
             * the total amount of requests being sent/streamed to be inside of a set limit.  */
            if (num_requests_in_flight > s_conservative_max_requests_in_flight) {
                goto has_work_remaining;
            }
        }

        /* If no parts have been requested, then we need to send an initial part request. */
        if (auto_ranged_get->synced_data.num_parts_requested == 0) {

            if (out_request == NULL) {
                goto has_work_remaining;
            }

            request = aws_s3_request_new(
                meta_request,
                AWS_S3_AUTO_RANGE_GET_REQUEST_TYPE_PART,
                1,
                AWS_S3_REQUEST_FLAG_RECORD_RESPONSE_HEADERS | AWS_S3_REQUEST_FLAG_PART_SIZE_RESPONSE_BODY);

            ++auto_ranged_get->synced_data.num_parts_requested;
            goto has_work_remaining;
        }

        /* Wait for the response of the first request. */
        if (auto_ranged_get->synced_data.num_parts_completed < 1) {
            goto has_work_remaining;
        }

        if (auto_ranged_get->synced_data.get_without_range) {
            if (auto_ranged_get->synced_data.get_without_range_sent) {
                if (auto_ranged_get->synced_data.get_without_range_completed) {
                    goto no_work_remaining;
                } else {
                    goto has_work_remaining;
                }
            }
            if (out_request == NULL) {
                goto has_work_remaining;
            }

            request = aws_s3_request_new(
                meta_request,
                AWS_S3_AUTO_RANGE_GET_REQUEST_TYPE_PART_WITHOUT_RANGE,
                0,
                AWS_S3_REQUEST_FLAG_RECORD_RESPONSE_HEADERS);

            auto_ranged_get->synced_data.get_without_range_sent = true;
            goto has_work_remaining;
        }

        /* If we have gotten a response for the first request, then the total number of parts for the object is now
         * known. Continue sending parts until the total number of parts is reached.*/
        if (auto_ranged_get->synced_data.num_parts_requested < auto_ranged_get->synced_data.total_num_parts) {
            if (out_request == NULL) {
                goto has_work_remaining;
            }

            request = aws_s3_request_new(
                meta_request,
                AWS_S3_AUTO_RANGE_GET_REQUEST_TYPE_PART,
                auto_ranged_get->synced_data.num_parts_requested + 1,
                AWS_S3_REQUEST_FLAG_PART_SIZE_RESPONSE_BODY);

            ++auto_ranged_get->synced_data.num_parts_requested;
            goto has_work_remaining;
        }

        /* If not all parts have attempted delivery to the caller, then there is still work being done. */
        if (meta_request->synced_data.num_parts_delivery_completed < auto_ranged_get->synced_data.total_num_parts) {
            goto has_work_remaining;
        }

    } else {

        /* Wait for all requests to complete (successfully or unsuccessfully) before finishing.*/
        if (auto_ranged_get->synced_data.num_parts_completed < auto_ranged_get->synced_data.num_parts_requested) {
            goto has_work_remaining;
        }

        if (auto_ranged_get->synced_data.get_without_range) {
            if (auto_ranged_get->synced_data.get_without_range_sent &&
                !auto_ranged_get->synced_data.get_without_range_completed) {
                goto has_work_remaining;
            } else {
                goto no_work_remaining;
            }
        }

        /* If some parts are still being delivered to the caller, then wait for those to finish. */
        if (meta_request->synced_data.num_parts_delivery_completed <
            meta_request->synced_data.num_parts_delivery_sent) {
            goto has_work_remaining;
        }
    }

    goto no_work_remaining;

has_work_remaining:
    work_remaining = true;

    if (request != NULL) {
        AWS_LOGF_DEBUG(
            AWS_LS_S3_META_REQUEST,
            "id=%p: Returning request %p for part %d of %d",
            (void *)meta_request,
            (void *)request,
            request->part_number,
            auto_ranged_get->synced_data.total_num_parts);
    }

no_work_remaining:

    if (!work_remaining) {
        aws_s3_meta_request_set_success_synced(meta_request, AWS_S3_RESPONSE_STATUS_SUCCESS);
    }

    aws_s3_meta_request_unlock_synced_data(meta_request);

    if (work_remaining) {
        if (request != NULL) {
            AWS_ASSERT(out_request != NULL);
            *out_request = request;
        }
    } else {
        AWS_ASSERT(request == NULL);
        aws_s3_meta_request_finish(meta_request);
    }

    return work_remaining;
}

/* Given a request, prepare it for sending based on its description. */
static int s_s3_auto_ranged_get_prepare_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(request);

    /* Generate a new ranged get request based on the original message. */
    struct aws_http_message *message = aws_s3_get_object_message_new(
        meta_request->allocator, meta_request->initial_request_message, request->part_number, meta_request->part_size);

    if (message == NULL) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST,
            "id=%p Could not create message for request with tag %d for auto-ranged-get meta request.",
            (void *)meta_request,
            request->request_tag);
        goto message_alloc_failed;
    }

    aws_s3_request_setup_send_data(request, message);
    aws_http_message_release(message);

    AWS_LOGF_DEBUG(
        AWS_LS_S3_META_REQUEST,
        "id=%p: Created request %p for part %d",
        (void *)meta_request,
        (void *)request,
        request->part_number);

    return AWS_OP_SUCCESS;

message_alloc_failed:

    return AWS_OP_ERR;
}

static void s_s3_auto_ranged_get_request_finished(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    int error_code) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(meta_request->impl);
    AWS_PRECONDITION(request);

    (void)request;
    (void)error_code;

    struct aws_s3_auto_ranged_get *auto_ranged_get = meta_request->impl;

    uint32_t num_parts = 0;

    /* Check if this was the first part and if it was successful. For a ranged-get request, the first part number will
     * be 1. For an empty file request, the part number will be 0.*/
    if (error_code == AWS_ERROR_SUCCESS && request->part_number <= 1) {
        uint64_t total_object_size = 0;
        if (request->request_tag == AWS_S3_AUTO_RANGE_GET_REQUEST_TYPE_PART) {
            struct aws_byte_cursor content_range_header_value;

            if (aws_http_headers_get(
                    request->send_data.response_headers, g_content_range_header_name, &content_range_header_value)) {
                AWS_LOGF_ERROR(
                    AWS_LS_S3_META_REQUEST,
                    "id=%p Could not find content range header for request %p",
                    (void *)meta_request,
                    (void *)request);

                error_code = AWS_ERROR_S3_MISSING_CONTENT_RANGE_HEADER;
                goto error_encountered;
            }

            uint64_t range_start = 0;
            uint64_t range_end = 0;

            /* The memory the byte cursor refers to should be valid, but if it's referring to a buffer that was
             * previously used, the null terminating character may not be where we expect. We copy to a string to
             * ensure that our null terminating character placement corresponds with the length. */
            struct aws_string *content_range_header_value_str =
                aws_string_new_from_cursor(meta_request->allocator, &content_range_header_value);

            /* Format of header is: "bytes StartByte-EndByte/TotalObjectSize" */
            sscanf(
                (const char *)content_range_header_value_str->bytes,
                "bytes %" PRIu64 "-%" PRIu64 "/%" PRIu64,
                &range_start,
                &range_end,
                &total_object_size);

            aws_string_destroy(content_range_header_value_str);
            content_range_header_value_str = NULL;

            if (total_object_size == 0) {
                AWS_LOGF_ERROR(
                    AWS_LS_S3_META_REQUEST, "id=%p Get Object has invalid content range.", (void *)meta_request);
                error_code = AWS_ERROR_S3_MISSING_CONTENT_RANGE_HEADER;
                goto error_encountered;
            }

            num_parts = (uint32_t)(total_object_size / meta_request->part_size);

            if (total_object_size % meta_request->part_size) {
                ++num_parts;
            }

            AWS_LOGF_DEBUG(
                AWS_LS_S3_META_REQUEST,
                "id=%p Object being requested is %" PRIu64 " bytes which will have %d parts based off of a %" PRIu64
                " part size.",
                (void *)meta_request,
                total_object_size,
                num_parts,
                (uint64_t)meta_request->part_size);
        }

        if (meta_request->headers_callback != NULL) {
            struct aws_http_headers *response_headers = aws_http_headers_new(meta_request->allocator);

            copy_http_headers(request->send_data.response_headers, response_headers);

            aws_http_headers_erase(response_headers, g_content_range_header_name);

            char content_length_buffer[64] = "";
            snprintf(content_length_buffer, sizeof(content_length_buffer), "%" PRIu64, total_object_size);
            aws_http_headers_set(
                response_headers, g_content_length_header_name, aws_byte_cursor_from_c_str(content_length_buffer));

            if (meta_request->headers_callback(
                    meta_request, response_headers, AWS_S3_RESPONSE_STATUS_SUCCESS, meta_request->user_data)) {

                error_code = aws_last_error_or_unknown();
            }

            aws_http_headers_release(response_headers);
        }
    }

error_encountered:

    aws_s3_meta_request_lock_synced_data(meta_request);

    if (request->request_tag == AWS_S3_AUTO_RANGE_GET_REQUEST_TYPE_PART) {
        ++auto_ranged_get->synced_data.num_parts_completed;

        if (error_code == AWS_ERROR_SUCCESS) {
            ++auto_ranged_get->synced_data.num_parts_successful;

            if (request->part_number == 1) {
                AWS_ASSERT(num_parts > 0);
                auto_ranged_get->synced_data.total_num_parts = num_parts;
            }

            aws_s3_meta_request_stream_response_body_synced(meta_request, request);

            AWS_LOGF_DEBUG(
                AWS_LS_S3_META_REQUEST,
                "id=%p: %d out of %d parts have completed.",
                (void *)meta_request,
                (auto_ranged_get->synced_data.num_parts_successful + auto_ranged_get->synced_data.num_parts_failed),
                auto_ranged_get->synced_data.total_num_parts);
        } else {
            ++auto_ranged_get->synced_data.num_parts_failed;
            if (s_check_empty_file_download_error(request)) {
                AWS_LOGF_DEBUG(
                    AWS_LS_S3_META_REQUEST,
                    "id=%p Getting an empty file, create a new request without range header to fetch the empty "
                    "file",
                    (void *)meta_request);
                auto_ranged_get->synced_data.get_without_range = true;
            } else {
                aws_s3_meta_request_set_fail_synced(meta_request, request, error_code);
            }
        }
    } else if (request->request_tag == AWS_S3_AUTO_RANGE_GET_REQUEST_TYPE_PART_WITHOUT_RANGE) {
        AWS_LOGF_DEBUG(AWS_LS_S3_META_REQUEST, "id=%p Get empty file completed", (void *)meta_request);
        auto_ranged_get->synced_data.get_without_range_completed = true;
        if (error_code != AWS_ERROR_SUCCESS) {
            aws_s3_meta_request_set_fail_synced(meta_request, request, error_code);
        }
    }

    aws_s3_meta_request_unlock_synced_data(meta_request);
}
