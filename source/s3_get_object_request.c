/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_get_object_request.h"
#include "aws/s3/private/s3_request.h"
#include "aws/s3/private/s3_util.h"
#include "aws/s3/s3_client.h"

#include <aws/common/byte_buf.h>
#include <aws/common/string.h>
#include <aws/http/request_response.h>
#include <aws/io/stream.h>
#include <inttypes.h>

static int s_s3_get_object_request_incoming_headers(
    struct aws_s3_request *request,
    struct aws_http_stream *stream,
    enum aws_http_header_block header_block,
    const struct aws_http_header *headers,
    size_t headers_count);

static int s_s3_get_object_request_incoming_headers_block_done(
    struct aws_s3_request *request,
    struct aws_http_stream *stream,
    enum aws_http_header_block header_block);

static int s_s3_get_object_incoming_body(
    struct aws_s3_request *request,
    struct aws_http_stream *stream,
    const struct aws_byte_cursor *data,
    struct aws_s3_body_meta_data *out_meta_data);

static void s_s3_get_object_request_destroy(struct aws_s3_request *request);

struct aws_s3_request_vtable g_aws_s3_request_get_object_vtable = {
    .destroy = s_s3_get_object_request_destroy,
    .incoming_headers = s_s3_get_object_request_incoming_headers,
    .incoming_header_block_done = s_s3_get_object_request_incoming_headers_block_done,
    .incoming_body = s_s3_get_object_incoming_body,
    .stream_complete = NULL,
    .request_finish = NULL};

struct aws_s3_request *aws_s3_get_object_request_new(
    struct aws_allocator *allocator,
    const struct aws_s3_request_options *options,
    const struct aws_s3_get_object_request_options *get_object_request_options) {

    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(options);
    AWS_PRECONDITION(get_object_request_options);

    if (get_object_request_options->range_end < get_object_request_options->range_start) {
        AWS_LOGF_ERROR(AWS_LS_S3_REQUEST, "Could not create aws_s3_get_object; specified range is invalid.");
        return NULL;
    }

    struct aws_s3_get_object_request *get_object =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_get_object_request));

    if (get_object == NULL) {
        AWS_LOGF_ERROR(AWS_LS_S3_REQUEST, "Could not allocate aws_s3_get_object_request");
        return NULL;
    }

    struct aws_s3_request *s3_request = &get_object->s3_request;

    /* Initialize the base type. */
    if (aws_s3_request_init(s3_request, allocator, AWS_S3_REQUEST_TYPE_GET_OBJECT, options)) {
        AWS_LOGF_ERROR(AWS_LS_S3_REQUEST, "id=%p Could not initialize base aws_s3_request type", (void *)s3_request);
        goto error_clean_up_request;
    }

    get_object->range_start = get_object_request_options->range_start;
    get_object->range_end = get_object_request_options->range_end;
    get_object->headers_finished_callback = get_object_request_options->headers_finished_callback;
    get_object->user_data = get_object_request_options->user_data;

    /* If we specified a range, add the appropriate header to the http request. */
    if (get_object_request_options->range_end > 0) {
        uint64_t range_start = get_object_request_options->range_start;
        uint64_t range_end = get_object_request_options->range_end;

        char range_value_buffer[128] = "";
        snprintf(range_value_buffer, sizeof(range_value_buffer), "bytes=%" PRIu64 "-%" PRIu64, range_start, range_end);

        struct aws_http_header range_header;
        AWS_ZERO_STRUCT(range_header);
        range_header.name = g_range_header_name;
        range_header.value = aws_byte_cursor_from_c_str(range_value_buffer);

        if (aws_http_message_add_header(s3_request->message, range_header)) {
            AWS_LOGF_ERROR(
                AWS_LS_S3_REQUEST,
                "id=%p Could not add Range header to get-object-request HTTP message.",
                (void *)s3_request);
            goto error_clean_up_request;
        }
    }

    return s3_request;

error_clean_up_request:

    if (s3_request != NULL) {
        aws_s3_request_destroy(s3_request);
        s3_request = NULL;
    }

    return NULL;
}

static int s_s3_get_object_request_incoming_headers(
    struct aws_s3_request *request,
    struct aws_http_stream *stream,
    enum aws_http_header_block header_block,
    const struct aws_http_header *headers,
    size_t headers_count) {

    AWS_PRECONDITION(request);
    AWS_PRECONDITION(stream);
    (void)stream;
    (void)header_block;

    struct aws_s3_get_object_request *get_object_request = (struct aws_s3_get_object_request *)request;
    struct aws_s3_get_object_result *get_object_result = &get_object_request->result;
    struct aws_s3_get_object_content_result_range *content_range = &get_object_result->content_range;

    /* Find the Content-Range header and extract the object size. */
    for (size_t i = 0; i < headers_count; ++i) {
        const struct aws_byte_cursor *name = &headers[i].name;
        const struct aws_byte_cursor *value = &headers[i].value;

        if (aws_http_header_name_eq(*name, g_content_length_header_name_name)) {
            sscanf((const char *)value->ptr, "%" PRIu64, &get_object_result->content_length);
        } else if (aws_http_header_name_eq(*name, g_content_range_header_name)) {
            /* Format of header is: "bytes StartByte-EndByte/TotalObjectSize" */
            sscanf(
                (const char *)value->ptr,
                "bytes %" PRIu64 "-%" PRIu64 "/%" PRIu64,
                &content_range->range_start,
                &content_range->range_end,
                &content_range->total_object_size);
        }
    }

    return AWS_OP_SUCCESS;
}

static int s_s3_get_object_request_incoming_headers_block_done(
    struct aws_s3_request *request,
    struct aws_http_stream *stream,
    enum aws_http_header_block header_block) {
    AWS_PRECONDITION(request);
    AWS_PRECONDITION(stream);

    (void)header_block;

    struct aws_s3_get_object_request *get_object_request = (struct aws_s3_get_object_request *)request;

    if (get_object_request->headers_finished_callback) {
        get_object_request->headers_finished_callback(get_object_request, stream, get_object_request->user_data);
    }

    return AWS_OP_SUCCESS;
}

static int s_s3_get_object_incoming_body(
    struct aws_s3_request *request,
    struct aws_http_stream *stream,
    const struct aws_byte_cursor *data,
    struct aws_s3_body_meta_data *out_meta_data) {
    AWS_PRECONDITION(request);
    AWS_PRECONDITION(stream);
    AWS_PRECONDITION(data);
    AWS_PRECONDITION(out_meta_data);

    (void)data;

    bool is_success = false;

    if (aws_s3_is_stream_response_status_success(stream, &is_success)) {
        return AWS_OP_ERR;
    }

    if (is_success) {
        struct aws_s3_get_object_request *get_object = (struct aws_s3_get_object_request *)request;
        out_meta_data->range_start = get_object->range_start;
        out_meta_data->range_end = get_object->range_end;
        out_meta_data->data_flags = AWS_S3_INCOMING_BODY_FLAG_OBJECT_DATA;
    }

    return AWS_OP_SUCCESS;
}

static void s_s3_get_object_request_destroy(struct aws_s3_request *request) {
    AWS_PRECONDITION(request);

    struct aws_s3_get_object_request *get_object = (struct aws_s3_get_object_request *)request;
    aws_mem_release(request->allocator, get_object);
}
