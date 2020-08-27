/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_client_impl.h"
#include "aws/s3/private/s3_meta_request_impl.h"
#include "aws/s3/private/s3_request.h"
#include "aws/s3/private/s3_util.h"

#include <aws/common/byte_buf.h>
#include <aws/common/string.h>
#include <aws/common/xml_parser.h>
#include <aws/http/request_response.h>
#include <aws/io/stream.h>
#include <inttypes.h>

extern struct aws_s3_request_vtable g_aws_s3_complete_multipart_upload_request_vtable;

static const struct aws_byte_cursor s_complete_payload_begin = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL(
    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
    "<CompleteMultipartUpload xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n");

static const struct aws_byte_cursor s_complete_payload_entry =
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("    <Part>\n"
                                          "        <ETag>%s</ETag>\n"
                                          "        <PartNumber>%d</PartNumber>\n"
                                          "    </Part>\n");

static const struct aws_byte_cursor s_complete_payload_end =
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("</CompleteMultipartUpload>");

static int s_s3_complete_multipart_prepare_for_send(struct aws_s3_request *request);

struct aws_s3_request_vtable g_aws_s3_complete_multipart_upload_request_vtable = {
    .request_type_flags = AWS_S3_REQUEST_TYPE_FLAG_MUST_BE_LAST,
    .prepare_for_send = s_s3_complete_multipart_prepare_for_send};

static void s_s3_add_request_to_completion_payload(struct aws_s3_request *request, void *user_data) {
    AWS_PRECONDITION(request);

    if (request->etag == NULL) {
        return;
    }

    struct aws_byte_buf *xml_payload = (struct aws_byte_buf *)user_data;

    char entry_buffer[1024] = ""; /* TODO */
    sprintf(
        entry_buffer,
        (const char *)s_complete_payload_entry.ptr,
        (const char *)request->etag->bytes,
        request->part_number);

    struct aws_byte_cursor entry_cursor = aws_byte_cursor_from_array(entry_buffer, strlen(entry_buffer));

    aws_byte_buf_append_dynamic(xml_payload, &entry_cursor);
}

static int s_s3_complete_multipart_prepare_for_send(struct aws_s3_request *request) {
    AWS_PRECONDITION(request);

    struct aws_s3_meta_request *meta_request = request->meta_request;
    AWS_PRECONDITION(meta_request);

    struct aws_http_message *message =
        aws_s3_request_util_copy_http_message(meta_request->allocator, meta_request->initial_request_message);

    if (message == NULL) {
        return AWS_OP_ERR;
    }

    if (aws_s3_request_util_set_multipart_request_path(meta_request->allocator, meta_request, 0, message)) {
        goto error_clean_up;
    }

    const struct aws_byte_cursor post_method = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("POST");

    aws_http_message_set_request_method(message, post_method);

    struct aws_http_headers *headers = aws_http_message_get_headers(message);

    if (headers == NULL) {
        goto error_clean_up;
    }

    if (aws_http_headers_erase(headers, g_content_length_header_name_name)) {
        goto error_clean_up;
    }

    if (aws_http_headers_erase(headers, g_content_type_header_name)) {
        goto error_clean_up;
    }

    /* Create XML payload with all of the etags of finished parts */
    {
        struct aws_s3_part_buffer *part_buffer = request->part_buffer;
        AWS_PRECONDITION(part_buffer);

        if (aws_byte_buf_append_dynamic(&part_buffer->buffer, &s_complete_payload_begin)) {
            goto error_clean_up;
        }

        aws_s3_meta_request_iterate_finished_requests(
            meta_request, s_s3_add_request_to_completion_payload, &part_buffer->buffer);

        if (aws_byte_buf_append_dynamic(&part_buffer->buffer, &s_complete_payload_end)) {
            goto error_clean_up;
        }

        request->input_stream = aws_s3_request_util_assign_body(meta_request->allocator, &part_buffer->buffer, message);

        if (request->input_stream == NULL) {
            goto error_clean_up;
        }
    }

    request->message = message;

    return AWS_OP_SUCCESS;

error_clean_up:

    if (message != NULL) {
        aws_http_message_release(message);
        message = NULL;
    }

    return AWS_OP_ERR;
}
