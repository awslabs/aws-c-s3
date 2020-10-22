/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_request_messages.h"
#include "aws/s3/private/s3_client_impl.h"
#include "aws/s3/private/s3_meta_request_impl.h"
#include "aws/s3/private/s3_util.h"
#include <aws/common/byte_buf.h>
#include <aws/common/string.h>
#include <aws/common/xml_parser.h>
#include <aws/http/request_response.h>
#include <aws/io/stream.h>
#include <aws/s3/s3.h>
#include <inttypes.h>

static struct aws_http_message *s_s3_message_util_copy_http_message(
    struct aws_allocator *allocator,
    struct aws_http_message *message);

static int s_s3_message_util_add_content_range_header(
    uint64_t part_index,
    uint64_t part_size,
    struct aws_http_message *out_message);

/* Create a new get object request from an existing get object request. Currently just adds an optional ranged header.
 */
struct aws_http_message *aws_s3_get_object_message_new(
    struct aws_allocator *allocator,
    struct aws_http_message *base_message,
    uint32_t part_number,
    uint64_t part_size) {
    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(base_message);

    struct aws_http_message *message = s_s3_message_util_copy_http_message(allocator, base_message);

    if (message == NULL) {
        return NULL;
    }

    if (part_number > 0) {
        if (s_s3_message_util_add_content_range_header(part_number - 1, part_size, message)) {
            goto error_clean_up;
        }
    }

    return message;

error_clean_up:

    if (message != NULL) {
        aws_http_message_release(message);
        message = NULL;
    }

    return NULL;
}

/* Copy an existing HTTP message's headers and body. */
static struct aws_http_message *s_s3_message_util_copy_http_message(
    struct aws_allocator *allocator,
    struct aws_http_message *base_message) {
    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(base_message);

    struct aws_http_message *message = aws_http_message_new_request(allocator);

    if (message == NULL) {
        return NULL;
    }

    struct aws_byte_cursor request_method;
    if (aws_http_message_get_request_method(base_message, &request_method)) {
        goto error_clean_up;
    }

    if (aws_http_message_set_request_method(message, request_method)) {
        goto error_clean_up;
    }

    struct aws_byte_cursor request_path;
    if (aws_http_message_get_request_path(base_message, &request_path)) {
        goto error_clean_up;
    }

    if (aws_http_message_set_request_path(message, request_path)) {
        goto error_clean_up;
    }

    size_t num_headers = aws_http_message_get_header_count(base_message);

    for (size_t header_index = 0; header_index < num_headers; ++header_index) {
        struct aws_http_header header;

        if (aws_http_message_get_header(base_message, &header, header_index)) {
            goto error_clean_up;
        }

        if (aws_http_message_add_header(message, header)) {
            goto error_clean_up;
        }
    }

    return message;

error_clean_up:

    if (message != NULL) {
        aws_http_message_release(message);
        message = NULL;
    }

    return NULL;
}

/* Add a content-range header.*/
static int s_s3_message_util_add_content_range_header(
    uint64_t part_index,
    uint64_t part_size,
    struct aws_http_message *out_message) {
    AWS_PRECONDITION(out_message);

    uint64_t range_start = part_index * part_size;
    uint64_t range_end = range_start + part_size - 1;

    /* TODO this is more than enough space, but maybe there's a better way to do this?
     * ((2^64)-1 = 20 characters;  2*20 + length-of("bytes=-") < 128) */
    char range_value_buffer[128] = "";
    snprintf(range_value_buffer, sizeof(range_value_buffer), "bytes=%" PRIu64 "-%" PRIu64, range_start, range_end);

    struct aws_http_header range_header;
    AWS_ZERO_STRUCT(range_header);
    range_header.name = g_range_header_name;
    range_header.value = aws_byte_cursor_from_c_str(range_value_buffer);

    if (aws_http_message_add_header(out_message, range_header)) {
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}
