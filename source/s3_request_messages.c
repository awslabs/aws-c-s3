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
#include <aws/http/request_response.h>
#include <aws/io/stream.h>
#include <aws/s3/s3.h>
#include <inttypes.h>

static struct aws_input_stream *s_s3_message_util_assign_body(
    struct aws_allocator *allocator,
    struct aws_byte_buf *byte_buf,
    struct aws_http_message *out_message);

static int s_s3_message_util_set_multipart_request_path(
    struct aws_allocator *allocator,
    const struct aws_string *upload_id,
    uint32_t part_number,
    struct aws_http_message *message);

static int s_s3_message_util_add_content_range_header(
    uint64_t part_index,
    uint64_t part_size,
    struct aws_http_message *out_message);

static int s_s3_create_multipart_set_up_request_path(struct aws_allocator *allocator, struct aws_http_message *message);

/* Create a new get object request from an existing get object request. Currently just adds an optional ranged header.
 */
struct aws_http_message *aws_s3_get_object_message_new(
    struct aws_allocator *allocator,
    struct aws_http_message *base_message,
    uint32_t part_number,
    uint64_t part_size) {
    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(base_message);

    struct aws_http_message *message = aws_s3_message_util_copy_http_message(allocator, base_message);

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

/* Create a new put object request from an existing put object request.  Currently just optionall adds part information
 * for a multipart upload. */
struct aws_http_message *aws_s3_put_object_message_new(
    struct aws_allocator *allocator,
    struct aws_http_message *base_message,
    struct aws_byte_buf *buffer,
    uint32_t part_number,
    const struct aws_string *upload_id) {
    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(base_message);

    struct aws_http_message *message = aws_s3_message_util_copy_http_message(allocator, base_message);

    if (message == NULL) {
        goto error_clean_up;
    }

    if (part_number > 0) {

        if (s_s3_message_util_set_multipart_request_path(allocator, upload_id, part_number, message)) {
            goto error_clean_up;
        }

        if (buffer != NULL) {
            if (s_s3_message_util_assign_body(allocator, buffer, message) == NULL) {
                goto error_clean_up;
            }
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

/* Creates a create-multipart-upload request from a given put objet request. */
struct aws_http_message *aws_s3_create_multipart_upload_message_new(
    struct aws_allocator *allocator,
    struct aws_http_message *base_message) {
    AWS_PRECONDITION(allocator);

    struct aws_http_message *message = aws_s3_message_util_copy_http_message(allocator, base_message);
    struct aws_http_headers *headers = NULL;

    if (message == NULL) {
        goto error_clean_up;
    }

    if (s_s3_create_multipart_set_up_request_path(allocator, message)) {
        goto error_clean_up;
    }

    headers = aws_http_message_get_headers(message);

    if (headers == NULL) {
        goto error_clean_up;
    }

    if (aws_http_headers_erase(headers, g_content_length_header_name)) {
        goto error_clean_up;
    }

    aws_http_message_set_request_method(message, g_post_method);

    aws_http_message_set_body_stream(message, NULL);

    return message;

error_clean_up:

    if (message != NULL) {
        aws_http_message_release(message);
        message = NULL;
    }

    return NULL;
}

static const struct aws_byte_cursor s_complete_payload_begin = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL(
    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
    "<CompleteMultipartUpload xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n");

static const struct aws_byte_cursor s_complete_payload_end =
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("</CompleteMultipartUpload>");

static const struct aws_byte_cursor s_part_section_string_0 = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("    <Part>\n"
                                                                                                    "        <ETag>");

static const struct aws_byte_cursor s_part_section_string_1 =
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("</ETag>\n"
                                          "         <PartNumber>");

static const struct aws_byte_cursor s_part_section_string_2 = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("</PartNumber>\n"
                                                                                                    "    </Part>\n");

/* Create a complete-multipart message, which includes an XML payload of all completed parts. */
struct aws_http_message *aws_s3_complete_multipart_message_new(
    struct aws_allocator *allocator,
    struct aws_http_message *base_message,
    struct aws_byte_buf *body_buffer,
    const struct aws_string *upload_id,
    const struct aws_array_list *etags) {
    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(base_message);
    AWS_PRECONDITION(body_buffer);
    AWS_PRECONDITION(upload_id);
    AWS_PRECONDITION(etags);

    struct aws_http_message *message = aws_s3_message_util_copy_http_message(allocator, base_message);
    struct aws_http_headers *headers = NULL;

    if (message == NULL) {
        goto error_clean_up;
    }

    if (s_s3_message_util_set_multipart_request_path(allocator, upload_id, 0, message)) {
        goto error_clean_up;
    }

    aws_http_message_set_request_method(message, g_post_method);

    headers = aws_http_message_get_headers(message);

    if (headers == NULL) {
        goto error_clean_up;
    }

    if (aws_http_headers_erase(headers, g_content_length_header_name)) {
        goto error_clean_up;
    }

    if (aws_http_headers_erase(headers, g_content_type_header_name)) {
        goto error_clean_up;
    }

    /* Create XML payload with all of the etags of finished parts */
    {
        aws_byte_buf_reset(body_buffer, false);

        if (aws_byte_buf_append(body_buffer, &s_complete_payload_begin)) {
            goto error_clean_up;
        }

        for (size_t etag_index = 0; etag_index < aws_array_list_length(etags); ++etag_index) {
            struct aws_string *etag = NULL;

            aws_array_list_get_at(etags, &etag, etag_index);

            AWS_FATAL_ASSERT(etag != NULL);

            if (aws_byte_buf_append(body_buffer, &s_part_section_string_0)) {
                goto error_clean_up;
            }

            struct aws_byte_cursor etag_byte_cursor = aws_byte_cursor_from_string(etag);

            if (aws_byte_buf_append(body_buffer, &etag_byte_cursor)) {
                goto error_clean_up;
            }

            if (aws_byte_buf_append(body_buffer, &s_part_section_string_1)) {
                goto error_clean_up;
            }

            char part_number_buffer[32] = "";
            int part_number = (int)(etag_index + 1);
            int part_number_num_char = snprintf(part_number_buffer, sizeof(part_number_buffer), "%d", part_number);
            struct aws_byte_cursor part_number_byte_cursor =
                aws_byte_cursor_from_array(part_number_buffer, part_number_num_char);

            if (aws_byte_buf_append(body_buffer, &part_number_byte_cursor)) {
                goto error_clean_up;
            }

            if (aws_byte_buf_append(body_buffer, &s_part_section_string_2)) {
                goto error_clean_up;
            }
        }

        if (aws_byte_buf_append(body_buffer, &s_complete_payload_end)) {
            goto error_clean_up;
        }

        s_s3_message_util_assign_body(allocator, body_buffer, message);
    }

    return message;

error_clean_up:

    AWS_LOGF_ERROR(AWS_LS_S3_GENERAL, "Could not create complete multipart message");

    if (message != NULL) {
        aws_http_message_release(message);
        message = NULL;
    }

    return NULL;
}

/* Sets up the request path for a create-multipart upload request. */
static int s_s3_create_multipart_set_up_request_path(
    struct aws_allocator *allocator,
    struct aws_http_message *message) {
    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(message);

    const struct aws_byte_cursor request_path_suffix = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("?uploads");

    struct aws_byte_cursor request_path;

    if (aws_http_message_get_request_path(message, &request_path)) {
        return AWS_OP_ERR;
    }

    struct aws_byte_buf request_path_buf;

    if (aws_byte_buf_init(&request_path_buf, allocator, request_path.len + request_path_suffix.len)) {
        return AWS_OP_ERR;
    }

    if (aws_byte_buf_append(&request_path_buf, &request_path)) {
        goto error_clean_request_path_buf;
    }

    if (aws_byte_buf_append(&request_path_buf, &request_path_suffix)) {
        goto error_clean_request_path_buf;
    }

    struct aws_byte_cursor new_request_path = aws_byte_cursor_from_buf(&request_path_buf);

    if (aws_http_message_set_request_path(message, new_request_path)) {
        goto error_clean_request_path_buf;
    }

    aws_byte_buf_clean_up(&request_path_buf);
    return AWS_OP_SUCCESS;

error_clean_request_path_buf:

    aws_byte_buf_clean_up(&request_path_buf);
    return AWS_OP_ERR;
}

/* Assign a buffer to an HTTP message, creating a stream and setting the content-length header */
static struct aws_input_stream *s_s3_message_util_assign_body(
    struct aws_allocator *allocator,
    struct aws_byte_buf *byte_buf,
    struct aws_http_message *out_message) {
    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(out_message);
    AWS_PRECONDITION(byte_buf);

    struct aws_byte_cursor part_buffer_byte_cursor = aws_byte_cursor_from_buf(byte_buf);
    struct aws_http_headers *headers = aws_http_message_get_headers(out_message);

    if (headers == NULL) {
        return NULL;
    }

    struct aws_input_stream *input_stream = aws_input_stream_new_from_cursor(allocator, &part_buffer_byte_cursor);

    if (input_stream == NULL) {
        goto error_clean_up;
    }

    char content_length_buffer[64] = "";
    snprintf(content_length_buffer, sizeof(content_length_buffer), "%" PRIu64, (uint64_t)part_buffer_byte_cursor.len);
    struct aws_byte_cursor content_length_cursor =
        aws_byte_cursor_from_array(content_length_buffer, strlen(content_length_buffer));

    if (aws_http_headers_set(headers, g_content_length_header_name, content_length_cursor)) {
        aws_input_stream_destroy(input_stream);
        goto error_clean_up;
    }

    aws_http_message_set_body_stream(out_message, input_stream);

    return input_stream;

error_clean_up:

    if (input_stream != NULL) {
        aws_input_stream_destroy(input_stream);
        input_stream = NULL;
    }

    return NULL;
}

/* Copy an existing HTTP message's headers and body. */
struct aws_http_message *aws_s3_message_util_copy_http_message(
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

/* Handle setting up the multipart request path for a message. */
/* TODO Should be a more compact way of writing this. */
static int s_s3_message_util_set_multipart_request_path(
    struct aws_allocator *allocator,
    const struct aws_string *upload_id,
    uint32_t part_number,
    struct aws_http_message *message) {

    const struct aws_byte_cursor question_mark = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("?");
    const struct aws_byte_cursor ampersand = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("&");
    const struct aws_byte_cursor part_number_arg = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("partNumber=");
    const struct aws_byte_cursor upload_id_arg = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("uploadId=");

    struct aws_byte_buf request_path_buf;
    struct aws_byte_cursor request_path;

    if (aws_http_message_get_request_path(message, &request_path)) {
        return AWS_OP_ERR;
    }

    if (aws_byte_buf_init(&request_path_buf, allocator, request_path.len)) {
        return AWS_OP_ERR;
    }

    if (aws_byte_buf_append_dynamic(&request_path_buf, &request_path)) {
        goto error_clean_up;
    }

    if (part_number > 0) {
        if (aws_byte_buf_append_dynamic(&request_path_buf, &question_mark)) {
            goto error_clean_up;
        }

        if (aws_byte_buf_append_dynamic(&request_path_buf, &part_number_arg)) {
            goto error_clean_up;
        }

        char part_number_buffer[32] = "";
        snprintf(part_number_buffer, sizeof(part_number_buffer), "%d", part_number);
        struct aws_byte_cursor part_number_cursor =
            aws_byte_cursor_from_array(part_number_buffer, strlen(part_number_buffer));

        if (aws_byte_buf_append_dynamic(&request_path_buf, &part_number_cursor)) {
            goto error_clean_up;
        }
    }

    if (upload_id != NULL) {

        struct aws_byte_cursor upload_id_cursor = aws_byte_cursor_from_string(upload_id);

        if (part_number > 0) {
            if (aws_byte_buf_append_dynamic(&request_path_buf, &ampersand)) {
                goto error_clean_up;
            }
        } else if (aws_byte_buf_append_dynamic(&request_path_buf, &question_mark)) {
            goto error_clean_up;
        }

        if (aws_byte_buf_append_dynamic(&request_path_buf, &upload_id_arg)) {
            goto error_clean_up;
        }

        if (aws_byte_buf_append_dynamic(&request_path_buf, &upload_id_cursor)) {
            goto error_clean_up;
        }
    }

    struct aws_byte_cursor new_request_path = aws_byte_cursor_from_buf(&request_path_buf);

    if (aws_http_message_set_request_path(message, new_request_path)) {
        goto error_clean_up;
    }

    aws_byte_buf_clean_up(&request_path_buf);
    return AWS_OP_SUCCESS;

error_clean_up:

    aws_byte_buf_clean_up(&request_path_buf);

    return AWS_OP_ERR;
}
