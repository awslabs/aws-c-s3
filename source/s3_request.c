/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_request.h"
#include "aws/s3/private/s3_client_impl.h"
#include "aws/s3/private/s3_meta_request_impl.h"
#include "aws/s3/private/s3_util.h"

#include <aws/common/assert.h>
#include <aws/common/string.h>
#include <aws/http/request_response.h>
#include <aws/io/stream.h>
#include <inttypes.h>

extern struct aws_s3_request_vtable g_aws_s3_get_object_request_vtable;
extern struct aws_s3_request_vtable g_aws_s3_seed_get_object_request_vtable;
extern struct aws_s3_request_vtable g_aws_s3_put_object_request_vtable;
extern struct aws_s3_request_vtable g_aws_s3_create_multipart_upload_request_vtable;
extern struct aws_s3_request_vtable g_aws_s3_complete_multipart_upload_request_vtable;

static struct aws_s3_request_vtable *s_s3_request_vtables[] = {
    &g_aws_s3_get_object_request_vtable,               /* AWS_S3_REQUEST_TYPE_GET_OBJECT */
    &g_aws_s3_seed_get_object_request_vtable,          /* AWS_S3_REQUEST_TYPE_SEED_GET_OBJECT */
    &g_aws_s3_put_object_request_vtable,               /* AWS_S3_REQUEST_TYPE_PUT_OBJECT */
    &g_aws_s3_create_multipart_upload_request_vtable,  /* AWS_S3_REQUEST_TYPE_CREATE_MULTIPART */
    &g_aws_s3_complete_multipart_upload_request_vtable /* AWS_S3_REQUEST_TYPE_UPLOAD_PART */
};

static int s_s3_request_incoming_body(
    struct aws_http_stream *stream,
    const struct aws_byte_cursor *data,
    void *user_data);

static void s_s3_request_on_stream_complete(struct aws_http_stream *stream, int error_code, void *user_data);

struct aws_s3_request *aws_s3_request_new(
    struct aws_s3_meta_request *meta_request,
    const struct aws_s3_request_options *options) {

    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(meta_request->allocator);
    AWS_PRECONDITION(options);

    struct aws_s3_request *request = aws_mem_calloc(meta_request->allocator, 1, sizeof(struct aws_s3_request));

    if (request == NULL) {
        AWS_LOGF_ERROR(AWS_LS_S3_REQUEST, "Could not allocate memory required for aws_s3_request.");
        return NULL;
    }

    request->request_type = options->request_type;
    request->part_number = options->part_number;

    return request;
}

uint32_t aws_s3_request_get_type_flags(struct aws_s3_request *request) {
    AWS_PRECONDITION(request);

    struct aws_s3_request_vtable *vtable = s_s3_request_vtables[request->request_type];
    AWS_FATAL_ASSERT(vtable != NULL);

    return vtable->request_type_flags;
}

int aws_s3_request_prepare_for_send(struct aws_s3_request *request, struct aws_s3_client *client) {
    AWS_PRECONDITION(request);
    AWS_PRECONDITION(client);

    struct aws_s3_meta_request *meta_request = request->meta_request;
    struct aws_s3_part_buffer *part_buffer = aws_s3_client_get_part_buffer(client);

    if (part_buffer == NULL) {
        AWS_LOGF_TRACE(AWS_LS_S3_REQUEST, "id=%p, Couldn't get part buffer from request.", (void *)request);
        return AWS_OP_ERR;
    }

    if (request->part_number > 0) {
        uint64_t part_index = request->part_number - 1;

        part_buffer->range_start = part_index * meta_request->part_size;
        part_buffer->range_end = part_buffer->range_start + meta_request->part_size - 1;

    } else {
        part_buffer->range_start = 0;
        part_buffer->range_end = 0;
    }

    request->part_buffer = part_buffer;

    struct aws_s3_request_vtable *vtable = s_s3_request_vtables[request->request_type];
    AWS_FATAL_ASSERT(vtable != NULL);

    if (vtable->prepare_for_send) {
        if (vtable->prepare_for_send(request)) {
            return AWS_OP_ERR;
        }
    }

    return AWS_OP_SUCCESS;
}

int aws_s3_request_make_request(
    struct aws_s3_request *request,
    struct aws_http_connection *http_connection,
    aws_s3_request_finished_callback_fn *finished_callback,
    void *user_data) {
    AWS_PRECONDITION(request);
    AWS_PRECONDITION(http_connection);
    AWS_PRECONDITION(finished_callback);
    AWS_PRECONDITION(user_data);

    struct aws_s3_request_vtable *vtable = s_s3_request_vtables[request->request_type];
    AWS_FATAL_ASSERT(vtable != NULL);

    request->finished_callback = finished_callback;
    request->user_data = user_data;

    /* Now that we have a signed request and a connection, go ahead and issue the request. */
    struct aws_http_make_request_options options;
    AWS_ZERO_STRUCT(options);
    options.self_size = sizeof(struct aws_http_make_request_options);
    options.request = request->message;
    options.user_data = request;
    options.on_response_headers = vtable->incoming_headers;
    options.on_response_header_block_done = vtable->incoming_headers_block_done;
    options.on_response_body = s_s3_request_incoming_body;
    options.on_complete = s_s3_request_on_stream_complete;

    struct aws_http_stream *stream = aws_http_connection_make_request(http_connection, &options);

    if (stream == NULL) {
        AWS_LOGF_ERROR(AWS_LS_S3_REQUEST, "id=%p: Could not make HTTP request", (void *)request);
        return AWS_OP_ERR;
    }

    if (aws_http_stream_activate(stream) != AWS_OP_SUCCESS) {
        aws_http_stream_release(stream);
        stream = NULL;

        AWS_LOGF_ERROR(AWS_LS_S3_REQUEST, "id=%p: Could not activate HTTP stream", (void *)request);
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}

static int s_s3_request_incoming_body(
    struct aws_http_stream *stream,
    const struct aws_byte_cursor *data,
    void *user_data) {

    AWS_PRECONDITION(stream);

    struct aws_s3_request *request = user_data;
    AWS_PRECONDITION(request);
    AWS_PRECONDITION(request->part_buffer);

    uint32_t request_type_flags = aws_s3_request_get_type_flags(request);

    if ((request_type_flags & AWS_S3_REQUEST_TYPE_FLAG_WRITE_BODY_TO_PART_BUFFER) != 0) {
        struct aws_s3_part_buffer *part_buffer = request->part_buffer;
        AWS_FATAL_ASSERT(part_buffer);

        if (aws_byte_buf_append(&part_buffer->buffer, data)) {
            return AWS_OP_ERR;
        }
    }

    struct aws_s3_request_vtable *vtable = s_s3_request_vtables[request->request_type];
    AWS_FATAL_ASSERT(vtable != NULL);

    if (vtable->incoming_body) {
        vtable->incoming_body(stream, data, user_data);
    }

    return AWS_OP_SUCCESS;
}

static void s_s3_request_on_stream_complete(struct aws_http_stream *stream, int error_code, void *user_data) {
    AWS_PRECONDITION(stream);
    AWS_PRECONDITION(user_data);

    struct aws_s3_request *request = user_data;

    AWS_PRECONDITION(request);

    // struct aws_input_stream *input_stream = aws_http_message_get_body_stream(request->message); TODO

    struct aws_s3_request_vtable *vtable = s_s3_request_vtables[request->request_type];
    AWS_FATAL_ASSERT(vtable);

    if (vtable->stream_complete) {
        if (vtable->stream_complete(stream, error_code, user_data)) {
            error_code = aws_last_error();
        }
    }

    if (request->part_buffer != NULL) {
        uint32_t request_type_flags = aws_s3_request_get_type_flags(request);

        if ((request_type_flags & AWS_S3_REQUEST_TYPE_FLAG_WRITE_PART_BUFFER_TO_CALLER) != 0) {
            aws_s3_meta_request_write_part_buffer_to_caller(request->meta_request, request->part_buffer);
        } else {
            aws_s3_client_release_part_buffer(request->meta_request->client, request->part_buffer);
        }

        request->part_buffer = NULL;
    }

    if (request->message != NULL) {
        aws_http_message_destroy(request->message);
        request->message = NULL;
    }

    if (request->finished_callback) {
        request->finished_callback(request, stream, error_code);
    }

    if (request->input_stream != NULL) {
        aws_input_stream_destroy(request->input_stream);
        request->input_stream = NULL;
    }

    /* TODO okay this cleard after the call? */
    request->finished_callback = NULL;
    request->user_data = NULL;
}

void aws_s3_request_destroy(struct aws_allocator *allocator, struct aws_s3_request *request) {
    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(request);

    if (request->message != NULL) {
        aws_http_message_release(request->message);
        request->message = NULL;
    }

    if (request->etag != NULL) {
        aws_string_destroy(request->etag);
        request->etag = NULL;
    }

    aws_mem_release(allocator, request);
    request = NULL;
}

struct aws_input_stream *aws_s3_request_util_assign_body(
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
        return NULL;
    }

    char content_length_buffer[64] = "";
    sprintf(content_length_buffer, "%" PRIu64, (uint64_t)part_buffer_byte_cursor.len);
    struct aws_byte_cursor content_length_cursor =
        aws_byte_cursor_from_array(content_length_buffer, strlen(content_length_buffer));

    if (aws_http_headers_set(headers, g_content_length_header_name_name, content_length_cursor)) {
        aws_input_stream_destroy(input_stream);
        return NULL;
    }

    aws_http_message_set_body_stream(out_message, input_stream);

    return input_stream;
}

struct aws_http_message *aws_s3_request_util_copy_http_message(
    struct aws_allocator *allocator,
    struct aws_http_message *message) {
    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(message);

    struct aws_http_message *message_copy = aws_http_message_new_request(allocator);

    if (message == NULL) {
        return NULL;
    }

    struct aws_byte_cursor request_method;
    if (aws_http_message_get_request_method(message, &request_method)) {
        goto error_clean_up;
    }

    if (aws_http_message_set_request_method(message_copy, request_method)) {
        goto error_clean_up;
    }

    struct aws_byte_cursor request_path;
    if (aws_http_message_get_request_path(message, &request_path)) {
        goto error_clean_up;
    }

    if (aws_http_message_set_request_path(message_copy, request_path)) {
        goto error_clean_up;
    }

    size_t num_headers = aws_http_message_get_header_count(message);

    for (size_t header_index = 0; header_index < num_headers; ++header_index) {
        struct aws_http_header header;

        if (aws_http_message_get_header(message, &header, header_index)) {
            goto error_clean_up;
        }

        if (aws_http_message_add_header(message_copy, header)) {
            goto error_clean_up;
        }
    }

    struct aws_input_stream *body_stream = aws_http_message_get_body_stream(message);
    aws_http_message_set_body_stream(message_copy, body_stream);

    return message_copy;

error_clean_up:

    if (message_copy != NULL) {
        aws_http_message_destroy(message_copy);
        message_copy = NULL;
    }

    return NULL;
}

int aws_s3_request_util_add_content_range_header(
    uint64_t part_index,
    uint64_t part_size,
    struct aws_http_message *out_message) {
    AWS_PRECONDITION(out_message);

    uint64_t range_start = part_index * part_size;
    uint64_t range_end = range_start + part_size - 1;

    char range_value_buffer[128] = ""; /* TODO */
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

int aws_s3_request_util_set_multipart_request_path(
    struct aws_allocator *allocator,
    struct aws_s3_meta_request *meta_request,
    uint32_t part_number,
    struct aws_http_message *message) {

    const struct aws_byte_cursor question_mark = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("?");
    const struct aws_byte_cursor ampersand = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("&");
    const struct aws_byte_cursor part_number_arg = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("partNumber=");
    const struct aws_byte_cursor upload_id_arg = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("uploadId=");

    struct aws_byte_buf request_path_buf;
    struct aws_byte_cursor request_path;
    struct aws_string *upload_id = NULL;

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
        sprintf(part_number_buffer, "%d", part_number);
        struct aws_byte_cursor part_number_cursor =
            aws_byte_cursor_from_array(part_number_buffer, strlen(part_number_buffer));

        if (aws_byte_buf_append_dynamic(&request_path_buf, &part_number_cursor)) {
            goto error_clean_up;
        }
    }

    if (meta_request != NULL) {

        upload_id = aws_s3_meta_request_get_upload_id(meta_request);

        if (upload_id == NULL) {
            goto error_clean_up;
        }

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

        aws_string_destroy(upload_id);
        upload_id = NULL;
    }

    struct aws_byte_cursor new_request_path = aws_byte_cursor_from_buf(&request_path_buf);

    if (aws_http_message_set_request_path(message, new_request_path)) {
        goto error_clean_up;
    }

    aws_byte_buf_clean_up(&request_path_buf);
    return AWS_OP_SUCCESS;

error_clean_up:

    aws_byte_buf_clean_up(&request_path_buf);

    if (upload_id != NULL) {
        aws_string_destroy(upload_id);
        upload_id = NULL;
    }

    return AWS_OP_ERR;
}
