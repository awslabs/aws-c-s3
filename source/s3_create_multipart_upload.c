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

extern struct aws_s3_request_vtable g_aws_s3_create_multipart_upload_request_vtable;

int s_s3_create_multipart_set_up_request_path(struct aws_allocator *allocator, struct aws_http_message *message);

int s_s3_create_multipart_upload_prepare_for_send(struct aws_s3_request *request);

static bool s_s3_create_multipart_upload_child_xml_node(
    struct aws_xml_parser *parser,
    struct aws_xml_node *node,
    void *user_data);

static bool s_s3_create_multipart_upload_root_xml_node(
    struct aws_xml_parser *parser,
    struct aws_xml_node *node,
    void *user_data);

static int s_s3_create_multipart_upload_finish(struct aws_http_stream *stream, int error_code, void *user_data);

struct aws_s3_request_vtable g_aws_s3_create_multipart_upload_request_vtable = {
    .request_type_flags = AWS_S3_REQUEST_TYPE_FLAG_WRITE_BODY_TO_PART_BUFFER,
    .prepare_for_send = s_s3_create_multipart_upload_prepare_for_send,
    .stream_complete = s_s3_create_multipart_upload_finish};

int s_s3_create_multipart_upload_prepare_for_send(struct aws_s3_request *request) {

    AWS_PRECONDITION(request);

    struct aws_s3_meta_request *meta_request = request->meta_request;
    AWS_PRECONDITION(meta_request);

    struct aws_http_message *message_copy =
        aws_s3_request_util_copy_http_message(meta_request->allocator, meta_request->initial_request_message);

    if (message_copy == NULL) {
        return AWS_OP_ERR;
    }

    if (s_s3_create_multipart_set_up_request_path(meta_request->allocator, message_copy)) {
        goto error_clean_up;
    }

    struct aws_http_headers *headers = aws_http_message_get_headers(message_copy);

    if (headers == NULL) {
        goto error_clean_up;
    }

    if (aws_http_headers_erase(headers, g_content_length_header_name_name)) {
        goto error_clean_up;
    }

    const struct aws_byte_cursor post_method = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("POST");
    aws_http_message_set_request_method(message_copy, post_method);

    request->message = message_copy;

    return AWS_OP_SUCCESS;

error_clean_up:

    if (message_copy != NULL) {
        aws_http_message_destroy(message_copy);
        message_copy = NULL;
    }

    return AWS_OP_ERR;
}

struct create_multipart_upload_xml_user_data {
    struct aws_allocator *allocator;
    struct aws_string *upload_id;
};

static bool s_s3_create_multipart_upload_child_xml_node(
    struct aws_xml_parser *parser,
    struct aws_xml_node *node,
    void *user_data) {

    const struct aws_byte_cursor upload_id_tag_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("UploadId");

    struct aws_byte_cursor node_name;

    if (aws_xml_node_get_name(node, &node_name)) {
        return false;
    }

    struct create_multipart_upload_xml_user_data *multipart_upload_xml_user_data = user_data;

    if (aws_byte_cursor_eq(&node_name, &upload_id_tag_name)) {

        struct aws_byte_cursor node_body;
        aws_xml_node_as_body(parser, node, &node_body);

        multipart_upload_xml_user_data->upload_id =
            aws_string_new_from_cursor(multipart_upload_xml_user_data->allocator, &node_body);

        return false;
    }

    return true;
}

static bool s_s3_create_multipart_upload_root_xml_node(
    struct aws_xml_parser *parser,
    struct aws_xml_node *node,
    void *user_data) {

    aws_xml_node_traverse(parser, node, s_s3_create_multipart_upload_child_xml_node, user_data);

    return false;
}

static int s_s3_create_multipart_upload_finish(struct aws_http_stream *stream, int error_code, void *user_data) {
    (void)stream;
    AWS_PRECONDITION(user_data);

    struct aws_s3_request *request = user_data;
    AWS_PRECONDITION(request);

    struct aws_s3_meta_request *meta_request = request->meta_request;
    AWS_PRECONDITION(meta_request);

    if (error_code != AWS_ERROR_SUCCESS) {
        return AWS_OP_ERR;
    }

    struct aws_byte_cursor body_contents = aws_byte_cursor_from_buf(&request->part_buffer->buffer);
    struct aws_xml_parser_options parser_options = {.doc = body_contents};
    struct aws_xml_parser *parser = aws_xml_parser_new(meta_request->allocator, &parser_options);

    int result = AWS_OP_ERR;

    struct create_multipart_upload_xml_user_data xml_user_data = {
        meta_request->allocator,
        NULL,
    };

    if (aws_xml_parser_parse(parser, s_s3_create_multipart_upload_root_xml_node, (void *)&xml_user_data)) {
        AWS_LOGF_ERROR(AWS_LS_S3_REQUEST, "id=%p Could not find upload id.", (void *)request);
        goto clean_up;
    }

    if (xml_user_data.upload_id == NULL) {
        AWS_LOGF_ERROR(AWS_LS_S3_REQUEST, "id=%p Could not find upload id.", (void *)request);
        goto clean_up;
    }

    struct aws_byte_cursor upload_id_cursor = aws_byte_cursor_from_string(xml_user_data.upload_id);
    if (aws_s3_meta_request_set_upload_id(meta_request, &upload_id_cursor)) {
        goto clean_up;
    }

    int64_t request_body_length = 0;

    if (aws_s3_meta_requests_get_total_object_size(meta_request, &request_body_length)) {
        goto clean_up;
    }

    if (aws_s3_meta_request_generate_ranged_requests(
            meta_request, AWS_S3_REQUEST_TYPE_PUT_OBJECT, 0, request_body_length - 1, 0)) {
        AWS_LOGF_ERROR(AWS_LS_S3_REQUEST, "id=%p Could not push upload part requests.", (void *)request);
        goto clean_up;
    }

    struct aws_s3_request_options complete_multipart_upload_request_options = {
        .request_type = AWS_S3_REQUEST_TYPE_COMPLETE_MULTIPART_UPLOAD,
    };

    if (aws_s3_meta_request_push_new_request(
            meta_request, &complete_multipart_upload_request_options, S3_PUSH_NEW_REQUEST_FLAG_QUEUING_DONE)) {
        AWS_LOGF_ERROR(AWS_LS_S3_REQUEST, "id=%p Could not push complete multiaprt upload request.", (void *)request);
        goto clean_up;
    }

    result = AWS_OP_SUCCESS;

clean_up:

    if (parser != NULL) {
        aws_xml_parser_destroy(parser);
        parser = NULL;
    }

    if (xml_user_data.upload_id != NULL) {
        aws_string_destroy(xml_user_data.upload_id);
        xml_user_data.upload_id = NULL;
    }

    return result;
}

int s_s3_create_multipart_set_up_request_path(struct aws_allocator *allocator, struct aws_http_message *message) {
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
