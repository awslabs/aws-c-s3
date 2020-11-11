/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_util.h"
#include <aws/common/string.h>
#include <aws/common/xml_parser.h>
#include <aws/http/request_response.h>
#include <aws/s3/s3.h>

const struct aws_byte_cursor g_host_header_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Host");
const struct aws_byte_cursor g_range_header_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Range");
const struct aws_byte_cursor g_etag_header_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("ETag");
const struct aws_byte_cursor g_content_range_header_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Content-Range");
const struct aws_byte_cursor g_content_type_header_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Content-Type");
const struct aws_byte_cursor g_content_length_header_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Content-Length");
const struct aws_byte_cursor g_accept_ranges_header_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("accept-ranges");
const struct aws_byte_cursor g_post_method = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("POST");

void copy_http_headers(const struct aws_http_headers *src, struct aws_http_headers *dest) {
    AWS_PRECONDITION(src);
    AWS_PRECONDITION(dest);

    size_t headers_count = aws_http_headers_count(src);

    for (size_t header_index = 0; header_index < headers_count; ++header_index) {
        struct aws_http_header header;

        aws_http_headers_get_index(src, header_index, &header);
        aws_http_headers_set(dest, header.name, header.value);
    }
}

struct top_level_xml_tag_value_user_data {
    struct aws_allocator *allocator;
    const struct aws_byte_cursor *tag_name;
    struct aws_string *result;
};

static bool s_top_level_xml_tag_value_child_xml_node(
    struct aws_xml_parser *parser,
    struct aws_xml_node *node,
    void *user_data) {

    struct aws_byte_cursor node_name;

    /* If we can't get the name of the node, stop traversing. */
    if (aws_xml_node_get_name(node, &node_name)) {
        return false;
    }

    struct top_level_xml_tag_value_user_data *xml_user_data = user_data;

    /* If the name of the node is what we are looking for, store the body of the node in our result, and stop
     * traversing. */
    if (aws_byte_cursor_eq(&node_name, xml_user_data->tag_name)) {

        struct aws_byte_cursor node_body;
        aws_xml_node_as_body(parser, node, &node_body);

        xml_user_data->result = aws_string_new_from_cursor(xml_user_data->allocator, &node_body);

        return false;
    }

    /* If we made it here, the tag hasn't been found yet, so return true to keep looking. */
    return true;
}

static bool s_top_level_xml_tag_value_root_xml_node(
    struct aws_xml_parser *parser,
    struct aws_xml_node *node,
    void *user_data) {

    /* Traverse the root node, and then return false to stop. */
    aws_xml_node_traverse(parser, node, s_top_level_xml_tag_value_child_xml_node, user_data);
    return false;
}

struct aws_string *get_top_level_xml_tag_value(
    struct aws_allocator *allocator,
    const struct aws_byte_cursor *tag_name,
    struct aws_byte_cursor *xml_body) {
    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(tag_name);
    AWS_PRECONDITION(xml_body);

    struct aws_xml_parser_options parser_options = {.doc = *xml_body};
    struct aws_xml_parser *parser = aws_xml_parser_new(allocator, &parser_options);

    struct top_level_xml_tag_value_user_data xml_user_data = {
        allocator,
        tag_name,
        NULL,
    };

    if (aws_xml_parser_parse(parser, s_top_level_xml_tag_value_root_xml_node, (void *)&xml_user_data)) {
        aws_string_destroy(xml_user_data.result);
        xml_user_data.result = NULL;
        goto clean_up;
    }

clean_up:

    aws_xml_parser_destroy(parser);

    return xml_user_data.result;
}
