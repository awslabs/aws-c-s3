/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_util.h"
#include "aws/s3/private/s3_client_impl.h"
#include <aws/auth/credentials.h>
#include <aws/common/string.h>
#include <aws/common/xml_parser.h>
#include <aws/http/request_response.h>
#include <aws/s3/s3.h>
#include <aws/s3/s3_client.h>

const struct aws_byte_cursor g_s3_client_version = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL(AWS_S3_CLIENT_VERSION);
const struct aws_byte_cursor g_s3_service_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("s3");
const struct aws_byte_cursor g_host_header_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Host");
const struct aws_byte_cursor g_range_header_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Range");
const struct aws_byte_cursor g_etag_header_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("ETag");
const struct aws_byte_cursor g_content_range_header_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Content-Range");
const struct aws_byte_cursor g_content_type_header_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Content-Type");
const struct aws_byte_cursor g_content_length_header_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Content-Length");
const struct aws_byte_cursor g_accept_ranges_header_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("accept-ranges");
const struct aws_byte_cursor g_acl_header_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-acl");
const struct aws_byte_cursor g_post_method = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("POST");
const struct aws_byte_cursor g_delete_method = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("DELETE");

const struct aws_byte_cursor g_user_agent_header_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("User-Agent");
const struct aws_byte_cursor g_user_agent_header_product_name =
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("CRTS3NativeClient");

const uint32_t g_s3_max_num_upload_parts = 10000;
const size_t g_s3_min_upload_part_size = MB_TO_BYTES(5);

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

struct aws_cached_signing_config_aws *aws_cached_signing_config_new(
    struct aws_allocator *allocator,
    const struct aws_signing_config_aws *signing_config) {
    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(signing_config);

    struct aws_cached_signing_config_aws *cached_signing_config =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_cached_signing_config_aws));

    cached_signing_config->allocator = allocator;

    cached_signing_config->config.config_type = signing_config->config_type;
    cached_signing_config->config.algorithm = signing_config->algorithm;
    cached_signing_config->config.signature_type = signing_config->signature_type;

    AWS_ASSERT(aws_byte_cursor_is_valid(&signing_config->region));

    if (signing_config->region.len > 0) {
        cached_signing_config->region = aws_string_new_from_cursor(allocator, &signing_config->region);

        cached_signing_config->config.region = aws_byte_cursor_from_string(cached_signing_config->region);
    }

    AWS_ASSERT(aws_byte_cursor_is_valid(&signing_config->service));

    if (signing_config->service.len > 0) {
        cached_signing_config->service = aws_string_new_from_cursor(allocator, &signing_config->service);

        cached_signing_config->config.service = aws_byte_cursor_from_string(cached_signing_config->service);
    }

    cached_signing_config->config.date = signing_config->date;

    cached_signing_config->config.should_sign_header = signing_config->should_sign_header;
    cached_signing_config->config.flags = signing_config->flags;

    AWS_ASSERT(aws_byte_cursor_is_valid(&signing_config->signed_body_value));

    if (signing_config->service.len > 0) {
        cached_signing_config->signed_body_value =
            aws_string_new_from_cursor(allocator, &signing_config->signed_body_value);

        cached_signing_config->config.signed_body_value =
            aws_byte_cursor_from_string(cached_signing_config->signed_body_value);
    }

    cached_signing_config->config.signed_body_header = signing_config->signed_body_header;

    if (signing_config->credentials != NULL) {
        aws_credentials_acquire(signing_config->credentials);
        cached_signing_config->config.credentials = signing_config->credentials;
    }

    if (signing_config->credentials_provider != NULL) {
        aws_credentials_provider_acquire(signing_config->credentials_provider);
        cached_signing_config->config.credentials_provider = signing_config->credentials_provider;
    }

    cached_signing_config->config.expiration_in_seconds = signing_config->expiration_in_seconds;

    return cached_signing_config;
}

void aws_cached_signing_config_destroy(struct aws_cached_signing_config_aws *cached_signing_config) {
    if (cached_signing_config == NULL) {
        return;
    }

    aws_credentials_release(cached_signing_config->config.credentials);
    aws_credentials_provider_release(cached_signing_config->config.credentials_provider);

    aws_string_destroy(cached_signing_config->service);
    aws_string_destroy(cached_signing_config->region);
    aws_string_destroy(cached_signing_config->signed_body_value);

    aws_mem_release(cached_signing_config->allocator, cached_signing_config);
}

void aws_s3_init_default_signing_config(
    struct aws_signing_config_aws *signing_config,
    const struct aws_byte_cursor region,
    struct aws_credentials_provider *credentials_provider) {
    AWS_PRECONDITION(signing_config);
    AWS_PRECONDITION(credentials_provider);

    AWS_ZERO_STRUCT(*signing_config);

    signing_config->config_type = AWS_SIGNING_CONFIG_AWS;
    signing_config->algorithm = AWS_SIGNING_ALGORITHM_V4;
    signing_config->credentials_provider = credentials_provider;
    signing_config->region = region;
    signing_config->service = g_s3_service_name;
    signing_config->signed_body_header = AWS_SBHT_X_AMZ_CONTENT_SHA256;
    signing_config->signed_body_value = g_aws_signed_body_value_unsigned_payload;
}

void replace_quote_entities(struct aws_allocator *allocator, struct aws_string *str, struct aws_byte_buf *out_buf) {
    AWS_PRECONDITION(str);

    aws_byte_buf_init(out_buf, allocator, str->len);

    struct aws_byte_cursor quote_entity = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("&quot;");
    struct aws_byte_cursor quote = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("\"");

    size_t i = 0;

    while (i < str->len) {
        size_t chars_remaining = str->len - i;

        if (chars_remaining >= quote_entity.len &&
            !strncmp((const char *)&str->bytes[i], (const char *)quote_entity.ptr, quote_entity.len)) {
            /* Append quote */
            aws_byte_buf_append(out_buf, &quote);
            i += quote_entity.len;
        } else {
            /* Append character */
            struct aws_byte_cursor character_cursor = aws_byte_cursor_from_array(&str->bytes[i], 1);
            aws_byte_buf_append(out_buf, &character_cursor);
            ++i;
        }
    }
}

int aws_last_error_or_unknown() {
    int error = aws_last_error();

    if (error == AWS_ERROR_SUCCESS) {
        return AWS_ERROR_UNKNOWN;
    }

    return error;
}

void aws_s3_add_user_agent_header(struct aws_allocator *allocator, struct aws_http_message *message) {
    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(message);

    const struct aws_byte_cursor space_delimeter = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL(" ");
    const struct aws_byte_cursor forward_slash = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("/");

    const size_t user_agent_product_version_length =
        g_user_agent_header_product_name.len + forward_slash.len + g_s3_client_version.len;

    struct aws_http_headers *headers = aws_http_message_get_headers(message);
    AWS_ASSERT(headers != NULL);

    struct aws_byte_cursor current_user_agent_header;
    AWS_ZERO_STRUCT(current_user_agent_header);

    struct aws_byte_buf user_agent_buffer;
    AWS_ZERO_STRUCT(user_agent_buffer);

    if (!aws_http_headers_get(headers, g_user_agent_header_name, &current_user_agent_header)) {
        /* If the header was found, then create a buffer with the total size we'll need, and append the curent user
         * agent header with a trailing space. */
        aws_byte_buf_init(
            &user_agent_buffer,
            allocator,
            current_user_agent_header.len + space_delimeter.len + user_agent_product_version_length);

        aws_byte_buf_append_dynamic(&user_agent_buffer, &current_user_agent_header);

        aws_byte_buf_append_dynamic(&user_agent_buffer, &space_delimeter);

    } else {
        AWS_ASSERT(aws_last_error() == AWS_ERROR_HTTP_HEADER_NOT_FOUND);

        /* If the header was not found, then create a buffer with just the size of the user agent string that is about
         * to be appended to the buffer. */
        aws_byte_buf_init(&user_agent_buffer, allocator, user_agent_product_version_length);
    }

    /* Append the client's user-agent string. */
    {
        aws_byte_buf_append_dynamic(&user_agent_buffer, &g_user_agent_header_product_name);
        aws_byte_buf_append_dynamic(&user_agent_buffer, &forward_slash);
        aws_byte_buf_append_dynamic(&user_agent_buffer, &g_s3_client_version);
    }

    /* Apply the updated header. */
    aws_http_headers_set(headers, g_user_agent_header_name, aws_byte_cursor_from_buf(&user_agent_buffer));

    /* Clean up the scratch buffer. */
    aws_byte_buf_clean_up(&user_agent_buffer);
}
