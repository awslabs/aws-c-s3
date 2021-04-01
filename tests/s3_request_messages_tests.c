/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_client_impl.h"
#include "aws/s3/private/s3_meta_request_impl.h"
#include "aws/s3/private/s3_request_messages.h"
#include "aws/s3/private/s3_util.h"
#include "s3_tester.h"
#include <aws/common/atomics.h>
#include <aws/common/byte_buf.h>
#include <aws/common/clock.h>
#include <aws/common/common.h>
#include <aws/common/ref_count.h>
#include <aws/http/request_response.h>
#include <aws/io/stream.h>
#include <aws/io/tls_channel_handler.h>
#include <aws/testing/aws_test_harness.h>
#include <inttypes.h>

AWS_TEST_CASE(test_s3_copy_http_message, s_test_s3_copy_http_message)
static int s_test_s3_copy_http_message(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    const struct aws_byte_cursor request_method = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("RequestMethod");
    const struct aws_byte_cursor request_path = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("RequestPath");

    const struct aws_http_header included_header = {
        .name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("IncludedHeader"),
        .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("IncludedHeaderValue"),
    };

    const struct aws_http_header excluded_header = {
        .name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("ExcludedHeader"),
        .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("ExcludedHeaderValue"),
    };

    struct aws_http_message *message = aws_http_message_new_request(allocator);
    ASSERT_TRUE(message != NULL);
    ASSERT_SUCCESS(aws_http_message_set_request_method(message, request_method));
    ASSERT_SUCCESS(aws_http_message_set_request_path(message, request_path));

    struct aws_http_headers *message_headers = aws_http_message_get_headers(message);
    ASSERT_TRUE(message != NULL);
    ASSERT_SUCCESS(aws_http_headers_add(message_headers, included_header.name, included_header.value));
    ASSERT_SUCCESS(aws_http_headers_add(message_headers, excluded_header.name, excluded_header.value));

    struct aws_http_message *copied_message =
        aws_s3_message_util_copy_http_message(allocator, message, &excluded_header.name, 1);
    ASSERT_TRUE(copied_message != NULL);

    struct aws_byte_cursor copied_request_method;
    AWS_ZERO_STRUCT(copied_request_method);
    ASSERT_SUCCESS(aws_http_message_get_request_method(copied_message, &copied_request_method));
    ASSERT_TRUE(aws_byte_cursor_eq(&request_method, &copied_request_method));

    struct aws_byte_cursor copied_request_path;
    AWS_ZERO_STRUCT(copied_request_path);
    ASSERT_SUCCESS(aws_http_message_get_request_path(copied_message, &copied_request_path));
    ASSERT_TRUE(aws_byte_cursor_eq(&request_path, &copied_request_path));

    struct aws_http_headers *copied_headers = aws_http_message_get_headers(copied_message);
    ASSERT_TRUE(copied_headers != NULL);
    ASSERT_TRUE(aws_http_headers_count(copied_headers) == 1);

    struct aws_http_header copied_header;
    AWS_ZERO_STRUCT(copied_header);
    ASSERT_SUCCESS(aws_http_headers_get_index(copied_headers, 0, &copied_header));
    ASSERT_TRUE(aws_byte_cursor_eq(&included_header.name, &copied_header.name));
    ASSERT_TRUE(aws_byte_cursor_eq(&included_header.value, &copied_header.value));

    aws_http_message_release(copied_message);
    aws_http_message_release(message);

    return 0;
}

AWS_TEST_CASE(test_s3_message_util_assign_body, s_test_s3_message_util_assign_body)
static int s_test_s3_message_util_assign_body(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_http_message *message = aws_http_message_new_request(allocator);

    const size_t test_buffer_size = 42;
    struct aws_byte_buf test_buffer;
    aws_byte_buf_init(&test_buffer, allocator, test_buffer_size);

    srand(0);

    for (size_t i = 0; i < test_buffer_size; ++i) {
        const char single_char = (char)(rand() % (int)('a' - 'z') + (int)'a');

        struct aws_byte_cursor single_char_cursor = {
            .ptr = (uint8_t*)&single_char,
            .len = 1,
        };

        aws_byte_buf_append(&test_buffer, &single_char_cursor);
    }

    struct aws_input_stream *input_stream = aws_s3_message_util_assign_body(allocator, &test_buffer, message);
    ASSERT_TRUE(input_stream != NULL);

    ASSERT_TRUE(aws_http_message_get_body_stream(message) == input_stream);

    struct aws_http_headers *headers = aws_http_message_get_headers(message);
    ASSERT_TRUE(headers != NULL);

    /* Check for the content length header. */
    struct aws_byte_cursor content_length_header_value;
    AWS_ZERO_STRUCT(content_length_header_value);
    ASSERT_SUCCESS(aws_http_headers_get(headers, g_content_length_header_name, &content_length_header_value));
    ASSERT_TRUE((size_t)atoi((const char *)content_length_header_value.ptr) == test_buffer_size);

    /* Check that the stream data is equal to the original buffer data. */
    struct aws_byte_buf stream_read_buffer;
    ASSERT_SUCCESS(aws_byte_buf_init(&stream_read_buffer, allocator, test_buffer_size));
    ASSERT_SUCCESS(aws_input_stream_read(input_stream, &stream_read_buffer));
    ASSERT_TRUE(aws_byte_buf_eq(&test_buffer, &stream_read_buffer));

    /* There should be no data left in the stream, so additional reads should not cause the buffer to change. */
    ASSERT_SUCCESS(aws_input_stream_read(input_stream, &stream_read_buffer));
    ASSERT_TRUE(aws_byte_buf_eq(&test_buffer, &stream_read_buffer));

    aws_byte_buf_clean_up(&stream_read_buffer);
    aws_input_stream_destroy(input_stream);
    aws_byte_buf_clean_up(&test_buffer);
    aws_http_message_release(message);

    return 0;
}

AWS_TEST_CASE(test_s3_get_object_message_new, s_test_s3_get_object_message_new)
static int s_test_s3_get_object_message_new(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    /*
    struct aws_http_message *aws_s3_get_object_message_new(
        struct aws_allocator *allocator,
        struct aws_http_message *base_message,
        uint32_t part_number,
        size_t part_size,
        bool has_range);
    */

    return 0;
}

AWS_TEST_CASE(test_s3_create_multipart_upload_message_new, s_test_s3_create_multipart_upload_message_new)
static int s_test_s3_create_multipart_upload_message_new(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    /*
    struct aws_http_message *aws_s3_create_multipart_upload_message_new(
        struct aws_allocator *allocator,
        struct aws_http_message *base_message);
    */

    return 0;
}

AWS_TEST_CASE(test_s3_upload_part_message_new, s_test_s3_upload_part_message_new)
static int s_test_s3_upload_part_message_new(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    /*
    struct aws_http_message *aws_s3_upload_part_message_new(
        struct aws_allocator *allocator,
        struct aws_http_message *base_message,
        struct aws_byte_buf *buffer,
        uint32_t part_number,
        const struct aws_string *upload_id);
    */

    return 0;
}

AWS_TEST_CASE(test_s3_complete_multipart_message_new, s_test_s3_complete_multipart_message_new)
static int s_test_s3_complete_multipart_message_new(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    /*
    struct aws_http_message *aws_s3_complete_multipart_message_new(
        struct aws_allocator *allocator,
        struct aws_http_message *base_message,
        struct aws_byte_buf *body_buffer,
        const struct aws_string *upload_id,
        const struct aws_array_list *etags);
    */

    return 0;
}

AWS_TEST_CASE(test_s3_abort_multipart_upload_message_new, s_test_s3_abort_multipart_upload_message_newt)
static int s_test_s3_abort_multipart_upload_message_newt(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    /*
    struct aws_http_message *aws_s3_abort_multipart_upload_message_new(
        struct aws_allocator *allocator,
        struct aws_http_message *base_message,
        const struct aws_string *upload_id);
    */

    return 0;
}
