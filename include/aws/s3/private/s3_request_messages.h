#ifndef AWS_S3_REQUEST_MESSAGES_H
#define AWS_S3_REQUEST_MESSAGES_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <inttypes.h>

struct aws_allocator;
struct aws_http_message;
struct aws_byte_buf;
struct aws_byte_cursor;
struct aws_string;
struct aws_array_list;

struct aws_http_message *aws_s3_get_object_message_new(
    struct aws_allocator *allocator,
    struct aws_http_message *base_message,
    uint32_t part_number,
    uint64_t part_size);

struct aws_http_message *aws_s3_put_object_message_new(
    struct aws_allocator *allocator,
    struct aws_http_message *base_message,
    struct aws_byte_buf *buffer,
    uint32_t part_number,
    const struct aws_string *upload_id);

struct aws_http_message *aws_s3_create_multipart_upload_message_new(
    struct aws_allocator *allocator,
    struct aws_http_message *base_message);

struct aws_http_message *aws_s3_complete_multipart_message_new(
    struct aws_allocator *allocator,
    struct aws_http_message *base_message,
    struct aws_byte_buf *buffer,
    const struct aws_string *upload_id,
    const struct aws_array_list *etags);

struct aws_string *aws_s3_create_multipart_upload_get_upload_id(
    struct aws_allocator *allocator,
    struct aws_byte_cursor *response_body);

#endif /* AWS_S3_REQUEST_H */
