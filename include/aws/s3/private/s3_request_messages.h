#ifndef AWS_S3_REQUEST_MESSAGES_H
#define AWS_S3_REQUEST_MESSAGES_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <inttypes.h>
#include <stddef.h>

struct aws_allocator;
struct aws_http_message;
struct aws_byte_buf;
struct aws_byte_cursor;
struct aws_string;
struct aws_array_list;

enum aws_s3_copy_http_message_flags {
    AWS_S3_COPY_MESSAGE_INCLUDE_SSE = 0x00000001,
    /* For multipart upload complete and abort, only host and two payer related headers are needed */
    AWS_S3_COPY_MESSAGE_MULTIPART_UPLOAD_OPS = 0x00000002,
    /* For ranged put, acl should not be there */
    AWS_S3_COPY_MESSAGE_WITHOUT_ACL = 0x00000004,
};

/* Create an HTTP request for an S3 Get Object Request, using the original request as a basis. If multipart is not
 * needed, part_number and part_size can be 0. */
struct aws_http_message *aws_s3_get_object_message_new(
    struct aws_allocator *allocator,
    struct aws_http_message *base_message,
    uint32_t part_number,
    size_t part_size);

/* Create an HTTP request for an S3 Put Object request, using the original request as a basis.  Creates and assigns a
 * body stream using the passed in buffer.  If multipart is not needed, part number and upload_id can be 0 and NULL,
 * respectively. */
struct aws_http_message *aws_s3_put_object_message_new(
    struct aws_allocator *allocator,
    struct aws_http_message *base_message,
    struct aws_byte_buf *buffer,
    uint32_t part_number,
    const struct aws_string *upload_id);

/* Create an HTTP request for an S3 Create-Multipart-Upload request. */
struct aws_http_message *aws_s3_create_multipart_upload_message_new(
    struct aws_allocator *allocator,
    struct aws_http_message *base_message);

/* Given a response body from a multipart upload, try to extract the upload id. */
struct aws_string *aws_s3_create_multipart_upload_get_upload_id(
    struct aws_allocator *allocator,
    struct aws_byte_cursor *response_body);

/* Create an HTTP request for an S3 Complete-Multipart-Upload request. Creates the necessary XML payload using the
 * passed in array list of ETags.  (Each ETag is assumed to be an aws_string*)  Buffer passed in will be used to store
 * said XML payload, which will be used as the body. */
struct aws_http_message *aws_s3_complete_multipart_message_new(
    struct aws_allocator *allocator,
    struct aws_http_message *base_message,
    struct aws_byte_buf *body_buffer,
    const struct aws_string *upload_id,
    const struct aws_array_list *etags);

struct aws_http_message *aws_s3_abort_multipart_upload_message_new(
    struct aws_allocator *allocator,
    struct aws_http_message *base_message,
    const struct aws_string *upload_id);

/* TODO: maybe set a list of the headers we want */
struct aws_http_message *aws_s3_message_util_copy_http_message(
    struct aws_allocator *allocator,
    struct aws_http_message *message,
    uint32_t flags);

struct aws_input_stream *aws_s3_message_util_assign_body(
    struct aws_allocator *allocator,
    struct aws_byte_buf *byte_buf,
    struct aws_http_message *out_message);

#endif /* AWS_S3_REQUEST_H */
