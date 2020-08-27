#ifndef AWS_S3_REQUEST_H
#define AWS_S3_REQUEST_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/atomics.h>
#include <aws/common/linked_list.h>
#include <aws/http/request_response.h>
#include <aws/s3/s3.h>

struct aws_s3_client;
struct aws_s3_meta_request;
struct aws_s3_request;
struct aws_s3_request_options;
struct aws_s3_part_buffer;

struct aws_allocator;
struct aws_http_message;
struct aws_http_stream;
struct aws_string;

enum aws_s3_request_type {
    AWS_S3_REQUEST_TYPE_GET_OBJECT,
    AWS_S3_REQUEST_TYPE_SEED_GET_OBJECT,
    AWS_S3_REQUEST_TYPE_PUT_OBJECT,
    AWS_S3_REQUEST_TYPE_CREATE_MULTIPART_UPLOAD,
    AWS_S3_REQUEST_TYPE_COMPLETE_MULTIPART_UPLOAD
};

enum aws_s3_request_type_flag {
    AWS_S3_REQUEST_TYPE_FLAG_WRITE_BODY_TO_PART_BUFFER = 0x00000001,
    AWS_S3_REQUEST_TYPE_FLAG_WRITE_PART_BUFFER_TO_CALLER = 0x00000002,
    AWS_S3_REQUEST_TYPE_FLAG_MUST_BE_LAST = 0x0000004
};

typedef void(aws_s3_request_finished_callback_fn)(
    struct aws_s3_request *request,
    struct aws_http_stream *stream,
    int error_code);

struct aws_s3_request_vtable {

    uint32_t request_type_flags;

    int (*prepare_for_send)(struct aws_s3_request *request);

    aws_http_on_incoming_headers_fn *incoming_headers;

    aws_http_on_incoming_header_block_done_fn *incoming_headers_block_done;

    aws_http_on_incoming_body_fn *incoming_body;

    int (*stream_complete)(struct aws_http_stream *stream, int error_code, void *user_data);
};

struct aws_s3_request_options {
    enum aws_s3_request_type request_type;

    uint32_t part_number;
};

/* Structure for all s3 requests.  There can be thousands of these in an individual file transfer, so it's worth trying
 * to keep this structure as light as possible.  Allocation is the responsibility of the owning meta request. */
struct aws_s3_request {
    enum aws_s3_request_type request_type;

    uint32_t part_number;

    /* The request's place in the meta request queue. */
    struct aws_linked_list_node node;

    struct aws_string *etag;

    /* BEGIN Transient Data - This data is only alive during a request, and maybe should be moved elsewhere. TODO */
    /* Actual HTTP message for the request.  */
    struct aws_http_message *message;

    /* Part buffer currently in use by this message. */
    struct aws_s3_part_buffer *part_buffer;

    struct aws_s3_meta_request *meta_request;

    aws_s3_request_finished_callback_fn *finished_callback;

    struct aws_input_stream *input_stream;

    void *user_data;
    /* END Transient Data */
};

struct aws_input_stream *aws_s3_request_util_assign_body(
    struct aws_allocator *allocator,
    struct aws_byte_buf *byte_buf,
    struct aws_http_message *out_message);

struct aws_http_message *aws_s3_request_util_copy_http_message(
    struct aws_allocator *allocator,
    struct aws_http_message *message);

int aws_s3_request_util_set_multipart_request_path(
    struct aws_allocator *allocator,
    struct aws_s3_meta_request *meta_request,
    uint32_t part_number,
    struct aws_http_message *message);

int aws_s3_request_util_add_content_range_header(
    uint64_t part_index,
    uint64_t part_size,
    struct aws_http_message *out_message);

struct aws_s3_request *aws_s3_request_new(
    struct aws_s3_meta_request *meta_request,
    const struct aws_s3_request_options *options);

uint32_t aws_s3_request_get_type_flags(struct aws_s3_request *request);

int aws_s3_request_prepare_for_send(
    struct aws_s3_request *request,
    struct aws_s3_client *client); /* TODO better way we can discover the client? */

int aws_s3_request_make_request(
    struct aws_s3_request *request,
    struct aws_http_connection *http_connection,
    aws_s3_request_finished_callback_fn *finished_callback,
    void *user_data);

void aws_s3_request_destroy(struct aws_allocator *allocator, struct aws_s3_request *request);

#endif /* AWS_S3_REQUEST_H */
