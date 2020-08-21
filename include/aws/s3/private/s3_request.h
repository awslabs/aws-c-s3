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
struct aws_s3_request_pipeline;

struct aws_allocator;
struct aws_signable;
struct aws_http_message;
struct aws_http_stream;
struct aws_string;

enum aws_s3_request_type {
    AWS_S3_REQUEST_TYPE_GET_OBJECT,
    AWS_S3_REQUEST_TYPE_PUT_OBJECT,
    AWS_S3_REQUEST_TYPE_CREATE_MULTIPART,
    AWS_S3_REQUEST_TYPE_UPLOAD_PART,
    AWS_S3_REQUEST_TYPE_COMPLETE_MULTIPART
};

enum aws_s3_incoming_body_flags { AWS_S3_INCOMING_BODY_FLAG_OBJECT_DATA = 0x00000001 };

struct aws_s3_body_meta_data {
    uint32_t data_flags;
    uint64_t range_start;
    uint64_t range_end;
};

/* VTable for s3 requests.  Right now, these are mostly HTTP handlers to allow different behavior per request type
 * during a request. */
struct aws_s3_request_vtable {

    void (*destroy)(struct aws_s3_request *request);

    int (*incoming_headers)(
        struct aws_s3_request *request,
        struct aws_http_stream *stream,
        enum aws_http_header_block header_block,
        const struct aws_http_header *headers,
        size_t headers_count);

    int (*incoming_header_block_done)(
        struct aws_s3_request *request,
        struct aws_http_stream *stream,
        enum aws_http_header_block header_block);

    int (*incoming_body)(
        struct aws_s3_request *request,
        struct aws_http_stream *stream,
        const struct aws_byte_cursor *data,
        struct aws_s3_body_meta_data *out_meta_data);

    void (*stream_complete)(struct aws_s3_request *request, struct aws_http_stream *stream, int error_code);

    void (*request_finish)(struct aws_s3_request *request, int error_code);
};

struct aws_s3_request_options {
    struct aws_http_message *message;
};

/* Base type for an s3 request.  There can be thousands of these in an individual file transfer, so it's worth trying to
 * keep this structure light.  Because this is an internal type and we want to optimize this structure, we assume that
 * all derived request types have this type first to get round having an impl pointer.
 *
 * We currently define a request_type to provide a little bit of type introspection, which we use to derive a VTable.
 * This might be swapped out in the future for a VTable pointer if turns out we don't really need that, since that field
 * will still align to the size of a pointer anyway. */
struct aws_s3_request {
    enum aws_s3_request_type request_type;

    /* TODO this should be implied by the owning meta request*/
    struct aws_allocator *allocator;

    /* The request's place in the meta request queue. */
    struct aws_linked_list_node node;

    /* Actual HTTP message for the request. */
    struct aws_http_message *message;

    /* It does not currently appear possible to re-sign a request, so we cache the signable for when a request fails
     * after signing and we need to retry it. */
    struct aws_signable *signable;
};

int aws_s3_request_init(
    struct aws_s3_request *request,
    struct aws_allocator *allocator,
    enum aws_s3_request_type request_type,
    const struct aws_s3_request_options *options);

void aws_s3_request_destroy(struct aws_s3_request *request);

/* TODO functions below are used only by the request pipeline for HTTP requests.  There should be a way to cleverly hide
 * these such that they don't need to be in the header file, where they look like a public interface. */
int aws_s3_request_incoming_headers(
    struct aws_s3_request *request,
    struct aws_http_stream *stream,
    enum aws_http_header_block header_block,
    const struct aws_http_header *headers,
    size_t headers_count);

int aws_s3_request_incoming_header_block_done(
    struct aws_s3_request *request,
    struct aws_http_stream *stream,
    enum aws_http_header_block header_block);

int aws_s3_request_incoming_body(
    struct aws_s3_request *request,
    struct aws_http_stream *stream,
    const struct aws_byte_cursor *data,
    struct aws_s3_body_meta_data *out_meta_data);

void aws_s3_request_stream_complete(struct aws_s3_request *request, struct aws_http_stream *stream, int error_code);

void aws_s3_request_finish(struct aws_s3_request *request, int error_code);

#endif /* AWS_S3_REQUEST_H */
