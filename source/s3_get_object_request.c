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
#include <aws/http/request_response.h>
#include <aws/io/stream.h>
#include <inttypes.h>

extern struct aws_s3_request_vtable g_aws_s3_get_object_request_vtable;
extern struct aws_s3_request_vtable g_aws_s3_seed_get_object_request_vtable;

static int s_s3_get_object_prepare_for_send(struct aws_s3_request *request);

static int s_s3_seed_get_object_incoming_headers(
    struct aws_http_stream *stream,
    enum aws_http_header_block header_block,
    const struct aws_http_header *headers,
    size_t headers_count,
    void *user_data);

struct aws_s3_request_vtable g_aws_s3_get_object_request_vtable = {
    .request_type_flags =
        AWS_S3_REQUEST_TYPE_FLAG_WRITE_BODY_TO_PART_BUFFER | AWS_S3_REQUEST_TYPE_FLAG_WRITE_PART_BUFFER_TO_CALLER,
    .prepare_for_send = s_s3_get_object_prepare_for_send};

struct aws_s3_request_vtable g_aws_s3_seed_get_object_request_vtable = {
    .request_type_flags =
        AWS_S3_REQUEST_TYPE_FLAG_WRITE_BODY_TO_PART_BUFFER | AWS_S3_REQUEST_TYPE_FLAG_WRITE_PART_BUFFER_TO_CALLER,
    .prepare_for_send = s_s3_get_object_prepare_for_send,
    .incoming_headers = s_s3_seed_get_object_incoming_headers};

static int s_s3_get_object_prepare_for_send(struct aws_s3_request *request) {
    AWS_PRECONDITION(request);
    AWS_PRECONDITION(request->meta_request);

    struct aws_s3_meta_request *meta_request = request->meta_request;

    struct aws_http_message *message =
        aws_s3_request_util_copy_http_message(meta_request->allocator, meta_request->initial_request_message);

    if (message == NULL) {
        return AWS_OP_ERR;
    }

    if (request->part_number > 0) {
        if (aws_s3_request_util_add_content_range_header(request->part_number - 1, meta_request->part_size, message)) {
            goto error_clean_up;
        }
    }

    request->message = message;

    return AWS_OP_SUCCESS;

error_clean_up:

    if (message != NULL) {
        aws_http_message_destroy(message);
        message = NULL;
    }

    return AWS_OP_ERR;
}

static int s_s3_seed_get_object_incoming_headers(
    struct aws_http_stream *stream,
    enum aws_http_header_block header_block,
    const struct aws_http_header *headers,
    size_t headers_count,
    void *user_data) {

    AWS_PRECONDITION(stream);
    AWS_PRECONDITION(user_data);
    struct aws_s3_request *request = user_data;

    (void)stream;
    (void)header_block;

    /* Find the Content-Range header and extract the object size. */
    for (size_t i = 0; i < headers_count; ++i) {
        const struct aws_byte_cursor *name = &headers[i].name;
        const struct aws_byte_cursor *value = &headers[i].value;

        if (!aws_http_header_name_eq(*name, g_content_range_header_name)) {
            continue;
        }

        uint64_t range_start = 0;
        uint64_t range_end = 0;
        uint64_t total_object_size = 0;
        /* Format of header is: "bytes StartByte-EndByte/TotalObjectSize" */
        sscanf(
            (const char *)value->ptr,
            "bytes %" PRIu64 "-%" PRIu64 "/%" PRIu64,
            &range_start,
            &range_end,
            &total_object_size);

        if (total_object_size == 0) {
            AWS_LOGF_ERROR(AWS_LS_S3_REQUEST, "id=%p S3 Get Object has invalid content range.", (void *)request);
            aws_raise_error(AWS_ERROR_S3_MISSING_CONTENT_RANGE_HEADER);
            return AWS_OP_ERR;
        }

        /* Try to queue additional requests based off the retrieved object size. */
        return aws_s3_meta_request_generate_ranged_requests(
            request->meta_request,
            AWS_S3_REQUEST_TYPE_GET_OBJECT,
            0,
            total_object_size - 1,
            AWS_S3_META_REQUEST_GEN_RANGED_FLAG_SKIP_FIRST | AWS_S3_META_REQUEST_GEN_RANGED_FLAG_QUEUING_DONE);
    }

    return AWS_OP_SUCCESS;
}
