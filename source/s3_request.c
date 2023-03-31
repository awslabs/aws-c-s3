/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_request.h"
#include "aws/s3/private/s3_meta_request_impl.h"
#include <aws/auth/signable.h>
#include <aws/io/stream.h>
#include <aws/s3/s3_client.h>

static void s_s3_request_destroy(void *user_data);

struct aws_s3_request *aws_s3_request_new(
    struct aws_s3_meta_request *meta_request,
    int request_tag,
    uint32_t part_number,
    uint32_t flags) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(meta_request->allocator);

    struct aws_s3_request *request = aws_mem_calloc(meta_request->allocator, 1, sizeof(struct aws_s3_request));

    aws_ref_count_init(&request->ref_count, request, (aws_simple_completion_callback *)s_s3_request_destroy);

    request->allocator = meta_request->allocator;
    request->meta_request = aws_s3_meta_request_acquire(meta_request);

    request->request_tag = request_tag;
    request->part_number = part_number;
    request->record_response_headers = (flags & AWS_S3_REQUEST_FLAG_RECORD_RESPONSE_HEADERS) != 0;
    request->part_size_response_body = (flags & AWS_S3_REQUEST_FLAG_PART_SIZE_RESPONSE_BODY) != 0;
    request->always_send = (flags & AWS_S3_REQUEST_FLAG_ALWAYS_SEND) != 0;

    return request;
}

void aws_s3_request_setup_send_data(struct aws_s3_request *request, struct aws_http_message *message) {
    AWS_PRECONDITION(request);
    AWS_PRECONDITION(message);

    aws_s3_request_clean_up_send_data(request);

    request->send_data.message = message;
    struct aws_s3_meta_request *meta_request = request->meta_request;
    if (meta_request->telemetry_callback) {
        /* start the telemetry for the request to be sent */
        request->send_data.metrics = aws_s3_request_metrics_new(request->allocator, message);
        /* Start the timestamp */
    }

    aws_http_message_acquire(message);
}

static void s_s3_request_clean_up_send_data_message(struct aws_s3_request *request) {
    AWS_PRECONDITION(request);

    struct aws_http_message *message = request->send_data.message;

    if (message == NULL) {
        return;
    }

    request->send_data.message = NULL;
    aws_http_message_release(message);
}

void aws_s3_request_clean_up_send_data(struct aws_s3_request *request) {
    AWS_PRECONDITION(request);

    s_s3_request_clean_up_send_data_message(request);

    aws_signable_destroy(request->send_data.signable);
    request->send_data.signable = NULL;
    if (request->send_data.metrics) {
        /* invoke callback */
        /* TODO: I checked the code path that can invoke this call, we never hold a lock from any of those code path.
         * But, can we be more clear that it's a requirement to not have lock held here? */
        struct aws_s3_meta_request *meta_request = request->meta_request;
        /* End the timestamp */
        if (meta_request->telemetry_callback) {
            meta_request->telemetry_callback(meta_request, request->send_data.metrics, meta_request->user_data);
        }
        request->send_data.metrics = aws_s3_request_metrics_release(request->send_data.metrics);
    }

    aws_http_headers_release(request->send_data.response_headers);
    request->send_data.response_headers = NULL;

    aws_byte_buf_clean_up(&request->send_data.response_body);

    AWS_ZERO_STRUCT(request->send_data);
}

void aws_s3_request_acquire(struct aws_s3_request *request) {
    AWS_PRECONDITION(request);

    aws_ref_count_acquire(&request->ref_count);
}

void aws_s3_request_release(struct aws_s3_request *request) {
    if (request == NULL) {
        return;
    }

    aws_ref_count_release(&request->ref_count);
}

static void s_s3_request_destroy(void *user_data) {
    struct aws_s3_request *request = user_data;

    if (request == NULL) {
        return;
    }

    aws_s3_request_clean_up_send_data(request);
    aws_byte_buf_clean_up(&request->request_body);
    aws_s3_meta_request_release(request->meta_request);

    aws_mem_release(request->allocator, request);
}

static void s_s3_request_metrics_destroy(void *arg) {
    struct aws_s3_request_metrics *metrics = arg;

    aws_mem_release(metrics->allocator, metrics);
}

struct aws_s3_request_metrics *aws_s3_request_metrics_new(
    struct aws_allocator *allocator,
    struct aws_http_message *message) {
    struct aws_s3_request_metrics *metrics = aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_request_metrics));
    metrics->allocator = allocator;
    struct aws_byte_cursor out_path;
    AWS_ZERO_STRUCT(out_path);
    int err = aws_http_message_get_request_path(message, &out_path);
    /* If there is no path of the message, it should be a program error. */
    AWS_ASSERT(!err);
    err = aws_byte_buf_init_copy_from_cursor(&metrics->req_resp_info_metrics.request_path_query, allocator, out_path);
    AWS_ASSERT(!err);
    aws_ref_count_init(&metrics->ref_count, metrics, s_s3_request_metrics_destroy);

    return metrics;
}
struct aws_s3_request_metrics *aws_s3_request_metrics_acquire(struct aws_s3_request_metrics *metrics) {
    if (!metrics) {
        return NULL;
    }

    aws_ref_count_acquire(&metrics->ref_count);
    return metrics;
}
struct aws_s3_request_metrics *aws_s3_request_metrics_release(struct aws_s3_request_metrics *metrics) {
    if (metrics != NULL) {
        aws_ref_count_release(&metrics->ref_count);
    }
    return NULL;
}
