/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_accel_context_impl.h"
#include "aws/s3/private/s3_client_impl.h"
#include "aws/s3/private/s3_get_object_request.h"
#include "aws/s3/private/s3_put_object_request.h"

#include <aws/http/request_response.h>

static int s_s3_client_accel_incoming_body(
    struct aws_s3_request *request,
    struct aws_http_stream *stream,
    const struct aws_byte_cursor *body,
    void *user_data);

static void s_s3_client_accel_object_finished(struct aws_s3_request *request, int error_code, void *user_data);

struct aws_s3_accel_context *aws_s3_client_accel_request(
    struct aws_s3_client *client,
    const struct aws_s3_accel_request_options *options) {

    AWS_PRECONDITION(client);
    AWS_PRECONDITION(options);

    if (options->message == NULL) {
        AWS_LOGF_ERROR(AWS_LS_S3_CLIENT, "id=%p Cannot accelerate request; options are invalid.", (void *)client);
        aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        return NULL;
    }

    struct aws_byte_cursor http_method_str;
    AWS_ZERO_STRUCT(http_method_str);

    if (aws_http_message_get_request_method(options->message, &http_method_str)) {
        AWS_LOGF_ERROR(AWS_LS_S3_CLIENT, "id=%p Could not get request method from http message", (void *)client);
        return NULL;
    }

    struct aws_s3_accel_context *context = aws_s3_accel_context_new(client->allocator, client, options);

    if (context == NULL) {
        goto context_create_failed;
    }

    aws_s3_accel_context_acquire(context);

    struct aws_s3_request_options request_options;
    AWS_ZERO_STRUCT(request_options);
    request_options.client = client;
    request_options.message = options->message;
    request_options.user_data = context;
    request_options.body_callback = s_s3_client_accel_incoming_body;
    request_options.finish_callback = s_s3_client_accel_object_finished;

    struct aws_s3_request *request = NULL;

    if (aws_byte_cursor_eq_c_str(&http_method_str, "GET")) {
        request = aws_s3_get_object_request_new(client->allocator, &request_options);
    } else if (aws_byte_cursor_eq_c_str(&http_method_str, "PUT")) {
        request = aws_s3_put_object_request_new(client->allocator, &request_options);
    }

    if (request == NULL) {
        goto request_create_failed;
    }

    if (s3_client_make_request(client, request)) {
        AWS_LOGF_ERROR(AWS_LS_S3_CLIENT, "id=%p Could not issue S3 Request", (void *)client);
        goto make_request_failed;
    }

    return context;

make_request_failed:

    if (request != NULL) {
        aws_s3_request_release(request);
        request = NULL;
    }

request_create_failed:

    if (context != NULL) {
        aws_s3_accel_context_release(context);
        context = NULL;
    }

context_create_failed:

    return NULL;
}

int s_s3_client_accel_incoming_body(
    struct aws_s3_request *request,
    struct aws_http_stream *stream,
    const struct aws_byte_cursor *body,
    void *user_data) {

    AWS_PRECONDITION(request);
    AWS_PRECONDITION(user_data);

    struct aws_s3_accel_context *context = user_data;

    if (context->body_callback != NULL) {
        return context->body_callback(context, stream, body, context->user_data);
    }

    return AWS_OP_SUCCESS;
}

void s_s3_client_accel_object_finished(struct aws_s3_request *request, int error_code, void *user_data) {

    AWS_PRECONDITION(request);
    AWS_PRECONDITION(user_data);

    struct aws_s3_accel_context *context = user_data;

    if (context->finish_callback != NULL) {
        context->finish_callback(context, error_code, context->user_data);
    }

    aws_s3_request_release(request);
    aws_s3_accel_context_release(context);
}
