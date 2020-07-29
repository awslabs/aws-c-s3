/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_client_impl.h"
#include "aws/s3/private/s3_get_object_request.h"
#include "aws/s3/private/s3_meta_request_impl.h"
#include "aws/s3/private/s3_put_object_request.h"

#include <aws/http/request_response.h>

static int s_s3_client_accel_incoming_body(
    struct aws_s3_request *request,
    struct aws_http_stream *stream,
    const struct aws_byte_cursor *body,
    void *user_data);

static void s_s3_client_accel_object_finished(struct aws_s3_request *request, int error_code, void *user_data);

struct aws_s3_meta_request *aws_s3_client_make_meta_request(
    struct aws_s3_client *client,
    const struct aws_s3_meta_request_options *options) {

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

    /* Spin up a new meta request for this acceleration */
    struct aws_s3_meta_request *meta_request = aws_s3_meta_request_new(client->allocator, client, options);

    if (meta_request == NULL) {
        goto meta_request_create_failed;
    }

    /* Grab an additional reference for the request_options below. */
    aws_s3_meta_request_acquire(meta_request);

    struct aws_s3_request_options request_options;
    AWS_ZERO_STRUCT(request_options);
    request_options.client = client;
    request_options.message = options->message;
    request_options.user_data = meta_request;
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

    /* Go ahead and just make the request for now.  Later, we'll be doing some actual acceleration here. */
    if (s3_client_make_request(client, request)) {
        AWS_LOGF_ERROR(AWS_LS_S3_CLIENT, "id=%p Could not issue S3 Request", (void *)client);
        goto make_request_failed;
    }

    return meta_request;

make_request_failed:

    if (request != NULL) {
        aws_s3_request_release(request);
        request = NULL;
    }

request_create_failed:

    if (meta_request != NULL) {
        /* Remove the reference added for the aws_s3_request. */
        aws_s3_meta_request_release(meta_request);

        /* Remove the initial reference added to completel clean up the aws_s3_meta_request. */
        aws_s3_meta_request_release(meta_request);
        meta_request = NULL;
    }

meta_request_create_failed:

    return NULL;
}

int s_s3_client_accel_incoming_body(
    struct aws_s3_request *request,
    struct aws_http_stream *stream,
    const struct aws_byte_cursor *body,
    void *user_data) {

    AWS_PRECONDITION(request);
    AWS_PRECONDITION(user_data);

    (void)request;

    struct aws_s3_meta_request *meta_request = user_data;

    /* Use the callback passed into the acceleration request if there is one.  (This callback will likely live in the
     * language bindings one day.) */
    if (meta_request->body_callback != NULL) {
        return meta_request->body_callback(meta_request, stream, body, meta_request->user_data);
    }

    return AWS_OP_SUCCESS;
}

void s_s3_client_accel_object_finished(struct aws_s3_request *request, int error_code, void *user_data) {

    AWS_PRECONDITION(request);
    AWS_PRECONDITION(user_data);

    (void)request;

    struct aws_s3_meta_request *meta_request = user_data;

    /* Use the finish callback passed into the acceleration request if there is one.  (This callback will likely live in
     * the language bindings one day.) */
    if (meta_request->finish_callback != NULL) {
        meta_request->finish_callback(meta_request, error_code, meta_request->user_data);
    }

    aws_s3_request_release(request);
    aws_s3_meta_request_release(meta_request);
}
