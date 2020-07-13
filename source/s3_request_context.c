/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/s3_request_context.h"
#include "aws/s3/private/s3_client_impl.h"
#include "aws/s3/s3_request.h"
#include "aws/s3/s3_request_result.h"

#include <aws/auth/signable.h>
#include <aws/common/allocator.h>
#include <aws/http/connection.h>
#include <aws/http/connection_manager.h>
#include <aws/http/request_response.h>

struct aws_s3_request_context_private {
    struct aws_s3_request_context context;

    struct aws_s3_client *client;
    struct aws_s3_request *request;
    struct aws_s3_request_result *result;
};

struct aws_s3_request_context *aws_s3_request_context_new(
    struct aws_allocator *allocator,
    struct aws_s3_client *client,
    struct aws_s3_request *request,
    struct aws_s3_request_result *result) {

    struct aws_s3_request_context_private *context_private =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_request_context_private));

    if (context_private == NULL) {
        return NULL;
    }

    struct aws_s3_request_context *context = &context_private->context;
    context->impl = context_private;
    context->allocator = allocator;

    aws_s3_client_acquire(client);

    context_private->client = client;

    aws_s3_request_acquire(request);

    context_private->request = request;

    aws_s3_request_result_acquire(result);

    context_private->result = result;
    return context;
}

void aws_s3_request_context_destroy(struct aws_s3_request_context *context) {

    struct aws_s3_request_context_private *context_private = (struct aws_s3_request_context_private *)context->impl;

    if (context->stream != NULL) {

        struct aws_http_connection *connection = aws_http_stream_get_connection(context->stream);

        if (connection != NULL) {
            aws_http_connection_manager_release_connection(context_private->client->connection_manager, connection);
            connection = NULL;
        }

        aws_http_stream_release(context->stream);
        context->stream = NULL;
    }

    if (context->signable != NULL) {
        aws_signable_destroy(context->signable);
        context->signable = NULL;
    }

    if (context->message != NULL) {
        aws_http_message_release(context->message);
        context->message = NULL;
    }

    if (context_private->result != NULL) {
        aws_s3_request_result_release(context_private->result);
        context_private->result = NULL;
    }

    if (context_private->request != NULL) {
        aws_s3_request_release(context_private->request);
        context_private->request = NULL;
    }

    if (context_private->client != NULL) {
        aws_s3_client_release(context_private->client);
        context_private->client = NULL;
    }

    aws_mem_release(context->allocator, context);
}

struct aws_s3_client *aws_s3_request_context_get_client(struct aws_s3_request_context *context) {
    struct aws_s3_request_context_private *context_private = (struct aws_s3_request_context_private *)context->impl;
    return context_private->client;
}

struct aws_s3_request *aws_s3_request_context_get_request(struct aws_s3_request_context *context) {
    struct aws_s3_request_context_private *context_private = (struct aws_s3_request_context_private *)context->impl;
    return context_private->request;
}

struct aws_s3_request_result *aws_s3_request_context_get_request_result(struct aws_s3_request_context *context) {
    struct aws_s3_request_context_private *context_private = (struct aws_s3_request_context_private *)context->impl;
    return context_private->result;
}
