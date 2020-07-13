/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "s3_get_object_result.h"
#include "s3_get_object_request.h"
#include <aws/common/string.h>

static void s_s3_request_result_get_object_destroy(struct aws_s3_request_result *request_result);

struct aws_s3_request_result *aws_s3_request_result_get_object_new(struct aws_allocator *allocator) {
    static struct aws_s3_request_result_vtable vtable = {.destroy = s_s3_request_result_get_object_destroy};

    struct aws_s3_request_result_get_object *request_result_get_object =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_request_result_get_object));

    if (request_result_get_object == NULL) {
        return NULL;
    }

    if (aws_s3_request_result_init(
            &request_result_get_object->request_result, allocator, &vtable, request_result_get_object)) {
        aws_mem_release(allocator, request_result_get_object);
        request_result_get_object = NULL;
        return NULL;
    }

    return &request_result_get_object->request_result;
}

static void s_s3_request_result_get_object_destroy(struct aws_s3_request_result *request_result) {

    struct aws_s3_request_result_get_object *request_result_get_object =
        (struct aws_s3_request_result_get_object *)request_result->impl;
    struct aws_s3_request_result_get_object_output *output = &request_result_get_object->output;

    if (output->content_type != NULL) {
        aws_string_destroy(output->content_type);
        output->content_type = NULL;
    }

    aws_mem_release(request_result->allocator, request_result->impl);
    request_result = NULL;
}

const struct aws_s3_request_result_get_object_output *aws_s3_request_result_get_object_get_output(
    const struct aws_s3_request_result *request_result) {
    struct aws_s3_request_result_get_object *request_result_get_object =
        (struct aws_s3_request_result_get_object *)request_result->impl;
    return &request_result_get_object->output;
}
