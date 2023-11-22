#ifndef AWS_S3_DEFAULT_META_REQUEST_H
#define AWS_S3_DEFAULT_META_REQUEST_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_meta_request_impl.h"

struct aws_s3_client;

struct aws_s3_meta_request_default {
    struct aws_s3_meta_request base;

    size_t content_length;

    /* Actual type for the single request (AWS_S3_REQUEST_TYPE_DEFAULT if unknown) */
    enum aws_s3_request_type request_type;

    /* S3 operation name for the single request (NULL if unknown) */
    struct aws_string *operation_name;

    /* Members to only be used when the mutex in the base type is locked. */
    struct {
        int cached_response_status;
        int request_error_code;

        uint32_t request_sent : 1;
        uint32_t request_completed : 1;

    } synced_data;
};

/* Creates a new default meta request. This will send the request as is and pass back the response.
 *
 * Sometimes, a default meta-request is used for other aws_s3_meta_request_types.
 * For example, if the request is simple (e.g. single part upload or download),
 * or if there's a header that's too complex to deal with.
 * In these cases, request_type and operation_name_override should be set.
 *
 * But if the user literally asked for a AWS_S3_META_REQUEST_TYPE_DEFAULT,
 * the request_type should be AWS_S3_META_REQUEST_TYPE_DEFAULT and
 * operation_name_override should be NULL (options->operation_name will be used
 * if it is set).
 */
struct aws_s3_meta_request *aws_s3_meta_request_default_new(
    struct aws_allocator *allocator,
    struct aws_s3_client *client,
    enum aws_s3_request_type request_type,
    const char *operation_name_override,
    uint64_t content_length,
    bool should_compute_content_md5,
    const struct aws_s3_meta_request_options *options);

#endif
