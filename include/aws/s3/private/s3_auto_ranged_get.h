#ifndef AWS_S3_AUTO_RANGED_GET_H
#define AWS_S3_AUTO_RANGED_GET_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_meta_request_impl.h"

enum aws_s3_auto_ranged_get_request_type {
    AWS_S3_AUTO_RANGE_GET_REQUEST_TYPE_PART,
    AWS_S3_AUTO_RANGE_GET_REQUEST_TYPE_PART_WITHOUT_RANGE,
};

struct aws_s3_auto_ranged_get {
    struct aws_s3_meta_request base;

    /* Members to only be used when the mutex in the base type is locked. */
    struct {
        uint32_t total_num_parts;

        uint32_t num_parts_requested;
        uint32_t num_parts_completed;
        uint32_t num_parts_successful;
        uint32_t num_parts_failed;

        uint32_t get_without_range : 1;
        uint32_t get_without_range_sent : 1;
        uint32_t get_without_range_completed : 1;
    } synced_data;
};

/* Creates a new auto-ranged get meta request.  This will do multiple parallel ranged-gets when appropriate. */
struct aws_s3_meta_request *aws_s3_meta_request_auto_ranged_get_new(
    struct aws_allocator *allocator,
    struct aws_s3_client *client,
    size_t part_size,
    const struct aws_s3_meta_request_options *options);

#endif
