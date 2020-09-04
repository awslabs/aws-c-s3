#ifndef AWS_S3_REQUEST_MESSAGES_H
#define AWS_S3_REQUEST_MESSAGES_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <inttypes.h>

struct aws_allocator;
struct aws_http_message;

/* Create an HTTP request for an S3 Get Object Request, using the original request as a basis. If multipart is not
 * needed, part_number and part_size can be 0. */
struct aws_http_message *aws_s3_get_object_message_new(
    struct aws_allocator *allocator,
    struct aws_http_message *base_message,
    uint32_t part_number,
    uint64_t part_size);

#endif /* AWS_S3_REQUEST_H */
