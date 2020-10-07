#ifndef AWS_S3_UTIL_H
#define AWS_S3_UTIL_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

/* This file provides access to useful constants and simple utility functions. */

#include <aws/common/byte_buf.h>

#define ASSERT_SYNCED_DATA_LOCK_HELD(object) AWS_ASSERT(aws_mutex_try_lock(&(object)->synced_data.lock) == AWS_OP_ERR)

struct aws_allocator;
struct aws_http_stream;
struct aws_event_loop;

enum aws_s3_response_status {
    AWS_S3_RESPONSE_STATUS_SUCCESS = 200,
    AWS_S3_RESPONSE_STATUS_RANGE_SUCCESS = 206,
    AWS_S3_RESPONSE_STATUS_INTERNAL_ERROR = 500
};

extern const struct aws_byte_cursor g_host_header_name;
extern const struct aws_byte_cursor g_range_header_name;
extern const struct aws_byte_cursor g_content_length_header_name_name;
extern const struct aws_byte_cursor g_content_range_header_name;
extern const struct aws_byte_cursor g_content_type_header_name;
extern const struct aws_byte_cursor g_content_length_header_name;
extern const struct aws_byte_cursor g_etag_header_name;
extern const struct aws_byte_cursor g_post_method;

#endif /* AWS_S3_UTIL_H */
