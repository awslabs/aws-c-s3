#ifndef AWS_S3_UTIL_H
#define AWS_S3_UTIL_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

/* This file provides access to useful constants and simple utility functions. */

#include <aws/common/byte_buf.h>

#if defined(DEBUG_BUILD)
#    define ASSERT_SYNCED_DATA_LOCK_HELD(object)                                                                       \
        AWS_FATAL_ASSERT(aws_mutex_try_lock(&(object)->synced_data.lock) == AWS_OP_ERR);
#else
#    define ASSERT_SYNCED_DATA_LOCK_HELD(object)
#endif

struct aws_http_stream;

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

bool aws_s3_is_response_status_success(int response_status);

int aws_s3_is_stream_response_status_success(struct aws_http_stream *stream, bool *out_is_success);

#endif /* AWS_S3_UTIL_H */
