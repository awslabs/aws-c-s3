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

typedef void(aws_s3_task_util_task_fn)(void **args);

/* Wrapper for tasks that allocates a payload of task/arguments and handles clean up of that payload. */
int aws_s3_task_util_new_task(
    struct aws_allocator *allocator,
    struct aws_event_loop *event_loop,
    aws_s3_task_util_task_fn *task_fn,
    uint64_t delay_ns,
    uint32_t num_args,
    ...);

#endif /* AWS_S3_UTIL_H */
