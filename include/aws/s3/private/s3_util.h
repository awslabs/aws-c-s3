#ifndef AWS_S3_UTIL_H
#define AWS_S3_UTIL_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

/* This file provides access to useful constants and simple utility functions. */

#include <aws/auth/signing_config.h>
#include <aws/common/byte_buf.h>

#define ASSERT_SYNCED_DATA_LOCK_HELD(object) AWS_ASSERT(aws_mutex_try_lock(&(object)->synced_data.lock) == AWS_OP_ERR)
#define KB_TO_BYTES(kb) ((kb)*1024)

struct aws_allocator;
struct aws_http_stream;
struct aws_http_headers;
struct aws_event_loop;

enum aws_s3_response_status {
    AWS_S3_RESPONSE_STATUS_SUCCESS = 200,
    AWS_S3_RESPONSE_STATUS_RANGE_SUCCESS = 206,
    AWS_S3_RESPONSE_STATUS_INTERNAL_ERROR = 500,
    AWS_S3_RESPONSE_STATUS_SLOW_DOWN = 503,
};

extern const struct aws_byte_cursor g_host_header_name;
extern const struct aws_byte_cursor g_range_header_name;
extern const struct aws_byte_cursor g_content_range_header_name;
extern const struct aws_byte_cursor g_content_type_header_name;
extern const struct aws_byte_cursor g_content_length_header_name;
extern const struct aws_byte_cursor g_accept_ranges_header_name;
extern const struct aws_byte_cursor g_etag_header_name;
extern const struct aws_byte_cursor g_post_method;

struct aws_cached_signing_config_aws {
    struct aws_allocator *allocator;
    struct aws_string *service;
    struct aws_string *region;
    struct aws_string *signed_body_value;

    struct aws_signing_config_aws config;
};

struct aws_cached_signing_config_aws *aws_cached_signing_config_new(
    struct aws_allocator *allocator,
    const struct aws_signing_config_aws *signing_config);

void aws_cached_signing_config_destroy(struct aws_cached_signing_config_aws *cached_signing_config);

/* Sets all headers specified for src on dest */
void copy_http_headers(const struct aws_http_headers *src, struct aws_http_headers *dest);

/* Get a top-level (exists directly under the root tag) tag value. */
struct aws_string *get_top_level_xml_tag_value(
    struct aws_allocator *allocator,
    const struct aws_byte_cursor *tag_name,
    struct aws_byte_cursor *xml_body);

#endif /* AWS_S3_UTIL_H */
