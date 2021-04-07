#ifndef AWS_S3_AUTO_RANGED_PUT_H
#define AWS_S3_AUTO_RANGED_PUT_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_meta_request_impl.h"

enum aws_s3_auto_ranged_put_request_tag {
    AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_CREATE_MULTIPART_UPLOAD,
    AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_PART,
    AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_ABORT_MULTIPART_UPLOAD,
    AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_COMPLETE_MULTIPART_UPLOAD
};

struct aws_s3_auto_ranged_put {
    struct aws_s3_meta_request base;

    /* Useable after the Create Multipart Upload request succeeds. */
    struct aws_string *upload_id;

    uint64_t content_length;

    /* Only meant for use in the update function, which is never called concurrently. */
    struct {
        uint32_t next_part_number;
    } threaded_update_data;

    /* Members to only be used when the mutex in the base type is locked. */
    struct {
        struct aws_array_list etag_list;

        uint32_t total_num_parts;
        uint32_t num_parts_sent;
        uint32_t num_parts_completed;
        uint32_t num_parts_successful;
        uint32_t num_parts_failed;

        struct aws_http_headers *needed_response_headers;

        int create_multipart_upload_error_code;
        int complete_multipart_upload_error_code;
        int abort_multipart_upload_error_code;

        uint32_t create_multipart_upload_sent : 1;
        uint32_t create_multipart_upload_completed : 1;
        uint32_t complete_multipart_upload_sent : 1;
        uint32_t complete_multipart_upload_completed : 1;
        uint32_t abort_multipart_upload_sent : 1;
        uint32_t abort_multipart_upload_completed : 1;

    } synced_data;
};

/* Creates a new auto-ranged put meta request.  This will do a multipart upload in parallel when appropriate. */
struct aws_s3_meta_request *aws_s3_meta_request_auto_ranged_put_new(
    struct aws_allocator *allocator,
    struct aws_s3_client *client,
    size_t part_size,
    uint64_t content_length,
    uint32_t num_parts,
    const struct aws_s3_meta_request_options *options);

#endif
