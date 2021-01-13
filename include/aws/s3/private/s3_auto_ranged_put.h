#ifndef AWS_S3_AUTO_RANGED_PUT_H
#define AWS_S3_AUTO_RANGED_PUT_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_client_impl.h"
#include "aws/s3/private/s3_meta_request_impl.h"
#include "aws/s3/private/s3_util.h"

enum aws_s3_auto_ranged_put_state {
    AWS_S3_AUTO_RANGED_PUT_STATE_START,
    AWS_S3_AUTO_RANGED_PUT_STATE_WAITING_FOR_CREATE,
    AWS_S3_AUTO_RANGED_PUT_STATE_SENDING_PARTS,
    AWS_S3_AUTO_RANGED_PUT_STATE_WAITING_FOR_PARTS,
    AWS_S3_AUTO_RANGED_PUT_STATE_WAITING_FOR_CANCEL,
    AWS_S3_AUTO_RANGED_PUT_STATE_SEND_COMPLETE,
    AWS_S3_AUTO_RANGED_PUT_STATE_WAITING_FOR_COMPLETE,
    AWS_S3_AUTO_RANGED_PUT_STATE_WAITING_FOR_SINGLE_REQUEST
};

enum aws_s3_auto_ranged_put_request_tag {
    AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_CREATE_MULTIPART_UPLOAD,
    AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_PART,
    AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_ABORT_MULTIPART_UPLOAD,
    AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_COMPLETE_MULTIPART_UPLOAD
};

struct aws_s3_auto_ranged_put {
    struct aws_s3_meta_request base;

    struct {
        enum aws_s3_auto_ranged_put_state state;
        struct aws_array_list etag_list;

        uint32_t total_num_parts;
        uint32_t next_part_number;
        uint32_t num_parts_sent;
        uint32_t num_parts_completed;

        struct aws_string *upload_id;
        struct aws_http_headers *needed_response_headers;

        struct aws_s3_request *failed_request;
        int finish_status_code;
        int error_code;

    } synced_data;
};

#endif
