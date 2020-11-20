#ifndef AWS_S3_H
#define AWS_S3_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/common.h>
#include <aws/io/logging.h>
#include <aws/s3/exports.h>

#define AWS_C_S3_PACKAGE_ID 9

enum aws_s3_errors {
    AWS_ERROR_S3_MISSING_CONTENT_RANGE_HEADER = AWS_ERROR_ENUM_BEGIN_RANGE(AWS_C_S3_PACKAGE_ID),
    AWS_ERROR_S3_MISSING_ETAG,
    AWS_ERROR_S3_INTERNAL_ERROR,
    AWS_ERROR_S3_SLOW_DOWN,
    AWS_ERROR_S3_INVALID_RESPONSE_STATUS,
    AWS_ERROR_S3_MISSING_UPLOAD_ID,
    AWS_ERROR_S3_NO_PART_BUFFER,
    AWS_ERROR_S3_END_RANGE = AWS_ERROR_ENUM_END_RANGE(AWS_C_S3_PACKAGE_ID)
};

enum aws_s3_subject {
    AWS_LS_S3_GENERAL = AWS_LOG_SUBJECT_BEGIN_RANGE(AWS_C_S3_PACKAGE_ID),
    AWS_LS_S3_CLIENT,
    AWS_LS_S3_REQUEST,
    AWS_LS_S3_META_REQUEST,
    AWS_LS_S3_VIP,
    AWS_LS_S3_VIP_CONNECTION,
    AWS_LS_S3_LAST = AWS_LOG_SUBJECT_END_RANGE(AWS_C_S3_PACKAGE_ID)
};

AWS_EXTERN_C_BEGIN

/**
 * Initializes internal datastructures used by aws-c-s3.
 * Must be called before using any functionality in aws-c-s3.
 */
AWS_S3_API
void aws_s3_library_init(struct aws_allocator *allocator);

/**
 * Shuts down the internal datastructures used by aws-c-s3.
 */
AWS_S3_API
void aws_s3_library_clean_up(void);

AWS_EXTERN_C_END

#endif /* AWS_S3_H */
