#ifndef AWS_S3_FILE_SYSTEM_SUPPORT_H
#define AWS_S3_FILE_SYSTEM_SUPPORT_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/s3/s3_client.h>

#include <aws/common/date_time.h>
#include <aws/common/string.h>

struct aws_s3_object_file_system_info {
    struct aws_byte_cursor prefix;
    struct aws_byte_cursor key;
    uint64_t size;
    struct aws_date_time last_modified;
    struct aws_byte_cursor e_tag;
};

struct aws_s3_paginator;

typedef bool(aws_s3_on_object_fn)(const struct aws_s3_object_file_system_info *info, void *user_data);
typedef void(aws_s3_on_object_list_finished)(const struct aws_s3_paginator *paginator, int error_code, void *user_data);

struct aws_s3_list_bucket_v2_params {
    struct aws_s3_client *client;
    struct aws_byte_cursor bucket_name;
    struct aws_byte_cursor prefix;
    struct aws_byte_cursor delimiter;
    struct aws_byte_cursor continuation_token;
    struct aws_byte_cursor endpoint;
    aws_s3_on_object_fn *on_object;
    aws_s3_on_object_list_finished *on_list_finished;
    void *user_data;
};

AWS_EXTERN_C_BEGIN

AWS_S3_API struct aws_s3_paginator *aws_s3_initiate_list_bucket(
    struct aws_allocator *allocator,
    const struct aws_s3_list_bucket_v2_params *params);
AWS_S3_API void aws_s3_paginator_acquire(struct aws_s3_paginator *paginator);
AWS_S3_API void aws_s3_paginator_release(struct aws_s3_paginator *paginator);
AWS_S3_API int aws_s3_paginator_continue(
    struct aws_s3_paginator *paginator,
    struct aws_signing_config_aws *signing_config);
AWS_S3_API int aws_s3_list_bucket_and_run_all_pages(
    struct aws_allocator *allocator,
    const struct aws_s3_list_bucket_v2_params *params,
    struct aws_signing_config_aws *signing_config);

AWS_EXTERN_C_END

#endif /* AWS_S3_FILE_SYSTEM_SUPPORT_H */
