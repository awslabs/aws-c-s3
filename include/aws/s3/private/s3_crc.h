#ifndef AWS_S3_CRC_H
#define AWS_S3_CRC_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/s3/s3.h>

#define AWS_CRC_LEN 4

AWS_EXTERN_C_BEGIN

AWS_S3_API
struct aws_hash *aws_hash_crc32_new(struct aws_allocator *allocator);

AWS_S3_API
struct aws_hash *aws_hash_crc32c_new(struct aws_allocator *allocator);

AWS_EXTERN_C_END

#endif /* AWS_S3_CRC_H */
