/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/s3/s3.h>
#include <aws/s3/s3_platform_info.h>

#include <aws/testing/aws_test_harness.h>

static int s_test_get_ec2_instance_type_sanity(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_s3_library_init(allocator);

    struct aws_s3_compute_platform_info_loader *loader = aws_s3_compute_platform_info_loader_new(allocator);

    struct aws_byte_cursor platform_info = aws_s3_get_ec2_instance_type(loader);

    aws_s3_compute_platform_info_loader_release(loader);
    aws_s3_library_clean_up();
    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(test_get_ec2_instance_type_sanity, s_test_get_ec2_instance_type_sanity)
