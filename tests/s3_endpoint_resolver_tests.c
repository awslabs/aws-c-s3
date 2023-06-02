/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */
#include <aws/s3/s3_endpoint_resolver.h>
#include <aws/testing/aws_test_harness.h>

AWS_TEST_CASE(test_s3_endpoint_resolver_create_destroy, s_test_s3_endpoint_resolver_create_destroy)
static int s_test_s3_endpoint_resolver_create_destroy(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    aws_s3_library_init(allocator);

    struct aws_s3_endpoint_resolver *resolver = aws_s3_endpoint_resolver_new(allocator);
    aws_s3_endpoint_resolver_release(resolver);

    aws_s3_library_clean_up();
    return 0;
}
