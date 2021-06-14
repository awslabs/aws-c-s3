/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_client_impl.h"
#include "aws/s3/private/s3_meta_request_impl.h"
#include "aws/s3/private/s3_util.h"
#include "s3_tester.h"
#include <aws/common/byte_buf.h>
#include <aws/common/clock.h>
#include <aws/common/common.h>
#include <aws/common/environment.h>
#include <aws/common/ref_count.h>
#include <aws/http/request_response.h>
#include <aws/io/channel_bootstrap.h>
#include <aws/io/host_resolver.h>
#include <aws/io/stream.h>
#include <aws/io/tls_channel_handler.h>
#include <aws/testing/aws_test_harness.h>
#include <inttypes.h>

AWS_TEST_CASE(test_s3_client_set_vip_connection_warm, s_test_s3_client_set_vip_connection_warm)
static int s_test_s3_client_set_vip_connection_warm(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    return 0;
}

AWS_TEST_CASE(test_s3_client_set_vip_connection_active, s_test_s3_client_set_vip_connection_active)
static int s_test_s3_client_set_vip_connection_active(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    return 0;
}

AWS_TEST_CASE(test_s3_vip_create_destroy, s_test_s3_vip_create_destroy)
static int s_test_s3_vip_create_destroy(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    return 0;
}

AWS_TEST_CASE(test_s3_client_add_remove_vips, s_test_s3_client_add_remove_vips)
static int s_test_s3_client_add_remove_vips(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    return 0;
}

AWS_TEST_CASE(test_s3_client_resolve_vips, s_test_s3_client_resolve_vips)
static int s_test_s3_client_resolve_vips(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    return 0;
}
