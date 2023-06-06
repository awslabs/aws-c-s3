/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */
#include <aws/sdkutils/endpoints_rule_engine.h>
#include <aws/testing/aws_test_harness.h>

#ifdef AWS_ENABLE_S3_ENDPOINT_RESOLVER
#    include <aws/s3/s3_endpoint_resolver.h>
#    include <aws/sdkutils/endpoints_rule_engine.h>

AWS_TEST_CASE(test_s3_endpoint_resolver_resolve_endpoint, s_test_s3_endpoint_resolver_resolve_endpoint)
static int s_test_s3_endpoint_resolver_resolve_endpoint(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    aws_s3_library_init(allocator);

    struct aws_endpoints_rule_engine *rule_engine = aws_s3_endpoint_resolver_new(allocator);
    ASSERT_NOT_NULL(rule_engine);
    struct aws_endpoints_request_context *context = aws_endpoints_request_context_new(allocator);
    ASSERT_NOT_NULL(context);
    ASSERT_SUCCESS(aws_endpoints_request_context_add_string(
        allocator, context, aws_byte_cursor_from_c_str("Region"), aws_byte_cursor_from_c_str("us-west-2")));
    ASSERT_SUCCESS(aws_endpoints_request_context_add_string(
        allocator, context, aws_byte_cursor_from_c_str("Bucket"), aws_byte_cursor_from_c_str("s3-bucket-test")));

    struct aws_endpoints_resolved_endpoint *resolved_endpoint;
    ASSERT_SUCCESS(aws_endpoints_rule_engine_resolve(rule_engine, context, &resolved_endpoint));

    ASSERT_INT_EQUALS(AWS_ENDPOINTS_RESOLVED_ENDPOINT, aws_endpoints_resolved_endpoint_get_type(resolved_endpoint));

    struct aws_byte_cursor url_cur;
    ASSERT_SUCCESS(aws_endpoints_resolved_endpoint_get_url(resolved_endpoint, &url_cur));

    ASSERT_CURSOR_VALUE_CSTRING_EQUALS(url_cur, "https://s3-bucket-test.s3.us-west-2.amazonaws.com");

    aws_endpoints_resolved_endpoint_release(resolved_endpoint);
    aws_endpoints_request_context_release(context);
    aws_endpoints_rule_engine_release(rule_engine);

    aws_s3_library_clean_up();
    return 0;
}

AWS_TEST_CASE(test_s3_endpoint_resolver_resolve_endpoint_fips, s_test_s3_endpoint_resolver_resolve_endpoint_fips)
static int s_test_s3_endpoint_resolver_resolve_endpoint_fips(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    aws_s3_library_init(allocator);

    struct aws_endpoints_rule_engine *rule_engine = aws_s3_endpoint_resolver_new(allocator);
    ASSERT_NOT_NULL(rule_engine);
    struct aws_endpoints_request_context *context = aws_endpoints_request_context_new(allocator);
    ASSERT_NOT_NULL(context);
    ASSERT_SUCCESS(aws_endpoints_request_context_add_string(
        allocator, context, aws_byte_cursor_from_c_str("Region"), aws_byte_cursor_from_c_str("us-east-1")));
    ASSERT_SUCCESS(aws_endpoints_request_context_add_string(
        allocator, context, aws_byte_cursor_from_c_str("Bucket"), aws_byte_cursor_from_c_str("s3-bucket-test")));
    ASSERT_SUCCESS(
        aws_endpoints_request_context_add_boolean(allocator, context, aws_byte_cursor_from_c_str("UseFIPS"), true));
    struct aws_endpoints_resolved_endpoint *resolved_endpoint;
    ASSERT_SUCCESS(aws_endpoints_rule_engine_resolve(rule_engine, context, &resolved_endpoint));

    ASSERT_INT_EQUALS(AWS_ENDPOINTS_RESOLVED_ENDPOINT, aws_endpoints_resolved_endpoint_get_type(resolved_endpoint));

    struct aws_byte_cursor url_cur;
    ASSERT_SUCCESS(aws_endpoints_resolved_endpoint_get_url(resolved_endpoint, &url_cur));

    ASSERT_CURSOR_VALUE_CSTRING_EQUALS(url_cur, "https://s3-bucket-test.s3-fips.us-east-1.amazonaws.com");

    aws_endpoints_resolved_endpoint_release(resolved_endpoint);
    aws_endpoints_request_context_release(context);
    aws_endpoints_rule_engine_release(rule_engine);

    aws_s3_library_clean_up();
    return 0;
}

AWS_TEST_CASE(
    test_s3_endpoint_resolver_resolve_endpoint_force_path_style,
    s_test_s3_endpoint_resolver_resolve_endpoint_force_path_style)
static int s_test_s3_endpoint_resolver_resolve_endpoint_force_path_style(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    aws_s3_library_init(allocator);

    struct aws_endpoints_rule_engine *rule_engine = aws_s3_endpoint_resolver_new(allocator);
    ASSERT_NOT_NULL(rule_engine);
    struct aws_endpoints_request_context *context = aws_endpoints_request_context_new(allocator);
    ASSERT_NOT_NULL(context);
    ASSERT_SUCCESS(aws_endpoints_request_context_add_string(
        allocator, context, aws_byte_cursor_from_c_str("Region"), aws_byte_cursor_from_c_str("us-east-1")));
    ASSERT_SUCCESS(aws_endpoints_request_context_add_string(
        allocator, context, aws_byte_cursor_from_c_str("Bucket"), aws_byte_cursor_from_c_str("s3-bucket-test")));
    ASSERT_SUCCESS(aws_endpoints_request_context_add_boolean(
        allocator, context, aws_byte_cursor_from_c_str("ForcePathStyle"), true));
    struct aws_endpoints_resolved_endpoint *resolved_endpoint;
    ASSERT_SUCCESS(aws_endpoints_rule_engine_resolve(rule_engine, context, &resolved_endpoint));

    ASSERT_INT_EQUALS(AWS_ENDPOINTS_RESOLVED_ENDPOINT, aws_endpoints_resolved_endpoint_get_type(resolved_endpoint));

    struct aws_byte_cursor url_cur;
    ASSERT_SUCCESS(aws_endpoints_resolved_endpoint_get_url(resolved_endpoint, &url_cur));

    ASSERT_CURSOR_VALUE_CSTRING_EQUALS(url_cur, "https://s3.us-east-1.amazonaws.com/s3-bucket-test");

    aws_endpoints_resolved_endpoint_release(resolved_endpoint);
    aws_endpoints_request_context_release(context);
    aws_endpoints_rule_engine_release(rule_engine);

    aws_s3_library_clean_up();
    return 0;
}

#endif
