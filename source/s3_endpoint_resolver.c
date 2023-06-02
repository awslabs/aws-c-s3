/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/ref_count.h>
#include <aws/s3/s3_endpoint_resolver.h>
#include <aws/sdkutils/endpoints_rule_engine.h>
#include <aws/sdkutils/partitions.h>

struct aws_s3_endpoint_resolver {
    struct aws_allocator *allocator;
    struct aws_ref_count ref_count;
    struct aws_endpoints_rule_engine *rule_engine;
};

static void s_aws_s3_endpoint_resolver_clean_up(struct aws_s3_endpoint_resolver *endpoint_resolver) {
    aws_endpoints_rule_engine_release(endpoint_resolver->rule_engine);
    aws_mem_release(endpoint_resolver->allocator, endpoint_resolver);
}

struct aws_s3_endpoint_resolver *aws_s3_endpoint_resolver_new(struct aws_allocator *allocator) {
    struct aws_s3_endpoint_resolver *resolver = aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_endpoint_resolver));

    resolver->allocator = allocator;
    aws_ref_count_init(
        &resolver->ref_count, resolver, (aws_simple_completion_callback *)s_aws_s3_endpoint_resolver_clean_up);

    struct aws_byte_cursor ruleset_cursor = aws_byte_cursor_from_c_str(aws_s3_endpoint_rule_set);
    struct aws_byte_cursor partitions_cursor = aws_byte_cursor_from_c_str(aws_s3_endpoint_resolver_partitions);
    struct aws_endpoints_ruleset *ruleset = aws_endpoints_ruleset_new_from_string(allocator, ruleset_cursor);
    struct aws_partitions_config *partitions = aws_partitions_config_new_from_string(allocator, partitions_cursor);

    resolver->rule_engine = aws_endpoints_rule_engine_new(allocator, ruleset, partitions);

    aws_endpoints_ruleset_release(ruleset);
    aws_partitions_config_release(partitions);

    return resolver;
}

struct aws_s3_endpoint_resolver *aws_s3_endpoint_resolver_acquire(struct aws_s3_endpoint_resolver *endpoint_resolver) {
    if (endpoint_resolver != NULL) {
        aws_ref_count_acquire(&endpoint_resolver->ref_count);
    }
    return endpoint_resolver;
}

struct aws_s3_endpoint_resolver *aws_s3_endpoint_resolver_release(struct aws_s3_endpoint_resolver *endpoint_resolver) {
    if (endpoint_resolver != NULL) {
        aws_ref_count_release(&endpoint_resolver->ref_count);
    }
    return endpoint_resolver;
}

struct aws_endpoints_resolved_endpoint *aws_s3_resolve_endpoint(
    struct aws_s3_endpoint_resolver *resolver,
    struct aws_endpoints_request_context *request_context) {

    struct aws_endpoints_resolved_endpoint *resolved_endpoint = NULL;
    aws_endpoints_rule_engine_resolve(resolver->rule_engine, request_context, &resolved_endpoint);

    return resolved_endpoint;
}
