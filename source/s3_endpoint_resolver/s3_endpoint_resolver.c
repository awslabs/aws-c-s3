/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/s3/s3_endpoint_resolver.h>
#include <aws/sdkutils/endpoints_rule_engine.h>
#include <aws/sdkutils/partitions.h>

struct aws_endpoints_rule_engine *aws_s3_endpoint_resolver_new(struct aws_allocator *allocator) {
    struct aws_endpoints_ruleset *ruleset = NULL;
    struct aws_partitions_config *partitions = NULL;
    struct aws_endpoints_rule_engine *rule_engine = NULL;

    struct aws_byte_cursor ruleset_cursor = aws_byte_cursor_from_c_str(aws_s3_endpoint_rule_set);
    struct aws_byte_cursor partitions_cursor = aws_byte_cursor_from_c_str(aws_s3_endpoint_resolver_partitions);

    ruleset = aws_endpoints_ruleset_new_from_string(allocator, ruleset_cursor);
    if (!ruleset) {
        goto cleanup;
    }

    partitions = aws_partitions_config_new_from_string(allocator, partitions_cursor);
    if (!partitions) {
        goto cleanup;
    }

    rule_engine = aws_endpoints_rule_engine_new(allocator, ruleset, partitions);

cleanup:
    aws_endpoints_ruleset_release(ruleset);
    aws_partitions_config_release(partitions);
    return rule_engine;
}

struct aws_endpoints_resolved_endpoint *aws_s3_endpoint_resolver_resolve_endpoint(
    struct aws_endpoints_rule_engine *rule_engine,
    struct aws_endpoints_request_context *request_context) {

    struct aws_endpoints_resolved_endpoint *resolved_endpoint = NULL;
    aws_endpoints_rule_engine_resolve(rule_engine, request_context, &resolved_endpoint);

    return resolved_endpoint;
}
