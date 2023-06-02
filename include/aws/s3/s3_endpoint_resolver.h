#ifndef AWS_S3_ENDPOINT_RESOLVER_H
#define AWS_S3_ENDPOINT_RESOLVER_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/s3/s3.h>
#include <aws/s3/s3_endpoint_ruleset/aws_s3_endpoint_resolver_partition.h>
#include <aws/s3/s3_endpoint_ruleset/aws_s3_endpoint_rule_set.h>

AWS_PUSH_SANE_WARNING_LEVEL
struct aws_allocator;
struct aws_s3_endpoint_resolver;
struct aws_endpoints_request_context;

AWS_EXTERN_C_BEGIN

AWS_S3_API
struct aws_s3_endpoint_resolver *aws_s3_endpoint_resolver_new(struct aws_allocator *allocator);
AWS_S3_API
struct aws_s3_endpoint_resolver *aws_s3_endpoint_resolver_acquire(struct aws_s3_endpoint_resolver *endpoint_resolver);
AWS_S3_API
struct aws_s3_endpoint_resolver *aws_s3_endpoint_resolver_release(struct aws_s3_endpoint_resolver *endpoint_resolver);

AWS_S3_API
struct aws_endpoints_resolved_endpoint *aws_s3_endpoint_resolver_resolve_endpoint(
    struct aws_s3_endpoint_resolver *endpoint_resolver,
    struct aws_endpoints_request_context *request_context);

AWS_EXTERN_C_END
AWS_POP_SANE_WARNING_LEVEL
#endif /* AWS_S3_ENDPOINT_RESOLVER_H */
