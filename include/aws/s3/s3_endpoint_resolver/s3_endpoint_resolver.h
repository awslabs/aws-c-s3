#ifndef AWS_S3_ENDPOINT_RESOLVER_H
#define AWS_S3_ENDPOINT_RESOLVER_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/s3/s3.h>

AWS_PUSH_SANE_WARNING_LEVEL
struct aws_allocator;
struct aws_s3_endpoint_resolver;
struct aws_endpoints_request_context;
extern const char aws_s3_endpoint_resolver_partitions[];
extern const char aws_s3_endpoint_rule_set[];

AWS_EXTERN_C_BEGIN

/**
 * Creates a new S3 endpoint resolver
 */
AWS_S3_API
struct aws_s3_endpoint_resolver *aws_s3_endpoint_resolver_new(struct aws_allocator *allocator);

/**
 * Increments the reference count on the aws_s3_endpoint_resolver, allowing the caller to take a reference to it.
 *
 * Returns the same aws_s3_endpoint_resolver passed in.
 */
AWS_S3_API
struct aws_s3_endpoint_resolver *aws_s3_endpoint_resolver_acquire(struct aws_s3_endpoint_resolver *endpoint_resolver);

/**
 * Decrements a aws_s3_endpoint_resolver's ref count.  When the ref count drops to zero, the resolver will be destroyed.
 * Returns NULL.
 */
AWS_S3_API
struct aws_s3_endpoint_resolver *aws_s3_endpoint_resolver_release(struct aws_s3_endpoint_resolver *endpoint_resolver);

/*
 * Resolve an s3 endpoint given s3 request context.
 * Resolved endpoint is ref counter and caller is responsible for releasing it.
 */
AWS_S3_API
struct aws_endpoints_resolved_endpoint *aws_s3_endpoint_resolver_resolve_endpoint(
    struct aws_s3_endpoint_resolver *endpoint_resolver,
    struct aws_endpoints_request_context *request_context);

AWS_EXTERN_C_END
AWS_POP_SANE_WARNING_LEVEL
#endif /* AWS_S3_ENDPOINT_RESOLVER_H */
