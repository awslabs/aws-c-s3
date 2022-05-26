#ifndef AWS_S3_PAGINATOR_H
#define AWS_S3_PAGINATOR_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/auth/signing_config.h>
#include <aws/s3/exports.h>
#include <aws/s3/s3_client.h>

#include <aws/common/common.h>
#include <aws/common/xml_parser.h>

struct aws_s3_paginator;

struct aws_s3_paginated_operation;

typedef int(aws_s3_next_http_message_fn)(
    struct aws_byte_cursor *continuation_token,
    void *user_data,
    struct aws_http_message **out_message);

typedef bool(
    aws_s3_on_result_node_encountered_fn)(struct aws_xml_parser *parser, struct aws_xml_node *node, void *user_data);

typedef void(aws_s3_on_page_finished_fn)(struct aws_s3_paginator *paginator, int error_code, void *user_data);

typedef void(aws_s3_on_paginated_operation_cleanup_fn)(void *user_data);

/**
 * Parameters for calling aws_s3_initiate_list_objects(). All values are copied out or re-seated and reference counted.
 */
struct aws_s3_paginator_params {
    /**
     * Must not be NULL. The internal call will increment the reference count on client.
     */
    struct aws_s3_client *client;

    struct aws_s3_paginated_operation *operation;
    /**
     * Optional. The continuation token for fetching the next page. You likely shouldn't set this
     * unless you have a special use case.
     */
    struct aws_byte_cursor continuation_token;

    /**
     * Must not be empty. Name of the bucket to list.
     */
    struct aws_byte_cursor bucket_name;
    /**
     * Must not be empty. Key with which multipart upload was initiated.
     */
    struct aws_byte_cursor endpoint;
    /**
     * Callback to invoke on each part that's listed.
     */

    aws_s3_on_page_finished_fn *on_page_finished_fn;
};

struct aws_s3_paginated_operation_params {

    const struct aws_byte_cursor *result_xml_node_name;

    const struct aws_byte_cursor *continuation_token_node_name;

    aws_s3_next_http_message_fn *next_message;
    aws_s3_on_result_node_encountered_fn *on_result_node_encountered_fn;

    aws_s3_on_paginated_operation_cleanup_fn *on_paginated_operation_cleanup;

    void *user_data;
};

AWS_EXTERN_C_BEGIN

AWS_S3_API struct aws_s3_paginator *aws_s3_initiate_paginator(
    struct aws_allocator *allocator,
    const struct aws_s3_paginator_params *params);

AWS_S3_API void aws_s3_paginator_acquire(struct aws_s3_paginator *paginator);
AWS_S3_API void aws_s3_paginator_release(struct aws_s3_paginator *paginator);

AWS_S3_API struct aws_s3_paginated_operation *aws_s3_paginated_operation_new(
    struct aws_allocator *allocator,
    const struct aws_s3_paginated_operation_params *params);

AWS_S3_API void aws_s3_paginated_operation_acquire(struct aws_s3_paginated_operation *operation);
AWS_S3_API void aws_s3_paginated_operation_release(struct aws_s3_paginated_operation *operation);

/**
 * Start the paginated operation. If there are more results to fetch, it will begin that work.
 *
 * Signing_config contains information for SigV4 signing for the operation. It must not be NULL. It will be copied.
 *
 * Returns AWS_OP_SUCCESS on successful start of the operation, and AWS_OP_ERR otherwise. Check aws_last_error() for
 * more information on the error that occurred.
 */
AWS_S3_API int aws_s3_paginator_continue(
    struct aws_s3_paginator *paginator,
    const struct aws_signing_config_aws *signing_config);

/**
 * If the paginator has more results to fetch, returns true.
 */
AWS_S3_API bool aws_s3_paginator_has_more_results(const struct aws_s3_paginator *paginator);

AWS_S3_API int aws_s3_construct_next_request_http_message(
    struct aws_s3_paginated_operation *operation,
    struct aws_byte_cursor *continuation_token,
    struct aws_http_message **out_message);

AWS_S3_API int aws_s3_paginated_operation_on_response(
    struct aws_s3_paginated_operation *operation,
    struct aws_byte_cursor *response_body,
    struct aws_string **continuation_token_out,
    bool *has_more_results_out);

AWS_EXTERN_C_END

#endif /* AWS_S3_PAGINATOR_H */
