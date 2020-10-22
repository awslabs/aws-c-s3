#ifndef AWS_S3_CLIENT_H
#define AWS_S3_CLIENT_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/s3/s3.h>

struct aws_allocator;

struct aws_http_stream;
struct aws_http_message;
struct aws_tls_connection_options;

struct aws_s3_client;
struct aws_s3_request;
struct aws_s3_meta_request;

enum aws_s3_meta_request_type {
    AWS_S3_META_REQUEST_TYPE_DEFAULT,
    AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
    AWS_S3_META_REQUEST_TYPE_PUT_OBJECT
};

typedef int(aws_s3_meta_request_receive_body_callback_fn)(
    struct aws_s3_meta_request *meta_request,
    const struct aws_byte_cursor *body,
    uint64_t range_start,
    uint64_t range_end,
    void *user_data);

typedef void(aws_s3_meta_request_finish_fn)(struct aws_s3_meta_request *meta_request, int error_code, void *user_data);

typedef void(aws_s3_meta_request_shutdown_fn)(void *user_data);

typedef void(aws_s3_client_shutdown_complete_callback_fn)(void *user_data);

/* Options for a new client. */
struct aws_s3_client_config {

    /* Region that the S3 bucket lives in. */
    struct aws_byte_cursor region;

    /* Client bootstrap used for common staples such as event loop group, host resolver, etc.. s*/
    struct aws_client_bootstrap *client_bootstrap;

    /* Credentials provider used to sign requests. */
    struct aws_credentials_provider *credentials_provider;

    /* Size of parts the files will be downloaded or uploaded in. */
    uint64_t part_size;

    /* TLS Options to be used for each connection.  Specify NULL to not use TLS. */
    struct aws_tls_connection_options *tls_connection_options;

    /* Timeout value, in milliseconds, used for each connection. */
    uint32_t connection_timeout_ms;

    /* Throughput target in Gbps that we are trying to reach. */
    double throughput_target_gbps;

    /* Amount of throughput in Gbps to designate to each VIP. */
    double throughput_per_vip_gbps;

    /* The number of connections that each VIP will have. */
    uint32_t num_connections_per_vip;

    /* Callback and associated user data for when the client has completed its shutdown process. */
    aws_s3_client_shutdown_complete_callback_fn *shutdown_callback;
    void *shutdown_callback_user_data;
};

/* Options for a new meta request, ie, file transfer that will be handled by the high performance client. */
struct aws_s3_meta_request_options {

    /* The type of meta request we will be trying to accelerate. */
    enum aws_s3_meta_request_type type;

    /* Initial HTTP message that defines what operation we are doing. */
    struct aws_http_message *message;

    /* User data for all callbacks. */
    void *user_data;

    /* Callback for incoming body data. */
    aws_s3_meta_request_receive_body_callback_fn *body_callback;

    /* Callback for when the meta request is completely finished. */
    aws_s3_meta_request_finish_fn *finish_callback;

    /* Callback for when the meta request has completely cleaned up. */
    aws_s3_meta_request_shutdown_fn *shutdown_callback;
};

AWS_EXTERN_C_BEGIN

AWS_S3_API
struct aws_s3_client *aws_s3_client_new(
    struct aws_allocator *allocator,
    const struct aws_s3_client_config *client_config);

AWS_S3_API
void aws_s3_client_acquire(struct aws_s3_client *client);

AWS_S3_API
void aws_s3_client_release(struct aws_s3_client *client);

AWS_S3_API
struct aws_s3_meta_request *aws_s3_client_make_meta_request(
    struct aws_s3_client *client,
    const struct aws_s3_meta_request_options *options);

AWS_S3_API
void aws_s3_meta_request_acquire(struct aws_s3_meta_request *meta_request);

AWS_S3_API
void aws_s3_meta_request_release(struct aws_s3_meta_request *meta_request);

AWS_EXTERN_C_END

#endif /* AWS_S3_CLIENT_H */
