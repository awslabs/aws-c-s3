#ifndef AWS_S3_CLIENT_H
#define AWS_S3_CLIENT_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/io/retry_strategy.h>
#include <aws/s3/s3.h>

#include <aws/auth/signing_config.h>

struct aws_allocator;

struct aws_http_stream;
struct aws_http_message;
struct aws_http_headers;
struct aws_tls_connection_options;
struct aws_input_stream;

struct aws_s3_client;
struct aws_s3_request;
struct aws_s3_meta_request;
struct aws_s3_meta_request_result;
struct aws_uri;

/**
 * A Meta Request represents a group of generated requests that are being done on behalf of the
 * original request. For example, one large GetObject request can be transformed into a series
 * of ranged GetObject requests that are executed in parallel to improve throughput.
 *
 * The aws_s3_meta_request_type is a hint of transformation to be applied.
 */
enum aws_s3_meta_request_type {

    /**
     * The Default meta request type sends any request to S3 as-is (with no transformation). For example,
     * it can be used to pass a CreateBucket request.
     */
    AWS_S3_META_REQUEST_TYPE_DEFAULT,

    /**
     * The GetObject request will be split into a series of ranged GetObject requests that are
     * executed in parallel to improve throughput, when possible.
     */
    AWS_S3_META_REQUEST_TYPE_GET_OBJECT,

    /**
     * The PutObject request will be split into MultiPart uploads that are executed in parallel
     * to improve throughput, when possible.
     */
    AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,

    /**
     * The CopyObject meta request performs a multi-part copy
     * using multiple S3 UploadPartCopy requests in parallel, or bypasses
     * a CopyObject request to S3 if the object size is not large enough for
     * a multipart upload.
     */
    AWS_S3_META_REQUEST_TYPE_COPY_OBJECT,

    AWS_S3_META_REQUEST_TYPE_MAX,
};

/**
 * Invoked to provide response headers received during execution of the meta request, both for
 * success and error HTTP status codes.
 *
 * Return AWS_OP_SUCCESS to continue processing the request.
 * Return AWS_OP_ERR to indicate failure and cancel the request.
 */
typedef int(aws_s3_meta_request_headers_callback_fn)(
    struct aws_s3_meta_request *meta_request,
    const struct aws_http_headers *headers,
    int response_status,
    void *user_data);

/**
 * Invoked to provide the request body as it is received.
 *
 * Return AWS_OP_SUCCESS to continue processing the request.
 * Return AWS_OP_ERR to indicate failure and cancel the request.
 */
typedef int(aws_s3_meta_request_receive_body_callback_fn)(

    /* The meta request that the callback is being issued for. */
    struct aws_s3_meta_request *meta_request,

    /* The body data for this chunk of the object. */
    const struct aws_byte_cursor *body,

    /* The byte index of the object that this refers to. For example, for an HTTP message that has a range header, the
       first chunk received will have a range_start that matches the range header's range-start.*/
    uint64_t range_start,

    /* User data specified by aws_s3_meta_request_options.*/
    void *user_data);

/**
 * Invoked when the entire meta request execution is complete.
 */
typedef void(aws_s3_meta_request_finish_fn)(
    struct aws_s3_meta_request *meta_request,
    const struct aws_s3_meta_request_result *meta_request_result,
    void *user_data);

/**
 * Information sent in the meta_request progress callback.
 */
struct aws_s3_meta_request_progress {

    /* Bytes transferred since the previous progress update */
    uint64_t bytes_transferred;

    /* Length of the entire meta request operation */
    uint64_t content_length;
};

/**
 * Invoked to report progress of multi-part upload and copy object requests.
 */
typedef void(aws_s3_meta_request_progress_fn)(
    struct aws_s3_meta_request *meta_request,
    const struct aws_s3_meta_request_progress *progress,
    void *user_data);

typedef void(aws_s3_meta_request_shutdown_fn)(void *user_data);

typedef void(aws_s3_client_shutdown_complete_callback_fn)(void *user_data);

enum aws_s3_meta_request_tls_mode {
    AWS_MR_TLS_ENABLED,
    AWS_MR_TLS_DISABLED,
};

enum aws_s3_meta_request_compute_content_md5 {
    AWS_MR_CONTENT_MD5_DISABLED,
    AWS_MR_CONTENT_MD5_ENABLED,
};

enum aws_s3_checksum_algorithm {
    AWS_SCA_NONE = 0,
    AWS_SCA_INIT,
    AWS_SCA_CRC32C = AWS_SCA_INIT,
    AWS_SCA_CRC32,
    AWS_SCA_SHA1,
    AWS_SCA_SHA256,
    AWS_SCA_COUNT,
};

/* Options for a new client. */
struct aws_s3_client_config {

    /* When set, this will cap the number of active connections. When 0, the client will determine this value based on
     * throughput_target_gbps. (Recommended) */
    uint32_t max_active_connections_override;

    /* Region that the S3 bucket lives in. */
    struct aws_byte_cursor region;

    /* Client bootstrap used for common staples such as event loop group, host resolver, etc.. s*/
    struct aws_client_bootstrap *client_bootstrap;

    /* How tls should be used while performing the request
     * If this is ENABLED:
     *     If tls_connection_options is not-null, then those tls options will be used
     *     If tls_connection_options is NULL, then default tls options will be used
     * If this is DISABLED:
     *     No tls options will be used, regardless of tls_connection_options value.
     */
    enum aws_s3_meta_request_tls_mode tls_mode;

    /* TLS Options to be used for each connection, if tls_mode is ENABLED. When compiling with BYO_CRYPTO, and tls_mode
     * is ENABLED, this is required. Otherwise, this is optional. */
    struct aws_tls_connection_options *tls_connection_options;

    /* Signing options to be used for each request. Specify NULL to not sign requests. */
    struct aws_signing_config_aws *signing_config;

    /* Size of parts the files will be downloaded or uploaded in. */
    size_t part_size;

    /* If the part size needs to be adjusted for service limits, this is the maximum size it will be adjusted to.. */
    size_t max_part_size;

    /* Throughput target in Gbps that we are trying to reach. */
    double throughput_target_gbps;

    /* Retry strategy to use. If NULL, a default retry strategy will be used. */
    struct aws_retry_strategy *retry_strategy;

    /**
     * For multi-part upload, content-md5 will be calculated if the AWS_MR_CONTENT_MD5_ENABLED is specified
     *     or initial request has content-md5 header.
     * For single-part upload, keep the content-md5 in the initial request unchanged. */
    enum aws_s3_meta_request_compute_content_md5 compute_content_md5;

    /* Callback and associated user data for when the client has completed its shutdown process. */
    aws_s3_client_shutdown_complete_callback_fn *shutdown_callback;
    void *shutdown_callback_user_data;
};

/* Options for a new meta request, ie, file transfer that will be handled by the high performance client. */
struct aws_s3_meta_request_options {

    /* The type of meta request we will be trying to accelerate. */
    enum aws_s3_meta_request_type type;

    /* Signing options to be used for each request created for this meta request.  If NULL, options in the client will
     * be used. If not NULL, these options will override the client options. */
    struct aws_signing_config_aws *signing_config;

    /* Initial HTTP message that defines what operation we are doing. */
    struct aws_http_message *message;

    /**
     * checksum algorithm will cause single part put requests to append a trailing checksum of the body corresponding to
     * the specified algorithm. Multipart uploads will have a checksum uploaded corresponding to each individual part,
     * and the body of the complete multipart upload request will contain a list of the checksums from erach part.
     * Get requests will be unaffected by this setting. Copy object requests will have a header specifying to s3 to
     * calculate a new checksum with the corresponding algorithm.
     * Note: If set, it will disable compute_content_md5
     */
    /* TODO add functionality for copy object */
    enum aws_s3_checksum_algorithm checksum_algorithm;

    /**
     * Enable checksumode header will be attached to get requests, this will tell s3 to send back checksums headers if
     * they exist.
     *
     * - If the object to get was uploaded as a single part upload with checksums, s3 will provide a checksum of the
     * entire object. The checksum of the entire body received will be calculated and compared to the checksum provided
     * by s3.
     * - If a checksum of a part object is received, the client will
     * calculate the corresponding checksum on the response bodies. The metarequest will finish with a did
     * validate field and set the error code to AWS_ERROR_S3_RESPONSE_CHECKSUM_MISMATCH if the calculated
     * checksum, and checksum found in the response header do not match.
     */
    /* TODO add a validation list, empty validation list is equivalent to false */
    bool validate_get_response_checksum;

    /* User data for all callbacks. */
    void *user_data;

    /**
     * Optional.
     * Invoked to provide response headers received during execution of the meta request.
     * See `aws_s3_meta_request_headers_callback_fn`.
     */
    aws_s3_meta_request_headers_callback_fn *headers_callback;

    /**
     * Invoked to provide the request body as it is received.
     * See `aws_s3_meta_request_receive_body_callback_fn`.
     */
    aws_s3_meta_request_receive_body_callback_fn *body_callback;

    /**
     * Invoked when the entire meta request execution is complete.
     * See `aws_s3_meta_request_finish_fn`.
     */
    aws_s3_meta_request_finish_fn *finish_callback;

    /* Callback for when the meta request has completely cleaned up. */
    aws_s3_meta_request_shutdown_fn *shutdown_callback;

    /**
     * Invoked to report progress of the meta request execution.
     * Currently, the progress callback is invoked only for the CopyObject meta request type.
     * TODO: support this callback for all the types of meta requests
     * See `aws_s3_meta_request_progress_fn`
     */
    aws_s3_meta_request_progress_fn *progress_callback;

    /**
     * Optional.
     * The endpoint for the meta request to connect to. If not set, then tls settings from client will
     * determine the port, and host header from `message` will determine the host for the connection.
     * Note: must match the host header from `message`
     */
    struct aws_uri *endpoint;
};

/* Result details of a meta request.
 *
 * If error_code is AWS_ERROR_SUCCESS, then response_status will match the response_status passed earlier by the header
 * callback and error_response_headers and error_response_body will be NULL.
 *
 * If error_code is equal to AWS_ERROR_S3_INVALID_RESPONSE_STATUS, then error_response_headers, error_response_body, and
 * response_status will be populated by the failed request.
 *
 * For all other error codes, response_status will be 0, and the error_response variables will be NULL.
 */
struct aws_s3_meta_request_result {

    /* HTTP Headers for the failed request that triggered finish of the meta request.  NULL if no request failed. */
    struct aws_http_headers *error_response_headers;

    /* Response body for the failed request that triggered finishing of the meta request.  NUll if no request failed.*/
    struct aws_byte_buf *error_response_body;

    /* Response status of the failed request or of the entire meta request. */
    int response_status;

    /* Only set for GET request.
     * Was the server side checksum compared against a calculated checksum of the response body. This may be false
     * even if validate_get_response_checksum was set because the object was uploaded without a checksum, or was
     * uploaded as a multipart object. */
    bool did_validate;

    /* algorithm used to validate checksum */
    enum aws_s3_checksum_algorithm validation_algorithm;

    /* Final error code of the meta request. */
    int error_code;
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
void aws_s3_meta_request_cancel(struct aws_s3_meta_request *meta_request);

AWS_S3_API
void aws_s3_meta_request_acquire(struct aws_s3_meta_request *meta_request);

AWS_S3_API
void aws_s3_meta_request_release(struct aws_s3_meta_request *meta_request);

AWS_S3_API
void aws_s3_init_default_signing_config(
    struct aws_signing_config_aws *signing_config,
    const struct aws_byte_cursor region,
    struct aws_credentials_provider *credentials_provider);

AWS_EXTERN_C_END

#endif /* AWS_S3_CLIENT_H */
