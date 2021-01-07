/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/s3/s3.h>

#include <aws/auth/auth.h>
#include <aws/common/error.h>
#include <aws/http/http.h>

#define AWS_DEFINE_ERROR_INFO_S3(CODE, STR) AWS_DEFINE_ERROR_INFO(CODE, STR, "aws-c-s3")

/* clang-format off */
static struct aws_error_info s_errors[] = {
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_MISSING_CONTENT_RANGE_HEADER, "Response missing required Content-Range header."),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_MISSING_ETAG, "Response missing required ETag header."),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_INTERNAL_ERROR, "Response code indicates internal server error"),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_SLOW_DOWN, "Response code indicates throttling"),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_INVALID_RESPONSE_STATUS, "Invalid response status from request"),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_MISSING_UPLOAD_ID, "Upload Id not found in create-multipart-upload response"),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_PROXY_ENV_NOT_FOUND, "Could not find proxy URI in environment variables."),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_PROXY_PARSE_FAILED, "Could not parse proxy URI"),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_UNSUPPORTED_PROXY_SCHEME, "Given Proxy URI has an unsupported scheme"),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_CANCELED, "Request successfully cancelled"),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_INVALID_ENDPOINT, "Invalid S3 endpoint")
};
/* clang-format on */

static struct aws_error_info_list s_error_list = {
    .error_list = s_errors,
    .count = sizeof(s_errors) / sizeof(struct aws_error_info),
};

static struct aws_log_subject_info s_s3_log_subject_infos[] = {
    DEFINE_LOG_SUBJECT_INFO(AWS_LS_S3_GENERAL, "S3General", "Subject for aws-c-s3 logging that defies categorization."),
    DEFINE_LOG_SUBJECT_INFO(AWS_LS_S3_CLIENT, "S3Client", "Subject for aws-c-s3 logging from an aws_s3_client."),
    DEFINE_LOG_SUBJECT_INFO(AWS_LS_S3_REQUEST, "S3Request", "Subject for aws-c-s3 logging from an aws_s3_request."),
    DEFINE_LOG_SUBJECT_INFO(
        AWS_LS_S3_META_REQUEST,
        "S3MetaRequest",
        "Subject for aws-c-s3 logging from an aws_s3_meta_request."),
    DEFINE_LOG_SUBJECT_INFO(AWS_LS_S3_VIP, "S3VIP", "Subject for aws-c-s3 logging from an aws_s3_vip."),
    DEFINE_LOG_SUBJECT_INFO(
        AWS_LS_S3_VIP_CONNECTION,
        "S3VIPConnection",
        "Subject for aws-c-s3 logging from an aws_s3_vip_connection.")};

static struct aws_log_subject_info_list s_s3_log_subject_list = {
    .subject_list = s_s3_log_subject_infos,
    .count = AWS_ARRAY_SIZE(s_s3_log_subject_infos),
};

static bool s_library_initialized = false;
static struct aws_allocator *s_library_allocator = NULL;

void aws_s3_library_init(struct aws_allocator *allocator) {
    if (s_library_initialized) {
        return;
    }

    if (allocator) {
        s_library_allocator = allocator;
    } else {
        s_library_allocator = aws_default_allocator();
    }

    aws_auth_library_init(s_library_allocator);
    aws_http_library_init(s_library_allocator);

    aws_register_error_info(&s_error_list);
    aws_register_log_subject_info_list(&s_s3_log_subject_list);

    s_library_initialized = true;
}

void aws_s3_library_clean_up(void) {
    if (!s_library_initialized) {
        return;
    }

    s_library_initialized = false;

    aws_unregister_log_subject_info_list(&s_s3_log_subject_list);
    aws_unregister_error_info(&s_error_list);
    aws_http_library_clean_up();
    aws_auth_library_clean_up();
    s_library_allocator = NULL;
}
