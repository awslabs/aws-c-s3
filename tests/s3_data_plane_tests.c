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
#include <aws/io/event_loop.h>
#include <aws/io/host_resolver.h>
#include <aws/io/stream.h>
#include <aws/io/tls_channel_handler.h>
#include <aws/testing/aws_test_harness.h>
#include <inttypes.h>

AWS_TEST_CASE(test_s3_get_performance, s_test_s3_get_performance)
static int s_test_s3_get_performance(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

#ifndef PERFORMANCE_TEST_LOGGING_ENABLED
    struct aws_logger_standard_options err_logger_options;
    AWS_ZERO_STRUCT(err_logger_options);
    err_logger_options.file = AWS_TESTING_REPORT_FD;
    err_logger_options.level = AWS_LL_ERROR;
    err_logger_options.filename = NULL;

    struct aws_logger err_logger;
    aws_logger_init_standard(&err_logger, aws_default_allocator(), &err_logger_options);
    aws_logger_set(&err_logger);
#endif

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client *client = NULL;
    struct aws_s3_tester_client_options client_options = {
        .throughput_target_gbps = PERFORMANCE_TEST_THROUGHPUT_TARGET_GBPS,
    };

    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    const struct aws_s3_tester_meta_request_options meta_request_test_options_template = {
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_SUCCESS,
        .get_options =
            {
                .object_path = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("/crt-canary-obj-multipart"),
            },
    };

    struct aws_s3_tester_meta_request_options meta_request_test_options[PERFORMANCE_TEST_NUM_TRANSFERS];
    const size_t num_meta_requests = sizeof(meta_request_test_options) / sizeof(meta_request_test_options[0]);

    for (size_t i = 0; i < num_meta_requests; ++i) {
        meta_request_test_options[i] = meta_request_test_options_template;
    }

    struct aws_s3_tester_send_meta_requests_options options = {
        .tester = &tester,
        .client = client,
        .num_meta_requests = num_meta_requests,
        .meta_request_test_options = meta_request_test_options,
    };

    ASSERT_SUCCESS(aws_s3_tester_send_meta_requests(&options, NULL));

    aws_s3_client_release(client);

#ifndef PERFORMANCE_TEST_LOGGING_ENABLED
    aws_logger_set(NULL);
    aws_logger_clean_up(&err_logger);
#endif

    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(test_s3_put_performance, s_test_s3_put_performance)
static int s_test_s3_put_performance(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

#ifndef PERFORMANCE_TEST_LOGGING_ENABLED
    struct aws_logger_standard_options err_logger_options;
    AWS_ZERO_STRUCT(err_logger_options);
    err_logger_options.file = AWS_TESTING_REPORT_FD;
    err_logger_options.level = AWS_LL_ERROR;
    err_logger_options.filename = NULL;

    struct aws_logger err_logger;
    aws_logger_init_standard(&err_logger, aws_default_allocator(), &err_logger_options);
    aws_logger_set(&err_logger);
#endif

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client *client = NULL;
    struct aws_s3_tester_client_options client_options = {
        .throughput_target_gbps = PERFORMANCE_TEST_THROUGHPUT_TARGET_GBPS,
    };

    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_s3_tester_meta_request_options meta_request_test_options_template = {
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_SUCCESS,
        .put_options =
            {
                .object_size_mb = PERFORMANCE_TEST_UPLOAD_OBJECT_SIZE_MB,
            },
    };

    struct aws_s3_tester_meta_request_options meta_request_test_options[PERFORMANCE_TEST_NUM_TRANSFERS];
    const size_t num_meta_requests = sizeof(meta_request_test_options) / sizeof(meta_request_test_options[0]);

    for (size_t i = 0; i < num_meta_requests; ++i) {
        meta_request_test_options[i] = meta_request_test_options_template;
    }

    struct aws_s3_tester_send_meta_requests_options options = {
        .tester = &tester,
        .client = client,
        .num_meta_requests = num_meta_requests,
        .meta_request_test_options = meta_request_test_options,
    };

#ifdef PERFORMANCE_TEST_VALIDATE_PUTS
    options.dont_validate_puts = false;
#else
    options.dont_validate_puts = true;
#endif

    ASSERT_SUCCESS(aws_s3_tester_send_meta_requests(&options, NULL));

    aws_s3_client_release(client);

#ifndef PERFORMANCE_TEST_LOGGING_ENABLED
    aws_logger_set(NULL);
    aws_logger_clean_up(&err_logger);
#endif

    aws_s3_tester_clean_up(&tester);

    return 0;
}
