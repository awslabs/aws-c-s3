/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/byte_buf.h>
#include <aws/common/common.h>
#include <aws/common/condition_variable.h>
#include <aws/common/logging.h>
#include <aws/testing/aws_test_harness.h>

#include <aws/io/event_loop.h>
#include <aws/io/host_resolver.h>

#include <aws/s3/s3.h>
#include <aws/s3/s3_client.h>
#include <aws/s3/s3_request.h>
#include <aws/s3/s3_request_result.h>

#include "s3_get_object_request.h"
#include "s3_get_object_result.h"

struct aws_s3_tester {
    struct aws_logger logger;

    struct aws_mutex lock;
    struct aws_condition_variable signal;
    bool received_finish_callback;
    bool clean_up_finished;
    int finish_error_code;
};

static int init_tester(struct aws_allocator *allocator, struct aws_s3_tester *tester) {
    (void)allocator;

    AWS_ZERO_STRUCT(*tester);

    struct aws_logger_standard_options logger_options = {.level = AWS_LOG_LEVEL_INFO, .file = stderr};

    ASSERT_SUCCESS(aws_logger_init_standard(&tester->logger, allocator, &logger_options));
    aws_logger_set(&tester->logger);

    if (aws_mutex_init(&tester->lock)) {
        return AWS_OP_ERR;
    }

    if (aws_condition_variable_init(&tester->signal)) {
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}

static void clean_up_tester(struct aws_s3_tester *tester) {
    aws_condition_variable_clean_up(&tester->signal);
    aws_mutex_clean_up(&tester->lock);

    aws_logger_set(NULL);
    aws_logger_clean_up(&tester->logger);
}

static bool s_has_tester_received_finish_callback(void *user_data) {
    struct aws_s3_tester *tester = (struct aws_s3_tester *)user_data;
    return tester->received_finish_callback;
}

static bool s_has_tester_clean_up_finished(void *user_data) {
    struct aws_s3_tester *tester = (struct aws_s3_tester *)user_data;
    return tester->clean_up_finished;
}

static int s_test_s3_get_object_body_callback(
    struct aws_s3_request_get_object *request,
    struct aws_http_stream *stream,
    const struct aws_byte_cursor *body,
    void *user_data) {
    (void)request;
    (void)stream;
    (void)user_data;

    AWS_LOGF_INFO(AWS_LS_S3_GENERAL, "Body of response: %s", (const char *)body->ptr);

    return AWS_OP_SUCCESS;
}

static void s_test_s3_get_object_finish(
    struct aws_s3_request *request,
    struct aws_s3_request_result *result,
    void *user_data) {
    (void)result;

    struct aws_s3_tester *tester = (struct aws_s3_tester *)user_data;

    aws_mutex_lock(&tester->lock);
    tester->received_finish_callback = true;
    tester->finish_error_code = aws_s3_request_result_get_error_code(result);
    aws_mutex_unlock(&tester->lock);

    aws_condition_variable_notify_one(&tester->signal);
}

static void s_tester_clean_up_finished(void *user_data) {
    struct aws_s3_tester *tester = (struct aws_s3_tester *)user_data;

    aws_mutex_lock(&tester->lock);
    tester->clean_up_finished = true;
    aws_mutex_unlock(&tester->lock);

    aws_condition_variable_notify_one(&tester->signal);
}

AWS_TEST_CASE(test_s3_get_object, s_test_s3_get_object)
static int s_test_s3_get_object(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_s3_library_init(allocator);

    struct aws_s3_tester tester;
    init_tester(allocator, &tester);

    struct aws_event_loop_group el_group;
    ASSERT_SUCCESS(aws_event_loop_group_default_init(&el_group, allocator, 1));

    struct aws_host_resolver host_resolver;
    AWS_ZERO_STRUCT(host_resolver);
    ASSERT_SUCCESS(aws_host_resolver_init_default(&host_resolver, allocator, 10, &el_group));

    struct aws_s3_client_config client_config = {.el_group = &el_group,
                                                 .host_resolver = &host_resolver,
                                                 .region = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("us-west-2"),
                                                 .bucket_name =
                                                     AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("aws-crt-canary-bucket-rc"),
                                                 .shutdown_callback = s_tester_clean_up_finished,
                                                 .shutdown_callback_user_data = &tester};

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    struct aws_s3_request_get_object_options get_object_options = {
        .key = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("/test_object.txt"),
        .body_callback = s_test_s3_get_object_body_callback,
        .request_options.user_data = &tester,
        .request_options.finish_callback = s_test_s3_get_object_finish};

    struct aws_s3_request *request = aws_s3_request_get_object_new(allocator, &get_object_options);
    ASSERT_NOT_NULL(request);

    ASSERT_SUCCESS(aws_s3_client_execute_request(client, request));

    aws_mutex_lock(&tester.lock);
    aws_condition_variable_wait_pred(&tester.signal, &tester.lock, s_has_tester_received_finish_callback, &tester);
    aws_mutex_unlock(&tester.lock);

    ASSERT_TRUE(tester.finish_error_code == AWS_ERROR_SUCCESS);

    aws_s3_request_release(request);
    request = NULL;

    aws_s3_client_release(client);
    client = NULL;

    aws_mutex_lock(&tester.lock);
    aws_condition_variable_wait_pred(&tester.signal, &tester.lock, s_has_tester_clean_up_finished, &tester);
    aws_mutex_unlock(&tester.lock);

    aws_host_resolver_clean_up(&host_resolver);
    aws_event_loop_group_clean_up(&el_group);

    clean_up_tester(&tester);

    aws_s3_library_clean_up();

    return 0;
}
