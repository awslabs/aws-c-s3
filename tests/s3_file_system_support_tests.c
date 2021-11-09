/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_client_impl.h"
#include "aws/s3/private/s3_file_system_support.h"
#include "s3_tester.h"

#include <aws/auth/credentials.h>
#include <aws/io/channel_bootstrap.h>
#include <aws/io/tls_channel_handler.h>
#include <aws/testing/aws_test_harness.h>

static int s_test_s3_list_bucket_init_mem_safety(struct aws_allocator *allocator, void *ctx) {
    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_tester_client_options client_options;
    AWS_ZERO_STRUCT(client_options);

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_s3_list_bucket_v2_params params = {
        .client = client,
        .endpoint = aws_byte_cursor_from_c_str("test-endpoint.com"),
        .bucket_name = aws_byte_cursor_from_c_str("test-bucket"),
    };

    struct aws_s3_paginator *paginator = aws_s3_initiate_list_bucket(allocator, &params);
    ASSERT_NOT_NULL(paginator);

    aws_s3_paginator_release(paginator);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(test_s3_list_bucket_init_mem_safety, s_test_s3_list_bucket_init_mem_safety)

static int s_test_s3_list_bucket_init_mem_safety_optional_copies(struct aws_allocator *allocator, void *ctx) {
    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_tester_client_options client_options;
    AWS_ZERO_STRUCT(client_options);

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_s3_list_bucket_v2_params params = {
        .client = client,
        .endpoint = aws_byte_cursor_from_c_str("test-endpoint.com"),
        .bucket_name = aws_byte_cursor_from_c_str("test-bucket"),
        .prefix = aws_byte_cursor_from_c_str("foo/bar"),
        .delimiter = aws_byte_cursor_from_c_str("/"),
        .continuation_token = aws_byte_cursor_from_c_str("base64_encrypted_thing"),
    };

    struct aws_s3_paginator *paginator = aws_s3_initiate_list_bucket(allocator, &params);
    ASSERT_NOT_NULL(paginator);

    aws_s3_paginator_release(paginator);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(
    test_s3_list_bucket_init_mem_safety_optional_copies,
    s_test_s3_list_bucket_init_mem_safety_optional_copies)

struct list_bucket_test_data {
    struct aws_signing_config_aws *signing_config;
    struct aws_mutex mutex;
    struct aws_condition_variable c_var;
    bool done;
    int error_code;
};

static bool s_on_paginator_finished_predicate(void *arg) {
    struct list_bucket_test_data *test_data = arg;
    return test_data->done;
}

static bool s_on_list_bucket_valid_object_fn(const struct aws_s3_object_file_system_info *info, void *user_data) {
    (void)info;
    struct list_bucket_test_data *test_data = user_data;
    (void)test_data;

    return true;
}

static void s_on_list_bucket_page_finished_fn(struct aws_s3_paginator *paginator, int error_code, void *user_data) {

    struct list_bucket_test_data *test_data = user_data;

    test_data->error_code = error_code;

    if (aws_s3_paginator_has_more_results(paginator)) {
        aws_s3_paginator_continue(paginator, test_data->signing_config);
    } else {
        aws_mutex_lock(&test_data->mutex);
        test_data->done = true;
        aws_mutex_unlock(&test_data->mutex);
        aws_condition_variable_notify_one(&test_data->c_var);
    }
}

static int s_test_s3_list_bucket_valid(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_tester_client_options client_options;
    AWS_ZERO_STRUCT(client_options);

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_signing_config_aws signing_config;
    AWS_ZERO_STRUCT(signing_config);
    aws_s3_init_default_signing_config(&signing_config, g_test_s3_region, tester.credentials_provider);

    struct list_bucket_test_data test_data = {
        .signing_config = &signing_config,
        .mutex = AWS_MUTEX_INIT,
        .c_var = AWS_CONDITION_VARIABLE_INIT,
        .done = false,
    };

    struct aws_byte_cursor endpoint = aws_byte_cursor_from_c_str("s3.us-west-2.amazonaws.com");

    struct aws_s3_list_bucket_v2_params params = {
        .client = client,
        .endpoint = endpoint,
        .bucket_name = g_test_bucket_name,
        .on_object = s_on_list_bucket_valid_object_fn,
        .on_list_finished = s_on_list_bucket_page_finished_fn,
        .user_data = &test_data,
    };

    struct aws_s3_paginator *paginator = aws_s3_initiate_list_bucket(allocator, &params);
    ASSERT_NOT_NULL(paginator);

    aws_mutex_lock(&test_data.mutex);
    aws_s3_paginator_continue(paginator, &signing_config);
    aws_condition_variable_wait_pred(&test_data.c_var, &test_data.mutex, s_on_paginator_finished_predicate, &test_data);
    aws_mutex_unlock(&test_data.mutex);

    aws_s3_paginator_release(paginator);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(test_s3_list_bucket_valid, s_test_s3_list_bucket_valid)