/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "s3_tester.h"
#include <aws/common/environment.h>
#include <aws/s3/private/s3_default_buffer_pool.h>
#include <aws/s3/private/s3_util.h>
#include <aws/testing/aws_test_harness.h>

#define TEST_CASE(NAME)                                                                                                \
    AWS_TEST_CASE(NAME, s_test_##NAME);                                                                                \
    static int s_test_##NAME(struct aws_allocator *allocator, void *ctx)

static const char *s_memory_limit_env_var = "AWS_CRT_S3_MEMORY_LIMIT_IN_GIB";

/* Copied from s3_default_buffer_pool.c */
static const size_t s_buffer_pool_reserved_mem = MB_TO_BYTES(128);

/**
 * Test that memory limit can be set via environment variable when config value is 0
 */
TEST_CASE(s3_client_memory_limit_from_env_var_valid) {
    (void)ctx;
    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    /* Set environment variable to 4 GiB */
    struct aws_string *env_var_name = aws_string_new_from_c_str(allocator, s_memory_limit_env_var);
    struct aws_string *env_var_value = aws_string_new_from_c_str(allocator, "1");
    ASSERT_SUCCESS(aws_set_environment_value(env_var_name, env_var_value));

    struct aws_s3_client_config client_config = {
        .part_size = MB_TO_BYTES(8),
        .throughput_target_gbps = 10.0,
        .memory_limit_in_bytes = 0, /* Will read from environment variable */
    };

    ASSERT_SUCCESS(aws_s3_tester_bind_client(
        &tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION | AWS_S3_TESTER_BIND_CLIENT_SIGNING));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);
    ASSERT_TRUE(client != NULL);

    /* Verify that buffer pool was configured with 4 GiB limit */
    size_t expected_memory_limit = GB_TO_BYTES(1) - s_buffer_pool_reserved_mem;
    ASSERT_TRUE(client->buffer_pool != NULL);
    struct aws_s3_default_buffer_pool_usage_stats stats = aws_s3_default_buffer_pool_get_usage(client->buffer_pool);
    ASSERT_UINT_EQUALS(stats.mem_limit, expected_memory_limit);

    aws_s3_client_release(client);

    /* Clean up environment variable */
    aws_string_destroy(env_var_name);
    aws_string_destroy(env_var_value);

    aws_s3_tester_clean_up(&tester);
    return AWS_OP_SUCCESS;
}

/**
 * Test that config value takes precedence over environment variable
 */
TEST_CASE(s3_client_memory_limit_config_takes_precedence) {
    (void)ctx;
    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    /* Set environment variable to 4 GiB */
    struct aws_string *env_var_name = aws_string_new_from_c_str(allocator, s_memory_limit_env_var);
    struct aws_string *env_var_value = aws_string_new_from_c_str(allocator, "1");
    ASSERT_SUCCESS(aws_set_environment_value(env_var_name, env_var_value));
    aws_string_destroy(env_var_name);
    aws_string_destroy(env_var_value);

    struct aws_s3_client_config client_config = {
        .part_size = MB_TO_BYTES(8),
        .throughput_target_gbps = 10.0,
        .memory_limit_in_bytes = GB_TO_BYTES(2), /* Config value should take precedence */
    };
    ASSERT_SUCCESS(aws_s3_tester_bind_client(
        &tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION | AWS_S3_TESTER_BIND_CLIENT_SIGNING));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);
    ASSERT_TRUE(client != NULL);

    /* The 2 GiB from config should be used, not the 1 GiB from env var */
    ASSERT_TRUE(client->buffer_pool != NULL);
    size_t expected_memory_limit = GB_TO_BYTES(2) - s_buffer_pool_reserved_mem;
    ASSERT_TRUE(client->buffer_pool != NULL);
    struct aws_s3_default_buffer_pool_usage_stats stats = aws_s3_default_buffer_pool_get_usage(client->buffer_pool);
    ASSERT_UINT_EQUALS(stats.mem_limit, expected_memory_limit);

    aws_s3_client_release(client);

    /* Clean up environment variable */
    env_var_name = aws_string_new_from_c_str(allocator, s_memory_limit_env_var);
    ASSERT_SUCCESS(aws_unset_environment_value(env_var_name));
    aws_string_destroy(env_var_name);

    aws_s3_tester_clean_up(&tester);
    return AWS_OP_SUCCESS;
}

/**
 * Test that invalid environment variable value causes client creation to fail
 */
TEST_CASE(s3_client_memory_limit_from_env_var_invalid) {
    (void)ctx;
    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    /* Set environment variable to invalid value */
    struct aws_string *env_var_name = aws_string_new_from_c_str(allocator, s_memory_limit_env_var);
    struct aws_string *env_var_value = aws_string_new_from_c_str(allocator, "invalid");
    ASSERT_SUCCESS(aws_set_environment_value(env_var_name, env_var_value));
    aws_string_destroy(env_var_name);
    aws_string_destroy(env_var_value);

    struct aws_s3_client_config client_config = {
        .part_size = MB_TO_BYTES(8),
        .throughput_target_gbps = 10.0,
        .memory_limit_in_bytes = 0, /* Will try to read from environment variable */
    };
    ASSERT_SUCCESS(aws_s3_tester_bind_client(
        &tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION | AWS_S3_TESTER_BIND_CLIENT_SIGNING));

    /* Client creation should fail due to invalid env var value */
    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);
    ASSERT_TRUE(client == NULL);
    ASSERT_INT_EQUALS(AWS_ERROR_INVALID_ARGUMENT, aws_last_error());
    /* Client failed to set up. */
    tester.bound_to_client = false;
    /* Clean up environment variable */
    env_var_name = aws_string_new_from_c_str(allocator, s_memory_limit_env_var);
    ASSERT_SUCCESS(aws_unset_environment_value(env_var_name));
    aws_string_destroy(env_var_name);

    aws_s3_tester_clean_up(&tester);
    return AWS_OP_SUCCESS;
}

/**
 * Test that environment variable with value causing overflow is handled properly
 */
TEST_CASE(s3_client_memory_limit_from_env_var_overflow) {
    (void)ctx;
    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    /* Set environment variable to a very large value that would overflow when converted to bytes */
    struct aws_string *env_var_name = aws_string_new_from_c_str(allocator, s_memory_limit_env_var);
    struct aws_string *env_var_value = aws_string_new_from_c_str(allocator, "18446744073709551615"); /* UINT64_MAX */
    ASSERT_SUCCESS(aws_set_environment_value(env_var_name, env_var_value));
    aws_string_destroy(env_var_name);
    aws_string_destroy(env_var_value);

    struct aws_s3_client_config client_config = {
        .part_size = MB_TO_BYTES(8),
        .throughput_target_gbps = 10.0,
        .memory_limit_in_bytes = 0, /* Will try to read from environment variable */
    };

    ASSERT_SUCCESS(aws_s3_tester_bind_client(
        &tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION | AWS_S3_TESTER_BIND_CLIENT_SIGNING));

    /* Client creation should fail due to overflow during GiB to bytes conversion */
    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);
    ASSERT_TRUE(client == NULL);
    ASSERT_INT_EQUALS(AWS_ERROR_INVALID_ARGUMENT, aws_last_error());
    /* Client failed to set up. */
    tester.bound_to_client = false;

    /* Clean up environment variable */
    env_var_name = aws_string_new_from_c_str(allocator, s_memory_limit_env_var);
    ASSERT_SUCCESS(aws_unset_environment_value(env_var_name));
    aws_string_destroy(env_var_name);
    aws_s3_tester_clean_up(&tester);
    return AWS_OP_SUCCESS;
}
