/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "s3_tester.h"
#include <aws/testing/aws_test_harness.h>

static void s_tester_clean_up_finished(void *user_data);

static bool s_has_tester_received_finish_callback(void *user_data);

static bool s_tester_has_clean_up_finished(void *user_data);

int aws_s3_tester_init(
    struct aws_allocator *allocator,
    struct aws_s3_tester *tester,
    const struct aws_byte_cursor bucket_name,
    const struct aws_byte_cursor region) {
    (void)allocator;

    AWS_ZERO_STRUCT(*tester);

    struct aws_logger_standard_options logger_options = {.level = AWS_LOG_LEVEL_INFO, .file = stderr};

    ASSERT_SUCCESS(aws_logger_init_standard(&tester->logger, allocator, &logger_options));
    aws_logger_set(&tester->logger);

    if (aws_mutex_init(&tester->lock)) {
        return AWS_OP_ERR;
    }

    if (aws_condition_variable_init(&tester->signal)) {
        goto condition_variable_failed;
    }

    tester->bucket_name = aws_string_new_from_array(allocator, bucket_name.ptr, bucket_name.len);

    if (tester->bucket_name == NULL) {
        goto bucket_name_failed;
    }

    tester->region = aws_string_new_from_array(allocator, region.ptr, region.len);

    if (tester->region == NULL) {
        goto region_name_failed;
    }

    struct aws_byte_cursor endpoint_url_part0 = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL(".s3.");
    struct aws_byte_cursor endpoint_url_part1 = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL(".amazonaws.com");
    size_t endpoint_buffer_len =
        (tester->bucket_name->len) + endpoint_url_part0.len + (tester->region->len) + endpoint_url_part1.len + 1;
    char *endpoint_buffer = aws_mem_acquire(allocator, endpoint_buffer_len);

    if (endpoint_buffer == NULL) {
        return AWS_OP_ERR;
    }

    endpoint_buffer[0] = '\0';

    strncat(endpoint_buffer, aws_string_c_str(tester->bucket_name), tester->bucket_name->len);
    strncat(endpoint_buffer, (const char *)endpoint_url_part0.ptr, endpoint_url_part0.len);
    strncat(endpoint_buffer, aws_string_c_str(tester->region), tester->region->len);
    strncat(endpoint_buffer, (const char *)endpoint_url_part1.ptr, endpoint_url_part1.len);

    tester->endpoint = aws_string_new_from_c_str(allocator, endpoint_buffer);

    if (tester->endpoint == NULL) {
        goto endpoint_setup_failed;
    }

    aws_mem_release(allocator, endpoint_buffer);

    ASSERT_SUCCESS(aws_event_loop_group_default_init(&tester->el_group, allocator, 1));
    ASSERT_SUCCESS(aws_host_resolver_init_default(&tester->host_resolver, allocator, 10, &tester->el_group));

    return AWS_OP_SUCCESS;

endpoint_setup_failed:

    if (tester->region != NULL) {
        aws_string_destroy(tester->region);
        tester->region = NULL;
    }

region_name_failed:

    if (tester->bucket_name != NULL) {
        aws_string_destroy(tester->bucket_name);
        tester->bucket_name = NULL;
    }

bucket_name_failed:

    aws_condition_variable_clean_up(&tester->signal);

condition_variable_failed:

    aws_mutex_clean_up(&tester->lock);

    return AWS_OP_ERR;
}

void aws_s3_tester_wait_for_finish(struct aws_s3_tester *tester) {
    AWS_PRECONDITION(tester);

    aws_mutex_lock(&tester->lock);
    aws_condition_variable_wait_pred(&tester->signal, &tester->lock, s_has_tester_received_finish_callback, tester);
    aws_mutex_unlock(&tester->lock);
}

void aws_s3_tester_wait_for_clean_up(struct aws_s3_tester *tester) {
    AWS_PRECONDITION(tester);

    aws_mutex_lock(&tester->lock);
    aws_condition_variable_wait_pred(&tester->signal, &tester->lock, s_tester_has_clean_up_finished, tester);
    aws_mutex_unlock(&tester->lock);
}

void aws_s3_tester_notify_finished(struct aws_s3_tester *tester, int error_code) {
    AWS_PRECONDITION(tester);

    aws_mutex_lock(&tester->lock);
    tester->received_finish_callback = true;
    tester->finish_error_code = error_code;
    aws_mutex_unlock(&tester->lock);

    aws_condition_variable_notify_one(&tester->signal);
}

void aws_s3_tester_clean_up(struct aws_s3_tester *tester) {

    aws_host_resolver_clean_up(&tester->host_resolver);
    aws_event_loop_group_clean_up(&tester->el_group);

    if (tester->region != NULL) {
        aws_string_destroy(tester->region);
        tester->region = NULL;
    }

    if (tester->bucket_name != NULL) {
        aws_string_destroy(tester->bucket_name);
        tester->bucket_name = NULL;
    }

    if (tester->endpoint != NULL) {
        aws_string_destroy(tester->endpoint);
        tester->endpoint = NULL;
    }

    aws_condition_variable_clean_up(&tester->signal);
    aws_mutex_clean_up(&tester->lock);

    aws_logger_set(NULL);
    aws_logger_clean_up(&tester->logger);
}

void aws_s3_tester_bind_client_shutdown(struct aws_s3_tester *tester, struct aws_s3_client_config *config) {
    config->shutdown_callback = s_tester_clean_up_finished, config->shutdown_callback_user_data = tester;
}

static void s_tester_clean_up_finished(void *user_data) {
    struct aws_s3_tester *tester = (struct aws_s3_tester *)user_data;

    aws_mutex_lock(&tester->lock);
    tester->clean_up_finished = true;
    aws_mutex_unlock(&tester->lock);

    aws_condition_variable_notify_one(&tester->signal);
}

static bool s_has_tester_received_finish_callback(void *user_data) {
    struct aws_s3_tester *tester = (struct aws_s3_tester *)user_data;
    return tester->received_finish_callback;
}

static bool s_tester_has_clean_up_finished(void *user_data) {
    struct aws_s3_tester *tester = (struct aws_s3_tester *)user_data;
    return tester->clean_up_finished;
}
