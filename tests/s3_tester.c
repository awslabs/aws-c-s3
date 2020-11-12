/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "s3_tester.h"
#include <aws/auth/credentials.h>
#include <aws/http/request_response.h>
#include <aws/io/channel_bootstrap.h>
#include <aws/io/event_loop.h>
#include <aws/io/host_resolver.h>
#include <aws/testing/aws_test_harness.h>
#include <inttypes.h>

static void s_test_s3_meta_request_header_callback(
    struct aws_s3_meta_request *meta_request,
    struct aws_http_headers *headers,
    int response_status,
    void *user_data) {
    (void)meta_request;

    struct aws_s3_tester_meta_request *tester_meta_request = (struct aws_s3_tester_meta_request *)user_data;

    tester_meta_request->response_headers = headers;
    aws_http_headers_acquire(headers);

    tester_meta_request->headers_response_status = response_status;
}

static void s_test_s3_meta_request_body_callback(
    struct aws_s3_meta_request *meta_request,
    const struct aws_byte_cursor *body,
    uint64_t range_start,
    uint64_t range_end,
    void *user_data) {
    (void)meta_request;
    (void)body;

    struct aws_s3_tester_meta_request *tester_meta_request = (struct aws_s3_tester_meta_request *)user_data;
    tester_meta_request->received_body_size += (range_end - range_start) + 1;

    AWS_LOGF_INFO(AWS_LS_S3_GENERAL, "Received range %" PRIu64 "-%" PRIu64, range_start, range_end);
}

static void s_test_s3_meta_request_finish(
    struct aws_s3_meta_request *meta_request,
    const struct aws_s3_meta_request_result *result,
    void *user_data) {
    (void)meta_request;

    struct aws_s3_tester_meta_request *tester_meta_request = (struct aws_s3_tester_meta_request *)user_data;
    struct aws_s3_tester *tester = (struct aws_s3_tester *)tester_meta_request->tester;

    tester_meta_request->error_response_headers = result->error_response_headers;

    if (result->error_response_headers != NULL) {
        aws_http_headers_acquire(result->error_response_headers);
    }

    if (result->error_response_body != NULL) {
        aws_byte_buf_init_copy(
            &tester_meta_request->error_response_body, tester->allocator, result->error_response_body);
    }

    tester_meta_request->finished_response_status = result->response_status;
    tester_meta_request->finished_error_code = result->error_code;

    aws_s3_tester_notify_finished(tester, result);
}

/* Wait for the cleanup notification.  This, and the s_tester_notify_clean_up_signal function are meant to be used for
 * sequential clean up only, and should not overlap with the "finish" callback.  (Both currently use the same
 * mutex/signal.) */
static void s_s3_tester_wait_for_clean_up_signal(struct aws_s3_tester *tester);

/* Notify the tester that a particular clean up step has finished. */
static void s_tester_notify_clean_up_signal(void *user_data);

static bool s_s3_tester_has_received_finish_callback(void *user_data);

static bool s_s3_tester_has_clean_up_finished(void *user_data);

struct aws_string *aws_s3_tester_build_endpoint_string(
    struct aws_allocator *allocator,
    const struct aws_byte_cursor *bucket_name,
    const struct aws_byte_cursor *region) {

    struct aws_byte_cursor endpoint_url_part0 = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL(".s3.");
    struct aws_byte_cursor endpoint_url_part1 = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL(".amazonaws.com");

    struct aws_byte_buf endpoint_buffer;
    aws_byte_buf_init(&endpoint_buffer, allocator, 128);

    aws_byte_buf_append_dynamic(&endpoint_buffer, bucket_name);
    aws_byte_buf_append_dynamic(&endpoint_buffer, &endpoint_url_part0);
    aws_byte_buf_append_dynamic(&endpoint_buffer, region);
    aws_byte_buf_append_dynamic(&endpoint_buffer, &endpoint_url_part1);

    struct aws_string *endpoint_string = aws_string_new_from_buf(allocator, &endpoint_buffer);

    aws_byte_buf_clean_up(&endpoint_buffer);

    return endpoint_string;
}

int aws_s3_tester_init(struct aws_allocator *allocator, struct aws_s3_tester *tester) {

    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(tester);

    (void)allocator;

    AWS_ZERO_STRUCT(*tester);

    tester->allocator = allocator;

    aws_s3_library_init(allocator);

    if (aws_mutex_init(&tester->synced_data.lock)) {
        return AWS_OP_ERR;
    }

    if (aws_condition_variable_init(&tester->signal)) {
        goto condition_variable_failed;
    }

    /* Setup an event loop group and host resolver. */
    tester->el_group = aws_event_loop_group_new_default(allocator, 0, NULL);
    ASSERT_TRUE(tester->el_group != NULL);

    tester->host_resolver = aws_host_resolver_new_default(allocator, 10, tester->el_group, NULL);
    ASSERT_TRUE(tester->host_resolver != NULL);

    /* Setup the client boot strap. */
    {
        struct aws_client_bootstrap_options bootstrap_options;
        AWS_ZERO_STRUCT(bootstrap_options);
        bootstrap_options.event_loop_group = tester->el_group;
        bootstrap_options.host_resolver = tester->host_resolver;
        bootstrap_options.user_data = tester;

        tester->client_bootstrap = aws_client_bootstrap_new(allocator, &bootstrap_options);
    }

    /* Setup the credentials provider */
    {
        struct aws_credentials_provider_chain_default_options credentials_config;
        AWS_ZERO_STRUCT(credentials_config);
        credentials_config.bootstrap = tester->client_bootstrap;
        tester->credentials_provider = aws_credentials_provider_new_chain_default(allocator, &credentials_config);
    }

    return AWS_OP_SUCCESS;

condition_variable_failed:

    aws_mutex_clean_up(&tester->synced_data.lock);

    return AWS_OP_ERR;
}

int aws_s3_tester_bind_client(struct aws_s3_tester *tester, struct aws_s3_client_config *config) {
    AWS_PRECONDITION(tester);
    AWS_PRECONDITION(config);

    ASSERT_TRUE(config->client_bootstrap == NULL);
    config->client_bootstrap = tester->client_bootstrap;

    ASSERT_TRUE(config->credentials_provider == NULL);
    config->credentials_provider = tester->credentials_provider;

    ASSERT_TRUE(config->shutdown_callback == NULL);
    config->shutdown_callback = s_tester_notify_clean_up_signal;

    ASSERT_TRUE(config->shutdown_callback_user_data == NULL);
    config->shutdown_callback_user_data = tester;

    return AWS_OP_SUCCESS;
}

int aws_s3_tester_bind_meta_request(
    struct aws_s3_tester *tester,
    struct aws_s3_meta_request_options *options,
    struct aws_s3_tester_meta_request *tester_meta_request) {

    AWS_ZERO_STRUCT(*tester_meta_request);
    tester_meta_request->tester = tester;

    aws_s3_tester_lock_synced_data(tester);
    ++tester->synced_data.desired_finish_count;
    aws_s3_tester_unlock_synced_data(tester);

    ASSERT_TRUE(options->headers_callback == NULL);
    options->headers_callback = s_test_s3_meta_request_header_callback;

    ASSERT_TRUE(options->body_callback == NULL);
    options->body_callback = s_test_s3_meta_request_body_callback;

    ASSERT_TRUE(options->finish_callback == NULL);
    options->finish_callback = s_test_s3_meta_request_finish;

    ASSERT_TRUE(options->user_data == NULL);
    options->user_data = tester_meta_request;

    return AWS_OP_SUCCESS;
}

void aws_s3_tester_meta_request_clean_up(struct aws_s3_tester_meta_request *test_meta_request) {
    if (test_meta_request == NULL) {
        return;
    }

    aws_http_headers_release(test_meta_request->error_response_headers);
    aws_byte_buf_clean_up(&test_meta_request->error_response_body);
    aws_http_headers_release(test_meta_request->response_headers);

    AWS_ZERO_STRUCT(*test_meta_request);
}

void aws_s3_tester_wait_for_finish(struct aws_s3_tester *tester) {
    AWS_PRECONDITION(tester);

    aws_s3_tester_lock_synced_data(tester);
    aws_condition_variable_wait_pred(
        &tester->signal, &tester->synced_data.lock, s_s3_tester_has_received_finish_callback, tester);
    aws_s3_tester_unlock_synced_data(tester);
}

void aws_s3_tester_notify_finished(struct aws_s3_tester *tester, const struct aws_s3_meta_request_result *result) {
    AWS_PRECONDITION(tester);
    AWS_PRECONDITION(result);

    bool notify = false;

    aws_s3_tester_lock_synced_data(tester);
    ++tester->synced_data.finish_count;

    if (tester->synced_data.desired_finish_count == 0 ||
        tester->synced_data.finish_count == tester->synced_data.desired_finish_count ||
        result->error_code != AWS_ERROR_SUCCESS) {
        tester->synced_data.received_finish_callback = true;
        tester->synced_data.finish_error_code = result->error_code;

        notify = true;
    }

    aws_s3_tester_unlock_synced_data(tester);

    if (notify) {
        aws_condition_variable_notify_one(&tester->signal);
    }
}

void aws_s3_tester_clean_up(struct aws_s3_tester *tester) {
    AWS_PRECONDITION(tester);

    s_s3_tester_wait_for_clean_up_signal(tester);

    aws_client_bootstrap_release(tester->client_bootstrap);
    tester->client_bootstrap = NULL;

    aws_credentials_provider_release(tester->credentials_provider);
    tester->credentials_provider = NULL;

    aws_host_resolver_release(tester->host_resolver);
    tester->host_resolver = NULL;

    aws_event_loop_group_release(tester->el_group);
    tester->el_group = NULL;

    aws_condition_variable_clean_up(&tester->signal);
    aws_mutex_clean_up(&tester->synced_data.lock);

    aws_s3_library_clean_up();

    aws_global_thread_creator_shutdown_wait_for(10);
}

void aws_s3_tester_lock_synced_data(struct aws_s3_tester *tester) {
    AWS_PRECONDITION(tester);
    aws_mutex_lock(&tester->synced_data.lock);
}

void aws_s3_tester_unlock_synced_data(struct aws_s3_tester *tester) {
    AWS_PRECONDITION(tester);

    aws_mutex_unlock(&tester->synced_data.lock);
}

void aws_s3_create_test_buffer(struct aws_allocator *allocator, size_t buffer_size, struct aws_byte_buf *out_buf) {
    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(out_buf);

    struct aws_byte_cursor test_string = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("This is an S3 test.");

    aws_byte_buf_init(out_buf, allocator, buffer_size);

    for (size_t buffer_pos = 0; buffer_pos < buffer_size; buffer_pos += test_string.len) {
        size_t buffer_size_remaining = buffer_size - buffer_pos;
        size_t string_copy_size = test_string.len;

        if (buffer_size_remaining < string_copy_size) {
            string_copy_size = buffer_size_remaining;
        }

        struct aws_byte_cursor from_byte_cursor = {.len = string_copy_size, .ptr = test_string.ptr};

        aws_byte_buf_append(out_buf, &from_byte_cursor);
    }
}

static void s_tester_notify_clean_up_signal(void *user_data) {
    AWS_PRECONDITION(user_data);

    struct aws_s3_tester *tester = (struct aws_s3_tester *)user_data;

    aws_s3_tester_lock_synced_data(tester);
    tester->synced_data.clean_up_flag = true;
    aws_s3_tester_unlock_synced_data(tester);

    aws_condition_variable_notify_one(&tester->signal);
}

static bool s_s3_tester_has_received_finish_callback(void *user_data) {
    AWS_PRECONDITION(user_data);
    struct aws_s3_tester *tester = (struct aws_s3_tester *)user_data;

    return tester->synced_data.received_finish_callback;
}

static bool s_s3_tester_has_clean_up_finished(void *user_data) {
    AWS_PRECONDITION(user_data);
    struct aws_s3_tester *tester = (struct aws_s3_tester *)user_data;

    return tester->synced_data.clean_up_flag;
}

static void s_s3_tester_wait_for_clean_up_signal(struct aws_s3_tester *tester) {
    AWS_PRECONDITION(tester);

    aws_s3_tester_lock_synced_data(tester);
    aws_condition_variable_wait_pred(
        &tester->signal, &tester->synced_data.lock, s_s3_tester_has_clean_up_finished, tester);

    /* Reset the clean up flag for any additional clean up steps */
    tester->synced_data.clean_up_flag = false;

    aws_s3_tester_unlock_synced_data(tester);
}
