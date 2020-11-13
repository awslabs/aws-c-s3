/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "s3_tester.h"
#include "aws/s3/private/s3_client_impl.h"
#include "aws/s3/private/s3_meta_request_impl.h"
#include "aws/s3/private/s3_util.h"
#include <aws/auth/credentials.h>
#include <aws/http/request_response.h>
#include <aws/io/channel_bootstrap.h>
#include <aws/io/event_loop.h>
#include <aws/io/host_resolver.h>
#include <aws/io/stream.h>
#include <aws/testing/aws_test_harness.h>
#include <inttypes.h>

static void s_test_s3_meta_request_header_callback(
    struct aws_s3_meta_request *meta_request,
    const struct aws_http_headers *headers,
    int response_status,
    void *user_data) {
    (void)meta_request;

    struct aws_s3_tester_meta_request *tester_meta_request = (struct aws_s3_tester_meta_request *)user_data;

    /* TODO copy this instead of making acquiring reference. */
    tester_meta_request->response_headers = (struct aws_http_headers *)headers;
    aws_http_headers_acquire(tester_meta_request->response_headers);

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

    ASSERT_TRUE(!tester->bound_to_client);
    tester->bound_to_client = true;

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

    if (tester->bound_to_client) {
        s_s3_tester_wait_for_clean_up_signal(tester);
        tester->bound_to_client = false;
    }

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

struct aws_s3_client *aws_s3_tester_dummy_client_new(struct aws_s3_tester *tester) {
    AWS_PRECONDITION(tester);

    struct aws_s3_client_config client_config = {
        .region = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("dummy_region"),
        .credentials_provider = tester->credentials_provider,
        .client_bootstrap = tester->client_bootstrap,
    };

    return aws_s3_client_new(tester->allocator, &client_config);
}

struct aws_http_message *aws_s3_tester_dummy_http_request_new(struct aws_s3_tester *tester) {
    AWS_PRECONDITION(tester);

    struct aws_http_message *message = aws_http_message_new_request(tester->allocator);

    struct aws_http_header host_header = {.name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Host"),
                                          .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("DummyHost")};

    aws_http_message_add_header(message, host_header);

    return message;
}

static void s_s3_dummy_meta_request_destroy(struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);

    aws_mem_release(meta_request->allocator, meta_request);
}

static bool s_s3_dummy_meta_request_has_work(const struct aws_s3_meta_request *meta_request) {
    return false;
}

static int s_s3_dummy_meta_request_next_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request **out_request) {
    (void)meta_request;
    (void)out_request;
    return AWS_OP_ERR;
}

static int s_s3_dummy_meta_request_prepare_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_client *client,
    struct aws_s3_request *request) {
    (void)meta_request;
    (void)client;
    (void)request;
    return AWS_OP_ERR;
}

static struct aws_s3_meta_request_vtable s_s3_dummy_meta_request_vtable = {
    .has_work = s_s3_dummy_meta_request_has_work,
    .next_request = s_s3_dummy_meta_request_next_request,
    .prepare_request = s_s3_dummy_meta_request_prepare_request,
    .incoming_headers = NULL,
    .incoming_headers_block_done = NULL,
    .incoming_body = NULL,
    .stream_complete = NULL,
    .destroy = s_s3_dummy_meta_request_destroy,
};

struct aws_s3_dummy_meta_request {
    struct aws_s3_meta_request base;
};

struct aws_s3_meta_request *aws_s3_tester_dummy_meta_request_new(
    struct aws_s3_tester *tester,
    struct aws_s3_client *dummy_client) {
    AWS_PRECONDITION(tester);

    struct aws_s3_dummy_meta_request *dummy_meta_request =
        aws_mem_calloc(tester->allocator, 1, sizeof(struct aws_s3_dummy_meta_request));

    struct aws_http_message *dummy_http_message = aws_s3_tester_dummy_http_request_new(tester);

    struct aws_s3_meta_request_options options = {
        .message = dummy_http_message,
    };

    struct aws_s3_meta_request_internal_options internal_options;
    internal_options.options = &options;
    internal_options.client = dummy_client;

    aws_s3_meta_request_init_base(
        tester->allocator,
        &internal_options,
        dummy_meta_request,
        &s_s3_dummy_meta_request_vtable,
        &dummy_meta_request->base);

    aws_http_message_release(dummy_http_message);
    dummy_http_message = NULL;

    aws_s3_client_release(dummy_meta_request->base.client);
    dummy_meta_request->base.client = NULL;

    return &dummy_meta_request->base;
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

struct aws_http_message *aws_s3_test_make_get_object_request(
    struct aws_allocator *allocator,
    struct aws_byte_cursor host,
    struct aws_byte_cursor key) {

    struct aws_http_message *message = aws_http_message_new_request(allocator);

    if (message == NULL) {
        return NULL;
    }

    struct aws_http_header host_header = {.name = g_host_header_name, .value = host};

    if (aws_http_message_add_header(message, host_header)) {
        goto error_clean_up_message;
    }

    if (aws_http_message_set_request_method(message, aws_http_method_get)) {
        goto error_clean_up_message;
    }

    if (aws_http_message_set_request_path(message, key)) {
        goto error_clean_up_message;
    }

    return message;

error_clean_up_message:

    if (message != NULL) {
        aws_http_message_release(message);
        message = NULL;
    }

    return NULL;
}

struct aws_http_message *aws_s3_test_make_put_object_request(
    struct aws_allocator *allocator,
    struct aws_byte_cursor host,
    struct aws_byte_cursor key,
    struct aws_byte_cursor content_type,
    struct aws_input_stream *body_stream) {

    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(body_stream);

    int64_t body_stream_length = 0;

    if (aws_input_stream_get_length(body_stream, &body_stream_length)) {
        return NULL;
    }

    struct aws_http_message *message = aws_http_message_new_request(allocator);

    if (message == NULL) {
        return NULL;
    }

    struct aws_http_header host_header = {.name = g_host_header_name, .value = host};

    struct aws_http_header content_type_header = {.name = g_content_type_header_name, .value = content_type};

    char content_length_buffer[64] = "";
    snprintf(content_length_buffer, sizeof(content_length_buffer), "%" PRId64 "", body_stream_length);

    struct aws_http_header content_length_header = {.name = g_content_length_header_name,
                                                    .value = aws_byte_cursor_from_c_str(content_length_buffer)};

    if (aws_http_message_add_header(message, host_header)) {
        goto error_clean_up_message;
    }

    if (aws_http_message_add_header(message, content_type_header)) {
        goto error_clean_up_message;
    }

    if (aws_http_message_add_header(message, content_length_header)) {
        goto error_clean_up_message;
    }

    if (aws_http_message_set_request_method(message, aws_http_method_put)) {
        goto error_clean_up_message;
    }

    if (aws_http_message_set_request_path(message, key)) {
        goto error_clean_up_message;
    }

    aws_http_message_set_body_stream(message, body_stream);

    return message;

error_clean_up_message:

    if (message != NULL) {
        aws_http_message_release(message);
        message = NULL;
    }

    return NULL;
}
