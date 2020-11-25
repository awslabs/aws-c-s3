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

const struct aws_byte_cursor g_test_body_content_type = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("text/plain");
const struct aws_byte_cursor g_test_s3_region = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("us-west-2");
const struct aws_byte_cursor g_test_bucket_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("aws-crt-canary-bucket");

static void s_test_s3_meta_request_header_callback(
    struct aws_s3_meta_request *meta_request,
    const struct aws_http_headers *headers,
    int response_status,
    void *user_data) {
    (void)meta_request;

    struct aws_s3_meta_request_test_results *meta_request_test_results =
        (struct aws_s3_meta_request_test_results *)user_data;

    aws_http_headers_release(meta_request_test_results->response_headers);

    /* TODO copy this instead of making acquiring reference. */
    meta_request_test_results->response_headers = (struct aws_http_headers *)headers;
    aws_http_headers_acquire(meta_request_test_results->response_headers);

    meta_request_test_results->headers_response_status = response_status;
}

static void s_test_s3_meta_request_body_callback(
    struct aws_s3_meta_request *meta_request,
    const struct aws_byte_cursor *body,
    uint64_t range_start,
    void *user_data) {
    (void)meta_request;
    (void)body;

    struct aws_s3_meta_request_test_results *meta_request_test_results = user_data;
    meta_request_test_results->received_body_size += body->len;

    AWS_LOGF_INFO(AWS_LS_S3_GENERAL, "Received range %" PRIu64 "-%" PRIu64, range_start, range_start + body->len - 1);
}

static void s_test_s3_meta_request_finish(
    struct aws_s3_meta_request *meta_request,
    const struct aws_s3_meta_request_result *result,
    void *user_data) {
    (void)meta_request;

    struct aws_s3_meta_request_test_results *meta_request_test_results =
        (struct aws_s3_meta_request_test_results *)user_data;
    struct aws_s3_tester *tester = (struct aws_s3_tester *)meta_request_test_results->tester;

    meta_request_test_results->error_response_headers = result->error_response_headers;

    if (result->error_response_headers != NULL) {
        aws_http_headers_acquire(result->error_response_headers);
    }

    if (result->error_response_body != NULL) {
        aws_byte_buf_init_copy(
            &meta_request_test_results->error_response_body, tester->allocator, result->error_response_body);
    }

    meta_request_test_results->finished_response_status = result->response_status;
    meta_request_test_results->finished_error_code = result->error_code;

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

    ASSERT_SUCCESS(aws_array_list_init_dynamic(
        &tester->client_vtable_patches, tester->allocator, 4, sizeof(struct aws_s3_client_vtable_patch)));

    ASSERT_SUCCESS(aws_array_list_init_dynamic(
        &tester->meta_request_vtable_patches, tester->allocator, 4, sizeof(struct aws_s3_meta_request_vtable_patch)));

    /* Setup an event loop group and host resolver. */
    tester->el_group = aws_event_loop_group_new_default(allocator, 0, NULL);
    ASSERT_TRUE(tester->el_group != NULL);

    struct aws_host_resolver_default_options resolver_options = {
        .max_entries = 10,
        .el_group = tester->el_group,
    };
    tester->host_resolver = aws_host_resolver_new_default(allocator, &resolver_options);
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

    ASSERT_TRUE(config->shutdown_callback == NULL);
    config->shutdown_callback = s_tester_notify_clean_up_signal;

    ASSERT_TRUE(config->shutdown_callback_user_data == NULL);
    config->shutdown_callback_user_data = tester;

    return AWS_OP_SUCCESS;
}

int aws_s3_tester_bind_meta_request(
    struct aws_s3_tester *tester,
    struct aws_s3_meta_request_options *options,
    struct aws_s3_meta_request_test_results *meta_request_test_results) {

    AWS_ZERO_STRUCT(*meta_request_test_results);
    meta_request_test_results->tester = tester;

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
    options->user_data = meta_request_test_results;

    return AWS_OP_SUCCESS;
}

void aws_s3_meta_request_test_results_clean_up(struct aws_s3_meta_request_test_results *test_meta_request) {
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

    bool notify = false;

    aws_s3_tester_lock_synced_data(tester);
    ++tester->synced_data.finish_count;

    int error_code = AWS_ERROR_SUCCESS;

    if (result != NULL) {
        error_code = result->error_code;
    }

    if (tester->synced_data.desired_finish_count == 0 ||
        tester->synced_data.finish_count == tester->synced_data.desired_finish_count ||
        (error_code != AWS_ERROR_SUCCESS)) {

        tester->synced_data.received_finish_callback = true;
        tester->synced_data.finish_error_code = error_code;

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

    aws_array_list_clean_up(&tester->client_vtable_patches);
    aws_array_list_clean_up(&tester->meta_request_vtable_patches);

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

struct aws_s3_client_vtable g_aws_s3_client_mock_vtable = {
    .schedule_meta_request_work = aws_s3_client_schedule_meta_request_work_empty,
    .sign_message = aws_s3_client_sign_request_empty,
    .get_http_connection = aws_s3_client_get_http_connection_empty,
};

struct aws_s3_client *aws_s3_tester_mock_client_new(struct aws_s3_tester *tester) {
    AWS_PRECONDITION(tester);

    struct aws_s3_client_config client_config = {
        .region = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("dummy_region"),
        .client_bootstrap = tester->client_bootstrap,
    };

    struct aws_s3_client *client = aws_s3_client_new(tester->allocator, &client_config);
    AWS_ASSERT(client);

    client->vtable = &g_aws_s3_client_mock_vtable;

    return client;
}

struct aws_http_message *aws_s3_tester_dummy_http_request_new(struct aws_s3_tester *tester) {
    AWS_PRECONDITION(tester);

    struct aws_http_message *message = aws_http_message_new_request(tester->allocator);

    struct aws_http_header host_header = {.name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Host"),
                                          .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("dummy_host")};

    aws_http_message_add_header(message, host_header);

    return message;
}

static void s_s3_empty_meta_request_destroy(struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);

    aws_mem_release(meta_request->allocator, meta_request->impl);
}

static struct aws_s3_meta_request_vtable s_s3_empty_meta_request_vtable = {
    .has_work = aws_s3_meta_request_has_work_empty,
    .next_request = aws_s3_meta_request_next_request_empty,
    .prepare_request = aws_s3_meta_request_prepare_request_empty,
    .incoming_headers = NULL,
    .incoming_headers_block_done = NULL,
    .incoming_body = NULL,
    .stream_complete = NULL,
    .destroy = s_s3_empty_meta_request_destroy,
};

struct aws_s3_empty_meta_request {
    struct aws_s3_meta_request base;
};

struct aws_s3_meta_request *aws_s3_tester_meta_request_new(
    struct aws_s3_tester *tester,
    struct aws_s3_meta_request_test_results *test_results,
    struct aws_s3_client *client) {
    AWS_PRECONDITION(tester);

    struct aws_s3_empty_meta_request *empty_meta_request =
        aws_mem_calloc(tester->allocator, 1, sizeof(struct aws_s3_empty_meta_request));

    struct aws_http_message *dummy_http_message = aws_s3_tester_dummy_http_request_new(tester);

    struct aws_s3_meta_request_options options = {
        .message = dummy_http_message,
    };

    if (test_results != NULL) {
        options.headers_callback = s_test_s3_meta_request_header_callback;
        options.body_callback = s_test_s3_meta_request_body_callback;
        options.finish_callback = s_test_s3_meta_request_finish;
        options.user_data = test_results;
    }

    bool release_client_ref = false;

    if (client == NULL) {
        client = aws_s3_tester_mock_client_new(tester);
        release_client_ref = true;
    }

    AWS_ASSERT(client);

    aws_s3_meta_request_init_base(
        tester->allocator,
        client,
        &options,
        empty_meta_request,
        &s_s3_empty_meta_request_vtable,
        &empty_meta_request->base);

    if (release_client_ref) {
        aws_s3_client_release(client);
    }

    aws_http_message_release(dummy_http_message);

    return &empty_meta_request->base;
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

struct aws_s3_client_vtable *aws_s3_tester_patch_client_vtable(
    struct aws_s3_tester *tester,
    struct aws_s3_client *client,
    size_t *out_index) {

    struct aws_s3_client_vtable_patch patch;
    AWS_ZERO_STRUCT(patch);

    /* Push a new vtable patch into the array. */
    aws_array_list_push_back(&tester->client_vtable_patches, (void *)&patch);

    /* Get a pointer to the new vtable patch. */
    size_t index = aws_array_list_length(&tester->client_vtable_patches) - 1;
    struct aws_s3_client_vtable_patch *patch_array_ptr = aws_s3_tester_get_client_vtable_patch(tester, index);

    /* Cache a pointer to the original vtable. */
    patch_array_ptr->original_vtable = client->vtable;

    /* Copy the original vtable contents into the patched vtable. */
    memcpy(&patch_array_ptr->patched_vtable, patch_array_ptr->original_vtable, sizeof(struct aws_s3_client_vtable));

    /* Point the client at the new vtable. */
    client->vtable = &patch_array_ptr->patched_vtable;

    if (out_index) {
        *out_index = index;
    }

    return &patch_array_ptr->patched_vtable;
}

struct aws_s3_client_vtable_patch *aws_s3_tester_get_client_vtable_patch(struct aws_s3_tester *tester, size_t index) {
    struct aws_s3_client_vtable_patch *patch = NULL;
    aws_array_list_get_at_ptr(&tester->client_vtable_patches, (void **)&patch, index);
    return patch;
}

struct aws_s3_meta_request_vtable *aws_s3_tester_patch_meta_request_vtable(
    struct aws_s3_tester *tester,
    struct aws_s3_meta_request *meta_request,
    size_t *out_index) {

    struct aws_s3_meta_request_vtable_patch patch;
    AWS_ZERO_STRUCT(patch);

    /* Push a new vtable patch into the array. */
    aws_array_list_push_back(&tester->meta_request_vtable_patches, (void *)&patch);

    /* Get a pointer to the new vtable patch. */
    size_t index = aws_array_list_length(&tester->meta_request_vtable_patches) - 1;
    struct aws_s3_meta_request_vtable_patch *patch_array_ptr =
        aws_s3_tester_get_meta_request_vtable_patch(tester, index);

    /* Cache a pointer to the original vtable. */
    patch_array_ptr->original_vtable = meta_request->vtable;

    /* Copy the original vtable contents into the patched vtable. */
    memcpy(
        &patch_array_ptr->patched_vtable, patch_array_ptr->original_vtable, sizeof(struct aws_s3_meta_request_vtable));

    /* Point the meta request at the new vtable. */
    meta_request->vtable = &patch_array_ptr->patched_vtable;

    if (out_index) {
        *out_index = index;
    }

    return &patch_array_ptr->patched_vtable;
}

struct aws_s3_meta_request_vtable_patch *aws_s3_tester_get_meta_request_vtable_patch(
    struct aws_s3_tester *tester,
    size_t index) {
    struct aws_s3_meta_request_vtable_patch *patch = NULL;
    aws_array_list_get_at_ptr(&tester->meta_request_vtable_patches, (void **)&patch, index);
    return patch;
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

int aws_s3_tester_send_get_object_meta_request(
    struct aws_s3_tester *tester,
    struct aws_s3_client *client,
    bool expect_success) {

    const struct aws_byte_cursor test_object_path = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("/get_object_test_1MB.txt");

    struct aws_string *host_name =
        aws_s3_tester_build_endpoint_string(tester->allocator, &g_test_bucket_name, &g_test_s3_region);

    /* Put together a simple S3 Get Object request. */
    struct aws_http_message *message = aws_s3_test_make_get_object_request(
        tester->allocator, aws_byte_cursor_from_string(host_name), test_object_path);

    struct aws_s3_meta_request_options options;
    AWS_ZERO_STRUCT(options);
    options.type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT;
    options.message = message;

    /* Trigger accelerating of our Get Object request. */
    struct aws_s3_meta_request_test_results meta_request_test_results;

    ASSERT_SUCCESS(aws_s3_tester_bind_meta_request(tester, &options, &meta_request_test_results));

    struct aws_s3_meta_request *meta_request = aws_s3_client_make_meta_request(client, &options);

    ASSERT_TRUE(meta_request != NULL);

    /* Wait for the request to finish. */
    aws_s3_tester_wait_for_finish(tester);

    aws_s3_tester_lock_synced_data(tester);

    if (expect_success) {
        ASSERT_TRUE(tester->synced_data.finish_error_code == AWS_ERROR_SUCCESS);
    } else {
        ASSERT_FALSE(tester->synced_data.finish_error_code == AWS_ERROR_SUCCESS);
    }

    aws_s3_tester_unlock_synced_data(tester);

    if (expect_success) {
        ASSERT_SUCCESS(aws_s3_tester_validate_get_object_results(&meta_request_test_results));
    }

    aws_s3_meta_request_release(meta_request);
    aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);

    aws_http_message_release(message);
    message = NULL;

    aws_string_destroy(host_name);
    host_name = NULL;

    return AWS_OP_SUCCESS;
}

int aws_s3_tester_validate_get_object_results(struct aws_s3_meta_request_test_results *meta_request_test_results) {
    AWS_PRECONDITION(meta_request_test_results);
    AWS_PRECONDITION(meta_request_test_results->tester);

    ASSERT_TRUE(meta_request_test_results->finished_response_status == 200);
    ASSERT_TRUE(
        meta_request_test_results->finished_response_status == meta_request_test_results->headers_response_status);
    ASSERT_TRUE(meta_request_test_results->finished_error_code == AWS_ERROR_SUCCESS);

    ASSERT_TRUE(meta_request_test_results->error_response_headers == NULL);
    ASSERT_TRUE(meta_request_test_results->error_response_body.len == 0);

    ASSERT_FALSE(
        aws_http_headers_has(meta_request_test_results->response_headers, aws_byte_cursor_from_c_str("Content-Range")));

    struct aws_s3_tester *tester = meta_request_test_results->tester;

    struct aws_byte_cursor content_length_cursor;
    AWS_ZERO_STRUCT(content_length_cursor);
    ASSERT_SUCCESS(aws_http_headers_get(
        meta_request_test_results->response_headers,
        aws_byte_cursor_from_c_str("Content-Length"),
        &content_length_cursor));

    struct aws_string *content_length_str = aws_string_new_from_cursor(tester->allocator, &content_length_cursor);

    char *content_length_str_end = NULL;
    uint64_t content_length = strtoull((const char *)content_length_str->bytes, &content_length_str_end, 10);

    aws_string_destroy(content_length_str);

    AWS_LOGF_DEBUG(
        AWS_LS_S3_GENERAL,
        "Content length in header is %" PRIu64 " and received body size is %" PRIu64,
        content_length,
        meta_request_test_results->received_body_size);

    ASSERT_TRUE(content_length == meta_request_test_results->received_body_size);

    return AWS_OP_SUCCESS;
}

int aws_s3_tester_send_put_object_meta_request(
    struct aws_s3_tester *tester,
    struct aws_s3_client *client,
    bool expect_success) {
    ASSERT_TRUE(tester != NULL);
    ASSERT_TRUE(client != NULL);

    const struct aws_byte_cursor test_object_path = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("/put_object_test_10MB.txt");

    struct aws_allocator *allocator = tester->allocator;

    struct aws_byte_buf test_buffer;
    aws_s3_create_test_buffer(allocator, 10 * 1024 * 1024, &test_buffer);

    struct aws_byte_cursor test_body_cursor = aws_byte_cursor_from_buf(&test_buffer);
    struct aws_input_stream *input_stream = aws_input_stream_new_from_cursor(allocator, &test_body_cursor);

    struct aws_string *host_name =
        aws_s3_tester_build_endpoint_string(allocator, &g_test_bucket_name, &g_test_s3_region);

    /* Put together a simple S3 Put Object request. */
    struct aws_http_message *message = aws_s3_test_make_put_object_request(
        allocator, aws_byte_cursor_from_string(host_name), test_object_path, g_test_body_content_type, input_stream);

    struct aws_s3_meta_request_options options;
    AWS_ZERO_STRUCT(options);
    options.type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT;
    options.message = message;

    struct aws_s3_meta_request_test_results meta_request_test_results;
    ASSERT_SUCCESS(aws_s3_tester_bind_meta_request(tester, &options, &meta_request_test_results));

    /* Wait for the request to finish. */
    struct aws_s3_meta_request *meta_request = aws_s3_client_make_meta_request(client, &options);

    ASSERT_TRUE(meta_request != NULL);

    /* Wait for the request to finish. */
    aws_s3_tester_wait_for_finish(tester);

    aws_s3_tester_lock_synced_data(tester);

    if (expect_success) {
        ASSERT_TRUE(tester->synced_data.finish_error_code == AWS_ERROR_SUCCESS);
    } else {
        ASSERT_FALSE(tester->synced_data.finish_error_code == AWS_ERROR_SUCCESS);
    }

    aws_s3_tester_unlock_synced_data(tester);

    if (expect_success) {
        ASSERT_SUCCESS(aws_s3_tester_validate_put_object_results(&meta_request_test_results));
    }

    aws_s3_meta_request_release(meta_request);

    aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);

    aws_http_message_release(message);
    message = NULL;

    aws_string_destroy(host_name);
    host_name = NULL;

    aws_input_stream_destroy(input_stream);
    input_stream = NULL;

    aws_byte_buf_clean_up(&test_buffer);

    return AWS_OP_SUCCESS;
}

int aws_s3_tester_validate_put_object_results(struct aws_s3_meta_request_test_results *meta_request_test_results) {
    ASSERT_TRUE(meta_request_test_results->finished_response_status == 200);
    ASSERT_TRUE(
        meta_request_test_results->finished_response_status == meta_request_test_results->headers_response_status);
    ASSERT_TRUE(meta_request_test_results->finished_error_code == AWS_ERROR_SUCCESS);

    ASSERT_TRUE(meta_request_test_results->error_response_headers == NULL);
    ASSERT_TRUE(meta_request_test_results->error_response_body.len == 0);

    struct aws_byte_cursor etag_byte_cursor;
    AWS_ZERO_STRUCT(etag_byte_cursor);
    ASSERT_SUCCESS(aws_http_headers_get(
        meta_request_test_results->response_headers, aws_byte_cursor_from_c_str("ETag"), &etag_byte_cursor));
    ASSERT_TRUE(etag_byte_cursor.len > 0);

    return AWS_OP_SUCCESS;
}

void aws_s3_client_schedule_meta_request_work_empty(
    struct aws_s3_client *client,
    struct aws_s3_meta_request *meta_request) {
    (void)client;
    (void)meta_request;
}

int aws_s3_client_sign_request_empty(
    struct aws_s3_client *client,
    struct aws_s3_request *request,
    aws_s3_client_sign_callback *callback,
    void *user_data) {
    (void)client;
    (void)request;
    (void)callback;
    (void)user_data;
    return AWS_OP_SUCCESS;
}

int aws_s3_client_get_http_connection_empty(
    struct aws_s3_client *client,
    struct aws_s3_vip_connection *vip_connection,
    aws_s3_client_get_http_connection_callback *callback,
    void *user_data) {
    (void)client;
    (void)vip_connection;
    (void)callback;
    (void)user_data;
    return AWS_OP_SUCCESS;
}

bool aws_s3_meta_request_has_work_empty(const struct aws_s3_meta_request *meta_request) {
    (void)meta_request;
    return false;
}

int aws_s3_meta_request_next_request_empty(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request **out_request) {
    (void)meta_request;
    (void)out_request;
    return AWS_OP_ERR;
}

int aws_s3_meta_request_prepare_request_empty(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_client *client,
    struct aws_s3_request *request) {
    (void)meta_request;
    (void)client;
    (void)request;
    return AWS_OP_ERR;
}
