/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_client_impl.h"
#include "aws/s3/private/s3_meta_request_impl.h"
#include "aws/s3/private/s3_util.h"
#include "s3_tester.h"
#include <aws/common/atomics.h>
#include <aws/common/byte_buf.h>
#include <aws/common/clock.h>
#include <aws/common/common.h>
#include <aws/common/ref_count.h>
#include <aws/http/connection_manager.h>
#include <aws/http/request_response.h>
#include <aws/io/stream.h>
#include <aws/io/tls_channel_handler.h>
#include <aws/testing/aws_test_harness.h>
#include <inttypes.h>

static void s_s3_client_acquire_http_connection_exceed_retries(
    struct aws_http_connection_manager *conn_manager,
    aws_http_connection_manager_on_connection_setup_fn *callback,
    void *user_data) {
    AWS_ASSERT(callback);
    (void)conn_manager;

    aws_raise_error(AWS_ERROR_HTTP_UNKNOWN);
    callback(NULL, AWS_ERROR_HTTP_UNKNOWN, user_data);
}

AWS_TEST_CASE(test_s3_client_exceed_retries, s_test_s3_client_exceed_retries)
static int s_test_s3_client_exceed_retries(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client_config client_config;
    AWS_ZERO_STRUCT(client_config);

    ASSERT_SUCCESS(aws_s3_tester_bind_client(
        &tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION | AWS_S3_TESTER_BIND_CLIENT_SIGNING));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    struct aws_s3_client_vtable *patched_client_vtable = aws_s3_tester_patch_client_vtable(&tester, client, NULL);
    patched_client_vtable->acquire_http_connection = s_s3_client_acquire_http_connection_exceed_retries;

    struct aws_s3_meta_request_test_results meta_request_test_results;
    aws_s3_meta_request_test_results_init(&meta_request_test_results, allocator);

    /* Don't specify EXPECT SUCCESS flag for aws_s3_tester_send_get_object_meta_request to expect a failure. */
    ASSERT_SUCCESS(aws_s3_tester_send_get_object_meta_request(
        &tester, client, g_pre_existing_object_1MB, 0, &meta_request_test_results));

    ASSERT_TRUE(meta_request_test_results.finished_error_code == AWS_ERROR_HTTP_UNKNOWN);

    aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);

    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return 0;
}

static void s_s3_client_acquire_http_connection_fail_first(
    struct aws_http_connection_manager *conn_manager,
    aws_http_connection_manager_on_connection_setup_fn *callback,
    void *user_data) {
    AWS_ASSERT(callback);

    struct aws_s3_connection *connection = user_data;

    struct aws_s3_client *client = connection->request->meta_request->endpoint->client;
    AWS_ASSERT(client);

    struct aws_s3_tester *tester = client->shutdown_callback_user_data;
    AWS_ASSERT(tester != NULL);

    if (aws_s3_tester_inc_counter1(tester) == 1) {
        aws_raise_error(AWS_ERROR_UNKNOWN);
        callback(NULL, AWS_ERROR_UNKNOWN, connection);
        return;
    }

    struct aws_s3_client_vtable *original_client_vtable =
        aws_s3_tester_get_client_vtable_patch(tester, 0)->original_vtable;

    original_client_vtable->acquire_http_connection(conn_manager, callback, user_data);
}

AWS_TEST_CASE(test_s3_client_acquire_connection_fail, s_test_s3_client_acquire_connection_fail)
static int s_test_s3_client_acquire_connection_fail(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client_config client_config = {.part_size = 64 * 1024};

    ASSERT_SUCCESS(aws_s3_tester_bind_client(
        &tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION | AWS_S3_TESTER_BIND_CLIENT_SIGNING));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    struct aws_s3_client_vtable *patched_client_vtable = aws_s3_tester_patch_client_vtable(&tester, client, NULL);
    patched_client_vtable->acquire_http_connection = s_s3_client_acquire_http_connection_fail_first;

    ASSERT_SUCCESS(aws_s3_tester_send_get_object_meta_request(
        &tester, client, g_pre_existing_object_1MB, AWS_S3_TESTER_SEND_META_REQUEST_EXPECT_SUCCESS, NULL));

    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return 0;
}

struct s3_fail_prepare_test_data {
    uint32_t num_requests_being_prepared_is_correct : 1;
};

static struct aws_future_void *s_s3_fail_prepare_request(struct aws_s3_request *request) {
    AWS_ASSERT(request != NULL);
    struct aws_future_void *future = aws_future_void_new(request->allocator);
    aws_future_void_set_error(future, AWS_ERROR_UNKNOWN);
    return future;
}

static struct aws_s3_meta_request *s_meta_request_factory_patch_prepare_request(
    struct aws_s3_client *client,
    const struct aws_s3_meta_request_options *options) {

    AWS_ASSERT(client != NULL);

    struct aws_s3_tester *tester = client->shutdown_callback_user_data;
    AWS_ASSERT(tester != NULL);

    struct aws_s3_client_vtable *original_client_vtable =
        aws_s3_tester_get_client_vtable_patch(tester, 0)->original_vtable;

    struct aws_s3_meta_request *meta_request = original_client_vtable->meta_request_factory(client, options);

    struct aws_s3_meta_request_vtable *patched_meta_request_vtable =
        aws_s3_tester_patch_meta_request_vtable(tester, meta_request, NULL);
    patched_meta_request_vtable->prepare_request = s_s3_fail_prepare_request;

    return meta_request;
}

static void s_s3_fail_prepare_finish_destroy(struct aws_s3_client *client) {
    AWS_ASSERT(client);

    struct aws_s3_tester *tester = client->shutdown_callback_user_data;
    AWS_ASSERT(tester != NULL);

    struct s3_fail_prepare_test_data *test_data = tester->user_data;
    AWS_ASSERT(test_data != NULL);

    test_data->num_requests_being_prepared_is_correct = client->threaded_data.num_requests_being_prepared == 0;

    struct aws_s3_client_vtable *original_client_vtable =
        aws_s3_tester_get_client_vtable_patch(tester, 0)->original_vtable;

    original_client_vtable->finish_destroy(client);
}

/* Test recovery when prepare request fails. */
AWS_TEST_CASE(test_s3_meta_request_fail_prepare_request, s_test_s3_meta_request_fail_prepare_request)
static int s_test_s3_meta_request_fail_prepare_request(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct s3_fail_prepare_test_data test_data;
    AWS_ZERO_STRUCT(test_data);
    tester.user_data = &test_data;

    struct aws_s3_client_config client_config;
    AWS_ZERO_STRUCT(client_config);

    ASSERT_SUCCESS(aws_s3_tester_bind_client(
        &tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION | AWS_S3_TESTER_BIND_CLIENT_SIGNING));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);
    struct aws_s3_client_vtable *patched_client_vtable = aws_s3_tester_patch_client_vtable(&tester, client, NULL);
    patched_client_vtable->meta_request_factory = s_meta_request_factory_patch_prepare_request;
    patched_client_vtable->finish_destroy = s_s3_fail_prepare_finish_destroy;

    ASSERT_SUCCESS(aws_s3_tester_send_get_object_meta_request(&tester, client, g_pre_existing_object_1MB, 0, NULL));

    aws_s3_tester_wait_for_counters(&tester);

    client = aws_s3_client_release(client);

    aws_s3_tester_clean_up(&tester);

    ASSERT_TRUE(test_data.num_requests_being_prepared_is_correct);

    return 0;
}

static void s_s3_meta_request_sign_request_fail(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    aws_signing_complete_fn *on_signing_complete,
    void *user_data) {
    (void)meta_request;
    (void)request;

    AWS_ASSERT(on_signing_complete != NULL);

    aws_raise_error(AWS_ERROR_UNKNOWN);
    on_signing_complete(NULL, AWS_ERROR_UNKNOWN, user_data);
}

static struct aws_s3_meta_request *s_s3_meta_request_factory_sign_request(
    struct aws_s3_client *client,
    const struct aws_s3_meta_request_options *options) {
    AWS_ASSERT(client != NULL);

    struct aws_s3_tester *tester = client->shutdown_callback_user_data;
    AWS_ASSERT(tester != NULL);

    struct aws_s3_client_vtable *original_client_vtable =
        aws_s3_tester_get_client_vtable_patch(tester, 0)->original_vtable;

    struct aws_s3_meta_request *meta_request = original_client_vtable->meta_request_factory(client, options);

    struct aws_s3_meta_request_vtable *patched_meta_request_vtable =
        aws_s3_tester_patch_meta_request_vtable(tester, meta_request, NULL);
    patched_meta_request_vtable->sign_request = s_s3_meta_request_sign_request_fail;

    return meta_request;
}

AWS_TEST_CASE(test_s3_meta_request_sign_request_fail, s_test_s3_meta_request_sign_request_fail)
static int s_test_s3_meta_request_sign_request_fail(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client *client = NULL;
    struct aws_s3_tester_client_options client_options;
    AWS_ZERO_STRUCT(client_options);

    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));
    struct aws_s3_client_vtable *patched_client_vtable = aws_s3_tester_patch_client_vtable(&tester, client, NULL);
    patched_client_vtable->meta_request_factory = s_s3_meta_request_factory_sign_request;

    struct aws_s3_meta_request_test_results meta_request_test_results;
    aws_s3_meta_request_test_results_init(&meta_request_test_results, allocator);

    struct aws_s3_tester_meta_request_options options = {
        .allocator = allocator,
        .client = client,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE,
        .get_options =
            {
                .object_path = g_pre_existing_object_1MB,
            },
    };

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &options, &meta_request_test_results));
    ASSERT_TRUE(meta_request_test_results.finished_error_code == AWS_ERROR_UNKNOWN);

    aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return 0;
}

struct s3_meta_request_prepare_request_fail_first_job {
    struct aws_allocator *allocator;
    struct aws_s3_request *request;
    struct aws_future_void *original_future; /* original future that we're intercepting and patching */
    struct aws_future_void *patched_future;  /* patched future to set when this job completes */
};

static void s_s3_meta_request_prepare_request_fail_first_on_original_done(void *user_data);

static struct aws_future_void *s_s3_meta_request_prepare_request_fail_first(struct aws_s3_request *request) {

    struct aws_s3_meta_request *meta_request = request->meta_request;
    AWS_ASSERT(meta_request);

    struct aws_s3_client *client = meta_request->client;
    AWS_ASSERT(client != NULL);

    struct aws_s3_tester *tester = client->shutdown_callback_user_data;
    AWS_ASSERT(tester != NULL);

    struct aws_future_void *patched_future = aws_future_void_new(request->allocator);

    struct s3_meta_request_prepare_request_fail_first_job *patched_prep =
        aws_mem_calloc(request->allocator, 1, sizeof(struct s3_meta_request_prepare_request_fail_first_job));

    patched_prep->allocator = request->allocator;
    patched_prep->patched_future = aws_future_void_acquire(patched_future);
    patched_prep->request = request;

    struct aws_s3_meta_request_vtable *original_meta_request_vtable =
        aws_s3_tester_get_meta_request_vtable_patch(tester, 0)->original_vtable;

    patched_prep->original_future = original_meta_request_vtable->prepare_request(request);
    aws_future_void_register_callback(
        patched_prep->original_future, s_s3_meta_request_prepare_request_fail_first_on_original_done, patched_prep);

    return patched_future;
}

static void s_s3_meta_request_prepare_request_fail_first_on_original_done(void *user_data) {

    struct s3_meta_request_prepare_request_fail_first_job *patched_prep = user_data;
    struct aws_s3_request *request = patched_prep->request;
    struct aws_s3_tester *tester = request->meta_request->client->shutdown_callback_user_data;

    int error_code = aws_future_void_get_error(patched_prep->original_future);
    if (error_code != AWS_ERROR_SUCCESS) {
        aws_future_void_set_error(patched_prep->patched_future, error_code);
        goto finish;
    }

    if (aws_s3_tester_inc_counter1(tester) == 1) {

        const struct aws_byte_cursor test_object_path =
            AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("/non-existing-file12345.txt");

        int set_request_path_result = aws_http_message_set_request_path(request->send_data.message, test_object_path);
        AWS_ASSERT(set_request_path_result == AWS_ERROR_SUCCESS);
        (void)set_request_path_result;
    }

    aws_future_void_set_result(patched_prep->patched_future);
finish:
    aws_future_void_release(patched_prep->original_future);
    aws_future_void_release(patched_prep->patched_future);
    aws_mem_release(patched_prep->allocator, patched_prep);
}

static void s_s3_meta_request_send_request_finish_fail_first(
    struct aws_s3_connection *connection,
    struct aws_http_stream *stream,
    int error_code) {

    struct aws_s3_client *client = connection->request->meta_request->client;
    AWS_ASSERT(client != NULL);

    struct aws_s3_tester *tester = client->shutdown_callback_user_data;
    AWS_ASSERT(tester != NULL);

    if (aws_s3_tester_inc_counter2(tester) == 1) {
        AWS_ASSERT(connection->request->send_data.response_status == 404);

        connection->request->send_data.response_status = AWS_S3_RESPONSE_STATUS_INTERNAL_ERROR;
    }

    struct aws_s3_meta_request_vtable *original_meta_request_vtable =
        aws_s3_tester_get_meta_request_vtable_patch(tester, 0)->original_vtable;

    original_meta_request_vtable->send_request_finish(connection, stream, error_code);
}

static struct aws_s3_meta_request *s_meta_request_factory_patch_send_request_finish(
    struct aws_s3_client *client,
    const struct aws_s3_meta_request_options *options) {

    struct aws_s3_tester *tester = client->shutdown_callback_user_data;
    AWS_ASSERT(tester != NULL);

    struct aws_s3_client_vtable *original_client_vtable =
        aws_s3_tester_get_client_vtable_patch(tester, 0)->original_vtable;

    struct aws_s3_meta_request *meta_request = original_client_vtable->meta_request_factory(client, options);

    struct aws_s3_meta_request_vtable *patched_meta_request_vtable =
        aws_s3_tester_patch_meta_request_vtable(tester, meta_request, NULL);
    patched_meta_request_vtable->prepare_request = s_s3_meta_request_prepare_request_fail_first;
    patched_meta_request_vtable->send_request_finish = s_s3_meta_request_send_request_finish_fail_first;

    return meta_request;
}

/* Test recovery when message response indicates an internal error. */
AWS_TEST_CASE(test_s3_meta_request_send_request_finish_fail, s_test_s3_meta_request_send_request_finish_fail)
static int s_test_s3_meta_request_send_request_finish_fail(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client_config client_config = {
        .part_size = 64 * 1024,
    };

    ASSERT_SUCCESS(aws_s3_tester_bind_client(
        &tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION | AWS_S3_TESTER_BIND_CLIENT_SIGNING));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);
    struct aws_s3_client_vtable *patched_client_vtable = aws_s3_tester_patch_client_vtable(&tester, client, NULL);
    patched_client_vtable->meta_request_factory = s_meta_request_factory_patch_send_request_finish;

    ASSERT_SUCCESS(aws_s3_tester_send_get_object_meta_request(
        &tester, client, g_pre_existing_object_1MB, AWS_S3_TESTER_SEND_META_REQUEST_EXPECT_SUCCESS, NULL));

    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return 0;
}

static void s_finished_request_remove_upload_id(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    int error_code) {
    (void)error_code;

    if (request->request_tag == AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_CREATE_MULTIPART_UPLOAD) {
        aws_byte_buf_reset(&request->send_data.response_body, false);
    }

    struct aws_s3_client *client = meta_request->client;
    struct aws_s3_tester *tester = client->shutdown_callback_user_data;

    struct aws_s3_meta_request_vtable *original_meta_request_vtable =
        aws_s3_tester_get_meta_request_vtable_patch(tester, 0)->original_vtable;

    original_meta_request_vtable->finished_request(meta_request, request, error_code);
}

static struct aws_s3_meta_request *s_meta_request_factory_patch_finished_request(
    struct aws_s3_client *client,
    const struct aws_s3_meta_request_options *options) {
    AWS_ASSERT(client != NULL);

    struct aws_s3_tester *tester = client->shutdown_callback_user_data;
    AWS_ASSERT(tester != NULL);

    struct aws_s3_client_vtable *original_client_vtable =
        aws_s3_tester_get_client_vtable_patch(tester, 0)->original_vtable;

    struct aws_s3_meta_request *meta_request = original_client_vtable->meta_request_factory(client, options);

    struct aws_s3_meta_request_vtable *patched_meta_request_vtable =
        aws_s3_tester_patch_meta_request_vtable(tester, meta_request, NULL);
    patched_meta_request_vtable->finished_request = s_finished_request_remove_upload_id;

    return meta_request;
}

AWS_TEST_CASE(test_s3_auto_range_put_missing_upload_id, s_test_s3_auto_range_put_missing_upload_id)
static int s_test_s3_auto_range_put_missing_upload_id(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client *client = NULL;
    struct aws_s3_tester_client_options client_options;
    AWS_ZERO_STRUCT(client_options);

    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));
    struct aws_s3_client_vtable *patched_client_vtable = aws_s3_tester_patch_client_vtable(&tester, client, NULL);
    patched_client_vtable->meta_request_factory = s_meta_request_factory_patch_finished_request;

    struct aws_s3_meta_request_test_results meta_request_test_results;
    aws_s3_meta_request_test_results_init(&meta_request_test_results, allocator);

    struct aws_s3_tester_meta_request_options options = {
        .allocator = allocator,
        .client = client,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE,
        .put_options =
            {
                .ensure_multipart = true,
            },
    };

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &options, &meta_request_test_results));
    ASSERT_TRUE(meta_request_test_results.finished_error_code == AWS_ERROR_S3_MISSING_UPLOAD_ID);

    aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);

    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}
