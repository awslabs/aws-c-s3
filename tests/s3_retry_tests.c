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
#include <aws/http/request_response.h>
#include <aws/io/stream.h>
#include <aws/testing/aws_test_harness.h>
#include <inttypes.h>

struct s3_retry_test_data {
    struct aws_atomic_var counter1;
    struct aws_atomic_var counter2;
};

static void s_s3_retry_test_data_init(struct s3_retry_test_data *data) {
    AWS_ASSERT(data != NULL);
    AWS_ZERO_STRUCT(*data);
    aws_atomic_init_int(&data->counter1, 0);
    aws_atomic_init_int(&data->counter2, 0);
}

static void s_s3_retry_test_data_clean_up(struct s3_retry_test_data *data) {
    AWS_ASSERT(data != NULL);
    AWS_ZERO_STRUCT(*data);
}

static size_t s_s3_retry_test_data_inc_counter1(struct s3_retry_test_data *data) {
    AWS_ASSERT(data != NULL);
    return aws_atomic_fetch_add(&data->counter1, 1);
}

static size_t s_s3_retry_test_data_inc_counter2(struct s3_retry_test_data *data) {
    AWS_ASSERT(data != NULL);
    return aws_atomic_fetch_add(&data->counter2, 1);
}

static void wait_for_retry_queue_not_empty(struct aws_s3_meta_request *meta_request) {
    AWS_ASSERT(meta_request != NULL);
    bool waiting = true;

    while (waiting) {
        aws_s3_meta_request_lock_synced_data(meta_request);

        waiting = aws_linked_list_empty(&meta_request->synced_data.retry_queue);

        aws_s3_meta_request_unlock_synced_data(meta_request);

        if (waiting) {
            aws_thread_current_sleep(100);
        }
    }
}

AWS_TEST_CASE(test_s3_meta_request_retry_queue_operations, s_test_s3_meta_request_retry_queue_operations)
static int s_test_s3_meta_request_retry_queue_operations(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    const int request_tag = 1234;
    const uint32_t part_number = 5678;

    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_meta_request *meta_request = aws_s3_tester_meta_request_new(&tester, NULL, NULL);
    ASSERT_TRUE(meta_request != NULL);

    struct aws_http_message *request_message = aws_s3_tester_dummy_http_request_new(&tester);
    ASSERT_TRUE(request_message != NULL);

    struct aws_s3_request *request =
        aws_s3_request_new(meta_request, request_tag, part_number, AWS_S3_REQUEST_DESC_RECORD_RESPONSE_HEADERS);
    ASSERT_TRUE(request != NULL);

    /* Make sure the request is set up right and nothing is currently in the queue. */
    ASSERT_TRUE(request->meta_request == meta_request);
    ASSERT_TRUE(aws_linked_list_empty(&meta_request->synced_data.retry_queue));

    /* Queue the request. */
    aws_s3_meta_request_retry_queue_push(meta_request, request);

    /* Retry queue doesn't hold onto the meta request reference when its in the queue so that clean up isn't blocked. */
    ASSERT_TRUE(request->meta_request == NULL);

    /* Make sure the request in the queue is equal to the request we pushed. */
    struct aws_linked_list_node *node = aws_linked_list_begin(&meta_request->synced_data.retry_queue);
    struct aws_s3_request *node_request = AWS_CONTAINER_OF(node, struct aws_s3_request, node);
    ASSERT_TRUE(node_request == request);

    /* Pop the request */
    aws_s3_meta_request_lock_synced_data(meta_request);

    struct aws_s3_request *popped_request = aws_s3_meta_request_retry_queue_pop_synced(meta_request);
    ASSERT_TRUE(request == popped_request);

    aws_s3_meta_request_unlock_synced_data(meta_request);

    /* Release the reference to the request that the queue gave us. */
    aws_s3_request_release(popped_request);
    popped_request = NULL;

    /* Make the sure the queue is empty, and the request now points to the meta request again.*/
    ASSERT_TRUE(aws_linked_list_empty(&meta_request->synced_data.retry_queue));
    ASSERT_TRUE(request->meta_request == meta_request);

    aws_s3_request_release(request);
    aws_http_message_release(request_message);
    aws_s3_meta_request_release(meta_request);

    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(test_s3_meta_request_retry_queue_clean_up, s_test_s3_meta_request_retry_queue_clean_up)
static int s_test_s3_meta_request_retry_queue_clean_up(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    const int request_tag = 1234;
    const uint32_t part_number = 5678;

    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_meta_request *meta_request = aws_s3_tester_meta_request_new(&tester, NULL, NULL);
    ASSERT_TRUE(meta_request != NULL);

    struct aws_http_message *request_message = aws_s3_tester_dummy_http_request_new(&tester);
    ASSERT_TRUE(request_message != NULL);

    struct aws_s3_request *request =
        aws_s3_request_new(meta_request, request_tag, part_number, AWS_S3_REQUEST_DESC_RECORD_RESPONSE_HEADERS);
    ASSERT_TRUE(request != NULL);

    /* Make sure the request is set up right and nothing is currently in the queue. */
    ASSERT_TRUE(request->meta_request == meta_request);
    ASSERT_TRUE(aws_linked_list_empty(&meta_request->synced_data.retry_queue));

    /* Queue the request. */
    aws_s3_meta_request_retry_queue_push(meta_request, request);

    aws_s3_request_release(request);

    /* Retry queue doesn't hold onto the meta request reference when its in the queue so that clean up isn't blocked. */
    ASSERT_TRUE(request->meta_request == NULL);

    /* Make sure the request in the queue is equal to the request we pushed. */
    struct aws_linked_list_node *node = aws_linked_list_begin(&meta_request->synced_data.retry_queue);
    struct aws_s3_request *node_request = AWS_CONTAINER_OF(node, struct aws_s3_request, node);
    ASSERT_TRUE(node_request == request);

    /* Clean everything up with the request still in the queue. */
    aws_http_message_release(request_message);
    aws_s3_meta_request_release(meta_request);

    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(test_s3_meta_request_handle_error_retry, s_test_s3_meta_request_handle_error_retry)
static int s_test_s3_meta_request_handle_error_retry(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    const int request_tag = 1234;
    const uint32_t part_number = 5678;

    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_meta_request *meta_request = aws_s3_tester_meta_request_new(&tester, NULL, NULL);
    ASSERT_TRUE(meta_request != NULL);

    struct aws_http_message *request_message = aws_s3_tester_dummy_http_request_new(&tester);
    ASSERT_TRUE(request_message != NULL);

    struct aws_s3_request *request =
        aws_s3_request_new(meta_request, request_tag, part_number, AWS_S3_REQUEST_DESC_RECORD_RESPONSE_HEADERS);
    ASSERT_TRUE(request != NULL);

    ASSERT_TRUE(request->retry_token == NULL);

    {
        aws_s3_meta_request_handle_error(meta_request, request, AWS_ERROR_S3_INTERNAL_ERROR);
        aws_s3_request_release(request);

        wait_for_retry_queue_not_empty(meta_request);

        aws_s3_meta_request_lock_synced_data(meta_request);
        struct aws_s3_request *queued_request = aws_s3_meta_request_retry_queue_pop_synced(meta_request);
        ASSERT_TRUE(queued_request == request);
        aws_s3_meta_request_unlock_synced_data(meta_request);
    }

    ASSERT_TRUE(request->retry_token != NULL);

    {
        aws_s3_meta_request_handle_error(meta_request, request, AWS_ERROR_S3_INTERNAL_ERROR);
        aws_s3_request_release(request);

        wait_for_retry_queue_not_empty(meta_request);

        aws_s3_meta_request_lock_synced_data(meta_request);
        struct aws_s3_request *queued_request = aws_s3_meta_request_retry_queue_pop_synced(meta_request);
        ASSERT_TRUE(queued_request == request);
        aws_s3_meta_request_unlock_synced_data(meta_request);
    }

    aws_s3_request_release(request);
    aws_http_message_release(request_message);
    aws_s3_meta_request_release(meta_request);

    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(test_s3_meta_request_handle_error_exceed_retries, s_test_s3_meta_request_handle_error_exceed_retries)
static int s_test_s3_meta_request_handle_error_exceed_retries(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    const int request_tag = 1234;
    const uint32_t part_number = 5678;

    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client_config client_config = {.region = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("dummy_region"),
                                                 .credentials_provider = tester.credentials_provider,
                                                 .client_bootstrap = tester.client_bootstrap,
                                                 .max_retries = 4};

    struct aws_s3_client *client = aws_s3_client_new(tester.allocator, &client_config);
    AWS_ASSERT(client);
    client->vtable = &g_aws_s3_client_mock_vtable;

    struct aws_s3_meta_request *meta_request = aws_s3_tester_meta_request_new(&tester, NULL, client);
    ASSERT_TRUE(meta_request != NULL);

    struct aws_http_message *request_message = aws_s3_tester_dummy_http_request_new(&tester);
    ASSERT_TRUE(request_message != NULL);

    struct aws_s3_request *request =
        aws_s3_request_new(meta_request, request_tag, part_number, AWS_S3_REQUEST_DESC_RECORD_RESPONSE_HEADERS);
    ASSERT_TRUE(request != NULL);

    ASSERT_TRUE(request->retry_token == NULL);

    bool finished = false;

    while (!finished) {
        aws_s3_meta_request_handle_error(meta_request, request, AWS_ERROR_S3_INTERNAL_ERROR);

        bool exists_in_retry_queue = false;

        while (!exists_in_retry_queue && !finished) {
            aws_s3_meta_request_lock_synced_data(meta_request);
            exists_in_retry_queue = !aws_linked_list_empty(&meta_request->synced_data.retry_queue);
            finished = meta_request->synced_data.state == AWS_S3_META_REQUEST_STATE_FINISHED;
            aws_s3_meta_request_unlock_synced_data(meta_request);

            if (!exists_in_retry_queue && !finished) {
                aws_thread_current_sleep(100);
            }
        }

        if (exists_in_retry_queue) {
            aws_s3_meta_request_lock_synced_data(meta_request);
            aws_s3_meta_request_retry_queue_pop_synced(meta_request);
            aws_s3_request_release(request);
            aws_s3_meta_request_unlock_synced_data(meta_request);
        }
    }

    ASSERT_TRUE(aws_last_error() == AWS_IO_MAX_RETRIES_EXCEEDED);
    ASSERT_TRUE(request->retry_token != NULL);

    aws_s3_request_release(request);
    aws_http_message_release(request_message);
    aws_s3_meta_request_release(meta_request);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(test_s3_meta_request_handle_error_fail, s_test_s3_meta_request_handle_error_fail)
static int s_test_s3_meta_request_handle_error_fail(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    const int request_tag = 1234;
    const uint32_t part_number = 5678;

    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    /* Test failure with error code and no request passed in. */
    {
        struct aws_s3_meta_request_test_results meta_request_test_results = {
            .tester = &tester,
        };

        struct aws_s3_meta_request *meta_request =
            aws_s3_tester_meta_request_new(&tester, &meta_request_test_results, NULL);
        ASSERT_TRUE(meta_request != NULL);

        struct aws_http_message *request_message = aws_s3_tester_dummy_http_request_new(&tester);
        ASSERT_TRUE(request_message != NULL);

        aws_s3_meta_request_handle_error(meta_request, NULL, AWS_ERROR_UNKNOWN);
        ASSERT_TRUE(meta_request_test_results.finished_error_code == AWS_ERROR_UNKNOWN);

        aws_http_message_release(request_message);
        aws_s3_meta_request_release(meta_request);
    }

    /* Test failure with request that has invalid response code. */
    {
        struct aws_s3_meta_request_test_results meta_request_test_results = {
            .tester = &tester,
        };

        struct aws_s3_meta_request *meta_request =
            aws_s3_tester_meta_request_new(&tester, &meta_request_test_results, NULL);
        ASSERT_TRUE(meta_request != NULL);

        struct aws_http_message *request_message = aws_s3_tester_dummy_http_request_new(&tester);
        ASSERT_TRUE(request_message != NULL);

        struct aws_s3_request *request =
            aws_s3_request_new(meta_request, request_tag, part_number, AWS_S3_REQUEST_DESC_RECORD_RESPONSE_HEADERS);
        ASSERT_TRUE(request != NULL);

        request->send_data.response_status = 404;

        aws_s3_meta_request_handle_error(meta_request, request, AWS_ERROR_S3_INVALID_RESPONSE_STATUS);
        ASSERT_TRUE(meta_request_test_results.finished_error_code == AWS_ERROR_S3_INVALID_RESPONSE_STATUS);
        ASSERT_TRUE(meta_request_test_results.finished_response_status == request->send_data.response_status);

        aws_s3_request_release(request);
        aws_http_message_release(request_message);
        aws_s3_meta_request_release(meta_request);
    }

    aws_s3_tester_clean_up(&tester);

    return 0;
}

static int s_s3_fail_first_next_request(struct aws_s3_meta_request *meta_request, struct aws_s3_request **out_request) {
    AWS_ASSERT(meta_request != NULL);

    struct aws_s3_client *client = aws_s3_meta_request_get_client(meta_request);
    AWS_ASSERT(client != NULL);

    struct aws_s3_tester *tester = client->shutdown_callback_user_data;
    AWS_ASSERT(tester != NULL);

    aws_s3_client_release(client);
    client = NULL;

    if (s_s3_retry_test_data_inc_counter1(tester->user_data) == 0) {
        aws_raise_error(AWS_ERROR_UNKNOWN);
        return AWS_OP_ERR;
    }

    struct aws_s3_meta_request_vtable *original_meta_request_vtable =
        aws_s3_tester_get_meta_request_vtable_patch(tester, 0)->original_vtable;

    return original_meta_request_vtable->next_request(meta_request, out_request);
}

static struct aws_s3_meta_request *s_meta_request_factory_patch_next_request(
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
    patched_meta_request_vtable->next_request = s_s3_fail_first_next_request;

    return meta_request;
}

/* Test recovery when prepare request fails. */
AWS_TEST_CASE(test_s3_meta_request_fail_next_request, s_test_s3_meta_request_fail_next_request)
static int s_test_s3_meta_request_fail_next_request(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct s3_retry_test_data retry_test_data;
    s_s3_retry_test_data_init(&retry_test_data);
    tester.user_data = &retry_test_data;

    struct aws_s3_client_config client_config = {.region = g_test_s3_region, .part_size = 64 * 1024};

    ASSERT_SUCCESS(aws_s3_tester_bind_client(&tester, &client_config));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);
    struct aws_s3_client_vtable *patched_client_vtable = aws_s3_tester_patch_client_vtable(&tester, client, NULL);
    patched_client_vtable->meta_request_factory = s_meta_request_factory_patch_next_request;

    ASSERT_SUCCESS(aws_s3_tester_send_get_object_meta_request(&tester, client, false));

    aws_s3_client_release(client);
    client = NULL;

    s_s3_retry_test_data_clean_up(&retry_test_data);

    aws_s3_tester_clean_up(&tester);

    return 0;
}

static int s_s3_fail_first_prepare_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_client *client,
    struct aws_s3_request *request) {

    AWS_ASSERT(client != NULL);

    struct aws_s3_tester *tester = client->shutdown_callback_user_data;
    AWS_ASSERT(tester != NULL);

    if (s_s3_retry_test_data_inc_counter1(tester->user_data) == 0) {
        aws_raise_error(AWS_ERROR_UNKNOWN);
        return AWS_OP_ERR;
    }

    struct aws_s3_meta_request_vtable *original_meta_request_vtable =
        aws_s3_tester_get_meta_request_vtable_patch(tester, 0)->original_vtable;

    return original_meta_request_vtable->prepare_request(meta_request, client, request);
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
    patched_meta_request_vtable->prepare_request = s_s3_fail_first_prepare_request;

    return meta_request;
}

/* Test recovery when prepare request fails. */
AWS_TEST_CASE(test_s3_meta_request_fail_prepare_request, s_test_s3_meta_request_fail_prepare_request)
static int s_test_s3_meta_request_fail_prepare_request(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct s3_retry_test_data retry_test_data;
    s_s3_retry_test_data_init(&retry_test_data);
    tester.user_data = &retry_test_data;

    struct aws_s3_client_config client_config = {.region = g_test_s3_region, .part_size = 64 * 1024};

    ASSERT_SUCCESS(aws_s3_tester_bind_client(&tester, &client_config));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);
    struct aws_s3_client_vtable *patched_client_vtable = aws_s3_tester_patch_client_vtable(&tester, client, NULL);
    patched_client_vtable->meta_request_factory = s_meta_request_factory_patch_prepare_request;

    ASSERT_SUCCESS(aws_s3_tester_send_get_object_meta_request(&tester, client, true));

    aws_s3_client_release(client);
    client = NULL;

    s_s3_retry_test_data_clean_up(&retry_test_data);

    aws_s3_tester_clean_up(&tester);

    return 0;
}

static int s_s3_client_sign_message_fail_first(
    struct aws_s3_client *client,
    struct aws_http_message *message,
    aws_s3_client_sign_callback *callback,
    void *user_data) {
    AWS_ASSERT(client != NULL);

    struct aws_s3_tester *tester = client->shutdown_callback_user_data;
    AWS_ASSERT(tester != NULL);

    if (s_s3_retry_test_data_inc_counter1(tester->user_data) == 0) {
        aws_raise_error(AWS_ERROR_UNKNOWN);
        return AWS_OP_ERR;
    }

    struct aws_s3_client_vtable *original_client_vtable =
        aws_s3_tester_get_client_vtable_patch(tester, 0)->original_vtable;

    return original_client_vtable->sign_message(client, message, callback, user_data);
}

/* Test recovery when sign message fails. */
AWS_TEST_CASE(test_s3_client_sign_message_fail, s_test_s3_client_sign_message_fail)
static int s_test_s3_client_sign_message_fail(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct s3_retry_test_data retry_test_data;
    s_s3_retry_test_data_init(&retry_test_data);
    tester.user_data = &retry_test_data;

    struct aws_s3_client_config client_config = {.region = g_test_s3_region, .part_size = 64 * 1024};

    ASSERT_SUCCESS(aws_s3_tester_bind_client(&tester, &client_config));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);
    struct aws_s3_client_vtable *patched_client_vtable = aws_s3_tester_patch_client_vtable(&tester, client, NULL);
    patched_client_vtable->sign_message = s_s3_client_sign_message_fail_first;

    ASSERT_SUCCESS(aws_s3_tester_send_get_object_meta_request(&tester, client, true));

    aws_s3_client_release(client);
    client = NULL;

    s_s3_retry_test_data_clean_up(&retry_test_data);

    aws_s3_tester_clean_up(&tester);

    return 0;
}

static int s_s3_meta_request_prepare_request_fail_first(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_client *client,
    struct aws_s3_request *request) {
    AWS_ASSERT(meta_request);
    AWS_ASSERT(client);
    AWS_ASSERT(request);

    struct aws_s3_tester *tester = client->shutdown_callback_user_data;
    AWS_ASSERT(tester != NULL);

    struct aws_s3_meta_request_vtable *original_meta_request_vtable =
        aws_s3_tester_get_meta_request_vtable_patch(tester, 0)->original_vtable;

    int result = original_meta_request_vtable->prepare_request(meta_request, client, request);

    if (result != AWS_OP_SUCCESS) {
        return result;
    }

    if (s_s3_retry_test_data_inc_counter1(tester->user_data) == 0) {

        const struct aws_byte_cursor test_object_path =
            AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("/non-existing-file12345.txt");

        int set_request_path_result = aws_http_message_set_request_path(request->send_data.message, test_object_path);
        AWS_ASSERT(set_request_path_result == AWS_ERROR_SUCCESS);
        (void)set_request_path_result;
    }

    return AWS_OP_SUCCESS;
}

static void s_s3_meta_request_send_request_finish_fail_first(
    struct aws_s3_vip_connection *vip_connection,
    struct aws_http_stream *stream,
    int error_code) {

    struct aws_s3_client *client = aws_s3_meta_request_get_client(vip_connection->work_data.request->meta_request);
    AWS_ASSERT(client != NULL);

    struct aws_s3_tester *tester = client->shutdown_callback_user_data;
    AWS_ASSERT(tester != NULL);

    aws_s3_client_release(client);
    client = NULL;

    if (s_s3_retry_test_data_inc_counter2(tester->user_data) == 0) {
        vip_connection->work_data.request->send_data.response_status = AWS_S3_RESPONSE_STATUS_INTERNAL_ERROR;
    }

    struct aws_s3_meta_request_vtable *original_meta_request_vtable =
        aws_s3_tester_get_meta_request_vtable_patch(tester, 0)->original_vtable;

    original_meta_request_vtable->send_request_finish(vip_connection, stream, error_code);
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

    struct s3_retry_test_data retry_test_data;
    s_s3_retry_test_data_init(&retry_test_data);
    tester.user_data = &retry_test_data;

    struct aws_s3_client_config client_config = {.region = g_test_s3_region, .part_size = 64 * 1024};

    ASSERT_SUCCESS(aws_s3_tester_bind_client(&tester, &client_config));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);
    struct aws_s3_client_vtable *patched_client_vtable = aws_s3_tester_patch_client_vtable(&tester, client, NULL);
    patched_client_vtable->meta_request_factory = s_meta_request_factory_patch_send_request_finish;

    ASSERT_SUCCESS(aws_s3_tester_send_get_object_meta_request(&tester, client, true));

    aws_s3_client_release(client);
    client = NULL;

    s_s3_retry_test_data_clean_up(&retry_test_data);

    aws_s3_tester_clean_up(&tester);

    return 0;
}

static int s_auto_range_put_stream_complete_remove_first_upload_id(
    struct aws_http_stream *stream,
    struct aws_s3_vip_connection *vip_connection) {

    AWS_ASSERT(vip_connection);

    struct aws_s3_client *client = aws_s3_meta_request_get_client(vip_connection->work_data.request->meta_request);
    AWS_ASSERT(client != NULL);

    struct aws_s3_tester *tester = client->shutdown_callback_user_data;
    AWS_ASSERT(tester != NULL);

    aws_s3_client_release(client);
    client = NULL;

    if (s_s3_retry_test_data_inc_counter1(tester->user_data) == 0) {
        struct aws_s3_request *request = vip_connection->work_data.request;
        aws_byte_buf_reset(&request->send_data.part_buffer->buffer, false);
    }

    struct aws_s3_meta_request_vtable *original_meta_request_vtable =
        aws_s3_tester_get_meta_request_vtable_patch(tester, 0)->original_vtable;

    return original_meta_request_vtable->stream_complete(stream, vip_connection);
}

static struct aws_s3_meta_request *s_meta_request_factory_patch_stream_complete(
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
    patched_meta_request_vtable->stream_complete = s_auto_range_put_stream_complete_remove_first_upload_id;

    return meta_request;
}

AWS_TEST_CASE(test_s3_auto_range_put_missing_upload_id, s_test_s3_auto_range_put_missing_upload_id)
static int s_test_s3_auto_range_put_missing_upload_id(struct aws_allocator *allocator, void *ctx) {

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct s3_retry_test_data retry_test_data;
    s_s3_retry_test_data_init(&retry_test_data);
    tester.user_data = &retry_test_data;

    struct aws_s3_client_config client_config = {.region = g_test_s3_region, .part_size = 5 * 1024 * 1024};

    ASSERT_SUCCESS(aws_s3_tester_bind_client(&tester, &client_config));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);
    struct aws_s3_client_vtable *patched_client_vtable = aws_s3_tester_patch_client_vtable(&tester, client, NULL);
    patched_client_vtable->meta_request_factory = s_meta_request_factory_patch_stream_complete;

    ASSERT_TRUE(client != NULL);

    aws_s3_tester_send_put_object_meta_request(&tester, client, true);

    aws_s3_client_release(client);
    client = NULL;

    s_s3_retry_test_data_clean_up(&retry_test_data);

    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}
