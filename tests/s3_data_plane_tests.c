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

AWS_TEST_CASE(test_s3_client_create_destroy, s_test_s3_client_create_destroy)
static int s_test_s3_client_create_destroy(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client_config client_config;
    AWS_ZERO_STRUCT(client_config);

    ASSERT_SUCCESS(aws_s3_tester_bind_client(
        &tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION | AWS_S3_TESTER_BIND_CLIENT_SIGNING));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    ASSERT_TRUE(client != NULL);

    aws_s3_client_release(client);
    client = NULL;

    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(test_s3_client_max_active_connections_override, s_test_s3_client_max_active_connections_override)
static int s_test_s3_client_max_active_connections_override(struct aws_allocator *allocator, void *ctx) {
    (void)allocator;
    (void)ctx;

    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client_config client_config = {
        .max_active_connections_override = 10,
    };

    ASSERT_SUCCESS(aws_s3_tester_bind_client(&tester, &client_config, 0));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    ASSERT_TRUE(client->max_active_connections_override == client_config.max_active_connections_override);

    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(test_s3_client_get_max_active_connections, s_test_s3_client_get_max_active_connections)
static int s_test_s3_client_get_max_active_connections(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    const uint32_t num_connections_per_vip_override = 5;

    struct aws_s3_client *mock_client = aws_s3_tester_mock_client_new(&tester);
    *((uint32_t *)&mock_client->ideal_vip_count) = 5;
    *((uint32_t *)&mock_client->max_active_connections_override) = 0;

    /* Behavior should not be affected by max_active_connections_override since it is 0. */
    ASSERT_TRUE(
        aws_s3_client_get_max_active_connections(mock_client, 0) ==
        mock_client->ideal_vip_count * g_max_num_connections_per_vip);
    ASSERT_TRUE(
        aws_s3_client_get_max_active_connections(mock_client, num_connections_per_vip_override) ==
        mock_client->ideal_vip_count * num_connections_per_vip_override);

    *((uint32_t *)&mock_client->max_active_connections_override) = 3;

    /* Max active connections override should now cap the calculated amount of active connections. */
    ASSERT_TRUE(
        aws_s3_client_get_max_active_connections(mock_client, 0) == mock_client->max_active_connections_override);
    ASSERT_TRUE(
        aws_s3_client_get_max_active_connections(mock_client, num_connections_per_vip_override) ==
        mock_client->max_active_connections_override);

    /* Max active connections override should be ignored since the calculated amount of connections is less. */
    *((uint32_t *)&mock_client->max_active_connections_override) = 100;
    ASSERT_TRUE(
        aws_s3_client_get_max_active_connections(mock_client, 0) ==
        mock_client->ideal_vip_count * g_max_num_connections_per_vip);
    ASSERT_TRUE(
        aws_s3_client_get_max_active_connections(mock_client, num_connections_per_vip_override) ==
        mock_client->ideal_vip_count * num_connections_per_vip_override);

    aws_s3_client_release(mock_client);
    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(test_s3_request_create_destroy, s_test_s3_request_create_destroy)
static int s_test_s3_request_create_destroy(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    const int request_tag = 1234;
    const uint32_t part_number = 5678;

    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_meta_request *meta_request = aws_s3_tester_mock_meta_request_new(&tester);
    ASSERT_TRUE(meta_request != NULL);

    struct aws_http_message *request_message = aws_s3_tester_dummy_http_request_new(&tester);
    ASSERT_TRUE(request_message != NULL);

    struct aws_s3_request *request =
        aws_s3_request_new(meta_request, request_tag, part_number, AWS_S3_REQUEST_FLAG_RECORD_RESPONSE_HEADERS);

    ASSERT_TRUE(request != NULL);

    ASSERT_TRUE(request->meta_request == meta_request);
    ASSERT_TRUE(request->part_number == part_number);
    ASSERT_TRUE(request->request_tag == request_tag);
    ASSERT_TRUE(request->record_response_headers == true);

    aws_s3_request_setup_send_data(request, request_message);

    ASSERT_TRUE(request->send_data.message != NULL);
    ASSERT_TRUE(request->send_data.response_headers == NULL);

    request->send_data.response_headers = aws_http_headers_new(allocator);
    ASSERT_TRUE(request->send_data.response_headers != NULL);

    aws_s3_request_clean_up_send_data(request);

    ASSERT_TRUE(request->send_data.message == NULL);
    ASSERT_TRUE(request->send_data.response_headers == NULL);
    ASSERT_TRUE(request->send_data.response_status == 0);

    aws_s3_request_release(request);
    aws_http_message_release(request_message);
    aws_s3_meta_request_release(meta_request);

    aws_s3_tester_clean_up(&tester);

    return 0;
}

struct s3_test_body_streaming_user_data {
    struct aws_s3_tester *tester;
    struct aws_allocator *sba_allocator;
    uint64_t expected_range_start;
    uint64_t received_body_size;
};

static int s_s3_meta_request_test_body_streaming_callback(
    struct aws_s3_meta_request *meta_request,
    const struct aws_byte_cursor *body,
    uint64_t range_start,
    void *user_data) {
    (void)meta_request;
    (void)body;
    (void)range_start;

    struct s3_test_body_streaming_user_data *body_streaming_user_data = user_data;

    body_streaming_user_data->received_body_size += body->len;

    ASSERT_TRUE(body_streaming_user_data->expected_range_start == range_start);
    body_streaming_user_data->expected_range_start += body->len;

    aws_s3_tester_inc_counter1(body_streaming_user_data->tester);

    return AWS_OP_SUCCESS;
}

/* Test the meta request body streaming functionality. */
AWS_TEST_CASE(test_s3_meta_request_body_streaming, s_test_s3_meta_request_body_streaming)
static int s_test_s3_meta_request_body_streaming(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    const uint32_t part_range0_start = 1;
    const uint32_t part_range0_end = part_range0_start + 4;

    const uint32_t part_range1_start = part_range0_end + 1;
    const uint32_t part_range1_end = part_range1_start + 4;

    const size_t request_response_body_size = 16;

    struct aws_byte_buf response_body_source_buffer;
    aws_byte_buf_init(&response_body_source_buffer, allocator, request_response_body_size);

    const struct aws_byte_cursor test_byte_cursor = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("0");

    for (size_t i = 0; i < request_response_body_size; ++i) {
        aws_byte_buf_append(&response_body_source_buffer, &test_byte_cursor);
    }

    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct s3_test_body_streaming_user_data body_streaming_user_data = {
        .tester = &tester,
    };

    struct aws_s3_client *mock_client = aws_s3_tester_mock_client_new(&tester);

    struct aws_s3_meta_request *meta_request = aws_s3_tester_mock_meta_request_new(&tester);
    ASSERT_TRUE(meta_request != NULL);

    struct aws_event_loop_group *event_loop_group = aws_event_loop_group_new_default(allocator, 0, NULL);
    aws_s3_client_acquire(mock_client);
    meta_request->client = mock_client;
    meta_request->user_data = &body_streaming_user_data;
    *((size_t *)&meta_request->part_size) = request_response_body_size;
    meta_request->body_callback = s_s3_meta_request_test_body_streaming_callback;
    meta_request->io_event_loop = aws_event_loop_group_get_next_loop(event_loop_group);

    /* Queue the first range of parts in order. Each part should be flushed one-by-one. */
    {
        for (uint32_t part_number = part_range0_start; part_number <= part_range0_end; ++part_number) {
            struct aws_s3_request *request = aws_s3_request_new(meta_request, 0, part_number, 0);
            aws_byte_buf_init_copy(&request->send_data.response_body, allocator, &response_body_source_buffer);

            aws_s3_tester_set_counter1_desired(&tester, part_number);

            aws_s3_meta_request_lock_synced_data(meta_request);

            aws_s3_meta_request_stream_response_body_synced(meta_request, request);
            ASSERT_TRUE(aws_priority_queue_size(&meta_request->synced_data.pending_body_streaming_requests) == 0);

            aws_s3_meta_request_unlock_synced_data(meta_request);

            aws_s3_tester_wait_for_counters(&tester);

            aws_s3_request_release(request);
        }
    }

    aws_s3_tester_set_counter1_desired(&tester, part_range1_end);

    /* Queue parts for second range, but skip over the first part.*/
    {
        uint32_t num_parts_queued = 0;

        ASSERT_TRUE(part_range1_start != part_range1_end);

        for (uint32_t part_number = part_range1_start + 1; part_number <= part_range1_end; ++part_number) {
            struct aws_s3_request *request = aws_s3_request_new(meta_request, 0, part_number, 0);
            aws_byte_buf_init_copy(&request->send_data.response_body, allocator, &response_body_source_buffer);

            aws_s3_meta_request_lock_synced_data(meta_request);
            aws_s3_meta_request_stream_response_body_synced(meta_request, request);
            aws_s3_meta_request_unlock_synced_data(meta_request);

            aws_s3_request_release(request);
            ++num_parts_queued;
        }

        aws_s3_meta_request_lock_synced_data(meta_request);
        ASSERT_TRUE(
            aws_priority_queue_size(&meta_request->synced_data.pending_body_streaming_requests) == num_parts_queued);
        aws_s3_meta_request_unlock_synced_data(meta_request);
    }

    /* Stream the last part of the body, which should flush the priority queue. */
    {
        struct aws_s3_request *request = aws_s3_request_new(meta_request, 0, part_range1_start, 0);
        aws_byte_buf_init_copy(&request->send_data.response_body, allocator, &response_body_source_buffer);

        aws_s3_meta_request_lock_synced_data(meta_request);
        aws_s3_meta_request_stream_response_body_synced(meta_request, request);
        aws_s3_meta_request_unlock_synced_data(meta_request);

        aws_s3_meta_request_lock_synced_data(meta_request);
        ASSERT_TRUE(aws_priority_queue_size(&meta_request->synced_data.pending_body_streaming_requests) == 0);
        aws_s3_meta_request_unlock_synced_data(meta_request);

        aws_s3_request_release(request);

        aws_s3_tester_wait_for_counters(&tester);
    }

    ASSERT_TRUE(body_streaming_user_data.received_body_size == (request_response_body_size * part_range1_end));

    aws_s3_meta_request_release(meta_request);
    aws_s3_client_release(mock_client);
    aws_event_loop_group_release(event_loop_group);
    aws_byte_buf_clean_up(&response_body_source_buffer);
    aws_s3_tester_clean_up(&tester);

    return 0;
}

/* Test aws_s3_client_queue_requests_threaded and aws_s3_client_dequeue_request_threaded */
AWS_TEST_CASE(test_s3_client_queue_requests, s_test_s3_client_queue_requests)
static int s_test_s3_client_queue_requests(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    aws_s3_tester_init(allocator, &tester);

    struct aws_s3_client *mock_client = aws_s3_tester_mock_client_new(&tester);
    aws_linked_list_init(&mock_client->threaded_data.request_queue);

    struct aws_s3_meta_request *mock_meta_request = aws_s3_tester_mock_meta_request_new(&tester);

    struct aws_s3_request *pivot_request = aws_s3_request_new(mock_meta_request, 0, 0, 0);

    struct aws_linked_list pivot_request_list;
    aws_linked_list_init(&pivot_request_list);

    struct aws_s3_request *requests[] = {
        aws_s3_request_new(mock_meta_request, 0, 0, 0),
        aws_s3_request_new(mock_meta_request, 0, 0, 0),
        aws_s3_request_new(mock_meta_request, 0, 0, 0),
    };

    const uint32_t num_requests = sizeof(requests) / sizeof(struct aws_s3_request *);

    struct aws_linked_list request_list;
    aws_linked_list_init(&request_list);

    {
        aws_linked_list_push_back(&pivot_request_list, &pivot_request->node);
        aws_s3_client_queue_requests_threaded(mock_client, &pivot_request_list, false);

        ASSERT_TRUE(mock_client->threaded_data.request_queue_size == 1);
        ASSERT_TRUE(!aws_linked_list_empty(&mock_client->threaded_data.request_queue));

        for (uint32_t i = 0; i < num_requests; ++i) {
            aws_linked_list_push_back(&request_list, &requests[i]->node);
        }

        /* Move the requests to the back of the queue. */
        aws_s3_client_queue_requests_threaded(mock_client, &request_list, false);
    }

    ASSERT_TRUE(aws_linked_list_empty(&request_list));
    ASSERT_TRUE(mock_client->threaded_data.request_queue_size == (num_requests + 1));
    ASSERT_TRUE(!aws_linked_list_empty(&mock_client->threaded_data.request_queue));

    {
        /* The first request should be the pivot request since the other requests were pushed to the back. */
        struct aws_s3_request *first_request = aws_s3_client_dequeue_request_threaded(mock_client);
        ASSERT_TRUE(first_request == pivot_request);

        ASSERT_TRUE(mock_client->threaded_data.request_queue_size == num_requests);
        ASSERT_TRUE(!aws_linked_list_empty(&mock_client->threaded_data.request_queue));
    }

    for (uint32_t i = 0; i < num_requests; ++i) {
        struct aws_s3_request *request = aws_s3_client_dequeue_request_threaded(mock_client);

        ASSERT_TRUE(request == requests[i]);

        ASSERT_TRUE(mock_client->threaded_data.request_queue_size == (num_requests - (i + 1)));

        if (i < num_requests - 1) {
            ASSERT_TRUE(!aws_linked_list_empty(&mock_client->threaded_data.request_queue));
        }
    }

    ASSERT_TRUE(mock_client->threaded_data.request_queue_size == 0);
    ASSERT_TRUE(aws_linked_list_empty(&mock_client->threaded_data.request_queue));

    {
        aws_linked_list_push_back(&pivot_request_list, &pivot_request->node);
        aws_s3_client_queue_requests_threaded(mock_client, &pivot_request_list, false);

        ASSERT_TRUE(mock_client->threaded_data.request_queue_size == 1);
        ASSERT_TRUE(!aws_linked_list_empty(&mock_client->threaded_data.request_queue));

        for (uint32_t i = 0; i < num_requests; ++i) {
            aws_linked_list_push_back(&request_list, &requests[i]->node);
        }

        /* Move the requests to the front of the queue. */
        aws_s3_client_queue_requests_threaded(mock_client, &request_list, true);
    }

    ASSERT_TRUE(aws_linked_list_empty(&request_list));
    ASSERT_TRUE(mock_client->threaded_data.request_queue_size == (num_requests + 1));
    ASSERT_TRUE(!aws_linked_list_empty(&mock_client->threaded_data.request_queue));

    for (uint32_t i = 0; i < num_requests; ++i) {
        struct aws_s3_request *request = aws_s3_client_dequeue_request_threaded(mock_client);

        ASSERT_TRUE(request == requests[i]);
    }

    {
        /* The last request should be the pivot request since the other requests were pushed to the front. */
        struct aws_s3_request *last_request = aws_s3_client_dequeue_request_threaded(mock_client);
        ASSERT_TRUE(last_request == pivot_request);
    }

    ASSERT_TRUE(aws_linked_list_empty(&mock_client->threaded_data.request_queue));
    ASSERT_TRUE(mock_client->threaded_data.request_queue_size == 0);

    for (uint32_t i = 0; i < num_requests; ++i) {
        aws_s3_request_release(requests[i]);
    }

    aws_s3_request_release(pivot_request);
    aws_s3_meta_request_release(mock_meta_request);
    aws_s3_client_release(mock_client);
    aws_s3_tester_clean_up(&tester);

    return 0;
}

struct test_work_meta_request_update_user_data {
    bool has_work_remaining;
    uint32_t num_prepares;
};

static bool s_s3_test_work_meta_request_update(
    struct aws_s3_meta_request *meta_request,
    uint32_t flags,
    struct aws_s3_request **out_request) {
    AWS_ASSERT(meta_request);
    (void)flags;

    struct test_work_meta_request_update_user_data *user_data = meta_request->user_data;

    if (out_request) {
        if (user_data->has_work_remaining) {
            *out_request = aws_s3_request_new(meta_request, 0, 0, 0);
        }
    }

    return user_data->has_work_remaining;
}

static void s_s3_test_work_meta_request_schedule_prepare_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    aws_s3_meta_request_prepare_request_callback_fn *callback,
    void *user_data) {
    (void)request;
    (void)callback;
    (void)user_data;

    AWS_ASSERT(meta_request);

    struct test_work_meta_request_update_user_data *test_user_data = meta_request->user_data;
    ++test_user_data->num_prepares;

    aws_s3_request_release(request);
}

/* Test that the client's meta-request-update function handles the case of the client not having been able to establish
 * any connections. */
AWS_TEST_CASE(test_s3_update_meta_requests_no_endpoint_conns, s_test_s3_update_meta_requests_no_endpoint_conns)
static int s_test_s3_update_meta_requests_no_endpoint_conns(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    aws_s3_tester_init(allocator, &tester);

    struct aws_s3_client *mock_client = aws_s3_tester_mock_client_new(&tester);

    aws_linked_list_init(&mock_client->threaded_data.request_queue);
    aws_linked_list_init(&mock_client->threaded_data.meta_requests);

    ///////////////////////////////////

    struct aws_s3_meta_request *mock_meta_request_0 = aws_s3_tester_mock_meta_request_new(&tester);
    struct test_work_meta_request_update_user_data mock_meta_request_0_data = {
        .has_work_remaining = false,
    };

    mock_meta_request_0->user_data = &mock_meta_request_0_data;

    struct aws_s3_meta_request_vtable *meta_request_vtable_0 =
        aws_s3_tester_patch_meta_request_vtable(&tester, mock_meta_request_0, NULL);
    meta_request_vtable_0->update = s_s3_test_work_meta_request_update;
    meta_request_vtable_0->schedule_prepare_request = s_s3_test_work_meta_request_schedule_prepare_request;

    /* Intentionally push this meta request first to test that it's properly removed from the list when the list has
     * items after it. */
    aws_linked_list_push_back(
        &mock_client->threaded_data.meta_requests, &mock_meta_request_0->client_process_work_threaded_data.node);

    aws_s3_meta_request_acquire(mock_meta_request_0);

    const uint32_t num_requests_to_queue = 10;

    for (uint32_t i = 0; i < num_requests_to_queue; ++i) {
        struct aws_s3_request *request = aws_s3_request_new(mock_meta_request_0, 0, 0, 0);
        aws_linked_list_push_back(&mock_client->threaded_data.request_queue, &request->node);
        ++mock_client->threaded_data.request_queue_size;
    }

    ///////////////////////////////////

    struct aws_s3_meta_request *mock_meta_request_1 = aws_s3_tester_mock_meta_request_new(&tester);
    struct test_work_meta_request_update_user_data mock_meta_request_1_data = {
        .has_work_remaining = true,
    };

    mock_meta_request_1->user_data = &mock_meta_request_1_data;

    struct aws_s3_meta_request_vtable *meta_request_vtable_1 =
        aws_s3_tester_patch_meta_request_vtable(&tester, mock_meta_request_1, NULL);
    meta_request_vtable_1->update = s_s3_test_work_meta_request_update;
    meta_request_vtable_1->schedule_prepare_request = s_s3_test_work_meta_request_schedule_prepare_request;

    aws_linked_list_push_back(
        &mock_client->threaded_data.meta_requests, &mock_meta_request_1->client_process_work_threaded_data.node);
    aws_s3_meta_request_acquire(mock_meta_request_1);

    ///////////////////////////////////

    aws_s3_client_update_meta_requests_threaded(mock_client, AWS_S3_META_REQUEST_UPDATE_FLAG_NO_ENDPOINT_CONNECTIONS);

    /* Make sure that no requests have been prepared. */
    {
        ASSERT_TRUE(mock_client->threaded_data.num_requests_being_prepared == 0);
        ASSERT_TRUE(mock_client->threaded_data.request_queue_size == 0);
        ASSERT_TRUE(aws_linked_list_empty(&mock_client->threaded_data.request_queue));

        uint32_t num_meta_requests_in_list = 0;
        bool meta_request_1_found = false;

        for (struct aws_linked_list_node *node = aws_linked_list_begin(&mock_client->threaded_data.meta_requests);
             node != aws_linked_list_end(&mock_client->threaded_data.meta_requests);
             node = aws_linked_list_next(node)) {

            struct aws_s3_meta_request *meta_request =
                AWS_CONTAINER_OF(node, struct aws_s3_meta_request, client_process_work_threaded_data);

            if (meta_request == mock_meta_request_1) {
                meta_request_1_found = true;
            }

            ++num_meta_requests_in_list;
        }

        ASSERT_TRUE(meta_request_1_found);
        ASSERT_TRUE(num_meta_requests_in_list == 1);
    }

    mock_meta_request_1_data.has_work_remaining = false;

    aws_s3_client_update_meta_requests_threaded(mock_client, AWS_S3_META_REQUEST_UPDATE_FLAG_NO_ENDPOINT_CONNECTIONS);

    /* Because the meta request has no work left, it should no longer be in the list. */
    {
        ASSERT_TRUE(aws_linked_list_empty(&mock_client->threaded_data.request_queue));
        ASSERT_TRUE(aws_linked_list_empty(&mock_client->threaded_data.meta_requests));
        ASSERT_TRUE(mock_client->threaded_data.num_requests_being_prepared == 0);
    }

    aws_s3_meta_request_release(mock_meta_request_0);
    aws_s3_meta_request_release(mock_meta_request_1);
    aws_s3_client_release(mock_client);
    aws_s3_tester_clean_up(&tester);

    return 0;
}

/* Test that the client will not start preparing requests if no connections have been established yet. */
AWS_TEST_CASE(test_s3_update_meta_requests_no_conns_yet, s_test_s3_update_meta_requests_no_conns_yet)
static int s_test_s3_update_meta_requests_no_conns_yet(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    aws_s3_tester_init(allocator, &tester);

    struct aws_s3_client *mock_client = aws_s3_tester_mock_client_new(&tester);

    aws_linked_list_init(&mock_client->threaded_data.request_queue);
    aws_linked_list_init(&mock_client->threaded_data.meta_requests);
    aws_linked_list_init(&mock_client->threaded_data.idle_vip_connections);

    ///////////////////////////////////

    struct aws_s3_meta_request *mock_meta_request = aws_s3_tester_mock_meta_request_new(&tester);

    struct test_work_meta_request_update_user_data mock_meta_request_data = {
        .has_work_remaining = true,
    };

    mock_meta_request->user_data = &mock_meta_request_data;

    struct aws_s3_meta_request_vtable *meta_request_vtable_1 =
        aws_s3_tester_patch_meta_request_vtable(&tester, mock_meta_request, NULL);
    meta_request_vtable_1->update = s_s3_test_work_meta_request_update;
    meta_request_vtable_1->schedule_prepare_request = s_s3_test_work_meta_request_schedule_prepare_request;

    aws_linked_list_push_back(
        &mock_client->threaded_data.meta_requests, &mock_meta_request->client_process_work_threaded_data.node);

    aws_s3_meta_request_acquire(mock_meta_request);

    ///////////////////////////////////

    aws_s3_client_update_meta_requests_threaded(mock_client, AWS_S3_META_REQUEST_UPDATE_FLAG_CONSERVATIVE);

    {
        ASSERT_TRUE(mock_client->threaded_data.request_queue_size == 0);
        ASSERT_TRUE(aws_linked_list_empty(&mock_client->threaded_data.request_queue));
        ASSERT_TRUE(mock_client->threaded_data.num_requests_being_prepared == 0);
        ASSERT_TRUE(aws_atomic_load_int(&mock_client->stats.num_requests_in_flight) == 0);
    }

    aws_s3_client_update_meta_requests_threaded(mock_client, 0);

    {
        ASSERT_TRUE(mock_client->threaded_data.request_queue_size == 0);
        ASSERT_TRUE(aws_linked_list_empty(&mock_client->threaded_data.request_queue));
        ASSERT_TRUE(mock_client->threaded_data.num_requests_being_prepared == 0);
        ASSERT_TRUE(aws_atomic_load_int(&mock_client->stats.num_requests_in_flight) == 0);
    }

    while (!aws_linked_list_empty(&mock_client->threaded_data.meta_requests)) {
        struct aws_linked_list_node *meta_request_node =
            aws_linked_list_pop_front(&mock_client->threaded_data.meta_requests);

        struct aws_s3_meta_request *meta_request =
            AWS_CONTAINER_OF(meta_request_node, struct aws_s3_meta_request, client_process_work_threaded_data);

        aws_s3_meta_request_release(meta_request);
    }

    aws_s3_meta_request_release(mock_meta_request);
    aws_s3_client_release(mock_client);
    aws_s3_tester_clean_up(&tester);

    return 0;
}

/* Test that the client will prepare requests under the correct conditions. */
AWS_TEST_CASE(test_s3_update_meta_requests_trigger_prepare, s_test_s3_update_meta_requests_trigger_prepare)
static int s_test_s3_update_meta_requests_trigger_prepare(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    aws_s3_tester_init(allocator, &tester);

    struct aws_s3_client *mock_client = aws_s3_tester_mock_client_new(&tester);

    *((uint32_t *)&mock_client->ideal_vip_count) = 1;
    aws_linked_list_init(&mock_client->threaded_data.request_queue);
    aws_linked_list_init(&mock_client->threaded_data.meta_requests);

    const uint32_t max_requests_prepare = aws_s3_client_get_max_requests_prepare(mock_client);

    /* Mock that there is one active connection so that the update meta requests function doesn't early out due to no
     * connections being established yet. */
    aws_atomic_store_int(&mock_client->stats.num_active_vip_connections, 1);

    ///////////////////////////////////

    struct aws_s3_meta_request *mock_meta_request_0 = aws_s3_tester_mock_meta_request_new(&tester);
    struct test_work_meta_request_update_user_data mock_meta_request_0_data = {
        .has_work_remaining = false,
    };

    mock_meta_request_0->user_data = &mock_meta_request_0_data;

    struct aws_s3_meta_request_vtable *meta_request_vtable_0 =
        aws_s3_tester_patch_meta_request_vtable(&tester, mock_meta_request_0, NULL);
    meta_request_vtable_0->update = s_s3_test_work_meta_request_update;
    meta_request_vtable_0->schedule_prepare_request = s_s3_test_work_meta_request_schedule_prepare_request;

    /* Intentionally push this meta request first to test that it's properly removed from the list when the list has
     * items after it. */
    aws_linked_list_push_back(
        &mock_client->threaded_data.meta_requests, &mock_meta_request_0->client_process_work_threaded_data.node);

    aws_s3_meta_request_acquire(mock_meta_request_0);

    ///////////////////////////////////

    struct aws_s3_meta_request *mock_meta_request_1 = aws_s3_tester_mock_meta_request_new(&tester);
    struct test_work_meta_request_update_user_data mock_meta_request_1_data = {
        .has_work_remaining = true,
    };

    mock_meta_request_1->user_data = &mock_meta_request_1_data;

    struct aws_s3_meta_request_vtable *meta_request_vtable_1 =
        aws_s3_tester_patch_meta_request_vtable(&tester, mock_meta_request_1, NULL);
    meta_request_vtable_1->update = s_s3_test_work_meta_request_update;
    meta_request_vtable_1->schedule_prepare_request = s_s3_test_work_meta_request_schedule_prepare_request;

    aws_linked_list_push_back(
        &mock_client->threaded_data.meta_requests, &mock_meta_request_1->client_process_work_threaded_data.node);
    aws_s3_meta_request_acquire(mock_meta_request_1);

    ///////////////////////////////////

    aws_s3_client_update_meta_requests_threaded(mock_client, 0);

    /* After the update, the max number of requests that can be prepared should have been triggered. */
    {
        ASSERT_TRUE(mock_client->threaded_data.request_queue_size == 0);
        ASSERT_TRUE(aws_linked_list_empty(&mock_client->threaded_data.request_queue));
        ASSERT_TRUE(mock_client->threaded_data.num_requests_being_prepared == max_requests_prepare);
        ASSERT_TRUE(aws_atomic_load_int(&mock_client->stats.num_requests_in_flight) == max_requests_prepare);

        uint32_t num_meta_requests_in_list = 0;
        bool meta_request_1_found = false;

        for (struct aws_linked_list_node *node = aws_linked_list_begin(&mock_client->threaded_data.meta_requests);
             node != aws_linked_list_end(&mock_client->threaded_data.meta_requests);
             node = aws_linked_list_next(node)) {

            struct aws_s3_meta_request *meta_request =
                AWS_CONTAINER_OF(node, struct aws_s3_meta_request, client_process_work_threaded_data);

            if (meta_request == mock_meta_request_1) {
                meta_request_1_found = true;
            }

            ++num_meta_requests_in_list;
        }

        ASSERT_TRUE(meta_request_1_found);
        ASSERT_TRUE(num_meta_requests_in_list == 1);
    }

    mock_meta_request_1_data.has_work_remaining = false;

    aws_s3_client_update_meta_requests_threaded(mock_client, AWS_S3_META_REQUEST_UPDATE_FLAG_NO_ENDPOINT_CONNECTIONS);

    {
        ASSERT_TRUE(aws_linked_list_empty(&mock_client->threaded_data.request_queue));
        ASSERT_TRUE(aws_linked_list_empty(&mock_client->threaded_data.meta_requests));
        ASSERT_TRUE(mock_client->threaded_data.num_requests_being_prepared == max_requests_prepare);
        ASSERT_TRUE(aws_atomic_load_int(&mock_client->stats.num_requests_in_flight) == max_requests_prepare);
    }

    while (!aws_linked_list_empty(&mock_client->threaded_data.meta_requests)) {
        struct aws_linked_list_node *meta_request_node =
            aws_linked_list_pop_front(&mock_client->threaded_data.meta_requests);

        struct aws_s3_meta_request *meta_request =
            AWS_CONTAINER_OF(meta_request_node, struct aws_s3_meta_request, client_process_work_threaded_data);

        aws_s3_meta_request_release(meta_request);
    }

    aws_s3_meta_request_release(mock_meta_request_0);
    aws_s3_meta_request_release(mock_meta_request_1);
    aws_s3_client_release(mock_client);
    aws_s3_tester_clean_up(&tester);

    return 0;
}

/* Test that queued requests will be assigned to connnections properly. */
AWS_TEST_CASE(test_s3_client_update_connections_request_assign, s_test_s3_client_update_connections_request_assign)
static int s_test_s3_client_update_connections_request_assign(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    aws_s3_tester_init(allocator, &tester);

    struct aws_s3_vip *mock_vip = aws_s3_tester_mock_vip_new(&tester);
    aws_atomic_init_int(&mock_vip->active, 1);

    struct aws_s3_vip_connection *vip_connection_0 = aws_s3_tester_mock_vip_connection_new(&tester);
    vip_connection_0->owning_vip = mock_vip;

    struct aws_s3_vip_connection *vip_connection_1 = aws_s3_tester_mock_vip_connection_new(&tester);
    vip_connection_1->owning_vip = mock_vip;

    struct aws_s3_meta_request *mock_meta_request = aws_s3_tester_mock_meta_request_new(&tester);
    struct aws_s3_request *request_0 = aws_s3_request_new(mock_meta_request, 0, 0, 0);
    struct aws_s3_request *request_1 = aws_s3_request_new(mock_meta_request, 0, 0, 0);
    struct aws_s3_request *request_2 = aws_s3_request_new(mock_meta_request, 0, 0, 0);

    struct aws_s3_client *mock_client = aws_s3_tester_mock_client_new(&tester);
    *((uint32_t *)&mock_client->ideal_vip_count) = 1;
    aws_linked_list_init(&mock_client->threaded_data.idle_vip_connections);
    aws_linked_list_push_back(&mock_client->threaded_data.idle_vip_connections, &vip_connection_0->node);
    aws_linked_list_push_back(&mock_client->threaded_data.idle_vip_connections, &vip_connection_1->node);

    aws_linked_list_init(&mock_client->threaded_data.request_queue);

    /* Push one request into the request queue. First connection in the list should get the request. */
    {
        aws_linked_list_push_back(&mock_client->threaded_data.request_queue, &request_0->node);
        ++mock_client->threaded_data.request_queue_size;

        aws_s3_client_update_connections_threaded(mock_client, true);

        ASSERT_TRUE(mock_client->threaded_data.request_queue_size == 0);
        ASSERT_TRUE(aws_linked_list_empty(&mock_client->threaded_data.request_queue));
        ASSERT_TRUE(aws_atomic_load_int(&mock_client->stats.num_active_vip_connections) == 1);
        ASSERT_TRUE(aws_atomic_load_int(&mock_client->stats.num_warm_vip_connections) == 1);

        ASSERT_TRUE(vip_connection_0->is_warm);
        ASSERT_TRUE(vip_connection_0->is_active);
        ASSERT_TRUE(vip_connection_0->request == request_0);

        ASSERT_FALSE(vip_connection_1->is_warm);
        ASSERT_FALSE(vip_connection_1->is_active);
        ASSERT_TRUE(vip_connection_1->request == NULL);

        vip_connection_0->request = NULL;
    }

    /* Push vip_connection_0 back into the list and requeue the request.  Even though vip_connection_0 is last in the
     * list, it should get the request because it is the only warm connection. */
    {
        aws_linked_list_push_back(&mock_client->threaded_data.idle_vip_connections, &vip_connection_0->node);
        aws_linked_list_push_back(&mock_client->threaded_data.request_queue, &request_0->node);
        ++mock_client->threaded_data.request_queue_size;

        aws_s3_client_update_connections_threaded(mock_client, true);

        ASSERT_TRUE(mock_client->threaded_data.request_queue_size == 0);
        ASSERT_TRUE(aws_linked_list_empty(&mock_client->threaded_data.request_queue));
        ASSERT_TRUE(aws_atomic_load_int(&mock_client->stats.num_active_vip_connections) == 1);
        ASSERT_TRUE(aws_atomic_load_int(&mock_client->stats.num_warm_vip_connections) == 1);

        ASSERT_TRUE(vip_connection_0->is_warm);
        ASSERT_TRUE(vip_connection_0->is_active);
        ASSERT_TRUE(vip_connection_0->request == request_0);

        ASSERT_FALSE(vip_connection_1->is_warm);
        ASSERT_FALSE(vip_connection_1->is_active);
        ASSERT_TRUE(vip_connection_1->request == NULL);

        vip_connection_0->request = NULL;
    }

    /* Requeue vip_connection_0 but don't queue any requests. Both connections should become inactive. */
    {
        aws_linked_list_push_back(&mock_client->threaded_data.idle_vip_connections, &vip_connection_0->node);
        aws_s3_client_update_connections_threaded(mock_client, true);

        ASSERT_TRUE(mock_client->threaded_data.request_queue_size == 0);
        ASSERT_TRUE(aws_linked_list_empty(&mock_client->threaded_data.request_queue));
        ASSERT_TRUE(aws_atomic_load_int(&mock_client->stats.num_active_vip_connections) == 0);
        ASSERT_TRUE(aws_atomic_load_int(&mock_client->stats.num_warm_vip_connections) == 1);

        ASSERT_TRUE(vip_connection_0->is_warm);
        ASSERT_FALSE(vip_connection_0->is_active);
        ASSERT_TRUE(vip_connection_0->request == NULL);

        ASSERT_FALSE(vip_connection_1->is_warm);
        ASSERT_FALSE(vip_connection_1->is_active);
        ASSERT_TRUE(vip_connection_1->request == NULL);
    }

    /* Queue three requests. Both connections should get a request, and one should be left in the queue*/
    {
        aws_linked_list_push_back(&mock_client->threaded_data.request_queue, &request_0->node);
        ++mock_client->threaded_data.request_queue_size;

        aws_linked_list_push_back(&mock_client->threaded_data.request_queue, &request_1->node);
        ++mock_client->threaded_data.request_queue_size;

        aws_linked_list_push_back(&mock_client->threaded_data.request_queue, &request_2->node);
        ++mock_client->threaded_data.request_queue_size;

        aws_s3_client_update_connections_threaded(mock_client, true);

        ASSERT_TRUE(mock_client->threaded_data.request_queue_size == 1);
        ASSERT_FALSE(aws_linked_list_empty(&mock_client->threaded_data.request_queue));
        ASSERT_TRUE(aws_atomic_load_int(&mock_client->stats.num_active_vip_connections) == 2);
        ASSERT_TRUE(aws_atomic_load_int(&mock_client->stats.num_warm_vip_connections) == 2);

        ASSERT_TRUE(vip_connection_0->is_warm);
        ASSERT_TRUE(vip_connection_0->is_active);
        ASSERT_TRUE(vip_connection_0->request == request_0);

        ASSERT_TRUE(vip_connection_1->is_warm);
        ASSERT_TRUE(vip_connection_1->is_active);
        ASSERT_TRUE(vip_connection_1->request == request_1);
    }

    aws_s3_request_release(request_0);
    aws_s3_request_release(request_1);
    aws_s3_request_release(request_2);
    aws_s3_meta_request_release(mock_meta_request);

    aws_mem_release(tester.allocator, vip_connection_0);
    aws_mem_release(tester.allocator, vip_connection_1);
    aws_mem_release(tester.allocator, mock_vip);

    aws_s3_client_release(mock_client);
    aws_s3_tester_clean_up(&tester);

    return 0;
}

/* Test that queued requests will not spin up new connections if the owning meta request prefers a smaller amount of
 * active connections. */
AWS_TEST_CASE(test_s3_client_update_connections_too_many_conns, s_test_s3_client_update_connections_too_many_conns)
static int s_test_s3_client_update_connections_too_many_conns(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    aws_s3_tester_init(allocator, &tester);

    struct aws_s3_vip *mock_vip = aws_s3_tester_mock_vip_new(&tester);
    aws_atomic_init_int(&mock_vip->active, 1);

    struct aws_s3_vip_connection *vip_connection = aws_s3_tester_mock_vip_connection_new(&tester);
    vip_connection->owning_vip = mock_vip;

    struct aws_s3_meta_request *mock_meta_request = aws_s3_tester_mock_meta_request_new(&tester);
    struct aws_s3_request *request = aws_s3_request_new(mock_meta_request, 0, 0, 0);

    struct aws_s3_client *mock_client = aws_s3_tester_mock_client_new(&tester);
    *((uint32_t *)&mock_client->ideal_vip_count) = 1;
    aws_linked_list_init(&mock_client->threaded_data.idle_vip_connections);
    aws_linked_list_push_back(&mock_client->threaded_data.idle_vip_connections, &vip_connection->node);

    aws_linked_list_init(&mock_client->threaded_data.request_queue);

    aws_atomic_store_int(
        &mock_client->stats.num_active_vip_connections, aws_s3_client_get_max_active_connections(mock_client, 0));
    aws_linked_list_push_back(&mock_client->threaded_data.request_queue, &request->node);
    ++mock_client->threaded_data.request_queue_size;

    /* Request should stay in queue due to more active connections than the meta request wants, and there not being any
     * warm connections. */
    {
        aws_s3_client_update_connections_threaded(mock_client, true);

        ASSERT_TRUE(mock_client->threaded_data.request_queue_size == 1);
        ASSERT_TRUE(!aws_linked_list_empty(&mock_client->threaded_data.request_queue));

        ASSERT_FALSE(vip_connection->is_warm);
        ASSERT_FALSE(vip_connection->is_active);
        ASSERT_TRUE(vip_connection->request == NULL);
    }

    /* Make the vip connection warm, which should cause the request to dequeue. */
    {
        aws_s3_client_set_vip_connection_warm(mock_client, vip_connection, true);

        aws_s3_client_update_connections_threaded(mock_client, true);

        ASSERT_TRUE(mock_client->threaded_data.request_queue_size == 0);
        ASSERT_TRUE(aws_linked_list_empty(&mock_client->threaded_data.request_queue));

        ASSERT_TRUE(vip_connection->is_warm);
        ASSERT_TRUE(vip_connection->is_active);
        ASSERT_TRUE(vip_connection->request == request);
    }

    aws_s3_request_release(request);
    aws_s3_meta_request_release(mock_meta_request);

    aws_mem_release(tester.allocator, vip_connection);
    aws_mem_release(tester.allocator, mock_vip);

    aws_s3_client_release(mock_client);
    aws_s3_tester_clean_up(&tester);

    return 0;
}

struct s3_test_update_connections_finish_result_user_data {
    struct aws_s3_request *request;
    uint32_t call_counter;
};

static void s_s3_test_meta_request_has_finish_result_finished_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    int error_code) {
    AWS_ASSERT(meta_request);
    AWS_ASSERT(request);
    (void)error_code;

    struct s3_test_update_connections_finish_result_user_data *user_data = meta_request->user_data;
    user_data->request = request;
    ++user_data->call_counter;
}

/* Test that the client will correctly discard requests for meta requests that are trying to finish. */
AWS_TEST_CASE(test_s3_client_update_connections_finish_result, s_test_s3_client_update_connections_finish_result)
static int s_test_s3_client_update_connections_finish_result(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    aws_s3_tester_init(allocator, &tester);

    struct aws_s3_vip *mock_vip = aws_s3_tester_mock_vip_new(&tester);
    aws_atomic_init_int(&mock_vip->active, 1);

    struct aws_s3_vip_connection *vip_connection = aws_s3_tester_mock_vip_connection_new(&tester);
    vip_connection->owning_vip = mock_vip;

    struct s3_test_update_connections_finish_result_user_data test_update_connections_finish_result_user_data;
    AWS_ZERO_STRUCT(test_update_connections_finish_result_user_data);

    struct aws_s3_meta_request *mock_meta_request = aws_s3_tester_mock_meta_request_new(&tester);
    mock_meta_request->synced_data.finish_result_set = true;
    mock_meta_request->user_data = &test_update_connections_finish_result_user_data;

    struct aws_s3_meta_request_vtable *mock_meta_request_vtable =
        aws_s3_tester_patch_meta_request_vtable(&tester, mock_meta_request, NULL);
    mock_meta_request_vtable->finished_request = s_s3_test_meta_request_has_finish_result_finished_request;

    struct aws_s3_client *mock_client = aws_s3_tester_mock_client_new(&tester);

    *((uint32_t *)&mock_client->ideal_vip_count) = 1;
    aws_linked_list_init(&mock_client->threaded_data.idle_vip_connections);
    aws_linked_list_push_back(&mock_client->threaded_data.idle_vip_connections, &vip_connection->node);

    aws_linked_list_init(&mock_client->threaded_data.request_queue);

    /* Verify that the request does not get sent. */
    {
        struct aws_s3_request *request = aws_s3_request_new(mock_meta_request, 0, 0, 0);
        aws_linked_list_push_back(&mock_client->threaded_data.request_queue, &request->node);
        ++mock_client->threaded_data.request_queue_size;

        aws_s3_client_update_connections_threaded(mock_client, true);

        ASSERT_TRUE(mock_client->threaded_data.request_queue_size == 0);
        ASSERT_TRUE(aws_linked_list_empty(&mock_client->threaded_data.request_queue));
        ASSERT_TRUE(aws_atomic_load_int(&mock_client->stats.num_active_vip_connections) == 0);

        ASSERT_TRUE(test_update_connections_finish_result_user_data.request == request);
        ASSERT_TRUE(test_update_connections_finish_result_user_data.call_counter == 1);

        ASSERT_FALSE(vip_connection->is_warm);
        ASSERT_FALSE(vip_connection->is_active);
        ASSERT_TRUE(vip_connection->request == NULL);

        test_update_connections_finish_result_user_data.request = 0;
        test_update_connections_finish_result_user_data.call_counter = 0;
    }

    /* Verify that the request still gets sent because it has the 'always send' flag. */
    {
        struct aws_s3_request *request = aws_s3_request_new(mock_meta_request, 0, 0, AWS_S3_REQUEST_FLAG_ALWAYS_SEND);
        aws_linked_list_push_back(&mock_client->threaded_data.request_queue, &request->node);
        ++mock_client->threaded_data.request_queue_size;

        aws_s3_client_update_connections_threaded(mock_client, true);

        ASSERT_TRUE(mock_client->threaded_data.request_queue_size == 0);
        ASSERT_TRUE(aws_linked_list_empty(&mock_client->threaded_data.request_queue));
        ASSERT_TRUE(aws_atomic_load_int(&mock_client->stats.num_active_vip_connections) == 1);

        ASSERT_TRUE(test_update_connections_finish_result_user_data.request == NULL);
        ASSERT_TRUE(test_update_connections_finish_result_user_data.call_counter == 0);

        ASSERT_TRUE(vip_connection->is_warm);
        ASSERT_TRUE(vip_connection->is_active);
        ASSERT_TRUE(vip_connection->request == request);

        aws_s3_request_release(request);
    }

    aws_s3_meta_request_release(mock_meta_request);

    aws_mem_release(tester.allocator, vip_connection);
    aws_mem_release(tester.allocator, mock_vip);

    aws_s3_client_release(mock_client);
    aws_s3_tester_clean_up(&tester);
    return 0;
}

struct s3_test_update_connections_clean_up_user_data {
    uint32_t call_count;
};

static void s_s3_test_update_connections_vip_connection_destroy(
    struct aws_s3_client *client,
    struct aws_s3_vip_connection *vip_connection) {
    (void)vip_connection;

    struct s3_test_update_connections_clean_up_user_data *test_data = client->shutdown_callback_user_data;

    ++test_data->call_count;
}

static bool s_http_connection_is_open_return_true(const struct aws_http_connection *http_connection) {
    (void)http_connection;
    return true;
}

static bool s_http_connection_is_open_return_false(const struct aws_http_connection *http_connection) {
    (void)http_connection;
    return false;
}

/* Test that connections are cleaned up by the update connections function given the correct conditions. */
AWS_TEST_CASE(test_s3_client_update_connections_clean_up, s_test_s3_client_update_connections_clean_up)
static int s_test_s3_client_update_connections_clean_up(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    aws_s3_tester_init(allocator, &tester);

    struct s3_test_update_connections_clean_up_user_data test_data;
    AWS_ZERO_STRUCT(test_data);

    struct aws_s3_vip *mock_vip = aws_s3_tester_mock_vip_new(&tester);
    aws_atomic_init_int(&mock_vip->active, 1);

    struct aws_s3_vip_connection *vip_connection = aws_s3_tester_mock_vip_connection_new(&tester);
    vip_connection->owning_vip = mock_vip;

    struct aws_s3_client *mock_client = aws_s3_tester_mock_client_new(&tester);
    struct aws_s3_client_vtable *mock_client_vtable = aws_s3_tester_patch_client_vtable(&tester, mock_client, NULL);
    mock_client->vtable->vip_connection_destroy = s_s3_test_update_connections_vip_connection_destroy;
    mock_client->shutdown_callback_user_data = &test_data;

    struct aws_http_connection *mock_http_connection = (struct aws_http_connection *)1;

    aws_linked_list_init(&mock_client->threaded_data.request_queue);
    aws_linked_list_init(&mock_client->threaded_data.idle_vip_connections);

    aws_linked_list_push_back(&mock_client->threaded_data.idle_vip_connections, &vip_connection->node);

    for (uint32_t test_truth_value = 0; test_truth_value < 32; ++test_truth_value) {
        bool vip_is_active = (test_truth_value & 0x01) != 0;
        bool client_is_active = (test_truth_value & 0x02) != 0;
        bool http_connection_null = (test_truth_value & 0x04) != 0;
        bool http_connection_open = (test_truth_value & 0x08) != 0;
        bool request_count_at_max = (test_truth_value & 0x10) != 0;

        if (vip_is_active) {
            aws_atomic_store_int(&mock_vip->active, 1);
        } else {
            aws_atomic_store_int(&mock_vip->active, 0);
        }

        if (http_connection_null) {
            vip_connection->http_connection = NULL;
        } else {
            vip_connection->http_connection = mock_http_connection;
        }

        if (http_connection_open) {
            mock_client_vtable->http_connection_is_open = s_http_connection_is_open_return_true;
        } else {
            mock_client_vtable->http_connection_is_open = s_http_connection_is_open_return_false;
        }

        if (request_count_at_max) {
            vip_connection->request_count = INT_MAX;
        } else {
            vip_connection->request_count = 0;
        }

        aws_s3_client_update_connections_threaded(mock_client, client_is_active);

        if (!vip_is_active &&
            (!client_is_active || (http_connection_null || !http_connection_open || request_count_at_max))) {

            ASSERT_TRUE(test_data.call_count == 1);
            test_data.call_count = 0;

            aws_linked_list_push_back(&mock_client->threaded_data.idle_vip_connections, &vip_connection->node);

        } else {

            ASSERT_TRUE(test_data.call_count == 0);
            test_data.call_count = 0;
        }
    }

    aws_mem_release(allocator, vip_connection);
    aws_mem_release(allocator, mock_vip);
    aws_s3_client_release(mock_client);
    aws_s3_tester_clean_up(&tester);

    return 0;
}

static int s_test_s3_get_object_helper(
    struct aws_allocator *allocator,
    enum aws_s3_client_tls_usage tls_usage,
    uint32_t extra_meta_request_flag,
    struct aws_byte_cursor s3_path) {
    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client_config client_config = {
        .part_size = 64 * 1024,
    };

    struct aws_tls_ctx_options tls_context_options;
    aws_tls_ctx_options_init_default_client(&tls_context_options, allocator);
    struct aws_tls_ctx *context = aws_tls_client_ctx_new(allocator, &tls_context_options);

    struct aws_tls_connection_options tls_connection_options;
    aws_tls_connection_options_init_from_ctx(&tls_connection_options, context);

    struct aws_string *endpoint =
        aws_s3_tester_build_endpoint_string(allocator, &g_test_bucket_name, &g_test_s3_region);
    struct aws_byte_cursor endpoint_cursor = aws_byte_cursor_from_string(endpoint);

    tls_connection_options.server_name = aws_string_new_from_cursor(allocator, &endpoint_cursor);

    switch (tls_usage) {
        case AWS_S3_TLS_ENABLED:
            client_config.tls_mode = AWS_MR_TLS_ENABLED;
            client_config.tls_connection_options = &tls_connection_options;
            break;
        case AWS_S3_TLS_DISABLED:
            client_config.tls_mode = AWS_MR_TLS_DISABLED;
            break;
        default:
            break;
    }

    ASSERT_SUCCESS(aws_s3_tester_bind_client(
        &tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION | AWS_S3_TESTER_BIND_CLIENT_SIGNING));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    uint32_t flags = AWS_S3_TESTER_SEND_META_REQUEST_EXPECT_SUCCESS | extra_meta_request_flag;
    ASSERT_SUCCESS(aws_s3_tester_send_get_object_meta_request(&tester, client, s3_path, flags, NULL));

    aws_string_destroy(endpoint);
    aws_tls_ctx_release(context);
    aws_tls_connection_options_clean_up(&tls_connection_options);
    aws_tls_ctx_options_clean_up(&tls_context_options);

    aws_s3_client_release(client);
    client = NULL;

    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(test_s3_get_object_tls_disabled, s_test_s3_get_object_tls_disabled)
static int s_test_s3_get_object_tls_disabled(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(s_test_s3_get_object_helper(allocator, AWS_S3_TLS_DISABLED, 0, g_s3_path_get_object_test_1MB));

    return 0;
}

AWS_TEST_CASE(test_s3_get_object_tls_enabled, s_test_s3_get_object_tls_enabled)
static int s_test_s3_get_object_tls_enabled(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(s_test_s3_get_object_helper(allocator, AWS_S3_TLS_ENABLED, 0, g_s3_path_get_object_test_1MB));

    return 0;
}

AWS_TEST_CASE(test_s3_get_object_tls_default, s_test_s3_get_object_tls_default)
static int s_test_s3_get_object_tls_default(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(s_test_s3_get_object_helper(allocator, AWS_S3_TLS_ENABLED, 0, g_s3_path_get_object_test_1MB));

    return 0;
}

AWS_TEST_CASE(test_s3_no_signing, s_test_s3_no_signing)
static int s_test_s3_no_signing(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client_config client_config;
    AWS_ZERO_STRUCT(client_config);

    ASSERT_SUCCESS(aws_s3_tester_bind_client(&tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    struct aws_string *host_name =
        aws_s3_tester_build_endpoint_string(allocator, &g_test_public_bucket_name, &g_test_s3_region);

    /* Put together a simple S3 Get Object request. */
    struct aws_http_message *message = aws_s3_test_get_object_request_new(
        allocator, aws_byte_cursor_from_string(host_name), g_s3_path_get_object_test_1MB);

    struct aws_s3_meta_request_options options;
    AWS_ZERO_STRUCT(options);
    options.type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT;
    options.message = message;

    /* Trigger accelerating of our Get Object request. */
    struct aws_s3_meta_request_test_results meta_request_test_results;
    AWS_ZERO_STRUCT(meta_request_test_results);

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request(
        &tester, client, &options, &meta_request_test_results, AWS_S3_TESTER_SEND_META_REQUEST_EXPECT_SUCCESS));
    ASSERT_SUCCESS(aws_s3_tester_validate_get_object_results(&meta_request_test_results, 0));

    aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);

    aws_http_message_release(message);
    aws_string_destroy(host_name);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(test_s3_signing_override, s_test_s3_signing_override)
static int s_test_s3_signing_override(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client_config client_config;
    AWS_ZERO_STRUCT(client_config);

    ASSERT_SUCCESS(aws_s3_tester_bind_client(&tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    struct aws_string *host_name =
        aws_s3_tester_build_endpoint_string(allocator, &g_test_bucket_name, &g_test_s3_region);

    /* Put together a simple S3 Get Object request. */
    struct aws_http_message *message = aws_s3_test_get_object_request_new(
        allocator, aws_byte_cursor_from_string(host_name), g_s3_path_get_object_test_1MB);

    /* Getting without signing should fail since the client has no signing set up. */
    {
        struct aws_s3_meta_request_options options;
        AWS_ZERO_STRUCT(options);
        options.type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT;
        options.message = message;

        /* Trigger accelerating of our Get Object request.*/
        struct aws_s3_meta_request_test_results meta_request_test_results;
        AWS_ZERO_STRUCT(meta_request_test_results);

        ASSERT_SUCCESS(aws_s3_tester_send_meta_request(&tester, client, &options, &meta_request_test_results, 0));
        ASSERT_TRUE(aws_s3_tester_validate_get_object_results(&meta_request_test_results, 0) != AWS_OP_SUCCESS);

        aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);
    }

    /* Getting with signing should succeed if we set up signing on the meta request. */
    {
        struct aws_s3_meta_request_options options;
        AWS_ZERO_STRUCT(options);
        options.type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT;
        options.message = message;
        options.signing_config = &tester.default_signing_config;

        /* Trigger accelerating of our Get Object request. */
        struct aws_s3_meta_request_test_results meta_request_test_results;
        AWS_ZERO_STRUCT(meta_request_test_results);

        ASSERT_SUCCESS(aws_s3_tester_send_meta_request(
            &tester, client, &options, &meta_request_test_results, AWS_S3_TESTER_SEND_META_REQUEST_EXPECT_SUCCESS));
        ASSERT_SUCCESS(aws_s3_tester_validate_get_object_results(&meta_request_test_results, 0));

        aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);
    }

    aws_http_message_release(message);
    aws_string_destroy(host_name);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(test_s3_get_object_less_than_part_size, s_test_s3_get_object_less_than_part_size)
static int s_test_s3_get_object_less_than_part_size(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client_config client_config = {
        .part_size = 20 * 1024 * 1024,
    };

    ASSERT_SUCCESS(aws_s3_tester_bind_client(
        &tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION | AWS_S3_TESTER_BIND_CLIENT_SIGNING));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    ASSERT_SUCCESS(aws_s3_tester_send_get_object_meta_request(
        &tester, client, g_s3_path_get_object_test_1MB, AWS_S3_TESTER_SEND_META_REQUEST_EXPECT_SUCCESS, NULL));

    aws_s3_client_release(client);
    client = NULL;

    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(test_s3_put_object_with_part_remainder, s_test_s3_put_object_with_part_remainder)
static int s_test_s3_put_object_with_part_remainder(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_tester_client_options client_options = {
        .part_size = 5 * 1024 * 1024,
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_s3_meta_request_test_results meta_request_test_results;
    AWS_ZERO_STRUCT(meta_request_test_results);

    struct aws_s3_tester_meta_request_options options = {
        .allocator = allocator,
        .client = client,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_SUCCESS,
        .put_options =
            {
                /* Object size meant to be one megabyte larger than the part size of the client. */
                .object_size_mb = 6,
            },
    };

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &options, &meta_request_test_results));
    aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);

    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(test_s3_get_object_multiple, s_test_s3_get_object_multiple)
static int s_test_s3_get_object_multiple(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    const struct aws_byte_cursor test_object_path = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("/get_object_test_1MB.txt");

    struct aws_s3_meta_request *meta_requests[4];
    struct aws_s3_meta_request_test_results meta_request_test_results[4];
    size_t num_meta_requests = sizeof(meta_requests) / sizeof(struct aws_s3_meta_request *);

    ASSERT_TRUE(
        num_meta_requests == (sizeof(meta_request_test_results) / sizeof(struct aws_s3_meta_request_test_results)));

    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client_config client_config = {
        .part_size = 64 * 1024,
    };

    ASSERT_SUCCESS(aws_s3_tester_bind_client(
        &tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION | AWS_S3_TESTER_BIND_CLIENT_SIGNING));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    struct aws_string *host_name =
        aws_s3_tester_build_endpoint_string(allocator, &g_test_bucket_name, &g_test_s3_region);

    /* Put together a simple S3 Get Object request. */
    struct aws_http_message *message =
        aws_s3_test_get_object_request_new(allocator, aws_byte_cursor_from_string(host_name), test_object_path);

    for (size_t i = 0; i < num_meta_requests; ++i) {
        AWS_ZERO_STRUCT(meta_request_test_results[i]);

        struct aws_s3_meta_request_options options;
        AWS_ZERO_STRUCT(options);
        options.type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT;
        options.message = message;

        ASSERT_SUCCESS(aws_s3_tester_bind_meta_request(&tester, &options, &meta_request_test_results[i]));

        /* Trigger accelerating of our Get Object request. */
        meta_requests[i] = aws_s3_client_make_meta_request(client, &options);

        ASSERT_TRUE(meta_requests[i] != NULL);
    }

    /* Wait for the request to finish. */
    aws_s3_tester_wait_for_meta_request_finish(&tester);

    aws_s3_tester_lock_synced_data(&tester);
    ASSERT_TRUE(tester.synced_data.finish_error_code == AWS_ERROR_SUCCESS);
    aws_s3_tester_unlock_synced_data(&tester);

    for (size_t i = 0; i < num_meta_requests; ++i) {
        aws_s3_meta_request_release(meta_requests[i]);
        meta_requests[i] = NULL;
    }

    aws_s3_tester_wait_for_meta_request_shutdown(&tester);

    for (size_t i = 0; i < num_meta_requests; ++i) {
        aws_s3_tester_validate_get_object_results(&meta_request_test_results[i], 0);
        aws_s3_meta_request_test_results_clean_up(&meta_request_test_results[i]);
    }

    aws_http_message_release(message);
    message = NULL;

    aws_string_destroy(host_name);
    host_name = NULL;

    aws_s3_client_release(client);
    client = NULL;

    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(test_s3_get_object_empty_object, s_test_s3_get_object_empty_default)
static int s_test_s3_get_object_empty_default(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    return (s_test_s3_get_object_helper(
        allocator, AWS_S3_TLS_ENABLED, 0, aws_byte_cursor_from_c_str("/get_object_test_0MB.txt")));
}

AWS_TEST_CASE(test_s3_get_object_sse_kms, s_test_s3_get_object_sse_kms)
static int s_test_s3_get_object_sse_kms(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    /* Keep TLS enabled for SSE related download, or it will fail. */
    return s_test_s3_get_object_helper(
        allocator,
        AWS_S3_TLS_ENABLED,
        AWS_S3_TESTER_SEND_META_REQUEST_SSE_KMS,
        aws_byte_cursor_from_c_str("/get_object_test_kms_10MB.txt"));
}

AWS_TEST_CASE(test_s3_get_object_sse_aes256, s_test_s3_get_object_sse_aes256)
static int s_test_s3_get_object_sse_aes256(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    /* Keep TLS enabled for SSE related download, or it will fail. */
    return s_test_s3_get_object_helper(
        allocator,
        AWS_S3_TLS_ENABLED,
        AWS_S3_TESTER_SEND_META_REQUEST_SSE_AES256,
        aws_byte_cursor_from_c_str("/get_object_test_aes256_10MB.txt"));
}

static int s_test_s3_put_object_helper(
    struct aws_allocator *allocator,
    enum aws_s3_client_tls_usage tls_usage,
    uint32_t extra_meta_request_flag) {
    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_tls_ctx_options tls_context_options;
    aws_tls_ctx_options_init_default_client(&tls_context_options, allocator);
    struct aws_tls_ctx *context = aws_tls_client_ctx_new(allocator, &tls_context_options);

    struct aws_tls_connection_options tls_connection_options;
    aws_tls_connection_options_init_from_ctx(&tls_connection_options, context);

    struct aws_string *endpoint =
        aws_s3_tester_build_endpoint_string(allocator, &g_test_bucket_name, &g_test_s3_region);
    struct aws_byte_cursor endpoint_cursor = aws_byte_cursor_from_string(endpoint);

    tls_connection_options.server_name = aws_string_new_from_cursor(allocator, &endpoint_cursor);

    struct aws_s3_client_config client_config = {
        .part_size = 5 * 1024 * 1024,
    };

    switch (tls_usage) {
        case AWS_S3_TLS_ENABLED:
            client_config.tls_mode = AWS_MR_TLS_ENABLED;
            client_config.tls_connection_options = &tls_connection_options;
            break;
        case AWS_S3_TLS_DISABLED:
            client_config.tls_mode = AWS_MR_TLS_DISABLED;
            break;
        default:
            break;
    }

    ASSERT_SUCCESS(aws_s3_tester_bind_client(
        &tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION | AWS_S3_TESTER_BIND_CLIENT_SIGNING));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    ASSERT_SUCCESS(aws_s3_tester_send_put_object_meta_request(
        &tester, client, 10, AWS_S3_TESTER_SEND_META_REQUEST_EXPECT_SUCCESS | extra_meta_request_flag, NULL));

    aws_string_destroy(endpoint);
    aws_tls_ctx_release(context);
    aws_tls_connection_options_clean_up(&tls_connection_options);
    aws_tls_ctx_options_clean_up(&tls_context_options);

    aws_s3_client_release(client);
    client = NULL;

    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(test_s3_put_object_tls_disabled, s_test_s3_put_object_tls_disabled)
static int s_test_s3_put_object_tls_disabled(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(s_test_s3_put_object_helper(allocator, AWS_S3_TLS_DISABLED, 0));

    return 0;
}

AWS_TEST_CASE(test_s3_put_object_tls_enabled, s_test_s3_put_object_tls_enabled)
static int s_test_s3_put_object_tls_enabled(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(s_test_s3_put_object_helper(allocator, AWS_S3_TLS_ENABLED, 0));

    return 0;
}

AWS_TEST_CASE(test_s3_put_object_tls_default, s_test_s3_put_object_tls_default)
static int s_test_s3_put_object_tls_default(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(s_test_s3_put_object_helper(allocator, AWS_S3_TLS_DEFAULT, 0));

    return 0;
}

AWS_TEST_CASE(test_s3_multipart_put_object_with_acl, s_test_s3_multipart_put_object_with_acl)
static int s_test_s3_multipart_put_object_with_acl(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(s_test_s3_put_object_helper(allocator, AWS_S3_TLS_DEFAULT, AWS_S3_TESTER_SEND_META_REQUEST_PUT_ACL));

    return 0;
}

AWS_TEST_CASE(test_s3_put_object_multiple, s_test_s3_put_object_multiple)
static int s_test_s3_put_object_multiple(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_meta_request *meta_requests[2];
    struct aws_s3_meta_request_test_results meta_request_test_results[2];
    struct aws_http_message *messages[2];
    struct aws_input_stream *input_streams[2];
    struct aws_byte_buf input_stream_buffers[2];
    size_t num_meta_requests = sizeof(meta_requests) / sizeof(struct aws_s3_meta_request *);

    ASSERT_TRUE(
        num_meta_requests == (sizeof(meta_request_test_results) / sizeof(struct aws_s3_meta_request_test_results)));

    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client_config client_config;
    AWS_ZERO_STRUCT(client_config);

    ASSERT_SUCCESS(aws_s3_tester_bind_client(
        &tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION | AWS_S3_TESTER_BIND_CLIENT_SIGNING));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    struct aws_string *host_name =
        aws_s3_tester_build_endpoint_string(allocator, &g_test_bucket_name, &g_test_s3_region);

    for (size_t i = 0; i < num_meta_requests; ++i) {
        AWS_ZERO_STRUCT(meta_request_test_results[i]);
        char object_path_buffer[128] = "";
        snprintf(object_path_buffer, sizeof(object_path_buffer), "/get_object_test_10MB_%zu.txt", i);
        AWS_ZERO_STRUCT(input_stream_buffers[i]);
        aws_s3_create_test_buffer(allocator, 10 * 1024ULL * 1024ULL, &input_stream_buffers[i]);
        struct aws_byte_cursor test_body_cursor = aws_byte_cursor_from_buf(&input_stream_buffers[i]);
        input_streams[i] = aws_input_stream_new_from_cursor(allocator, &test_body_cursor);
        struct aws_byte_cursor test_object_path = aws_byte_cursor_from_c_str(object_path_buffer);
        messages[i] = aws_s3_test_put_object_request_new(
            allocator,
            aws_byte_cursor_from_string(host_name),
            test_object_path,
            g_test_body_content_type,
            input_streams[i],
            0);
        struct aws_s3_meta_request_options options;
        AWS_ZERO_STRUCT(options);
        options.type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT;
        options.message = messages[i];

        ASSERT_SUCCESS(aws_s3_tester_bind_meta_request(&tester, &options, &meta_request_test_results[i]));

        /* Trigger accelerating of our Put Object request. */
        meta_requests[i] = aws_s3_client_make_meta_request(client, &options);

        ASSERT_TRUE(meta_requests[i] != NULL);
    }

    /* Wait for the request to finish. */
    aws_s3_tester_wait_for_meta_request_finish(&tester);

    aws_s3_tester_lock_synced_data(&tester);
    ASSERT_TRUE(tester.synced_data.finish_error_code == AWS_ERROR_SUCCESS);
    aws_s3_tester_unlock_synced_data(&tester);

    for (size_t i = 0; i < num_meta_requests; ++i) {
        aws_s3_meta_request_release(meta_requests[i]);
        meta_requests[i] = NULL;
    }

    aws_s3_tester_wait_for_meta_request_shutdown(&tester);

    for (size_t i = 0; i < num_meta_requests; ++i) {
        aws_s3_tester_validate_get_object_results(&meta_request_test_results[i], 0);
        aws_s3_meta_request_test_results_clean_up(&meta_request_test_results[i]);
    }

    for (size_t i = 0; i < num_meta_requests; ++i) {
        aws_http_message_release(messages[i]);
        aws_input_stream_destroy(input_streams[i]);
        aws_byte_buf_clean_up(&input_stream_buffers[i]);
    }

    aws_string_destroy(host_name);
    host_name = NULL;

    aws_s3_client_release(client);
    client = NULL;

    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(test_s3_put_object_less_than_part_size, s_test_s3_put_object_less_than_part_size)
static int s_test_s3_put_object_less_than_part_size(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client_config client_config = {
        .part_size = 20 * 1024 * 1024,
    };

    ASSERT_SUCCESS(aws_s3_tester_bind_client(
        &tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION | AWS_S3_TESTER_BIND_CLIENT_SIGNING));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    ASSERT_TRUE(client != NULL);

    ASSERT_SUCCESS(aws_s3_tester_send_put_object_meta_request(
        &tester, client, 10, AWS_S3_TESTER_SEND_META_REQUEST_EXPECT_SUCCESS, NULL));

    aws_s3_client_release(client);
    client = NULL;

    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(test_s3_put_object_empty_object, s_test_s3_put_object_empty_object)
static int s_test_s3_put_object_empty_object(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client_config client_config;
    AWS_ZERO_STRUCT(client_config);

    ASSERT_SUCCESS(aws_s3_tester_bind_client(
        &tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION | AWS_S3_TESTER_BIND_CLIENT_SIGNING));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    ASSERT_TRUE(client != NULL);

    ASSERT_SUCCESS(aws_s3_tester_send_put_object_meta_request(
        &tester, client, 0, AWS_S3_TESTER_SEND_META_REQUEST_EXPECT_SUCCESS, NULL));

    aws_s3_client_release(client);
    client = NULL;

    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(test_s3_put_object_sse_kms, s_test_s3_put_object_sse_kms)
static int s_test_s3_put_object_sse_kms(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client_config client_config = {
        .part_size = 20 * 1024 * 1024,
    };

    ASSERT_SUCCESS(aws_s3_tester_bind_client(
        &tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION | AWS_S3_TESTER_BIND_CLIENT_SIGNING));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    ASSERT_TRUE(client != NULL);

    ASSERT_SUCCESS(aws_s3_tester_send_put_object_meta_request(
        &tester,
        client,
        10,
        AWS_S3_TESTER_SEND_META_REQUEST_EXPECT_SUCCESS | AWS_S3_TESTER_SEND_META_REQUEST_SSE_KMS,
        NULL));

    aws_s3_client_release(client);
    client = NULL;

    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(test_s3_put_object_sse_kms_multipart, s_test_s3_put_object_sse_kms_multipart)
static int s_test_s3_put_object_sse_kms_multipart(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client_config client_config = {
        .part_size = 5 * 1024 * 1024,
    };

    ASSERT_SUCCESS(aws_s3_tester_bind_client(
        &tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION | AWS_S3_TESTER_BIND_CLIENT_SIGNING));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    ASSERT_TRUE(client != NULL);

    ASSERT_SUCCESS(aws_s3_tester_send_put_object_meta_request(
        &tester,
        client,
        10,
        AWS_S3_TESTER_SEND_META_REQUEST_EXPECT_SUCCESS | AWS_S3_TESTER_SEND_META_REQUEST_SSE_KMS,
        NULL));

    aws_s3_client_release(client);
    client = NULL;

    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(test_s3_put_object_sse_aes256, s_test_s3_put_object_sse_aes256)
static int s_test_s3_put_object_sse_aes256(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client_config client_config = {
        .part_size = 20 * 1024 * 1024,
    };

    ASSERT_SUCCESS(aws_s3_tester_bind_client(
        &tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION | AWS_S3_TESTER_BIND_CLIENT_SIGNING));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    ASSERT_TRUE(client != NULL);

    ASSERT_SUCCESS(aws_s3_tester_send_put_object_meta_request(
        &tester,
        client,
        10,
        AWS_S3_TESTER_SEND_META_REQUEST_EXPECT_SUCCESS | AWS_S3_TESTER_SEND_META_REQUEST_SSE_AES256,
        NULL));

    aws_s3_client_release(client);
    client = NULL;

    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(test_s3_put_object_sse_aes256_multipart, s_test_s3_put_object_sse_aes256_multipart)
static int s_test_s3_put_object_sse_aes256_multipart(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client_config client_config = {
        .part_size = 5 * 1024 * 1024,
    };

    ASSERT_SUCCESS(aws_s3_tester_bind_client(
        &tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION | AWS_S3_TESTER_BIND_CLIENT_SIGNING));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    ASSERT_TRUE(client != NULL);

    ASSERT_SUCCESS(aws_s3_tester_send_put_object_meta_request(
        &tester,
        client,
        10,
        AWS_S3_TESTER_SEND_META_REQUEST_EXPECT_SUCCESS | AWS_S3_TESTER_SEND_META_REQUEST_SSE_AES256,
        NULL));

    aws_s3_client_release(client);
    client = NULL;

    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(test_s3_put_object_double_slashes, s_test_s3_put_object_double_slashes)
static int s_test_s3_put_object_double_slashes(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_meta_request_test_results meta_request_test_results;
    AWS_ZERO_STRUCT(meta_request_test_results);

    struct aws_s3_tester_meta_request_options options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .put_options =
            {
                .object_size_mb = 1,
                .object_path_override = aws_byte_cursor_from_c_str("/prefix//test.txt"),
            },
    };

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(NULL, &options, &meta_request_test_results));

    aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);

    return 0;
}

AWS_TEST_CASE(test_s3_meta_request_default, s_test_s3_meta_request_default)
static int s_test_s3_meta_request_default(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client_config client_config;
    AWS_ZERO_STRUCT(client_config);

    ASSERT_SUCCESS(aws_s3_tester_bind_client(
        &tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION | AWS_S3_TESTER_BIND_CLIENT_SIGNING));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    const struct aws_byte_cursor test_object_path = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("/get_object_test_1MB.txt");

    struct aws_string *host_name =
        aws_s3_tester_build_endpoint_string(allocator, &g_test_bucket_name, &g_test_s3_region);

    /* Put together a simple S3 Get Object request. */
    struct aws_http_message *message =
        aws_s3_test_get_object_request_new(allocator, aws_byte_cursor_from_string(host_name), test_object_path);

    struct aws_s3_meta_request_options options;
    AWS_ZERO_STRUCT(options);

    /* Pass the request through as a default request so that it goes through as-is. */
    options.type = AWS_S3_META_REQUEST_TYPE_DEFAULT;
    options.message = message;

    struct aws_s3_meta_request_test_results meta_request_test_results;
    AWS_ZERO_STRUCT(meta_request_test_results);

    ASSERT_SUCCESS(aws_s3_tester_bind_meta_request(&tester, &options, &meta_request_test_results));

    struct aws_s3_meta_request *meta_request = aws_s3_client_make_meta_request(client, &options);

    ASSERT_TRUE(meta_request != NULL);

    /* Wait for the request to finish. */
    aws_s3_tester_wait_for_meta_request_finish(&tester);

    aws_s3_tester_lock_synced_data(&tester);

    ASSERT_TRUE(tester.synced_data.finish_error_code == AWS_ERROR_SUCCESS);

    aws_s3_tester_unlock_synced_data(&tester);

    ASSERT_SUCCESS(aws_s3_tester_validate_get_object_results(&meta_request_test_results, 0));

    aws_s3_meta_request_release(meta_request);
    meta_request = NULL;

    aws_s3_tester_wait_for_meta_request_shutdown(&tester);

    aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);

    aws_http_message_release(message);
    message = NULL;

    aws_string_destroy(host_name);
    host_name = NULL;

    aws_s3_client_release(client);
    client = NULL;

    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(test_s3_error_missing_file, s_test_s3_error_missing_file)
static int s_test_s3_error_missing_file(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    const struct aws_byte_cursor test_object_path =
        AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("/non-existing-file12345.txt");

    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client_config client_config = {
        .part_size = 64 * 1024,
    };

    ASSERT_SUCCESS(aws_s3_tester_bind_client(
        &tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION | AWS_S3_TESTER_BIND_CLIENT_SIGNING));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    struct aws_string *host_name =
        aws_s3_tester_build_endpoint_string(allocator, &g_test_bucket_name, &g_test_s3_region);

    /* Put together a simple S3 Get Object request. */
    struct aws_http_message *message =
        aws_s3_test_get_object_request_new(allocator, aws_byte_cursor_from_string(host_name), test_object_path);

    struct aws_s3_meta_request_options options;
    AWS_ZERO_STRUCT(options);
    options.type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT;
    options.message = message;

    /* Trigger accelerating of our Get Object request. */
    struct aws_s3_meta_request_test_results meta_request_test_results;
    AWS_ZERO_STRUCT(meta_request_test_results);

    ASSERT_SUCCESS(aws_s3_tester_bind_meta_request(&tester, &options, &meta_request_test_results));

    struct aws_s3_meta_request *meta_request = aws_s3_client_make_meta_request(client, &options);

    ASSERT_TRUE(meta_request != NULL);

    /* Wait for the request to finish. */
    aws_s3_tester_wait_for_meta_request_finish(&tester);

    aws_s3_tester_lock_synced_data(&tester);
    ASSERT_TRUE(tester.synced_data.finish_error_code != AWS_ERROR_SUCCESS);
    aws_s3_tester_unlock_synced_data(&tester);

    ASSERT_TRUE(meta_request_test_results.finished_response_status == 404);
    ASSERT_TRUE(meta_request_test_results.finished_error_code != AWS_ERROR_SUCCESS);

    ASSERT_TRUE(meta_request_test_results.error_response_headers != NULL);
    ASSERT_TRUE(meta_request_test_results.error_response_body.len > 0);

    aws_s3_meta_request_release(meta_request);
    meta_request = NULL;

    aws_s3_tester_wait_for_meta_request_shutdown(&tester);
    aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);

    aws_http_message_release(message);
    message = NULL;

    aws_string_destroy(host_name);
    host_name = NULL;

    aws_s3_client_release(client);
    client = NULL;

    aws_s3_tester_clean_up(&tester);

    return 0;
}

static void s_test_s3_existing_host_entry_address_resolved_callback(
    struct aws_host_resolver *resolver,
    const struct aws_string *host_name,
    int err_code,
    const struct aws_array_list *host_addresses,
    void *user_data) {
    (void)resolver;
    (void)host_name;
    (void)err_code;
    (void)host_addresses;

    struct aws_s3_tester *tester = user_data;
    AWS_ASSERT(tester);
    aws_s3_tester_notify_signal(tester);
}

AWS_TEST_CASE(test_s3_existing_host_entry, s_test_s3_existing_host_entry)
static int s_test_s3_existing_host_entry(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client_config client_config;
    AWS_ZERO_STRUCT(client_config);

    ASSERT_SUCCESS(aws_s3_tester_bind_client(
        &tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION | AWS_S3_TESTER_BIND_CLIENT_SIGNING));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    struct aws_string *host_name =
        aws_s3_tester_build_endpoint_string(allocator, &g_test_public_bucket_name, &g_test_s3_region);

    {
        struct aws_host_resolution_config host_resolver_config;
        AWS_ZERO_STRUCT(host_resolver_config);
        host_resolver_config.impl = aws_default_dns_resolve;
        host_resolver_config.max_ttl = 30;
        host_resolver_config.impl_data = NULL;

        ASSERT_SUCCESS(aws_host_resolver_resolve_host(
            client_config.client_bootstrap->host_resolver,
            host_name,
            s_test_s3_existing_host_entry_address_resolved_callback,
            &host_resolver_config,
            &tester));

        aws_s3_tester_wait_for_signal(&tester);
    }

    /* Put together a simple S3 Get Object request. */
    struct aws_http_message *message = aws_s3_test_get_object_request_new(
        allocator, aws_byte_cursor_from_string(host_name), g_s3_path_get_object_test_1MB);

    struct aws_s3_meta_request_options options;
    AWS_ZERO_STRUCT(options);
    options.type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT;
    options.message = message;

    /* Trigger accelerating of our Get Object request. */
    struct aws_s3_meta_request_test_results meta_request_test_results;
    AWS_ZERO_STRUCT(meta_request_test_results);

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request(
        &tester, client, &options, &meta_request_test_results, AWS_S3_TESTER_SEND_META_REQUEST_EXPECT_SUCCESS));
    ASSERT_SUCCESS(aws_s3_tester_validate_get_object_results(&meta_request_test_results, 0));

    aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);

    aws_http_message_release(message);
    aws_string_destroy(host_name);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(test_s3_bad_endpoint, s_test_s3_bad_endpoint)
static int s_test_s3_bad_endpoint(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client_config client_config;
    AWS_ZERO_STRUCT(client_config);

    ASSERT_SUCCESS(aws_s3_tester_bind_client(
        &tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION | AWS_S3_TESTER_BIND_CLIENT_SIGNING));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    struct aws_byte_cursor test_key = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("test_key");

    AWS_STATIC_STRING_FROM_LITERAL(invalid_host_name, "invalid_host_name");

    /* Construct a message that points to an invalid host name. Key can be anything. */
    struct aws_http_message *message =
        aws_s3_test_get_object_request_new(allocator, aws_byte_cursor_from_string(invalid_host_name), test_key);

    struct aws_s3_meta_request_options options;
    AWS_ZERO_STRUCT(options);
    options.type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT;
    options.message = message;

    struct aws_s3_meta_request_test_results meta_request_test_results;
    AWS_ZERO_STRUCT(meta_request_test_results);

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request(&tester, client, &options, &meta_request_test_results, 0));

    ASSERT_TRUE(meta_request_test_results.finished_error_code == AWS_ERROR_S3_NO_ENDPOINT_CONNECTIONS);

    aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);

    aws_http_message_release(message);
    aws_s3_client_release(client);
    client = NULL;

    aws_s3_tester_clean_up(&tester);

    return 0;
}

static int s_s3_test_headers_callback_raise_error(
    struct aws_s3_meta_request *meta_request,
    const struct aws_http_headers *headers,
    int response_status,
    void *user_data) {
    (void)meta_request;
    (void)headers;
    (void)response_status;
    (void)user_data;
    aws_raise_error(AWS_ERROR_UNKNOWN);
    return AWS_OP_ERR;
}

static int s_s3_test_body_callback_raise_error(
    struct aws_s3_meta_request *meta_request,
    const struct aws_byte_cursor *body,
    uint64_t range_start,
    void *user_data) {
    (void)meta_request;
    (void)body;
    (void)range_start;
    (void)user_data;
    aws_raise_error(AWS_ERROR_UNKNOWN);
    return AWS_OP_ERR;
}

AWS_TEST_CASE(test_s3_put_object_fail_headers_callback, s_test_s3_put_object_fail_headers_callback)
static int s_test_s3_put_object_fail_headers_callback(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_meta_request_test_results meta_request_test_results;
    AWS_ZERO_STRUCT(meta_request_test_results);

    struct aws_s3_tester_meta_request_options options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .headers_callback = s_s3_test_headers_callback_raise_error,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE,
        .put_options =
            {
                .ensure_multipart = true,
            },
    };

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(NULL, &options, &meta_request_test_results));
    ASSERT_TRUE(meta_request_test_results.finished_error_code == AWS_ERROR_UNKNOWN);

    aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);

    return 0;
}

AWS_TEST_CASE(test_s3_put_object_fail_body_callback, s_test_s3_put_object_fail_body_callback)
static int s_test_s3_put_object_fail_body_callback(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester_meta_request_options options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .body_callback = s_s3_test_body_callback_raise_error,

        /* Put object currently never invokes the body callback, which means it should not fail. */
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_SUCCESS,

        .put_options =
            {
                .ensure_multipart = true,
            },
    };

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(NULL, &options, NULL));

    return 0;
}

AWS_TEST_CASE(test_s3_get_object_fail_headers_callback, s_test_s3_get_object_fail_headers_callback)
static int s_test_s3_get_object_fail_headers_callback(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_meta_request_test_results meta_request_test_results;
    AWS_ZERO_STRUCT(meta_request_test_results);

    struct aws_s3_tester_meta_request_options options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .headers_callback = s_s3_test_headers_callback_raise_error,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE,
        .get_options =
            {
                .object_path = g_s3_path_get_object_test_1MB,
            },
    };

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(NULL, &options, &meta_request_test_results));
    ASSERT_TRUE(meta_request_test_results.finished_error_code == AWS_ERROR_UNKNOWN);

    aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);

    return 0;
}

AWS_TEST_CASE(test_s3_get_object_fail_body_callback, s_test_s3_get_object_fail_body_callback)
static int s_test_s3_get_object_fail_body_callback(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_meta_request_test_results meta_request_test_results;
    AWS_ZERO_STRUCT(meta_request_test_results);

    struct aws_s3_tester_meta_request_options options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .body_callback = s_s3_test_body_callback_raise_error,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE,
        .get_options =
            {
                .object_path = g_s3_path_get_object_test_1MB,
            },
    };

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(NULL, &options, &meta_request_test_results));
    ASSERT_TRUE(meta_request_test_results.finished_error_code == AWS_ERROR_UNKNOWN);

    aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);

    return 0;
}

AWS_TEST_CASE(test_s3_default_fail_headers_callback, s_test_s3_default_fail_headers_callback)
static int s_test_s3_default_fail_headers_callback(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_meta_request_test_results meta_request_test_results;
    AWS_ZERO_STRUCT(meta_request_test_results);

    struct aws_s3_tester_meta_request_options options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_DEFAULT,
        .headers_callback = s_s3_test_headers_callback_raise_error,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE,
        .default_type_options =
            {
                .mode = AWS_S3_TESTER_DEFAULT_TYPE_MODE_GET,
            },
        .get_options =
            {
                .object_path = g_s3_path_get_object_test_1MB,
            },
    };

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(NULL, &options, &meta_request_test_results));
    ASSERT_TRUE(meta_request_test_results.finished_error_code == AWS_ERROR_UNKNOWN);

    aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);

    return 0;
}

AWS_TEST_CASE(test_s3_default_fail_body_callback, s_test_s3_default_fail_body_callback)
static int s_test_s3_default_fail_body_callback(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_meta_request_test_results meta_request_test_results;
    AWS_ZERO_STRUCT(meta_request_test_results);

    struct aws_s3_tester_meta_request_options options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_DEFAULT,
        .body_callback = s_s3_test_body_callback_raise_error,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE,
        .default_type_options =
            {
                .mode = AWS_S3_TESTER_DEFAULT_TYPE_MODE_GET,
            },
        .get_options =
            {
                .object_path = g_s3_path_get_object_test_1MB,
            },
    };

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(NULL, &options, &meta_request_test_results));
    ASSERT_TRUE(meta_request_test_results.finished_error_code == AWS_ERROR_UNKNOWN);

    aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);

    return 0;
}

AWS_TEST_CASE(test_s3_put_fail_object_invalid_request, s_test_s3_put_fail_object_invalid_request)
static int s_test_s3_put_fail_object_invalid_request(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_meta_request_test_results meta_request_test_results;
    AWS_ZERO_STRUCT(meta_request_test_results);

    struct aws_s3_tester_meta_request_options options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE,
        .put_options =
            {
                .ensure_multipart = true,
                .invalid_request = true,
            },
    };

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(NULL, &options, &meta_request_test_results));
    ASSERT_UINT_EQUALS(meta_request_test_results.finished_error_code, AWS_ERROR_S3_INVALID_RESPONSE_STATUS);

    aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);

    return 0;
}

AWS_TEST_CASE(
    test_s3_put_single_part_fail_object_inputstream_fail_reading,
    s_test_s3_put_single_part_fail_object_inputstream_fail_reading)
static int s_test_s3_put_single_part_fail_object_inputstream_fail_reading(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_meta_request_test_results meta_request_test_results;
    AWS_ZERO_STRUCT(meta_request_test_results);

    struct aws_s3_tester_meta_request_options options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE,
        .put_options =
            {
                .ensure_multipart = true,
                .invalid_input_stream = true,
                .content_length = 10,
            },
    };

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(NULL, &options, &meta_request_test_results));

    ASSERT_TRUE(meta_request_test_results.finished_error_code != AWS_ERROR_SUCCESS);

    aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);

    return 0;
}

AWS_TEST_CASE(test_s3_put_fail_object_inputstream_fail_reading, s_test_s3_put_fail_object_inputstream_fail_reading)
static int s_test_s3_put_fail_object_inputstream_fail_reading(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_meta_request_test_results meta_request_test_results;
    AWS_ZERO_STRUCT(meta_request_test_results);

    struct aws_s3_tester_meta_request_options options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE,
        .put_options =
            {
                .ensure_multipart = true,
                .invalid_input_stream = true,
                .content_length = 10 * 1024 * 1024,
            },
    };

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(NULL, &options, &meta_request_test_results));

    ASSERT_UINT_EQUALS(meta_request_test_results.finished_error_code, AWS_IO_STREAM_READ_FAILED);

    aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);

    return 0;
}

AWS_TEST_CASE(test_s3_different_endpoints, s_test_s3_different_endpoints)
static int s_test_s3_different_endpoints(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client *client = NULL;
    struct aws_s3_tester_client_options client_options;

    AWS_ZERO_STRUCT(client_options);
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    {
        struct aws_s3_meta_request_test_results meta_request_test_results;
        AWS_ZERO_STRUCT(meta_request_test_results);

        struct aws_s3_tester_meta_request_options options = {
            .allocator = allocator,
            .client = client,
            .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
            .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_SUCCESS,
            .get_options =
                {
                    .object_path = g_s3_path_get_object_test_1MB,
                },
        };

        ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &options, &meta_request_test_results));
        aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);
    }

    {
        struct aws_s3_meta_request_test_results meta_request_test_results;
        AWS_ZERO_STRUCT(meta_request_test_results);

        struct aws_byte_cursor bucket_without_file = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("aws-crt-test-stuff");

        struct aws_s3_tester_meta_request_options options = {
            .allocator = allocator,
            .client = client,
            .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
            .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE,
            .bucket_name = &bucket_without_file,
            .get_options =
                {
                    .object_path = g_s3_path_get_object_test_1MB,
                },
        };

        ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &options, &meta_request_test_results));
        ASSERT_TRUE(meta_request_test_results.finished_error_code == AWS_ERROR_INVALID_ARGUMENT);

        aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);
    }

    aws_s3_client_release(client);

    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(test_s3_put_object_clamp_part_size, s_test_s3_put_object_clamp_part_size)
static int s_test_s3_put_object_clamp_part_size(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client_config client_config = {
        .part_size = 64 * 1024,
        .max_part_size = 64 * 1024,
    };

    ASSERT_TRUE(client_config.part_size < g_s3_min_upload_part_size);
    ASSERT_TRUE(client_config.max_part_size < g_s3_min_upload_part_size);

    ASSERT_SUCCESS(aws_s3_tester_bind_client(
        &tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION | AWS_S3_TESTER_BIND_CLIENT_SIGNING));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    ASSERT_TRUE(client != NULL);

    struct aws_s3_meta_request_test_results test_results;
    AWS_ZERO_STRUCT(test_results);

    /* Upload should now succeed even when specifying a smaller than allowed part size. */
    ASSERT_SUCCESS(aws_s3_tester_send_put_object_meta_request(
        &tester, client, 10, AWS_S3_TESTER_SEND_META_REQUEST_EXPECT_SUCCESS, &test_results));

    ASSERT_TRUE(test_results.part_size == g_s3_min_upload_part_size);

    aws_s3_meta_request_test_results_clean_up(&test_results);

    aws_s3_client_release(client);
    client = NULL;

    aws_s3_tester_clean_up(&tester);

    return 0;
}

struct replace_quote_entities_test_case {
    struct aws_string *test_string;
    struct aws_byte_cursor expected_result;
};

AWS_TEST_CASE(test_s3_replace_quote_entities, s_test_s3_replace_quote_entities)
static int s_test_s3_replace_quote_entities(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct replace_quote_entities_test_case test_cases[] = {
        {
            .test_string = aws_string_new_from_c_str(allocator, "&quot;testtest"),
            .expected_result = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("\"testtest"),
        },
        {
            .test_string = aws_string_new_from_c_str(allocator, "testtest&quot;"),
            .expected_result = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("testtest\""),
        },
        {
            .test_string = aws_string_new_from_c_str(allocator, "&quot;&quot;"),
            .expected_result = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("\"\""),
        },
        {
            .test_string = aws_string_new_from_c_str(allocator, "testtest"),
            .expected_result = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("testtest"),
        },
        {
            .test_string = aws_string_new_from_c_str(allocator, ""),
            .expected_result = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL(""),
        },
    };

    for (size_t i = 0; i < (sizeof(test_cases) / sizeof(struct replace_quote_entities_test_case)); ++i) {
        struct replace_quote_entities_test_case *test_case = &test_cases[i];

        struct aws_byte_buf result_byte_buf;
        AWS_ZERO_STRUCT(result_byte_buf);

        replace_quote_entities(allocator, test_case->test_string, &result_byte_buf);

        struct aws_byte_cursor result_byte_cursor = aws_byte_cursor_from_buf(&result_byte_buf);

        ASSERT_TRUE(aws_byte_cursor_eq(&test_case->expected_result, &result_byte_cursor));

        aws_byte_buf_clean_up(&result_byte_buf);
        aws_string_destroy(test_case->test_string);
        test_case->test_string = NULL;
    }

    aws_s3_tester_clean_up(&tester);

    return 0;
}

static int s_get_expected_user_agent(struct aws_allocator *allocator, struct aws_byte_buf *dest) {
    AWS_ASSERT(allocator);
    AWS_ASSERT(dest);

    const struct aws_byte_cursor forward_slash = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("/");

    ASSERT_SUCCESS(aws_byte_buf_init(dest, allocator, 32));
    ASSERT_SUCCESS(aws_byte_buf_append_dynamic(dest, &g_user_agent_header_product_name));
    ASSERT_SUCCESS(aws_byte_buf_append_dynamic(dest, &forward_slash));
    ASSERT_SUCCESS(aws_byte_buf_append_dynamic(dest, &g_s3_client_version));

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(test_add_user_agent_header, s_test_add_user_agent_header)
static int s_test_add_user_agent_header(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    const struct aws_byte_cursor forward_slash = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("/");
    const struct aws_byte_cursor single_space = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL(" ");

    struct aws_byte_buf expected_user_agent_value_buf;
    s_get_expected_user_agent(allocator, &expected_user_agent_value_buf);

    struct aws_byte_cursor expected_user_agent_value = aws_byte_cursor_from_buf(&expected_user_agent_value_buf);

    {
        struct aws_byte_cursor user_agent_value;
        AWS_ZERO_STRUCT(user_agent_value);

        struct aws_http_message *message = aws_http_message_new_request(allocator);

        aws_s3_add_user_agent_header(allocator, message);

        struct aws_http_headers *headers = aws_http_message_get_headers(message);

        ASSERT_TRUE(headers != NULL);
        ASSERT_SUCCESS(aws_http_headers_get(headers, g_user_agent_header_name, &user_agent_value));
        ASSERT_TRUE(aws_byte_cursor_eq(&user_agent_value, &expected_user_agent_value));

        aws_http_message_release(message);
    }

    {
        const struct aws_byte_cursor dummy_agent_header_value =
            AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("dummy_user_agent_product/dummy_user_agent_value");

        struct aws_byte_buf total_expected_user_agent_value_buf;
        aws_byte_buf_init(&total_expected_user_agent_value_buf, allocator, 64);
        aws_byte_buf_append_dynamic(&total_expected_user_agent_value_buf, &dummy_agent_header_value);
        aws_byte_buf_append_dynamic(&total_expected_user_agent_value_buf, &single_space);

        aws_byte_buf_append_dynamic(&total_expected_user_agent_value_buf, &g_user_agent_header_product_name);
        aws_byte_buf_append_dynamic(&total_expected_user_agent_value_buf, &forward_slash);
        aws_byte_buf_append_dynamic(&total_expected_user_agent_value_buf, &g_s3_client_version);

        struct aws_byte_cursor total_expected_user_agent_value =
            aws_byte_cursor_from_buf(&total_expected_user_agent_value_buf);

        struct aws_http_message *message = aws_http_message_new_request(allocator);
        struct aws_http_headers *headers = aws_http_message_get_headers(message);
        ASSERT_TRUE(headers != NULL);

        ASSERT_SUCCESS(aws_http_headers_add(headers, g_user_agent_header_name, dummy_agent_header_value));

        aws_s3_add_user_agent_header(allocator, message);

        {
            struct aws_byte_cursor user_agent_value;
            AWS_ZERO_STRUCT(user_agent_value);
            ASSERT_SUCCESS(aws_http_headers_get(headers, g_user_agent_header_name, &user_agent_value));
            ASSERT_TRUE(aws_byte_cursor_eq(&user_agent_value, &total_expected_user_agent_value));
        }

        aws_byte_buf_clean_up(&total_expected_user_agent_value_buf);
        aws_http_message_release(message);
    }

    aws_byte_buf_clean_up(&expected_user_agent_value_buf);
    aws_s3_tester_clean_up(&tester);

    return 0;
}

static int s_s3_test_user_agent_meta_request_prepare_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request) {

    AWS_ASSERT(meta_request != NULL);

    struct aws_s3_meta_request_test_results *results = meta_request->user_data;
    AWS_ASSERT(results != NULL);

    struct aws_s3_tester *tester = results->tester;
    AWS_ASSERT(tester != NULL);

    struct aws_s3_meta_request_vtable *original_meta_request_vtable =
        aws_s3_tester_get_meta_request_vtable_patch(tester, 0)->original_vtable;

    if (original_meta_request_vtable->prepare_request(meta_request, request)) {
        return AWS_OP_ERR;
    }

    struct aws_byte_buf expected_user_agent_value_buf;
    s_get_expected_user_agent(meta_request->allocator, &expected_user_agent_value_buf);

    const struct aws_byte_cursor null_terminator = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("\0");
    ASSERT_SUCCESS(aws_byte_buf_append_dynamic(&expected_user_agent_value_buf, &null_terminator));

    struct aws_byte_cursor expected_user_agent_value = aws_byte_cursor_from_buf(&expected_user_agent_value_buf);

    struct aws_http_message *message = request->send_data.message;
    struct aws_http_headers *headers = aws_http_message_get_headers(message);

    struct aws_byte_cursor user_agent_value;
    AWS_ZERO_STRUCT(user_agent_value);

    ASSERT_SUCCESS(aws_http_headers_get(headers, g_user_agent_header_name, &user_agent_value));

    const char *find_result = strstr((const char *)user_agent_value.ptr, (const char *)expected_user_agent_value.ptr);

    ASSERT_TRUE(find_result != NULL);
    ASSERT_TRUE(find_result < (const char *)(user_agent_value.ptr + user_agent_value.len));

    aws_byte_buf_clean_up(&expected_user_agent_value_buf);
    return AWS_OP_SUCCESS;
}

static struct aws_s3_meta_request *s_s3_meta_request_factory_override_prepare_request(
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
    patched_meta_request_vtable->prepare_request = s_s3_test_user_agent_meta_request_prepare_request;

    return meta_request;
}

int s_s3_test_sending_user_agent_create_client(struct aws_s3_tester *tester, struct aws_s3_client **client) {
    AWS_ASSERT(tester);

    struct aws_s3_tester_client_options client_options;
    AWS_ZERO_STRUCT(client_options);

    ASSERT_SUCCESS(aws_s3_tester_client_new(tester, &client_options, client));

    struct aws_s3_client_vtable *patched_client_vtable = aws_s3_tester_patch_client_vtable(tester, *client, NULL);
    patched_client_vtable->meta_request_factory = s_s3_meta_request_factory_override_prepare_request;

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(test_s3_auto_ranged_get_sending_user_agent, s_test_s3_auto_ranged_get_sending_user_agent)
static int s_test_s3_auto_ranged_get_sending_user_agent(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(s_s3_test_sending_user_agent_create_client(&tester, &client));

    {
        struct aws_s3_tester_meta_request_options options = {
            .allocator = allocator,
            .client = client,
            .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
            .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_SUCCESS,
            .get_options =
                {
                    .object_path = g_s3_path_get_object_test_1MB,
                },
        };

        ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &options, NULL));
    }

    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(test_s3_auto_ranged_put_sending_user_agent, s_test_s3_auto_ranged_put_sending_user_agent)
static int s_test_s3_auto_ranged_put_sending_user_agent(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(s_s3_test_sending_user_agent_create_client(&tester, &client));

    {
        struct aws_s3_tester_meta_request_options options = {
            .allocator = allocator,
            .client = client,
            .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
            .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_SUCCESS,
            .put_options =
                {
                    .ensure_multipart = true,
                },
        };

        ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &options, NULL));
    }

    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(test_s3_default_sending_meta_request, s_test_s3_default_sending_meta_request)
static int s_test_s3_default_sending_meta_request(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(s_s3_test_sending_user_agent_create_client(&tester, &client));

    {
        struct aws_s3_tester_meta_request_options options = {
            .allocator = allocator,
            .client = client,
            .meta_request_type = AWS_S3_META_REQUEST_TYPE_DEFAULT,
            .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_SUCCESS,
            .default_type_options =
                {
                    .mode = AWS_S3_TESTER_DEFAULT_TYPE_MODE_GET,
                },
            .get_options =
                {
                    .object_path = g_s3_path_get_object_test_1MB,
                },
        };

        ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &options, NULL));
    }

    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return 0;
}
