/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_checksums.h"
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
#include <aws/http/status_code.h>
#include <aws/io/channel_bootstrap.h>
#include <aws/io/event_loop.h>
#include <aws/io/host_resolver.h>
#include <aws/io/stream.h>
#include <aws/io/tls_channel_handler.h>
#include <aws/io/uri.h>
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

AWS_TEST_CASE(test_s3_client_byo_crypto_no_options, s_test_s3_client_byo_crypto_no_options)
static int s_test_s3_client_byo_crypto_no_options(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client_config client_config = {
        .tls_mode = AWS_MR_TLS_ENABLED,
    };

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    ASSERT_TRUE(aws_last_error() == AWS_ERROR_INVALID_ARGUMENT);
    ASSERT_TRUE(client == NULL);

    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(test_s3_client_byo_crypto_with_options, s_test_s3_client_byo_crypto_with_options)
static int s_test_s3_client_byo_crypto_with_options(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_tls_connection_options tls_conn_options;
    AWS_ZERO_STRUCT(tls_conn_options);

    struct aws_s3_client_config client_config;
    AWS_ZERO_STRUCT(client_config);
    ASSERT_SUCCESS(aws_s3_tester_bind_client(&tester, &client_config, 0));

    client_config.tls_mode = AWS_MR_TLS_ENABLED;
    client_config.tls_connection_options = &tls_conn_options;

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    ASSERT_TRUE(client != NULL);

    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return 0;
}

size_t s_test_max_active_connections_host_count = 0;

size_t s_test_get_max_active_connections_host_address_count(
    struct aws_host_resolver *host_resolver,
    const struct aws_string *host_name,
    uint32_t flags) {
    (void)host_resolver;
    (void)host_name;
    (void)flags;

    return s_test_max_active_connections_host_count;
}

AWS_TEST_CASE(test_s3_client_get_max_active_connections, s_test_s3_client_get_max_active_connections)
static int s_test_s3_client_get_max_active_connections(struct aws_allocator *allocator, void *ctx) {
    (void)allocator;
    (void)ctx;

    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_client_bootstrap mock_client_bootstrap;
    AWS_ZERO_STRUCT(mock_client_bootstrap);

    struct aws_s3_client *mock_client = aws_s3_tester_mock_client_new(&tester);
    *((uint32_t *)&mock_client->max_active_connections_override) = 0;
    *((uint32_t *)&mock_client->ideal_vip_count) = 10;
    mock_client->client_bootstrap = &mock_client_bootstrap;
    mock_client->vtable->get_host_address_count = s_test_get_max_active_connections_host_address_count;

    struct aws_s3_meta_request *mock_meta_requests[AWS_S3_META_REQUEST_TYPE_MAX];

    for (size_t i = 0; i < AWS_S3_META_REQUEST_TYPE_MAX; ++i) {
        /* Verify that g_max_num_connections_per_vip and g_num_conns_per_vip_meta_request_look_up are set up
         * correctly.*/
        ASSERT_TRUE(g_max_num_connections_per_vip >= g_num_conns_per_vip_meta_request_look_up[i]);

        /* Setup test data. */
        mock_meta_requests[i] = aws_s3_tester_mock_meta_request_new(&tester);
        mock_meta_requests[i]->type = i;
        mock_meta_requests[i]->endpoint = aws_s3_tester_mock_endpoint_new(&tester);
    }

    /* With host count at 0, we should allow for one VIP worth of max-active-connections. */
    {
        s_test_max_active_connections_host_count = 0;

        ASSERT_TRUE(
            aws_s3_client_get_max_active_connections(mock_client, NULL) ==
            mock_client->ideal_vip_count * g_max_num_connections_per_vip);

        for (size_t i = 0; i < AWS_S3_META_REQUEST_TYPE_MAX; ++i) {
            ASSERT_TRUE(
                aws_s3_client_get_max_active_connections(mock_client, mock_meta_requests[i]) ==
                g_num_conns_per_vip_meta_request_look_up[i]);
        }
    }

    s_test_max_active_connections_host_count = 2;

    /* Behavior should not be affected by max_active_connections_override since it is 0, and should just be in relation
     * to ideal-vip-count and host-count. */
    {
        ASSERT_TRUE(
            aws_s3_client_get_max_active_connections(mock_client, NULL) ==
            mock_client->ideal_vip_count * g_max_num_connections_per_vip);

        for (size_t i = 0; i < AWS_S3_META_REQUEST_TYPE_MAX; ++i) {
            ASSERT_TRUE(
                aws_s3_client_get_max_active_connections(mock_client, mock_meta_requests[i]) ==
                s_test_max_active_connections_host_count * g_num_conns_per_vip_meta_request_look_up[i]);
        }
    }

    /* Max active connections override should now cap the calculated amount of active connections. */
    {
        *((uint32_t *)&mock_client->max_active_connections_override) = 3;

        ASSERT_TRUE(
            mock_client->max_active_connections_override <
            mock_client->ideal_vip_count * g_max_num_connections_per_vip);

        ASSERT_TRUE(
            aws_s3_client_get_max_active_connections(mock_client, NULL) ==
            mock_client->max_active_connections_override);

        for (size_t i = 0; i < AWS_S3_META_REQUEST_TYPE_MAX; ++i) {
            ASSERT_TRUE(
                mock_client->max_active_connections_override <
                s_test_max_active_connections_host_count * g_num_conns_per_vip_meta_request_look_up[i]);

            ASSERT_TRUE(
                aws_s3_client_get_max_active_connections(mock_client, mock_meta_requests[i]) ==
                mock_client->max_active_connections_override);
        }
    }

    /* Max active connections override should be ignored since the calculated amount of max connections is less. */
    {
        *((uint32_t *)&mock_client->max_active_connections_override) = 100000;

        ASSERT_TRUE(
            mock_client->max_active_connections_override >
            mock_client->ideal_vip_count * g_max_num_connections_per_vip);

        ASSERT_TRUE(
            aws_s3_client_get_max_active_connections(mock_client, NULL) ==
            mock_client->ideal_vip_count * g_max_num_connections_per_vip);

        for (size_t i = 0; i < AWS_S3_META_REQUEST_TYPE_MAX; ++i) {
            ASSERT_TRUE(
                mock_client->max_active_connections_override >
                s_test_max_active_connections_host_count * g_num_conns_per_vip_meta_request_look_up[i]);

            ASSERT_TRUE(
                aws_s3_client_get_max_active_connections(mock_client, mock_meta_requests[i]) ==
                s_test_max_active_connections_host_count * g_num_conns_per_vip_meta_request_look_up[i]);
        }
    }

    for (size_t i = 0; i < AWS_S3_META_REQUEST_TYPE_MAX; ++i) {
        aws_s3_meta_request_release(mock_meta_requests[i]);
        mock_meta_requests[i] = NULL;
    }

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
    struct aws_allocator *allocator;
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

    const uint64_t total_object_size = (uint64_t)part_range1_end * request_response_body_size;

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

            aws_s3_get_part_range(
                0ULL,
                total_object_size - 1,
                (uint64_t)request_response_body_size,
                part_number,
                &request->part_range_start,
                &request->part_range_end);

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

            aws_s3_get_part_range(
                0ULL,
                total_object_size - 1,
                (uint64_t)request_response_body_size,
                part_number,
                &request->part_range_start,
                &request->part_range_end);

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

        aws_s3_get_part_range(
            0ULL,
            total_object_size - 1,
            (uint64_t)request_response_body_size,
            part_range1_start,
            &request->part_range_start,
            &request->part_range_end);

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

static size_t s_test_s3_update_meta_request_trigger_prepare_host_address_count = 0;

static size_t s_test_s3_update_meta_request_trigger_prepare_get_host_address_count(
    struct aws_host_resolver *host_resolver,
    const struct aws_string *host_name,
    uint32_t flags) {
    (void)host_resolver;
    (void)host_name;
    (void)flags;
    return s_test_s3_update_meta_request_trigger_prepare_host_address_count;
}

static int s_validate_prepared_requests(
    struct aws_s3_client *client,
    size_t expected_num_being_prepared,
    struct aws_s3_meta_request *meta_request_with_work,
    struct aws_s3_meta_request *meta_request_without_work) {

    ASSERT_TRUE(client->threaded_data.request_queue_size == 0);
    ASSERT_TRUE(aws_linked_list_empty(&client->threaded_data.request_queue));
    ASSERT_TRUE(client->threaded_data.num_requests_being_prepared == expected_num_being_prepared);
    ASSERT_TRUE(aws_atomic_load_int(&client->stats.num_requests_in_flight) == expected_num_being_prepared);

    uint32_t num_meta_requests_in_list = 0;
    bool meta_request_with_work_found = false;

    for (struct aws_linked_list_node *node = aws_linked_list_begin(&client->threaded_data.meta_requests);
         node != aws_linked_list_end(&client->threaded_data.meta_requests);
         node = aws_linked_list_next(node)) {

        struct aws_s3_meta_request *meta_request =
            AWS_CONTAINER_OF(node, struct aws_s3_meta_request, client_process_work_threaded_data);

        if (meta_request == meta_request_with_work) {
            meta_request_with_work_found = true;
        }

        ASSERT_TRUE(meta_request != meta_request_without_work);

        ++num_meta_requests_in_list;
    }

    ASSERT_TRUE(meta_request_with_work_found);
    ASSERT_TRUE(num_meta_requests_in_list == 1);

    return AWS_OP_SUCCESS;
}

/* Test that the client will prepare requests correctly. */
AWS_TEST_CASE(test_s3_update_meta_requests_trigger_prepare, s_test_s3_update_meta_requests_trigger_prepare)
static int s_test_s3_update_meta_requests_trigger_prepare(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    aws_s3_tester_init(allocator, &tester);

    struct aws_client_bootstrap mock_bootstrap;
    AWS_ZERO_STRUCT(mock_bootstrap);

    const uint32_t ideal_vip_count = 10;

    struct aws_s3_client *mock_client = aws_s3_tester_mock_client_new(&tester);
    mock_client->client_bootstrap = &mock_bootstrap;
    mock_client->vtable->get_host_address_count = s_test_s3_update_meta_request_trigger_prepare_get_host_address_count;
    *((uint32_t *)&mock_client->ideal_vip_count) = ideal_vip_count;
    aws_linked_list_init(&mock_client->threaded_data.request_queue);
    aws_linked_list_init(&mock_client->threaded_data.meta_requests);

    struct aws_s3_meta_request *mock_meta_request_without_work = aws_s3_tester_mock_meta_request_new(&tester);
    mock_meta_request_without_work->endpoint = aws_s3_tester_mock_endpoint_new(&tester);

    struct test_work_meta_request_update_user_data mock_meta_request_without_work_data = {
        .has_work_remaining = false,
    };

    mock_meta_request_without_work->user_data = &mock_meta_request_without_work_data;

    struct aws_s3_meta_request_vtable *meta_request_without_work_vtable =
        aws_s3_tester_patch_meta_request_vtable(&tester, mock_meta_request_without_work, NULL);
    meta_request_without_work_vtable->update = s_s3_test_work_meta_request_update;
    meta_request_without_work_vtable->schedule_prepare_request = s_s3_test_work_meta_request_schedule_prepare_request;

    /* Intentionally push this meta request first to test that it's properly removed from the list. */
    aws_linked_list_push_back(
        &mock_client->threaded_data.meta_requests,
        &mock_meta_request_without_work->client_process_work_threaded_data.node);

    aws_s3_meta_request_acquire(mock_meta_request_without_work);

    struct aws_s3_meta_request *mock_meta_request_with_work = aws_s3_tester_mock_meta_request_new(&tester);
    struct test_work_meta_request_update_user_data mock_meta_request_with_work_data = {
        .has_work_remaining = true,
    };

    mock_meta_request_with_work->endpoint = aws_s3_tester_mock_endpoint_new(&tester);
    mock_meta_request_with_work->user_data = &mock_meta_request_with_work_data;

    struct aws_s3_meta_request_vtable *mock_meta_request_with_work_vtable =
        aws_s3_tester_patch_meta_request_vtable(&tester, mock_meta_request_with_work, NULL);
    mock_meta_request_with_work_vtable->update = s_s3_test_work_meta_request_update;
    mock_meta_request_with_work_vtable->schedule_prepare_request = s_s3_test_work_meta_request_schedule_prepare_request;

    aws_linked_list_push_back(
        &mock_client->threaded_data.meta_requests,
        &mock_meta_request_with_work->client_process_work_threaded_data.node);
    aws_s3_meta_request_acquire(mock_meta_request_with_work);

    /* With no known addresses, the amount of requests that can be prepared should only be enough for one VIP. */
    {
        s_test_s3_update_meta_request_trigger_prepare_host_address_count = 0;
        aws_s3_client_update_meta_requests_threaded(mock_client);

        ASSERT_SUCCESS(s_validate_prepared_requests(
            mock_client, g_max_num_connections_per_vip, mock_meta_request_with_work, mock_meta_request_without_work));
    }

    /* When the number of known addresses is greater than or equal to the ideal vip count, the max number of requests
     * should be reached. */
    {
        const uint32_t max_requests_prepare = aws_s3_client_get_max_requests_prepare(mock_client);

        s_test_s3_update_meta_request_trigger_prepare_host_address_count = (size_t)(ideal_vip_count);
        aws_s3_client_update_meta_requests_threaded(mock_client);

        ASSERT_SUCCESS(s_validate_prepared_requests(
            mock_client, max_requests_prepare, mock_meta_request_with_work, mock_meta_request_without_work));

        s_test_s3_update_meta_request_trigger_prepare_host_address_count = (size_t)(ideal_vip_count + 1);
        aws_s3_client_update_meta_requests_threaded(mock_client);

        ASSERT_SUCCESS(s_validate_prepared_requests(
            mock_client, max_requests_prepare, mock_meta_request_with_work, mock_meta_request_without_work));
    }

    while (!aws_linked_list_empty(&mock_client->threaded_data.meta_requests)) {
        struct aws_linked_list_node *meta_request_node =
            aws_linked_list_pop_front(&mock_client->threaded_data.meta_requests);

        struct aws_s3_meta_request *meta_request =
            AWS_CONTAINER_OF(meta_request_node, struct aws_s3_meta_request, client_process_work_threaded_data);

        aws_s3_meta_request_release(meta_request);
    }

    aws_s3_meta_request_release(mock_meta_request_without_work);
    aws_s3_meta_request_release(mock_meta_request_with_work);
    aws_s3_client_release(mock_client);
    aws_s3_tester_clean_up(&tester);
    return 0;
}

struct s3_test_update_connections_finish_result_user_data {
    struct aws_s3_request *finished_request;
    struct aws_s3_request *create_connection_request;

    uint32_t finished_request_call_counter;
    uint32_t create_connection_request_call_counter;
};

static void s_s3_test_meta_request_has_finish_result_finished_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    int error_code) {
    AWS_ASSERT(meta_request);
    AWS_ASSERT(request);
    (void)error_code;

    struct s3_test_update_connections_finish_result_user_data *user_data = meta_request->user_data;
    user_data->finished_request = request;
    ++user_data->finished_request_call_counter;
}

static void s_s3_test_meta_request_has_finish_result_client_create_connection_for_request(
    struct aws_s3_client *client,
    struct aws_s3_request *request) {
    (void)client;
    (void)request;
    AWS_ASSERT(client);
    AWS_ASSERT(request);

    struct aws_s3_meta_request *meta_request = request->meta_request;

    struct s3_test_update_connections_finish_result_user_data *user_data = meta_request->user_data;
    user_data->create_connection_request = request;
    ++user_data->create_connection_request_call_counter;
}

size_t s_test_update_conns_finish_result_host_address_count(
    struct aws_host_resolver *host_resolver,
    const struct aws_string *host_name,
    uint32_t flags) {
    (void)host_resolver;
    (void)host_name;
    (void)flags;

    return 1;
}

/* Test that the client will correctly discard requests for meta requests that are trying to finish. */
AWS_TEST_CASE(test_s3_client_update_connections_finish_result, s_test_s3_client_update_connections_finish_result)
static int s_test_s3_client_update_connections_finish_result(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    aws_s3_tester_init(allocator, &tester);

    struct s3_test_update_connections_finish_result_user_data test_update_connections_finish_result_user_data;
    AWS_ZERO_STRUCT(test_update_connections_finish_result_user_data);

    /* Put together a mock meta request that is finished. */
    struct aws_s3_meta_request *mock_meta_request = aws_s3_tester_mock_meta_request_new(&tester);
    mock_meta_request->synced_data.finish_result_set = true;
    mock_meta_request->user_data = &test_update_connections_finish_result_user_data;
    mock_meta_request->endpoint = aws_s3_tester_mock_endpoint_new(&tester);

    struct aws_s3_meta_request_vtable *mock_meta_request_vtable =
        aws_s3_tester_patch_meta_request_vtable(&tester, mock_meta_request, NULL);
    mock_meta_request_vtable->finished_request = s_s3_test_meta_request_has_finish_result_finished_request;

    struct aws_client_bootstrap mock_client_bootstrap;
    AWS_ZERO_STRUCT(mock_client_bootstrap);

    struct aws_s3_client *mock_client = aws_s3_tester_mock_client_new(&tester);
    mock_client->client_bootstrap = &mock_client_bootstrap;
    mock_client->vtable->get_host_address_count = s_test_update_conns_finish_result_host_address_count;
    mock_client->vtable->create_connection_for_request =
        s_s3_test_meta_request_has_finish_result_client_create_connection_for_request;

    *((uint32_t *)&mock_client->ideal_vip_count) = 1;

    aws_linked_list_init(&mock_client->threaded_data.request_queue);

    /* Verify that the request does not get sent because the meta request has finish-result. */
    {
        struct aws_s3_request *request = aws_s3_request_new(mock_meta_request, 0, 0, 0);
        aws_linked_list_push_back(&mock_client->threaded_data.request_queue, &request->node);
        ++mock_client->threaded_data.request_queue_size;

        aws_s3_client_update_connections_threaded(mock_client);

        /* Request should still have been dequeued, but immediately passed to the meta request finish function. */
        ASSERT_TRUE(mock_client->threaded_data.request_queue_size == 0);
        ASSERT_TRUE(aws_linked_list_empty(&mock_client->threaded_data.request_queue));

        ASSERT_TRUE(test_update_connections_finish_result_user_data.finished_request == request);
        ASSERT_TRUE(test_update_connections_finish_result_user_data.finished_request_call_counter == 1);

        ASSERT_TRUE(test_update_connections_finish_result_user_data.create_connection_request == NULL);
        ASSERT_TRUE(test_update_connections_finish_result_user_data.create_connection_request_call_counter == 0);
    }

    AWS_ZERO_STRUCT(test_update_connections_finish_result_user_data);

    /* Verify that a request with the 'always send' flag still gets sent when the meta request has a finish-result. */
    {
        struct aws_s3_request *request = aws_s3_request_new(mock_meta_request, 0, 0, AWS_S3_REQUEST_FLAG_ALWAYS_SEND);
        aws_linked_list_push_back(&mock_client->threaded_data.request_queue, &request->node);
        ++mock_client->threaded_data.request_queue_size;

        aws_s3_client_update_connections_threaded(mock_client);

        /* Request should have been dequeued, and then sent on a connection. */
        ASSERT_TRUE(mock_client->threaded_data.request_queue_size == 0);
        ASSERT_TRUE(aws_linked_list_empty(&mock_client->threaded_data.request_queue));

        ASSERT_TRUE(test_update_connections_finish_result_user_data.finished_request == NULL);
        ASSERT_TRUE(test_update_connections_finish_result_user_data.finished_request_call_counter == 0);

        ASSERT_TRUE(test_update_connections_finish_result_user_data.create_connection_request == request);
        ASSERT_TRUE(test_update_connections_finish_result_user_data.create_connection_request_call_counter == 1);

        aws_s3_request_release(request);
    }

    aws_s3_meta_request_release(mock_meta_request);

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

    struct aws_tls_connection_options tls_connection_options;
    AWS_ZERO_STRUCT(tls_connection_options);

#ifndef BYO_CRYPTO
    struct aws_tls_ctx_options tls_context_options;
    aws_tls_ctx_options_init_default_client(&tls_context_options, allocator);

    struct aws_tls_ctx *context = aws_tls_client_ctx_new(allocator, &tls_context_options);
    aws_tls_connection_options_init_from_ctx(&tls_connection_options, context);
#endif

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

#ifndef BYO_CRYPTO
    aws_tls_ctx_release(context);
    aws_tls_connection_options_clean_up(&tls_connection_options);
    aws_tls_ctx_options_clean_up(&tls_context_options);
#endif

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

    ASSERT_SUCCESS(s_test_s3_get_object_helper(allocator, AWS_S3_TLS_DEFAULT, 0, g_s3_path_get_object_test_1MB));

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

    for (size_t i = 0; i < num_meta_requests; ++i) {
        /* Put together a simple S3 Get Object request. */
        struct aws_http_message *message =
            aws_s3_test_get_object_request_new(allocator, aws_byte_cursor_from_string(host_name), test_object_path);

        AWS_ZERO_STRUCT(meta_request_test_results[i]);

        struct aws_s3_meta_request_options options;
        AWS_ZERO_STRUCT(options);
        options.type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT;
        options.message = message;

        ASSERT_SUCCESS(aws_s3_tester_bind_meta_request(&tester, &options, &meta_request_test_results[i]));

        /* Trigger accelerating of our Get Object request. */
        meta_requests[i] = aws_s3_client_make_meta_request(client, &options);

        ASSERT_TRUE(meta_requests[i] != NULL);

        aws_http_message_release(message);
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
        allocator, AWS_S3_TLS_ENABLED, AWS_S3_TESTER_SEND_META_REQUEST_SSE_AES256, g_pre_existing_object_aes256_10MB);
}

static int s_test_s3_put_object_helper(
    struct aws_allocator *allocator,
    enum aws_s3_client_tls_usage tls_usage,
    uint32_t extra_meta_request_flag,
    const enum aws_s3_checksum_algorithm checksum_algorithm) {
    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    if (checksum_algorithm) {
        ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    } else {
        ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    }

    struct aws_tls_connection_options tls_connection_options;
    AWS_ZERO_STRUCT(tls_connection_options);

#ifndef BYO_CRYPTO
    struct aws_tls_ctx_options tls_context_options;
    aws_tls_ctx_options_init_default_client(&tls_context_options, allocator);

    struct aws_tls_ctx *context = aws_tls_client_ctx_new(allocator, &tls_context_options);
    aws_tls_connection_options_init_from_ctx(&tls_connection_options, context);
#endif

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

#ifndef BYO_CRYPTO
    aws_tls_ctx_release(context);
    aws_tls_connection_options_clean_up(&tls_connection_options);
    aws_tls_ctx_options_clean_up(&tls_context_options);
#endif

    aws_s3_client_release(client);
    client = NULL;

    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(test_s3_put_object_tls_disabled, s_test_s3_put_object_tls_disabled)
static int s_test_s3_put_object_tls_disabled(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(s_test_s3_put_object_helper(allocator, AWS_S3_TLS_DISABLED, 0, AWS_SCA_NONE));

    return 0;
}

AWS_TEST_CASE(test_s3_put_object_tls_enabled, s_test_s3_put_object_tls_enabled)
static int s_test_s3_put_object_tls_enabled(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(s_test_s3_put_object_helper(allocator, AWS_S3_TLS_ENABLED, 0, AWS_SCA_NONE));

    return 0;
}

AWS_TEST_CASE(test_s3_put_object_tls_default, s_test_s3_put_object_tls_default)
static int s_test_s3_put_object_tls_default(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(s_test_s3_put_object_helper(allocator, AWS_S3_TLS_DEFAULT, 0, AWS_SCA_NONE));

    return 0;
}

AWS_TEST_CASE(test_s3_multipart_put_object_with_acl, s_test_s3_multipart_put_object_with_acl)
static int s_test_s3_multipart_put_object_with_acl(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(s_test_s3_put_object_helper(
        allocator, AWS_S3_TLS_DEFAULT, AWS_S3_TESTER_SEND_META_REQUEST_PUT_ACL, AWS_SCA_NONE));

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
        &tester, client, 1, AWS_S3_TESTER_SEND_META_REQUEST_EXPECT_SUCCESS, NULL));

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

static int s_test_s3_put_object_content_md5_helper(
    struct aws_allocator *allocator,
    bool multipart_upload,
    uint32_t flags,
    enum aws_s3_meta_request_compute_content_md5 compute_content_md5) {
    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    size_t part_size = 5 * 1024 * 1024;
    if (!multipart_upload) {
        /* content_length < part_size */
        part_size = 15 * 1024 * 1024;
    }

    struct aws_s3_client_config client_config = {
        .part_size = part_size,
    };

    client_config.compute_content_md5 = compute_content_md5;

    ASSERT_SUCCESS(aws_s3_tester_bind_client(
        &tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION | AWS_S3_TESTER_BIND_CLIENT_SIGNING));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    ASSERT_TRUE(client != NULL);

    ASSERT_SUCCESS(aws_s3_tester_send_put_object_meta_request(&tester, client, 10, flags, NULL));

    aws_s3_client_release(client);
    client = NULL;

    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(
    test_s3_put_object_singlepart_no_content_md5_enabled,
    s_test_s3_put_object_singlepart_no_content_md5_enabled)
static int s_test_s3_put_object_singlepart_no_content_md5_enabled(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    uint32_t flags = AWS_S3_TESTER_SEND_META_REQUEST_EXPECT_SUCCESS;
    ASSERT_SUCCESS(s_test_s3_put_object_content_md5_helper(allocator, false, flags, AWS_MR_CONTENT_MD5_ENABLED));

    return 0;
}

AWS_TEST_CASE(
    test_s3_put_object_singlepart_no_content_md5_disabled,
    s_test_s3_put_object_singlepart_no_content_md5_disabled)
static int s_test_s3_put_object_singlepart_no_content_md5_disabled(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    uint32_t flags = AWS_S3_TESTER_SEND_META_REQUEST_EXPECT_SUCCESS;
    ASSERT_SUCCESS(s_test_s3_put_object_content_md5_helper(allocator, false, flags, AWS_MR_CONTENT_MD5_DISABLED));

    return 0;
}

AWS_TEST_CASE(
    test_s3_put_object_singlepart_correct_content_md5_enabled,
    s_test_s3_put_object_singlepart_correct_content_md5_enabled)
static int s_test_s3_put_object_singlepart_correct_content_md5_enabled(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    uint32_t flags =
        AWS_S3_TESTER_SEND_META_REQUEST_WITH_CORRECT_CONTENT_MD5 | AWS_S3_TESTER_SEND_META_REQUEST_EXPECT_SUCCESS;
    ASSERT_SUCCESS(s_test_s3_put_object_content_md5_helper(allocator, false, flags, AWS_MR_CONTENT_MD5_ENABLED));

    return 0;
}

AWS_TEST_CASE(
    test_s3_put_object_singlepart_correct_content_md5_disabled,
    s_test_s3_put_object_singlepart_correct_content_md5_disabled)
static int s_test_s3_put_object_singlepart_correct_content_md5_disabled(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    uint32_t flags =
        AWS_S3_TESTER_SEND_META_REQUEST_WITH_CORRECT_CONTENT_MD5 | AWS_S3_TESTER_SEND_META_REQUEST_EXPECT_SUCCESS;
    ASSERT_SUCCESS(s_test_s3_put_object_content_md5_helper(allocator, false, flags, AWS_MR_CONTENT_MD5_DISABLED));

    return 0;
}

AWS_TEST_CASE(
    test_s3_put_object_singlepart_incorrect_content_md5_enabled,
    s_test_s3_put_object_singlepart_incorrect_content_md5_enabled)
static int s_test_s3_put_object_singlepart_incorrect_content_md5_enabled(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    uint32_t flags = AWS_S3_TESTER_SEND_META_REQUEST_WITH_INCORRECT_CONTENT_MD5;
    ASSERT_SUCCESS(s_test_s3_put_object_content_md5_helper(allocator, false, flags, AWS_MR_CONTENT_MD5_ENABLED));

    return 0;
}

AWS_TEST_CASE(
    test_s3_put_object_singlepart_incorrect_content_md5_disabled,
    s_test_s3_put_object_singlepart_incorrect_content_md5_disabled)
static int s_test_s3_put_object_singlepart_incorrect_content_md5_disabled(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    uint32_t flags = AWS_S3_TESTER_SEND_META_REQUEST_WITH_INCORRECT_CONTENT_MD5;
    ASSERT_SUCCESS(s_test_s3_put_object_content_md5_helper(allocator, false, flags, AWS_MR_CONTENT_MD5_DISABLED));

    return 0;
}

AWS_TEST_CASE(
    test_s3_put_object_multipart_no_content_md5_enabled,
    s_test_s3_put_object_multipart_no_content_md5_enabled)
static int s_test_s3_put_object_multipart_no_content_md5_enabled(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    uint32_t flags = AWS_S3_TESTER_SEND_META_REQUEST_EXPECT_SUCCESS;
    ASSERT_SUCCESS(s_test_s3_put_object_content_md5_helper(allocator, true, flags, AWS_MR_CONTENT_MD5_ENABLED));

    return 0;
}

AWS_TEST_CASE(
    test_s3_put_object_multipart_no_content_md5_disabled,
    s_test_s3_put_object_multipart_no_content_md5_disabled)
static int s_test_s3_put_object_multipart_no_content_md5_disabled(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    uint32_t flags = AWS_S3_TESTER_SEND_META_REQUEST_EXPECT_SUCCESS;
    ASSERT_SUCCESS(s_test_s3_put_object_content_md5_helper(allocator, true, flags, AWS_MR_CONTENT_MD5_DISABLED));

    return 0;
}

AWS_TEST_CASE(
    test_s3_put_object_multipart_correct_content_md5_enabled,
    s_test_s3_put_object_multipart_correct_content_md5_enabled)
static int s_test_s3_put_object_multipart_correct_content_md5_enabled(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    uint32_t flags =
        AWS_S3_TESTER_SEND_META_REQUEST_WITH_CORRECT_CONTENT_MD5 | AWS_S3_TESTER_SEND_META_REQUEST_EXPECT_SUCCESS;
    ASSERT_SUCCESS(s_test_s3_put_object_content_md5_helper(allocator, true, flags, AWS_MR_CONTENT_MD5_ENABLED));

    return 0;
}

AWS_TEST_CASE(
    test_s3_put_object_multipart_correct_content_md5_disabled,
    s_test_s3_put_object_multipart_correct_content_md5_disabled)
static int s_test_s3_put_object_multipart_correct_content_md5_disabled(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    uint32_t flags =
        AWS_S3_TESTER_SEND_META_REQUEST_WITH_CORRECT_CONTENT_MD5 | AWS_S3_TESTER_SEND_META_REQUEST_EXPECT_SUCCESS;
    ASSERT_SUCCESS(s_test_s3_put_object_content_md5_helper(allocator, true, flags, AWS_MR_CONTENT_MD5_DISABLED));

    return 0;
}

AWS_TEST_CASE(
    test_s3_put_object_multipart_incorrect_content_md5_enabled,
    s_test_s3_put_object_multipart_incorrect_content_md5_enabled)
static int s_test_s3_put_object_multipart_incorrect_content_md5_enabled(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    uint32_t flags =
        AWS_S3_TESTER_SEND_META_REQUEST_WITH_INCORRECT_CONTENT_MD5 | AWS_S3_TESTER_SEND_META_REQUEST_EXPECT_SUCCESS;
    ASSERT_SUCCESS(s_test_s3_put_object_content_md5_helper(allocator, true, flags, AWS_MR_CONTENT_MD5_ENABLED));

    return 0;
}

AWS_TEST_CASE(
    test_s3_put_object_multipart_incorrect_content_md5_disabled,
    s_test_s3_put_object_multipart_incorrect_content_md5_disabled)
static int s_test_s3_put_object_multipart_incorrect_content_md5_disabled(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    uint32_t flags =
        AWS_S3_TESTER_SEND_META_REQUEST_WITH_INCORRECT_CONTENT_MD5 | AWS_S3_TESTER_SEND_META_REQUEST_EXPECT_SUCCESS;
    ASSERT_SUCCESS(s_test_s3_put_object_content_md5_helper(allocator, true, flags, AWS_MR_CONTENT_MD5_DISABLED));

    return 0;
}

static int s_test_s3_upload_part_message_helper(struct aws_allocator *allocator, bool should_compute_content_md5) {

    aws_s3_library_init(allocator);

    struct aws_byte_buf test_buffer;
    aws_s3_create_test_buffer(allocator, 19 /* size of "This is an S3 test." */, &test_buffer);
    /* base64 encoded md5 of "This is an S3 test." */
    struct aws_byte_cursor expected_content_md5 = aws_byte_cursor_from_c_str("+y3U+EY5uFXhVVmRoiJWyA==");

    struct aws_byte_cursor test_body_cursor = aws_byte_cursor_from_buf(&test_buffer);
    struct aws_input_stream *input_stream = aws_input_stream_new_from_cursor(allocator, &test_body_cursor);

    struct aws_byte_cursor host_name = aws_byte_cursor_from_c_str("dummy_host");

    struct aws_byte_cursor test_object_path = aws_byte_cursor_from_c_str("dummy_key");

    /* Put together a simple S3 Put Object request. */
    struct aws_http_message *base_message = aws_s3_test_put_object_request_new(
        allocator, host_name, test_object_path, g_test_body_content_type, input_stream, AWS_S3_TESTER_SSE_NONE);

    uint32_t part_number = 1;
    struct aws_string *upload_id = aws_string_new_from_c_str(allocator, "dummy_upload_id");

    struct aws_http_message *new_message = aws_s3_upload_part_message_new(
        allocator, base_message, &test_buffer, part_number, upload_id, should_compute_content_md5, AWS_SCA_NONE, NULL);

    struct aws_http_headers *new_headers = aws_http_message_get_headers(new_message);
    if (should_compute_content_md5) {
        ASSERT_TRUE(aws_http_headers_has(new_headers, g_content_md5_header_name));
        struct aws_byte_cursor content_md5;
        aws_http_headers_get(new_headers, g_content_md5_header_name, &content_md5);
        ASSERT_BIN_ARRAYS_EQUALS(expected_content_md5.ptr, expected_content_md5.len, content_md5.ptr, content_md5.len);
    } else {
        ASSERT_FALSE(aws_http_headers_has(new_headers, g_content_md5_header_name));
    }

    struct aws_input_stream *new_body_stream = aws_http_message_get_body_stream(new_message);
    aws_input_stream_destroy(new_body_stream);
    new_body_stream = NULL;

    aws_http_message_release(new_message);
    new_message = NULL;

    aws_http_message_release(base_message);
    base_message = NULL;

    aws_string_destroy(upload_id);
    upload_id = NULL;

    aws_input_stream_destroy(input_stream);
    input_stream = NULL;

    aws_byte_buf_clean_up(&test_buffer);

    aws_s3_library_clean_up();

    return 0;
}

AWS_TEST_CASE(test_s3_upload_part_message_with_content_md5, s_test_s3_upload_part_message_with_content_md5)
static int s_test_s3_upload_part_message_with_content_md5(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(s_test_s3_upload_part_message_helper(allocator, true));

    return 0;
}

AWS_TEST_CASE(test_s3_upload_part_message_without_content_md5, s_test_s3_upload_part_message_without_content_md5)
static int s_test_s3_upload_part_message_without_content_md5(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(s_test_s3_upload_part_message_helper(allocator, false));

    return 0;
}

AWS_TEST_CASE(
    test_s3_create_multipart_upload_message_with_content_md5,
    s_test_s3_create_multipart_upload_message_with_content_md5)
static int s_test_s3_create_multipart_upload_message_with_content_md5(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_byte_buf test_buffer;
    aws_s3_create_test_buffer(allocator, 19 /* size of "This is an S3 test." */, &test_buffer);

    struct aws_byte_cursor test_body_cursor = aws_byte_cursor_from_buf(&test_buffer);
    struct aws_input_stream *input_stream = aws_input_stream_new_from_cursor(allocator, &test_body_cursor);

    struct aws_byte_cursor host_name = aws_byte_cursor_from_c_str("dummy_host");

    struct aws_byte_cursor test_object_path = aws_byte_cursor_from_c_str("dummy_key");

    /* Put together a simple S3 Put Object request. */
    struct aws_http_message *base_message = aws_s3_test_put_object_request_new(
        allocator, host_name, test_object_path, g_test_body_content_type, input_stream, AWS_S3_TESTER_SSE_NONE);

    struct aws_http_header content_md5_header = {
        .name = g_content_md5_header_name,
        .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("dummy_content_md5"),
    };
    ASSERT_SUCCESS(aws_http_message_add_header(base_message, content_md5_header));

    struct aws_http_headers *base_headers = aws_http_message_get_headers(base_message);
    ASSERT_TRUE(aws_http_headers_has(base_headers, g_content_md5_header_name));

    struct aws_http_message *new_message =
        aws_s3_create_multipart_upload_message_new(allocator, base_message, AWS_SCA_NONE);

    struct aws_http_headers *new_headers = aws_http_message_get_headers(new_message);
    ASSERT_FALSE(aws_http_headers_has(new_headers, g_content_md5_header_name));

    aws_http_message_release(new_message);
    new_message = NULL;

    aws_http_message_release(base_message);
    base_message = NULL;

    aws_input_stream_destroy(input_stream);
    input_stream = NULL;

    aws_byte_buf_clean_up(&test_buffer);

    return 0;
}

AWS_TEST_CASE(
    test_s3_complete_multipart_message_with_content_md5,
    s_test_s3_complete_multipart_message_with_content_md5)
static int s_test_s3_complete_multipart_message_with_content_md5(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_byte_buf test_buffer;
    aws_s3_create_test_buffer(allocator, 19 /* size of "This is an S3 test." */, &test_buffer);

    struct aws_byte_cursor test_body_cursor = aws_byte_cursor_from_buf(&test_buffer);
    struct aws_input_stream *input_stream = aws_input_stream_new_from_cursor(allocator, &test_body_cursor);

    struct aws_byte_cursor host_name = aws_byte_cursor_from_c_str("dummy_host");

    struct aws_byte_cursor test_object_path = aws_byte_cursor_from_c_str("dummy_key");

    /* Put together a simple S3 Put Object request. */
    struct aws_http_message *base_message = aws_s3_test_put_object_request_new(
        allocator, host_name, test_object_path, g_test_body_content_type, input_stream, AWS_S3_TESTER_SSE_NONE);

    struct aws_http_header content_md5_header = {
        .name = g_content_md5_header_name,
        .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("dummy_content_md5"),
    };
    ASSERT_SUCCESS(aws_http_message_add_header(base_message, content_md5_header));

    struct aws_http_headers *base_headers = aws_http_message_get_headers(base_message);
    ASSERT_TRUE(aws_http_headers_has(base_headers, g_content_md5_header_name));

    struct aws_byte_buf body_buffer;
    aws_byte_buf_init(&body_buffer, allocator, 512);

    struct aws_string *upload_id = aws_string_new_from_c_str(allocator, "dummy_upload_id");

    struct aws_array_list etags;
    ASSERT_SUCCESS(aws_array_list_init_dynamic(&etags, allocator, 0, sizeof(struct aws_string)));

    struct aws_http_message *new_message = aws_s3_complete_multipart_message_new(
        allocator, base_message, &body_buffer, upload_id, &etags, NULL, AWS_SCA_NONE);

    struct aws_http_headers *new_headers = aws_http_message_get_headers(new_message);
    ASSERT_FALSE(aws_http_headers_has(new_headers, g_content_md5_header_name));

    struct aws_input_stream *new_body_stream = aws_http_message_get_body_stream(new_message);
    aws_input_stream_destroy(new_body_stream);
    new_body_stream = NULL;

    aws_http_message_release(new_message);
    new_message = NULL;

    aws_http_message_release(base_message);
    base_message = NULL;

    aws_array_list_clean_up(&etags);

    aws_string_destroy(upload_id);
    upload_id = NULL;

    aws_byte_buf_clean_up(&body_buffer);

    aws_input_stream_destroy(input_stream);
    input_stream = NULL;

    aws_byte_buf_clean_up(&test_buffer);

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

AWS_TEST_CASE(test_s3_round_trip, s_test_s3_round_trip)
static int s_test_s3_round_trip(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_tester_client_options client_options = {
        .part_size = 16 * 1024,
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    /* should we generate a unique path each time? */
    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/prefix/round_trip/test.txt");
    struct aws_s3_tester_meta_request_options put_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .client = client,
        .put_options =
            {
                .object_size_mb = 1,
                .object_path_override = object_path,
            },
    };

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &put_options, NULL));

    /*** GET FILE ***/

    struct aws_s3_tester_meta_request_options get_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_SUCCESS,
        .client = client,
        .get_options =
            {
                .object_path = object_path,
            },
    };

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &get_options, NULL));

    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(test_s3_round_trip_default_get, s_test_s3_round_trip_default_get)
static int s_test_s3_round_trip_default_get(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_tester_client_options client_options = {
        .part_size = 16 * 1024,
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    /* should we generate a unique path each time? */
    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/prefix/round_trip/test_default.txt");

    struct aws_s3_tester_meta_request_options put_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .client = client,
        .put_options =
            {
                .object_size_mb = 1,
                .object_path_override = object_path,
            },
    };

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &put_options, NULL));

    /*** GET FILE ***/

    struct aws_s3_tester_meta_request_options get_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_DEFAULT,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_SUCCESS,
        .client = client,
        .get_options =
            {
                .object_path = object_path,
            },
        .default_type_options =
            {
                .mode = AWS_S3_TESTER_DEFAULT_TYPE_MODE_GET,
            },
    };

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &get_options, NULL));

    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return 0;
}

int s_s3_validate_headers_checksum_set(
    struct aws_s3_meta_request *meta_request,
    const struct aws_http_headers *headers,
    int response_status,
    void *user_data) {
    (void)response_status;
    (void)headers;
    struct aws_s3_meta_request_test_results *meta_request_test_results =
        (struct aws_s3_meta_request_test_results *)user_data;
    ASSERT_NOT_NULL(meta_request->meta_request_level_running_response_sum);
    ASSERT_INT_EQUALS(
        meta_request->meta_request_level_running_response_sum->algorithm, meta_request_test_results->algorithm);
    return AWS_OP_SUCCESS;
}

int s_s3_validate_headers_checksum_unset(
    struct aws_s3_meta_request *meta_request,
    const struct aws_http_headers *headers,
    int response_status,
    void *user_data) {
    (void)response_status;
    (void)headers;
    (void)user_data;
    ASSERT_NULL(meta_request->meta_request_level_running_response_sum);
    return AWS_OP_SUCCESS;
}

void s_s3_test_validate_checksum(
    struct aws_s3_meta_request *meta_request,
    const struct aws_s3_meta_request_result *result,
    void *user_data) {
    (void)meta_request;
    (void)user_data;
    AWS_FATAL_ASSERT(result->did_validate);
    AWS_FATAL_ASSERT(result->error_code == AWS_OP_SUCCESS);
}
void s_s3_test_no_validate_checksum(
    struct aws_s3_meta_request *meta_request,
    const struct aws_s3_meta_request_result *result,
    void *user_data) {
    (void)meta_request;
    (void)user_data;
    AWS_FATAL_ASSERT(!result->did_validate);
    AWS_FATAL_ASSERT(result->error_code == AWS_OP_SUCCESS);
}

AWS_TEST_CASE(test_s3_round_trip_default_get_fc, s_test_s3_round_trip_default_get_fc)
static int s_test_s3_round_trip_default_get_fc(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_tester_client_options client_options = {
        .part_size = 16 * 1024,
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    /* should we generate a unique path each time? */
    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/prefix/round_trip/test_default_fc.txt");

    struct aws_s3_tester_meta_request_options put_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .client = client,
        .checksum_algorithm = AWS_SCA_CRC32,
        .validate_get_response_checksum = false,
        .put_options =
            {
                .object_size_mb = 1,
                .object_path_override = object_path,
            },
    };

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &put_options, NULL));

    /*** GET FILE ***/

    struct aws_s3_tester_meta_request_options get_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_DEFAULT,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_SUCCESS,
        .client = client,
        .checksum_algorithm = AWS_SCA_CRC32,
        .validate_get_response_checksum = true,
        .get_options =
            {
                .object_path = object_path,
            },
        .default_type_options =
            {
                .mode = AWS_S3_TESTER_DEFAULT_TYPE_MODE_GET,
            },
        .finish_callback = s_s3_test_validate_checksum,
        .headers_callback = s_s3_validate_headers_checksum_set,
    };

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &get_options, NULL));

    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(test_s3_round_trip_multipart_get_fc, s_test_s3_round_trip_multipart_get_fc)
static int s_test_s3_round_trip_multipart_get_fc(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_tester_client_options client_options = {
        .part_size = 16 * 1024,
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    /* should we generate a unique path each time? */
    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/prefix/round_trip/test_fc.txt");

    struct aws_s3_tester_meta_request_options put_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .client = client,
        .checksum_algorithm = AWS_SCA_CRC32,
        .validate_get_response_checksum = false,
        .put_options =
            {
                .object_size_mb = 1,
                .object_path_override = object_path,
            },
    };

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &put_options, NULL));

    /*** GET FILE ***/

    struct aws_s3_tester_meta_request_options get_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_SUCCESS,
        .client = client,
        .checksum_algorithm = AWS_SCA_CRC32,
        .validate_get_response_checksum = true,
        .get_options =
            {
                .object_path = object_path,
            },
        .finish_callback = s_s3_test_validate_checksum,
        .headers_callback = s_s3_validate_headers_checksum_set,
    };

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &get_options, NULL));

    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(test_s3_round_trip_mpu_multipart_get_fc, s_test_s3_round_trip_mpu_multipart_get_fc)
static int s_test_s3_round_trip_mpu_multipart_get_fc(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_tester_client_options client_options = {
        .part_size = MB_TO_BYTES(5),
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    /* should we generate a unique path each time? */
    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/prefix/round_trip/test_mpu_fc.txt");

    struct aws_s3_tester_meta_request_options put_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .client = client,
        .checksum_algorithm = AWS_SCA_CRC32,
        .validate_get_response_checksum = false,
        .put_options =
            {
                .object_size_mb = 10,
                .object_path_override = object_path,
            },
    };

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &put_options, NULL));

    /*** GET FILE ***/

    struct aws_s3_tester_meta_request_options get_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_SUCCESS,
        .client = client,
        .checksum_algorithm = AWS_SCA_CRC32,
        .validate_get_response_checksum = true,
        .get_options =
            {
                .object_path = object_path,
            },
        .finish_callback = s_s3_test_no_validate_checksum,
        .headers_callback = s_s3_validate_headers_checksum_unset,
    };

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &get_options, NULL));

    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(test_s3_round_trip_mpu_default_get_fc, s_test_s3_round_trip_mpu_default_get_fc)
static int s_test_s3_round_trip_mpu_default_get_fc(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_tester_client_options client_options = {
        .part_size = MB_TO_BYTES(5),
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    /* should we generate a unique path each time? */
    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/prefix/round_trip/test_mpu_default_get_fc.txt");

    struct aws_s3_tester_meta_request_options put_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .client = client,
        .checksum_algorithm = AWS_SCA_CRC32,
        .validate_get_response_checksum = false,
        .put_options =
            {
                .object_size_mb = 10,
                .object_path_override = object_path,
            },
    };

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &put_options, NULL));

    /*** GET FILE ***/

    struct aws_s3_tester_meta_request_options get_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_DEFAULT,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_SUCCESS,
        .client = client,
        .checksum_algorithm = AWS_SCA_CRC32,
        .validate_get_response_checksum = true,
        .get_options =
            {
                .object_path = object_path,
            },
        .default_type_options =
            {
                .mode = AWS_S3_TESTER_DEFAULT_TYPE_MODE_GET,
            },
        .finish_callback = s_s3_test_no_validate_checksum,
        .headers_callback = s_s3_validate_headers_checksum_unset,
    };

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &get_options, NULL));

    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

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

    ASSERT_TRUE(meta_request_test_results.finished_error_code == AWS_IO_DNS_INVALID_NAME);

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

static struct aws_atomic_var s_test_headers_callback_invoked;

static int s_s3_test_headers_callback_check_returns_success(
    struct aws_s3_meta_request *meta_request,
    const struct aws_http_headers *headers,
    int response_status,
    void *user_data) {
    (void)meta_request;
    (void)headers;
    (void)response_status;
    (void)user_data;

    /* increments counter to check if callback was invoked exactly once */
    aws_atomic_fetch_add(&s_test_headers_callback_invoked, 1);

    return AWS_OP_SUCCESS;
}

static int s_s3_test_headers_callback_check_returns_error(
    struct aws_s3_meta_request *meta_request,
    const struct aws_http_headers *headers,
    int response_status,
    void *user_data) {
    (void)meta_request;
    (void)headers;
    (void)response_status;
    (void)user_data;

    /* increments counter to check if callback was invoked exactly once */
    aws_atomic_fetch_add(&s_test_headers_callback_invoked, 1);

    aws_raise_error(AWS_ERROR_UNKNOWN);
    return AWS_OP_ERR;
}

AWS_TEST_CASE(test_s3_default_invoke_headers_callback_on_error, s_test_s3_default_invoke_headers_callback_on_error)
static int s_test_s3_default_invoke_headers_callback_on_error(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_meta_request_test_results meta_request_test_results;
    AWS_ZERO_STRUCT(meta_request_test_results);
    aws_atomic_init_int(&s_test_headers_callback_invoked, 0);

    struct aws_byte_cursor invalid_path = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("___INVALID_PATH___");

    struct aws_s3_tester_meta_request_options options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_DEFAULT,
        .headers_callback = s_s3_test_headers_callback_check_returns_success,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE,
        .default_type_options =
            {
                .mode = AWS_S3_TESTER_DEFAULT_TYPE_MODE_GET,
            },
        .get_options =
            {
                .object_path = invalid_path,
            },
    };

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(NULL, &options, &meta_request_test_results));
    ASSERT_INT_EQUALS(aws_atomic_load_int(&s_test_headers_callback_invoked), 1);
    ASSERT_TRUE(meta_request_test_results.finished_error_code == AWS_ERROR_S3_INVALID_RESPONSE_STATUS);

    aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);

    return 0;
}

AWS_TEST_CASE(
    test_s3_default_invoke_headers_callback_cancels_on_error,
    s_test_s3_default_invoke_headers_callback_cancels_on_error)
static int s_test_s3_default_invoke_headers_callback_cancels_on_error(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_meta_request_test_results meta_request_test_results;
    AWS_ZERO_STRUCT(meta_request_test_results);
    aws_atomic_init_int(&s_test_headers_callback_invoked, 0);

    struct aws_byte_cursor invalid_path = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("___INVALID_PATH___");

    struct aws_s3_tester_meta_request_options options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_DEFAULT,
        .headers_callback = s_s3_test_headers_callback_check_returns_error,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE,
        .default_type_options =
            {
                .mode = AWS_S3_TESTER_DEFAULT_TYPE_MODE_GET,
            },
        .get_options =
            {
                .object_path = invalid_path,
            },
    };

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(NULL, &options, &meta_request_test_results));
    ASSERT_INT_EQUALS(aws_atomic_load_int(&s_test_headers_callback_invoked), 1);
    ASSERT_TRUE(meta_request_test_results.finished_error_code == AWS_ERROR_UNKNOWN);

    aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);

    return 0;
}

AWS_TEST_CASE(
    test_s3_get_object_invoke_headers_callback_on_error,
    s_test_s3_get_object_invoke_headers_callback_on_error)
static int s_test_s3_get_object_invoke_headers_callback_on_error(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_meta_request_test_results meta_request_test_results;
    AWS_ZERO_STRUCT(meta_request_test_results);
    aws_atomic_init_int(&s_test_headers_callback_invoked, 0);

    struct aws_byte_cursor invalid_path = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("___INVALID_PATH___");

    struct aws_s3_tester_meta_request_options options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .headers_callback = s_s3_test_headers_callback_check_returns_success,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE,
        .get_options =
            {
                .object_path = invalid_path,
            },
    };

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(NULL, &options, &meta_request_test_results));
    ASSERT_INT_EQUALS(aws_atomic_load_int(&s_test_headers_callback_invoked), 1);
    ASSERT_TRUE(meta_request_test_results.finished_error_code == AWS_ERROR_S3_INVALID_RESPONSE_STATUS);

    aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);

    return 0;
}

AWS_TEST_CASE(
    test_s3_put_object_invoke_headers_callback_on_error,
    s_test_s3_put_object_invoke_headers_callback_on_error)
static int s_test_s3_put_object_invoke_headers_callback_on_error(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_meta_request_test_results meta_request_test_results;
    AWS_ZERO_STRUCT(meta_request_test_results);
    aws_atomic_init_int(&s_test_headers_callback_invoked, 0);

    struct aws_s3_tester_meta_request_options options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .headers_callback = s_s3_test_headers_callback_check_returns_success,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE,
        .put_options =
            {
                .object_size_mb = 10,
                .invalid_request = true,
            },
    };

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(NULL, &options, &meta_request_test_results));
    ASSERT_INT_EQUALS(aws_atomic_load_int(&s_test_headers_callback_invoked), 1);
    ASSERT_UINT_EQUALS(meta_request_test_results.finished_error_code, AWS_ERROR_S3_INVALID_RESPONSE_STATUS);

    aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);

    return 0;
}

AWS_TEST_CASE(
    test_s3_put_object_invoke_headers_callback_on_error_with_user_cancellation,
    s_test_s3_put_object_invoke_headers_callback_on_error_with_user_cancellation)
static int s_test_s3_put_object_invoke_headers_callback_on_error_with_user_cancellation(
    struct aws_allocator *allocator,
    void *ctx) {
    (void)ctx;

    struct aws_s3_meta_request_test_results meta_request_test_results;
    AWS_ZERO_STRUCT(meta_request_test_results);
    aws_atomic_init_int(&s_test_headers_callback_invoked, 0);

    struct aws_s3_tester_meta_request_options options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .headers_callback = s_s3_test_headers_callback_check_returns_error,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE,
        .put_options =
            {
                .ensure_multipart = true,
            },
    };

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(NULL, &options, &meta_request_test_results));
    ASSERT_INT_EQUALS(aws_atomic_load_int(&s_test_headers_callback_invoked), 1);
    ASSERT_UINT_EQUALS(meta_request_test_results.finished_error_code, AWS_ERROR_UNKNOWN);

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
                .object_size_mb = 1,
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

static void s_s3_test_user_agent_meta_request_finished_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    int error_code) {

    AWS_ASSERT(meta_request != NULL);

    struct aws_s3_meta_request_test_results *results = meta_request->user_data;
    AWS_ASSERT(results != NULL);

    struct aws_s3_tester *tester = results->tester;
    AWS_ASSERT(tester != NULL);

    struct aws_byte_buf expected_user_agent_value_buf;
    s_get_expected_user_agent(meta_request->allocator, &expected_user_agent_value_buf);

    struct aws_byte_cursor expected_user_agent_value = aws_byte_cursor_from_buf(&expected_user_agent_value_buf);

    struct aws_http_message *message = request->send_data.message;
    struct aws_http_headers *headers = aws_http_message_get_headers(message);

    struct aws_byte_cursor user_agent_value;
    AWS_ZERO_STRUCT(user_agent_value);

    AWS_FATAL_ASSERT(aws_http_headers_get(headers, g_user_agent_header_name, &user_agent_value) == AWS_OP_SUCCESS);
    AWS_FATAL_ASSERT(aws_byte_cursor_eq(&user_agent_value, &expected_user_agent_value));
    aws_byte_buf_clean_up(&expected_user_agent_value_buf);

    struct aws_s3_meta_request_vtable *original_meta_request_vtable =
        aws_s3_tester_get_meta_request_vtable_patch(tester, 0)->original_vtable;

    original_meta_request_vtable->finished_request(meta_request, request, error_code);
}

static struct aws_s3_meta_request *s_s3_meta_request_factory_override_finished_request(
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
    patched_meta_request_vtable->finished_request = s_s3_test_user_agent_meta_request_finished_request;

    return meta_request;
}

int s_s3_test_sending_user_agent_create_client(struct aws_s3_tester *tester, struct aws_s3_client **client) {
    AWS_ASSERT(tester);

    struct aws_s3_tester_client_options client_options;
    AWS_ZERO_STRUCT(client_options);

    ASSERT_SUCCESS(aws_s3_tester_client_new(tester, &client_options, client));

    struct aws_s3_client_vtable *patched_client_vtable = aws_s3_tester_patch_client_vtable(tester, *client, NULL);
    patched_client_vtable->meta_request_factory = s_s3_meta_request_factory_override_finished_request;

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

AWS_TEST_CASE(test_s3_default_sending_meta_request_user_agent, s_test_s3_default_sending_meta_request_user_agent)
static int s_test_s3_default_sending_meta_request_user_agent(struct aws_allocator *allocator, void *ctx) {
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

struct range_requests_test_user_data {
    struct aws_http_headers *headers;
    struct aws_byte_buf *body_buffer;
};

static int s_range_requests_headers_callback(
    struct aws_s3_meta_request *meta_request,
    const struct aws_http_headers *headers,
    int response_status,
    void *user_data) {
    (void)meta_request;
    (void)response_status;

    struct aws_s3_meta_request_test_results *test_results = user_data;
    struct range_requests_test_user_data *test_user_data = test_results->tester->user_data;

    if (test_user_data != NULL) {
        copy_http_headers(headers, test_user_data->headers);
    }

    return AWS_OP_SUCCESS;
}

static int s_range_requests_receive_body_callback(
    struct aws_s3_meta_request *meta_request,
    const struct aws_byte_cursor *body,
    uint64_t range_start,
    void *user_data) {

    (void)meta_request;
    (void)range_start;

    struct aws_s3_meta_request_test_results *test_results = user_data;
    struct range_requests_test_user_data *test_user_data = test_results->tester->user_data;

    aws_byte_buf_append_dynamic(test_user_data->body_buffer, body);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(test_s3_range_requests, s_test_s3_range_requests)
static int s_test_s3_range_requests(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    const struct aws_byte_cursor object_names[] = {
        g_pre_existing_object_1MB,
        g_pre_existing_object_kms_10MB,
        g_pre_existing_object_aes256_10MB,
    };

    enum aws_s3_tester_sse_type object_sse_types[] = {
        AWS_S3_TESTER_SSE_NONE,
        AWS_S3_TESTER_SSE_KMS,
        AWS_S3_TESTER_SSE_AES256,
    };

    const struct aws_byte_cursor ranges[] = {
        // No range at all.
        {0, NULL},

        // First 8K.  8K < client's 16K part size.
        AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("bytes=0-8191"),

        // First 0.5 MB.  0.5 MB < 1 MB test file.
        AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("bytes=0-524287"),

        // 0.5 MB - 2 MB range.  This overlaps and goes beyond the 1 MB test file size.
        AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("bytes=524288-2097151"),

        // Get everything after the first 0.5 MB
        AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("bytes=524288-"),

        // Last 0.5 MB
        AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("bytes=-524288"),

        // Everything after first 8K
        AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("bytes=8192-"),

        // Last 8K
        AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("bytes=-8192"),
    };

    /* List of headers that should have matching values between the auto_ranged_get and default (which sends the HTTP
     * request as-is to S3) meta request.*/
    const struct aws_byte_cursor headers_that_should_match[] = {
        AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("ETag"),
        AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Accept-Ranges"),
        AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Content-Range"),
        AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Content-Type"),
        AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Content-Length"),
        AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Server"),
        AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-server-side-encryption"),
        AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-server-side-encryption-aws-kms-key"),
    };

    /* List of headers that are okay to be in the auto_ranged_get response and not in the default response, or vice
     * versa.*/
    const struct aws_byte_cursor headers_to_ignore[] = {
        AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Connection"),
    };

    struct aws_s3_tester_client_options client_options = {
        .part_size = 16 * 1024,
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    const size_t num_object_names = sizeof(object_names) / sizeof(object_names[0]);
    const size_t num_ranges = sizeof(ranges) / sizeof(ranges[0]);

    for (size_t object_name_index = 0; object_name_index < num_object_names; ++object_name_index) {
        for (size_t range_index = 0; range_index < num_ranges; ++range_index) {

            AWS_LOGF_INFO(
                AWS_LS_S3_GENERAL, "Testing object name %d and range %d", (int)object_name_index, (int)range_index);

            struct aws_byte_buf range_get_buffer;
            aws_byte_buf_init(&range_get_buffer, allocator, 256);
            struct aws_http_headers *range_get_headers = aws_http_headers_new(allocator);

            struct aws_byte_buf verify_range_get_buffer;
            aws_byte_buf_init(&verify_range_get_buffer, allocator, 256);
            struct aws_http_headers *verify_range_get_headers = aws_http_headers_new(allocator);

            struct aws_s3_tester_meta_request_options options = {
                .allocator = allocator,
                .client = client,
                .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
                .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_SUCCESS,
                .headers_callback = s_range_requests_headers_callback,
                .body_callback = s_range_requests_receive_body_callback,
                .get_options =
                    {
                        .object_path = object_names[object_name_index],
                        .object_range = ranges[range_index],
                    },
                .sse_type = object_sse_types[object_name_index],
            };

            {
                struct range_requests_test_user_data test_user_data = {
                    .headers = range_get_headers,
                    .body_buffer = &range_get_buffer,
                };

                tester.user_data = &test_user_data;

                ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &options, NULL));
            }

            /* Send a default meta request (which just pushes the request directly to S3) with the same options to
             * verify the format of each request. */
            struct aws_s3_tester_meta_request_options verify_options = {
                .allocator = allocator,
                .client = client,
                .meta_request_type = AWS_S3_META_REQUEST_TYPE_DEFAULT,
                .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_SUCCESS,
                .headers_callback = s_range_requests_headers_callback,
                .body_callback = s_range_requests_receive_body_callback,
                .default_type_options =
                    {
                        .mode = AWS_S3_TESTER_DEFAULT_TYPE_MODE_GET,
                    },
                .get_options =
                    {
                        .object_path = object_names[object_name_index],
                        .object_range = ranges[range_index],
                    },
                .sse_type = object_sse_types[object_name_index],
            };

            {
                struct range_requests_test_user_data test_user_data = {
                    .headers = verify_range_get_headers,
                    .body_buffer = &verify_range_get_buffer,
                };

                tester.user_data = &test_user_data;

                ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &verify_options, NULL));
            }

            /* Compare headers. */
            for (size_t i = 0; i < aws_http_headers_count(verify_range_get_headers); ++i) {
                struct aws_http_header verify_header;
                ASSERT_SUCCESS(aws_http_headers_get_index(verify_range_get_headers, i, &verify_header));

                bool ignore_header = false;

                for (size_t j = 0; j < sizeof(headers_to_ignore) / sizeof(headers_to_ignore[0]); ++j) {
                    if (aws_byte_cursor_eq_ignore_case(&headers_to_ignore[j], &verify_header.name)) {
                        ignore_header = true;
                        break;
                    }
                }

                if (ignore_header) {
                    aws_http_headers_erase(range_get_headers, verify_header.name);
                    continue;
                }

                AWS_LOGF_INFO(
                    AWS_LS_S3_GENERAL,
                    "%d,%d Checking for header " PRInSTR,
                    (int)object_name_index,
                    (int)range_index,
                    AWS_BYTE_CURSOR_PRI(verify_header.name));

                struct aws_byte_cursor header_value;
                ASSERT_SUCCESS(aws_http_headers_get(range_get_headers, verify_header.name, &header_value));

                for (size_t j = 0; j < sizeof(headers_that_should_match) / sizeof(headers_that_should_match[0]); ++j) {
                    if (!aws_byte_cursor_eq_ignore_case(&headers_that_should_match[j], &verify_header.name)) {
                        continue;
                    }

                    AWS_LOGF_INFO(
                        AWS_LS_S3_GENERAL,
                        "%d,%d Header Contents " PRInSTR " vs " PRInSTR,
                        (int)object_name_index,
                        (int)range_index,
                        AWS_BYTE_CURSOR_PRI(verify_header.value),
                        AWS_BYTE_CURSOR_PRI(header_value));

                    ASSERT_TRUE(aws_byte_cursor_eq(&verify_header.value, &header_value));
                }

                aws_http_headers_erase(range_get_headers, verify_header.name);
            }

            for (size_t i = 0; i < aws_http_headers_count(range_get_headers); ++i) {
                struct aws_http_header header;

                ASSERT_SUCCESS(aws_http_headers_get_index(range_get_headers, i, &header));

                AWS_LOGF_INFO(AWS_LS_S3_GENERAL, "Left over header: " PRInSTR, AWS_BYTE_CURSOR_PRI(header.name));
            }

            ASSERT_TRUE(aws_http_headers_count(range_get_headers) == 0);

            /* Compare Body Contents */
            ASSERT_TRUE(aws_byte_buf_eq(&range_get_buffer, &verify_range_get_buffer));

            aws_http_headers_release(range_get_headers);
            aws_byte_buf_clean_up(&range_get_buffer);

            aws_http_headers_release(verify_range_get_headers);
            aws_byte_buf_clean_up(&verify_range_get_buffer);
        }
    }

    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(test_s3_not_satisfiable_range, s_test_s3_not_satisfiable_range)
static int s_test_s3_not_satisfiable_range(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_tester_client_options client_options = {
        .part_size = 16 * 1024,
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_s3_tester_meta_request_options options = {
        .allocator = allocator,
        .client = client,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE,
        .headers_callback = s_range_requests_headers_callback,
        .body_callback = s_range_requests_receive_body_callback,
        .get_options =
            {
                .object_path = g_pre_existing_object_1MB,
                .object_range = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("bytes=2097151-"),
            },
    };

    struct aws_s3_meta_request_test_results results;
    AWS_ZERO_STRUCT(results);

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &options, &results));

    ASSERT_TRUE(results.finished_response_status == AWS_HTTP_STATUS_CODE_416_REQUESTED_RANGE_NOT_SATISFIABLE);

    aws_s3_meta_request_test_results_clean_up(&results);

    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return 0;
}

static const struct aws_byte_cursor g_x_amz_copy_source_name =
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-copy-source");

struct aws_http_message *copy_object_request_new(
    struct aws_allocator *allocator,
    struct aws_byte_cursor x_amz_source,
    struct aws_byte_cursor endpoint,
    struct aws_byte_cursor destination_key) {

    AWS_PRECONDITION(allocator);

    struct aws_http_message *message = aws_http_message_new_request(allocator);

    if (message == NULL) {
        return NULL;
    }

    /* the URI path is / followed by the key */
    char destination_path[1024];
    snprintf(destination_path, sizeof(destination_path), "/%.*s", (int)destination_key.len, destination_key.ptr);

    if (aws_http_message_set_request_path(message, aws_byte_cursor_from_c_str(destination_path))) {
        goto error_clean_up_message;
    }

    struct aws_http_header host_header = {.name = g_host_header_name, .value = endpoint};
    if (aws_http_message_add_header(message, host_header)) {
        goto error_clean_up_message;
    }

    struct aws_byte_buf copy_source_value_encoded;
    aws_byte_buf_init(&copy_source_value_encoded, allocator, 1024);
    aws_byte_buf_append_encoding_uri_param(&copy_source_value_encoded, &x_amz_source);

    struct aws_http_header copy_source_header = {
        .name = g_x_amz_copy_source_name,
        .value = aws_byte_cursor_from_buf(&copy_source_value_encoded),
    };

    if (aws_http_message_add_header(message, copy_source_header)) {
        goto error_clean_up_message;
    }

    if (aws_http_message_set_request_method(message, aws_http_method_put)) {
        goto error_clean_up_message;
    }

    aws_byte_buf_clean_up(&copy_source_value_encoded);
    return message;

error_clean_up_message:

    aws_byte_buf_clean_up(&copy_source_value_encoded);
    if (message != NULL) {
        aws_http_message_release(message);
        message = NULL;
    }

    return NULL;
}

struct copy_object_test_data {
    struct aws_mutex mutex;
    struct aws_condition_variable c_var;
    bool execution_completed;
    bool headers_callback_was_invoked;
    int meta_request_error_code;
    int response_status_code;
};

static void s_copy_object_meta_request_finish(
    struct aws_s3_meta_request *meta_request,
    const struct aws_s3_meta_request_result *meta_request_result,
    void *user_data) {

    (void)meta_request;

    struct copy_object_test_data *test_data = user_data;

    /* if error response body is available, dump it to test result to help investigation of failed tests */
    if (meta_request_result->error_response_body != NULL && meta_request_result->error_response_body->len > 0) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_GENERAL,
            "Response error body: %.*s",
            (int)meta_request_result->error_response_body->len,
            meta_request_result->error_response_body->buffer);
    }

    aws_mutex_lock(&test_data->mutex);
    test_data->meta_request_error_code = meta_request_result->error_code;
    test_data->response_status_code = meta_request_result->response_status;
    test_data->execution_completed = true;
    aws_mutex_unlock(&test_data->mutex);
    aws_condition_variable_notify_one(&test_data->c_var);
}

static int s_copy_object_meta_request_headers_callback(
    struct aws_s3_meta_request *meta_request,
    const struct aws_http_headers *headers,
    int response_status,
    void *user_data) {

    (void)meta_request;
    (void)headers;
    (void)response_status;

    struct copy_object_test_data *test_data = user_data;

    aws_mutex_lock(&test_data->mutex);
    test_data->headers_callback_was_invoked = true;
    aws_mutex_unlock(&test_data->mutex);

    return AWS_OP_SUCCESS;
}

static bool s_copy_test_completion_predicate(void *arg) {
    struct copy_object_test_data *test_data = arg;
    return test_data->execution_completed;
}

static int s_test_s3_copy_object_from_x_amz_copy_source(
    struct aws_allocator *allocator,
    struct aws_byte_cursor x_amz_copy_source,
    struct aws_byte_cursor destination_key,
    int expected_error_code,
    int expected_response_status) {

    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client_config client_config;
    AWS_ZERO_STRUCT(client_config);

    ASSERT_SUCCESS(aws_s3_tester_bind_client(
        &tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION | AWS_S3_TESTER_BIND_CLIENT_SIGNING));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    struct aws_byte_cursor destination_bucket = g_test_bucket_name;

    char endpoint[1024];
    snprintf(
        endpoint,
        sizeof(endpoint),
        "%.*s.s3.%s.amazonaws.com",
        (int)destination_bucket.len,
        destination_bucket.ptr,
        g_test_s3_region.ptr);

    /* creates a CopyObject request */
    struct aws_http_message *message =
        copy_object_request_new(allocator, x_amz_copy_source, aws_byte_cursor_from_c_str(endpoint), destination_key);

    struct copy_object_test_data test_data;
    AWS_ZERO_STRUCT(test_data);

    test_data.c_var = (struct aws_condition_variable)AWS_CONDITION_VARIABLE_INIT;
    aws_mutex_init(&test_data.mutex);

    struct aws_s3_meta_request_options meta_request_options = {
        .user_data = &test_data,
        .body_callback = NULL,
        .signing_config = client_config.signing_config,
        .finish_callback = s_copy_object_meta_request_finish,
        .headers_callback = s_copy_object_meta_request_headers_callback,
        .message = message,
        .shutdown_callback = NULL,
        .type = AWS_S3_META_REQUEST_TYPE_COPY_OBJECT,
    };

    struct aws_s3_meta_request *meta_request = aws_s3_client_make_meta_request(client, &meta_request_options);
    ASSERT_NOT_NULL(meta_request);

    /* wait completion of the meta request */
    aws_mutex_lock(&test_data.mutex);
    aws_condition_variable_wait_pred(&test_data.c_var, &test_data.mutex, s_copy_test_completion_predicate, &test_data);
    aws_mutex_unlock(&test_data.mutex);

    /* assert error_code and response_status_code */
    ASSERT_INT_EQUALS(expected_error_code, test_data.meta_request_error_code);
    ASSERT_INT_EQUALS(expected_response_status, test_data.response_status_code);

    /* assert headers callback was invoked */
    ASSERT_TRUE(test_data.headers_callback_was_invoked);

    aws_s3_meta_request_release(meta_request);
    aws_mutex_clean_up(&test_data.mutex);
    aws_http_message_destroy(message);
    aws_s3_client_release(client);
    client = NULL;

    aws_s3_tester_clean_up(&tester);

    return 0;
}

static int s_test_s3_copy_object_helper(
    struct aws_allocator *allocator,
    struct aws_byte_cursor source_key,
    struct aws_byte_cursor destination_key,
    int expected_error_code,
    int expected_response_status) {

    struct aws_byte_cursor source_bucket = g_test_bucket_name;

    char copy_source_value[1024];
    snprintf(
        copy_source_value,
        sizeof(copy_source_value),
        "%.*s/%.*s",
        (int)source_bucket.len,
        source_bucket.ptr,
        (int)source_key.len,
        source_key.ptr);

    struct aws_byte_cursor x_amz_copy_source = aws_byte_cursor_from_c_str(copy_source_value);

    return s_test_s3_copy_object_from_x_amz_copy_source(
        allocator, x_amz_copy_source, destination_key, expected_error_code, expected_response_status);
}

AWS_TEST_CASE(test_s3_copy_small_object, s_test_s3_copy_small_object)
static int s_test_s3_copy_small_object(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_byte_cursor source_key = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("pre-existing_object_1MB.txt");
    struct aws_byte_cursor destination_key = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("copies/get_object_test_1MB.txt");
    return s_test_s3_copy_object_helper(
        allocator, source_key, destination_key, AWS_ERROR_SUCCESS, AWS_HTTP_STATUS_CODE_200_OK);
}

AWS_TEST_CASE(test_s3_multipart_copy_large_object, s_test_s3_multipart_copy_large_object)
static int s_test_s3_multipart_copy_large_object(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_byte_cursor source_key = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("put_object_test_5GB_1.txt");
    struct aws_byte_cursor destination_key = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("copies/put_object_test_5GB_1.txt");
    return s_test_s3_copy_object_helper(
        allocator, source_key, destination_key, AWS_ERROR_SUCCESS, AWS_HTTP_STATUS_CODE_200_OK);
}

AWS_TEST_CASE(test_s3_copy_object_invalid_source_key, s_test_s3_copy_object_invalid_source_key)
static int s_test_s3_copy_object_invalid_source_key(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_byte_cursor source_key = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("__INVALID__");
    struct aws_byte_cursor destination_key = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("copies/__INVALID__");
    return s_test_s3_copy_object_helper(
        allocator,
        source_key,
        destination_key,
        AWS_ERROR_S3_INVALID_RESPONSE_STATUS,
        AWS_HTTP_STATUS_CODE_404_NOT_FOUND);
}

/**
 * Test a bypass Copy Object meta request using a slash prefix in the x_amz_copy_source header.
 * S3 supports both bucket/key and /bucket/key
 * This test validates the fix for the bug described in https://sim.amazon.com/issues/AWSCRT-730
 */
AWS_TEST_CASE(test_s3_copy_source_prefixed_by_slash, s_test_s3_copy_source_prefixed_by_slash)
static int s_test_s3_copy_source_prefixed_by_slash(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_byte_cursor source_key = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("pre-existing_object_1MB.txt");
    struct aws_byte_cursor destination_key = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("copies/get_object_test_1MB.txt");

    struct aws_byte_cursor source_bucket = g_test_bucket_name;

    char copy_source_value[1024];
    snprintf(
        copy_source_value,
        sizeof(copy_source_value),
        "/%.*s/%.*s",
        (int)source_bucket.len,
        source_bucket.ptr,
        (int)source_key.len,
        source_key.ptr);

    struct aws_byte_cursor x_amz_copy_source = aws_byte_cursor_from_c_str(copy_source_value);

    return s_test_s3_copy_object_from_x_amz_copy_source(
        allocator, x_amz_copy_source, destination_key, AWS_ERROR_SUCCESS, AWS_HTTP_STATUS_CODE_200_OK);
}

/**
 * Test multipart Copy Object meta request using a slash prefix in the x_amz_copy_source header.
 * S3 supports both bucket/key and /bucket/key
 * This test validates the fix for the bug described in https://sim.amazon.com/issues/AWSCRT-730
 */
AWS_TEST_CASE(test_s3_copy_source_prefixed_by_slash_multipart, s_test_s3_copy_source_prefixed_by_slash_multipart)
static int s_test_s3_copy_source_prefixed_by_slash_multipart(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_byte_cursor source_key = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("put_object_test_5GB_1.txt");
    struct aws_byte_cursor destination_key = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("copies/put_object_test_5GB_1.txt");

    struct aws_byte_cursor source_bucket = g_test_bucket_name;

    char copy_source_value[1024];
    snprintf(
        copy_source_value,
        sizeof(copy_source_value),
        "/%.*s/%.*s",
        (int)source_bucket.len,
        source_bucket.ptr,
        (int)source_key.len,
        source_key.ptr);

    struct aws_byte_cursor x_amz_copy_source = aws_byte_cursor_from_c_str(copy_source_value);

    return s_test_s3_copy_object_from_x_amz_copy_source(
        allocator, x_amz_copy_source, destination_key, AWS_ERROR_SUCCESS, AWS_HTTP_STATUS_CODE_200_OK);
}
