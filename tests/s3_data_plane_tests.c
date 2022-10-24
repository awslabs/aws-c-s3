/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_checksums.h"
#include "aws/s3/private/s3_client_impl.h"
#include "aws/s3/private/s3_meta_request_impl.h"
#include "aws/s3/private/s3_util.h"
#include "aws/s3/s3_client.h"
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

AWS_TEST_CASE(test_s3_client_monitoring_options_override, s_test_s3_client_monitoring_options_override)
static int s_test_s3_client_monitoring_options_override(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_http_connection_monitoring_options monitoring_options = {.minimum_throughput_bytes_per_second = 3000};

    struct aws_s3_client_config client_config = {.monitoring_options = &monitoring_options};

    ASSERT_SUCCESS(aws_s3_tester_bind_client(&tester, &client_config, 0));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    ASSERT_TRUE(
        client->monitoring_options->minimum_throughput_bytes_per_second ==
        client_config.monitoring_options->minimum_throughput_bytes_per_second);

    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(test_s3_client_proxy_ev_settings_override, s_test_s3_client_proxy_ev_settings_override)
static int s_test_s3_client_proxy_ev_settings_override(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_tls_connection_options tls_conn_options;
    AWS_ZERO_STRUCT(tls_conn_options);

    struct proxy_env_var_settings proxy_ev_settings = {.env_var_type = AWS_HPEV_ENABLE,
                                                       .tls_options = &tls_conn_options};

    struct aws_s3_client_config client_config = {.proxy_ev_settings = &proxy_ev_settings};

    ASSERT_SUCCESS(aws_s3_tester_bind_client(&tester, &client_config, 0));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    ASSERT_TRUE(client->proxy_ev_settings->env_var_type == client_config.proxy_ev_settings->env_var_type);

    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(test_s3_client_tcp_keep_alive_options_override, s_test_s3_client_tcp_keep_alive_options_override)
static int s_test_s3_client_tcp_keep_alive_options_override(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_tcp_keep_alive_options keep_alive_options = {.keep_alive_interval_sec = 20};

    struct aws_s3_client_config client_config = {.tcp_keep_alive_options = &keep_alive_options};

    ASSERT_SUCCESS(aws_s3_tester_bind_client(&tester, &client_config, 0));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    ASSERT_TRUE(
        client->tcp_keep_alive_options->keep_alive_interval_sec ==
        client_config.tcp_keep_alive_options->keep_alive_interval_sec);

    aws_s3_client_release(client);
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
    aws_s3_meta_request_test_results_init(&meta_request_test_results, allocator);

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
        aws_s3_meta_request_test_results_init(&meta_request_test_results, allocator);

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
        aws_s3_meta_request_test_results_init(&meta_request_test_results, allocator);

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
    aws_s3_meta_request_test_results_init(&meta_request_test_results, allocator);

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

        aws_s3_meta_request_test_results_init(&meta_request_test_results[i], allocator);

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

/**
 * Test read-backpressure functionality by repeatedly:
 * - letting the download stall
 * - incrementing the read window
 * - repeat...
 */
static int s_apply_backpressure_until_meta_request_finish(
    struct aws_s3_tester *tester,
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_meta_request_test_results *test_results,
    size_t part_size,
    size_t window_initial_size,
    uint64_t window_increment_size) {

    /* Remember the last time something happened (we received download data, or incremented read window) */
    uint64_t last_time_something_happened;
    ASSERT_SUCCESS(aws_sys_clock_get_ticks(&last_time_something_happened));

    /* To ensure that backpressure is working, we wait a bit after download stalls
     * before incrementing the read window again.
     * This number also controls the max time we wait for bytes to start arriving
     * after incrementing the window.
     * If the magic number is too high the test will be slow,
     * if it's too low the test will fail on slow networks */
    const uint64_t wait_duration_with_nothing_happening =
        aws_timestamp_convert(3, AWS_TIMESTAMP_SECS, AWS_TIMESTAMP_NANOS, NULL);

    uint64_t accumulated_window_increments = window_initial_size;
    uint64_t accumulated_data_size = 0;

    while (true) {
        /* Check if meta-request is done (don't exit yet, we want to check some numbers first...) */
        aws_s3_tester_lock_synced_data(tester);
        bool done = tester->synced_data.meta_requests_finished != 0;
        aws_s3_tester_unlock_synced_data(tester);

        /* Check how much data we've received */
        size_t received_body_size_delta = aws_atomic_exchange_int(&test_results->received_body_size_delta, 0);
        accumulated_data_size += (uint64_t)received_body_size_delta;

        /* Check that we haven't received more data than the window allows.
         * TODO: Stop allowing "hacky wiggle room". The current implementation
         *       may push more bytes to the user (up to 1 part) than they've asked for. */
        uint64_t hacky_wiggle_room = part_size;
        uint64_t max_data_allowed = accumulated_window_increments + hacky_wiggle_room;
        ASSERT_TRUE(accumulated_data_size <= max_data_allowed, "Received more data than the read window allows");

        /* If we're done, we're done */
        if (done) {
            break;
        }

        /* Figure out how long it's been since we last received data */
        uint64_t current_time;
        ASSERT_SUCCESS(aws_sys_clock_get_ticks(&current_time));

        if (received_body_size_delta != 0) {
            last_time_something_happened = current_time;
        }

        uint64_t duration_since_something_happened = current_time - last_time_something_happened;

        /* If it seems like data has stopped flowing... */
        if (duration_since_something_happened >= wait_duration_with_nothing_happening) {

            /* Assert that data stopped flowing because the window reached 0. */
            uint64_t current_window = aws_sub_u64_saturating(accumulated_window_increments, accumulated_data_size);
            ASSERT_INT_EQUALS(0, current_window, "Data stopped flowing but read window isn't 0 yet.");

            /* Open the window a bit (this resets the "something happened" timer */
            accumulated_window_increments += window_increment_size;
            aws_s3_meta_request_increment_read_window(meta_request, window_increment_size);

            last_time_something_happened = current_time;
        }

        /* Sleep a moment, and loop again... */
        aws_thread_current_sleep(aws_timestamp_convert(100, AWS_TIMESTAMP_MILLIS, AWS_TIMESTAMP_NANOS, NULL));
    }

    return AWS_OP_SUCCESS;
}

static int s_test_s3_get_object_backpressure_helper(
    struct aws_allocator *allocator,
    size_t part_size,
    size_t window_initial_size,
    uint64_t window_increment_size) {

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client_config client_config = {
        .part_size = part_size,
        .enable_read_backpressure = true,
        .initial_read_window = window_initial_size,
    };

    ASSERT_SUCCESS(aws_s3_tester_bind_client(
        &tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION | AWS_S3_TESTER_BIND_CLIENT_SIGNING));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);
    ASSERT_NOT_NULL(client);

    const struct aws_byte_cursor test_object_path = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("/get_object_test_1MB.txt");

    struct aws_string *host_name =
        aws_s3_tester_build_endpoint_string(allocator, &g_test_bucket_name, &g_test_s3_region);

    /* Put together a simple S3 Get Object request. */
    struct aws_http_message *message =
        aws_s3_test_get_object_request_new(allocator, aws_byte_cursor_from_string(host_name), test_object_path);

    struct aws_s3_meta_request_options options = {
        .type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .message = message,
    };

    struct aws_s3_meta_request_test_results meta_request_test_results;
    aws_s3_meta_request_test_results_init(&meta_request_test_results, allocator);

    ASSERT_SUCCESS(aws_s3_tester_bind_meta_request(&tester, &options, &meta_request_test_results));

    struct aws_s3_meta_request *meta_request = aws_s3_client_make_meta_request(client, &options);

    ASSERT_TRUE(meta_request != NULL);

    /* Increment read window bit by bit until all data is downloaded */
    ASSERT_SUCCESS(s_apply_backpressure_until_meta_request_finish(
        &tester, meta_request, &meta_request_test_results, part_size, window_initial_size, window_increment_size));

    aws_s3_tester_lock_synced_data(&tester);

    ASSERT_TRUE(tester.synced_data.finish_error_code == AWS_ERROR_SUCCESS);

    aws_s3_tester_unlock_synced_data(&tester);

    ASSERT_SUCCESS(aws_s3_tester_validate_get_object_results(&meta_request_test_results, 0));

    /* Regression test:
     * Ensure that it's safe to call increment-window even after the meta-request has finished */
    aws_s3_meta_request_increment_read_window(meta_request, 1024);

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

AWS_TEST_CASE(test_s3_get_object_backpressure_small_increments, s_test_s3_get_object_backpressure_small_increments)
static int s_test_s3_get_object_backpressure_small_increments(struct aws_allocator *allocator, void *ctx) {
    /* Test increments smaller than part-size.
     * Only 1 part at a time should be in flight */
    (void)ctx;
    size_t file_size = 1 * 1024 * 1024; /* Test downloads 1MB file */
    size_t part_size = file_size / 4;
    size_t window_initial_size = 1024;
    uint64_t window_increment_size = part_size / 2;
    return s_test_s3_get_object_backpressure_helper(allocator, part_size, window_initial_size, window_increment_size);
}

AWS_TEST_CASE(test_s3_get_object_backpressure_big_increments, s_test_s3_get_object_backpressure_big_increments)
static int s_test_s3_get_object_backpressure_big_increments(struct aws_allocator *allocator, void *ctx) {
    /* Test increments larger than part-size.
     * Multiple parts should be in flight at a time */
    (void)ctx;
    size_t file_size = 1 * 1024 * 1024; /* Test downloads 1MB file */
    size_t part_size = file_size / 8;
    size_t window_initial_size = 1024;
    uint64_t window_increment_size = part_size * 3;
    return s_test_s3_get_object_backpressure_helper(allocator, part_size, window_initial_size, window_increment_size);
}

AWS_TEST_CASE(test_s3_get_object_backpressure_initial_size_zero, s_test_s3_get_object_backpressure_initial_size_zero)
static int s_test_s3_get_object_backpressure_initial_size_zero(struct aws_allocator *allocator, void *ctx) {
    /* Test with initial window size of zero */
    (void)ctx;
    size_t file_size = 1 * 1024 * 1024; /* Test downloads 1MB file */
    size_t part_size = file_size / 4;
    size_t window_initial_size = 0;
    uint64_t window_increment_size = part_size / 2;
    return s_test_s3_get_object_backpressure_helper(allocator, part_size, window_initial_size, window_increment_size);
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
        aws_s3_meta_request_test_results_init(&meta_request_test_results[i], allocator);
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
        aws_input_stream_release(input_streams[i]);
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

AWS_TEST_CASE(test_s3_put_object_sse_c_aes256_multipart, s_test_s3_put_object_sse_c_aes256_multipart)
static int s_test_s3_put_object_sse_c_aes256_multipart(struct aws_allocator *allocator, void *ctx) {
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

    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/prefix/round_trip/test_sse_c.txt");

    struct aws_s3_tester_meta_request_options put_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .client = client,
        .sse_type = AWS_S3_TESTER_SSE_C_AES256,
        .put_options =
            {
                .object_size_mb = 10,
                .object_path_override = object_path,
            },
    };

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &put_options, NULL));
    aws_s3_client_release(client);
    client = NULL;

    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(
    test_s3_put_object_sse_c_aes256_multipart_with_checksum,
    s_test_s3_put_object_sse_c_aes256_multipart_with_checksum)
static int s_test_s3_put_object_sse_c_aes256_multipart_with_checksum(struct aws_allocator *allocator, void *ctx) {
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

    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/prefix/round_trip/test_sse_c.txt");

    struct aws_s3_tester_meta_request_options put_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .client = client,
        .sse_type = AWS_S3_TESTER_SSE_C_AES256,
        .checksum_algorithm = AWS_SCA_CRC32,
        .put_options =
            {
                .object_size_mb = 10,
                .object_path_override = object_path,
            },
    };

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &put_options, NULL));
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
        allocator, base_message, &test_buffer, part_number, upload_id, should_compute_content_md5, NULL, NULL);

    struct aws_http_headers *new_headers = aws_http_message_get_headers(new_message);
    if (should_compute_content_md5) {
        ASSERT_TRUE(aws_http_headers_has(new_headers, g_content_md5_header_name));
        struct aws_byte_cursor content_md5;
        aws_http_headers_get(new_headers, g_content_md5_header_name, &content_md5);
        ASSERT_BIN_ARRAYS_EQUALS(expected_content_md5.ptr, expected_content_md5.len, content_md5.ptr, content_md5.len);
    } else {
        ASSERT_FALSE(aws_http_headers_has(new_headers, g_content_md5_header_name));
    }

    aws_http_message_release(new_message);
    new_message = NULL;

    aws_http_message_release(base_message);
    base_message = NULL;

    aws_string_destroy(upload_id);
    upload_id = NULL;

    aws_input_stream_release(input_stream);
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

    aws_input_stream_release(input_stream);
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

    aws_http_message_release(new_message);
    new_message = NULL;

    aws_http_message_release(base_message);
    base_message = NULL;

    aws_array_list_clean_up(&etags);

    aws_string_destroy(upload_id);
    upload_id = NULL;

    aws_byte_buf_clean_up(&body_buffer);

    aws_input_stream_release(input_stream);
    input_stream = NULL;

    aws_byte_buf_clean_up(&test_buffer);

    return 0;
}

AWS_TEST_CASE(test_s3_put_object_double_slashes, s_test_s3_put_object_double_slashes)
static int s_test_s3_put_object_double_slashes(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_meta_request_test_results meta_request_test_results;
    aws_s3_meta_request_test_results_init(&meta_request_test_results, allocator);

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
    struct aws_s3_meta_request_test_results *meta_request_test_results =
        (struct aws_s3_meta_request_test_results *)user_data;
    AWS_FATAL_ASSERT(result->did_validate);
    AWS_FATAL_ASSERT(result->validation_algorithm == meta_request_test_results->algorithm);
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
        .expected_validate_checksum_alg = AWS_SCA_CRC32,
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
        .validate_get_response_checksum = true,
        .expected_validate_checksum_alg = AWS_SCA_CRC32,
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

/* Test the multipart uploaded object was downloaded with same part size, which will download the object matches all the
 * parts and validate the parts checksum. */
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
        .expected_validate_checksum_alg = AWS_SCA_CRC32,
        .validate_get_response_checksum = true,
        .get_options =
            {
                .object_path = object_path,
            },
        .finish_callback = s_s3_test_validate_checksum,
    };

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &get_options, NULL));

    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(
    test_s3_round_trip_mpu_multipart_get_with_list_algorithm_fc,
    s_test_s3_round_trip_mpu_multipart_get_with_list_algorithm_fc)
static int s_test_s3_round_trip_mpu_multipart_get_with_list_algorithm_fc(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_tester_client_options client_options = {
        .part_size = MB_TO_BYTES(5),
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

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

    struct aws_array_list response_checksum_list;
    /* Check for all algorithm but the CRC32 */
    ASSERT_SUCCESS(
        aws_array_list_init_dynamic(&response_checksum_list, allocator, 4, sizeof(enum aws_s3_checksum_algorithm)));
    enum aws_s3_checksum_algorithm alg = AWS_SCA_CRC32C;
    ASSERT_SUCCESS(aws_array_list_push_back(&response_checksum_list, &alg));
    alg = AWS_SCA_SHA1;
    ASSERT_SUCCESS(aws_array_list_push_back(&response_checksum_list, &alg));
    alg = AWS_SCA_SHA256;
    ASSERT_SUCCESS(aws_array_list_push_back(&response_checksum_list, &alg));

    struct aws_s3_tester_meta_request_options get_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_SUCCESS,
        .client = client,
        .expected_validate_checksum_alg = AWS_SCA_CRC32,
        .validate_get_response_checksum = true,
        .validate_checksum_algorithms = &response_checksum_list,
        .get_options =
            {
                .object_path = object_path,
            },
        .finish_callback = s_s3_test_no_validate_checksum,
    };

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &get_options, NULL));

    /* Push all the algorithms to the list for validation, now we should have the checksum validated. */
    alg = AWS_SCA_CRC32;
    ASSERT_SUCCESS(aws_array_list_push_back(&response_checksum_list, &alg));
    get_options.finish_callback = s_s3_test_validate_checksum;
    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &get_options, NULL));

    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);
    aws_array_list_clean_up(&response_checksum_list);

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
        .expected_validate_checksum_alg = AWS_SCA_CRC32,
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

AWS_TEST_CASE(test_s3_chunked_then_unchunked, s_test_s3_chunked_then_unchunked)
static int s_test_s3_chunked_then_unchunked(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    /* Test to see if signed_body_value modified when signing chunked request */
    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_tester_client_options client_options = {
        .part_size = MB_TO_BYTES(5),
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_byte_cursor chunked_object_path =
        aws_byte_cursor_from_c_str("/prefix/chunked_unchunked/test_chunked.txt");

    struct aws_s3_tester_meta_request_options chunked_put_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .client = client,
        .checksum_algorithm = AWS_SCA_CRC32,
        .validate_get_response_checksum = false,
        .put_options =
            {
                .object_size_mb = 10,
                .object_path_override = chunked_object_path,
            },
    };

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &chunked_put_options, NULL));

    struct aws_byte_cursor unchunked_object_path =
        aws_byte_cursor_from_c_str("/prefix/chunked_unchunked/test_unchunked.txt");

    struct aws_s3_tester_meta_request_options unchunked_put_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .client = client,
        .checksum_algorithm = AWS_SCA_NONE,
        .validate_get_response_checksum = false,
        .put_options =
            {
                .object_size_mb = 10,
                .object_path_override = unchunked_object_path,
            },
    };

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &unchunked_put_options, NULL));

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
    aws_s3_meta_request_test_results_init(&meta_request_test_results, allocator);

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
    aws_s3_meta_request_test_results_init(&meta_request_test_results, allocator);

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
    aws_s3_meta_request_test_results_init(&meta_request_test_results, allocator);

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
    aws_s3_meta_request_test_results_init(&meta_request_test_results, allocator);

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
    aws_s3_meta_request_test_results_init(&meta_request_test_results, allocator);

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
    aws_s3_meta_request_test_results_init(&meta_request_test_results, allocator);

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
    aws_s3_meta_request_test_results_init(&meta_request_test_results, allocator);

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
    aws_s3_meta_request_test_results_init(&meta_request_test_results, allocator);

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
    aws_s3_meta_request_test_results_init(&meta_request_test_results, allocator);
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
    aws_s3_meta_request_test_results_init(&meta_request_test_results, allocator);
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
    aws_s3_meta_request_test_results_init(&meta_request_test_results, allocator);
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
    aws_s3_meta_request_test_results_init(&meta_request_test_results, allocator);
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
    aws_s3_meta_request_test_results_init(&meta_request_test_results, allocator);
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
    aws_s3_meta_request_test_results_init(&meta_request_test_results, allocator);

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
    aws_s3_meta_request_test_results_init(&meta_request_test_results, allocator);

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
    aws_s3_meta_request_test_results_init(&meta_request_test_results, allocator);

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
    aws_s3_meta_request_test_results_init(&meta_request_test_results, allocator);

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
    aws_s3_meta_request_test_results_init(&test_results, allocator);

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
    aws_s3_meta_request_test_results_init(&results, allocator);

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
    struct aws_byte_cursor unencoded_destination_path = aws_byte_cursor_from_c_str(destination_path);
    struct aws_byte_buf copy_destination_path_encoded;
    aws_byte_buf_init(&copy_destination_path_encoded, allocator, 1024);
    aws_byte_buf_append_encoding_uri_path(&copy_destination_path_encoded, &unencoded_destination_path);
    if (aws_http_message_set_request_path(message, aws_byte_cursor_from_buf(&copy_destination_path_encoded))) {
        goto error_clean_up_message;
    }

    struct aws_http_header host_header = {.name = g_host_header_name, .value = endpoint};
    if (aws_http_message_add_header(message, host_header)) {
        goto error_clean_up_message;
    }

    struct aws_byte_buf copy_source_value_encoded;
    aws_byte_buf_init(&copy_source_value_encoded, allocator, 1024);
    aws_byte_buf_append_encoding_uri_path(&copy_source_value_encoded, &x_amz_source);

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
    aws_byte_buf_clean_up(&copy_destination_path_encoded);
    return message;

error_clean_up_message:

    aws_byte_buf_clean_up(&copy_source_value_encoded);
    aws_byte_buf_clean_up(&copy_destination_path_encoded);
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

AWS_TEST_CASE(test_s3_copy_small_object_special_char, s_test_s3_copy_small_object_special_char)
static int s_test_s3_copy_small_object_special_char(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_byte_cursor source_key = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("copies/file_1MB_@.txt");
    struct aws_byte_cursor destination_key = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("copies/destination_1MB_@.txt");

    return s_test_s3_copy_object_helper(
        allocator, source_key, destination_key, AWS_ERROR_SUCCESS, AWS_HTTP_STATUS_CODE_200_OK);
}

AWS_TEST_CASE(test_s3_multipart_copy_large_object_special_char, s_test_s3_multipart_copy_large_object_special_char)
static int s_test_s3_multipart_copy_large_object_special_char(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_byte_cursor source_key = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("copies/file_5GB_@.txt");
    struct aws_byte_cursor destination_key = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("copies/destination_5GB_@.txt");

    return s_test_s3_copy_object_helper(
        allocator, source_key, destination_key, AWS_ERROR_SUCCESS, AWS_HTTP_STATUS_CODE_200_OK);
}

AWS_TEST_CASE(test_s3_multipart_copy_large_object, s_test_s3_multipart_copy_large_object)
static int s_test_s3_multipart_copy_large_object(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_byte_cursor source_key = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("get_object_test_5120MB.txt");
    struct aws_byte_cursor destination_key = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("copies/get_object_test_5120MB.txt");
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

    struct aws_byte_cursor source_key = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("get_object_test_5120MB.txt");
    struct aws_byte_cursor destination_key = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("copies/get_object_test_5120MB.txt");

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

static int s_s3_get_object_mrap_helper(struct aws_allocator *allocator, bool multipart) {

    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_signing_config_aws signing_config = tester.default_signing_config;
    /* Use Sigv4A for signing */
    signing_config.algorithm = AWS_SIGNING_ALGORITHM_V4_ASYMMETRIC;
    /* Use * for region to sign */
    signing_config.region = aws_byte_cursor_from_c_str("*");

    struct aws_s3_client_config client_config = {
        .part_size = multipart ? 64 * 1024 : 20 * 1024 * 1024,
        .region = aws_byte_cursor_from_c_str("*"),
        .signing_config = &signing_config,
    };

    ASSERT_SUCCESS(aws_s3_tester_bind_client(&tester, &client_config, 0 /*flag*/));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);
    struct aws_s3_meta_request_test_results meta_request_test_results;
    aws_s3_meta_request_test_results_init(&meta_request_test_results, allocator);
    struct aws_s3_tester_meta_request_options options = {
        .allocator = allocator,
        .client = client,
        .mrap_test = true,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_SUCCESS,
        .get_options =
            {
                .object_path = g_s3_path_get_object_test_1MB,
            },
    };

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &options, &meta_request_test_results));
    aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);

    aws_s3_client_release(client);
    client = NULL;

    aws_s3_tester_clean_up(&tester);

    return 0;
}

/* Test single-part get object through MRAP (multi-region access point) */
AWS_TEST_CASE(test_s3_get_object_less_than_part_size_mrap, s_test_s3_get_object_less_than_part_size_mrap)
static int s_test_s3_get_object_less_than_part_size_mrap(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    return s_s3_get_object_mrap_helper(allocator, false /*multipart*/);
}

/* Test multi-part get object through MRAP (multi-region access point) */
AWS_TEST_CASE(test_s3_get_object_multipart_mrap, s_test_s3_get_object_multipart_mrap)
static int s_test_s3_get_object_multipart_mrap(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    return s_s3_get_object_mrap_helper(allocator, true /*multipart*/);
}

static int s_s3_put_object_mrap_helper(struct aws_allocator *allocator, bool multipart) {
    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_signing_config_aws signing_config = tester.default_signing_config;
    /* Use Sigv4A for signing */
    signing_config.algorithm = AWS_SIGNING_ALGORITHM_V4_ASYMMETRIC;
    /* Use * for region to sign */
    signing_config.region = aws_byte_cursor_from_c_str("*");

    struct aws_s3_client_config client_config = {
        .part_size = 5 * 1024 * 1024,
        .region = aws_byte_cursor_from_c_str("*"),
        .signing_config = &signing_config,
    };

    ASSERT_SUCCESS(aws_s3_tester_bind_client(&tester, &client_config, 0 /*flag*/));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    ASSERT_TRUE(client != NULL);

    struct aws_s3_meta_request_test_results meta_request_test_results;
    aws_s3_meta_request_test_results_init(&meta_request_test_results, allocator);

    struct aws_s3_tester_meta_request_options options = {
        .allocator = allocator,
        .client = client,
        .mrap_test = true,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_SUCCESS,
        .put_options =
            {
                .object_size_mb = multipart ? 10 : 1,
            },
    };

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &options, &meta_request_test_results));
    aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);

    aws_s3_client_release(client);
    client = NULL;

    aws_s3_tester_clean_up(&tester);

    return 0;
}

/* Test single-part put object through MRAP (multi-region access point) */
AWS_TEST_CASE(test_s3_put_object_less_than_part_size_mrap, s_test_s3_put_object_less_than_part_size_mrap)
static int s_test_s3_put_object_less_than_part_size_mrap(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    return s_s3_put_object_mrap_helper(allocator, false /*multipart*/);
}
/* Test multi-part put object through MRAP (multi-region access point) */
AWS_TEST_CASE(test_s3_put_object_multipart_mrap, s_test_s3_put_object_multipart_mrap)
static int s_test_s3_put_object_multipart_mrap(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    return s_s3_put_object_mrap_helper(allocator, true /*multipart*/);
}

struct aws_http_message *put_object_request_new(
    struct aws_allocator *allocator,
    struct aws_byte_cursor key,
    struct aws_byte_cursor endpoint,
    struct aws_input_stream *body_stream) {

    AWS_PRECONDITION(allocator);

    struct aws_http_message *message = aws_http_message_new_request(allocator);

    if (message == NULL) {
        return NULL;
    }

    if (aws_http_message_set_request_path(message, key)) {
        goto error_clean_up_message;
    }

    struct aws_http_header host_header = {
        .name = g_host_header_name,
        .value = endpoint,
    };
    if (aws_http_message_add_header(message, host_header)) {
        goto error_clean_up_message;
    }

    char content_length_c_str[1024];
    int64_t content_length = 0;
    aws_input_stream_get_length(body_stream, &content_length);
    snprintf(content_length_c_str, sizeof(content_length_c_str), "%" PRIu64, content_length);

    struct aws_http_header content_length_header = {
        .name = g_content_length_header_name,
        .value = aws_byte_cursor_from_c_str(content_length_c_str),
    };

    if (aws_http_message_add_header(message, content_length_header)) {
        goto error_clean_up_message;
    }

    if (aws_http_message_set_request_method(message, aws_http_method_put)) {
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

struct put_object_pause_resume_test_data {
    struct aws_mutex mutex;
    struct aws_condition_variable c_var;

    /* execution of the test meta request completed */
    bool execution_completed;

    /* accumulator of amount of bytes uploaded */
    struct aws_atomic_var total_bytes_uploaded;

    /* the offset where upload should be paused */
    struct aws_atomic_var request_pause_offset;

    struct aws_atomic_var pause_requested;

    struct aws_atomic_var pause_result;

    /* the persistable state of the paused request */
    struct aws_atomic_var persistable_state_ptr;

    int meta_request_error_code;
    int response_status_code;
};

static void s_put_pause_resume_meta_request_finish(
    struct aws_s3_meta_request *meta_request,
    const struct aws_s3_meta_request_result *meta_request_result,
    void *user_data) {

    (void)meta_request;

    struct put_object_pause_resume_test_data *test_data = user_data;

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

static bool s_put_pause_resume_test_completion_predicate(void *arg) {
    struct put_object_pause_resume_test_data *test_data = arg;
    return test_data->execution_completed;
}

static void s_pause_meta_request_progress(
    struct aws_s3_meta_request *meta_request,
    const struct aws_s3_meta_request_progress *progress,
    void *user_data) {

    AWS_ASSERT(meta_request);
    AWS_ASSERT(progress);
    AWS_ASSERT(user_data);

    struct put_object_pause_resume_test_data *test_data = user_data;

    aws_atomic_fetch_add(&test_data->total_bytes_uploaded, (size_t)progress->bytes_transferred);

    size_t total_bytes_uploaded = aws_atomic_load_int(&test_data->total_bytes_uploaded);
    size_t offset_to_pause = aws_atomic_load_int(&test_data->request_pause_offset);

    if (total_bytes_uploaded >= offset_to_pause) {
        /* offset of the upload at which we should pause was reached. let's pause the upload */

        size_t expected = false;
        bool request_pause = aws_atomic_compare_exchange_int(&test_data->pause_requested, &expected, true);
        if (!request_pause) {
            /* if the meta request has already been paused previously, do nothing. */
            return;
        }

        struct aws_string *persistable_state = NULL;
        int pause_result = aws_s3_meta_request_pause(meta_request, &persistable_state);
        aws_atomic_store_int(&test_data->pause_result, pause_result);
        aws_atomic_store_ptr(&test_data->persistable_state_ptr, persistable_state);
    }
}

/* total length of the object to simulate for upload */
static const size_t s_pause_resume_object_length_128MB = 128 * 1024 * 1024;

static int s_pause_resume_receive_body_callback(
    struct aws_s3_meta_request *meta_request,
    const struct aws_byte_cursor *body,
    uint64_t range_start,
    void *user_data) {

    (void)meta_request;
    (void)range_start;
    (void)user_data;

    // TODO: this is a bit hacky, as it will try to compare every partial get result we receive to the input stream.
    // Something better?

    struct aws_input_stream *input_stream =
        aws_s3_test_input_stream_new(meta_request->allocator, s_pause_resume_object_length_128MB);

    struct aws_byte_buf buf;
    aws_byte_buf_init(&buf, meta_request->allocator, (size_t)range_start);
    aws_input_stream_read(input_stream, &buf);

    aws_byte_buf_clean_up(&buf);
    aws_byte_buf_init(&buf, meta_request->allocator, body->len);
    aws_input_stream_read(input_stream, &buf);

    struct aws_byte_cursor input_cur = aws_byte_cursor_from_buf(&buf);

    bool body_matches_expected = aws_byte_cursor_eq(&input_cur, body);

    aws_input_stream_destroy(input_stream);
    aws_byte_buf_clean_up(&buf);

    ASSERT_TRUE(body_matches_expected);
    return AWS_OP_SUCCESS;
}

static int s_test_s3_put_pause_resume_helper(
    struct aws_s3_tester *tester,
    struct aws_allocator *allocator,
    void *ctx,
    struct put_object_pause_resume_test_data *test_data,
    struct aws_byte_cursor destination_key,
    struct aws_input_stream *upload_body_stream,
    struct aws_string *resume_state,
    enum aws_s3_checksum_algorithm checksum_algorithm,
    int expected_error_code,
    int expected_response_status) {

    (void)ctx;

    struct aws_s3_client_config client_config;
    AWS_ZERO_STRUCT(client_config);

    ASSERT_SUCCESS(aws_s3_tester_bind_client(
        tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION | AWS_S3_TESTER_BIND_CLIENT_SIGNING));

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

    /* creates a PutObject request */
    struct aws_http_message *message =
        put_object_request_new(allocator, destination_key, aws_byte_cursor_from_c_str(endpoint), upload_body_stream);

    test_data->c_var = (struct aws_condition_variable)AWS_CONDITION_VARIABLE_INIT;
    aws_mutex_init(&test_data->mutex);
    test_data->execution_completed = false;

    struct aws_s3_checksum_config checksum_config = {
        .checksum_algorithm = checksum_algorithm,
        .location = checksum_algorithm == AWS_SCA_NONE ? AWS_SCL_NONE : AWS_SCL_TRAILER,
    };

    struct aws_s3_meta_request_options meta_request_options = {
        .user_data = test_data,
        .body_callback = NULL,
        .signing_config = client_config.signing_config,
        .finish_callback = s_put_pause_resume_meta_request_finish,
        .headers_callback = NULL,
        .progress_callback = s_pause_meta_request_progress,
        .message = message,
        .shutdown_callback = NULL,
        .resume_token = NULL,
        .type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .checksum_config = &checksum_config,
    };

    struct aws_byte_cursor resume_state_cur;
    if (resume_state) {
        resume_state_cur = aws_byte_cursor_from_string(resume_state);
        meta_request_options.resume_token = &resume_state_cur;
    }

    struct aws_s3_meta_request *meta_request = aws_s3_client_make_meta_request(client, &meta_request_options);
    ASSERT_NOT_NULL(meta_request);

    /* wait completion of the meta request */
    aws_mutex_lock(&test_data->mutex);
    aws_condition_variable_wait_pred(
        &test_data->c_var, &test_data->mutex, s_put_pause_resume_test_completion_predicate, test_data);
    aws_mutex_unlock(&test_data->mutex);

    /* assert error_code and response_status_code */
    ASSERT_INT_EQUALS(expected_error_code, test_data->meta_request_error_code);
    ASSERT_INT_EQUALS(expected_response_status, test_data->response_status_code);

    aws_s3_meta_request_release(meta_request);
    aws_mutex_clean_up(&test_data->mutex);
    aws_http_message_destroy(message);

    if (expected_error_code == AWS_ERROR_SUCCESS) {
        /* get the file and verify it matches what we uploaded */
        struct aws_s3_tester_meta_request_options options = {
            .allocator = allocator,
            .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
            .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_SUCCESS,
            .body_callback = s_pause_resume_receive_body_callback,
            .client = client,
            .get_options =
                {
                    .object_path = destination_key,
                },
        };

        struct aws_s3_meta_request_test_results results;
        aws_s3_meta_request_test_results_init(&results, allocator);

        ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(tester, &options, &results));
        aws_s3_meta_request_test_results_clean_up(&results);
    }

    aws_s3_client_release(client);
    client = NULL;

    return 0;
}

AWS_TEST_CASE(test_s3_put_pause_resume, s_test_s3_put_pause_resume)
static int s_test_s3_put_pause_resume(struct aws_allocator *allocator, void *ctx) {
    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_byte_cursor destination_key = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("/upload/test_pause_resume.txt");

    struct put_object_pause_resume_test_data test_data;
    AWS_ZERO_STRUCT(test_data);

    /* initialize the atomic members */
    aws_atomic_init_int(&test_data.total_bytes_uploaded, 0);
    aws_atomic_init_int(&test_data.request_pause_offset, 0);
    aws_atomic_init_int(&test_data.pause_requested, false);
    aws_atomic_init_int(&test_data.pause_result, 0);
    aws_atomic_init_ptr(&test_data.persistable_state_ptr, NULL);

    /* offset of the upload where pause should be requested by test client */
    aws_atomic_store_int(&test_data.request_pause_offset, 8 * 1024 * 1024);

    /* stream used to initiate upload */
    struct aws_input_stream *initial_upload_stream =
        aws_s3_test_input_stream_new(allocator, s_pause_resume_object_length_128MB);

    /* starts the upload request that will be paused by s_s3_put_pause_resume_stream_on_read() */
    ASSERT_SUCCESS(s_test_s3_put_pause_resume_helper(
        &tester,
        allocator,
        ctx,
        &test_data,
        destination_key,
        initial_upload_stream,
        NULL,
        AWS_SCA_NONE,
        AWS_ERROR_S3_PAUSED,
        0));
    if (tester.bound_to_client) {
        aws_s3_tester_wait_for_client_shutdown(&tester);
        tester.bound_to_client = false;
    }

    aws_input_stream_destroy(initial_upload_stream);

    /* new stream used to resume upload. it begins at the offset specified in the persistable state */
    struct aws_input_stream *resume_upload_stream =
        aws_s3_test_input_stream_new(allocator, s_pause_resume_object_length_128MB);
    struct aws_string *persistable_state = aws_atomic_load_ptr(&test_data.persistable_state_ptr);

    size_t bytes_uploaded = aws_atomic_load_int(&test_data.total_bytes_uploaded);

    /* offset where pause should be requested is set to a value greater than content length,
     * to avoid any more pause when resuming the upload */
    aws_atomic_store_int(&test_data.request_pause_offset, s_pause_resume_object_length_128MB * 2);
    aws_atomic_store_int(&test_data.total_bytes_uploaded, 0);

    ASSERT_SUCCESS(s_test_s3_put_pause_resume_helper(
        &tester,
        allocator,
        ctx,
        &test_data,
        destination_key,
        resume_upload_stream,
        persistable_state,
        AWS_SCA_NONE,
        AWS_ERROR_SUCCESS,
        AWS_HTTP_STATUS_CODE_200_OK));

    bytes_uploaded = aws_atomic_load_int(&test_data.total_bytes_uploaded);

    /* bytes uploaded is smaller since we are skipping uploaded parts */
    ASSERT_TRUE(bytes_uploaded < s_pause_resume_object_length_128MB);

    aws_string_destroy(persistable_state);
    aws_input_stream_destroy(resume_upload_stream);
    aws_s3_tester_clean_up(&tester);

    return AWS_ERROR_SUCCESS;
}

AWS_TEST_CASE(test_s3_put_pause_resume_all_parts_done, s_test_s3_put_pause_resume_all_parts_done)
static int s_test_s3_put_pause_resume_all_parts_done(struct aws_allocator *allocator, void *ctx) {
    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_byte_cursor destination_key =
        AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("/upload/test_pause_resume_all_parts_done.txt");

    struct put_object_pause_resume_test_data test_data;
    AWS_ZERO_STRUCT(test_data);

    /* initialize the atomic members */
    aws_atomic_init_int(&test_data.total_bytes_uploaded, 0);
    aws_atomic_init_int(&test_data.request_pause_offset, 0);
    aws_atomic_init_int(&test_data.pause_requested, false);
    aws_atomic_init_int(&test_data.pause_result, 0);
    aws_atomic_init_ptr(&test_data.persistable_state_ptr, NULL);

    /* offset of the upload where pause should be requested by test client */
    aws_atomic_store_int(&test_data.request_pause_offset, 128 * 1024 * 1024);

    /* stream used to initiate upload */
    struct aws_input_stream *initial_upload_stream =
        aws_s3_test_input_stream_new(allocator, s_pause_resume_object_length_128MB);

    /* starts the upload request that will be paused by s_s3_put_pause_resume_stream_on_read() */
    ASSERT_SUCCESS(s_test_s3_put_pause_resume_helper(
        &tester,
        allocator,
        ctx,
        &test_data,
        destination_key,
        initial_upload_stream,
        NULL,
        AWS_SCA_NONE,
        AWS_ERROR_S3_PAUSED,
        0));
    if (tester.bound_to_client) {
        aws_s3_tester_wait_for_client_shutdown(&tester);
        tester.bound_to_client = false;
    }

    aws_input_stream_destroy(initial_upload_stream);

    /* new stream used to resume upload. it begins at the offset specified in the persistable state */
    struct aws_input_stream *resume_upload_stream =
        aws_s3_test_input_stream_new(allocator, s_pause_resume_object_length_128MB);
    struct aws_string *persistable_state = aws_atomic_load_ptr(&test_data.persistable_state_ptr);

    AWS_LOGF_INFO(AWS_LS_S3_GENERAL, "Persistable state %p", persistable_state);

    size_t bytes_uploaded = aws_atomic_load_int(&test_data.total_bytes_uploaded);

    /* offset where pause should be requested is set to a value greater than content length,
     * to avoid any more pause when resuming the upload */
    aws_atomic_store_int(&test_data.request_pause_offset, s_pause_resume_object_length_128MB * 2);
    aws_atomic_store_int(&test_data.total_bytes_uploaded, 0);

    ASSERT_SUCCESS(s_test_s3_put_pause_resume_helper(
        &tester,
        allocator,
        ctx,
        &test_data,
        destination_key,
        resume_upload_stream,
        persistable_state,
        AWS_SCA_NONE,
        AWS_ERROR_SUCCESS,
        AWS_HTTP_STATUS_CODE_200_OK));

    bytes_uploaded = aws_atomic_load_int(&test_data.total_bytes_uploaded);

    /* bytes uploaded is smaller since we are skipping uploaded parts */
    ASSERT_INT_EQUALS(bytes_uploaded, 0);

    aws_string_destroy(persistable_state);
    aws_input_stream_destroy(resume_upload_stream);
    aws_s3_tester_clean_up(&tester);

    return AWS_ERROR_SUCCESS;
}

AWS_TEST_CASE(test_s3_put_pause_resume_invalid_checksum, s_test_s3_put_pause_resume_invalid_checksum)
static int s_test_s3_put_pause_resume_invalid_checksum(struct aws_allocator *allocator, void *ctx) {
    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_byte_cursor destination_key =
        AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("/upload/test_pause_resume_checksum.txt");

    struct put_object_pause_resume_test_data test_data;
    AWS_ZERO_STRUCT(test_data);

    /* initialize the atomic members */
    aws_atomic_init_int(&test_data.total_bytes_uploaded, 0);
    aws_atomic_init_int(&test_data.request_pause_offset, 0);
    aws_atomic_init_int(&test_data.pause_requested, false);
    aws_atomic_init_int(&test_data.pause_result, 0);
    aws_atomic_init_ptr(&test_data.persistable_state_ptr, NULL);

    /* offset of the upload where pause should be requested by test client */
    aws_atomic_store_int(&test_data.request_pause_offset, 8 * 1024 * 1024);

    /* stream used to initiate upload */
    struct aws_input_stream *initial_upload_stream =
        aws_s3_test_input_stream_new(allocator, s_pause_resume_object_length_128MB);

    /* starts the upload request that will be paused by s_s3_put_pause_resume_stream_on_read() */
    ASSERT_SUCCESS(s_test_s3_put_pause_resume_helper(
        &tester,
        allocator,
        ctx,
        &test_data,
        destination_key,
        initial_upload_stream,
        NULL,
        AWS_SCA_CRC32,
        AWS_ERROR_S3_PAUSED,
        0));
    if (tester.bound_to_client) {
        aws_s3_tester_wait_for_client_shutdown(&tester);
        tester.bound_to_client = false;
    }

    aws_input_stream_destroy(initial_upload_stream);

    /* new stream used to resume upload. it begins at the offset specified in the persistable state */
    struct aws_input_stream *resume_upload_stream = aws_s3_test_input_stream_new_with_value_type(
        allocator, s_pause_resume_object_length_128MB, TEST_STREAM_VALUE_2);

    struct aws_string *persistable_state = aws_atomic_load_ptr(&test_data.persistable_state_ptr);

    size_t bytes_uploaded = aws_atomic_load_int(&test_data.total_bytes_uploaded);

    /* offset where pause should be requested is set to a value greater than content length,
     * to avoid any more pause when resuming the upload */
    aws_atomic_store_int(&test_data.request_pause_offset, s_pause_resume_object_length_128MB * 2);
    aws_atomic_store_int(&test_data.total_bytes_uploaded, 0);

    ASSERT_SUCCESS(s_test_s3_put_pause_resume_helper(
        &tester,
        allocator,
        ctx,
        &test_data,
        destination_key,
        resume_upload_stream,
        persistable_state,
        AWS_SCA_CRC32,
        AWS_ERROR_S3_RESUMED_PART_CHECKSUM_MISMATCH,
        0));

    bytes_uploaded = aws_atomic_load_int(&test_data.total_bytes_uploaded);

    /* bytes uploaded is smaller since we are skipping uploaded parts */
    ASSERT_TRUE(bytes_uploaded < s_pause_resume_object_length_128MB);

    aws_string_destroy(persistable_state);
    aws_input_stream_destroy(resume_upload_stream);
    aws_s3_tester_clean_up(&tester);

    return AWS_ERROR_SUCCESS;
}
