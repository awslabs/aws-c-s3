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
#include <aws/io/host_resolver.h>
#include <aws/io/stream.h>
#include <aws/io/tls_channel_handler.h>
#include <aws/testing/aws_test_harness.h>
#include <inttypes.h>

AWS_TEST_CASE(test_s3_client_set_vip_connection_warm, s_test_s3_client_set_vip_connection_warm)
static int s_test_s3_client_set_vip_connection_warm(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    aws_s3_tester_init(allocator, &tester);

    struct aws_s3_client *mock_client = aws_s3_tester_mock_client_new(&tester);

    struct aws_s3_vip *mock_vip = aws_s3_tester_mock_vip_new(&tester);
    aws_atomic_init_int(&mock_vip->active, 1);
    mock_vip->owning_client = mock_client;

    struct aws_s3_vip_connection *mock_vip_connection = aws_s3_tester_mock_vip_connection_new(&tester);
    mock_vip_connection->owning_vip = mock_vip;

    ASSERT_FALSE(mock_vip_connection->is_warm);
    ASSERT_TRUE(aws_atomic_load_int(&mock_client->stats.num_warm_vip_connections) == 0);

    aws_s3_client_set_vip_connection_warm(mock_client, mock_vip_connection, true);

    /* Connection should now be warm, and the total should increase by one. */
    ASSERT_TRUE(mock_vip_connection->is_warm);
    ASSERT_TRUE(aws_atomic_load_int(&mock_client->stats.num_warm_vip_connections) == 1);

    aws_s3_client_set_vip_connection_warm(mock_client, mock_vip_connection, true);

    /* Connection should still be warm, and the total should stay at one since connection was already warm. */
    ASSERT_TRUE(mock_vip_connection->is_warm);
    ASSERT_TRUE(aws_atomic_load_int(&mock_client->stats.num_warm_vip_connections) == 1);

    aws_s3_client_set_vip_connection_warm(mock_client, mock_vip_connection, false);

    /* Connection should no longer be warm, and the total should now be zero. */
    ASSERT_FALSE(mock_vip_connection->is_warm);
    ASSERT_TRUE(aws_atomic_load_int(&mock_client->stats.num_warm_vip_connections) == 0);

    aws_s3_client_set_vip_connection_warm(mock_client, mock_vip_connection, false);

    /* Connection should still no longer be warm, and the total should stay at zero since connection was already not
     * warm. */
    ASSERT_FALSE(mock_vip_connection->is_warm);
    ASSERT_TRUE(aws_atomic_load_int(&mock_client->stats.num_warm_vip_connections) == 0);

    aws_s3_tester_mock_vip_connection_destroy(&tester, mock_vip_connection);
    aws_s3_tester_mock_vip_destroy(&tester, mock_vip);
    aws_s3_client_release(mock_client);
    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(test_s3_client_set_vip_connection_active, s_test_s3_client_set_vip_connection_active)
static int s_test_s3_client_set_vip_connection_active(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    aws_s3_tester_init(allocator, &tester);

    struct aws_s3_client *mock_client = aws_s3_tester_mock_client_new(&tester);

    struct aws_s3_vip *mock_vip = aws_s3_tester_mock_vip_new(&tester);
    aws_atomic_init_int(&mock_vip->active, 1);
    mock_vip->owning_client = mock_client;

    struct aws_s3_vip_connection *mock_vip_connection = aws_s3_tester_mock_vip_connection_new(&tester);
    mock_vip_connection->owning_vip = mock_vip;

    ASSERT_FALSE(mock_vip_connection->is_active);
    ASSERT_TRUE(aws_atomic_load_int(&mock_client->stats.num_active_vip_connections) == 0);

    aws_s3_client_set_vip_connection_active(mock_client, mock_vip_connection, true);

    /* Connection should now be active, and the total should increase by one. */
    ASSERT_TRUE(mock_vip_connection->is_active);
    ASSERT_TRUE(aws_atomic_load_int(&mock_client->stats.num_active_vip_connections) == 1);

    aws_s3_client_set_vip_connection_active(mock_client, mock_vip_connection, true);

    /* Connection should still be active, and the total should stay at one since connection was already active. */
    ASSERT_TRUE(mock_vip_connection->is_active);
    ASSERT_TRUE(aws_atomic_load_int(&mock_client->stats.num_active_vip_connections) == 1);

    aws_s3_client_set_vip_connection_active(mock_client, mock_vip_connection, false);

    /* Connection should no longer be active, and the total should now be zero. */
    ASSERT_FALSE(mock_vip_connection->is_active);
    ASSERT_TRUE(aws_atomic_load_int(&mock_client->stats.num_active_vip_connections) == 0);

    aws_s3_client_set_vip_connection_active(mock_client, mock_vip_connection, false);

    /* Connection should still no longer be active, and the total should stay at zero since connection was already not
     * active. */
    ASSERT_FALSE(mock_vip_connection->is_active);
    ASSERT_TRUE(aws_atomic_load_int(&mock_client->stats.num_active_vip_connections) == 0);

    aws_s3_tester_mock_vip_connection_destroy(&tester, mock_vip_connection);
    aws_s3_tester_mock_vip_destroy(&tester, mock_vip);
    aws_s3_client_release(mock_client);
    aws_s3_tester_clean_up(&tester);

    return 0;
}

static void s_test_s3_vip_create_destroy_vip_shutdown_callback(void *user_data) {
    AWS_ASSERT(user_data);
    struct aws_s3_tester *tester = user_data;
    aws_s3_tester_inc_counter1(tester);
}

AWS_TEST_CASE(test_s3_vip_create_destroy, s_test_s3_vip_create_destroy)
static int s_test_s3_vip_create_destroy(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client_config client_config;
    AWS_ZERO_STRUCT(client_config);

    ASSERT_SUCCESS(aws_s3_tester_bind_client(
        &tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION | AWS_S3_TESTER_BIND_CLIENT_SIGNING));

    aws_s3_tester_set_counter1_desired(&tester, 1);

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    struct aws_linked_list vip_connections;
    aws_linked_list_init(&vip_connections);

    const uint32_t num_vip_connections_requested = 2;
    const struct aws_byte_cursor dummy_host_address = aws_byte_cursor_from_c_str("dummy_host_address");

    struct aws_s3_vip *vip = aws_s3_vip_new(
        client,
        &dummy_host_address,
        &dummy_host_address,
        num_vip_connections_requested,
        &vip_connections,
        s_test_s3_vip_create_destroy_vip_shutdown_callback,
        &tester);

    ASSERT_TRUE(vip != NULL);
    ASSERT_TRUE(vip->synced_data.num_vip_connections == num_vip_connections_requested);

    uint32_t num_vip_connections_returned = 0;

    /* Loop through all of the VIP connections, doing some validation and destroying them. Note: because we are not
     * actually adding this VIP to the client, it is necessary to destroy the connections here in order for the VIP
     * destruction to finish correctly. */
    while (!aws_linked_list_empty(&vip_connections)) {
        struct aws_linked_list_node *node = aws_linked_list_pop_front(&vip_connections);
        struct aws_s3_vip_connection *vip_connection = AWS_CONTAINER_OF(node, struct aws_s3_vip_connection, node);

        ASSERT_TRUE(vip_connection->owning_vip == vip);

        aws_s3_vip_connection_destroy(client, vip_connection);
        ++num_vip_connections_returned;
    }

    ASSERT_TRUE(num_vip_connections_requested == num_vip_connections_returned);

    aws_s3_vip_start_destroy(vip);
    vip = NULL;

    aws_s3_tester_wait_for_counters(&tester);

    aws_s3_client_release(client);
    client = NULL;

    aws_s3_tester_clean_up(&tester);

    return 0;
}

static void s_clean_up_host_address_array_list(struct aws_array_list *host_address_array_list) {
    AWS_ASSERT(host_address_array_list);

    for (uint32_t i = 0; i < aws_array_list_length(host_address_array_list); ++i) {
        struct aws_host_address *host_address = NULL;
        aws_array_list_get_at_ptr(host_address_array_list, (void **)&host_address, i);
        aws_host_address_clean_up(host_address);
    }

    aws_array_list_clean_up(host_address_array_list);
}

static void s_schedule_process_work_synced_do_nothing(struct aws_s3_client *client) {
    (void)client;
}

AWS_TEST_CASE(test_s3_client_add_remove_vips, s_test_s3_client_add_remove_vips)
static int s_test_s3_client_add_remove_vips(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client_config client_config;
    AWS_ZERO_STRUCT(client_config);
    client_config.throughput_target_gbps = 100;

    ASSERT_SUCCESS(aws_s3_tester_bind_client(
        &tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION | AWS_S3_TESTER_BIND_CLIENT_SIGNING));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    struct aws_s3_client_vtable *patched_client_vtable = aws_s3_tester_patch_client_vtable(&tester, client, NULL);
    patched_client_vtable->schedule_process_work_synced = s_schedule_process_work_synced_do_nothing;

    aws_s3_client_lock_synced_data(client);
    client->synced_data.endpoint = aws_string_new_from_c_str(allocator, "dummy_host");
    aws_s3_client_unlock_synced_data(client);

    struct aws_array_list host_addresses;
    ASSERT_SUCCESS(aws_array_list_init_dynamic(&host_addresses, allocator, 4, sizeof(struct aws_host_address)));

    /* Create a list of addresses that has one more address over the limit. */
    for (uint32_t i = 0; i < client->ideal_vip_count + 1; ++i) {

        char address_buffer[128] = "";
        snprintf(address_buffer, sizeof(address_buffer), "dummy_address_%d", i);
        struct aws_string *address = aws_string_new_from_c_str(allocator, address_buffer);

        struct aws_host_address host_address = {
            .allocator = allocator,
            .host = aws_string_new_from_c_str(allocator, "dummy_host"),
            .address = address,
            .record_type = AWS_ADDRESS_RECORD_TYPE_A,
        };

        ASSERT_SUCCESS(aws_array_list_push_back(&host_addresses, &host_address));
    }

    /* Force the client into an inactive state to verify that trying to add vips while the client is not active results
     * in no vips being added. */
    {
        aws_s3_client_lock_synced_data(client);
        ASSERT_TRUE(client->synced_data.active);
        client->synced_data.active = false;
        aws_s3_client_unlock_synced_data(client);

        aws_s3_client_add_vips(client, &host_addresses);

        aws_s3_client_lock_synced_data(client);
        ASSERT_TRUE(client->synced_data.active_vip_count == 0);
        aws_s3_client_unlock_synced_data(client);

        aws_s3_client_lock_synced_data(client);
        client->synced_data.active = true;
        aws_s3_client_unlock_synced_data(client);
    }

    /* Add the vips.  It should not go over the ideal vip count, even though we have added one extra to the list. */
    {
        aws_s3_client_add_vips(client, &host_addresses);

        aws_s3_client_lock_synced_data(client);
        ASSERT_TRUE(client->synced_data.active_vip_count == client->ideal_vip_count);
        aws_s3_client_unlock_synced_data(client);
    }

    /* Try to double add the same vips.  Number of vips should stay the same. */
    {
        aws_s3_client_add_vips(client, &host_addresses);

        aws_s3_client_lock_synced_data(client);
        ASSERT_TRUE(client->synced_data.active_vip_count == client->ideal_vip_count);
        aws_s3_client_unlock_synced_data(client);
    }

    /* Try to remove a vip that doesn't exist. */
    {
        struct aws_host_address not_added_host_address = {
            .allocator = allocator,
            .host = aws_string_new_from_c_str(allocator, "dummy_host_not_added"),
            .address = aws_string_new_from_c_str(allocator, "dummy_address_not_added"),
            .record_type = AWS_ADDRESS_RECORD_TYPE_A,
        };

        struct aws_array_list not_added_host_addresses;
        ASSERT_SUCCESS(
            aws_array_list_init_dynamic(&not_added_host_addresses, allocator, 1, sizeof(struct aws_host_address)));
        ASSERT_SUCCESS(aws_array_list_push_back(&not_added_host_addresses, &not_added_host_address));

        aws_s3_client_remove_vips(client, &not_added_host_addresses);

        aws_s3_client_lock_synced_data(client);
        ASSERT_TRUE(client->synced_data.active_vip_count == client->ideal_vip_count);
        aws_s3_client_unlock_synced_data(client);

        s_clean_up_host_address_array_list(&not_added_host_addresses);
    }

    /* Remove a single vip. */
    {
        struct aws_array_list host_addresses_to_remove;
        ASSERT_SUCCESS(
            aws_array_list_init_dynamic(&host_addresses_to_remove, allocator, 1, sizeof(struct aws_host_address)));

        struct aws_host_address *first_host_address = NULL;
        aws_array_list_get_at_ptr(&host_addresses, (void **)&first_host_address, 0);

        struct aws_host_address first_host_address_copy;
        AWS_ZERO_STRUCT(first_host_address_copy);
        aws_host_address_copy(first_host_address, &first_host_address_copy);

        aws_array_list_push_back(&host_addresses_to_remove, &first_host_address_copy);

        aws_s3_client_remove_vips(client, &host_addresses_to_remove);

        aws_s3_client_lock_synced_data(client);
        ASSERT_TRUE(client->synced_data.active_vip_count == client->ideal_vip_count - 1);
        aws_s3_client_unlock_synced_data(client);

        s_clean_up_host_address_array_list(&host_addresses_to_remove);
    }

    /* Make sure that we don't endlessly add vips until enough have cleaned up.*/
    {
        /* Block the work task from running so that no vips will clean up until we unblock it. */

        struct aws_array_list host_addresses_to_remove_add;
        ASSERT_SUCCESS(
            aws_array_list_init_dynamic(&host_addresses_to_remove_add, allocator, 1, sizeof(struct aws_host_address)));

        struct aws_host_address *first_host_address = NULL;
        aws_array_list_get_at_ptr(&host_addresses, (void **)&first_host_address, 0);

        struct aws_host_address first_host_address_copy;
        AWS_ZERO_STRUCT(first_host_address_copy);
        aws_host_address_copy(first_host_address, &first_host_address_copy);

        aws_array_list_push_back(&host_addresses_to_remove_add, &first_host_address_copy);

        /* Continually add and remove a vip.  Without the processing of work, the active vip count will stay the same
         * after each add-remove, but the allocated count will not go down because it needs the work task to clean
         * up vips. */
        for (uint32_t i = 0; i < (aws_s3_client_get_max_allocated_vip_count(client) + 1); ++i) {
            aws_s3_client_add_vips(client, &host_addresses_to_remove_add);

            aws_s3_client_remove_vips(client, &host_addresses_to_remove_add);
        }

        /* Make sure the allocated count is less than or equal to the limit.*/
        aws_s3_client_lock_synced_data(client);
        ASSERT_TRUE(client->synced_data.allocated_vip_count <= aws_s3_client_get_max_allocated_vip_count(client));
        aws_s3_client_unlock_synced_data(client);

        s_clean_up_host_address_array_list(&host_addresses_to_remove_add);
    }

    {
        struct aws_s3_client_vtable *original_client_vtable =
            aws_s3_tester_get_client_vtable_patch(&tester, 0)->original_vtable;

        patched_client_vtable->schedule_process_work_synced = original_client_vtable->schedule_process_work_synced;
    }

    aws_s3_client_release(client);
    client = NULL;

    s_clean_up_host_address_array_list(&host_addresses);

    aws_s3_tester_clean_up(&tester);

    return 0;
}

struct resolve_vips_test_data {
    struct {
        struct aws_string *host_address;
        bool host_address_removed;
    } synced_data;
};

static int s_resolve_vips_test_s3_client_add_vips(
    struct aws_s3_client *client,
    const struct aws_array_list *host_addresses) {
    AWS_ASSERT(client);
    AWS_ASSERT(host_addresses);

    struct aws_s3_tester *tester = client->shutdown_callback_user_data;
    bool inc_counter = false;

    aws_s3_tester_lock_synced_data(tester);
    struct resolve_vips_test_data *test_data = tester->user_data;

    if (test_data->synced_data.host_address == NULL) {

        struct aws_host_address *first_host_address = NULL;
        aws_array_list_get_at_ptr(host_addresses, (void **)&first_host_address, 0);

        test_data->synced_data.host_address =
            aws_string_new_from_string(client->allocator, first_host_address->address);
        inc_counter = true;

        AWS_LOGF_INFO(AWS_LS_S3_GENERAL, "Test acquired ip %s", (const char *)first_host_address->address->bytes);
    }

    aws_s3_tester_unlock_synced_data(tester);

    if (inc_counter) {
        aws_s3_tester_inc_counter1(tester);
    }

    struct aws_s3_client_vtable *original_client_vtable =
        aws_s3_tester_get_client_vtable_patch(tester, 0)->original_vtable;

    return original_client_vtable->add_vips(client, host_addresses);
}

static void s_resolve_vips_test_s3_client_remove_vips(
    struct aws_s3_client *client,
    const struct aws_array_list *host_addresses) {
    AWS_ASSERT(client);
    AWS_ASSERT(host_addresses);

    struct aws_s3_tester *tester = client->shutdown_callback_user_data;
    bool inc_counter = false;

    aws_s3_tester_lock_synced_data(tester);
    struct resolve_vips_test_data *test_data = tester->user_data;

    if (test_data->synced_data.host_address != NULL && !test_data->synced_data.host_address_removed) {

        struct aws_byte_cursor host_address_cursor = aws_byte_cursor_from_string(test_data->synced_data.host_address);

        for (uint32_t i = 0; i < aws_array_list_length(host_addresses); ++i) {

            struct aws_host_address *current_host_address = NULL;
            aws_array_list_get_at_ptr(host_addresses, (void **)&current_host_address, i);

            struct aws_byte_cursor current_host_address_cursor =
                aws_byte_cursor_from_string(current_host_address->address);

            if (aws_byte_cursor_eq(&host_address_cursor, &current_host_address_cursor)) {
                AWS_LOGF_INFO(
                    AWS_LS_S3_GENERAL,
                    "Test received notification to remove ip %s",
                    (const char *)current_host_address->address->bytes);

                test_data->synced_data.host_address_removed = true;
                inc_counter = true;
            }
        }
    }
    aws_s3_tester_unlock_synced_data(tester);

    if (inc_counter) {
        aws_s3_tester_inc_counter1(tester);
    }

    struct aws_s3_client_vtable *original_client_vtable =
        aws_s3_tester_get_client_vtable_patch(tester, 0)->original_vtable;

    original_client_vtable->remove_vips(client, host_addresses);
}

AWS_TEST_CASE(test_s3_client_resolve_vips, s_test_s3_client_resolve_vips)
static int s_test_s3_client_resolve_vips(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct resolve_vips_test_data test_data;
    AWS_ZERO_STRUCT(test_data);

    tester.user_data = &test_data;

    aws_s3_tester_set_counter1_desired(&tester, 2);

    /* Set a low DNS TTL setting. */
    aws_s3_set_dns_ttl(3);

    struct aws_s3_client_config client_config;
    AWS_ZERO_STRUCT(client_config);

    ASSERT_SUCCESS(aws_s3_tester_bind_client(
        &tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION | AWS_S3_TESTER_BIND_CLIENT_SIGNING));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    struct aws_s3_client_vtable *patched_client_vtable = aws_s3_tester_patch_client_vtable(&tester, client, NULL);
    patched_client_vtable->add_vips = s_resolve_vips_test_s3_client_add_vips;
    patched_client_vtable->remove_vips = s_resolve_vips_test_s3_client_remove_vips;

    ASSERT_SUCCESS(aws_s3_tester_send_get_object_meta_request(
        &tester, client, g_s3_path_get_object_test_1MB, AWS_S3_TESTER_SEND_META_REQUEST_EXPECT_SUCCESS, NULL));

    aws_s3_tester_wait_for_counters(&tester);

    aws_s3_client_release(client);
    client = NULL;

    aws_string_destroy(test_data.synced_data.host_address);

    aws_s3_tester_clean_up(&tester);

    return 0;
}
