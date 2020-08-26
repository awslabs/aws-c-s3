/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_client_impl.h"
#include "aws/s3/private/s3_get_object_request.h"
#include "aws/s3/private/s3_meta_request_impl.h"
#include "aws/s3/private/s3_put_object_request.h"
#include "aws/s3/private/s3_request.h"
#include "aws/s3/private/s3_util.h"
#include "aws/s3/private/s3_vip_connection.h"
#include "aws/s3/private/s3_work_util.h"

#include <aws/auth/credentials.h>

#include <aws/common/atomics.h>
#include <aws/common/clock.h>
#include <aws/common/string.h>

#include <aws/http/connection.h>
#include <aws/http/connection_manager.h>
#include <aws/http/request_response.h>

#include <aws/io/channel_bootstrap.h>
#include <aws/io/event_loop.h>
#include <aws/io/host_resolver.h>
#include <aws/io/socket.h>

#include <inttypes.h>
#include <math.h>

static const int32_t s_s3_vip_connection_timeout_seconds = 3; // TODO
static const int32_t s_s3_vip_connection_port = 80;           // TODO

static const uint64_t s_default_part_size = 20 * 1024 * 1024;
static const size_t s_default_dns_host_address_ttl = 2 * 60;
static const double s_default_throughput_target_gbps = 5.0;
static const double s_default_throughput_per_vip = 6.25; // TODO provide analysis on how we reached this constant.
static const uint32_t s_default_num_connections_per_vip = 10;

static const size_t s_vips_list_initial_capacity = 16;
static const size_t s_meta_request_list_initial_capacity = 16;

static void s_s3_client_lock_synced_data(struct aws_s3_client *client);

static void s_s3_client_unlock_synced_data(struct aws_s3_client *client);

static int s_s3_client_vip_init_synced(
    struct aws_s3_client *client,
    struct aws_s3_vip *vip,
    struct aws_byte_cursor host_address);

static void s_s3_client_vip_clean_up_synced(struct aws_s3_client *client, struct aws_s3_vip *vip);

static int s_s3_client_destroy(struct aws_s3_client *client);
static void s_s3_client_destroy_task(uint32_t num_args, void **args);

static size_t s_find_vip(struct aws_array_list *vip_list, struct aws_byte_cursor host_address);

static int s_s3_client_add_vip(struct aws_s3_client *client, struct aws_byte_cursor host_address);
static void s_s3_client_add_vip_task(uint32_t num_args, void **args);

static int s_s3_client_remove_vip(struct aws_s3_client *client, struct aws_byte_cursor host_address);
static void s_s3_client_remove_vip_task(uint32_t num_args, void **args);

static int s_s3_client_push_meta_request(struct aws_s3_client *client, struct aws_s3_meta_request *meta_request);
static void s_s3_client_push_meta_request_task(uint32_t num_args, void **args);

static int s_s3_client_remove_meta_request(struct aws_s3_client *client, struct aws_s3_meta_request *meta_request);
static void s_s3_client_remove_meta_request_task(uint32_t num_args, void **args);

struct aws_s3_meta_request *aws_s3_client_make_meta_request(
    struct aws_s3_client *client,
    const struct aws_s3_meta_request_options *options);

static void s_s3_client_meta_request_finished_callback(
    struct aws_s3_meta_request *meta_request,
    int error_code,
    void *user_data);

static void s_s3_client_set_state_synced(struct aws_s3_client *client, enum aws_s3_client_state state);

static void s_s3_client_resolved_address_callback(struct aws_host_address *host_address, void *user_data);

static void s_s3_task_manager_shutdown_callback(void *user_data);

static void s_s3_client_vip_connection_shutdown_callback(void *user_data);
static void s_s3_client_vip_connection_shutdown_callback_task(uint32_t num_args, void **args);

static void s_s3_client_vip_http_connection_manager_shutdown_callback(void *user_data);
static void s_s3_client_vip_http_connection_manager_shutdown_callback_task(uint32_t num_args, void **args);

static int s_s3_client_start_resolving_addresses(struct aws_s3_client *client);
static void s_s3_client_stop_resolving_addresses(struct aws_s3_client *client);

static void s_s3_client_lock_synced_data(struct aws_s3_client *client) {
    aws_mutex_lock(&client->synced_data.lock);
}

static void s_s3_client_unlock_synced_data(struct aws_s3_client *client) {
    aws_mutex_unlock(&client->synced_data.lock);
}

struct aws_s3_client *aws_s3_client_new(
    struct aws_allocator *allocator,
    const struct aws_s3_client_config *client_config) {

    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(client_config);

    if (client_config->client_bootstrap == NULL) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_CLIENT,
            "Cannot create client from client_config; client_bootstrap provided in options is invalid.");
        aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        return NULL;
    }

    if (client_config->credentials_provider == NULL) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_CLIENT,
            "Cannot create client from client_config; credentials_provider provided in options is invalid.");
        aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        return NULL;
    }

    if (client_config->throughput_target_gbps < 0.0) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_CLIENT, "Cannot create client from client_config; throughput_target_gbps cannot be negative.");
        aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        return NULL;
    }

    if (client_config->throughput_per_vip < 0.0) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_CLIENT, "Cannot create client from client_config; throughput_per_vip cannot be negative.");
        aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        return NULL;
    }

    struct aws_s3_client *client = aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_client));

    if (client == NULL) {
        AWS_LOGF_ERROR(AWS_LS_S3_CLIENT, "Could not allocate aws_s3_client");
        return NULL;
    }

    client->allocator = allocator;

    /* Initialize client reference count. */
    aws_atomic_init_int(&client->ref_count, 0);
    aws_s3_client_acquire(client);

    /* Store our client bootstrap. */
    client->client_bootstrap = client_config->client_bootstrap;

    client->event_loop = aws_event_loop_group_get_next_loop(client_config->client_bootstrap->event_loop_group);

    /* Store credentials provider and grab a reference. */
    client->credentials_provider = client_config->credentials_provider;
    aws_credentials_provider_acquire(client->credentials_provider);

    /* Make a copy of the region string. */
    client->region = aws_string_new_from_array(allocator, client_config->region.ptr, client_config->region.len);

    if (client->region == NULL) {
        AWS_LOGF_ERROR(AWS_LS_S3_CLIENT, "id=%p: Could not allocate aws_s3_client region string", (void *)client);
        goto error_clean_up;
    }

    /* Make a copy of the endpoint string. */
    client->endpoint = aws_string_new_from_array(allocator, client_config->endpoint.ptr, client_config->endpoint.len);

    if (client->endpoint == NULL) {
        AWS_LOGF_ERROR(AWS_LS_S3_CLIENT, "id=%p: Could not allocate aws_s3_client endpoint string", (void *)client);
        goto error_clean_up;
    }

    if (client_config->part_size != 0) {
        client->part_size = client_config->part_size;
    } else {
        client->part_size = s_default_part_size;
    }

    if (client_config->dns_host_address_ttl != 0) {
        client->dns_host_address_ttl = client_config->dns_host_address_ttl;
    } else {
        client->dns_host_address_ttl = s_default_dns_host_address_ttl;
    }

    if (client_config->throughput_target_gbps != 0.0) {
        client->throughput_target_gbps = client_config->throughput_target_gbps;
    } else {
        client->throughput_target_gbps = s_default_throughput_target_gbps;
    }

    if (client_config->throughput_per_vip != 0.0) {
        client->throughput_per_vip = client_config->throughput_per_vip;
    } else {
        client->throughput_per_vip = s_default_throughput_per_vip;
    }

    if (client_config->num_connections_per_vip != 0) {
        client->num_connections_per_vip = client_config->num_connections_per_vip;
    } else {
        client->num_connections_per_vip = s_default_num_connections_per_vip;
    }

    /* Determine how many vips are ideal by dividing target-throughput by throughput-per-vip. */
    {
        double ideal_vip_count_double = client->throughput_target_gbps / client->throughput_per_vip;
        client->ideal_vip_count = (uint32_t)ceil(ideal_vip_count_double);
    }

    aws_mutex_init(&client->synced_data.lock);

    /* Set up our array list meta requests. */
    if (aws_array_list_init_dynamic(
            &client->synced_data.vips, client->allocator, s_vips_list_initial_capacity, sizeof(struct aws_s3_vip))) {
        goto error_clean_up;
    }

    /* Set up our array list meta requests. */
    if (aws_array_list_init_dynamic(
            &client->synced_data.vip_connections,
            client->allocator,
            s_vips_list_initial_capacity * client->num_connections_per_vip,
            sizeof(struct aws_s3_vip_connection *))) {
        goto error_clean_up;
    }

    /* Set up our array list meta requests. */
    if (aws_array_list_init_dynamic(
            &client->synced_data.meta_requests,
            client->allocator,
            s_meta_request_list_initial_capacity,
            sizeof(struct aws_s3_meta_request *))) {
        goto error_clean_up;
    }

    struct aws_s3_task_manager_options task_manager_options = {.allocator = client->allocator,
                                                               .event_loop = client->event_loop,
                                                               .shutdown_callback = s_s3_task_manager_shutdown_callback,
                                                               .shutdown_user_data = client};

    client->task_manager = aws_s3_task_manager_new(client->allocator, &task_manager_options);

    /* Initialize shutdown options and tracking. */
    client->shutdown_callback = client_config->shutdown_callback;
    client->shutdown_callback_user_data = client_config->shutdown_callback_user_data;

    if (s_s3_client_start_resolving_addresses(client)) {
        goto error_clean_up;
    }

    return client;

error_clean_up:

    /* NOTE: it's important that no async operations have been started if we hit here, so that we can immediately clean
     * up.  Otherwise, we might have to set shutdown_callback sooner and awkarddly rely on it. */
    if (client != NULL) {
        aws_s3_client_release(client);
        client = NULL;
    }

    return NULL;
}

/* Initialize a new VIP structure for the client to use, given an address. Assumes lock is held. */
static int s_s3_client_vip_init_synced(
    struct aws_s3_client *client,
    struct aws_s3_vip *vip,
    struct aws_byte_cursor host_address) {
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(vip);

    AWS_ZERO_STRUCT(*vip);

    ASSERT_SYNCED_DATA_LOCK_HELD(client);

    /* Copy over the host address. */
    vip->host_address = aws_string_new_from_array(client->allocator, host_address.ptr, host_address.len);

    if (vip->host_address == NULL) {
        AWS_LOGF_ERROR(AWS_LS_S3_VIP, "id=%p: Could not allocate aws_s3_vip host address string.", (void *)vip);
        goto error_clean_up;
    }

    /* Try to set up an HTTP connection manager. */
    struct aws_socket_options socket_options;
    AWS_ZERO_STRUCT(socket_options);
    socket_options.type = AWS_SOCKET_STREAM;
    socket_options.domain = AWS_SOCKET_IPV4;
    socket_options.connect_timeout_ms = (uint32_t)aws_timestamp_convert(
        s_s3_vip_connection_timeout_seconds, AWS_TIMESTAMP_SECS, AWS_TIMESTAMP_MILLIS, NULL);

    struct aws_http_connection_manager_options manager_options;
    AWS_ZERO_STRUCT(manager_options);
    manager_options.bootstrap = client->client_bootstrap;
    manager_options.initial_window_size = SIZE_MAX;
    manager_options.socket_options = &socket_options;
    manager_options.tls_connection_options = NULL;
    manager_options.proxy_options = NULL;
    manager_options.host = aws_byte_cursor_from_string(vip->host_address);
    manager_options.port = s_s3_vip_connection_port;
    manager_options.max_connections = client->num_connections_per_vip * 2;
    manager_options.shutdown_complete_callback = s_s3_client_vip_http_connection_manager_shutdown_callback;
    manager_options.shutdown_complete_user_data = client;

    vip->http_connection_manager = aws_http_connection_manager_new(client->allocator, &manager_options);

    if (vip->http_connection_manager == NULL) {
        AWS_LOGF_ERROR(AWS_LS_S3_VIP, "id=%p: Could not allocate aws_s3_vip connection manager.", (void *)vip);
        goto error_clean_up;
    }

    /* Setup all of our vip connections. */
    for (size_t conn_index = 0; conn_index < client->num_connections_per_vip; ++conn_index) {
        struct aws_s3_vip_connection_options vip_connection_options;
        AWS_ZERO_STRUCT(vip_connection_options);
        vip_connection_options.allocator = client->allocator;
        vip_connection_options.event_loop = client->event_loop;
        vip_connection_options.region = aws_byte_cursor_from_string(client->region);
        vip_connection_options.credentials_provider = client->credentials_provider;
        vip_connection_options.http_connection_manager = vip->http_connection_manager;
        vip_connection_options.shutdown_callback = s_s3_client_vip_connection_shutdown_callback;
        vip_connection_options.user_data = client;
        vip_connection_options.vip_identifier = vip->host_address;

        struct aws_s3_vip_connection *vip_connection =
            aws_s3_vip_connection_new(client->allocator, &vip_connection_options);

        if (vip_connection == NULL) {
            AWS_LOGF_ERROR(AWS_LS_S3_VIP, "id=%p: Could not allocate aws_s3_vip_connection.", (void *)vip);

            goto error_clean_up;
        }

        /* Push all existing meta requests into the connection of the new VIP. */
        for (size_t meta_request_index = 0;
             meta_request_index < aws_array_list_length(&client->synced_data.meta_requests);
             ++meta_request_index) {
            struct aws_s3_meta_request *meta_request = NULL;
            aws_array_list_get_at(&client->synced_data.meta_requests, &meta_request, meta_request_index);

            aws_s3_vip_connection_push_meta_request(vip_connection, meta_request);
        }

        if (aws_array_list_push_back(&client->synced_data.vip_connections, &vip_connection)) {
            AWS_LOGF_ERROR(
                AWS_LS_S3_VIP,
                "id=%p Could not push VIP connection %p to VIP's connection list.",
                (void *)vip,
                (void *)vip_connection);

            aws_s3_vip_connection_release(vip_connection);
            vip_connection = NULL;

            goto error_clean_up;
        }
    }

    /* Increment our allocated counts for clean up purposes.  Shutdown callbacks will decrement these accordingly. */
    ++client->synced_data.num_http_conn_managers_allocated;
    client->synced_data.num_vip_connections_allocated += client->num_connections_per_vip;

    return AWS_OP_SUCCESS;

error_clean_up:

    s_s3_client_vip_clean_up_synced(client, vip);

    return AWS_OP_ERR;
}

/* Releases the memory for a vip structure. Assumes lock is held. */
static void s_s3_client_vip_clean_up_synced(struct aws_s3_client *client, struct aws_s3_vip *vip) {
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(vip);

    ASSERT_SYNCED_DATA_LOCK_HELD(client);

    size_t vip_conn_index = 0;

    /* Go through all the VIP connections and remove the ones that we don't want anymore. */
    while (vip_conn_index < aws_array_list_length(&client->synced_data.vip_connections)) {
        struct aws_s3_vip_connection *vip_connection = NULL;
        aws_array_list_get_at(&client->synced_data.vip_connections, &vip_connection, vip_conn_index);

        void *vip_identifier = aws_s3_vip_connection_get_vip_identifier(vip_connection);

        if ((void *)vip->host_address == vip_identifier) {
            aws_s3_vip_connection_release(vip_connection);

            aws_array_list_swap(
                &client->synced_data.vip_connections,
                vip_conn_index,
                aws_array_list_length(&client->synced_data.vip_connections) - 1);
            aws_array_list_pop_back(&client->synced_data.vip_connections);
        } else {
            ++vip_conn_index;
        }
    }

    /* Release the VIP's reference to it's connection manager. */
    if (vip->http_connection_manager != NULL) {
        aws_http_connection_manager_release(vip->http_connection_manager);
        vip->http_connection_manager = NULL;
    }

    /* Clean up the address string. */
    if (vip->host_address != NULL) {
        aws_string_destroy(vip->host_address);
        vip->host_address = NULL;
    }
}

void aws_s3_client_acquire(struct aws_s3_client *client) {
    AWS_PRECONDITION(client);

    aws_atomic_fetch_add(&client->ref_count, 1);
}

void aws_s3_client_release(struct aws_s3_client *client) {
    AWS_PRECONDITION(client);

    size_t prev_ref_count = aws_atomic_fetch_sub(&client->ref_count, 1);

    if (prev_ref_count > 1) {
        return;
    }

    /* Issue our clean up action. */
    if (s_s3_client_destroy(client)) {
        AWS_LOGF_ERROR(AWS_LS_S3_CLIENT, "id=%p: Error initiating clean up action for client.", (void *)client);
    }
}

static int s_s3_client_destroy(struct aws_s3_client *client) {
    AWS_PRECONDITION(client);

    return aws_s3_task_manager_create_task(client->task_manager, s_s3_client_destroy_task, 0, 1, client);
}

static void s_s3_client_destroy_task(uint32_t num_args, void **args) {
    AWS_PRECONDITION(num_args == 1);
    (void)num_args;

    struct aws_s3_client *client = args[0];

    s_s3_client_lock_synced_data(client);
    s_s3_client_set_state_synced(client, AWS_S3_CLIENT_STATE_CLEAN_UP);
    s_s3_client_unlock_synced_data(client);
}

static size_t s_find_vip(struct aws_array_list *vip_list, struct aws_byte_cursor host_address) {
    AWS_PRECONDITION(vip_list);

    size_t num_vips = aws_array_list_length(vip_list);

    for (size_t vip_index = 0; vip_index < num_vips; ++vip_index) {

        struct aws_s3_vip *vip = NULL;

        aws_array_list_get_at_ptr(vip_list, (void **)&vip, vip_index);

        struct aws_byte_cursor vip_host_address = aws_byte_cursor_from_string(vip->host_address);

        if (aws_byte_cursor_eq(&host_address, &vip_host_address)) {

            return vip_index;
        }
    }

    return (size_t)-1;
}

static int s_s3_client_add_vip(struct aws_s3_client *client, struct aws_byte_cursor host_address) {

    struct aws_string *copied_host_address =
        aws_string_new_from_array(client->allocator, host_address.ptr, host_address.len);

    return aws_s3_task_manager_create_task(
        client->task_manager, s_s3_client_add_vip_task, 0, 2, client, copied_host_address);
}

static void s_s3_client_add_vip_task(uint32_t num_args, void **args) {
    AWS_PRECONDITION(num_args == 2);
    (void)num_args;

    struct aws_s3_client *client = args[0];
    struct aws_string *host_address = args[1];

    s_s3_client_lock_synced_data(client);

    size_t vip_index = s_find_vip(&client->synced_data.vips, aws_byte_cursor_from_string(host_address));

    /* If we didn't find a match in the table, we have a VIP to add! */
    if (vip_index != (size_t)-1) {
        goto clean_up;
    }

    AWS_LOGF_INFO(
        AWS_LS_S3_CLIENT, "id=%p: Creating new VIP for address %s", (void *)client, (const char *)host_address->bytes);

    /* Allocate the new VIP. */
    struct aws_s3_vip vip;
    s_s3_client_vip_init_synced(client, &vip, aws_byte_cursor_from_string(host_address));

    /* TODO Would be cool if we could lengthen the size of the array and initialize directly into memory passed back. */
    aws_array_list_push_back(&client->synced_data.vips, &vip); // TODO possible error here

    s_s3_client_unlock_synced_data(client);

clean_up:

    if (host_address != NULL) {
        aws_string_destroy(host_address);
        host_address = NULL;
    }
}

static int s_s3_client_remove_vip(struct aws_s3_client *client, struct aws_byte_cursor host_address) {
    struct aws_string *copied_host_address =
        aws_string_new_from_array(client->allocator, host_address.ptr, host_address.len);

    if (copied_host_address == NULL) {
        AWS_LOGF_ERROR(AWS_LS_S3_CLIENT, "id=%p Could not allocate host address string copy.", (void *)client);
    }

    return aws_s3_task_manager_create_task(
        client->task_manager, s_s3_client_remove_vip_task, 0, 2, client, copied_host_address);
}

static void s_s3_client_remove_vip_task(uint32_t num_args, void **args) {
    AWS_PRECONDITION(num_args == 2);
    (void)num_args;

    struct aws_s3_client *client = args[0];
    struct aws_string *host_address = args[1];

    AWS_PRECONDITION(client);
    AWS_PRECONDITION(host_address);

    s_s3_client_lock_synced_data(client);

    size_t vip_index = s_find_vip(&client->synced_data.vips, aws_byte_cursor_from_string(host_address));

    if (vip_index == (size_t)-1) {
        s_s3_client_unlock_synced_data(client);
        return;
    }

    struct aws_s3_vip *vip = NULL;
    aws_array_list_get_at_ptr(&client->synced_data.vips, (void **)&vip, vip_index);
    s_s3_client_vip_clean_up_synced(client, vip);

    aws_array_list_swap(&client->synced_data.vips, vip_index, aws_array_list_length(&client->synced_data.vips) - 1);
    aws_array_list_pop_back(&client->synced_data.vips);

    s_s3_client_unlock_synced_data(client);

    aws_string_destroy(host_address);
    host_address = NULL;
}

static int s_s3_client_push_meta_request(struct aws_s3_client *client, struct aws_s3_meta_request *meta_request) {
    aws_s3_meta_request_acquire(meta_request);

    return aws_s3_task_manager_create_task(
        client->task_manager, s_s3_client_push_meta_request_task, 0, 2, client, meta_request);
}

static void s_s3_client_push_meta_request_task(uint32_t num_args, void **args) {
    AWS_PRECONDITION(num_args == 2);
    (void)num_args;

    struct aws_s3_client *client = args[0];
    struct aws_s3_meta_request *meta_request = args[1];

    s_s3_client_lock_synced_data(client);

    /* Add our new meta request to our request list */
    if (aws_array_list_push_back(&client->synced_data.meta_requests, &meta_request)) {
        aws_s3_meta_request_release(meta_request);

        AWS_LOGF_ERROR(
            AWS_LS_S3_CLIENT,
            "id=%p Could not push meta request %p to client's meta request list.",
            (void *)client,
            (void *)meta_request);

        return;
    }

    for (size_t vip_conn_index = 0; vip_conn_index < aws_array_list_length(&client->synced_data.vip_connections);
         ++vip_conn_index) {
        struct aws_s3_vip_connection *vip_connection = NULL;
        aws_array_list_get_at(&client->synced_data.vip_connections, &vip_connection, vip_conn_index);

        if (aws_s3_vip_connection_push_meta_request(vip_connection, meta_request)) {
            AWS_LOGF_ERROR(AWS_LS_S3_CLIENT, "id=%p Could not push meta request to VIP connection", (void *)client);
        }
    }

    s_s3_client_unlock_synced_data(client);
}

static int s_s3_client_remove_meta_request(struct aws_s3_client *client, struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(meta_request);

    aws_s3_meta_request_acquire(meta_request);

    return aws_s3_task_manager_create_task(
        client->task_manager, s_s3_client_remove_meta_request_task, 0, 2, client, meta_request);
}

static void s_s3_client_remove_meta_request_task(uint32_t num_args, void **args) {
    AWS_PRECONDITION(num_args == 2);
    (void)num_args;

    struct aws_s3_client *client = args[0];
    struct aws_s3_meta_request *meta_request = args[1];

    AWS_PRECONDITION(client);
    AWS_PRECONDITION(meta_request);

    s_s3_client_lock_synced_data(client);

    /* Remove the meta request from our own internal list. */
    for (size_t meta_request_index = 0; meta_request_index < aws_array_list_length(&client->synced_data.meta_requests);
         ++meta_request_index) {
        struct aws_s3_meta_request *meta_request_item = NULL;
        aws_array_list_get_at(&client->synced_data.meta_requests, &meta_request_item, meta_request_index);

        if (meta_request_item == meta_request) {
            aws_array_list_erase(&client->synced_data.meta_requests, meta_request_index);
            aws_s3_meta_request_release(meta_request);
            break;
        }
    }

    for (size_t vip_conn_index = 0; vip_conn_index < aws_array_list_length(&client->synced_data.vip_connections);
         ++vip_conn_index) {
        struct aws_s3_vip_connection *vip_connection = NULL;
        aws_array_list_get_at(&client->synced_data.vip_connections, &vip_connection, vip_conn_index);

        if (aws_s3_vip_connection_remove_meta_request(vip_connection, meta_request)) {
            AWS_LOGF_ERROR(AWS_LS_S3_CLIENT, "id=%p Could not remove meta request from VIP connection", (void *)client);
        }
    }

    s_s3_client_unlock_synced_data(client);

    aws_s3_meta_request_release(meta_request);
}

struct aws_s3_meta_request *aws_s3_client_make_meta_request(
    struct aws_s3_client *client,
    const struct aws_s3_meta_request_options *options) {

    AWS_LOGF_INFO(AWS_LS_S3_CLIENT, "id=%p Initiating making of meta request", (void *)client);

    AWS_PRECONDITION(client);
    AWS_PRECONDITION(options);

    if (options->type != AWS_S3_META_REQUEST_TYPE_GET_OBJECT && options->type != AWS_S3_META_REQUEST_TYPE_PUT_OBJECT) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_CLIENT,
            "id=%p Cannot create meta s3 request; invalid meta request type specified.",
            (void *)client);
        aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        return NULL;
    }

    if (options->message == NULL) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_CLIENT,
            "id=%p Cannot create meta s3 request; message provided in options is invalid.",
            (void *)client);
        aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        return NULL;
    }

    struct aws_s3_meta_request_internal_options internal_options;
    internal_options.options = options;
    internal_options.finish_callback = s_s3_client_meta_request_finished_callback;
    internal_options.user_data = client;

    /* Spin up a new meta request for this acceleration */
    struct aws_s3_meta_request *meta_request = aws_s3_meta_request_new(client->allocator, client, &internal_options);

    if (meta_request == NULL) {
        AWS_LOGF_ERROR(AWS_LS_S3_CLIENT, "id=%p: Could not create new meta request.", (void *)client);
        return NULL;
    }

    if (s_s3_client_push_meta_request(client, meta_request)) {
        AWS_LOGF_ERROR(AWS_LS_S3_CLIENT, "id=%p: Could not initate pushing of new meta request.", (void *)client);
        aws_s3_meta_request_release(meta_request);
        return NULL;
    }

    return meta_request;
}

/* Callback for when the meta request is finished. */
static void s_s3_client_meta_request_finished_callback(
    struct aws_s3_meta_request *meta_request,
    int error_code,
    void *user_data) {
    (void)error_code;

    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(user_data);

    struct aws_s3_client *client = user_data;

    s_s3_client_remove_meta_request(client, meta_request);
}

/* Shutdown callback for our async work controller. */
static void s_s3_task_manager_shutdown_callback(void *user_data) {
    AWS_PRECONDITION(user_data);

    struct aws_s3_client *client = user_data;

    s_s3_client_lock_synced_data(client);
    s_s3_client_set_state_synced(client, AWS_S3_CLIENT_STATE_CLEAN_UP_TASK_MANAGER_FINISHED);
    s_s3_client_unlock_synced_data(client);
}

/* Callback for address being resolved by the host resolver. */
static void s_s3_client_resolved_address_callback(struct aws_host_address *host_address, void *user_data) {
    AWS_PRECONDITION(host_address);
    AWS_PRECONDITION(user_data);

    struct aws_s3_client *client = user_data;

    /* Issue an async action to create a VIP from the resolved address. */
    if (s_s3_client_add_vip(client, aws_byte_cursor_from_string(host_address->address))) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_CLIENT,
            "id=%p: Could not initate adding VIP with address %s to client.",
            (void *)client,
            (const char *)host_address->address->bytes);
    }
}

static void s_s3_client_vip_connection_shutdown_callback(void *user_data) {
    AWS_PRECONDITION(user_data);

    struct aws_s3_client *client = user_data;

    if (aws_s3_task_manager_create_task(
            client->task_manager, s_s3_client_vip_connection_shutdown_callback_task, 0, 1, client)) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_CLIENT,
            "id=%p: Shutdown for client failed during vip connection shutdown callback.",
            (void *)client);
    }
}

static void s_s3_client_vip_connection_shutdown_callback_task(uint32_t num_args, void **args) {
    AWS_PRECONDITION(num_args == 1);
    (void)num_args;

    struct aws_s3_client *client = args[0];
    AWS_PRECONDITION(client);

    s_s3_client_lock_synced_data(client);

    --client->synced_data.num_vip_connections_allocated;

    if (client->synced_data.state == AWS_S3_CLIENT_STATE_CLEAN_UP_VIPS &&
        client->synced_data.num_http_conn_managers_allocated == 0 &&
        client->synced_data.num_vip_connections_allocated == 0) {
        s_s3_client_set_state_synced(client, AWS_S3_CLIENT_STATE_CLEAN_UP_VIPS_FINISHED);
    }

    s_s3_client_unlock_synced_data(client);
}

static void s_s3_client_vip_http_connection_manager_shutdown_callback(void *user_data) {
    AWS_PRECONDITION(user_data);

    struct aws_s3_client *client = user_data;
    AWS_PRECONDITION(client);

    if (aws_s3_task_manager_create_task(
            client->task_manager, s_s3_client_vip_http_connection_manager_shutdown_callback_task, 0, 1, client)) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_CLIENT,
            "id=%p: Shutdown for client failed during http connection manager shutdown callback.",
            (void *)client);
    }
}

static void s_s3_client_vip_http_connection_manager_shutdown_callback_task(uint32_t num_args, void **args) {
    AWS_PRECONDITION(num_args == 1);
    (void)num_args;

    struct aws_s3_client *client = args[0];
    AWS_PRECONDITION(client);

    s_s3_client_lock_synced_data(client);

    --client->synced_data.num_http_conn_managers_allocated;

    if (client->synced_data.state == AWS_S3_CLIENT_STATE_CLEAN_UP_VIPS &&
        client->synced_data.num_http_conn_managers_allocated == 0 &&
        client->synced_data.num_vip_connections_allocated == 0) {
        s_s3_client_set_state_synced(client, AWS_S3_CLIENT_STATE_CLEAN_UP_VIPS_FINISHED);
    }

    s_s3_client_unlock_synced_data(client);
}

/* Updates client's place in its state machine.  Assumes that the sync lock has been held. */
static void s_s3_client_set_state_synced(struct aws_s3_client *client, enum aws_s3_client_state state) {
    AWS_PRECONDITION(client);

    ASSERT_SYNCED_DATA_LOCK_HELD(client);

    if (client->synced_data.state == state) {
        return;
    }

    client->synced_data.state = state;

    switch (client->synced_data.state) {
        case AWS_S3_CLIENT_STATE_ACTIVE: {
            break;
        }
        case AWS_S3_CLIENT_STATE_CLEAN_UP: {
            s_s3_client_set_state_synced(client, AWS_S3_CLIENT_STATE_CLEAN_UP_RESOLVE);
            break;
        }
        case AWS_S3_CLIENT_STATE_CLEAN_UP_RESOLVE: {
            s_s3_client_stop_resolving_addresses(client);

            s_s3_client_set_state_synced(client, AWS_S3_CLIENT_STATE_CLEAN_UP_RESOLVE_FINISHED);
            break;
        }
        case AWS_S3_CLIENT_STATE_CLEAN_UP_RESOLVE_FINISHED: {
            s_s3_client_set_state_synced(client, AWS_S3_CLIENT_STATE_CLEAN_UP_VIPS);
            break;
        }
        case AWS_S3_CLIENT_STATE_CLEAN_UP_VIPS: {

            if (client->synced_data.num_http_conn_managers_allocated == 0 &&
                client->synced_data.num_vip_connections_allocated == 0) {
                s_s3_client_set_state_synced(client, AWS_S3_CLIENT_STATE_CLEAN_UP_VIPS_FINISHED);
            } else {
                for (size_t vip_index = 0; vip_index < aws_array_list_length(&client->synced_data.vips); ++vip_index) {
                    struct aws_s3_vip *vip = NULL;
                    aws_array_list_get_at_ptr(&client->synced_data.vips, (void **)&vip, vip_index);
                    s_s3_client_remove_vip(client, aws_byte_cursor_from_string(vip->host_address));
                }
            }

            break;
        }
        case AWS_S3_CLIENT_STATE_CLEAN_UP_VIPS_FINISHED: {
            s_s3_client_set_state_synced(client, AWS_S3_CLIENT_STATE_CLEAN_UP_TASK_MANAGER);
            break;
        }
        case AWS_S3_CLIENT_STATE_CLEAN_UP_TASK_MANAGER: {
            if (client->task_manager != NULL) {
                aws_s3_task_manager_destroy(client->task_manager);
                client->task_manager = NULL;
            }
            break;
        }
        case AWS_S3_CLIENT_STATE_CLEAN_UP_TASK_MANAGER_FINISHED: {
            s_s3_client_set_state_synced(client, AWS_S3_CLIENT_STATE_CLEAN_UP_FINISH_RELEASE);
            break;
        }
        case AWS_S3_CLIENT_STATE_CLEAN_UP_FINISH_RELEASE: {
            if (client->credentials_provider != NULL) {
                aws_credentials_provider_release(client->credentials_provider);
                client->credentials_provider = NULL;
            }

            if (client->region != NULL) {
                aws_string_destroy(client->region);
                client->region = NULL;
            }

            if (client->endpoint != NULL) {
                aws_string_destroy(client->endpoint);
                client->endpoint = NULL;
            }

            aws_mutex_clean_up(&client->synced_data.lock);

            /* Clear out our meta request list. */
            for (size_t meta_request_index = 0;
                 meta_request_index < aws_array_list_length(&client->synced_data.meta_requests);
                 ++meta_request_index) {
                struct aws_s3_meta_request *meta_request = NULL;
                aws_array_list_get_at(&client->synced_data.meta_requests, &meta_request, meta_request_index);
                aws_s3_meta_request_release(meta_request);
            }

            aws_array_list_clean_up(&client->synced_data.vips);
            aws_array_list_clean_up(&client->synced_data.vip_connections);
            aws_array_list_clean_up(&client->synced_data.meta_requests);

            aws_s3_client_shutdown_complete_callback_fn *shutdown_callback = client->shutdown_callback;
            void *shutdown_user_data = client->shutdown_callback_user_data;

            aws_mem_release(client->allocator, client);
            client = NULL;

            shutdown_callback(shutdown_user_data);
            break;
        }
        default:
            AWS_FATAL_ASSERT(false);
            break;
    }
}

/* BEGIN Temporary Hack - The following section is a temporary hack for listening to host resolution events that have
 * been hacked in via a branch of aws-c-io. */
static void s_on_host_resolved_stub(
    struct aws_host_resolver *resolver,
    const struct aws_string *host_name,
    int err_code,
    const struct aws_array_list *host_addresses,
    void *user_data) {
    (void)resolver;
    (void)host_name;
    (void)err_code;
    (void)host_addresses;
    (void)user_data;
}

static int s_s3_client_start_resolving_addresses(struct aws_s3_client *client) {

    struct aws_host_resolution_config host_resolver_config;
    AWS_ZERO_STRUCT(host_resolver_config);
    host_resolver_config.impl = aws_default_dns_resolve;
    host_resolver_config.max_ttl = client->dns_host_address_ttl;
    host_resolver_config.resolved_address_callback = s_s3_client_resolved_address_callback;
    host_resolver_config.address_expired_callback = NULL;
    host_resolver_config.impl_data = client;

    if (aws_host_resolver_resolve_host(
            client->client_bootstrap->host_resolver,
            client->endpoint,
            s_on_host_resolved_stub,
            &host_resolver_config,
            client)) {

        AWS_LOGF_ERROR(
            AWS_LS_S3_CLIENT,
            "id=%p: Error trying to resolve host for endpoint %s",
            (void *)client,
            (const char *)client->endpoint->bytes);

        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}

static void s_s3_client_stop_resolving_addresses(struct aws_s3_client *client) {

    struct aws_host_resolution_config host_resolver_config;
    AWS_ZERO_STRUCT(host_resolver_config);
    host_resolver_config.impl = aws_default_dns_resolve;
    host_resolver_config.max_ttl = client->dns_host_address_ttl;
    host_resolver_config.resolved_address_callback = NULL;
    host_resolver_config.address_expired_callback = NULL;
    host_resolver_config.impl_data = NULL;

    if (aws_host_resolver_resolve_host(
            client->client_bootstrap->host_resolver,
            client->endpoint,
            s_on_host_resolved_stub,
            &host_resolver_config,
            client)) {

        AWS_LOGF_ERROR(AWS_LS_S3_CLIENT, "id=%p: Error trying to clean up state in host resolver.", (void *)client);
    }
}
/* END Temporary Hack */
