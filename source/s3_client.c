/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_client_impl.h"
#include "aws/s3/private/s3_meta_request_impl.h"
#include "aws/s3/private/s3_request.h"
#include "aws/s3/private/s3_util.h"
#include "aws/s3/private/s3_work_util.h"

#include <aws/auth/credentials.h>
#include <aws/auth/signable.h>
#include <aws/auth/signing.h>
#include <aws/auth/signing_config.h>
#include <aws/auth/signing_result.h>
#include <aws/common/assert.h>
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

static const int32_t s_s3_max_request_count_per_connection = 100;

static const int32_t s_s3_vip_connection_timeout_seconds = 3; // TODO
static const int32_t s_s3_vip_connection_port = 80;           // TODO
static const uint64_t s_vip_connection_processing_retry_offset_ms = 50;

static const uint64_t s_default_part_size = 20 * 1024 * 1024;
static const size_t s_default_dns_host_address_ttl = 2 * 60;
static const double s_default_throughput_target_gbps = 5.0;
static const double s_default_throughput_per_vip = 6.25; // TODO provide analysis on how we reached this constant.
static const uint32_t s_default_num_connections_per_vip = 10;

static const size_t s_vips_list_initial_capacity = 16;
static const size_t s_meta_request_list_initial_capacity = 16;

/* BEGIN Locking Functions */
static void s_s3_client_lock_synced_data(struct aws_s3_client *client);
static void s_s3_client_unlock_synced_data(struct aws_s3_client *client);
/* END Locking Functions */

/* BEGIN Allocation/Destruction Functions */
static bool s_s3_client_is_cleaning_up(struct aws_s3_client *client);
static void s_s3_client_conditional_clean_up_async(struct aws_s3_client *client);
static void s_s3_client_conditional_clean_up(struct aws_s3_client *client);

static void s_s3_task_manager_shutdown_callback(void *user_data);
static void s_s3_client_vip_http_connection_manager_shutdown_callback(void *user_data);

static int s_s3_client_vip_init_synced(
    struct aws_s3_client *client,
    struct aws_s3_vip *vip,
    struct aws_byte_cursor host_address);

static void s_s3_client_vip_clean_up_synced(struct aws_s3_client *client, struct aws_s3_vip *vip);

struct aws_s3_vip_connection *aws_s3_vip_connection_new_synced(struct aws_s3_client *client, struct aws_s3_vip *vip);
void s_s3_vip_connection_destroy_synced(struct aws_s3_client *client, struct aws_s3_vip_connection *vip_connection);
/* END Allocation/Destruction Functions */

/* BEGIN Utility Functions */
static size_t s_s3_find_vip(struct aws_array_list *vip_list, struct aws_byte_cursor host_address);
/* END Utility Functions*/

/* BEGIN Part Buffer Pool Functions */
static void s_s3_part_buffer_pool_init(struct aws_s3_part_buffer_pool *pool);
static void s_s3_client_add_new_part_buffers_to_pool(struct aws_s3_client *client, size_t num_buffers);
static void s_s3_client_destroy_part_buffer_pool_synced(struct aws_s3_client *client);
/* END Part Buffer Pool Functions */

/* BEGIN Meta Request Functions  */
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
/* END Meta Request Functions */

/* BEGIN VIP Functions */
static void s_s3_client_resolved_address_callback(struct aws_host_address *host_address, void *user_data);

static int s_s3_client_add_vip(struct aws_s3_client *client, struct aws_byte_cursor host_address);
static void s_s3_client_add_vip_task(uint32_t num_args, void **args);

static int s_s3_client_remove_vip(struct aws_s3_client *client, struct aws_byte_cursor host_address);
static void s_s3_client_remove_vip_task(uint32_t num_args, void **args);
/* END VIP Functions */

/* BEGIN VIP Connection Functions */
static int s_s3_client_vip_connection_process_meta_requests(
    struct aws_s3_client *client,
    struct aws_s3_vip_connection *vip_connection);

static void s_s3_client_vip_connection_process_meta_requests_loop_task(uint32_t num_args, void **args);

static void s_s3_client_vip_connection_request_prepare(uint32_t num_args, void **args);

static void s_s3_client_vip_connection_request_start(
    struct aws_s3_vip_connection_make_request_state *make_request_state);

static void s_s3_vip_connection_request_signing_complete(
    struct aws_signing_result *result,
    int error_code,
    void *user_data);

static void s_s3_client_vip_connection_acquire_request_connection(
    struct aws_s3_vip_connection_make_request_state *make_request_state);

static void s_s3_client_vip_connection_on_acquire_request_connection(
    struct aws_http_connection *http_connection,
    int error_code,
    void *user_data);

static void s_s3_client_vip_connection_request_finished_callback(
    struct aws_s3_request *request,
    struct aws_http_stream *stream,
    int error_code);

static void s_s3_client_vip_connection_end_request(
    struct aws_s3_vip_connection_make_request_state *make_request_state,
    int error_code);
/* END VIP Connection Functions*/

/* BEGIN TEMP Host Resolver Functions */
static int s_s3_client_start_resolving_addresses(struct aws_s3_client *client);
static void s_s3_client_stop_resolving_addresses(struct aws_s3_client *client);
/* END TEMP Host Resolver Functions */

/* BEGIN Locking Functions */
static void s_s3_client_lock_synced_data(struct aws_s3_client *client) {
    aws_mutex_lock(&client->synced_data.lock);
}

static void s_s3_client_unlock_synced_data(struct aws_s3_client *client) {
    aws_mutex_unlock(&client->synced_data.lock);
}
/* END Locking Functions */

/* BEGIN Allocation/Destruction Functions */
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

    aws_atomic_init_int(&client->num_vip_connections_allocated, 0);
    aws_atomic_init_int(&client->num_http_conn_managers_allocated, 0);
    aws_atomic_init_int(&client->resolving_hosts, 0);

    aws_mutex_init(&client->synced_data.lock);

    /* Set up our array list of VIP's  */
    if (aws_array_list_init_dynamic(
            &client->synced_data.vips, client->allocator, s_vips_list_initial_capacity, sizeof(struct aws_s3_vip))) {
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

    aws_linked_list_init(&client->synced_data.idle_vip_connections);
    aws_linked_list_init(&client->synced_data.active_vip_connections);

    struct aws_s3_task_manager_options task_manager_options = {.allocator = client->allocator,
                                                               .event_loop = client->event_loop,
                                                               .shutdown_callback = s_s3_task_manager_shutdown_callback,
                                                               .shutdown_user_data = client};

    client->task_manager = aws_s3_task_manager_new(client->allocator, &task_manager_options);

    s_s3_part_buffer_pool_init(&client->synced_data.part_buffer_pool);

    /* Initialize shutdown options and tracking. */
    client->shutdown_callback = client_config->shutdown_callback;
    client->shutdown_callback_user_data = client_config->shutdown_callback_user_data;

    if (s_s3_client_start_resolving_addresses(client)) {
        goto error_clean_up;
    }

    return client;

error_clean_up:

    /* NOTE: it's important that no async operations have been started if we hit here, so that we can immediately
     * clean up.  Otherwise, we might have to set shutdown_callback sooner and awkarddly rely on it. */
    if (client != NULL) {
        aws_s3_client_release(client);
        client = NULL;
    }

    return NULL;
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

    s_s3_client_conditional_clean_up_async(client);
}

static bool s_s3_client_is_cleaning_up(struct aws_s3_client *client) {
    AWS_PRECONDITION(client);

    size_t ref_count = aws_atomic_load_int(&client->ref_count);

    return (ref_count == 0);
}

static void s_s3_client_conditional_clean_up_task(uint32_t num_args, void **args) {
    AWS_PRECONDITION(num_args == 1);
    (void)num_args;

    AWS_PRECONDITION(args);

    struct aws_s3_client *client = args[0];
    AWS_PRECONDITION(client);

    s_s3_client_conditional_clean_up(client);
}

static void s_s3_client_conditional_clean_up_async(struct aws_s3_client *client) {
    AWS_PRECONDITION(client);

    if (!s_s3_client_is_cleaning_up(client)) {
        return;
    }

    if (aws_s3_task_manager_create_task(client->task_manager, s_s3_client_conditional_clean_up_task, 0, 1, client)) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_CLIENT, "id=%p Could not create task required for cleaning up the client", (void *)client);
    }
}

static void s_s3_client_conditional_clean_up(struct aws_s3_client *client) {
    AWS_PRECONDITION(client);

    if (!s_s3_client_is_cleaning_up(client)) {
        return;
    }

    s_s3_client_lock_synced_data(client);

    s_s3_client_stop_resolving_addresses(client);

    for (size_t vip_index = 0; vip_index < aws_array_list_length(&client->synced_data.vips); ++vip_index) {
        struct aws_s3_vip *vip = NULL;
        aws_array_list_get_at_ptr(&client->synced_data.vips, (void **)&vip, vip_index);
        s_s3_client_remove_vip(client, aws_byte_cursor_from_string(vip->host_address));
    }

    size_t num_http_conn_managers_allocated = aws_atomic_load_int(&client->num_http_conn_managers_allocated);

    if (num_http_conn_managers_allocated > 0) {
        s_s3_client_unlock_synced_data(client);
        return;
    }

    size_t num_vip_connections_allocated = aws_atomic_load_int(&client->num_vip_connections_allocated);

    if (num_vip_connections_allocated > 0) {
        s_s3_client_unlock_synced_data(client);
        return;
    }

    if (client->task_manager != NULL) {
        aws_s3_task_manager_destroy(client->task_manager);
        client->task_manager = NULL;
        s_s3_client_unlock_synced_data(client);
        return;
    }

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
    for (size_t meta_request_index = 0; meta_request_index < aws_array_list_length(&client->synced_data.meta_requests);
         ++meta_request_index) {
        struct aws_s3_meta_request *meta_request = NULL;
        aws_array_list_get_at(&client->synced_data.meta_requests, &meta_request, meta_request_index);
        aws_s3_meta_request_release(meta_request);
    }

    aws_array_list_clean_up(&client->synced_data.vips);
    aws_array_list_clean_up(&client->synced_data.meta_requests);

    s_s3_client_destroy_part_buffer_pool_synced(client);

    aws_s3_client_shutdown_complete_callback_fn *shutdown_callback = client->shutdown_callback;
    void *shutdown_user_data = client->shutdown_callback_user_data;

    aws_mem_release(client->allocator, client);
    client = NULL;

    shutdown_callback(shutdown_user_data);
}

/* Shutdown callback for our task manager. */
static void s_s3_task_manager_shutdown_callback(void *user_data) {
    AWS_PRECONDITION(user_data);

    struct aws_s3_client *client = user_data;
    s_s3_client_conditional_clean_up(client);
}

static void s_s3_client_vip_http_connection_manager_shutdown_callback(void *user_data) {
    AWS_PRECONDITION(user_data);

    struct aws_s3_client *client = user_data;
    AWS_PRECONDITION(client);

    aws_atomic_fetch_sub(&client->num_http_conn_managers_allocated, 1);

    s_s3_client_conditional_clean_up_async(client);
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

    /* Increment our allocated counts for clean up purposes.  Shutdown callbacks will decrement these accordingly.
     */
    aws_atomic_fetch_add(&client->num_http_conn_managers_allocated, 1);

    /* Setup all of our vip connections. */
    for (size_t conn_index = 0; conn_index < client->num_connections_per_vip; ++conn_index) {
        struct aws_s3_vip_connection *vip_connection = aws_s3_vip_connection_new_synced(client, vip);

        if (vip_connection == NULL) {
            AWS_LOGF_ERROR(AWS_LS_S3_VIP, "id=%p: Could not allocate aws_s3_vip_connection.", (void *)vip);

            goto error_clean_up;
        }

        aws_linked_list_push_back(&client->synced_data.active_vip_connections, &vip_connection->node);

        s_s3_client_vip_connection_process_meta_requests(client, vip_connection);
    }

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

    if (!aws_linked_list_empty(&client->synced_data.active_vip_connections)) {
        struct aws_linked_list_node *current_node = aws_linked_list_front(&client->synced_data.active_vip_connections);

        /* Tell any active connections of this VIP that they need to shutdown. */
        while (current_node != aws_linked_list_end(&client->synced_data.active_vip_connections)) {
            struct aws_s3_vip_connection *vip_connection =
                AWS_CONTAINER_OF(current_node, struct aws_s3_vip_connection, node);

            if (vip_connection->vip_id == (void *)vip->host_address) {
                vip_connection->pending_destruction = true;
            }

            current_node = aws_linked_list_next(current_node);
        }
    }

    if (!aws_linked_list_empty(&client->synced_data.idle_vip_connections)) {
        struct aws_linked_list_node *current_node = aws_linked_list_front(&client->synced_data.idle_vip_connections);

        /* Immediately free any connections that are idle. */
        while (current_node != aws_linked_list_end(&client->synced_data.idle_vip_connections)) {
            struct aws_s3_vip_connection *vip_connection =
                AWS_CONTAINER_OF(current_node, struct aws_s3_vip_connection, node);

            struct aws_linked_list_node *next_node = aws_linked_list_next(current_node);

            if (vip_connection->vip_id == (void *)vip->host_address) {
                s_s3_vip_connection_destroy_synced(client, vip_connection);
                aws_linked_list_remove(current_node);
            }

            current_node = next_node;
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

struct aws_s3_vip_connection *aws_s3_vip_connection_new_synced(struct aws_s3_client *client, struct aws_s3_vip *vip) {
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(vip);

    ASSERT_SYNCED_DATA_LOCK_HELD(client);

    struct aws_s3_vip_connection *vip_connection =
        aws_mem_calloc(client->allocator, 1, sizeof(struct aws_s3_vip_connection));

    if (vip_connection == NULL) {
        AWS_LOGF_ERROR(AWS_LS_S3_VIP_CONNECTION, "Could not allocate new aws_s3_vip_connection.");
        return NULL;
    }

    aws_atomic_fetch_add(&client->num_vip_connections_allocated, 1);

    vip_connection->vip_id = (void *)vip->host_address;

    vip_connection->http_connection_manager = vip->http_connection_manager;
    aws_http_connection_manager_acquire(vip->http_connection_manager);

    return vip_connection;
}

void s_s3_vip_connection_destroy_synced(struct aws_s3_client *client, struct aws_s3_vip_connection *vip_connection) {
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(vip_connection);

    ASSERT_SYNCED_DATA_LOCK_HELD(client);

    if (vip_connection->http_connection_manager != NULL) {
        if (vip_connection->make_request_state.http_connection != NULL) {
            aws_http_connection_manager_release_connection(
                vip_connection->http_connection_manager, vip_connection->make_request_state.http_connection);
        }
        aws_http_connection_manager_release(vip_connection->http_connection_manager);
        vip_connection->http_connection_manager = NULL;
    }

    aws_atomic_fetch_sub(&client->num_vip_connections_allocated, 1);

    aws_mem_release(client->allocator, vip_connection);

    s_s3_client_conditional_clean_up_async(client);
}
/* END Allocation/Destruction Functions */

/* BEGIN Utility Functions */
static size_t s_s3_find_vip(struct aws_array_list *vip_list, struct aws_byte_cursor host_address) {
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
/* END Utility Functions*/

/* BEGIN Part Buffer Pool Functions */
struct aws_s3_part_buffer *aws_s3_client_get_part_buffer(struct aws_s3_client *client) {
    AWS_PRECONDITION(client);

    struct aws_s3_part_buffer *result = NULL;

    s_s3_client_lock_synced_data(client);

    struct aws_s3_part_buffer_pool *pool = &client->synced_data.part_buffer_pool;
    struct aws_linked_list *free_list = &pool->free_list;

    if (!aws_linked_list_empty(free_list)) {
        struct aws_linked_list_node *part_buffer_node = aws_linked_list_pop_back(free_list);
        result = AWS_CONTAINER_OF(part_buffer_node, struct aws_s3_part_buffer, node);
    }

    s_s3_client_unlock_synced_data(client);

    if (result != NULL) {
        aws_byte_buf_reset(&result->buffer, true);
    }

    return result;
}

void aws_s3_client_release_part_buffer(struct aws_s3_client *client, struct aws_s3_part_buffer *part_buffer) {
    AWS_PRECONDITION(client);

    s_s3_client_lock_synced_data(client);

    struct aws_s3_part_buffer_pool *pool = &client->synced_data.part_buffer_pool;
    struct aws_linked_list *free_list = &pool->free_list;

    aws_linked_list_push_back(free_list, &part_buffer->node);

    s_s3_client_unlock_synced_data(client);
}

static void s_s3_part_buffer_pool_init(struct aws_s3_part_buffer_pool *pool) {
    pool->num_allocated = 0;
    aws_linked_list_init(&pool->free_list);
}

static void s_s3_client_add_new_part_buffers_to_pool(struct aws_s3_client *client, size_t num_buffers) {
    AWS_PRECONDITION(client);

    struct aws_linked_list stack_list;
    aws_linked_list_init(&stack_list);

    for (size_t buffer_index = 0; buffer_index < num_buffers; ++buffer_index) {
        struct aws_s3_part_buffer *part_buffer =
            aws_mem_calloc(client->allocator, 1, sizeof(struct aws_s3_part_buffer));

        if (part_buffer == NULL) {
            AWS_LOGF_ERROR(AWS_LS_S3_CLIENT, "id=%p Could not allocate additional part buffer", (void *)client);
            return;
        }

        aws_byte_buf_init(&part_buffer->buffer, client->allocator, client->part_size);

        aws_linked_list_push_back(&stack_list, &part_buffer->node);
    }

    s_s3_client_lock_synced_data(client);

    struct aws_s3_part_buffer_pool *pool = &client->synced_data.part_buffer_pool;
    struct aws_linked_list *free_list = &pool->free_list;

    while (!aws_linked_list_empty(&stack_list)) {
        struct aws_linked_list_node *node = aws_linked_list_pop_back(&stack_list);
        aws_linked_list_push_back(free_list, node);
        ++pool->num_allocated;
    }

    s_s3_client_unlock_synced_data(client);
}

static void s_s3_client_destroy_part_buffer_pool_synced(struct aws_s3_client *client) {
    AWS_PRECONDITION(client);

    ASSERT_SYNCED_DATA_LOCK_HELD(client);

    struct aws_s3_part_buffer_pool *pool = &client->synced_data.part_buffer_pool;
    struct aws_linked_list *free_list = &pool->free_list;

    int32_t num_popped = 0;

    while (!aws_linked_list_empty(free_list)) {
        struct aws_linked_list_node *part_buffer_node = aws_linked_list_pop_back(free_list);
        struct aws_s3_part_buffer *part_buffer = AWS_CONTAINER_OF(part_buffer_node, struct aws_s3_part_buffer, node);

        aws_byte_buf_clean_up(&part_buffer->buffer);
        aws_mem_release(client->allocator, part_buffer);
        ++num_popped;
    }

    int32_t num_leaked = pool->num_allocated - num_popped;

    if (num_leaked > 0) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_CLIENT,
            "id=%p Not all part buffers were returned to free list: %d leaked.",
            (void *)client,
            num_leaked);
    }
}
/* END Part Buffer Pool Functions*/

/*  BEGIN Meta Request Functions  */
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

    while (!aws_linked_list_empty(&client->synced_data.idle_vip_connections)) {
        struct aws_linked_list_node *vip_connection_node =
            aws_linked_list_pop_front(&client->synced_data.idle_vip_connections);
        struct aws_s3_vip_connection *vip_connection =
            AWS_CONTAINER_OF(vip_connection_node, struct aws_s3_vip_connection, node);

        aws_linked_list_push_back(&client->synced_data.active_vip_connections, vip_connection_node);

        s_s3_client_vip_connection_process_meta_requests(client, vip_connection);
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

    bool removed_meta_request = false;
    size_t meta_request_index = 0;

    /* Remove the meta request from our own internal list. */
    for (; meta_request_index < aws_array_list_length(&client->synced_data.meta_requests); ++meta_request_index) {
        struct aws_s3_meta_request *meta_request_item = NULL;
        aws_array_list_get_at(&client->synced_data.meta_requests, &meta_request_item, meta_request_index);

        if (meta_request_item == meta_request) {
            aws_array_list_erase(&client->synced_data.meta_requests, meta_request_index);
            aws_s3_meta_request_release(meta_request);

            removed_meta_request = true;
            break;
        }
    }

    if (removed_meta_request && aws_linked_list_empty(&client->synced_data.active_vip_connections)) {
        struct aws_linked_list_node *current_node = aws_linked_list_front(&client->synced_data.active_vip_connections);

        while (current_node != aws_linked_list_end(&client->synced_data.active_vip_connections)) {
            struct aws_s3_vip_connection *vip_connection =
                AWS_CONTAINER_OF(current_node, struct aws_s3_vip_connection, node);

            /* Update our next meta request index if needed. This is just to help keep processing order in tact, but
             * might be unnecessary/overkill. */
            size_t *next_meta_request_index = &vip_connection->next_meta_request_index;

            if (meta_request_index < *next_meta_request_index) {
                --(*next_meta_request_index);
            } else if (meta_request_index > *next_meta_request_index) {
                *next_meta_request_index =
                    *next_meta_request_index % aws_array_list_length(&client->synced_data.meta_requests);
            }

            current_node = aws_linked_list_next(current_node);
        }
    }

    s_s3_client_unlock_synced_data(client);

    aws_s3_meta_request_release(meta_request);
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
/* END Meta Request Functions */

/* BEGIN VIP Functions */
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

    if (s_s3_client_is_cleaning_up(client)) {
        s_s3_client_unlock_synced_data(client);
        return;
    }

    size_t vip_index = s_s3_find_vip(&client->synced_data.vips, aws_byte_cursor_from_string(host_address));

    /* If we didn't find a match in the table, we have a VIP to add! */
    if (vip_index != (size_t)-1) {
        goto error_vip_exists;
    }

    AWS_LOGF_INFO(
        AWS_LS_S3_CLIENT, "id=%p: Creating new VIP for address %s", (void *)client, (const char *)host_address->bytes);

    /* Allocate the new VIP. */
    struct aws_s3_vip vip;
    s_s3_client_vip_init_synced(client, &vip, aws_byte_cursor_from_string(host_address));

    /* TODO Would be cool if we could lengthen the size of the array and initialize directly into memory passed
     * back. */
    if (aws_array_list_push_back(&client->synced_data.vips, &vip)) {
        goto error_push_back_failed;
    }

    s_s3_client_unlock_synced_data(client);

    s_s3_client_add_new_part_buffers_to_pool(client, client->num_connections_per_vip);

    if (host_address != NULL) {
        aws_string_destroy(host_address);
        host_address = NULL;
    }

    return;

error_push_back_failed:

    s_s3_client_lock_synced_data(client);
    s_s3_client_vip_clean_up_synced(client, &vip);
    s_s3_client_unlock_synced_data(client);

error_vip_exists:

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

    size_t vip_index = s_s3_find_vip(&client->synced_data.vips, aws_byte_cursor_from_string(host_address));

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
/* END VIP Functions */

/* BEGIN VIP Connection Functions */
static int s_s3_client_vip_connection_process_meta_requests(
    struct aws_s3_client *client,
    struct aws_s3_vip_connection *vip_connection) {

    if (aws_s3_task_manager_create_task(
            client->task_manager,
            s_s3_client_vip_connection_process_meta_requests_loop_task,
            0,
            2,
            client,
            vip_connection)) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_CLIENT,
            "id=%p: Could not initate processing of meta requests on vip connection.",
            (void *)client);
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}

/* Task function for trying find a request that can be processed. */
static void s_s3_client_vip_connection_process_meta_requests_loop_task(uint32_t num_args, void **args) {
    AWS_PRECONDITION(num_args == 2);
    (void)num_args;

    struct aws_s3_client *client = args[0];
    AWS_PRECONDITION(client);

    struct aws_s3_vip_connection *vip_connection = args[1];
    AWS_PRECONDITION(vip_connection);

    s_s3_client_lock_synced_data(client);

    size_t num_meta_requests = aws_array_list_length(&client->synced_data.meta_requests);

    if (vip_connection->pending_destruction) {
        aws_linked_list_remove(&vip_connection->node);
        s_s3_vip_connection_destroy_synced(client, vip_connection);
        s_s3_client_unlock_synced_data(client);
        return;
    }

    /* If we don't have anything to do, go back to idle state. */
    if (num_meta_requests == 0) {
        vip_connection->next_meta_request_index = 0;

        aws_linked_list_remove(&vip_connection->node);
        aws_linked_list_push_back(&client->synced_data.idle_vip_connections, &vip_connection->node);
        s_s3_client_unlock_synced_data(client);
        return;
    }

    size_t next_meta_request_index = vip_connection->next_meta_request_index;

    /* Index that is relative to the value of next_meta_request_index.*/
    size_t relative_meta_request_index = 0;

    struct aws_s3_meta_request *meta_request = NULL;
    struct aws_s3_request *request = NULL;

    for (; relative_meta_request_index < num_meta_requests; ++relative_meta_request_index) {

        /* From our relative index, grab an actual index. */
        size_t meta_request_index = (relative_meta_request_index + next_meta_request_index) % num_meta_requests;

        aws_array_list_get_at(&client->synced_data.meta_requests, &meta_request, meta_request_index);

        /* Try popping a request from this meta request. */
        request = aws_s3_meta_request_pop_request(meta_request);

        /* If we successfully got one, then go ahead and calculate a new next_meta_request_index value. */
        if (request) {
            next_meta_request_index = (meta_request_index + 1) % num_meta_requests;
            break;
        }
    }

    s_s3_client_unlock_synced_data(client);

    /* Store our new next_meta_reqest_index value if we have a request, or reset it if we couldn't find anything. */
    if (request) {
        vip_connection->next_meta_request_index = next_meta_request_index;
    } else {
        vip_connection->next_meta_request_index = 0;
    }

    if (request) {

        struct aws_s3_vip_connection_make_request_state *make_request_state = &vip_connection->make_request_state;
        make_request_state->client = client;
        make_request_state->vip_connection = vip_connection;
        make_request_state->meta_request = meta_request;
        make_request_state->request = request;

        aws_s3_task_manager_create_task(
            client->task_manager, s_s3_client_vip_connection_request_prepare, 0, 1, make_request_state);

    } else {
        /* If there isn't an s3 request right now, don't completely shutdown--check back in a little bit to see if
         * there is additional work.*/
        uint64_t time_offset_ns = aws_timestamp_convert(
            s_vip_connection_processing_retry_offset_ms, AWS_TIMESTAMP_MILLIS, AWS_TIMESTAMP_NANOS, NULL);

        aws_s3_task_manager_create_task(
            client->task_manager,
            s_s3_client_vip_connection_process_meta_requests_loop_task,
            time_offset_ns,
            2,
            client,
            vip_connection);
    }
}

static void s_s3_client_vip_connection_request_prepare(uint32_t num_args, void **args) {

    AWS_PRECONDITION(num_args == 1)
    (void)num_args;

    struct aws_s3_vip_connection_make_request_state *make_request_state = args[0];

    AWS_PRECONDITION(make_request_state);
    AWS_PRECONDITION(make_request_state->client);
    AWS_PRECONDITION(make_request_state->vip_connection);
    AWS_PRECONDITION(make_request_state->meta_request);
    AWS_PRECONDITION(make_request_state->request);

    if (aws_s3_request_prepare_for_send(make_request_state->request, make_request_state->client)) {
        s_s3_client_vip_connection_end_request(make_request_state, AWS_ERROR_UNKNOWN);
    } else {
        s_s3_client_vip_connection_request_start(make_request_state);
    }
}

/* Start issuing the actual request It is the responsibility of the caller to not call this again until a clean-up
 * callback has been triggered. */
static void s_s3_client_vip_connection_request_start(
    struct aws_s3_vip_connection_make_request_state *make_request_state) {
    AWS_PRECONDITION(make_request_state)
    AWS_PRECONDITION(make_request_state->client);
    AWS_PRECONDITION(make_request_state->vip_connection);
    AWS_PRECONDITION(make_request_state->meta_request);
    AWS_PRECONDITION(make_request_state->request);

    struct aws_s3_client *client = make_request_state->client;
    struct aws_s3_request *request = make_request_state->request;
    struct aws_s3_vip_connection *vip_connection = make_request_state->vip_connection;

    AWS_LOGF_DEBUG(AWS_LS_S3_REQUEST_PIPELINE, "id=%p Making request for VIP connection", (void *)vip_connection);

    int error_code = AWS_ERROR_SUCCESS;

    make_request_state->signable = aws_signable_new_http_request(client->allocator, request->message);

    if (make_request_state->signable == NULL) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_VIP_CONNECTION, "id=%p: Could not allocate signable for http request", (void *)vip_connection);
        error_code = aws_last_error();
        goto error_finish;
    }

    struct aws_date_time now;
    aws_date_time_init_now(&now);

    struct aws_byte_cursor service_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("s3");

    struct aws_signing_config_aws signing_config;
    AWS_ZERO_STRUCT(signing_config);
    signing_config.config_type = AWS_SIGNING_CONFIG_AWS;
    signing_config.algorithm = AWS_SIGNING_ALGORITHM_V4;
    signing_config.credentials_provider = client->credentials_provider;
    signing_config.region = aws_byte_cursor_from_array(client->region->bytes, client->region->len);
    signing_config.service = service_name;
    signing_config.date = now;
    signing_config.signed_body_value = g_aws_signed_body_value_unsigned_payload;
    signing_config.signed_body_header = AWS_SBHT_X_AMZ_CONTENT_SHA256;

    if (aws_sign_request_aws(
            client->allocator,
            make_request_state->signable,
            (struct aws_signing_config_base *)&signing_config,
            s_s3_vip_connection_request_signing_complete,
            make_request_state)) {
        AWS_LOGF_ERROR(AWS_LS_S3_VIP_CONNECTION, "id=%p: Could not sign request", (void *)vip_connection);
        error_code = aws_last_error();
        goto error_finish;
    }

    return;

error_finish:

    s_s3_client_vip_connection_end_request(make_request_state, error_code);
}

static void s_s3_vip_connection_request_signing_complete(
    struct aws_signing_result *result,
    int error_code,
    void *user_data) {
    AWS_PRECONDITION(user_data);

    struct aws_s3_vip_connection_make_request_state *make_request_state = user_data;
    struct aws_s3_client *client = make_request_state->client;
    struct aws_s3_vip_connection *vip_connection = make_request_state->vip_connection;
    struct aws_s3_request *request = make_request_state->request;

    AWS_PRECONDITION(request);

    if (error_code != AWS_OP_SUCCESS) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_VIP_CONNECTION,
            "id=%p: Could not sign request due to error_code %d",
            (void *)vip_connection,
            error_code);

        goto error_finish;
    }

    if (aws_apply_signing_result_to_http_request(request->message, client->allocator, result)) {
        error_code = aws_last_error();
        AWS_LOGF_ERROR(
            AWS_LS_S3_REQUEST_PIPELINE,
            "id=%p: Could not apply signing result to http request due to error %d",
            (void *)vip_connection,
            error_code);

        goto error_finish;
    }

    s_s3_client_vip_connection_acquire_request_connection(make_request_state);

    return;

error_finish:

    s_s3_client_vip_connection_end_request(make_request_state, error_code);
}

static void s_s3_client_vip_connection_acquire_request_connection(
    struct aws_s3_vip_connection_make_request_state *make_request_state) {
    AWS_PRECONDITION(make_request_state);

    struct aws_s3_vip_connection *vip_connection = make_request_state->vip_connection;

    struct aws_http_connection **http_connection = &vip_connection->make_request_state.http_connection;
    uint32_t *connection_request_count = &vip_connection->make_request_state.request_count;

    if (*http_connection != NULL) {
        /* If we're at the max request count, set us up to get a new connection.  Also close the original connection so
         * that the connection manager doesn't reuse it.*/
        /* TODO maybe find a more visible way of preventing the
         * connection from going back into the pool. */
        if (*connection_request_count == s_s3_max_request_count_per_connection) {
            aws_http_connection_close(*http_connection);
            aws_http_connection_manager_release_connection(vip_connection->http_connection_manager, *http_connection);

            *http_connection = NULL;
            *connection_request_count = 0;
        } else if (!aws_http_connection_is_open(*http_connection)) {
            /* If our connection is closed for some reason, also get rid of it.*/
            aws_http_connection_manager_release_connection(vip_connection->http_connection_manager, *http_connection);

            *http_connection = NULL;
            *connection_request_count = 0;
        }
    }

    if (*http_connection != NULL) {
        s_s3_client_vip_connection_on_acquire_request_connection(
            *http_connection, AWS_ERROR_SUCCESS, make_request_state);
    } else {
        aws_http_connection_manager_acquire_connection(
            vip_connection->http_connection_manager,
            s_s3_client_vip_connection_on_acquire_request_connection,
            make_request_state);
    }
}

static void s_s3_client_vip_connection_on_acquire_request_connection(
    struct aws_http_connection *incoming_http_connection,
    int error_code,
    void *user_data) {

    AWS_PRECONDITION(user_data);

    struct aws_s3_vip_connection_make_request_state *make_request_state = user_data;
    struct aws_s3_vip_connection *vip_connection = make_request_state->vip_connection;
    struct aws_s3_request *request = make_request_state->request;

    if (error_code != AWS_ERROR_SUCCESS || incoming_http_connection == NULL) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_VIP_CONNECTION,
            "id=%p: Could not acquire connection due to error code %d (%s)",
            (void *)vip_connection,
            error_code,
            aws_error_str(error_code));
        goto error_finish;
    }

    struct aws_http_connection **current_http_connection = &vip_connection->make_request_state.http_connection;

    /* If our cached connection is not equal to the one we just received, switch to the received one. */
    if (*current_http_connection != incoming_http_connection) {

        if (*current_http_connection != NULL) {

            aws_http_connection_manager_release_connection(
                vip_connection->http_connection_manager, *current_http_connection);

            *current_http_connection = NULL;
        }

        *current_http_connection = incoming_http_connection;
        vip_connection->make_request_state.request_count = 0;
    }

    if (aws_s3_request_make_request(
            request,
            *current_http_connection,
            s_s3_client_vip_connection_request_finished_callback,
            make_request_state)) {
        goto error_finish;
    }

    return;

error_finish:
    s_s3_client_vip_connection_end_request(make_request_state, error_code);
}

static void s_s3_client_vip_connection_request_finished_callback(
    struct aws_s3_request *request,
    struct aws_http_stream *stream,
    int error_code) {
    AWS_PRECONDITION(request);
    AWS_PRECONDITION(request->user_data);
    AWS_PRECONDITION(stream);
    (void)request;

    struct aws_s3_vip_connection_make_request_state *make_request_state = request->user_data;
    make_request_state->http_stream = stream;

    s_s3_client_vip_connection_end_request(make_request_state, error_code);
}

static void s_s3_client_vip_connection_end_request(
    struct aws_s3_vip_connection_make_request_state *make_request_state,
    int error_code) {
    AWS_PRECONDITION(make_request_state);

    struct aws_s3_vip_connection *vip_connection = make_request_state->vip_connection;
    struct aws_s3_client *client = make_request_state->client;
    struct aws_s3_request *request = make_request_state->request;
    struct aws_http_stream *stream = make_request_state->http_stream;
    struct aws_s3_meta_request *meta_request = make_request_state->meta_request;

    int response_status = 0;

    if (error_code != AWS_ERROR_SUCCESS) {
        AWS_LOGF_DEBUG(
            AWS_LS_S3_VIP_CONNECTION,
            "id=%p VIP Connection finished processing request with error code %d (%s)",
            (void *)vip_connection,
            error_code,
            aws_error_str(error_code));
    } else if (stream == NULL) {
        error_code = AWS_ERROR_UNKNOWN;
    } else if (aws_http_stream_get_incoming_response_status(stream, &response_status)) {
        error_code = aws_last_error();
    } else if (
        response_status == AWS_S3_RESPONSE_STATUS_SUCCESS || response_status == AWS_S3_RESPONSE_STATUS_RANGE_SUCCESS) {
        /* Nothing to do for success respone status*/
    } else if (response_status == AWS_S3_RESPONSE_STATUS_INTERNAL_ERROR) {
        error_code = AWS_ERROR_S3_INTERNAL_ERROR;
        aws_raise_error(AWS_ERROR_S3_INTERNAL_ERROR);
    } else {
        error_code = AWS_ERROR_S3_INVALID_RESPONSE_STATUS;
        aws_raise_error(AWS_ERROR_S3_INVALID_RESPONSE_STATUS);
    }

    ++vip_connection->make_request_state.request_count;

    aws_s3_meta_request_finish_request(meta_request, request, error_code);

    if (make_request_state->http_stream != NULL) {
        aws_http_stream_release(make_request_state->http_stream);
        make_request_state->http_stream = NULL;
    }

    if (make_request_state->signable != NULL) {
        aws_signable_destroy(make_request_state->signable);
        make_request_state->signable = NULL;
    }

    make_request_state->meta_request = NULL;
    make_request_state->request = NULL;

    void *args[] = {client, vip_connection};

    s_s3_client_vip_connection_process_meta_requests_loop_task(2, args);
}
/* END VIP Connection Functions */

/* BEGIN TEMP Host Resolver Functions */
/* These are temporary hacks; the following section is a temporary hack for listening to host resolution events that
 * have been hacked in via a branch of aws-c-io. */
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

    bool already_resolving_hosts = aws_atomic_exchange_int(&client->resolving_hosts, 1) == 1;

    if (already_resolving_hosts) {
        return AWS_OP_SUCCESS;
    }

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

    uint64_t already_stopped_resolving_hosts = aws_atomic_exchange_int(&client->resolving_hosts, 0) == 0;

    if (already_stopped_resolving_hosts) {
        return;
    }

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
/* END TEMP Host Resolver Functions */
