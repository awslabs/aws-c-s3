/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_client_impl.h"
#include "aws/s3/private/s3_meta_request_impl.h"
#include "aws/s3/private/s3_util.h"

#include <aws/auth/credentials.h>
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
#include <aws/io/tls_channel_handler.h>

#include <inttypes.h>
#include <math.h>

static const uint32_t s_s3_max_request_count_per_connection = 100;

static const uint16_t s_http_port = 80;
static const uint16_t s_https_port = 443;

static const uint64_t s_default_part_size = 20 * 1024 * 1024;
static const size_t s_default_dns_host_address_ttl_seconds = 2 * 60;
static const uint32_t s_default_connection_timeout_ms = 3000;
static const double s_default_throughput_target_gbps = 5.0;
static const double s_default_throughput_per_vip_gbps = 6.25; // TODO provide analysis on how we reached this constant.
static const uint32_t s_default_num_connections_per_vip = 10;
static const uint32_t s_default_max_retries = 16;

struct aws_s3_client_work {
    struct aws_linked_list_node node;
    void *user_data;
};

static void s_s3_client_lock_synced_data(struct aws_s3_client *client);
static void s_s3_client_unlock_synced_data(struct aws_s3_client *client);

static void s_s3_client_start_destroy(void *user_data);
static void s_s3_client_finish_destroy(void *user_data);

/* Interfaces with "internal" reference count.  For more info, see the comments by the internal_ref_count variable in
 * the header. */
static void s_s3_client_internal_acquire(struct aws_s3_client *client);
static void s_s3_client_internal_release(struct aws_s3_client *client);

static void s_s3_client_vip_http_connection_manager_shutdown_callback(void *user_data);

/* Initializes/cleans up a VIP structure.  Both assume the lock is already held.  */
static struct aws_s3_vip *s_s3_client_vip_new(struct aws_s3_client *client, const struct aws_byte_cursor *host_address);
static void s_s3_client_vip_destroy(struct aws_s3_vip *vip);
static void s_s3_client_vip_finish_destroy(void *user_data);

/* Allocates/Destroy a VIP Connection structure. */
struct aws_s3_vip_connection *aws_s3_vip_connection_new(struct aws_s3_client *client, struct aws_s3_vip *vip);
void s_s3_vip_connection_destroy(struct aws_s3_client *client, struct aws_s3_vip_connection *vip_connection);

/* TODO add option to grab work memory from a pool */
static struct aws_s3_client_work *s_s3_client_work_new(struct aws_s3_client *client, void *user_data) {
    AWS_PRECONDITION(client);

    struct aws_s3_client_work *work = aws_mem_calloc(client->allocator, 1, sizeof(struct aws_s3_client_work));
    work->user_data = user_data;

    return work;
}

static void s_s3_client_work_destroy(struct aws_s3_client *client, struct aws_s3_client_work *work) {
    AWS_PRECONDITION(client);

    if (work == NULL) {
        return;
    }

    aws_mem_release(client->allocator, work);
}

static void s_s3_client_work_destroy_list(struct aws_s3_client *client, struct aws_linked_list *work_list) {
    while (!aws_linked_list_empty(work_list)) {
        struct aws_linked_list_node *work_node = aws_linked_list_pop_back(work_list);
        struct aws_s3_client_work *work = AWS_CONTAINER_OF(work_node, struct aws_s3_client_work, node);

        s_s3_client_work_destroy(client, work);
    }
}

static struct aws_s3_vip *s_s3_find_vip(
    const struct aws_linked_list *vip_list,
    const struct aws_byte_cursor *host_address);

static void s_s3_part_buffer_pool_init(struct aws_s3_part_buffer_pool *pool);
static void s_s3_client_add_new_part_buffers_to_pool_synced(struct aws_s3_client *client, const size_t num_buffers);
static void s_s3_client_destroy_part_buffer_pool(struct aws_s3_client *client);

static struct aws_s3_meta_request *s_s3_client_meta_request_factory(
    struct aws_s3_client *client,
    const struct aws_s3_meta_request_options *options);

static int s_s3_client_add_vips(struct aws_s3_client *client, const struct aws_array_list *host_addresses);

static void s_s3_client_schedule_meta_request_work(
    struct aws_s3_client *client,
    struct aws_s3_meta_request *meta_request);

static void s_s3_client_schedule_process_work_task_synced(struct aws_s3_client *client);

static void s_s3_client_process_work_task(struct aws_task *task, void *arg, enum aws_task_status task_status);

/* Callback for when a single request (not an entire meta request) has finished on a VIP connection. */
static void s_s3_client_vip_connection_request_finished(void *user_data);

/* Handles getting an HTTP connection for the caller, given the vip_connection reference. */
static int s_s3_client_get_http_connection(
    struct aws_s3_client *client,
    struct aws_s3_vip_connection *vip_connection,
    aws_s3_client_get_http_connection_callback *callback,
    void *user_data);

static void s_s3_client_vip_connection_on_acquire_request_connection(
    struct aws_http_connection *http_connection,
    int error_code,
    void *user_data);

static int s_s3_client_start_resolving_addresses_synced(struct aws_s3_client *client);

static void s_s3_client_lock_synced_data(struct aws_s3_client *client) {
    aws_mutex_lock(&client->synced_data.lock);
}

static void s_s3_client_unlock_synced_data(struct aws_s3_client *client) {
    aws_mutex_unlock(&client->synced_data.lock);
}

static struct aws_s3_client_vtable s_s3_client_default_vtable = {
    .meta_request_factory = s_s3_client_meta_request_factory,
    .schedule_meta_request_work = s_s3_client_schedule_meta_request_work,
    .get_http_connection = s_s3_client_get_http_connection,
};

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

    /* Cannot be less than zero.  If zero, use default. */
    if (client_config->throughput_target_gbps < 0.0) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_CLIENT,
            "Cannot create client from client_config; throughput_target_gbps cannot less than or equal to 0.");
        aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        return NULL;
    }

    /* Cannot be less than zero.  If zero, use default. */
    if (client_config->throughput_per_vip_gbps < 0.0) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_CLIENT,
            "Cannot create client from client_config; throughput_per_vip_gbps cannot less than or equal to 0.");
        aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        return NULL;
    }

    struct aws_s3_client *client = aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_client));

    client->allocator = allocator;
    client->vtable = &s_s3_client_default_vtable;

    aws_ref_count_init(&client->ref_count, client, (aws_simple_completion_callback *)s_s3_client_start_destroy);
    aws_ref_count_init(
        &client->internal_ref_count, client, (aws_simple_completion_callback *)s_s3_client_finish_destroy);

    /* Store our client bootstrap. */
    client->client_bootstrap = client_config->client_bootstrap;
    aws_client_bootstrap_acquire(client_config->client_bootstrap);

    aws_event_loop_group_acquire(client_config->client_bootstrap->event_loop_group);
    client->event_loop = aws_event_loop_group_get_next_loop(client_config->client_bootstrap->event_loop_group);

    /* Make a copy of the region string. */
    client->region = aws_string_new_from_array(allocator, client_config->region.ptr, client_config->region.len);

    if (client_config->part_size != 0) {
        *((uint64_t *)&client->part_size) = client_config->part_size;
    } else {
        *((uint64_t *)&client->part_size) = s_default_part_size;
    }

    if (client_config->connection_timeout_ms != 0) {
        *((uint32_t *)&client->connection_timeout_ms) = client_config->connection_timeout_ms;
    } else {
        *((uint32_t *)&client->connection_timeout_ms) = s_default_connection_timeout_ms;
    }

    if (client_config->tls_connection_options != NULL) {

        client->tls_connection_options =
            aws_mem_calloc(client->allocator, 1, sizeof(struct aws_tls_connection_options));

        aws_tls_connection_options_copy(client->tls_connection_options, client_config->tls_connection_options);
    }

    if (client_config->throughput_target_gbps != 0.0) {
        *((double *)&client->throughput_target_gbps) = client_config->throughput_target_gbps;
    } else {
        *((double *)&client->throughput_target_gbps) = s_default_throughput_target_gbps;
    }

    if (client_config->throughput_per_vip_gbps != 0.0) {
        *((double *)&client->throughput_per_vip_gbps) = client_config->throughput_per_vip_gbps;
    } else {
        *((double *)&client->throughput_per_vip_gbps) = s_default_throughput_per_vip_gbps;
    }

    if (client_config->num_connections_per_vip != 0) {
        *((uint32_t *)&client->num_connections_per_vip) = client_config->num_connections_per_vip;
    } else {
        *((uint32_t *)&client->num_connections_per_vip) = s_default_num_connections_per_vip;
    }

    /* Determine how many vips are ideal by dividing target-throughput by throughput-per-vip. */
    {
        double ideal_vip_count_double = client->throughput_target_gbps / client->throughput_per_vip_gbps;
        *((uint32_t *)&client->ideal_vip_count) = (uint32_t)ceil(ideal_vip_count_double);
    }

    if (client_config->signing_config) {
        client->cached_signing_config = aws_cached_signing_config_new(client->allocator, client_config->signing_config);
    }

    aws_mutex_init(&client->synced_data.lock);

    aws_linked_list_init(&client->synced_data.vips);
    aws_linked_list_init(&client->synced_data.pending_vip_connection_updates);
    aws_linked_list_init(&client->synced_data.pending_vip_connection_removals);
    aws_linked_list_init(&client->synced_data.pending_meta_requests);

    aws_linked_list_init(&client->threaded_data.idle_vip_connections);
    aws_linked_list_init(&client->threaded_data.meta_requests);

    s_s3_part_buffer_pool_init(&client->synced_data.part_buffer_pool);

    if (client_config->retry_strategy != NULL) {
        aws_retry_strategy_acquire(client_config->retry_strategy);
        client->retry_strategy = client_config->retry_strategy;
    } else {
        struct aws_exponential_backoff_retry_options retry_options = {
            .el_group = client_config->client_bootstrap->event_loop_group,
            .max_retries = s_default_max_retries,
        };

        client->retry_strategy = aws_retry_strategy_new_exponential_backoff(allocator, &retry_options);
    }

    /* Initialize shutdown options and tracking. */
    client->shutdown_callback = client_config->shutdown_callback;
    client->shutdown_callback_user_data = client_config->shutdown_callback_user_data;

    return client;
}

void aws_s3_client_acquire(struct aws_s3_client *client) {
    AWS_PRECONDITION(client);

    aws_ref_count_acquire(&client->ref_count);
}

void aws_s3_client_release(struct aws_s3_client *client) {
    if (client == NULL) {
        return;
    }

    aws_ref_count_release(&client->ref_count);
}

void aws_s3_client_schedule_meta_request_work(struct aws_s3_client *client, struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(client->vtable);
    AWS_PRECONDITION(client->vtable->schedule_meta_request_work);

    client->vtable->schedule_meta_request_work(client, meta_request);
}

int aws_s3_client_get_http_connection(
    struct aws_s3_client *client,
    struct aws_s3_vip_connection *vip_connection,
    aws_s3_client_get_http_connection_callback *callback,
    void *user_data) {

    AWS_PRECONDITION(client);
    AWS_PRECONDITION(client->vtable);
    AWS_PRECONDITION(client->vtable->get_http_connection);

    return client->vtable->get_http_connection(client, vip_connection, callback, user_data);
}

static void s_s3_client_start_destroy(void *user_data) {
    struct aws_s3_client *client = user_data;
    AWS_PRECONDITION(client);

    struct aws_linked_list local_vip_list;
    aws_linked_list_init(&local_vip_list);

    s_s3_client_lock_synced_data(client);

    client->synced_data.cleaning_up = true;

    if (client->synced_data.host_listener != NULL) {
        aws_host_resolver_remove_host_listener(
            client->client_bootstrap->host_resolver, client->synced_data.host_listener);
        client->synced_data.host_listener = NULL;
    }

    /* Swap out all VIP's so that we can clean them up outside of the lock. */
    aws_linked_list_swap_contents(&local_vip_list, &client->synced_data.vips);
    client->synced_data.vip_count = 0;

    s_s3_client_unlock_synced_data(client);

    /* Iterate through the local list, removing each VIP. */
    while (!aws_linked_list_empty(&local_vip_list)) {
        struct aws_linked_list_node *vip_node = aws_linked_list_pop_back(&local_vip_list);

        struct aws_s3_vip *vip = AWS_CONTAINER_OF(vip_node, struct aws_s3_vip, node);

        s_s3_client_vip_destroy(vip);
        vip = NULL;
    }

    /* Release the initial internal ref count that we have held since allocation. */
    s_s3_client_internal_release(client);
}

static void s_s3_client_internal_acquire(struct aws_s3_client *client) {
    AWS_PRECONDITION(client);

    aws_ref_count_acquire(&client->internal_ref_count);
}

static void s_s3_client_internal_release(struct aws_s3_client *client) {
    AWS_PRECONDITION(client);

    aws_ref_count_release(&client->internal_ref_count);
}

/* Called once all internal references have been released. */
static void s_s3_client_finish_destroy(void *user_data) {

    struct aws_s3_client *client = user_data;
    AWS_PRECONDITION(client);

    aws_string_destroy(client->region);
    client->region = NULL;

    aws_string_destroy(client->synced_data.endpoint);
    client->synced_data.endpoint = NULL;

    if (client->tls_connection_options) {
        aws_tls_connection_options_clean_up(client->tls_connection_options);
        aws_mem_release(client->allocator, client->tls_connection_options);
        client->tls_connection_options = NULL;
    }

    aws_mutex_clean_up(&client->synced_data.lock);

    AWS_ASSERT(aws_linked_list_empty(&client->synced_data.pending_vip_connection_updates));
    AWS_ASSERT(aws_linked_list_empty(&client->synced_data.pending_vip_connection_removals));
    AWS_ASSERT(aws_linked_list_empty(&client->synced_data.pending_meta_requests));

    AWS_ASSERT(aws_linked_list_empty(&client->threaded_data.idle_vip_connections));
    AWS_ASSERT(aws_linked_list_empty(&client->threaded_data.meta_requests));

    s_s3_client_destroy_part_buffer_pool(client);

    aws_retry_strategy_release(client->retry_strategy);

    aws_event_loop_group_release(client->client_bootstrap->event_loop_group);

    aws_client_bootstrap_release(client->client_bootstrap);
    aws_cached_signing_config_destroy(client->cached_signing_config);

    aws_s3_client_shutdown_complete_callback_fn *shutdown_callback = client->shutdown_callback;
    void *shutdown_user_data = client->shutdown_callback_user_data;

    aws_mem_release(client->allocator, client);
    client = NULL;

    if (shutdown_callback != NULL) {
        shutdown_callback(shutdown_user_data);
    }
}

static void s_s3_client_vip_http_connection_manager_shutdown_callback(void *user_data) {
    AWS_PRECONDITION(user_data);

    struct aws_s3_client *client = user_data;
    AWS_PRECONDITION(client);

    s_s3_client_internal_release(client);
}

/* Initialize a new VIP structure for the client to use, given an address. Assumes lock is held. */
static struct aws_s3_vip *s_s3_client_vip_new(
    struct aws_s3_client *client,
    const struct aws_byte_cursor *host_address) {
    AWS_PRECONDITION(client);

    struct aws_s3_vip *vip = aws_mem_calloc(client->allocator, 1, sizeof(struct aws_s3_vip));

    aws_ref_count_init(&vip->internal_ref_count, vip, s_s3_client_vip_finish_destroy);

    vip->owning_client = client;
    s_s3_client_internal_acquire(client);

    /* Copy over the host address. */
    vip->host_address = aws_string_new_from_array(client->allocator, host_address->ptr, host_address->len);

    /* Try to set up an HTTP connection manager. */
    struct aws_socket_options socket_options;
    AWS_ZERO_STRUCT(socket_options);
    socket_options.type = AWS_SOCKET_STREAM;
    socket_options.domain = AWS_SOCKET_IPV4;
    socket_options.connect_timeout_ms = client->connection_timeout_ms;

    struct aws_http_connection_manager_options manager_options;
    AWS_ZERO_STRUCT(manager_options);
    manager_options.bootstrap = client->client_bootstrap;
    manager_options.initial_window_size = SIZE_MAX;
    manager_options.socket_options = &socket_options;
    manager_options.proxy_options = NULL;
    manager_options.host = aws_byte_cursor_from_string(vip->host_address);
    manager_options.max_connections = client->num_connections_per_vip * 2;
    manager_options.shutdown_complete_callback = s_s3_client_vip_http_connection_manager_shutdown_callback;
    manager_options.shutdown_complete_user_data = client;
    manager_options.tls_connection_options = client->tls_connection_options;

    if (manager_options.tls_connection_options != NULL) {
        manager_options.port = s_https_port;
    } else {
        manager_options.port = s_http_port;
    }

    vip->http_connection_manager = aws_http_connection_manager_new(client->allocator, &manager_options);

    if (vip->http_connection_manager == NULL) {
        AWS_LOGF_ERROR(AWS_LS_S3_VIP, "id=%p: Could not allocate aws_s3_vip connection manager.", (void *)vip);
        goto error_clean_up;
    }

    aws_linked_list_init(&vip->vip_connections);

    /* Acquire internal reference for the HTTP Connection Manager. */
    s_s3_client_internal_acquire(client);

    return vip;

error_clean_up:

    if (vip != NULL) {
        s_s3_client_vip_destroy(vip);
        vip = NULL;
    }

    return NULL;
}

/* Releases the memory for a vip structure. Assumes lock is held. */
static void s_s3_client_vip_destroy(struct aws_s3_vip *vip) {
    AWS_PRECONDITION(vip);

    struct aws_s3_client *client = vip->owning_client;
    s_s3_client_lock_synced_data(client);

    while (!aws_linked_list_empty(&vip->vip_connections)) {
        struct aws_linked_list_node *vip_connection_node = aws_linked_list_pop_back(&vip->vip_connections);

        struct aws_s3_vip_connection *vip_connection =
            AWS_CONTAINER_OF(vip_connection_node, struct aws_s3_vip_connection, synced_data);

        struct aws_s3_client_work *work = s_s3_client_work_new(client, vip_connection);

        aws_linked_list_push_back(&client->synced_data.pending_vip_connection_removals, &work->node);
    }

    s_s3_client_schedule_process_work_task_synced(client);

    s_s3_client_unlock_synced_data(client);

    aws_ref_count_release(&vip->internal_ref_count);
    vip = NULL;
}

static void s_s3_client_vip_finish_destroy(void *user_data) {
    struct aws_s3_vip *vip = user_data;
    AWS_PRECONDITION(vip);

    /* Release the VIP's reference to it's connection manager. */
    aws_http_connection_manager_release(vip->http_connection_manager);
    vip->http_connection_manager = NULL;

    /* Clean up the address string. */
    aws_string_destroy(vip->host_address);
    vip->host_address = NULL;

    struct aws_s3_client *client = vip->owning_client;
    aws_mem_release(client->allocator, vip);

    s_s3_client_internal_release(client);
}

/* Allocate a new VIP Connection structure for a given VIP. */
struct aws_s3_vip_connection *aws_s3_vip_connection_new(struct aws_s3_client *client, struct aws_s3_vip *vip) {
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(vip);

    struct aws_s3_vip_connection *vip_connection =
        aws_mem_calloc(client->allocator, 1, sizeof(struct aws_s3_vip_connection));

    vip_connection->owning_vip = vip;
    aws_ref_count_acquire(&vip->internal_ref_count);

    aws_http_connection_manager_acquire(vip->http_connection_manager);

    /* Acquire internal reference for our VIP Connection structure. */
    s_s3_client_internal_acquire(client);

    return vip_connection;
}

/* Destroy a VIP Connection structure. */
void s_s3_vip_connection_destroy(struct aws_s3_client *client, struct aws_s3_vip_connection *vip_connection) {

    if (client == NULL || vip_connection == NULL) {
        return;
    }

    struct aws_s3_vip *owning_vip = vip_connection->owning_vip;

    if (owning_vip->http_connection_manager != NULL) {
        if (vip_connection->http_connection != NULL) {
            aws_http_connection_manager_release_connection(
                owning_vip->http_connection_manager, vip_connection->http_connection);

            vip_connection->http_connection = NULL;
        }
        aws_http_connection_manager_release(owning_vip->http_connection_manager);
    }

    aws_ref_count_release(&owning_vip->internal_ref_count);
    owning_vip = NULL;
    vip_connection->owning_vip = NULL;

    aws_s3_meta_request_release(vip_connection->threaded_data.meta_request);
    vip_connection->threaded_data.meta_request = NULL;

    aws_mem_release(client->allocator, vip_connection);

    s_s3_client_internal_release(client);
}

static struct aws_s3_vip *s_s3_find_vip(
    const struct aws_linked_list *vip_list,
    const struct aws_byte_cursor *host_address) {
    AWS_PRECONDITION(vip_list);

    if (aws_linked_list_empty(vip_list)) {
        return false;
    }

    struct aws_linked_list_node *vip_node = aws_linked_list_begin(vip_list);

    while (vip_node != aws_linked_list_end(vip_list)) {
        struct aws_s3_vip *vip = AWS_CONTAINER_OF(vip_node, struct aws_s3_vip, node);

        struct aws_byte_cursor vip_host_address = aws_byte_cursor_from_string(vip->host_address);

        if (aws_byte_cursor_eq(host_address, &vip_host_address)) {
            return vip;
        }

        vip_node = aws_linked_list_next(vip_node);
    }

    return NULL;
}

struct aws_s3_part_buffer *aws_s3_client_get_part_buffer(struct aws_s3_client *client, uint32_t part_number) {
    AWS_PRECONDITION(client);

    struct aws_s3_part_buffer *result = NULL;

    s_s3_client_lock_synced_data(client);

    struct aws_s3_part_buffer_pool *pool = &client->synced_data.part_buffer_pool;
    struct aws_linked_list *free_list = &pool->free_list;

    /* Grab a part buffer from our free list if we have one. */
    if (!aws_linked_list_empty(free_list)) {
        struct aws_linked_list_node *part_buffer_node = aws_linked_list_pop_back(free_list);
        result = AWS_CONTAINER_OF(part_buffer_node, struct aws_s3_part_buffer, node);
    }

    s_s3_client_unlock_synced_data(client);

    if (result != NULL) {
        result->client = client;
        aws_s3_client_acquire(client);

        if (part_number > 0) {
            result->range_start = (part_number - 1) * client->part_size;
        } else {
            result->range_start = 0;
        }

        aws_byte_buf_reset(&result->buffer, false);
    }

    return result;
}

void aws_s3_part_buffer_release(struct aws_s3_part_buffer *part_buffer) {
    if (part_buffer == NULL) {
        return;
    }

    AWS_PRECONDITION(part_buffer->client);

    struct aws_s3_client *client = part_buffer->client;

    s_s3_client_lock_synced_data(client);

    struct aws_s3_part_buffer_pool *pool = &client->synced_data.part_buffer_pool;
    struct aws_linked_list *free_list = &pool->free_list;

    aws_linked_list_push_back(free_list, &part_buffer->node);

    part_buffer->client = NULL;

    s_s3_client_unlock_synced_data(client);

    aws_s3_client_release(client);
}

static void s_s3_part_buffer_pool_init(struct aws_s3_part_buffer_pool *pool) {
    pool->num_allocated = 0;
    aws_linked_list_init(&pool->free_list);
}

static void s_s3_client_add_new_part_buffers_to_pool_synced(struct aws_s3_client *client, const size_t num_buffers) {
    AWS_PRECONDITION(client);

    ASSERT_SYNCED_DATA_LOCK_HELD(client);

    struct aws_s3_part_buffer_pool *pool = &client->synced_data.part_buffer_pool;
    struct aws_linked_list *free_list = &pool->free_list;

    for (size_t buffer_index = 0; buffer_index < num_buffers; ++buffer_index) {
        struct aws_s3_part_buffer *part_buffer =
            aws_mem_calloc(client->allocator, 1, sizeof(struct aws_s3_part_buffer));

        aws_byte_buf_init(&part_buffer->buffer, client->allocator, client->part_size);

        aws_linked_list_push_back(free_list, &part_buffer->node);
        ++pool->num_allocated;
    }
}

static void s_s3_client_destroy_part_buffer_pool(struct aws_s3_client *client) {
    AWS_PRECONDITION(client);

    struct aws_s3_part_buffer_pool *pool = &client->synced_data.part_buffer_pool;
    struct aws_linked_list *free_list = &pool->free_list;

    size_t num_popped = 0;

    while (!aws_linked_list_empty(free_list)) {
        struct aws_linked_list_node *part_buffer_node = aws_linked_list_pop_back(free_list);
        struct aws_s3_part_buffer *part_buffer = AWS_CONTAINER_OF(part_buffer_node, struct aws_s3_part_buffer, node);

        aws_byte_buf_clean_up(&part_buffer->buffer);
        aws_mem_release(client->allocator, part_buffer);
        ++num_popped;
    }

    AWS_ASSERT(pool->num_allocated == num_popped);
}

/* Public facing make-meta-request function. */
struct aws_s3_meta_request *aws_s3_client_make_meta_request(
    struct aws_s3_client *client,
    const struct aws_s3_meta_request_options *options) {

    AWS_LOGF_INFO(AWS_LS_S3_CLIENT, "id=%p Initiating making of meta request", (void *)client);

    AWS_PRECONDITION(client);
    AWS_PRECONDITION(client->vtable);
    AWS_PRECONDITION(client->vtable->meta_request_factory);
    AWS_PRECONDITION(options);

    if (options->type != AWS_S3_META_REQUEST_TYPE_DEFAULT && options->type != AWS_S3_META_REQUEST_TYPE_GET_OBJECT &&
        options->type != AWS_S3_META_REQUEST_TYPE_PUT_OBJECT) {
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

    struct aws_http_headers *message_headers = aws_http_message_get_headers(options->message);

    if (message_headers == NULL) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_CLIENT,
            "id=%p Cannot create meta s3 request; message provided in options does not contain headers.",
            (void *)client);
        aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        return NULL;
    }

    struct aws_byte_cursor host_header_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Host");
    struct aws_byte_cursor host_header_value;

    if (aws_http_headers_get(message_headers, host_header_name, &host_header_value)) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_CLIENT,
            "id=%p Cannot create meta s3 request; message provided in options does not have a 'Host' header.",
            (void *)client);
        aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        return NULL;
    }

    s_s3_client_lock_synced_data(client);

    /* TODO This is temporary until we add multiple bucket support. */
    if (client->synced_data.endpoint == NULL) {
        client->synced_data.endpoint =
            aws_string_new_from_array(client->allocator, host_header_value.ptr, host_header_value.len);

        if (s_s3_client_start_resolving_addresses_synced(client)) {
            AWS_LOGF_ERROR(
                AWS_LS_S3_CLIENT, "id=%p: Could not start resolving endpoint for meta request.", (void *)client);
            return NULL;
        }
    }

    s_s3_client_unlock_synced_data(client);

    struct aws_s3_meta_request *meta_request = client->vtable->meta_request_factory(client, options);

    if (meta_request == NULL) {
        AWS_LOGF_ERROR(AWS_LS_S3_CLIENT, "id=%p: Could not create new meta request.", (void *)client);
        return NULL;
    }

    aws_s3_client_schedule_meta_request_work(client, meta_request);

    AWS_LOGF_INFO(AWS_LS_S3_CLIENT, "id=%p: Created meta request %p", (void *)client, (void *)meta_request);

    return meta_request;
}

static struct aws_s3_meta_request *s_s3_client_meta_request_factory(
    struct aws_s3_client *client,
    const struct aws_s3_meta_request_options *options) {
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(options);

    /* TODO just pass in client? */
    struct aws_s3_meta_request_internal_options internal_options;
    AWS_ZERO_STRUCT(internal_options);
    internal_options.options = options;
    internal_options.client = client;

    /* Call the appropriate meta-request new function. */
    if (options->type == AWS_S3_META_REQUEST_TYPE_GET_OBJECT) {
        return aws_s3_meta_request_auto_ranged_get_new(client->allocator, &internal_options);
    } else if (options->type == AWS_S3_META_REQUEST_TYPE_PUT_OBJECT) {
        return aws_s3_meta_request_auto_ranged_put_new(client->allocator, &internal_options);
    } else if (options->type == AWS_S3_META_REQUEST_TYPE_DEFAULT) {
        return aws_s3_meta_request_default_new(client->allocator, &internal_options);
    } else {
        AWS_FATAL_ASSERT(false);
    }

    return NULL;
}

static void s_s3_client_schedule_meta_request_work(
    struct aws_s3_client *client,
    struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(meta_request);

    s_s3_client_lock_synced_data(client);

    aws_s3_meta_request_acquire(meta_request);

    struct aws_s3_client_work *work = s_s3_client_work_new(client, meta_request);
    aws_linked_list_push_back(&client->synced_data.pending_meta_requests, &work->node);

    s_s3_client_schedule_process_work_task_synced(client);

    s_s3_client_unlock_synced_data(client);
}

static int s_s3_client_add_vips(struct aws_s3_client *client, const struct aws_array_list *host_addresses) {
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(host_addresses);

    struct aws_s3_vip *vip = NULL;
    int result = AWS_OP_SUCCESS;

    s_s3_client_lock_synced_data(client);

    if (client->synced_data.cleaning_up) {
        goto unlock;
    }

    for (size_t address_index = 0; address_index < aws_array_list_length(host_addresses); ++address_index) {

        if (client->synced_data.vip_count >= client->ideal_vip_count) {
            break;
        }

        struct aws_host_address *host_address = NULL;

        aws_array_list_get_at_ptr(host_addresses, (void **)&host_address, address_index);

        /* For now, only support ipv4 addresses. */
        if (host_address->record_type != AWS_ADDRESS_RECORD_TYPE_A) {
            continue;
        }

        struct aws_byte_cursor host_address_byte_cursor = aws_byte_cursor_from_string(host_address->address);

        /* If we didn't find a match in the table, we have a VIP to add! */
        if (s_s3_find_vip(&client->synced_data.vips, &host_address_byte_cursor) != NULL) {
            continue;
        }

        AWS_LOGF_INFO(
            AWS_LS_S3_CLIENT,
            "id=%p Initiating creation of VIP with address '%s'",
            (void *)client,
            (const char *)host_address_byte_cursor.ptr);

        /* Allocate the new VIP. */
        vip = s_s3_client_vip_new(client, &host_address_byte_cursor);

        if (vip == NULL) {
            result = AWS_OP_ERR;
            break;
        }

        aws_linked_list_push_back(&client->synced_data.vips, &vip->node);
        ++client->synced_data.vip_count;

        s_s3_client_add_new_part_buffers_to_pool_synced(client, client->num_connections_per_vip);

        /* Setup all of our vip connections. */
        for (size_t conn_index = 0; conn_index < client->num_connections_per_vip; ++conn_index) {
            struct aws_s3_vip_connection *vip_connection = aws_s3_vip_connection_new(client, vip);

            aws_linked_list_push_back(&vip->vip_connections, &vip_connection->synced_data.vip_node);

            struct aws_s3_client_work *work = s_s3_client_work_new(client, vip_connection);

            aws_linked_list_push_back(&client->synced_data.pending_vip_connection_updates, &work->node);
        }

        s_s3_client_schedule_process_work_task_synced(client);
    }

unlock:

    s_s3_client_unlock_synced_data(client);

    return result;
}

static void s_s3_client_schedule_process_work_task_synced(struct aws_s3_client *client) {
    ASSERT_SYNCED_DATA_LOCK_HELD(client);

    if (client->synced_data.process_work_task_scheduled) {
        return;
    }

    s_s3_client_internal_acquire(client);

    aws_task_init(
        &client->synced_data.process_work_task, s_s3_client_process_work_task, client, "s3_client_process_work_task");

    aws_event_loop_schedule_task_now(client->event_loop, &client->synced_data.process_work_task);

    client->synced_data.process_work_task_scheduled = true;
}

/* Task function for trying to find a request that can be processed. */
static void s_s3_client_process_work_task(struct aws_task *task, void *arg, enum aws_task_status task_status) {
    AWS_PRECONDITION(task);
    (void)task;
    (void)task_status;

    struct aws_s3_client *client = arg;
    AWS_PRECONDITION(client);

    /* Client keeps a reference to the event loop group; a 'canceled' status should not be happen.*/
    AWS_ASSERT(task_status == AWS_TASK_STATUS_RUN_READY);

    struct aws_linked_list vip_connections_updates;
    struct aws_linked_list vip_connections_removals;
    struct aws_linked_list meta_requests_adds;
    struct aws_linked_list work_destroy_list;

    aws_linked_list_init(&vip_connections_updates);
    aws_linked_list_init(&vip_connections_removals);
    aws_linked_list_init(&meta_requests_adds);
    aws_linked_list_init(&work_destroy_list);

    s_s3_client_lock_synced_data(client);

    /* Once we exit this mutex, someone can reschedule this task. */
    client->synced_data.process_work_task_scheduled = false;

    aws_linked_list_swap_contents(&vip_connections_updates, &client->synced_data.pending_vip_connection_updates);
    aws_linked_list_swap_contents(&vip_connections_removals, &client->synced_data.pending_vip_connection_removals);
    aws_linked_list_swap_contents(&meta_requests_adds, &client->synced_data.pending_meta_requests);

    s_s3_client_unlock_synced_data(client);

    /*******************/
    /* Step 1: Process VIP connection removals, flagging each VIP connection for destruction */
    /*******************/
    while (!aws_linked_list_empty(&vip_connections_removals)) {
        struct aws_linked_list_node *work_node = aws_linked_list_pop_back(&vip_connections_removals);
        struct aws_s3_client_work *work = AWS_CONTAINER_OF(work_node, struct aws_s3_client_work, node);
        struct aws_s3_vip_connection *vip_connection = work->user_data;

        /* We can't necessarily clean up a VIP connection immediately--it may still be in the middle of processing a
         * request. Instead, we flag it for pending destruction, and the next time it becomes idle, it will be
         * destroyed. */
        vip_connection->threaded_data.pending_destruction = true;

        aws_linked_list_push_back(&work_destroy_list, &work->node);
    }

    /*******************/
    /* Step 2: Process meta request adds. */
    /*******************/
    while (!aws_linked_list_empty(&meta_requests_adds)) {
        struct aws_linked_list_node *work_node = aws_linked_list_pop_back(&meta_requests_adds);
        struct aws_s3_client_work *work = AWS_CONTAINER_OF(work_node, struct aws_s3_client_work, node);
        struct aws_s3_meta_request *meta_request = work->user_data;

        /* If this is already scheduled, go ahead and clean it up. */
        if (meta_request->threaded_data.added_to_client) {
            aws_s3_meta_request_release(meta_request);
        } else {
            /* Push it to our list.  We don't call aws_s3_meta_request_acquire here since we still have the reference
             * for when the meta request was added to the work list. */
            aws_linked_list_push_back(&client->threaded_data.meta_requests, &meta_request->threaded_data.node);
            meta_request->threaded_data.added_to_client = true;
        }

        aws_linked_list_push_back(&work_destroy_list, &work->node);
    }

    /*******************/
    /* Step 3: Check all of the incoming VIP connections for meta requests.  If they already have a meta request, that
     * means they just finished a request, so we check for remaining work.  If there isn't any remaining work, then we
     * remove that meta request from our list. */
    /*******************/
    for (struct aws_linked_list_node *work_node = aws_linked_list_begin(&vip_connections_updates);
         work_node != aws_linked_list_end(&vip_connections_updates);
         work_node = aws_linked_list_next(work_node)) {
        struct aws_s3_client_work *work = AWS_CONTAINER_OF(work_node, struct aws_s3_client_work, node);
        struct aws_s3_vip_connection *vip_connection = work->user_data;

        struct aws_s3_meta_request *meta_request = vip_connection->threaded_data.meta_request;

        /* If this VIP connection has a meta request that no longer has work, we can remove that meta request.*/
        if (meta_request == NULL || aws_s3_meta_request_has_work(meta_request)) {
            continue;
        }

        /* Try to find the meta request before the current meta request. */
        struct aws_s3_meta_request *prev_meta_request = NULL;

        if (&meta_request->threaded_data.node != aws_linked_list_begin(&client->threaded_data.meta_requests)) {
            struct aws_linked_list_node *prev_node = aws_linked_list_prev(&meta_request->threaded_data.node);
            prev_meta_request = AWS_CONTAINER_OF(prev_node, struct aws_s3_meta_request, threaded_data);
        }

        /* Go through all of the VIP connections that this meta request is referenced by, and switch their meta
         * request to the meta request before it.  This is so that the processing ordering of meta requests will still
         * be maintained in the next step when we actually go to the next request in the list. */
        while (!aws_linked_list_empty(&meta_request->threaded_data.referenced_vip_connections)) {
            struct aws_linked_list_node *ref_vip_connection_node =
                aws_linked_list_pop_back(&meta_request->threaded_data.referenced_vip_connections);

            struct aws_s3_vip_connection *ref_vip_connection =
                AWS_CONTAINER_OF(ref_vip_connection_node, struct aws_s3_vip_connection, threaded_data);

            /* All VIP connections in this referenced list should be referencing this meta request. */
            AWS_ASSERT(ref_vip_connection->threaded_data.meta_request == meta_request);

            /* Clear out the reference to this meta request */
            aws_s3_meta_request_release(meta_request);

            ref_vip_connection->threaded_data.meta_request = prev_meta_request;

            /* If we just pointed our VIP connection to a new meta request, then acquire a reference and add the VIP
             * connection to the reference list. */
            if (prev_meta_request != NULL) {
                aws_s3_meta_request_acquire(prev_meta_request);
                aws_linked_list_push_back(
                    &prev_meta_request->threaded_data.referenced_vip_connections,
                    &ref_vip_connection->threaded_data.meta_request_reference_node);
            }
        }

        /* Remove the meta request from our list. */
        aws_linked_list_remove(&meta_request->threaded_data.node);
        meta_request->threaded_data.added_to_client = false;
        aws_s3_meta_request_release(meta_request);
    }

    /*******************/
    /* Step 4: Move all idle connections into our local update list.  If it turns out we can't find work for them,
     * they'll arrive back in the idle vip connections list by the end of the next loop. */
    /*******************/
    aws_linked_list_move_all_back(&vip_connections_updates, &client->threaded_data.idle_vip_connections);

    /*******************/
    /* Step 5: Go through all VIP connections that could take new work, both incoming and what we know is already idle,
     * and try to assign a meta request to each. */
    /*******************/
    while (!aws_linked_list_empty(&vip_connections_updates)) {
        struct aws_linked_list_node *work_node = aws_linked_list_pop_back(&vip_connections_updates);
        struct aws_s3_client_work *work = AWS_CONTAINER_OF(work_node, struct aws_s3_client_work, node);
        struct aws_s3_vip_connection *vip_connection = work->user_data;

        /* If this VIP connection is pending destruction, go ahead and clean it up now. */
        if (vip_connection->threaded_data.pending_destruction) {
            s_s3_vip_connection_destroy(client, vip_connection);
            aws_linked_list_push_back(&work_destroy_list, &work->node);
            continue;
        }

        struct aws_linked_list_node *next_meta_request_node = NULL;

        /* If this VIP connection has a meta request, clear it out, and try to grab the next one in the list. */
        if (vip_connection->threaded_data.meta_request != NULL) {
            next_meta_request_node =
                aws_linked_list_next(&vip_connection->threaded_data.meta_request->threaded_data.node);

            aws_linked_list_remove(&vip_connection->threaded_data.meta_request_reference_node);
            aws_s3_meta_request_release(vip_connection->threaded_data.meta_request);
            vip_connection->threaded_data.meta_request = NULL;
        }

        bool has_next_meta_request_node =
            next_meta_request_node != NULL &&
            next_meta_request_node != aws_linked_list_end(&client->threaded_data.meta_requests);

        /* If we don't have a next-meta-request node (either we didn't have a meta request, or it's next-node didn't get
         * us anything), and we do have something in our meta request list, then start at the beginning of the list.*/
        if (!has_next_meta_request_node && !aws_linked_list_empty(&client->threaded_data.meta_requests)) {
            next_meta_request_node = aws_linked_list_begin(&client->threaded_data.meta_requests);
        }

        /* If we still don't have a meta request node, there's nothing for this VIP connection to do. */
        if (next_meta_request_node == NULL) {
            AWS_ASSERT(vip_connection->threaded_data.meta_request == NULL);
            aws_linked_list_push_back(&client->threaded_data.idle_vip_connections, &work->node);
            continue;
        }

        struct aws_s3_meta_request *next_meta_request =
            AWS_CONTAINER_OF(next_meta_request_node, struct aws_s3_meta_request, threaded_data);

        /* Acquire a new reference to this meta request for our VIP connection. */
        aws_s3_meta_request_acquire(next_meta_request);
        vip_connection->threaded_data.meta_request = next_meta_request;

        aws_linked_list_push_back(
            &next_meta_request->threaded_data.referenced_vip_connections,
            &vip_connection->threaded_data.meta_request_reference_node);

        /* Acquire an internal reference to be released by s_s3_client_vip_connection_request_finished. */
        s_s3_client_internal_acquire(client);

        aws_s3_meta_request_send_next_request(
            vip_connection->threaded_data.meta_request,
            vip_connection,
            s_s3_client_vip_connection_request_finished,
            vip_connection);

        aws_linked_list_push_back(&work_destroy_list, &work->node);
    }

    s_s3_client_work_destroy_list(client, &work_destroy_list);

    s_s3_client_internal_release(client);
}

/* Called by the meta request when it has finished using this VIP connection for a single request. */
static void s_s3_client_vip_connection_request_finished(void *user_data) {
    struct aws_s3_vip_connection *vip_connection = user_data;
    AWS_PRECONDITION(vip_connection);

    struct aws_s3_vip *vip = vip_connection->owning_vip;
    AWS_PRECONDITION(vip);

    struct aws_s3_client *client = vip->owning_client;
    AWS_PRECONDITION(client);

    s_s3_client_lock_synced_data(client);

    struct aws_s3_client_work *work = s_s3_client_work_new(client, vip_connection);
    aws_linked_list_push_back(&client->synced_data.pending_vip_connection_updates, &work->node);
    s_s3_client_schedule_process_work_task_synced(client);

    s_s3_client_unlock_synced_data(client);

    s_s3_client_internal_release(client);
}

struct s3_client_get_http_connection_payload {
    struct aws_s3_client *client;
    struct aws_s3_vip_connection *vip_connection;
    aws_s3_client_get_http_connection_callback *callback;
    void *user_data;
};

/* Handles getting an HTTP connection for the caller, given the vip_connection reference. */
static int s_s3_client_get_http_connection(
    struct aws_s3_client *client,
    struct aws_s3_vip_connection *vip_connection,
    aws_s3_client_get_http_connection_callback *callback,
    void *user_data) {

    AWS_PRECONDITION(client);
    AWS_PRECONDITION(vip_connection);

    struct s3_client_get_http_connection_payload *payload =
        aws_mem_acquire(client->allocator, sizeof(struct s3_client_get_http_connection_payload));

    payload->client = client;
    aws_s3_client_acquire(client);
    payload->vip_connection = vip_connection;
    payload->callback = callback;
    payload->user_data = user_data;

    struct aws_http_connection **http_connection = &vip_connection->http_connection;
    uint32_t *connection_request_count = &vip_connection->request_count;

    struct aws_http_connection_manager *http_connection_manager = vip_connection->owning_vip->http_connection_manager;

    /* If we have a cached connection, see if we still want to use it. */
    if (*http_connection != NULL) {
        /* If we're at the max request count, set us up to get a new connection.  Also close the original connection
         * so that the connection manager doesn't reuse it.*/
        /* TODO maybe find a more visible way of preventing the
         * connection from going back into the pool. */
        if (*connection_request_count == s_s3_max_request_count_per_connection) {
            aws_http_connection_close(*http_connection);

            /* TODO handle possible error here? */
            aws_http_connection_manager_release_connection(http_connection_manager, *http_connection);

            *http_connection = NULL;
            *connection_request_count = 0;
        } else if (!aws_http_connection_is_open(*http_connection)) {
            /* If our connection is closed for some reason, also get rid of it.*/
            aws_http_connection_manager_release_connection(http_connection_manager, *http_connection);

            *http_connection = NULL;
            *connection_request_count = 0;
        }
    }

    if (*http_connection != NULL) {
        s_s3_client_vip_connection_on_acquire_request_connection(*http_connection, AWS_ERROR_SUCCESS, payload);
    } else {
        aws_http_connection_manager_acquire_connection(
            http_connection_manager, s_s3_client_vip_connection_on_acquire_request_connection, payload);
    }

    return AWS_OP_SUCCESS;
}

static void s_s3_client_vip_connection_on_acquire_request_connection(
    struct aws_http_connection *incoming_http_connection,
    int error_code,
    void *user_data) {

    struct s3_client_get_http_connection_payload *payload = user_data;
    AWS_PRECONDITION(user_data);

    struct aws_s3_client *client = payload->client;
    AWS_PRECONDITION(client);

    struct aws_s3_vip_connection *vip_connection = payload->vip_connection;
    AWS_PRECONDITION(vip_connection);

    if (error_code != AWS_ERROR_SUCCESS) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_VIP_CONNECTION,
            "id=%p: Could not acquire connection due to error code %d (%s)",
            (void *)vip_connection,
            error_code,
            aws_error_str(error_code));

        if (payload->callback != NULL) {
            payload->callback(NULL, error_code, payload->user_data);
        }

        goto clean_up;
    }

    struct aws_http_connection_manager *http_connection_manager = vip_connection->owning_vip->http_connection_manager;
    struct aws_http_connection **current_http_connection = &vip_connection->http_connection;

    /* If our cached connection is not equal to the one we just received, switch to the received one. */
    if (*current_http_connection != incoming_http_connection) {

        if (*current_http_connection != NULL) {

            aws_http_connection_manager_release_connection(http_connection_manager, *current_http_connection);

            *current_http_connection = NULL;
        }

        *current_http_connection = incoming_http_connection;
        vip_connection->request_count = 0;
    }

    /* Notify the caller of their HTTP connection. */
    if (payload->callback != NULL) {
        payload->callback(*current_http_connection, AWS_ERROR_SUCCESS, payload->user_data);
    }

clean_up:

    aws_s3_client_release(payload->client);
    aws_mem_release(client->allocator, payload);
    payload = NULL;
}

static void s_s3_client_on_host_resolver_address_resolved(
    struct aws_host_resolver *resolver,
    const struct aws_string *host_name,
    int err_code,
    const struct aws_array_list *host_addresses,
    void *user_data) {

    (void)resolver;
    (void)host_name;
    (void)err_code;

    AWS_PRECONDITION(resolver);
    AWS_PRECONDITION(host_name);
    AWS_PRECONDITION(host_addresses);
    AWS_PRECONDITION(user_data);

    struct aws_s3_client *client = user_data;

    if (err_code != AWS_ERROR_SUCCESS) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_CLIENT,
            "id=%p Error when resolving endpoint '%s' for S3 client %d (%s)",
            (void *)client,
            (const char *)host_name->bytes,
            err_code,
            aws_error_str(err_code));
        return;
    }

    AWS_ASSERT(host_addresses);
    s_s3_client_add_vips(client, host_addresses);
    s_s3_client_internal_release(client);
}

static void s_s3_client_host_listener_resolved_address_callback(
    struct aws_host_listener *listener,
    const struct aws_array_list *host_addresses,
    void *user_data) {
    (void)listener;

    AWS_PRECONDITION(listener);
    AWS_PRECONDITION(host_addresses);
    AWS_PRECONDITION(user_data);

    struct aws_s3_client *client = user_data;
    s_s3_client_add_vips(client, host_addresses);
}

static void s_s3_client_host_listener_shutdown_callback(void *user_data) {
    AWS_PRECONDITION(user_data);
    struct aws_s3_client *client = user_data;
    s_s3_client_internal_release(client);
}

static int s_s3_client_start_resolving_addresses_synced(struct aws_s3_client *client) {
    ASSERT_SYNCED_DATA_LOCK_HELD(client);
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(client->client_bootstrap);
    AWS_PRECONDITION(client->client_bootstrap->host_resolver);

    struct aws_host_resolver *host_resolver = client->client_bootstrap->host_resolver;

    struct aws_host_listener *host_listener = NULL;
    struct aws_host_listener_options options = {.host_name = aws_byte_cursor_from_string(client->synced_data.endpoint),
                                                .resolved_address_callback =
                                                    s_s3_client_host_listener_resolved_address_callback,
                                                .shutdown_callback = s_s3_client_host_listener_shutdown_callback,
                                                .user_data = client};

    host_listener = aws_host_resolver_add_host_listener(host_resolver, &options);

    if (host_listener == NULL) {

        AWS_LOGF_ERROR(
            AWS_LS_S3_CLIENT,
            "id=%p: Error trying to add listener for endpoint %s",
            (void *)client,
            (const char *)client->synced_data.endpoint->bytes);

        return AWS_OP_ERR;
    }

    /* Acquire internal ref for host listener so that we don't clean up until the listener shutdown callback is
     * called.*/
    s_s3_client_internal_acquire(client);
    client->synced_data.host_listener = host_listener;

    struct aws_host_resolution_config host_resolver_config;
    AWS_ZERO_STRUCT(host_resolver_config);
    host_resolver_config.impl = aws_default_dns_resolve;
    host_resolver_config.max_ttl = s_default_dns_host_address_ttl_seconds;
    host_resolver_config.impl_data = client;

    /* Acquire internal ref for resolve host callback so that we don't clean up until that callback is called. */
    s_s3_client_internal_acquire(client);

    if (aws_host_resolver_resolve_host(
            host_resolver,
            client->synced_data.endpoint,
            s_s3_client_on_host_resolver_address_resolved,
            &host_resolver_config,
            client)) {

        AWS_LOGF_ERROR(
            AWS_LS_S3_CLIENT,
            "id=%p: Error trying to resolve host for endpoint %s",
            (void *)client,
            (const char *)client->synced_data.endpoint->bytes);

        aws_host_resolver_remove_host_listener(host_resolver, client->synced_data.host_listener);
        client->synced_data.host_listener = NULL;

        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}
