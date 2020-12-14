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
#include <aws/io/retry_strategy.h>
#include <aws/io/socket.h>
#include <aws/io/stream.h>
#include <aws/io/tls_channel_handler.h>

#include <inttypes.h>
#include <math.h>

enum aws_s3_meta_request_work_op {
    AWS_S3_META_REQUEST_WORK_OP_PUSH,
    AWS_S3_META_REQUEST_WORK_OP_REMOVE,
};

struct aws_s3_meta_request_work {
    struct aws_linked_list_node node;
    struct aws_s3_meta_request *meta_request;
    enum aws_s3_meta_request_work_op op;
};

static const uint32_t s_s3_max_request_count_per_connection = 100;
static const uint32_t s_connection_timeout_ms = 3000;
static const double s_throughput_per_vip_gbps = 6.25;
static const uint32_t s_num_connections_per_vip = 10;

static const uint16_t s_http_port = 80;
static const uint16_t s_https_port = 443;

static const uint64_t s_default_part_size = 5 * 1024 * 1024;
static const uint64_t s_default_max_part_size = 20 * 1024 * 1024;
static const size_t s_default_dns_host_address_ttl_seconds = 2 * 60;
static const double s_default_throughput_target_gbps = 5.0;
static const uint32_t s_default_max_retries = 5;

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

static struct aws_s3_vip *s_s3_find_vip(
    const struct aws_linked_list *vip_list,
    const struct aws_byte_cursor *host_address);

static struct aws_s3_meta_request *s_s3_client_meta_request_factory(
    struct aws_s3_client *client,
    const struct aws_s3_meta_request_options *options);

static int s_s3_client_add_vips(struct aws_s3_client *client, const struct aws_array_list *host_addresses);

static void s_s3_client_schedule_meta_request_work(
    struct aws_s3_client *client,
    struct aws_s3_meta_request *meta_request,
    enum aws_s3_meta_request_work_op op);

static void s_s3_client_push_meta_request(struct aws_s3_client *client, struct aws_s3_meta_request *meta_request);

static void s_s3_client_remove_meta_request(struct aws_s3_client *client, struct aws_s3_meta_request *meta_request);

static void s_s3_client_schedule_process_work_task_synced(struct aws_s3_client *client);
static void s_s3_client_process_work_task(struct aws_task *task, void *arg, enum aws_task_status task_status);

static void s_s3_client_body_streaming_task(struct aws_task *task, void *arg, enum aws_task_status task_status);

static int s_s3_client_get_http_connection(struct aws_s3_client *client, struct aws_s3_vip_connection *vip_connection);
/* Handles getting an HTTP connection for the caller, given the vip_connection reference. */
static int s_s3_client_get_http_connection_default(
    struct aws_s3_client *client,
    struct aws_s3_vip_connection *vip_connection);

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
    .push_meta_request = s_s3_client_push_meta_request,
    .remove_meta_request = s_s3_client_remove_meta_request,
    .get_http_connection = s_s3_client_get_http_connection_default,
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

    struct aws_s3_client *client = aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_client));

    client->allocator = allocator;
    client->sba_allocator = aws_small_block_allocator_new(allocator, true);

    client->vtable = &s_s3_client_default_vtable;

    aws_ref_count_init(&client->ref_count, client, (aws_simple_completion_callback *)s_s3_client_start_destroy);
    aws_ref_count_init(
        &client->internal_ref_count, client, (aws_simple_completion_callback *)s_s3_client_finish_destroy);

    /* Store our client bootstrap. */
    client->client_bootstrap = client_config->client_bootstrap;
    aws_client_bootstrap_acquire(client_config->client_bootstrap);

    struct aws_event_loop_group *event_loop_group = client_config->client_bootstrap->event_loop_group;
    aws_event_loop_group_acquire(event_loop_group);

    client->process_work_event_loop = aws_event_loop_group_get_next_loop(event_loop_group);

    size_t num_event_loops = aws_array_list_length(&client->client_bootstrap->event_loop_group->event_loops);
    size_t num_streaming_threads = num_event_loops / 2;

    if (num_streaming_threads < 1) {
        num_streaming_threads = 1;
    }

    client->body_streaming_elg = aws_event_loop_group_new_default(client->allocator, num_streaming_threads, NULL);

    /* Make a copy of the region string. */
    client->region = aws_string_new_from_array(allocator, client_config->region.ptr, client_config->region.len);

    if (client_config->part_size != 0) {
        *((uint64_t *)&client->part_size) = client_config->part_size;
    } else {
        *((uint64_t *)&client->part_size) = s_default_part_size;
    }

    if (client_config->max_part_size != 0) {
        *((uint64_t *)&client->max_part_size) = client_config->max_part_size;
    } else {
        *((uint64_t *)&client->max_part_size) = s_default_max_part_size;
    }

    if (client_config->max_part_size < client_config->part_size) {
        *((uint64_t *)&client_config->max_part_size) = client_config->part_size;
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

    /* Determine how many vips are ideal by dividing target-throughput by throughput-per-vip. */
    {
        double ideal_vip_count_double = client->throughput_target_gbps / s_throughput_per_vip_gbps;
        *((uint32_t *)&client->ideal_vip_count) = (uint32_t)ceil(ideal_vip_count_double);
    }

    if (client_config->signing_config) {
        client->cached_signing_config = aws_cached_signing_config_new(client->allocator, client_config->signing_config);
    }

    aws_mutex_init(&client->synced_data.lock);

    aws_linked_list_init(&client->synced_data.vips);
    aws_linked_list_init(&client->synced_data.pending_vip_connection_updates);
    aws_linked_list_init(&client->synced_data.pending_meta_request_work);

    aws_linked_list_init(&client->threaded_data.idle_vip_connections);
    aws_linked_list_init(&client->threaded_data.meta_requests);

    if (client_config->retry_strategy != NULL) {
        aws_retry_strategy_acquire(client_config->retry_strategy);
        client->retry_strategy = client_config->retry_strategy;
    } else {
        struct aws_exponential_backoff_retry_options backoff_retry_options = {
            .el_group = client_config->client_bootstrap->event_loop_group,
            .max_retries = s_default_max_retries,
        };

        struct aws_standard_retry_options retry_options = {
            .backoff_retry_options = backoff_retry_options,
        };

        client->retry_strategy = aws_retry_strategy_new_standard(allocator, &retry_options);
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

void aws_s3_client_push_meta_request(struct aws_s3_client *client, struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(client->vtable);
    AWS_PRECONDITION(client->vtable->push_meta_request);

    client->vtable->push_meta_request(client, meta_request);
}

void aws_s3_client_remove_meta_request(struct aws_s3_client *client, struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(client->vtable);
    AWS_PRECONDITION(client->vtable->remove_meta_request);

    client->vtable->remove_meta_request(client, meta_request);
}

static int s_s3_client_get_http_connection(struct aws_s3_client *client, struct aws_s3_vip_connection *vip_connection) {

    AWS_PRECONDITION(client);
    AWS_PRECONDITION(client->vtable);
    AWS_PRECONDITION(client->vtable->get_http_connection);

    return client->vtable->get_http_connection(client, vip_connection);
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
    AWS_ASSERT(aws_linked_list_empty(&client->synced_data.pending_meta_request_work));

    AWS_ASSERT(aws_linked_list_empty(&client->threaded_data.idle_vip_connections));
    AWS_ASSERT(aws_linked_list_empty(&client->threaded_data.meta_requests));

    aws_retry_strategy_release(client->retry_strategy);

    aws_event_loop_group_release(client->body_streaming_elg);
    aws_event_loop_group_release(client->client_bootstrap->event_loop_group);

    aws_client_bootstrap_release(client->client_bootstrap);
    aws_cached_signing_config_destroy(client->cached_signing_config);

    aws_small_block_allocator_destroy(client->sba_allocator);

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
    socket_options.connect_timeout_ms = s_connection_timeout_ms;

    struct aws_http_connection_manager_options manager_options;
    AWS_ZERO_STRUCT(manager_options);
    manager_options.bootstrap = client->client_bootstrap;
    manager_options.initial_window_size = SIZE_MAX;
    manager_options.socket_options = &socket_options;
    manager_options.proxy_options = NULL;
    manager_options.host = aws_byte_cursor_from_string(vip->host_address);
    manager_options.max_connections = s_num_connections_per_vip;
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

    aws_atomic_init_int(&vip->cleaning_up, 0);

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

/* Releases the memory for a vip structure. */
static void s_s3_client_vip_destroy(struct aws_s3_vip *vip) {
    AWS_PRECONDITION(vip);

    aws_atomic_store_int(&vip->cleaning_up, 1);

    struct aws_s3_client *client = vip->owning_client;
    s_s3_client_lock_synced_data(client);
    --client->synced_data.vip_count;
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

    struct aws_byte_cursor host_header_value;

    if (aws_http_headers_get(message_headers, g_host_header_name, &host_header_value)) {
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

    meta_request->client_data.body_streaming_event_loop = aws_event_loop_group_get_next_loop(client->body_streaming_elg);

    aws_s3_client_push_meta_request(client, meta_request);

    AWS_LOGF_INFO(AWS_LS_S3_CLIENT, "id=%p: Created meta request %p", (void *)client, (void *)meta_request);

    return meta_request;
}

static struct aws_s3_meta_request *s_s3_client_meta_request_factory(
    struct aws_s3_client *client,
    const struct aws_s3_meta_request_options *options) {
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(options);

    struct aws_http_headers *initial_message_headers = aws_http_message_get_headers(options->message);
    AWS_ASSERT(initial_message_headers);

    uint64_t content_length = 0;
    struct aws_byte_cursor content_length_cursor;
    bool content_length_header_found = false;

    if (!aws_http_headers_get(initial_message_headers, g_content_length_header_name, &content_length_cursor)) {
        struct aws_string *content_length_str = aws_string_new_from_cursor(client->allocator, &content_length_cursor);
        char *content_length_str_end = NULL;

        content_length = strtoull((const char *)content_length_str->bytes, &content_length_str_end, 10);
        aws_string_destroy(content_length_str);

        content_length_str = NULL;
        content_length_header_found = true;
    }

    /* Call the appropriate meta-request new function. */
    if (options->type == AWS_S3_META_REQUEST_TYPE_GET_OBJECT) {

        /* TODO If we already have a ranged header, we can break the range up into parts too.  However,
         * this requires some additional logic.  For now just a default meta request. */
        if (aws_http_headers_has(initial_message_headers, g_range_header_name)) {
            AWS_LOGF_ERROR(
                AWS_LS_S3_META_REQUEST,
                "Could not create auto-ranged-get meta request; handling of ranged header is currently unsupported.");
            aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
            return NULL;
        }

        return aws_s3_meta_request_auto_ranged_get_new(client->allocator, client, client->part_size, options);
    } else if (options->type == AWS_S3_META_REQUEST_TYPE_PUT_OBJECT) {

        if (!content_length_header_found) {
            AWS_LOGF_ERROR(
                AWS_LS_S3_META_REQUEST,
                "Could not create auto-ranged-put meta request; there is no Content-Length header present.");
            aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
            return NULL;
        }

        struct aws_input_stream *input_stream = aws_http_message_get_body_stream(options->message);

        if (input_stream == NULL) {
            AWS_LOGF_ERROR(
                AWS_LS_S3_META_REQUEST, "Could not create auto-ranged-put meta request; body stream is NULL.");
            aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
            return NULL;
        }

        if (content_length < client->part_size) {
            return aws_s3_meta_request_default_new(client->allocator, client, content_length, options);
        }

        uint64_t part_size = content_length / g_max_num_upload_parts;

        if (part_size > client->max_part_size) {
            AWS_LOGF_ERROR(
                AWS_LS_S3_META_REQUEST,
                "Could not create auto-ranged-put meta request; required part size for put request is %" PRIu64
                ", but current maximum part size is %" PRIu64,
                part_size,
                client->max_part_size);
            aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
            return NULL;
        }

        if (part_size < client->part_size) {
            part_size = client->part_size;
        }

        uint32_t num_parts = (uint32_t)(content_length / part_size);

        if ((content_length % part_size) > 0) {
            ++num_parts;
        }

        return aws_s3_meta_request_auto_ranged_put_new(client->allocator, client, part_size, num_parts, options);
    } else if (options->type == AWS_S3_META_REQUEST_TYPE_DEFAULT) {
        return aws_s3_meta_request_default_new(client->allocator, client, content_length, options);
    } else {
        AWS_FATAL_ASSERT(false);
    }

    return NULL;
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

        /* Allocate the new VIP. */
        vip = s_s3_client_vip_new(client, &host_address_byte_cursor);

        if (vip == NULL) {
            result = AWS_OP_ERR;
            break;
        }

        aws_linked_list_push_back(&client->synced_data.vips, &vip->node);
        ++client->synced_data.vip_count;

        AWS_LOGF_INFO(
            AWS_LS_S3_CLIENT,
            "id=%p Initiating creation of VIP with address '%s', total vip count %d",
            (void *)client,
            (const char *)host_address_byte_cursor.ptr,
            client->synced_data.vip_count);

        /* Setup all of our vip connections. */
        for (size_t conn_index = 0; conn_index < s_num_connections_per_vip; ++conn_index) {
            struct aws_s3_vip_connection *vip_connection = aws_s3_vip_connection_new(client, vip);

            aws_linked_list_push_back(&client->synced_data.pending_vip_connection_updates, &vip_connection->node);
        }

        s_s3_client_schedule_process_work_task_synced(client);
    }

unlock:

    s_s3_client_unlock_synced_data(client);

    return result;
}

static void s_s3_client_schedule_meta_request_work(
    struct aws_s3_client *client,
    struct aws_s3_meta_request *meta_request,
    enum aws_s3_meta_request_work_op op) {
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(meta_request);

    s_s3_client_lock_synced_data(client);

    struct aws_s3_meta_request_work *meta_request_work =
        aws_mem_calloc(client->sba_allocator, 1, sizeof(struct aws_s3_meta_request_work));
    meta_request_work->meta_request = meta_request;
    meta_request_work->op = op;

    aws_s3_meta_request_acquire(meta_request);
    aws_linked_list_push_back(&client->synced_data.pending_meta_request_work, &meta_request_work->node);

    s_s3_client_schedule_process_work_task_synced(client);
    s_s3_client_unlock_synced_data(client);
}

static void s_s3_client_push_meta_request(struct aws_s3_client *client, struct aws_s3_meta_request *meta_request) {
    s_s3_client_schedule_meta_request_work(client, meta_request, AWS_S3_META_REQUEST_WORK_OP_PUSH);
}

static void s_s3_client_remove_meta_request(struct aws_s3_client *client, struct aws_s3_meta_request *meta_request) {
    s_s3_client_schedule_meta_request_work(client, meta_request, AWS_S3_META_REQUEST_WORK_OP_REMOVE);
}

static void s_s3_client_schedule_process_work_task_synced(struct aws_s3_client *client) {
    ASSERT_SYNCED_DATA_LOCK_HELD(client);

    if (client->synced_data.process_work_task_scheduled) {
        return;
    }

    s_s3_client_internal_acquire(client);

    aws_task_init(
        &client->synced_data.process_work_task, s_s3_client_process_work_task, client, "s3_client_process_work_task");

    aws_event_loop_schedule_task_now(client->process_work_event_loop, &client->synced_data.process_work_task);

    client->synced_data.process_work_task_scheduled = true;
}

static struct aws_s3_meta_request *s_s3_client_next_meta_request_threaded(
    struct aws_s3_client *client,
    struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(meta_request);

    struct aws_s3_meta_request *next_meta_request = NULL;

    /* Figure out which meta request is next line in early so that we can remove the meta request if we need to.
     */
    struct aws_linked_list_node *next_node =
        aws_linked_list_next(&meta_request->client_process_work_threaded_data.node);

    /* If the next node is the end node, wrap around to the beginning node. */
    if (next_node != aws_linked_list_end(&client->threaded_data.meta_requests)) {
        next_meta_request = AWS_CONTAINER_OF(next_node, struct aws_s3_meta_request, client_process_work_threaded_data);
    }

    return next_meta_request;
}

static void s_s3_client_remove_meta_request_threaded(
    struct aws_s3_client *client,
    struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(meta_request);
    (void)client;

    aws_linked_list_remove(&meta_request->client_process_work_threaded_data.node);
    meta_request->client_process_work_threaded_data.scheduled = false;
    aws_s3_meta_request_release(meta_request);
}

/* Task function for trying to find a request that can be processed. */
static void s_s3_client_process_work_task(struct aws_task *task, void *arg, enum aws_task_status task_status) {
    AWS_PRECONDITION(task);
    (void)task;
    (void)task_status;

    struct aws_s3_client *client = arg;
    AWS_PRECONDITION(client);

    /* Client keeps a reference to the event loop group; a 'canceled' status should not happen.*/
    AWS_ASSERT(task_status == AWS_TASK_STATUS_RUN_READY);

    struct aws_linked_list meta_request_work_list;
    aws_linked_list_init(&meta_request_work_list);

    struct aws_linked_list vip_connections_updates;
    aws_linked_list_init(&vip_connections_updates);

    /*******************/
    /* Step 1: Move everything into thread local memory. */
    /*******************/
    s_s3_client_lock_synced_data(client);

    /* Once we exit this mutex, someone can reschedule this task. */
    client->synced_data.process_work_task_scheduled = false;

    aws_linked_list_swap_contents(&meta_request_work_list, &client->synced_data.pending_meta_request_work);
    aws_linked_list_swap_contents(&vip_connections_updates, &client->synced_data.pending_vip_connection_updates);

    client->threaded_data.num_requests_in_flight -= client->synced_data.pending_request_count;
    client->synced_data.pending_request_count = 0;

    s_s3_client_unlock_synced_data(client);

    /*******************/
    /* Step 2: Process any meta request work. */
    /*******************/
    while (!aws_linked_list_empty(&meta_request_work_list)) {
        struct aws_linked_list_node *node = aws_linked_list_pop_back(&meta_request_work_list);
        struct aws_s3_meta_request_work *meta_request_work =
            AWS_CONTAINER_OF(node, struct aws_s3_meta_request_work, node);

        AWS_FATAL_ASSERT(meta_request_work != NULL);
        AWS_FATAL_ASSERT(meta_request_work->meta_request != NULL);

        struct aws_s3_meta_request *meta_request = meta_request_work->meta_request;

        if (meta_request_work->op == AWS_S3_META_REQUEST_WORK_OP_PUSH) {
            if (!meta_request->client_process_work_threaded_data.scheduled) {
                aws_linked_list_push_back(
                    &client->threaded_data.meta_requests, &meta_request->client_process_work_threaded_data.node);

                meta_request->client_process_work_threaded_data.scheduled = true;
            }
        } else if (meta_request_work->op == AWS_S3_META_REQUEST_WORK_OP_REMOVE) {
            if (meta_request->client_process_work_threaded_data.scheduled) {

                if (client->threaded_data.next_meta_request == meta_request) {
                    client->threaded_data.next_meta_request =
                        s_s3_client_next_meta_request_threaded(client, meta_request);
                }

                s_s3_client_remove_meta_request_threaded(client, meta_request);
            }
        }

        aws_mem_release(client->sba_allocator, meta_request_work);
    }

    /*******************/
    /* Step 3: Move all idle connections into our local update list. */
    /*******************/
    aws_linked_list_move_all_back(&vip_connections_updates, &client->threaded_data.idle_vip_connections);

    /*******************/
    /* Step 4: Go through all VIP connections, cleaning up old ones, and assigning requests where possible. */
    /*******************/
    const uint32_t max_requests_multiplier = 4;
    const uint32_t max_requests_in_flight =
        client->ideal_vip_count * s_num_connections_per_vip * max_requests_multiplier;

    uint32_t num_meta_requests = 0;

    for (struct aws_linked_list_node *node = aws_linked_list_begin(&client->threaded_data.meta_requests);
         node != aws_linked_list_end(&client->threaded_data.meta_requests);
         node = aws_linked_list_next(node)) {
        ++num_meta_requests;
    }

    while (!aws_linked_list_empty(&vip_connections_updates)) {

        struct aws_linked_list_node *node = aws_linked_list_pop_front(&vip_connections_updates);
        struct aws_s3_vip_connection *vip_connection = AWS_CONTAINER_OF(node, struct aws_s3_vip_connection, node);

        size_t cleaning_up = aws_atomic_load_int(&vip_connection->owning_vip->cleaning_up);

        /* If this VIP connection is pending destruction, go ahead and clean it up now. */
        if (cleaning_up) {
            s_s3_vip_connection_destroy(client, vip_connection);
            continue;
        }

        AWS_ASSERT(vip_connection->request == NULL);

        /* If we don't have any meta requests or how many requests we have in flight is over the maximum allowed, put
         * the vip connection into the idle list. */
        if (aws_linked_list_empty(&client->threaded_data.meta_requests) ||
            client->threaded_data.num_requests_in_flight >= max_requests_in_flight) {
            aws_linked_list_push_back(&client->threaded_data.idle_vip_connections, &vip_connection->node);
            continue;
        }

        struct aws_s3_meta_request *current_meta_request = client->threaded_data.next_meta_request;
        struct aws_s3_request *request = NULL;

        /* While we haven't found a request yet for our VIP Connection and there are still meta requests to look
         * through...*/
        while (request == NULL && !aws_linked_list_empty(&client->threaded_data.meta_requests)) {

            if (current_meta_request == NULL) {
                struct aws_linked_list_node *begin_node = aws_linked_list_begin(&client->threaded_data.meta_requests);
                current_meta_request =
                    AWS_CONTAINER_OF(begin_node, struct aws_s3_meta_request, client_process_work_threaded_data);
            }

            struct aws_s3_meta_request *next_meta_request =
                s_s3_client_next_meta_request_threaded(client, current_meta_request);

            /* Grab the next request from the meta request. */
            request = aws_s3_meta_request_next_request(current_meta_request);

            /* If the meta request is null, then this meta request is currently out requests, so remove it. */
            if (request == NULL) {
                s_s3_client_remove_meta_request_threaded(client, current_meta_request);
                current_meta_request = NULL;
            }

            current_meta_request = next_meta_request;
        }

        client->threaded_data.next_meta_request = current_meta_request;

        /* If a request couldn't be found, this vip connection is now idle. */
        if (request == NULL) {
            aws_linked_list_push_back(&client->threaded_data.idle_vip_connections, &vip_connection->node);
        } else {
            /* If a request could be found for the vip connection, grab an HTTP connection. */

            /* At this point, the vip connection owns the only existing ref count to the request.*/
            vip_connection->request = request;
            ++client->threaded_data.num_requests_in_flight;
            s_s3_client_get_http_connection(client, vip_connection);
        }
    }

    uint32_t num_idle_vip_conns = 0;

    for (struct aws_linked_list_node *node = aws_linked_list_begin(&client->threaded_data.idle_vip_connections);
         node != aws_linked_list_end(&client->threaded_data.idle_vip_connections);
         node = aws_linked_list_next(node)) {
        ++num_idle_vip_conns;
    }

    AWS_LOGF_INFO(
        AWS_LS_S3_CLIENT,
        "id=%p Number of meta requests %d, requests in flight %d, idle conns %d",
        (void *)client,
        num_meta_requests,
        client->threaded_data.num_requests_in_flight,
        num_idle_vip_conns);

    s_s3_client_internal_release(client);
}

/* Handles getting an HTTP connection for the caller, given the vip_connection reference. */
static int s_s3_client_get_http_connection_default(
    struct aws_s3_client *client,
    struct aws_s3_vip_connection *vip_connection) {
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(vip_connection);

    s_s3_client_internal_acquire(client);

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

            AWS_LOGF_INFO(
                AWS_LS_S3_CLIENT, "id=%p VIP Connection %p hit request limit.", (void *)client, (void *)vip_connection);

        } else if (!aws_http_connection_is_open(*http_connection)) {
            AWS_LOGF_INFO(
                AWS_LS_S3_CLIENT,
                "id=%p VIP Connection %p being released because it is not open.",
                (void *)client,
                (void *)vip_connection);

            /* If our connection is closed for some reason, also get rid of it.*/
            aws_http_connection_manager_release_connection(http_connection_manager, *http_connection);

            *http_connection = NULL;
            *connection_request_count = 0;
        }
    }

    if (*http_connection != NULL) {
        s_s3_client_vip_connection_on_acquire_request_connection(*http_connection, AWS_ERROR_SUCCESS, vip_connection);
    } else {
        aws_http_connection_manager_acquire_connection(
            http_connection_manager, s_s3_client_vip_connection_on_acquire_request_connection, vip_connection);
    }

    return AWS_OP_SUCCESS;
}

static void s_s3_client_vip_connection_on_acquire_request_connection(
    struct aws_http_connection *incoming_http_connection,
    int error_code,
    void *user_data) {

    struct aws_s3_vip_connection *vip_connection = user_data;
    AWS_PRECONDITION(vip_connection);
    AWS_PRECONDITION(vip_connection->owning_vip);

    struct aws_s3_client *client = vip_connection->owning_vip->owning_client;
    AWS_PRECONDITION(client);

    struct aws_s3_request *request = vip_connection->request;
    AWS_PRECONDITION(request);

    struct aws_s3_meta_request *meta_request = request->meta_request;
    AWS_PRECONDITION(meta_request);

    if (error_code != AWS_ERROR_SUCCESS) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_VIP_CONNECTION,
            "id=%p: Could not acquire connection due to error code %d (%s)",
            (void *)vip_connection,
            error_code,
            aws_error_str(error_code));

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

        AWS_LOGF_INFO(
            AWS_LS_S3_CLIENT,
            "id=%p Incoming connection has changed on VIP Connection %p.  Resetting local request count.",
            (void *)client,
            (void *)vip_connection);
    } else {
        ++vip_connection->request_count;

        AWS_LOGF_INFO(
            AWS_LS_S3_CLIENT,
            "id=%p Incoming connection has NOT changed on VIP Connection %p.  Increasing local request count. %d",
            (void *)client,
            (void *)vip_connection,
            vip_connection->request_count);
    }

    aws_s3_meta_request_make_request(meta_request, client, vip_connection);

clean_up:

    s_s3_client_internal_release(client);
}

/* Called by the meta request when it has finished using this VIP connection for a single request. */
void aws_s3_client_notify_connection_finished(
    struct aws_s3_client *client,
    struct aws_s3_vip_connection *vip_connection) {
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(vip_connection);

    /* If this VIP connection has a meta request, then it's a finished request, so clear it out. */
    if (vip_connection->request != NULL) {
        aws_s3_request_release(vip_connection->request);
        vip_connection->request = NULL;
    }

    s_s3_client_lock_synced_data(client);

    aws_linked_list_push_back(&client->synced_data.pending_vip_connection_updates, &vip_connection->node);
    s_s3_client_schedule_process_work_task_synced(client);

    s_s3_client_unlock_synced_data(client);
}

void aws_s3_client_notify_request_destroyed(struct aws_s3_client *client) {
    s_s3_client_lock_synced_data(client);
    ++client->synced_data.pending_request_count;
    s_s3_client_schedule_process_work_task_synced(client);
    s_s3_client_unlock_synced_data(client);
}

struct s3_streaming_body_payload {
    struct aws_s3_client *client;
    struct aws_linked_list requests;
    struct aws_task task;
};

void aws_s3_client_stream_response_body(
    struct aws_s3_client *client,
    struct aws_s3_meta_request *meta_request,
    struct aws_linked_list *requests) {
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(requests);

    struct s3_streaming_body_payload *payload =
        aws_mem_calloc(client->sba_allocator, 1, sizeof(struct s3_streaming_body_payload));

    s_s3_client_internal_acquire(client);
    payload->client = client;

    aws_linked_list_init(&payload->requests);
    aws_linked_list_move_all_back(&payload->requests, requests);

    aws_task_init(&payload->task, s_s3_client_body_streaming_task, payload, "s3_client_body_streaming_task");
    aws_event_loop_schedule_task_now(meta_request->client_data.body_streaming_event_loop, &payload->task);
}

static void s_s3_client_body_streaming_task(struct aws_task *task, void *arg, enum aws_task_status task_status) {
    (void)task;
    (void)task_status;

    struct s3_streaming_body_payload *payload = arg;
    AWS_PRECONDITION(payload);

    struct aws_s3_client *client = payload->client;
    AWS_PRECONDITION(client);

    /* Client owns this event loop group. A cancel should not be possible. */
    AWS_ASSERT(task_status == AWS_TASK_STATUS_RUN_READY);

    while (!aws_linked_list_empty(&payload->requests)) {
        struct aws_linked_list_node *request_node = aws_linked_list_pop_front(&payload->requests);
        struct aws_s3_request *request = AWS_CONTAINER_OF(request_node, struct aws_s3_request, node);
        struct aws_byte_cursor body_buffer_byte_cursor = aws_byte_cursor_from_buf(&request->send_data.response_body);

        AWS_ASSERT(request->part_number >= 1);

        struct aws_s3_meta_request *meta_request = request->meta_request;

        uint64_t range_start = (request->part_number - 1) * meta_request->part_size;
        if (meta_request->body_callback != NULL) {
            meta_request->body_callback(meta_request, &body_buffer_byte_cursor, range_start, meta_request->user_data);
        }

        aws_s3_request_release(request);
    }

    aws_mem_release(client->sba_allocator, payload);
    s_s3_client_internal_release(client);
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
