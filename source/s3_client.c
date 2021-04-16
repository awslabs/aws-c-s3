/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_auto_ranged_get.h"
#include "aws/s3/private/s3_auto_ranged_put.h"
#include "aws/s3/private/s3_client_impl.h"
#include "aws/s3/private/s3_default_meta_request.h"
#include "aws/s3/private/s3_meta_request_impl.h"
#include "aws/s3/private/s3_util.h"

#include <aws/auth/credentials.h>
#include <aws/common/assert.h>
#include <aws/common/atomics.h>
#include <aws/common/clock.h>
#include <aws/common/device_random.h>
#include <aws/common/environment.h>
#include <aws/common/string.h>
#include <aws/common/system_info.h>
#include <aws/http/connection.h>
#include <aws/http/connection_manager.h>
#include <aws/http/proxy.h>
#include <aws/http/request_response.h>
#include <aws/io/channel_bootstrap.h>
#include <aws/io/event_loop.h>
#include <aws/io/host_resolver.h>
#include <aws/io/retry_strategy.h>
#include <aws/io/socket.h>
#include <aws/io/stream.h>
#include <aws/io/tls_channel_handler.h>
#include <aws/io/uri.h>

#include <inttypes.h>
#include <math.h>

struct aws_s3_meta_request_work {
    struct aws_linked_list_node node;
    struct aws_s3_meta_request *meta_request;
};

static const enum aws_log_level s_log_level_client_stats = AWS_LL_INFO;

static const uint32_t s_s3_max_request_count_per_connection = 100;
static const uint32_t s_connection_timeout_ms = 3000;
static const uint32_t s_max_requests_multiplier = 4;

/* TODO Provide analysis on origins of this value. */
static const double s_throughput_per_vip_gbps = 3.33;

/* Preferred amount of active vip connections per meta request type. */
static const uint32_t s_num_conns_per_vip_meta_request_look_up[AWS_S3_META_REQUEST_TYPE_MAX] = {
    10, /* AWS_S3_META_REQUEST_TYPE_DEFAULT */
    10, /* AWS_S3_META_REQUEST_TYPE_GET_OBJECT */
    10, /* AWS_S3_META_REQUEST_TYPE_PUT_OBJECT */
};

/* Should be max of s_num_conns_per_vip_meta_request_look_up */
const uint32_t g_max_num_connections_per_vip = 10;

/* 50 = 0.5 * 100, where 100 is the max number of requests allowed per connection */
static const uint8_t s_max_request_jitter_range = 50;

static const uint16_t s_http_port = 80;
static const uint16_t s_https_port = 443;

/* TODO Provide more information on these values. */
static const size_t s_default_part_size = 8 * 1024 * 1024;
static const size_t s_default_max_part_size = 32 * 1024 * 1024;
static const double s_default_throughput_target_gbps = 10.0;
static const uint32_t s_default_max_retries = 5;

static size_t s_dns_host_address_ttl_seconds = 5 * 60;

AWS_STATIC_STRING_FROM_LITERAL(s_http_proxy_env_var, "HTTP_PROXY");

/* Called when ref count is 0. */
static void s_s3_client_start_destroy(void *user_data);

/* Called by s_s3_client_process_work_default when all shutdown criteria has been met. */
static void s_s3_client_finish_destroy_default(struct aws_s3_client *client);

/* Called when the body streaming elg shutdown has completed. */
static void s_s3_client_body_streaming_elg_shutdown(void *user_data);

typedef void(s3_client_vip_update_synced_data_state_fn)(struct aws_s3_vip *vip);

/* Used to atomically update vip state during clean-up and check for finishing shutdown. */
static void s_s3_vip_check_for_shutdown(struct aws_s3_vip *vip, s3_client_vip_update_synced_data_state_fn *update_fn);

/* Called by s_s3_vip_check_for_shutdown when all shutdown criteria for the vip has been met. */
static void s_s3_vip_finish_destroy(void *user_data);

/* Callback for when the vip's connection manager has shut down. */
static void s_s3_vip_http_connection_manager_shutdown_callback(void *user_data);

static void s_s3_client_setup_vip_connection_retry_token(
    struct aws_s3_client *client,
    struct aws_s3_vip_connection *vip_connection);

/* Handles acquiring an HTTP connection for the caller, given the vip_connection reference.  (Calls the corresponding
 * virtual function to do so.) */
static void s_s3_client_acquire_http_connection(
    struct aws_s3_client *client,
    struct aws_s3_vip_connection *vip_connection);

/* Callback which handles the HTTP connection retrieved by acquire_http_connection. */
static void s_s3_client_on_acquire_http_connection(
    struct aws_http_connection *http_connection,
    int error_code,
    void *user_data);

static void s_s3_client_push_meta_request_synced(
    struct aws_s3_client *client,
    struct aws_s3_meta_request *meta_request);

/* Schedule task for processing work. (Calls the corresponding vtable function.) */
static void s_s3_client_schedule_process_work_synced(struct aws_s3_client *client);

/* Default implementation for scheduling processing of work. */
static void s_s3_client_schedule_process_work_synced_default(struct aws_s3_client *client);

/* Actual task function that processes work. */
static void s_s3_client_process_work_task(struct aws_task *task, void *arg, enum aws_task_status task_status);

static void s_s3_client_process_work_default(struct aws_s3_client *client);

/* Ask the host resolver to start resolving addresses. */
static int s_s3_client_start_resolving_addresses(struct aws_s3_client *client);

static int s_s3_client_add_vips_default(struct aws_s3_client *client, const struct aws_array_list *host_addresses);

static void s_s3_client_remove_vips_default(struct aws_s3_client *client, const struct aws_array_list *host_addresses);

/* Default factory function for creating a meta request. */
static struct aws_s3_meta_request *s_s3_client_meta_request_factory_default(
    struct aws_s3_client *client,
    const struct aws_s3_meta_request_options *options);

static bool s_s3_client_http_connection_is_open(const struct aws_http_connection *connection);

/* Default remove-meta-request function to be used in the client vtable. */
static void s_s3_client_acquire_http_connection_default(
    struct aws_s3_client *client,
    struct aws_s3_vip_connection *vip_connection,
    aws_http_connection_manager_on_connection_setup_fn *on_connection_acquired_callback);

static void s_s3_vip_connection_destroy_default(
    struct aws_s3_client *client,
    struct aws_s3_vip_connection *vip_connection);

static struct aws_s3_client_vtable s_s3_client_default_vtable = {
    .meta_request_factory = s_s3_client_meta_request_factory_default,
    .acquire_http_connection = s_s3_client_acquire_http_connection_default,
    .add_vips = s_s3_client_add_vips_default,
    .remove_vips = s_s3_client_remove_vips_default,
    .http_connection_is_open = s_s3_client_http_connection_is_open,
    .vip_connection_destroy = s_s3_vip_connection_destroy_default,
    .schedule_process_work_synced = s_s3_client_schedule_process_work_synced_default,
    .process_work = s_s3_client_process_work_default,
    .finish_destroy = s_s3_client_finish_destroy_default,
};

void aws_s3_set_dns_ttl(size_t ttl) {
    s_dns_host_address_ttl_seconds = ttl;
}

uint32_t aws_s3_client_get_max_active_connections(struct aws_s3_client *client, uint32_t num_connections_per_vip) {
    AWS_PRECONDITION(client);

    if (num_connections_per_vip == 0) {
        num_connections_per_vip = g_max_num_connections_per_vip;
    }

    uint32_t max_active_connections = client->ideal_vip_count * num_connections_per_vip;

    if (client->max_active_connections_override > 0 &&
        client->max_active_connections_override < max_active_connections) {
        max_active_connections = client->max_active_connections_override;
    }

    return max_active_connections;
}

uint32_t aws_s3_client_get_max_requests_in_flight(struct aws_s3_client *client) {
    AWS_PRECONDITION(client);
    return aws_s3_client_get_max_active_connections(client, 0) * s_max_requests_multiplier;
}

uint32_t aws_s3_client_get_max_requests_prepare(struct aws_s3_client *client) {
    return aws_s3_client_get_max_active_connections(client, 0);
}

uint32_t aws_s3_client_get_max_allocated_vip_count(struct aws_s3_client *client) {
    return (client->ideal_vip_count * 2);
}

void aws_s3_client_lock_synced_data(struct aws_s3_client *client) {
    aws_mutex_lock(&client->synced_data.lock);
}

void aws_s3_client_unlock_synced_data(struct aws_s3_client *client) {
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

    aws_mutex_init(&client->synced_data.lock);

    aws_linked_list_init(&client->synced_data.vips);
    aws_linked_list_init(&client->synced_data.pending_vip_connection_updates);
    aws_linked_list_init(&client->synced_data.pending_meta_request_work);
    aws_linked_list_init(&client->synced_data.prepared_requests);

    aws_linked_list_init(&client->threaded_data.idle_vip_connections);
    aws_linked_list_init(&client->threaded_data.meta_requests);
    aws_linked_list_init(&client->threaded_data.request_queue);

    aws_atomic_init_int(&client->stats.num_requests_network_io, 0);
    aws_atomic_init_int(&client->stats.num_requests_stream_queued_waiting, 0);
    aws_atomic_init_int(&client->stats.num_requests_streaming, 0);
    aws_atomic_init_int(&client->stats.num_requests_in_flight, 0);
    aws_atomic_init_int(&client->stats.num_allocated_vip_connections, 0);
    aws_atomic_init_int(&client->stats.num_active_vip_connections, 0);
    aws_atomic_init_int(&client->stats.num_warm_vip_connections, 0);

    *((uint32_t *)&client->max_active_connections_override) = client_config->max_active_connections_override;

    /* Store our client bootstrap. */
    client->client_bootstrap = aws_client_bootstrap_acquire(client_config->client_bootstrap);

    struct aws_event_loop_group *event_loop_group = client_config->client_bootstrap->event_loop_group;
    aws_event_loop_group_acquire(event_loop_group);

    client->process_work_event_loop = aws_event_loop_group_get_next_loop(event_loop_group);

    /* Set up body streaming ELG */
    {
        uint16_t num_event_loops =
            (uint16_t)aws_array_list_length(&client->client_bootstrap->event_loop_group->event_loops);
        uint16_t num_streaming_threads = num_event_loops;

        if (num_streaming_threads < 1) {
            num_streaming_threads = 1;
        }

        struct aws_shutdown_callback_options body_streaming_elg_shutdown_options = {
            .shutdown_callback_fn = s_s3_client_body_streaming_elg_shutdown,
            .shutdown_callback_user_data = client,
        };

        if (aws_get_cpu_group_count() > 1) {
            client->body_streaming_elg = aws_event_loop_group_new_default_pinned_to_cpu_group(
                client->allocator, num_streaming_threads, 1, &body_streaming_elg_shutdown_options);
        } else {
            client->body_streaming_elg = aws_event_loop_group_new_default(
                client->allocator, num_streaming_threads, &body_streaming_elg_shutdown_options);
        }
        client->synced_data.body_streaming_elg_allocated = true;
    }

    /* Make a copy of the region string. */
    client->region = aws_string_new_from_array(allocator, client_config->region.ptr, client_config->region.len);

    if (client_config->part_size != 0) {
        *((size_t *)&client->part_size) = client_config->part_size;
    } else {
        *((size_t *)&client->part_size) = s_default_part_size;
    }

    if (client_config->max_part_size != 0) {
        *((size_t *)&client->max_part_size) = client_config->max_part_size;
    } else {
        *((size_t *)&client->max_part_size) = s_default_max_part_size;
    }

    if (client_config->max_part_size < client_config->part_size) {
        *((size_t *)&client_config->max_part_size) = client_config->part_size;
    }

    if (client_config->tls_mode == AWS_MR_TLS_ENABLED) {
        client->tls_connection_options =
            aws_mem_calloc(client->allocator, 1, sizeof(struct aws_tls_connection_options));

        if (client->tls_connection_options == NULL) {
            goto on_error;
        }

        if (client_config->tls_connection_options != NULL) {
            aws_tls_connection_options_copy(client->tls_connection_options, client_config->tls_connection_options);
        } else {
            struct aws_tls_ctx_options default_tls_ctx_options;
            AWS_ZERO_STRUCT(default_tls_ctx_options);

            aws_tls_ctx_options_init_default_client(&default_tls_ctx_options, allocator);

            struct aws_tls_ctx *default_tls_ctx = aws_tls_client_ctx_new(allocator, &default_tls_ctx_options);
            if (default_tls_ctx == NULL) {
                goto on_error;
            }

            aws_tls_connection_options_init_from_ctx(client->tls_connection_options, default_tls_ctx);

            aws_tls_ctx_release(default_tls_ctx);
            aws_tls_ctx_options_clean_up(&default_tls_ctx_options);
        }
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

    client->synced_data.active = true;

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

on_error:

    aws_s3_client_release(client);

    return NULL;
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

static void s_s3_client_start_destroy(void *user_data) {
    struct aws_s3_client *client = user_data;
    AWS_PRECONDITION(client);

    AWS_LOGF_DEBUG(AWS_LS_S3_CLIENT, "id=%p Client starting destruction.", (void *)client);

    struct aws_linked_list local_vip_list;
    aws_linked_list_init(&local_vip_list);

    struct aws_host_listener *host_listener = NULL;

    aws_s3_client_lock_synced_data(client);

    client->synced_data.active = false;

    /* Prevent the client from cleaning up inbetween the mutex unlock/re-lock below.*/
    client->synced_data.start_destroy_executing = true;

    /* Grab the host listener from the synced_data so that we can remove it outside of the lock. */
    host_listener = client->synced_data.host_listener;
    client->synced_data.host_listener = NULL;

    /* Swap out all VIP's so that we can clean them up outside of the lock. */
    aws_linked_list_swap_contents(&local_vip_list, &client->synced_data.vips);
    client->synced_data.active_vip_count = 0;

    aws_s3_client_unlock_synced_data(client);

    if (host_listener != NULL) {
        aws_host_resolver_remove_host_listener(client->client_bootstrap->host_resolver, host_listener);
        host_listener = NULL;
    }

    /* Iterate through the local list, removing each VIP. */
    while (!aws_linked_list_empty(&local_vip_list)) {
        struct aws_linked_list_node *vip_node = aws_linked_list_pop_back(&local_vip_list);
        struct aws_s3_vip *vip = AWS_CONTAINER_OF(vip_node, struct aws_s3_vip, node);
        aws_s3_vip_start_destroy(vip);
    }

    aws_event_loop_group_release(client->body_streaming_elg);
    client->body_streaming_elg = NULL;

    aws_s3_client_lock_synced_data(client);
    client->synced_data.start_destroy_executing = false;

    /* Schedule the work task to clean up outstanding connections and also to call s_s3_client_finish_destroy function
     * if everything cleaning up asynchronously has finished.  */
    s_s3_client_schedule_process_work_synced(client);
    aws_s3_client_unlock_synced_data(client);
}

static void s_s3_client_finish_destroy_default(struct aws_s3_client *client) {
    AWS_PRECONDITION(client);

    AWS_LOGF_DEBUG(AWS_LS_S3_CLIENT, "id=%p Client finishing destruction.", (void *)client);

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

static void s_s3_client_body_streaming_elg_shutdown(void *user_data) {
    struct aws_s3_client *client = user_data;
    AWS_PRECONDITION(client);

    AWS_LOGF_DEBUG(AWS_LS_S3_CLIENT, "id=%p Client body streaming ELG shutdown.", (void *)client);

    aws_s3_client_lock_synced_data(client);
    client->synced_data.body_streaming_elg_allocated = false;
    s_s3_client_schedule_process_work_synced(client);
    aws_s3_client_unlock_synced_data(client);
}

static int s_s3_client_get_proxy_uri(struct aws_s3_client *client, struct aws_uri *proxy_uri) {
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(client->allocator);

    struct aws_allocator *allocator = client->allocator;
    struct aws_string *proxy_uri_string = NULL;

    int result = AWS_OP_ERR;
    const struct aws_string *env_variable_name = NULL;

    if (aws_get_environment_value(allocator, s_http_proxy_env_var, &proxy_uri_string) == AWS_OP_SUCCESS &&
        proxy_uri_string != NULL) {
        env_variable_name = s_http_proxy_env_var;
    } else {
        aws_raise_error(AWS_ERROR_S3_PROXY_ENV_NOT_FOUND);
        goto clean_up;
    }

    AWS_LOGF_INFO(
        AWS_LS_S3_CLIENT,
        "id=%p Found proxy URI %s in environment variable %s",
        (void *)client,
        (const char *)proxy_uri_string->bytes,
        (const char *)env_variable_name->bytes);

    struct aws_byte_cursor proxy_uri_cursor = aws_byte_cursor_from_string(proxy_uri_string);

    if (aws_uri_init_parse(proxy_uri, allocator, &proxy_uri_cursor)) {
        AWS_LOGF_ERROR(AWS_LS_S3_CLIENT, "id=%p Could not parse found proxy URI.", (void *)client);
        aws_raise_error(AWS_ERROR_S3_PROXY_PARSE_FAILED);
        goto clean_up;
    }

    if (aws_byte_cursor_eq_ignore_case(&proxy_uri->scheme, &aws_http_scheme_http)) {
        /* Nothing to do. */
    } else if (proxy_uri->scheme.len > 0) {
        AWS_LOGF_ERROR(AWS_LS_S3_CLIENT, "id=%p Proxy URI contains unsupported scheme.", (void *)client);

        aws_raise_error(AWS_ERROR_S3_UNSUPPORTED_PROXY_SCHEME);
        goto clean_up;
    }

    result = AWS_OP_SUCCESS;

clean_up:

    aws_string_destroy(proxy_uri_string);
    return result;
}

/* Initialize a new VIP structure for the client to use, given an address. Assumes lock is held. */
struct aws_s3_vip *aws_s3_vip_new(
    struct aws_s3_client *client,
    const struct aws_byte_cursor *host_address,
    const struct aws_byte_cursor *server_name,
    uint32_t num_vip_connections,
    struct aws_linked_list *out_vip_connections_list,
    aws_s3_vip_shutdown_callback_fn *shutdown_callback,
    void *shutdown_user_data) {
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(host_address);
    AWS_PRECONDITION(server_name);
    AWS_PRECONDITION(out_vip_connections_list);

    struct aws_s3_vip *vip = aws_mem_calloc(client->allocator, 1, sizeof(struct aws_s3_vip));
    vip->owning_client = client;

    /* Copy over the host address. */
    vip->host_address = aws_string_new_from_array(client->allocator, host_address->ptr, host_address->len);

    vip->shutdown_callback = shutdown_callback;
    vip->shutdown_user_data = shutdown_user_data;

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
    manager_options.host = aws_byte_cursor_from_string(vip->host_address);
    manager_options.max_connections = num_vip_connections;
    manager_options.shutdown_complete_callback = s_s3_vip_http_connection_manager_shutdown_callback;
    manager_options.shutdown_complete_user_data = vip;

    struct aws_uri proxy_uri;
    AWS_ZERO_STRUCT(proxy_uri);
    struct aws_http_proxy_options *proxy_options = NULL;
    struct aws_tls_connection_options *proxy_tls_options = NULL;
    struct aws_tls_connection_options *manager_tls_options = NULL;

    if (s_s3_client_get_proxy_uri(client, &proxy_uri) == AWS_OP_SUCCESS) {
        proxy_options = aws_mem_calloc(client->allocator, 1, sizeof(struct aws_http_proxy_options));
        proxy_options->host = proxy_uri.host_name;
        proxy_options->port = proxy_uri.port;

        manager_options.proxy_options = proxy_options;
    }

    if (client->tls_connection_options != NULL) {
        manager_tls_options = aws_mem_calloc(client->allocator, 1, sizeof(struct aws_tls_connection_options));
        aws_tls_connection_options_copy(manager_tls_options, client->tls_connection_options);

        /* TODO fix this in the actual aws_tls_connection_options_set_server_name function. */
        if (manager_tls_options->server_name != NULL) {
            aws_string_destroy(manager_tls_options->server_name);
            manager_tls_options->server_name = NULL;
        }

        aws_tls_connection_options_set_server_name(
            manager_tls_options, client->allocator, (struct aws_byte_cursor *)server_name);

        manager_options.tls_connection_options = manager_tls_options;
        manager_options.port = s_https_port;
    } else {
        manager_options.port = s_http_port;
    }

    vip->http_connection_manager = aws_http_connection_manager_new(client->allocator, &manager_options);
    vip->synced_data.http_connection_manager_active = true;

    AWS_LOGF_DEBUG(
        AWS_LS_S3_CLIENT,
        "id=%p: Created connection manager %p for VIP %p",
        (void *)client,
        (void *)vip->http_connection_manager,
        (void *)vip);

    if (manager_tls_options != NULL) {
        aws_tls_connection_options_clean_up(manager_tls_options);
        aws_mem_release(client->allocator, manager_tls_options);
        manager_tls_options = NULL;
    }

    if (proxy_tls_options != NULL) {
        aws_tls_connection_options_clean_up(proxy_tls_options);
        aws_mem_release(client->allocator, proxy_tls_options);
        proxy_tls_options = NULL;
    }

    if (proxy_options != NULL) {
        aws_mem_release(client->allocator, proxy_options);
        proxy_options = NULL;
    }

    aws_uri_clean_up(&proxy_uri);

    if (vip->http_connection_manager == NULL) {
        AWS_LOGF_ERROR(AWS_LS_S3_VIP, "id=%p: Could not allocate aws_s3_vip connection manager.", (void *)vip);
        goto error_clean_up;
    }

    aws_atomic_init_int(&vip->active, 1);

    for (uint32_t i = 0; i < num_vip_connections; ++i) {
        struct aws_s3_vip_connection *vip_connection =
            aws_mem_calloc(client->allocator, 1, sizeof(struct aws_s3_vip_connection));

        aws_atomic_fetch_add(&client->stats.num_allocated_vip_connections, 1);

        vip_connection->owning_vip = vip;
        ++vip->synced_data.num_vip_connections;
        aws_linked_list_push_back(out_vip_connections_list, &vip_connection->node);
    }

    return vip;

error_clean_up:

    if (vip != NULL) {
        aws_string_destroy(vip->host_address);
        aws_mem_release(client->allocator, vip);
        vip = NULL;
    }

    return NULL;
}

static void s_s3_vip_set_reset_active(struct aws_s3_vip *vip) {
    AWS_PRECONDITION(vip);
    aws_atomic_store_int(&vip->active, 0);
}

/* Releases the memory for a vip structure. */
void aws_s3_vip_start_destroy(struct aws_s3_vip *vip) {
    if (vip == NULL) {
        return;
    }

    AWS_LOGF_DEBUG(AWS_LS_S3_VIP, "id=%p Starting destroy of VIP.", (void *)vip);

    s_s3_vip_check_for_shutdown(vip, s_s3_vip_set_reset_active);
}

static void s_s3_vip_check_for_shutdown(struct aws_s3_vip *vip, s3_client_vip_update_synced_data_state_fn *update_fn) {
    AWS_PRECONDITION(vip);
    AWS_PRECONDITION(vip->owning_client);

    bool finish_destroy = false;
    struct aws_http_connection_manager *conn_manager = NULL;

    aws_s3_client_lock_synced_data(vip->owning_client);

    if (update_fn != NULL) {
        update_fn(vip);
    }

    /* If this vip is active, we are not cleaning up. */
    if (aws_atomic_load_int(&vip->active) == 1) {
        goto unlock;
    }

    /* If this vip still has connections, we are not done cleaning up. */
    if (vip->synced_data.num_vip_connections > 0) {
        goto unlock;
    }

    /* If the connection manager is active, then we can try initiating a clean up of it now. */
    if (vip->synced_data.http_connection_manager_active) {

        /* If the connection manager is not NULL, take the pointer from the synced data so that it we can clean it up
         * outside of the lock. We reset the pointer on the synced_data so that nothing else entering this function will
         * trigger a double release. */
        if (vip->http_connection_manager != NULL) {
            conn_manager = vip->http_connection_manager;
            vip->http_connection_manager = NULL;
        }

        goto unlock;
    }

    finish_destroy = true;

unlock:
    aws_s3_client_unlock_synced_data(vip->owning_client);

    if (conn_manager != NULL) {
        aws_http_connection_manager_release(conn_manager);
        conn_manager = NULL;
    }

    if (finish_destroy) {
        s_s3_vip_finish_destroy(vip);
    }
}

static void s_s3_vip_set_conn_manager_shutdown_synced(struct aws_s3_vip *vip) {
    AWS_PRECONDITION(vip);
    AWS_PRECONDITION(vip->owning_client);
    ASSERT_SYNCED_DATA_LOCK_HELD(vip->owning_client);
    vip->synced_data.http_connection_manager_active = false;
}

static void s_s3_vip_http_connection_manager_shutdown_callback(void *user_data) {
    AWS_PRECONDITION(user_data);

    struct aws_s3_vip *vip = user_data;
    AWS_PRECONDITION(vip);

    AWS_LOGF_DEBUG(
        AWS_LS_S3_CLIENT, "id=%p VIP %p Connection manager shutdown", (void *)vip->owning_client, (void *)vip);

    s_s3_vip_check_for_shutdown(vip, s_s3_vip_set_conn_manager_shutdown_synced);
}

static void s_s3_vip_finish_destroy(void *user_data) {
    struct aws_s3_vip *vip = user_data;
    AWS_PRECONDITION(vip);

    AWS_LOGF_DEBUG(AWS_LS_S3_VIP, "id=%p Finishing destroy of VIP.", (void *)vip);

    /* Clean up the address string. */
    aws_string_destroy(vip->host_address);
    vip->host_address = NULL;

    void *shutdown_user_data = vip->shutdown_user_data;
    aws_s3_vip_shutdown_callback_fn *shutdown_callback = vip->shutdown_callback;

    struct aws_s3_client *client = vip->owning_client;
    aws_mem_release(client->allocator, vip);

    if (shutdown_callback != NULL) {
        shutdown_callback(shutdown_user_data);
    }
}

void aws_s3_client_set_vip_connection_warm(
    struct aws_s3_client *client,
    struct aws_s3_vip_connection *vip_connection,
    bool is_warm) {
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(vip_connection);

    if ((bool)vip_connection->is_warm == is_warm) {
        return;
    }

    vip_connection->is_warm = is_warm;

    if (is_warm) {
        aws_atomic_fetch_add(&client->stats.num_warm_vip_connections, 1);
    } else {
        aws_atomic_fetch_sub(&client->stats.num_warm_vip_connections, 1);
    }
}

void aws_s3_client_set_vip_connection_active(
    struct aws_s3_client *client,
    struct aws_s3_vip_connection *vip_connection,
    bool is_active) {
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(vip_connection);

    if ((bool)vip_connection->is_active == is_active) {
        return;
    }

    vip_connection->is_active = is_active;

    if (is_active) {
        aws_atomic_fetch_add(&client->stats.num_active_vip_connections, 1);
    } else {
        aws_atomic_fetch_sub(&client->stats.num_active_vip_connections, 1);
    }
}

uint32_t aws_s3_client_queue_requests_threaded(
    struct aws_s3_client *client,
    struct aws_linked_list *request_list,
    bool queue_front) {
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(request_list);

    uint32_t request_list_size = 0;

    for (struct aws_linked_list_node *node = aws_linked_list_begin(request_list);
         node != aws_linked_list_end(request_list);
         node = aws_linked_list_next(node)) {
        ++request_list_size;
    }

    if (queue_front) {
        aws_linked_list_move_all_front(&client->threaded_data.request_queue, request_list);
    } else {
        aws_linked_list_move_all_back(&client->threaded_data.request_queue, request_list);
    }

    client->threaded_data.request_queue_size += request_list_size;
    return request_list_size;
}

struct aws_s3_request *aws_s3_client_dequeue_request_threaded(struct aws_s3_client *client) {
    AWS_PRECONDITION(client);

    if (aws_linked_list_empty(&client->threaded_data.request_queue)) {
        return NULL;
    }

    struct aws_linked_list_node *request_node = aws_linked_list_pop_front(&client->threaded_data.request_queue);
    struct aws_s3_request *request = AWS_CONTAINER_OF(request_node, struct aws_s3_request, node);

    --client->threaded_data.request_queue_size;

    return request;
}

static void s_s3_vip_sub_num_vip_connections_synced(struct aws_s3_vip *vip) {
    AWS_PRECONDITION(vip);
    AWS_PRECONDITION(vip->owning_client);
    ASSERT_SYNCED_DATA_LOCK_HELD(vip->owning_client);
    --vip->synced_data.num_vip_connections;
}

void aws_s3_vip_connection_destroy(struct aws_s3_client *client, struct aws_s3_vip_connection *vip_connection) {
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(client->vtable);
    AWS_PRECONDITION(client->vtable->vip_connection_destroy);

    client->vtable->vip_connection_destroy(client, vip_connection);
}

/* Destroy a VIP Connection structure. */
static void s_s3_vip_connection_destroy_default(
    struct aws_s3_client *client,
    struct aws_s3_vip_connection *vip_connection) {
    if (vip_connection == NULL) {
        return;
    }

    struct aws_s3_vip *owning_vip = vip_connection->owning_vip;

    AWS_LOGF_DEBUG(
        AWS_LS_S3_VIP_CONNECTION,
        "id=%p Destroying VIP Connection owned by vip %p.",
        (void *)vip_connection,
        (void *)owning_vip);

    aws_s3_client_set_vip_connection_warm(client, vip_connection, false);
    aws_s3_client_set_vip_connection_active(client, vip_connection, false);

    aws_atomic_fetch_sub(&owning_vip->owning_client->stats.num_allocated_vip_connections, 1);

    if (vip_connection->http_connection != NULL) {
        AWS_ASSERT(owning_vip->http_connection_manager);

        aws_http_connection_manager_release_connection(
            owning_vip->http_connection_manager, vip_connection->http_connection);

        vip_connection->http_connection = NULL;
    }

    aws_retry_token_release(vip_connection->retry_token);
    vip_connection->retry_token = NULL;

    aws_mem_release(client->allocator, vip_connection);
    s_s3_vip_check_for_shutdown(owning_vip, s_s3_vip_sub_num_vip_connections_synced);
}

struct aws_s3_vip *aws_s3_find_vip(const struct aws_linked_list *vip_list, const struct aws_byte_cursor *host_address) {
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

    bool endpoint_matches = false;

    aws_s3_client_lock_synced_data(client);

    /* TODO This is temporary until we add multiple bucket support. */
    if (client->synced_data.endpoint == NULL) {
        client->synced_data.endpoint =
            aws_string_new_from_array(client->allocator, host_header_value.ptr, host_header_value.len);
        endpoint_matches = true;
    } else {
        struct aws_byte_cursor synced_endpoint_byte_cursor = aws_byte_cursor_from_string(client->synced_data.endpoint);
        endpoint_matches = aws_byte_cursor_eq_ignore_case(&synced_endpoint_byte_cursor, &host_header_value);
    }

    aws_s3_client_unlock_synced_data(client);

    if (!endpoint_matches) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_CLIENT,
            "id=%p Cannot create meta s3 request; message points to a different host than previous requests. "
            "Currently, only one endpoint is supported per client.",
            (void *)client);
        aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        return NULL;
    }

    if (s_s3_client_start_resolving_addresses(client)) {
        AWS_LOGF_ERROR(AWS_LS_S3_CLIENT, "id=%p: Could not start resolving endpoint for meta request.", (void *)client);
        return NULL;
    }

    struct aws_s3_meta_request *meta_request = client->vtable->meta_request_factory(client, options);

    if (meta_request == NULL) {
        AWS_LOGF_ERROR(AWS_LS_S3_CLIENT, "id=%p: Could not create new meta request.", (void *)client);
        return NULL;
    }

    aws_s3_client_lock_synced_data(client);
    s_s3_client_push_meta_request_synced(client, meta_request);
    s_s3_client_schedule_process_work_synced(client);
    aws_s3_client_unlock_synced_data(client);

    AWS_LOGF_INFO(AWS_LS_S3_CLIENT, "id=%p: Created meta request %p", (void *)client, (void *)meta_request);

    return meta_request;
}

static struct aws_s3_meta_request *s_s3_client_meta_request_factory_default(
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

        size_t client_part_size = client->part_size;
        size_t client_max_part_size = client->max_part_size;

        if (client_part_size < g_s3_min_upload_part_size) {
            AWS_LOGF_ERROR(
                AWS_LS_S3_META_REQUEST,
                "Client config part size of %" PRIu64 " is less than the minimum upload part size of %" PRIu64
                ". Using to the minimum part-size for upload.",
                (uint64_t)client_part_size,
                (uint64_t)g_s3_min_upload_part_size);

            client_part_size = g_s3_min_upload_part_size;
        }

        if (client_max_part_size < g_s3_min_upload_part_size) {
            AWS_LOGF_ERROR(
                AWS_LS_S3_META_REQUEST,
                "Client config max part size of %" PRIu64 " is less than the minimum upload part size of %" PRIu64
                ". Clamping to the minimum part-size for upload.",
                (uint64_t)client_max_part_size,
                (uint64_t)g_s3_min_upload_part_size);

            client_max_part_size = g_s3_min_upload_part_size;
        }

        if (content_length < client_part_size) {
            return aws_s3_meta_request_default_new(client->allocator, client, content_length, options);
        }

        uint64_t part_size_uint64 = content_length / (uint64_t)g_s3_max_num_upload_parts;

        if (part_size_uint64 > SIZE_MAX) {
            AWS_LOGF_ERROR(
                AWS_LS_S3_META_REQUEST,
                "Could not create auto-ranged-put meta request; required part size of %" PRIu64
                " bytes is too large for platform.",
                part_size_uint64);

            aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
            return NULL;
        }

        size_t part_size = (size_t)part_size_uint64;

        if (part_size > client_max_part_size) {
            AWS_LOGF_ERROR(
                AWS_LS_S3_META_REQUEST,
                "Could not create auto-ranged-put meta request; required part size for put request is %" PRIu64
                ", but current maximum part size is %" PRIu64,
                (uint64_t)part_size,
                (uint64_t)client_max_part_size);
            aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
            return NULL;
        }

        if (part_size < client_part_size) {
            part_size = client_part_size;
        }

        uint32_t num_parts = (uint32_t)(content_length / part_size);

        if ((content_length % part_size) > 0) {
            ++num_parts;
        }

        return aws_s3_meta_request_auto_ranged_put_new(
            client->allocator, client, part_size, content_length, num_parts, options);
    } else if (options->type == AWS_S3_META_REQUEST_TYPE_DEFAULT) {
        return aws_s3_meta_request_default_new(client->allocator, client, content_length, options);
    } else {
        AWS_FATAL_ASSERT(false);
    }

    return NULL;
}

static void s_s3_client_vip_shutdown_callback(void *user_data) {
    AWS_PRECONDITION(user_data);
    struct aws_s3_client *client = user_data;

    aws_s3_client_lock_synced_data(client);
    --client->synced_data.allocated_vip_count;
    s_s3_client_schedule_process_work_synced(client);
    aws_s3_client_unlock_synced_data(client);
}

static void s_s3_client_push_meta_request_synced(
    struct aws_s3_client *client,
    struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(meta_request);
    ASSERT_SYNCED_DATA_LOCK_HELD(client);

    struct aws_s3_meta_request_work *meta_request_work =
        aws_mem_calloc(client->sba_allocator, 1, sizeof(struct aws_s3_meta_request_work));

    aws_s3_meta_request_acquire(meta_request);
    meta_request_work->meta_request = meta_request;
    aws_linked_list_push_back(&client->synced_data.pending_meta_request_work, &meta_request_work->node);
}

static void s_s3_client_schedule_process_work_synced(struct aws_s3_client *client) {
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(client->vtable);
    AWS_PRECONDITION(client->vtable->schedule_process_work_synced);

    ASSERT_SYNCED_DATA_LOCK_HELD(client);

    client->vtable->schedule_process_work_synced(client);
}

static void s_s3_client_schedule_process_work_synced_default(struct aws_s3_client *client) {
    ASSERT_SYNCED_DATA_LOCK_HELD(client);

    if (client->synced_data.process_work_task_scheduled) {
        return;
    }

    aws_task_init(
        &client->synced_data.process_work_task, s_s3_client_process_work_task, client, "s3_client_process_work_task");

    aws_event_loop_schedule_task_now(client->process_work_event_loop, &client->synced_data.process_work_task);

    client->synced_data.process_work_task_scheduled = true;
}

void aws_s3_client_schedule_process_work(struct aws_s3_client *client) {
    AWS_PRECONDITION(client);

    aws_s3_client_lock_synced_data(client);
    s_s3_client_schedule_process_work_synced(client);
    aws_s3_client_unlock_synced_data(client);
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

    /* Client keeps a reference to the event loop group; a 'canceled' status should not happen.*/
    AWS_ASSERT(task_status == AWS_TASK_STATUS_RUN_READY);

    struct aws_s3_client *client = arg;
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(client->vtable);
    AWS_PRECONDITION(client->vtable->process_work);

    client->vtable->process_work(client);
}

static void s_s3_client_process_work_default(struct aws_s3_client *client) {
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(client->vtable);
    AWS_PRECONDITION(client->vtable->finish_destroy);

    bool client_active = false;
    bool invalid_endpoint = false;

    struct aws_linked_list meta_request_work_list;
    aws_linked_list_init(&meta_request_work_list);

    /*******************/
    /* Step 1: Move relevant data into thread local memory. */
    /*******************/
    AWS_LOGF_DEBUG(
        AWS_LS_S3_CLIENT,
        "id=%p s_s3_client_process_work_default - Moving relevant synced_data into threaded_data.",
        (void *)client);
    aws_s3_client_lock_synced_data(client);

    client_active = client->synced_data.active != 0;
    invalid_endpoint = client->synced_data.invalid_endpoint != 0;

    /* Once we exit this mutex, someone can reschedule this task. */
    client->synced_data.process_work_task_scheduled = false;
    client->synced_data.process_work_task_in_progress = true;

    aws_linked_list_swap_contents(&meta_request_work_list, &client->synced_data.pending_meta_request_work);

    aws_linked_list_move_all_back(
        &client->threaded_data.idle_vip_connections, &client->synced_data.pending_vip_connection_updates);

    uint32_t num_requests_queued =
        aws_s3_client_queue_requests_threaded(client, &client->synced_data.prepared_requests, false);

    {
        int sub_result = aws_sub_u32_checked(
            client->threaded_data.num_requests_being_prepared,
            num_requests_queued,
            &client->threaded_data.num_requests_being_prepared);

        AWS_ASSERT(sub_result == AWS_OP_SUCCESS);
        (void)sub_result;
    }

    {
        int sub_result = aws_sub_u32_checked(
            client->threaded_data.num_requests_being_prepared,
            client->synced_data.num_failed_prepare_requests,
            &client->threaded_data.num_requests_being_prepared);

        client->synced_data.num_failed_prepare_requests = 0;

        AWS_ASSERT(sub_result == AWS_OP_SUCCESS);
        (void)sub_result;
    }

    aws_s3_client_unlock_synced_data(client);

    /*******************/
    /* Step 2: Push meta requests into the thread local list if they haven't already been scheduled. */
    /*******************/
    AWS_LOGF_DEBUG(
        AWS_LS_S3_CLIENT, "id=%p s_s3_client_process_work_default - Processing any new meta requests.", (void *)client);

    while (!aws_linked_list_empty(&meta_request_work_list)) {
        struct aws_linked_list_node *node = aws_linked_list_pop_back(&meta_request_work_list);
        struct aws_s3_meta_request_work *meta_request_work =
            AWS_CONTAINER_OF(node, struct aws_s3_meta_request_work, node);

        AWS_FATAL_ASSERT(meta_request_work != NULL);
        AWS_FATAL_ASSERT(meta_request_work->meta_request != NULL);

        struct aws_s3_meta_request *meta_request = meta_request_work->meta_request;

        if (!meta_request->client_process_work_threaded_data.scheduled) {
            aws_linked_list_push_back(
                &client->threaded_data.meta_requests, &meta_request->client_process_work_threaded_data.node);

            meta_request->client_process_work_threaded_data.scheduled = true;
        } else {
            aws_s3_meta_request_release(meta_request);
            meta_request = NULL;
        }

        aws_mem_release(client->sba_allocator, meta_request_work);
    }

    /*******************/
    /* Step 3: Update relevant meta requests and connections. */
    /*******************/
    {
        if (invalid_endpoint) {
            AWS_LOGF_DEBUG(
                AWS_LS_S3_CLIENT, "id=%p Updating meta requests with 'no-endpoint-connections' flag.", (void *)client);
            aws_s3_client_update_meta_requests_threaded(
                client, AWS_S3_META_REQUEST_UPDATE_FLAG_NO_ENDPOINT_CONNECTIONS);
        } else {

            /* We first update meta requests with the "conservative" flag. This will tell each meta request that
             * connections can still potentially be shared between meta requests.  (ie: an individual meta request
             * shouldn't necessarily try to issue as many requests as it can, unless thats okay for performance.)
             */
            AWS_LOGF_DEBUG(AWS_LS_S3_CLIENT, "id=%p Updating meta requests with 'conservative' flag.", (void *)client);
            aws_s3_client_update_meta_requests_threaded(client, AWS_S3_META_REQUEST_UPDATE_FLAG_CONSERVATIVE);

            /* If we still have connections left over, then don't pass any flags, indicating to meta requests that any
             * logic for better sharing connections between meta requests can now be ignored. */
            AWS_LOGF_DEBUG(
                AWS_LS_S3_CLIENT, "id=%p Updating meta requests without 'conservative' flag.", (void *)client);
            aws_s3_client_update_meta_requests_threaded(client, 0);
        }

        /* Update connections, assigning requests where possible. */
        AWS_LOGF_DEBUG(
            AWS_LS_S3_CLIENT, "id=%p Updating vip connections, assigning requests where possible.", (void *)client);
        aws_s3_client_update_connections_threaded(client, client_active);
    }

    /*******************/
    /* Step 4: Log client stats. */
    /*******************/
    {
        uint32_t num_idle_connections = 0;

        for (struct aws_linked_list_node *node = aws_linked_list_begin(&client->threaded_data.idle_vip_connections);
             node != aws_linked_list_end(&client->threaded_data.idle_vip_connections);
             node = aws_linked_list_next(node)) {
            ++num_idle_connections;
        }

        uint32_t num_requests_in_flight = (uint32_t)aws_atomic_load_int(&client->stats.num_requests_in_flight);
        uint32_t num_requests_network_io = (uint32_t)aws_atomic_load_int(&client->stats.num_requests_network_io);
        uint32_t num_requests_stream_queued_waiting =
            (uint32_t)aws_atomic_load_int(&client->stats.num_requests_stream_queued_waiting);
        uint32_t num_requests_streaming = (uint32_t)aws_atomic_load_int(&client->stats.num_requests_streaming);
        uint32_t total_approx_requests = num_requests_network_io + num_requests_stream_queued_waiting +
                                         num_requests_streaming + client->threaded_data.num_requests_being_prepared +
                                         client->threaded_data.request_queue_size;

        uint32_t num_allocated_connections =
            (uint32_t)aws_atomic_load_int(&client->stats.num_allocated_vip_connections);
        uint32_t num_active_vip_connections = (uint32_t)aws_atomic_load_int(&client->stats.num_active_vip_connections);

        AWS_LOGF(
            s_log_level_client_stats,
            AWS_LS_S3_CLIENT_STATS,
            "id=%p Requests-in-flight(approx/exact):%d/%d  Requests-preparing:%d  Requests_queued:%d  "
            "Requests-network:%d  Requests-waiting:%d  Requests-streaming:%d  Idle-connections:%d  "
            "Allocated-connections:%d  Active-connections:%d",
            (void *)client,
            total_approx_requests,
            num_requests_in_flight,
            client->threaded_data.num_requests_being_prepared,
            client->threaded_data.request_queue_size,
            num_requests_network_io,
            num_requests_stream_queued_waiting,
            num_requests_streaming,
            num_idle_connections,
            num_allocated_connections,
            num_active_vip_connections);
    }

    /*******************/
    /* Step 5: Check for client shutdown. */
    /*******************/
    {
        aws_s3_client_lock_synced_data(client);
        client->synced_data.process_work_task_in_progress = false;

        /* This flag should never be set twice. If it was, that means a double-free could occur.*/
        AWS_ASSERT(!client->synced_data.finish_destroy);

        bool finish_destroy = client->synced_data.active == false &&
                              client->synced_data.waiting_for_first_host_resolve_callback == false &&
                              client->synced_data.start_destroy_executing == false &&
                              client->synced_data.allocated_vip_count == 0 &&
                              client->synced_data.host_listener_allocated == false &&
                              client->synced_data.body_streaming_elg_allocated == false &&
                              client->synced_data.process_work_task_scheduled == false &&
                              client->synced_data.process_work_task_in_progress == false;

        client->synced_data.finish_destroy = finish_destroy;

        if (!client->synced_data.active) {
            AWS_LOGF_DEBUG(
                AWS_LS_S3_CLIENT,
                "id=%p Client shutdown progress: waiting_for_first_host_resolve_callback=%d  "
                "starting_destroy_executing=%d "
                " allocated_vip_count=%d  host_listener_allocated=%d  body_streaming_elg_allocated=%d  "
                "process_work_task_scheduled=%d  process_work_task_in_progress=%d  finish_destroy=%d",
                (void *)client,
                (int)client->synced_data.waiting_for_first_host_resolve_callback,
                (int)client->synced_data.start_destroy_executing,
                (int)client->synced_data.allocated_vip_count,
                (int)client->synced_data.host_listener_allocated,
                (int)client->synced_data.body_streaming_elg_allocated,
                (int)client->synced_data.process_work_task_scheduled,
                (int)client->synced_data.process_work_task_in_progress,
                (int)client->synced_data.finish_destroy);
        }

        aws_s3_client_unlock_synced_data(client);

        if (finish_destroy) {
            client->vtable->finish_destroy(client);
        }
    }
}

static void s_s3_client_prepare_callback_queue_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    int error_code,
    void *user_data);

void aws_s3_client_update_meta_requests_threaded(struct aws_s3_client *client, uint32_t meta_request_update_flags) {
    AWS_PRECONDITION(client);

    /* If we have an invalid endpoint, then try to finish any work in progress. */
    /* TODO once we have multiple bucket support, this should only stop meta requests attached to bad endpoints. */
    if ((meta_request_update_flags & AWS_S3_META_REQUEST_UPDATE_FLAG_NO_ENDPOINT_CONNECTIONS) != 0) {

        /* Make sure that the request queue is empty. */
        while (!aws_linked_list_empty(&client->threaded_data.request_queue)) {
            struct aws_s3_request *request = aws_s3_client_dequeue_request_threaded(client);
            aws_s3_meta_request_finished_request(request->meta_request, request, AWS_ERROR_S3_NO_ENDPOINT_CONNECTIONS);
            aws_s3_request_release(request);
        }

        struct aws_linked_list_node *meta_request_node = aws_linked_list_begin(&client->threaded_data.meta_requests);

        while (meta_request_node != aws_linked_list_end(&client->threaded_data.meta_requests)) {
            struct aws_s3_meta_request *meta_request =
                AWS_CONTAINER_OF(meta_request_node, struct aws_s3_meta_request, client_process_work_threaded_data);

            struct aws_linked_list_node *next_meta_request_node = aws_linked_list_next(meta_request_node);

            bool work_remaining =
                aws_s3_meta_request_update(meta_request, AWS_S3_META_REQUEST_UPDATE_FLAG_NO_ENDPOINT_CONNECTIONS, NULL);

            /* While no meta request should be spinning up new requests in this case, it is possible that there is
             * something outside of network connectivity that a meta request is waiting for. (example: parts being
             * streamed to the caller) This may require the work task function to be called multiple times before every
             * meta request can be removed. */
            if (!work_remaining) {
                s_s3_client_remove_meta_request_threaded(client, meta_request);
            }

            meta_request_node = next_meta_request_node;
        }

        return;
    }

    /* Don't spin up new work if we haven't made any connections. */
    if (aws_atomic_load_int(&client->stats.num_active_vip_connections) == 0 &&
        aws_linked_list_empty(&client->threaded_data.idle_vip_connections)) {
        return;
    }

    const uint32_t max_requests_in_flight = aws_s3_client_get_max_requests_in_flight(client);
    const uint32_t max_requests_prepare = aws_s3_client_get_max_requests_prepare(client);

    struct aws_linked_list meta_requests_work_remaining;
    aws_linked_list_init(&meta_requests_work_remaining);

    uint32_t num_requests_in_flight = (uint32_t)aws_atomic_load_int(&client->stats.num_requests_in_flight);

    /* While our number of prepared/queued requests is less than the max, and the total requests in flight is also less
     * than the maximum, and we have meta requests to get requests from, then try to prepare requests for being
     * queued. */
    while ((client->threaded_data.num_requests_being_prepared + client->threaded_data.request_queue_size) <
               max_requests_prepare &&
           num_requests_in_flight < max_requests_in_flight &&
           !aws_linked_list_empty(&client->threaded_data.meta_requests)) {

        struct aws_linked_list_node *meta_request_node = aws_linked_list_begin(&client->threaded_data.meta_requests);
        struct aws_s3_meta_request *meta_request =
            AWS_CONTAINER_OF(meta_request_node, struct aws_s3_meta_request, client_process_work_threaded_data);

        struct aws_s3_request *request = NULL;

        /* Try to grab the next request from the meta request. */
        bool work_remaining = aws_s3_meta_request_update(meta_request, meta_request_update_flags, &request);

        if (work_remaining) {
            /* If there is work remaining, but we didn't get a request back, take the meta request out of the list so
             * that we don't use it again during this function, with the intention of putting it back in the list before
             * this function ends. */
            if (request == NULL) {
                aws_linked_list_remove(&meta_request->client_process_work_threaded_data.node);
                aws_linked_list_push_back(
                    &meta_requests_work_remaining, &meta_request->client_process_work_threaded_data.node);
            } else {
                request->tracked_by_client = true;

                ++client->threaded_data.num_requests_being_prepared;

                num_requests_in_flight = (uint32_t)aws_atomic_fetch_add(&client->stats.num_requests_in_flight, 1) + 1;

                aws_s3_meta_request_prepare_request(
                    meta_request, request, s_s3_client_prepare_callback_queue_request, client);
            }
        } else {
            s_s3_client_remove_meta_request_threaded(client, meta_request);
        }
    }

    aws_linked_list_move_all_front(&client->threaded_data.meta_requests, &meta_requests_work_remaining);
}

static void s_s3_client_prepare_callback_queue_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    int error_code,
    void *user_data) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(request);

    struct aws_s3_client *client = user_data;
    AWS_PRECONDITION(client);

    if (error_code != AWS_ERROR_SUCCESS) {
        aws_s3_meta_request_finished_request(meta_request, request, error_code);

        aws_s3_request_release(request);
        request = NULL;
    }

    aws_s3_client_lock_synced_data(client);

    if (error_code == AWS_ERROR_SUCCESS) {
        aws_linked_list_push_back(&client->synced_data.prepared_requests, &request->node);
    } else {
        ++client->synced_data.num_failed_prepare_requests;
    }

    s_s3_client_schedule_process_work_synced(client);
    aws_s3_client_unlock_synced_data(client);
}

void aws_s3_client_update_connections_threaded(struct aws_s3_client *client, bool client_active) {
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(client->vtable);
    AWS_PRECONDITION(client->vtable->http_connection_is_open);

    struct aws_linked_list vip_connections_updates;
    aws_linked_list_init(&vip_connections_updates);

    struct aws_linked_list left_over_requests;
    aws_linked_list_init(&left_over_requests);

    /* Move all idle connections into our local update list. */
    aws_linked_list_swap_contents(&vip_connections_updates, &client->threaded_data.idle_vip_connections);

    /* Clean up any connections that are dead, and sort warm connections to the front of the list. */
    while (!aws_linked_list_empty(&vip_connections_updates)) {
        struct aws_linked_list_node *vip_connection_node = aws_linked_list_pop_front(&vip_connections_updates);
        struct aws_s3_vip_connection *vip_connection =
            AWS_CONTAINER_OF(vip_connection_node, struct aws_s3_vip_connection, node);

        aws_s3_client_set_vip_connection_active(client, vip_connection, false);

        size_t owning_vip_active = aws_atomic_load_int(&vip_connection->owning_vip->active);

        AWS_ASSERT(vip_connection->request == NULL);

        /* If the owning vip isn't active, and the client is either also inactive, or the connection is dead, clean it
         * up. */
        if (!owning_vip_active &&
            (!client_active || (vip_connection->http_connection == NULL ||
                                !client->vtable->http_connection_is_open(vip_connection->http_connection) ||
                                vip_connection->request_count >= s_s3_max_request_count_per_connection))) {

            aws_s3_client_set_vip_connection_warm(client, vip_connection, false);

            aws_s3_vip_connection_destroy(client, vip_connection);
        } else {
            if (vip_connection->is_warm) {
                aws_linked_list_push_front(&client->threaded_data.idle_vip_connections, vip_connection_node);
            } else {
                aws_linked_list_push_back(&client->threaded_data.idle_vip_connections, vip_connection_node);
            }
        }
    }

    /* While there are idle connections and requests in the queue, try to assign requests to idle connections. */
    while (!aws_linked_list_empty(&client->threaded_data.idle_vip_connections) &&
           !aws_linked_list_empty(&client->threaded_data.request_queue)) {

        struct aws_linked_list_node *vip_connection_node =
            aws_linked_list_pop_front(&client->threaded_data.idle_vip_connections);
        struct aws_s3_vip_connection *vip_connection =
            AWS_CONTAINER_OF(vip_connection_node, struct aws_s3_vip_connection, node);

        struct aws_s3_request *request = aws_s3_client_dequeue_request_threaded(client);

        uint32_t num_conns_per_vip = s_num_conns_per_vip_meta_request_look_up[request->meta_request->type];
        const uint32_t max_active_connections = aws_s3_client_get_max_active_connections(client, num_conns_per_vip);

        /* Unless the request is marked "always send", if this meta request has a finish result, then finish the request
         * now and release it. */
        if (!request->always_send && aws_s3_meta_request_has_finish_result(request->meta_request)) {
            /* Put the unused connection at the front of the list so that it is used in the next iteration.*/
            aws_linked_list_push_front(&client->threaded_data.idle_vip_connections, &vip_connection->node);

            aws_s3_meta_request_finished_request(request->meta_request, request, AWS_ERROR_S3_CANCELED);

            aws_s3_request_release(request);
            request = NULL;
        } else if (
            vip_connection->is_warm ||
            aws_atomic_load_int(&client->stats.num_active_vip_connections) < max_active_connections) {
            vip_connection->request = request;

            AWS_ASSERT(!vip_connection->is_active);

            aws_s3_client_set_vip_connection_active(client, vip_connection, true);
            aws_s3_client_set_vip_connection_warm(client, vip_connection, true);

            aws_atomic_fetch_add(&client->stats.num_requests_network_io, 1);

            s_s3_client_setup_vip_connection_retry_token(client, vip_connection);
        } else {
            /* Put the unused connection at the front of the list so that it is used in the next iteration.*/
            aws_linked_list_push_front(&client->threaded_data.idle_vip_connections, &vip_connection->node);

            /* Push the request into the left-over list to be used in a future call of this function. */
            aws_linked_list_push_back(&left_over_requests, &request->node);
        }
    }

    aws_s3_client_queue_requests_threaded(client, &left_over_requests, true);
}

static void s_s3_client_acquired_retry_token(
    struct aws_retry_strategy *retry_strategy,
    int error_code,
    struct aws_retry_token *token,
    void *user_data);

static void s_s3_client_retry_ready(struct aws_retry_token *token, int error_code, void *user_data);

static void s_s3_client_setup_vip_connection_retry_token_default(
    struct aws_s3_client *client,
    struct aws_s3_vip_connection *vip_connection);

static void s_s3_client_setup_vip_connection_retry_token(
    struct aws_s3_client *client,
    struct aws_s3_vip_connection *vip_connection) {
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(client->vtable);

    if (client->vtable->setup_vip_connection_retry_token) {
        client->vtable->setup_vip_connection_retry_token(client, vip_connection);
        return;
    }

    s_s3_client_setup_vip_connection_retry_token_default(client, vip_connection);
}

static void s_s3_client_setup_vip_connection_retry_token_default(
    struct aws_s3_client *client,
    struct aws_s3_vip_connection *vip_connection) {
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(vip_connection);

    struct aws_s3_request *request = vip_connection->request;
    AWS_PRECONDITION(request);

    struct aws_s3_meta_request *meta_request = request->meta_request;
    AWS_PRECONDITION(meta_request);

    AWS_ASSERT(vip_connection->retry_token == NULL);

    struct aws_byte_cursor host_header_value;
    AWS_ZERO_STRUCT(host_header_value);

    struct aws_http_headers *message_headers = aws_http_message_get_headers(meta_request->initial_request_message);
    AWS_ASSERT(message_headers);

    int get_header_result = aws_http_headers_get(message_headers, g_host_header_name, &host_header_value);
    AWS_ASSERT(get_header_result == AWS_OP_SUCCESS);
    (void)get_header_result;

    if (aws_retry_strategy_acquire_retry_token(
            client->retry_strategy, &host_header_value, s_s3_client_acquired_retry_token, vip_connection, 0)) {

        AWS_LOGF_ERROR(
            AWS_LS_S3_CLIENT,
            "id=%p Client could not acquire retry token for request %p due to error %d (%s)",
            (void *)client,
            (void *)request,
            aws_last_error_or_unknown(),
            aws_error_str(aws_last_error_or_unknown()));

        goto reset_vip_connection;
    }

    return;

reset_vip_connection:

    aws_s3_client_notify_connection_finished(
        client, vip_connection, aws_last_error_or_unknown(), AWS_S3_VIP_CONNECTION_FINISH_CODE_FAILED);
}

static void s_s3_client_acquired_retry_token(
    struct aws_retry_strategy *retry_strategy,
    int error_code,
    struct aws_retry_token *token,
    void *user_data) {

    AWS_PRECONDITION(retry_strategy);
    (void)retry_strategy;

    struct aws_s3_vip_connection *vip_connection = user_data;
    AWS_PRECONDITION(vip_connection);

    struct aws_s3_request *request = vip_connection->request;
    AWS_PRECONDITION(request);

    struct aws_s3_vip *vip = vip_connection->owning_vip;
    AWS_PRECONDITION(vip);

    struct aws_s3_client *client = vip->owning_client;
    AWS_PRECONDITION(client);

    if (error_code != AWS_ERROR_SUCCESS) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_CLIENT,
            "id=%p Client could not get retry token for vip connection %p processing request %p due to error %d (%s)",
            (void *)client,
            (void *)vip_connection,
            (void *)request,
            error_code,
            aws_error_str(error_code));

        goto error_clean_up;
    }

    AWS_ASSERT(token);

    vip_connection->retry_token = token;

    s_s3_client_acquire_http_connection(client, vip_connection);

    return;

error_clean_up:

    aws_s3_client_notify_connection_finished(
        client, vip_connection, error_code, AWS_S3_VIP_CONNECTION_FINISH_CODE_FAILED);
}

static bool s_s3_client_http_connection_is_open(const struct aws_http_connection *connection) {
    return aws_http_connection_is_open(connection);
}

static void s_s3_client_acquire_http_connection(
    struct aws_s3_client *client,
    struct aws_s3_vip_connection *vip_connection) {

    AWS_PRECONDITION(client);
    AWS_PRECONDITION(client->vtable);
    AWS_PRECONDITION(client->vtable->acquire_http_connection);

    client->vtable->acquire_http_connection(client, vip_connection, s_s3_client_on_acquire_http_connection);
}

/* Handles getting an HTTP connection for the caller, given the vip_connection reference. */
static void s_s3_client_acquire_http_connection_default(
    struct aws_s3_client *client,
    struct aws_s3_vip_connection *vip_connection,
    aws_http_connection_manager_on_connection_setup_fn *on_connection_acquired_callback) {
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(client->vtable);
    AWS_PRECONDITION(client->vtable->http_connection_is_open);
    AWS_PRECONDITION(vip_connection);

    struct aws_http_connection **http_connection = &vip_connection->http_connection;
    uint32_t *connection_request_count = &vip_connection->request_count;

    struct aws_http_connection_manager *http_connection_manager = vip_connection->owning_vip->http_connection_manager;

    /* If we have a cached connection, see if we still want to use it. */
    if (*http_connection != NULL) {

        /* If we're at the max request count, set us up to get a new connection.  Also close the original connection
         * so that the connection manager doesn't reuse it.*/
        /* TODO maybe find a more visible way of preventing the
         * connection from going back into the pool. */
        if (*connection_request_count >= vip_connection->max_request_count) {
            aws_http_connection_close(*http_connection);

            /* TODO handle possible error here? */
            aws_http_connection_manager_release_connection(http_connection_manager, *http_connection);

            *http_connection = NULL;
            *connection_request_count = 0;

            AWS_LOGF_INFO(
                AWS_LS_S3_CLIENT, "id=%p VIP Connection %p hit request limit.", (void *)client, (void *)vip_connection);

        } else if (!client->vtable->http_connection_is_open(*http_connection)) {
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
        on_connection_acquired_callback(*http_connection, AWS_ERROR_SUCCESS, vip_connection);
    } else {
        aws_http_connection_manager_acquire_connection(
            http_connection_manager, on_connection_acquired_callback, vip_connection);
    }
}

static void s_s3_client_on_acquire_http_connection(
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

        goto error_clean_up;
    }

    struct aws_http_connection_manager *http_connection_manager = vip_connection->owning_vip->http_connection_manager;
    AWS_ASSERT(http_connection_manager);

    struct aws_http_connection **current_http_connection = &vip_connection->http_connection;

    /* If our cached connection is not equal to the one we just received, switch to the received one. */
    if (*current_http_connection != incoming_http_connection) {

        if (*current_http_connection != NULL) {
            aws_http_connection_manager_release_connection(http_connection_manager, *current_http_connection);
            *current_http_connection = NULL;
        }

        AWS_ASSERT(s_s3_max_request_count_per_connection > s_max_request_jitter_range);

        uint8_t jitter_value = 0;
        if (aws_device_random_u8(&jitter_value)) {
            AWS_LOGF_ERROR(
                AWS_LS_S3_CLIENT, "id=%p Could not get random value for request count jitter.", (void *)client);
        }

        jitter_value %= s_max_request_jitter_range;

        *current_http_connection = incoming_http_connection;
        vip_connection->request_count = 0;
        vip_connection->max_request_count = s_s3_max_request_count_per_connection - (uint32_t)jitter_value;

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

    aws_s3_meta_request_send_request(meta_request, vip_connection);

    return;

error_clean_up:

    aws_s3_client_notify_connection_finished(
        client, vip_connection, error_code, AWS_S3_VIP_CONNECTION_FINISH_CODE_RETRY);
}

/* Called by aws_s3_meta_request when it has finished using this VIP connection for a single request. */
void aws_s3_client_notify_connection_finished(
    struct aws_s3_client *client,
    struct aws_s3_vip_connection *vip_connection,
    int error_code,
    enum aws_s3_vip_connection_finish_code finish_code) {
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(vip_connection);

    struct aws_s3_request *request = vip_connection->request;
    AWS_PRECONDITION(request);

    struct aws_s3_meta_request *meta_request = request->meta_request;

    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(meta_request->initial_request_message);

    /* If we're trying to setup a retry... */
    if (finish_code == AWS_S3_VIP_CONNECTION_FINISH_CODE_RETRY) {

        if (vip_connection->retry_token == NULL) {
            AWS_LOGF_ERROR(
                AWS_LS_S3_CLIENT,
                "id=%p Client could not schedule retry of request %p for meta request %p",
                (void *)client,
                (void *)request,
                (void *)meta_request);

            goto reset_vip_connection;
        }

        if (aws_s3_meta_request_is_finished(meta_request)) {
            AWS_LOGF_DEBUG(
                AWS_LS_S3_CLIENT,
                "id=%p Client not scheduling retry of request %p for meta request %p with token %p because meta "
                "request has been flagged as finished.",
                (void *)client,
                (void *)request,
                (void *)meta_request,
                (void *)vip_connection->retry_token);

            goto reset_vip_connection;
        }

        AWS_LOGF_DEBUG(
            AWS_LS_S3_CLIENT,
            "id=%p Client scheduling retry of request %p for meta request %p with token %p.",
            (void *)client,
            (void *)request,
            (void *)meta_request,
            (void *)vip_connection->retry_token);

        enum aws_retry_error_type error_type = AWS_RETRY_ERROR_TYPE_TRANSIENT;

        switch (error_code) {
            case AWS_ERROR_S3_INTERNAL_ERROR:
                error_type = AWS_RETRY_ERROR_TYPE_SERVER_ERROR;
                break;

            case AWS_ERROR_S3_SLOW_DOWN:
                error_type = AWS_RETRY_ERROR_TYPE_THROTTLING;
                break;
        }

        /* Ask the retry strategy to schedule a retry of the request. */
        if (aws_retry_strategy_schedule_retry(
                vip_connection->retry_token, error_type, s_s3_client_retry_ready, vip_connection)) {
            error_code = aws_last_error_or_unknown();

            AWS_LOGF_ERROR(
                AWS_LS_S3_CLIENT,
                "id=%p Client could not retry request %p for meta request %p with token %p due to error %d (%s)",
                (void *)client,
                (void *)request,
                (void *)meta_request,
                (void *)vip_connection->retry_token,
                error_code,
                aws_error_str(error_code));

            goto reset_vip_connection;
        }

        return;
    }

reset_vip_connection:

    if (vip_connection->retry_token != NULL) {
        /* If we have a retry token and successfully finished, record that success. */
        if (finish_code == AWS_S3_VIP_CONNECTION_FINISH_CODE_SUCCESS) {
            aws_retry_token_record_success(vip_connection->retry_token);
        }

        aws_retry_token_release(vip_connection->retry_token);
        vip_connection->retry_token = NULL;
    }

    /* If we weren't successful, and we're here, that means this failure is not eligible for a retry. So finish the
     * meta request, and close our HTTP connection. */
    if (finish_code != AWS_S3_VIP_CONNECTION_FINISH_CODE_SUCCESS) {
        if (vip_connection->http_connection != NULL) {
            aws_http_connection_close(vip_connection->http_connection);
        }
    }

    aws_atomic_fetch_sub(&client->stats.num_requests_network_io, 1);

    aws_s3_meta_request_finished_request(meta_request, request, error_code);

    /* Grab a reference to the meta request since we got it from the request, and we want to use after we release the
     * request.*/
    aws_s3_meta_request_acquire(meta_request);

    /* Get rid of the attached request. */
    aws_s3_request_release(vip_connection->request);
    vip_connection->request = NULL;

    /* Throw this VIP Connection structure back into the update list. */
    aws_s3_client_lock_synced_data(client);
    aws_linked_list_push_back(&client->synced_data.pending_vip_connection_updates, &vip_connection->node);
    s_s3_client_schedule_process_work_synced(client);
    aws_s3_client_unlock_synced_data(client);

    aws_s3_meta_request_release(meta_request);
}

static void s_s3_client_prepare_request_callback_retry_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    int error_code,
    void *user_data);

static void s_s3_client_retry_ready(struct aws_retry_token *token, int error_code, void *user_data) {
    AWS_PRECONDITION(token);
    (void)token;

    struct aws_s3_vip_connection *vip_connection = user_data;
    AWS_PRECONDITION(vip_connection);
    AWS_PRECONDITION(vip_connection->owning_vip);

    struct aws_s3_client *client = vip_connection->owning_vip->owning_client;
    AWS_PRECONDITION(client);

    struct aws_s3_request *request = vip_connection->request;
    AWS_PRECONDITION(request);

    struct aws_s3_meta_request *meta_request = request->meta_request;
    AWS_PRECONDITION(meta_request);

    /* If we couldn't retry this request, then bail on the entire meta request. */
    if (error_code != AWS_ERROR_SUCCESS) {

        AWS_LOGF_ERROR(
            AWS_LS_S3_CLIENT,
            "id=%p Client could not retry request %p for meta request %p due to error %d (%s)",
            (void *)client,
            (void *)meta_request,
            (void *)request,
            error_code,
            aws_error_str(error_code));

        goto error_clean_up;
    }

    AWS_LOGF_DEBUG(
        AWS_LS_S3_META_REQUEST,
        "id=%p Client retrying request %p for meta request %p on connection %p with retry token %p",
        (void *)client,
        (void *)request,
        (void *)meta_request,
        (void *)vip_connection,
        (void *)vip_connection->retry_token);

    vip_connection->is_retry = true;

    aws_s3_meta_request_prepare_request(
        meta_request, request, s_s3_client_prepare_request_callback_retry_request, vip_connection);

    return;

error_clean_up:

    aws_s3_client_notify_connection_finished(
        client, vip_connection, error_code, AWS_S3_VIP_CONNECTION_FINISH_CODE_FAILED);
}

/* Called by aws_s3_request when it has finished being destroyed */
void aws_s3_client_notify_request_destroyed(struct aws_s3_client *client, struct aws_s3_request *request) {
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(request);

    if (request->tracked_by_client) {
        aws_s3_client_lock_synced_data(client);
        aws_atomic_fetch_sub(&client->stats.num_requests_in_flight, 1);
        s_s3_client_schedule_process_work_synced(client);
        aws_s3_client_unlock_synced_data(client);
    }
}

static void s_s3_client_prepare_request_callback_retry_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    int error_code,
    void *user_data) {
    AWS_PRECONDITION(meta_request);
    (void)meta_request;

    AWS_PRECONDITION(request);
    (void)request;

    struct aws_s3_vip_connection *vip_connection = user_data;
    AWS_PRECONDITION(vip_connection);

    struct aws_s3_vip *owning_vip = vip_connection->owning_vip;
    AWS_PRECONDITION(owning_vip);

    struct aws_s3_client *client = owning_vip->owning_client;
    AWS_PRECONDITION(client);

    if (error_code == AWS_ERROR_SUCCESS) {
        AWS_ASSERT(vip_connection->retry_token);

        s_s3_client_acquired_retry_token(
            client->retry_strategy, AWS_ERROR_SUCCESS, vip_connection->retry_token, vip_connection);
    } else {
        aws_s3_client_notify_connection_finished(
            client, vip_connection, error_code, AWS_S3_VIP_CONNECTION_FINISH_CODE_FAILED);
    }
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

        aws_s3_client_lock_synced_data(client);
        client->synced_data.invalid_endpoint = true;
        s_s3_client_schedule_process_work_synced(client);
        aws_s3_client_unlock_synced_data(client);
    } else {
        AWS_ASSERT(host_addresses);

        if (aws_s3_client_add_vips(client, host_addresses)) {
            int last_error_code = aws_last_error_or_unknown();
            AWS_LOGF_ERROR(
                AWS_LS_S3_CLIENT,
                "id=%p Error %d occurred whileadding VIPs (%s)",
                (void *)client,
                last_error_code,
                aws_error_str(last_error_code));
        }
    }

    aws_s3_client_lock_synced_data(client);
    client->synced_data.waiting_for_first_host_resolve_callback = false;
    s_s3_client_schedule_process_work_synced(client);
    aws_s3_client_unlock_synced_data(client);
}

int aws_s3_client_add_vips(struct aws_s3_client *client, const struct aws_array_list *host_addresses) {
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(client->vtable);
    AWS_PRECONDITION(client->vtable->add_vips);

    return client->vtable->add_vips(client, host_addresses);
}

static int s_s3_client_add_vips_default(struct aws_s3_client *client, const struct aws_array_list *host_addresses) {
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(host_addresses);

    struct aws_s3_vip *vip = NULL;
    int result = AWS_OP_SUCCESS;

    aws_s3_client_lock_synced_data(client);

    if (!client->synced_data.active) {
        goto unlock;
    }

    struct aws_byte_cursor server_name = aws_byte_cursor_from_string(client->synced_data.endpoint);
    bool vip_added = false;

    for (size_t address_index = 0; address_index < aws_array_list_length(host_addresses); ++address_index) {

        if (client->synced_data.active_vip_count >= client->ideal_vip_count) {
            break;
        }

        if (client->synced_data.allocated_vip_count >= aws_s3_client_get_max_allocated_vip_count(client)) {
            AWS_LOGF_WARN(
                AWS_LS_S3_CLIENT,
                "id=%p Allocated VIP count (%d) is greater than or equal to the maximum amount of allowed allocated "
                "VIPs (%d). Waiting for enough VIPs to clean up before accepting any new addresses.",
                (void *)client,
                client->synced_data.allocated_vip_count,
                client->ideal_vip_count);
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
        if (aws_s3_find_vip(&client->synced_data.vips, &host_address_byte_cursor) != NULL) {
            continue;
        }

        struct aws_linked_list vip_connections;
        aws_linked_list_init(&vip_connections);

        /* Allocate the new VIP. */
        vip = aws_s3_vip_new(
            client,
            &host_address_byte_cursor,
            &server_name,
            g_max_num_connections_per_vip,
            &vip_connections,
            s_s3_client_vip_shutdown_callback,
            client);

        if (vip == NULL) {
            result = AWS_OP_ERR;
            break;
        }

        aws_linked_list_move_all_back(&client->synced_data.pending_vip_connection_updates, &vip_connections);

        ++client->synced_data.allocated_vip_count;
        ++client->synced_data.active_vip_count;

        aws_linked_list_push_back(&client->synced_data.vips, &vip->node);

        AWS_LOGF_INFO(
            AWS_LS_S3_CLIENT,
            "id=%p Initiating creation of VIP with address '%s', total active vip count %d",
            (void *)client,
            (const char *)host_address_byte_cursor.ptr,
            client->synced_data.active_vip_count);

        vip_added = true;
    }

    if (vip_added) {
        s_s3_client_schedule_process_work_synced(client);
    }

unlock:

    aws_s3_client_unlock_synced_data(client);

    return result;
}

void aws_s3_client_remove_vips(struct aws_s3_client *client, const struct aws_array_list *host_addresses) {
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(client->vtable);
    AWS_PRECONDITION(client->vtable->remove_vips);

    client->vtable->remove_vips(client, host_addresses);
}

static void s_s3_client_remove_vips_default(struct aws_s3_client *client, const struct aws_array_list *host_addresses) {
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(host_addresses);

    bool vips_destroyed = false;

    for (size_t address_index = 0; address_index < aws_array_list_length(host_addresses); ++address_index) {
        aws_s3_client_lock_synced_data(client);

        struct aws_host_address *host_address = NULL;
        aws_array_list_get_at_ptr(host_addresses, (void **)&host_address, address_index);

        struct aws_byte_cursor host_address_byte_cursor = aws_byte_cursor_from_string(host_address->address);
        struct aws_s3_vip *vip = aws_s3_find_vip(&client->synced_data.vips, &host_address_byte_cursor);

        if (vip != NULL) {
            aws_linked_list_remove(&vip->node);
            AWS_ASSERT(client->synced_data.active_vip_count > 0);
            --client->synced_data.active_vip_count;
        }

        aws_s3_client_unlock_synced_data(client);

        if (vip == NULL) {
            continue;
        }

        AWS_LOGF_INFO(
            AWS_LS_S3_CLIENT,
            "id=%p Removing VIP with address '%s', total active vip count %d",
            (void *)client,
            (const char *)host_address_byte_cursor.ptr,
            client->synced_data.active_vip_count);

        aws_s3_vip_start_destroy(vip);
        vips_destroyed = true;
    }

    if (vips_destroyed) {
        aws_s3_client_lock_synced_data(client);
        s_s3_client_schedule_process_work_synced(client);
        aws_s3_client_unlock_synced_data(client);
    }
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
    aws_s3_client_add_vips(client, host_addresses);
}

static void s_s3_client_host_listener_expired_address_callback(
    struct aws_host_listener *listener,
    const struct aws_array_list *host_addresses,
    void *user_data) {
    (void)listener;

    AWS_PRECONDITION(listener);
    AWS_PRECONDITION(host_addresses);
    AWS_PRECONDITION(user_data);

    struct aws_s3_client *client = user_data;
    aws_s3_client_remove_vips(client, host_addresses);
}

static void s_s3_client_host_listener_shutdown_callback(void *user_data) {
    AWS_PRECONDITION(user_data);
    struct aws_s3_client *client = user_data;

    aws_s3_client_lock_synced_data(client);
    client->synced_data.host_listener_allocated = false;
    s_s3_client_schedule_process_work_synced(client);
    aws_s3_client_unlock_synced_data(client);
}

static int s_s3_client_start_resolving_addresses(struct aws_s3_client *client) {
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(client->client_bootstrap);
    AWS_PRECONDITION(client->client_bootstrap->host_resolver);

    struct aws_host_resolver *host_resolver = client->client_bootstrap->host_resolver;

    struct aws_host_listener *host_listener = NULL;
    struct aws_host_listener_options options = {
        .host_name = aws_byte_cursor_from_string(client->synced_data.endpoint),
        .resolved_address_callback = s_s3_client_host_listener_resolved_address_callback,
        .expired_address_callback = s_s3_client_host_listener_expired_address_callback,
        .shutdown_callback = s_s3_client_host_listener_shutdown_callback,
        .pin_host_entry = true,
        .user_data = client,
    };

    bool listener_already_exists = false;
    bool error_occurred = false;

    aws_s3_client_lock_synced_data(client);

    if (client->synced_data.host_listener != NULL) {
        listener_already_exists = true;
        goto unlock;
    }

    host_listener = aws_host_resolver_add_host_listener(host_resolver, &options);

    if (host_listener == NULL) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_CLIENT,
            "id=%p: Error trying to add listener for endpoint %s",
            (void *)client,
            (const char *)client->synced_data.endpoint->bytes);
        error_occurred = true;
        goto unlock;
    }

    AWS_ASSERT(client->synced_data.active);

    client->synced_data.host_listener = host_listener;
    client->synced_data.host_listener_allocated = true;
    client->synced_data.waiting_for_first_host_resolve_callback = true;

unlock:
    aws_s3_client_unlock_synced_data(client);

    if (listener_already_exists) {
        return AWS_OP_SUCCESS;
    }

    if (error_occurred) {
        return AWS_OP_ERR;
    }

    struct aws_host_resolution_config host_resolver_config;
    AWS_ZERO_STRUCT(host_resolver_config);
    host_resolver_config.impl = aws_default_dns_resolve;
    host_resolver_config.max_ttl = s_dns_host_address_ttl_seconds;
    host_resolver_config.impl_data = client;

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
