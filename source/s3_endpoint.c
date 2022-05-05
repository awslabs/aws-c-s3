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

static const uint32_t s_connection_timeout_ms = 3000;
static const uint16_t s_http_port = 80;
static const uint16_t s_https_port = 443;

static void s_s3_endpoint_on_host_resolver_address_resolved(
    struct aws_host_resolver *resolver,
    const struct aws_string *host_name,
    int err_code,
    const struct aws_array_list *host_addresses,
    void *user_data);

static struct aws_http_connection_manager *s_s3_endpoint_create_http_connection_manager(
    struct aws_s3_endpoint *endpoint,
    const struct aws_string *host_name,
    struct aws_client_bootstrap *client_bootstrap,
    const struct aws_tls_connection_options *tls_connection_options,
    uint32_t max_connections,
    uint16_t port);

static void s_s3_endpoint_http_connection_manager_shutdown_callback(void *user_data);

static void s_s3_endpoint_ref_count_zero(void *user_data);

struct aws_s3_endpoint *aws_s3_endpoint_new(
    struct aws_allocator *allocator,
    const struct aws_s3_endpoint_options *options) {
    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(options);
    AWS_PRECONDITION(options->host_name);

    struct aws_s3_endpoint *endpoint = aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_endpoint));
    aws_ref_count_init(&endpoint->ref_count, endpoint, s_s3_endpoint_ref_count_zero);

    endpoint->allocator = allocator;
    endpoint->host_name = options->host_name;

    struct aws_host_resolution_config host_resolver_config;
    AWS_ZERO_STRUCT(host_resolver_config);
    host_resolver_config.impl = aws_default_dns_resolve;
    host_resolver_config.max_ttl = options->dns_host_address_ttl_seconds;
    host_resolver_config.impl_data = NULL;

    if (aws_host_resolver_resolve_host(
            options->client_bootstrap->host_resolver,
            endpoint->host_name,
            s_s3_endpoint_on_host_resolver_address_resolved,
            &host_resolver_config,
            NULL)) {

        AWS_LOGF_ERROR(
            AWS_LS_S3_ENDPOINT,
            "id=%p: Error trying to resolve host for endpoint %s",
            (void *)endpoint,
            (const char *)endpoint->host_name->bytes);

        goto error_cleanup;
    }

    endpoint->http_connection_manager = s_s3_endpoint_create_http_connection_manager(
        endpoint,
        options->host_name,
        options->client_bootstrap,
        options->tls_connection_options,
        options->max_connections,
        options->port);

    if (endpoint->http_connection_manager == NULL) {
        goto error_cleanup;
    }

    endpoint->shutdown_callback = options->shutdown_callback;
    endpoint->user_data = options->user_data;

    return endpoint;

error_cleanup:

    aws_string_destroy(options->host_name);

    aws_mem_release(allocator, endpoint);

    return NULL;
}

static struct aws_http_connection_manager *s_s3_endpoint_create_http_connection_manager(
    struct aws_s3_endpoint *endpoint,
    const struct aws_string *host_name,
    struct aws_client_bootstrap *client_bootstrap,
    const struct aws_tls_connection_options *tls_connection_options,
    uint32_t max_connections,
    uint16_t port) {
    AWS_PRECONDITION(endpoint);
    AWS_PRECONDITION(client_bootstrap);
    AWS_PRECONDITION(host_name);

    struct aws_byte_cursor host_name_cursor = aws_byte_cursor_from_string(host_name);

    /* Try to set up an HTTP connection manager. */
    struct aws_socket_options socket_options;
    AWS_ZERO_STRUCT(socket_options);
    socket_options.type = AWS_SOCKET_STREAM;
    socket_options.domain = AWS_SOCKET_IPV4;
    socket_options.connect_timeout_ms = s_connection_timeout_ms;
    struct proxy_env_var_settings proxy_ev_settings;
    AWS_ZERO_STRUCT(proxy_ev_settings);
    /* Turn on envrionment variable for proxy by default */
    proxy_ev_settings.env_var_type = AWS_HPEV_ENABLE;

    struct aws_http_connection_manager_options manager_options;
    AWS_ZERO_STRUCT(manager_options);
    manager_options.bootstrap = client_bootstrap;
    manager_options.initial_window_size = SIZE_MAX;
    manager_options.socket_options = &socket_options;
    manager_options.host = host_name_cursor;
    manager_options.max_connections = max_connections;
    manager_options.shutdown_complete_callback = s_s3_endpoint_http_connection_manager_shutdown_callback;
    manager_options.shutdown_complete_user_data = endpoint;
    manager_options.proxy_ev_settings = &proxy_ev_settings;

    struct aws_tls_connection_options *manager_tls_options = NULL;

    if (tls_connection_options != NULL) {
        manager_tls_options = aws_mem_calloc(endpoint->allocator, 1, sizeof(struct aws_tls_connection_options));
        aws_tls_connection_options_copy(manager_tls_options, tls_connection_options);

        /* TODO fix this in the actual aws_tls_connection_options_set_server_name function. */
        if (manager_tls_options->server_name != NULL) {
            aws_string_destroy(manager_tls_options->server_name);
            manager_tls_options->server_name = NULL;
        }

        aws_tls_connection_options_set_server_name(manager_tls_options, endpoint->allocator, &host_name_cursor);

        manager_options.tls_connection_options = manager_tls_options;
        manager_options.port = port == 0 ? s_https_port : port;
    } else {
        manager_options.port = port == 0 ? s_http_port : port;
    }

    struct aws_http_connection_manager *http_connection_manager =
        aws_http_connection_manager_new(endpoint->allocator, &manager_options);

    if (manager_tls_options != NULL) {
        aws_tls_connection_options_clean_up(manager_tls_options);
        aws_mem_release(endpoint->allocator, manager_tls_options);
        manager_tls_options = NULL;
    }

    if (http_connection_manager == NULL) {
        AWS_LOGF_ERROR(AWS_LS_S3_ENDPOINT, "id=%p: Could not create http connection manager.", (void *)endpoint);
        return NULL;
    }

    AWS_LOGF_DEBUG(
        AWS_LS_S3_ENDPOINT,
        "id=%p: Created connection manager %p for endpoint",
        (void *)endpoint,
        (void *)endpoint->http_connection_manager);

    return http_connection_manager;
}

struct aws_s3_endpoint *aws_s3_endpoint_acquire(struct aws_s3_endpoint *endpoint) {
    AWS_PRECONDITION(endpoint);

    aws_ref_count_acquire(&endpoint->ref_count);

    return endpoint;
}

void aws_s3_endpoint_release(struct aws_s3_endpoint *endpoint) {
    if (endpoint == NULL) {
        return;
    }

    aws_ref_count_release(&endpoint->ref_count);
}

void aws_s3_client_endpoint_release(struct aws_s3_client *client, struct aws_s3_endpoint *endpoint) {
    AWS_PRECONDITION(endpoint);
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(endpoint->handled_by_client);

    /* BEGIN CRITICAL SECTION */
    {
        aws_s3_client_lock_synced_data(client);
        /* The last refcount to release */
        if (aws_atomic_load_int(&endpoint->ref_count.ref_count) == 1) {
            aws_hash_table_remove(&client->synced_data.endpoints, endpoint->host_name, NULL, NULL);
        }
        aws_s3_client_unlock_synced_data(client);
    }
    /* END CRITICAL SECTION */

    aws_ref_count_release(&endpoint->ref_count);
}

static void s_s3_endpoint_ref_count_zero(void *user_data) {
    struct aws_s3_endpoint *endpoint = user_data;
    AWS_PRECONDITION(endpoint);

    if (endpoint->http_connection_manager != NULL) {
        struct aws_http_connection_manager *http_connection_manager = endpoint->http_connection_manager;
        endpoint->http_connection_manager = NULL;
        aws_http_connection_manager_release(http_connection_manager);
    } else {
        s_s3_endpoint_http_connection_manager_shutdown_callback(endpoint->user_data);
    }
}

static void s_s3_endpoint_http_connection_manager_shutdown_callback(void *user_data) {
    struct aws_s3_endpoint *endpoint = user_data;
    AWS_ASSERT(endpoint);

    aws_s3_endpoint_shutdown_fn *shutdown_callback = endpoint->shutdown_callback;
    void *endpoint_user_data = endpoint->user_data;

    aws_mem_release(endpoint->allocator, endpoint);

    if (shutdown_callback != NULL) {
        shutdown_callback(endpoint_user_data);
    }
}

static void s_s3_endpoint_on_host_resolver_address_resolved(
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
