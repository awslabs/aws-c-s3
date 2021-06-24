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

AWS_STATIC_STRING_FROM_LITERAL(s_http_proxy_env_var, "HTTP_PROXY");

static int s_s3_endpoint_get_proxy_uri(struct aws_s3_endpoint *endpoint, struct aws_uri *proxy_uri);

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
    uint32_t max_connections);

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
        options->max_connections);

    if (endpoint->http_connection_manager == NULL) {
        goto error_cleanup;
    }

    endpoint->ref_count_zero_callback = options->ref_count_zero_callback;
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
    uint32_t max_connections) {
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

    struct aws_http_connection_manager_options manager_options;
    AWS_ZERO_STRUCT(manager_options);
    manager_options.bootstrap = client_bootstrap;
    manager_options.initial_window_size = SIZE_MAX;
    manager_options.socket_options = &socket_options;
    manager_options.host = host_name_cursor;
    manager_options.max_connections = max_connections;
    manager_options.shutdown_complete_callback = s_s3_endpoint_http_connection_manager_shutdown_callback;
    manager_options.shutdown_complete_user_data = endpoint;

    struct aws_uri proxy_uri;
    AWS_ZERO_STRUCT(proxy_uri);
    struct aws_http_proxy_options *proxy_options = NULL;
    struct aws_tls_connection_options *proxy_tls_options = NULL;
    struct aws_tls_connection_options *manager_tls_options = NULL;

    if (s_s3_endpoint_get_proxy_uri(endpoint, &proxy_uri) == AWS_OP_SUCCESS) {
        proxy_options = aws_mem_calloc(endpoint->allocator, 1, sizeof(struct aws_http_proxy_options));
        proxy_options->host = proxy_uri.host_name;
        proxy_options->port = proxy_uri.port;

        manager_options.proxy_options = proxy_options;
    } else if (aws_last_error() != AWS_ERROR_S3_PROXY_ENV_NOT_FOUND) {
        return NULL;
    }

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
        manager_options.port = s_https_port;
    } else {
        manager_options.port = s_http_port;
    }

    struct aws_http_connection_manager *http_connection_manager =
        aws_http_connection_manager_new(endpoint->allocator, &manager_options);

    if (manager_tls_options != NULL) {
        aws_tls_connection_options_clean_up(manager_tls_options);
        aws_mem_release(endpoint->allocator, manager_tls_options);
        manager_tls_options = NULL;
    }

    if (proxy_tls_options != NULL) {
        aws_tls_connection_options_clean_up(proxy_tls_options);
        aws_mem_release(endpoint->allocator, proxy_tls_options);
        proxy_tls_options = NULL;
    }

    if (proxy_options != NULL) {
        aws_mem_release(endpoint->allocator, proxy_options);
        proxy_options = NULL;
    }

    aws_uri_clean_up(&proxy_uri);

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

static int s_s3_endpoint_get_proxy_uri(struct aws_s3_endpoint *endpoint, struct aws_uri *proxy_uri) {
    AWS_PRECONDITION(endpoint);
    AWS_PRECONDITION(proxy_uri);

    struct aws_allocator *allocator = endpoint->allocator;
    AWS_PRECONDITION(allocator);

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
        AWS_LS_S3_ENDPOINT,
        "id=%p Found proxy URI %s in environment variable %s",
        (void *)endpoint,
        (const char *)proxy_uri_string->bytes,
        (const char *)env_variable_name->bytes);

    struct aws_byte_cursor proxy_uri_cursor = aws_byte_cursor_from_string(proxy_uri_string);

    if (aws_uri_init_parse(proxy_uri, allocator, &proxy_uri_cursor)) {
        AWS_LOGF_ERROR(AWS_LS_S3_ENDPOINT, "id=%p Could not parse found proxy URI.", (void *)endpoint);
        aws_raise_error(AWS_ERROR_S3_PROXY_PARSE_FAILED);
        goto clean_up;
    }

    if (aws_byte_cursor_eq_ignore_case(&proxy_uri->scheme, &aws_http_scheme_http)) {
        /* Nothing to do. */
    } else if (proxy_uri->scheme.len > 0) {
        AWS_LOGF_ERROR(AWS_LS_S3_ENDPOINT, "id=%p Proxy URI contains unsupported scheme.", (void *)endpoint);

        aws_raise_error(AWS_ERROR_S3_UNSUPPORTED_PROXY_SCHEME);
        goto clean_up;
    }

    result = AWS_OP_SUCCESS;

clean_up:

    aws_string_destroy(proxy_uri_string);
    return result;
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

static void s_s3_endpoint_ref_count_zero(void *user_data) {
    struct aws_s3_endpoint *endpoint = user_data;
    AWS_PRECONDITION(endpoint);

    if (endpoint->ref_count_zero_callback != NULL && !endpoint->ref_count_zero_callback(endpoint)) {
        return;
    }

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
