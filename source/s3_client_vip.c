/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_client_impl.h"
#include "aws/s3/private/s3_meta_request_impl.h"
#include "aws/s3/private/s3_util.h"

#include <aws/common/assert.h>
#include <aws/common/environment.h>
#include <aws/common/string.h>
#include <aws/http/connection.h>
#include <aws/http/connection_manager.h>
#include <aws/http/request_response.h>
#include <aws/io/channel_bootstrap.h>
#include <aws/io/event_loop.h>
#include <aws/io/host_resolver.h>
#include <aws/io/socket.h>
#include <aws/io/tls_channel_handler.h>
#include <aws/io/uri.h>

#include <inttypes.h>
#include <math.h>

static const uint32_t s_connection_timeout_ms = 3000;
static const uint16_t s_http_port = 80;
static const uint16_t s_https_port = 443;
static size_t s_dns_host_address_ttl_seconds = 5 * 60;

const uint32_t g_num_connections_per_vip = 10;

AWS_STATIC_STRING_FROM_LITERAL(s_http_proxy_env_var, "HTTP_PROXY");

typedef void(s3_client_vip_update_synced_data_state_fn)(struct aws_s3_vip *vip);

/* Callback for when the vip's connection manager has shut down. */
static void s_s3_vip_http_connection_manager_shutdown_callback(void *user_data);

/* Used to atomically update vip state during clean-up and check for finishing shutdown. */
static void s_s3_vip_check_for_shutdown(struct aws_s3_vip *vip, s3_client_vip_update_synced_data_state_fn *update_fn);

/* Called by s_s3_vip_check_for_shutdown when all shutdown criteria for the vip has been met. */
static void s_s3_vip_finish_destroy(void *user_data);

void aws_s3_set_dns_ttl(size_t ttl) {
    s_dns_host_address_ttl_seconds = ttl;
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

static void s_s3_vip_set_conn_manager_shutdown(struct aws_s3_vip *vip) {
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

    s_s3_vip_check_for_shutdown(vip, s_s3_vip_set_conn_manager_shutdown);
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

static void s_s3_vip_sub_num_vip_connections_synced(struct aws_s3_vip *vip) {
    AWS_PRECONDITION(vip);
    AWS_PRECONDITION(vip->owning_client);
    ASSERT_SYNCED_DATA_LOCK_HELD(vip->owning_client);
    --vip->synced_data.num_vip_connections;
}

/* Destroy a VIP Connection structure. */
void aws_s3_vip_connection_destroy(struct aws_s3_client *client, struct aws_s3_vip_connection *vip_connection) {

    if (client == NULL) {
        AWS_ASSERT(vip_connection == NULL);
        return;
    }

    if (vip_connection == NULL) {
        return;
    }

    struct aws_s3_vip *owning_vip = vip_connection->owning_vip;

    AWS_LOGF_DEBUG(
        AWS_LS_S3_VIP_CONNECTION,
        "id=%p Destroying VIP Connection owned by vip %p.",
        (void *)vip_connection,
        (void *)owning_vip);

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
        aws_s3_client_schedule_process_work_synced(client);
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
}

int aws_s3_client_add_vips(struct aws_s3_client *client, const struct aws_array_list *host_addresses) {
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(client->vtable);
    AWS_PRECONDITION(client->vtable->add_vips);

    return client->vtable->add_vips(client, host_addresses);
}

static void s_s3_client_sub_vip_count_synced(struct aws_s3_client *client) {
    AWS_PRECONDITION(client);
    ASSERT_SYNCED_DATA_LOCK_HELD(client);
    --client->synced_data.allocated_vip_count;
}

static void s_s3_client_vip_shutdown_callback(void *user_data) {
    AWS_PRECONDITION(user_data);
    struct aws_s3_client *client = user_data;

    aws_s3_client_check_for_shutdown(client, s_s3_client_sub_vip_count_synced);
}

int aws_s3_client_add_vips_default(struct aws_s3_client *client, const struct aws_array_list *host_addresses) {
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
            g_num_connections_per_vip,
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
        aws_s3_client_schedule_process_work_synced(client);
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

void aws_s3_client_remove_vips_default(struct aws_s3_client *client, const struct aws_array_list *host_addresses) {
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
        aws_s3_client_schedule_process_work_synced(client);
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

static void s_s3_client_set_host_listener_shutdown_synced(struct aws_s3_client *client) {
    AWS_PRECONDITION(client);
    ASSERT_SYNCED_DATA_LOCK_HELD(client);

    AWS_LOGF_DEBUG(AWS_LS_S3_CLIENT, "id=%p: Host listener finished shutdown.", (void *)client);

    client->synced_data.host_listener_allocated = false;
}

static void s_s3_client_host_listener_shutdown_callback(void *user_data) {
    AWS_PRECONDITION(user_data);
    struct aws_s3_client *client = user_data;

    aws_s3_client_check_for_shutdown(client, s_s3_client_set_host_listener_shutdown_synced);
}

int aws_s3_client_start_resolving_addresses(struct aws_s3_client *client) {
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
