/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_vip.h"
#include "aws/s3/private/s3_client_impl.h"
#include "aws/s3/private/s3_meta_request_impl.h"
#include "aws/s3/private/s3_request.h"
#include "aws/s3/private/s3_vip_connection.h"
#include "aws/s3/private/s3_work_util.h"

#include <aws/auth/signable.h>
#include <aws/auth/signing.h>
#include <aws/auth/signing_config.h>
#include <aws/auth/signing_result.h>
#include <aws/common/clock.h>
#include <aws/common/string.h>
#include <aws/http/connection.h>
#include <aws/http/connection_manager.h>
#include <aws/http/request_response.h>
#include <aws/io/channel_bootstrap.h>
#include <aws/io/event_loop.h>
#include <aws/io/host_resolver.h>
#include <aws/io/socket.h>

const int32_t s_s3_vip_connection_timeout_seconds = 3; // TODO
const int32_t s_s3_vip_connection_port = 80;           // TODO

static struct aws_s3_vip *s_s3_client_vip_new(
    struct aws_allocator *allocator,
    struct aws_s3_client *client,
    struct aws_byte_cursor host_address);

/* Initiate de-allocating a VIP structure. */
static void s_s3_client_vip_destroy(struct aws_s3_client *client, struct aws_s3_vip *vip);

/* Callback for when a particular VIP connection has shutdown. */
static void s_s3_client_vip_connection_shutdown_callback(void *user_data);

/* Callback for when the VIP's HTTP Connection Manger has finished shutting down. */
static void s_s3_client_vip_http_connection_manager_shutdown_callback(void *user_data);

/* Allocates a new VIP structure for the client to use, given an address. */
static struct aws_s3_vip *s_s3_client_vip_new(
    struct aws_allocator *allocator,
    struct aws_s3_client *client,
    struct aws_byte_cursor host_address) {
    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(client);

    struct aws_s3_client_vip_pair *http_conn_manager_context = NULL;
    struct aws_s3_vip *vip = aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_vip));

    if (vip == NULL) {
        AWS_LOGF_ERROR(AWS_LS_S3_VIP, "Could not allocate aws_s3_vip structure for client id=%p.", (void *)client);
        goto error_clean_up;
    }

    /* Update our allocated VIP count.  We track this for clean up purposes. */
    ++client->num_vips_allocated;

    vip->allocator = allocator;

    /* Copy over the host address. */
    vip->host_address = aws_string_new_from_array(allocator, host_address.ptr, host_address.len);

    if (vip->host_address == NULL) {
        AWS_LOGF_ERROR(AWS_LS_S3_VIP, "id=%p: Could not allocate aws_s3_vip host address string.", (void *)vip);
        goto error_clean_up;
    }

    /* Setup the connection manager */
    http_conn_manager_context = aws_s3_client_vip_pair_new(allocator, client, vip);

    if (http_conn_manager_context == NULL) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_VIP, "id=%p: Could not allocate aws_s3_client_vip_pair for s_s3_client_vip_new.", (void *)vip);
        goto error_clean_up;
    }

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
    manager_options.shutdown_complete_user_data = http_conn_manager_context;

    vip->http_connection_manager = aws_http_connection_manager_new(allocator, &manager_options);

    if (vip->http_connection_manager == NULL) {

        /* If we couldn't allocate the connection manager, free the context, since we rely on the http connection
         * manager shutdown callback to free it, and it would otherwise leak. */
        if (http_conn_manager_context != NULL) {
            aws_s3_client_vip_pair_destroy(http_conn_manager_context);
            http_conn_manager_context = NULL;
        }

        AWS_LOGF_ERROR(AWS_LS_S3_VIP, "id=%p: Could not allocate aws_s3_vip connection manager.", (void *)vip);
        goto error_clean_up;
    }

    /* Initialize our array of connections. */
    if (aws_array_list_init_dynamic(
            &vip->vip_connections, allocator, client->ideal_vip_count, sizeof(struct aws_s3_vip_connection *))) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_VIP, "id=%p: Could not initialize array list of connections for aws_s3_vip.", (void *)vip);
        goto error_clean_up;
    }

    /* Setup all of our vip connections. */
    for (size_t conn_index = 0; conn_index < client->num_connections_per_vip; ++conn_index) {
        struct aws_s3_vip_connection_options vip_connection_options;
        AWS_ZERO_STRUCT(vip_connection_options);
        vip_connection_options.allocator = vip->allocator;
        vip_connection_options.event_loop = client->event_loop;
        vip_connection_options.region = aws_byte_cursor_from_string(client->region);
        vip_connection_options.credentials_provider = client->credentials_provider;
        vip_connection_options.http_connection_manager = vip->http_connection_manager;
        vip_connection_options.shutdown_callback = s_s3_client_vip_connection_shutdown_callback;

        // TODO are these client pairs overkill?  Maybe we should just cache them in diferent places.
        struct aws_s3_client_vip_pair *vip_connection_context = aws_s3_client_vip_pair_new(allocator, client, vip);

        if (vip_connection_context == NULL) {
            AWS_LOGF_ERROR(
                AWS_LS_S3_VIP,
                "id=%p: Could not allocate connection context for use with aws_s3_vip_connection.",
                (void *)vip);
            goto error_clean_up;
        }

        vip_connection_options.user_data = vip_connection_context;

        struct aws_s3_vip_connection *vip_connection = aws_s3_vip_connection_new(allocator, &vip_connection_options);

        if (vip_connection == NULL) {
            AWS_LOGF_ERROR(AWS_LS_S3_VIP, "id=%p: Could not allocate aws_s3_vip_connection.", (void *)vip);

            aws_s3_client_vip_pair_destroy(vip_connection_context);
            vip_connection_context = NULL;

            goto error_clean_up;
        }

        ++vip->num_allocated_vip_connections;

        if (aws_array_list_push_back(&vip->vip_connections, &vip_connection)) {
            AWS_LOGF_ERROR(
                AWS_LS_S3_VIP,
                "id=%p Could not push VIP connection %p to VIP's connection list.",
                (void *)vip,
                (void *)vip_connection);

            aws_s3_client_vip_pair_destroy(vip_connection_context);
            vip_connection_context = NULL;

            aws_s3_vip_connection_release(vip_connection);
            vip_connection = NULL;

            goto error_clean_up;
        }
    }

    return vip;

error_clean_up:

    /* Clean up whatever we were able to allocate. */
    if (vip != NULL) {
        s_s3_client_vip_destroy(client, vip);
        vip = NULL;
    }

    return NULL;
}

/* Releases the memory for a vip structure. */
static void s_s3_client_vip_destroy(struct aws_s3_client *client, struct aws_s3_vip *vip) {
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(vip);

    aws_s3_client_set_vip_state(client, vip, AWS_S3_VIP_STATE_CLEAN_UP);
}

/* Quick check to determine if we're already in a clean up state. */
bool aws_s3_client_vip_is_cleaning_up(struct aws_s3_client *client, const struct aws_s3_vip *vip) {
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(vip);

    return vip->state >= AWS_S3_VIP_STATE_CLEAN_UP && vip->state <= AWS_S3_VIP_STATE_CLEAN_UP_FINISH_RELEASE;
}

/* Function for setting up a new VIP. */
int aws_s3_client_add_vip(struct aws_s3_client *client, struct aws_byte_cursor host_address) {
    AWS_PRECONDITION(client);

    int op_result = AWS_OP_SUCCESS;
    struct aws_hash_element *vip_hash_element = NULL;

    /* Copy the passed in string*/
    struct aws_string *host_address_string =
        aws_string_new_from_array(client->allocator, host_address.ptr, host_address.len);

    if (host_address_string == NULL) {
        AWS_LOGF_ERROR(AWS_LS_S3_CLIENT, "id=%p: Could not allocate host address string for VIP.", (void *)client);

        goto clean_up;
    }

    /* Find that string in the has table. */
    aws_hash_table_find(&client->vips_table, host_address_string, &vip_hash_element);

    /* If we didn't find a match in the table, we have a VIP to add! */
    if (vip_hash_element == NULL) {
        AWS_LOGF_INFO(
            AWS_LS_S3_CLIENT, "id=%p: Creating new VIP for address %s", (void *)client, (const char *)host_address.ptr);

        /* Allocate the new VIP. */
        struct aws_s3_vip *vip = s_s3_client_vip_new(client->allocator, client, host_address);

        if (vip == NULL) {
            AWS_LOGF_ERROR(
                AWS_LS_S3_CLIENT,
                "id=%p: Could not successfully create VIP for address %s",
                (void *)client,
                (const char *)host_address.ptr);

            op_result = AWS_OP_ERR;
            goto clean_up;
        }

        /* "Move" the copied string into the hash table. */
        if (aws_hash_table_put(&client->vips_table, host_address_string, vip, NULL)) {
            AWS_LOGF_ERROR(
                AWS_LS_S3_CLIENT,
                "id=%p: Could not insert VIP for address %s into VIP table.",
                (void *)client,
                (const char *)host_address.ptr);

            op_result = AWS_OP_ERR;
            s_s3_client_vip_destroy(client, vip);
            goto clean_up;
        }

        /* String is now owned by the hash table, so NULL out the original reference to prevent clean up. */
        host_address_string = NULL;

        /* Push the VIP into our list. */
        aws_linked_list_push_back(&client->vips, &vip->node);

        /* Increment the number of allocated VIP's. */
        ++client->num_active_vips;

        /* Push all existing meta requests into the connections of the new VIP. */
        for (size_t meta_request_index = 0; meta_request_index < aws_array_list_length(&client->meta_requests);
             ++meta_request_index) {
            struct aws_s3_meta_request *meta_request = NULL;
            aws_array_list_get_at(&client->meta_requests, &meta_request, meta_request_index);

            for (size_t conn_index = 0; conn_index < aws_array_list_length(&vip->vip_connections); ++conn_index) {
                struct aws_s3_vip_connection *vip_connection = NULL;
                aws_array_list_get_at(&vip->vip_connections, &vip_connection, conn_index);
                aws_s3_vip_connection_push_meta_request(vip_connection, meta_request);
            }
        }
    }

clean_up:

    if (host_address_string != NULL) {
        aws_string_destroy(host_address_string);
        host_address_string = NULL;
    }

    return op_result;
}

int aws_s3_client_remove_vip(struct aws_s3_client *client, struct aws_s3_vip *vip) {
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(vip);

    struct aws_hash_element *existing_vip_hash_element = NULL;
    aws_hash_table_find(&client->vips_table, vip->host_address, &existing_vip_hash_element);

    if (existing_vip_hash_element && existing_vip_hash_element->value == vip) {
        aws_hash_table_remove(&client->vips_table, vip->host_address, NULL, NULL);
    }

    aws_linked_list_remove(&vip->node);

    s_s3_client_vip_destroy(client, vip);

    return AWS_OP_SUCCESS;
}

void aws_s3_client_set_vip_state(struct aws_s3_client *client, struct aws_s3_vip *vip, enum aws_s3_vip_state state) {
    AWS_PRECONDITION(vip);

    if (vip->state == state) {
        return;
    }

    vip->state = state;

    switch (vip->state) {
        case AWS_S3_VIP_STATE_INIT: {
            break;
        }
        case AWS_S3_VIP_STATE_ACTIVE: {
            break;
        }
        case AWS_S3_VIP_STATE_CLEAN_UP: {
            aws_s3_client_set_vip_state(client, vip, AWS_S3_VIP_STATE_CLEAN_UP_CONNS);
            break;
        }
        case AWS_S3_VIP_STATE_CLEAN_UP_CONNS: {

            if (!aws_array_list_is_valid(&vip->vip_connections) || vip->num_allocated_vip_connections == 0) {
                aws_s3_client_set_vip_state(client, vip, AWS_S3_VIP_STATE_CLEAN_UP_CONNS_FINISHED);
            } else {
                /* Clean up our vip connections array list. */
                for (size_t conn_index = 0; conn_index < aws_array_list_length(&vip->vip_connections); ++conn_index) {
                    struct aws_s3_vip_connection *vip_connection = NULL;
                    aws_array_list_get_at(&vip->vip_connections, &vip_connection, conn_index);

                    aws_s3_vip_connection_release(vip_connection);
                    vip_connection = NULL;

                    aws_array_list_set_at(&vip->vip_connections, &vip_connection, conn_index);
                }
            }
            break;
        }
        case AWS_S3_VIP_STATE_CLEAN_UP_CONNS_FINISHED: {
            aws_array_list_clean_up(&vip->vip_connections);

            aws_s3_client_set_vip_state(client, vip, AWS_S3_VIP_STATE_CLEAN_UP_CONN_MANAGER);
            break;
        }
        case AWS_S3_VIP_STATE_CLEAN_UP_CONN_MANAGER: {

            /* Clean up the HTTP connection manager. */
            if (vip->http_connection_manager != NULL) {
                aws_http_connection_manager_release(vip->http_connection_manager);
                vip->http_connection_manager = NULL;
            } else {
                aws_s3_client_set_vip_state(client, vip, AWS_S3_VIP_STATE_CLEAN_UP_CONN_MANAGER_FINISHED);
            }

            break;
        }
        case AWS_S3_VIP_STATE_CLEAN_UP_CONN_MANAGER_FINISHED: {
            aws_s3_client_set_vip_state(client, vip, AWS_S3_VIP_STATE_CLEAN_UP_FINISH_RELEASE);
            break;
        }
        case AWS_S3_VIP_STATE_CLEAN_UP_FINISH_RELEASE: {
            AWS_PRECONDITION(vip);

            if (vip->host_address != NULL) {
                aws_string_destroy(vip->host_address);
                vip->host_address = NULL;
            }

            aws_mem_release(vip->allocator, vip);
            vip = NULL;

            --client->num_vips_allocated;

            /* If we are currently cleaning up and waiting for VIP's to be done, go to the next step of cleanup if this
             * was the last VIP to clean up. */
            if (client->state == AWS_S3_CLIENT_STATE_CLEAN_UP_VIPS && client->num_vips_allocated == 0) {
                aws_s3_client_set_state(client, AWS_S3_CLIENT_STATE_CLEAN_UP_VIPS_FINISHED);
            }
            break;
        }
        default:
            AWS_FATAL_ASSERT(false);
            break;
    }
}

static void s_s3_client_vip_connection_shutdown_callback(void *user_data) {
    AWS_PRECONDITION(user_data);

    struct aws_s3_client_vip_pair *client_vip_pair = user_data;
    AWS_PRECONDITION(client_vip_pair->client);
    AWS_PRECONDITION(client_vip_pair->vip);

    if (aws_s3_client_async_action(
            client_vip_pair->client,
            AWS_S3_CLIENT_ASYNC_ACTION_HANDLE_VIP_CONNECTION_SHUTDOWN,
            client_vip_pair->vip,
            NULL,
            NULL)) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_VIP,
            "id=%p: Could not initiate async action for handling VIP connection shutdown.",
            (void *)client_vip_pair->vip);
    }

    aws_s3_client_vip_pair_destroy(client_vip_pair);
    client_vip_pair = NULL;
}

static void s_s3_client_vip_http_connection_manager_shutdown_callback(void *user_data) {
    AWS_PRECONDITION(user_data);

    struct aws_s3_client_vip_pair *client_vip_pair = user_data;
    AWS_PRECONDITION(client_vip_pair->client);
    AWS_PRECONDITION(client_vip_pair->vip);

    if (aws_s3_client_async_action(
            client_vip_pair->client,
            AWS_S3_CLIENT_ASYNC_ACTION_HANDLE_CONN_MANAGER_SHUTDOWN,
            client_vip_pair->vip,
            NULL,
            NULL)) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_VIP,
            "id=%p: Could not initiate async action for handling VIP connection manager shutdown.",
            (void *)client_vip_pair->vip);
    }

    aws_s3_client_vip_pair_destroy(client_vip_pair);
    client_vip_pair = NULL;
}

struct aws_s3_client_vip_pair *aws_s3_client_vip_pair_new(
    struct aws_allocator *allocator,
    struct aws_s3_client *client,
    struct aws_s3_vip *vip) {
    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(vip);

    struct aws_s3_client_vip_pair *client_vip_pair = aws_mem_acquire(allocator, sizeof(struct aws_s3_client_vip_pair));

    if (client_vip_pair == NULL) {
        return client_vip_pair;
    }

    client_vip_pair->allocator = allocator;
    client_vip_pair->client = client;
    client_vip_pair->vip = vip;

    return client_vip_pair;
}

void aws_s3_client_vip_pair_destroy(struct aws_s3_client_vip_pair *client_vip_pair) {
    AWS_PRECONDITION(client_vip_pair);
    aws_mem_release(client_vip_pair->allocator, client_vip_pair);
    client_vip_pair = NULL;
}
