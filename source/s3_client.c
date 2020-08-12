/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_client_impl.h"
#include "aws/s3/private/s3_get_object_request.h"
#include "aws/s3/private/s3_meta_request_impl.h"
#include "aws/s3/private/s3_put_object_request.h"
#include "aws/s3/private/s3_request.h"
#include "aws/s3/private/s3_vip.h"
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

static uint64_t s_default_part_size = 5 * 1024 * 1024;
static size_t s_default_dns_host_address_ttl = 2 * 60;
static double s_default_throughput_target_gbps = 5.0;
static double s_default_throughput_per_vip = 6.25; // TODO provide analysis on how we reached this constant.
static uint32_t s_default_num_connections_per_vip = 10;

static const size_t s_meta_request_list_initial_capacity = 16;

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

/* Callback used for when the work controller has finished shutting down. */
static void s_s3_work_controller_shutdown_callback(void *user_data);

/* Callback for when the host resolver has found a new address. */
static void s_s3_client_resolved_address_callback(struct aws_host_address *host_address, void *user_data);

/* Internal callback for one a meta request has finished. */
static void s_s3_client_meta_request_finished_callback(
    struct aws_s3_meta_request *meta_request,
    int error_code,
    void *user_data);

/* Callback for our work controller that handles processing work. */
static void s_s3_client_process_async_work(struct aws_s3_async_work *work);

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

    aws_mutex_init(&client->lock);
    aws_linked_list_init(&client->vips);

    /* Set up our array list meta requests. */
    if (aws_array_list_init_dynamic(
            &client->meta_requests,
            client->allocator,
            s_meta_request_list_initial_capacity,
            sizeof(struct aws_s3_meta_request *))) {
        goto error_clean_up;
    }

    /* Setup our look up table of VIP address -> VIP structure */
    if (aws_hash_table_init(
            &client->vips_table,
            allocator,
            client->ideal_vip_count,
            aws_hash_string,
            aws_hash_callback_string_eq,
            aws_hash_callback_string_destroy,
            NULL)) {
        goto error_clean_up;
    }

    struct aws_s3_async_work_controller_options async_work_controller_options = {
        .allocator = client->allocator,
        .event_loop = client->event_loop,
        .work_fn = s_s3_client_process_async_work,
        .shutdown_callback = s_s3_work_controller_shutdown_callback,
        .shutdown_user_data = client};

    aws_s3_async_work_controller_init(&client->async_work_controller, &async_work_controller_options);

    /* Initialize shutdown options and tracking. */
    client->shutdown_callback = client_config->shutdown_callback;
    client->shutdown_callback_user_data = client_config->shutdown_callback_user_data;

    aws_s3_client_set_state(client, AWS_S3_CLIENT_STATE_ACTIVE);

    /* NOTE This is a temporary HACK for listening to host resolution events that have been hacked in via a branch of
     * aws-c-io. */
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
    if (aws_s3_client_async_action(client, AWS_S3_CLIENT_ASYNC_ACTION_CLEAN_UP, NULL, NULL, NULL)) {
        AWS_LOGF_ERROR(AWS_LS_S3_CLIENT, "id=%p: Error initiating clean up action for client.", (void *)client);
    }
}

uint32_t aws_s3_client_get_num_active_vips(struct aws_s3_client *client) {
    uint32_t result = 0;
    aws_mutex_lock(&client->lock);
    result = client->num_active_vips;
    aws_mutex_unlock(&client->lock);
    return result;
}

/* Issue a client async action.  We enforce what values go to what parameters to simplify the async work function. */
int aws_s3_client_async_action(
    struct aws_s3_client *client,
    enum aws_s3_client_async_action action,
    struct aws_s3_vip *vip,
    struct aws_s3_meta_request *meta_request,
    void *extra) {

    if (meta_request != NULL) {
        aws_s3_meta_request_acquire(meta_request);
    }

    struct aws_s3_async_work_options async_work_options = {.action = action,
                                                           .work_controller = &client->async_work_controller,
                                                           .params = {client, vip, meta_request, extra}};

    return aws_s3_async_work_dispatch(&async_work_options);
}

/* This function processes asynchronous work. */
static void s_s3_client_process_async_work(struct aws_s3_async_work *work) {

    struct aws_s3_client *client = work->params[0];
    struct aws_s3_vip *vip = work->params[1];
    struct aws_s3_meta_request *meta_request = work->params[2];
    void *extra = work->params[3];

    aws_mutex_lock(&client->lock);

    switch (work->action) {
        case AWS_S3_CLIENT_ASYNC_ACTION_PUSH_META_REQUEST: {

            /* Grab an additional reference for the meta request for its place in the list. */
            aws_s3_meta_request_acquire(meta_request);

            /* Add our new meta request to our request list */
            if (aws_array_list_push_back(&client->meta_requests, &meta_request)) {
                aws_s3_meta_request_release(meta_request);

                AWS_LOGF_ERROR(
                    AWS_LS_S3_CLIENT,
                    "id=%p Could not push meta request %p to client's meta request list.",
                    (void *)client,
                    (void *)meta_request);

                break;
            }

            struct aws_linked_list_node *vip_node = aws_linked_list_begin(&client->vips);

            /* Go through all VIP's and push this request to all of their vip connections. */
            while (vip_node != aws_linked_list_end(&client->vips)) {
                struct aws_s3_vip *vip = AWS_CONTAINER_OF(vip_node, struct aws_s3_vip, node);

                if (aws_s3_client_vip_is_cleaning_up(client, vip)) {
                    vip_node = aws_linked_list_next(vip_node);
                    continue;
                }

                for (size_t conn_index = 0; conn_index < aws_array_list_length(&vip->vip_connections); ++conn_index) {
                    struct aws_s3_vip_connection *vip_connection = NULL;
                    aws_array_list_get_at(&vip->vip_connections, &vip_connection, conn_index);

                    if (aws_s3_vip_connection_push_meta_request(vip_connection, meta_request)) {
                        AWS_LOGF_ERROR(
                            AWS_LS_S3_CLIENT,
                            "id=%p: Error pushing meta request to VIP connection %p of VIP %p.",
                            (void *)client,
                            (void *)vip_connection,
                            (void *)vip);
                    }
                }

                vip_node = aws_linked_list_next(vip_node);
            }
            break;
        }
        case AWS_S3_CLIENT_ASYNC_ACTION_REMOVE_META_REQUEST: {
            bool request_found = false;

            /* Remove the meta request from our own internal list. */
            for (size_t meta_request_index = 0; meta_request_index < aws_array_list_length(&client->meta_requests);
                 ++meta_request_index) {
                struct aws_s3_meta_request *meta_request_item = NULL;
                aws_array_list_get_at(&client->meta_requests, &meta_request_item, meta_request_index);

                if (meta_request_item == meta_request) {
                    aws_array_list_erase(&client->meta_requests, meta_request_index);
                    aws_s3_meta_request_release(meta_request);
                    request_found = true;
                    break;
                }
            }

            /* Remove the meta request from each active VIP. */
            if (request_found && !aws_linked_list_empty(&client->vips)) {

                struct aws_linked_list_node *vip_node = aws_linked_list_begin(&client->vips);

                while (vip_node != aws_linked_list_end(&client->vips)) {
                    struct aws_s3_vip *vip = AWS_CONTAINER_OF(vip_node, struct aws_s3_vip, node);

                    if (aws_s3_client_vip_is_cleaning_up(client, vip)) {
                        vip_node = aws_linked_list_next(vip_node);
                        continue;
                    }

                    for (size_t conn_index = 0; conn_index < aws_array_list_length(&vip->vip_connections);
                         ++conn_index) {
                        struct aws_s3_vip_connection *vip_connection = NULL;
                        aws_array_list_get_at(&vip->vip_connections, &vip_connection, conn_index);
                        aws_s3_vip_connection_remove_meta_request(vip_connection, meta_request);
                    }

                    vip_node = aws_linked_list_next(vip_node);
                }
            }

            break;
        }
        case AWS_S3_CLIENT_ASYNC_ACTION_ADD_VIP: {
            struct aws_string *host_address = extra;

            if (aws_s3_client_add_vip(client, aws_byte_cursor_from_string(host_address))) {
                AWS_LOGF_ERROR(
                    AWS_LS_S3_CLIENT,
                    "id=%p: Error adding VIP to client with address %s.",
                    (void *)client,
                    (const char *)host_address->bytes);
            }

            aws_string_destroy(host_address);
            host_address = NULL;
            break;
        }
        case AWS_S3_CLIENT_ASYNC_ACTION_REMOVE_VIP: {
            /* Initiate completely shutting down and removing this VIP. */
            if (aws_s3_client_remove_vip(client, vip)) {
                AWS_LOGF_ERROR(
                    AWS_LS_S3_CLIENT, "id=%p: Error removing VIP %p from client.", (void *)client, (void *)vip);
            }
            break;
        }
        case AWS_S3_CLIENT_ASYNC_ACTION_HANDLE_VIP_CONNECTION_SHUTDOWN: {
            --vip->num_allocated_vip_connections;

            /* If we still have some connections that are cleaning up, return. */
            if (vip->num_allocated_vip_connections == 0) {
                aws_s3_client_set_vip_state(client, vip, AWS_S3_VIP_STATE_CLEAN_UP_CONNS_FINISHED);
            }
            break;
        }
        case AWS_S3_CLIENT_ASYNC_ACTION_HANDLE_CONN_MANAGER_SHUTDOWN: {

            /* If clean up was waiting for the connection manager to shutdown, then we can advance the clean up state.
             */
            if (client->state == AWS_S3_VIP_STATE_CLEAN_UP_CONN_MANAGER) {
                aws_s3_client_set_vip_state(client, vip, AWS_S3_VIP_STATE_CLEAN_UP_CONN_MANAGER_FINISHED);
            }

            break;
        }
        case AWS_S3_CLIENT_ASYNC_ACTION_CLEAN_UP: {

            /* Initiate our clean up process. */
            aws_s3_client_set_state(client, AWS_S3_CLIENT_STATE_CLEAN_UP);
            break;
        }
        default:
            AWS_FATAL_ASSERT(false);
            break;
    }

    aws_mutex_unlock(&client->lock);

    /* Release the meta request reference (if we have one) for this async work. */
    if (meta_request != NULL) {
        aws_s3_meta_request_release(meta_request);
        meta_request = NULL;
    }
}

/* Updates client's place in its state machine. */
void aws_s3_client_set_state(struct aws_s3_client *client, enum aws_s3_client_state state) {
    AWS_PRECONDITION(client);

    if (client->state == state) {
        return;
    }

    client->state = state;

    switch (client->state) {
        case AWS_S3_CLIENT_STATE_INIT: {
            break;
        }
        case AWS_S3_CLIENT_STATE_ACTIVE: {
            break;
        }
        case AWS_S3_CLIENT_STATE_CLEAN_UP: {
            aws_s3_client_set_state(client, AWS_S3_CLIENT_STATE_CLEAN_UP_RESOLVE);
            break;
        }
        case AWS_S3_CLIENT_STATE_CLEAN_UP_RESOLVE: {

            /* NOTE this ia HACK to reset hacked in callbacks that exist in a specific branch.  This will be removed. */
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

                AWS_LOGF_ERROR(
                    AWS_LS_S3_CLIENT, "id=%p: Error trying to clean up state in host resolver.", (void *)client);
            }

            aws_s3_client_set_state(client, AWS_S3_CLIENT_STATE_CLEAN_UP_RESOLVE_FINISHED);
            break;
        }
        case AWS_S3_CLIENT_STATE_CLEAN_UP_RESOLVE_FINISHED: {
            aws_s3_client_set_state(client, AWS_S3_CLIENT_STATE_CLEAN_UP_VIPS);
            break;
        }
        case AWS_S3_CLIENT_STATE_CLEAN_UP_VIPS: {

            /* If we already don't have vips allocated, then we can advance ot the next state.  Otherwise, initiate
             * removal for any VIP's that we currently have. */
            if (client->num_vips_allocated == 0) {
                aws_s3_client_set_state(client, AWS_S3_CLIENT_STATE_CLEAN_UP_VIPS_FINISHED);
            } else {
                struct aws_linked_list_node *vip_node = aws_linked_list_begin(&client->vips);

                while (vip_node != aws_linked_list_end(&client->vips)) {
                    struct aws_s3_vip *vip = AWS_CONTAINER_OF(vip_node, struct aws_s3_vip, node);

                    aws_s3_client_async_action(client, AWS_S3_CLIENT_ASYNC_ACTION_REMOVE_VIP, vip, NULL, NULL);

                    vip_node = aws_linked_list_next(vip_node);
                }
            }
            break;
        }
        case AWS_S3_CLIENT_STATE_CLEAN_UP_VIPS_FINISHED: {
            aws_s3_client_set_state(client, AWS_S3_CLIENT_STATE_CLEAN_UP_WORK_CONTROLLER);
            break;
        }
        case AWS_S3_CLIENT_STATE_CLEAN_UP_WORK_CONTROLLER: {
            aws_s3_async_work_controller_shutdown(&client->async_work_controller);
            break;
        }
        case AWS_S3_CLIENT_STATE_CLEAN_UP_WORK_CONTROLLER_FINISHED: {
            aws_s3_client_set_state(client, AWS_S3_CLIENT_STATE_CLEAN_UP_FINISH_RELEASE);
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

            /* Clear out our meta request list. */
            for (size_t meta_request_index = 0; meta_request_index < aws_array_list_length(&client->meta_requests);
                 ++meta_request_index) {
                struct aws_s3_meta_request *meta_request = NULL;
                aws_array_list_get_at(&client->meta_requests, &meta_request, meta_request_index);
                aws_s3_meta_request_release(meta_request);
            }

            aws_array_list_clean_up(&client->meta_requests);

            aws_hash_table_clean_up(&client->vips_table);

            aws_s3_client_shutdown_complete_callback_fn *shutdown_callback = client->shutdown_callback;
            void *shutdown_user_data = client->shutdown_callback_user_data;

            aws_mutex_clean_up(&client->lock);

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

    /* Actual pushing of the meta request will happen asynchronously. */
    if (aws_s3_client_async_action(client, AWS_S3_CLIENT_ASYNC_ACTION_PUSH_META_REQUEST, NULL, meta_request, NULL)) {
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

    /* Remove the meta request from anywhere needed asynchronously. */
    if (aws_s3_client_async_action(client, AWS_S3_CLIENT_ASYNC_ACTION_REMOVE_META_REQUEST, NULL, meta_request, NULL)) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_CLIENT,
            "id=%p: Could not initate removal of finished meta request %p.",
            (void *)client,
            (void *)meta_request);
        return;
    }
}

/* Shutdown callback for our async work controller. */
static void s_s3_work_controller_shutdown_callback(void *user_data) {
    AWS_PRECONDITION(user_data);

    struct aws_s3_client *client = user_data;

    aws_mutex_lock(&client->lock);
    if (client->state == AWS_S3_CLIENT_STATE_CLEAN_UP_WORK_CONTROLLER) {
        /* Advance the clean up state. */
        aws_s3_client_set_state(client, AWS_S3_CLIENT_STATE_CLEAN_UP_WORK_CONTROLLER_FINISHED);
    }
    aws_mutex_unlock(&client->lock);
}

/* Callback for address being resolved by the host resolver. */
static void s_s3_client_resolved_address_callback(struct aws_host_address *host_address, void *user_data) {
    AWS_PRECONDITION(host_address);
    AWS_PRECONDITION(user_data);

    struct aws_s3_client *client = user_data;
    struct aws_string *copied_host_adddress =
        aws_string_new_from_array(client->allocator, host_address->address->bytes, host_address->address->len);

    if (copied_host_adddress == NULL) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_CLIENT,
            "id=%p: Could not allocate copy of host address %s to add new VIP to client.",
            (void *)client,
            (const char *)host_address->address->bytes);

        goto error_clean_up;
    }

    /* Issue an async action to create a VIP from the resolved address. */
    if (aws_s3_client_async_action(client, AWS_S3_CLIENT_ASYNC_ACTION_ADD_VIP, NULL, NULL, copied_host_adddress)) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_CLIENT,
            "id=%p: Could not initate adding VIP with address %s to client.",
            (void *)client,
            (void *)copied_host_adddress->bytes);

        goto error_clean_up;
    }

    return;

error_clean_up:

    if (copied_host_adddress != NULL) {
        aws_string_destroy(copied_host_adddress);
        copied_host_adddress = NULL;
    }
}
