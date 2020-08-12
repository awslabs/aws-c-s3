/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_vip_connection.h"
#include "aws/s3/private/s3_client_impl.h"
#include "aws/s3/private/s3_meta_request_impl.h"
#include "aws/s3/private/s3_request.h"
#include "aws/s3/private/s3_vip.h"
#include "aws/s3/private/s3_work_util.h"

#include <aws/auth/credentials.h>
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

enum s3_vip_connection_async_action {
    S3_VIP_CONNECTION_ASYNC_ACTION_PUSH_META_REQUEST,
    S3_VIP_CONNECTION_ASYNC_ACTION_REMOVE_META_REQUEST,
    S3_VIP_CONNECTION_ASYNC_ACTION_PROCESS_META_REQUESTS,
    S3_VIP_CONNECTION_ASYNC_ACTION_CLEAN_UP
};

static const size_t s_meta_request_list_initial_capacity = 16;
static const size_t s_out_of_order_removal_list_initial_capacity = 16;
static const uint64_t s_vip_connection_processing_retry_offset_ms = 50;
static const int32_t s_s3_max_request_count_per_connection = 100;

/* Returns if we are in a clean up part of the state machine. */
static bool s_s3_vip_connection_is_cleaning_up(const struct aws_s3_vip_connection *vip_connection);

/* Issue an async action supported the VIP connection structure. */
static int s_s3_vip_connection_async_action(
    struct aws_s3_vip_connection *vip_connection,
    enum s3_vip_connection_async_action action,
    struct aws_s3_meta_request *meta_request,
    void *extra,
    uint64_t time_offset_ns);

static void s_s3_vip_connection_process_async_work(struct aws_s3_async_work *work);

/* Set if we're currently processing work or not. (active or idle) */
static void s_s3_vip_connection_set_processing_state(
    struct aws_s3_vip_connection *vip_connection,
    enum aws_s3_vip_connection_processing_state processing_state);

/* Go through an array list of meta requests and find the one specified. */
static size_t s_find_meta_request_index(
    const struct aws_array_list *meta_requests,
    const struct aws_s3_meta_request *meta_request);

/* Update the vip connection's state machine. */
static void s_s3_vip_connection_set_state(
    struct aws_s3_vip_connection *vip_connection,
    enum aws_s3_vip_connection_state state);

/* Triggered when the VIP connection's work controller shuts down. */
static void s_s3_work_controller_shutdown_callback(void *user_data);

/* Triggered when we need to process the next meta reqeust. */
static void s_s3_vip_connection_process_meta_requests(struct aws_s3_vip_connection *vip_connection);

/* Start making the actual HTTP request. */
static int s_s3_vip_connection_make_request(struct aws_s3_vip_connection *vip_connection);

/* Callback for when the signing of a particular request has completed. */
static void s_s3_vip_connection_signing_complete(struct aws_signing_result *result, int error_code, void *user_data);

/* Make sure we have a connection for the request, getting one if we don't already. */
static void s_s3_vip_connection_acquire_connection(struct aws_s3_vip_connection *vip_connection);

/* Callback to receive the connection we will use for the current request. */
static void s_s3_vip_connection_on_acquire_connection(
    struct aws_http_connection *connection,
    int error_code,
    void *user_data);

/* For making requests, VIP connections use these callbacks for HTTP requests, which will call into the actual
 * aws_s3_request functions.  We could potentially call the s3-request functions directly, but this will allow us to do
 * any VIP level processing per request if needed. */
static void s_s3_vip_connection_stream_complete(struct aws_http_stream *stream, int error_code, void *user_data);

static int s_s3_vip_connection_incoming_headers(
    struct aws_http_stream *stream,
    enum aws_http_header_block header_block,
    const struct aws_http_header *headers,
    size_t num_headers,
    void *user_data);

static int s_s3_vip_connection_incoming_header_block_done(
    struct aws_http_stream *stream,
    enum aws_http_header_block header_block,
    void *user_data);

static int s_s3_vip_connection_incoming_body(
    struct aws_http_stream *stream,
    const struct aws_byte_cursor *data,
    void *user_data);

static void s_s3_vip_connection_stream_complete(struct aws_http_stream *stream, int error_code, void *user_data);

static void s_s3_vip_connection_finish_request(struct aws_s3_vip_connection *vip_connection, int error_code);

struct aws_s3_vip_connection *aws_s3_vip_connection_new(
    struct aws_allocator *allocator,
    const struct aws_s3_vip_connection_options *options) {
    AWS_PRECONDITION(options);
    AWS_PRECONDITION(options->allocator);
    AWS_PRECONDITION(options->event_loop);
    AWS_PRECONDITION(options->credentials_provider);
    AWS_PRECONDITION(options->http_connection_manager);

    struct aws_s3_vip_connection *vip_connection = aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_vip_connection));

    if (vip_connection == NULL) {
        AWS_LOGF_ERROR(AWS_LS_S3_VIP_CONNECTION, "Could not allocate new aws_s3_vip_connection.");
        return NULL;
    }

    /* Initialize ref count */
    aws_atomic_store_int(&vip_connection->ref_count, 0);
    aws_s3_vip_connection_acquire(vip_connection);

    /* Copy over any options relevant for copy */
    vip_connection->allocator = options->allocator;
    vip_connection->event_loop = options->event_loop;

    vip_connection->credentials_provider = options->credentials_provider;
    aws_credentials_provider_acquire(options->credentials_provider);

    vip_connection->http_connection_manager = options->http_connection_manager;
    aws_http_connection_manager_acquire(vip_connection->http_connection_manager);

    vip_connection->region = aws_string_new_from_array(options->allocator, options->region.ptr, options->region.len);

    if (vip_connection->region == NULL) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_VIP_CONNECTION,
            "id=%p Could not allocate region string for aws_s3_vip_connection.",
            (void *)vip_connection);
        goto failed_alloc_region;
    }

    aws_mutex_init(&vip_connection->lock);

    struct aws_s3_async_work_controller_options async_work_controller_options = {
        .allocator = vip_connection->allocator,
        .event_loop = vip_connection->event_loop,
        .work_fn = s_s3_vip_connection_process_async_work,
        .shutdown_callback = s_s3_work_controller_shutdown_callback,
        .shutdown_user_data = vip_connection};

    /* Initialize our work controller so that we can do async actions */
    aws_s3_async_work_controller_init(&vip_connection->async_work_controller, &async_work_controller_options);

    /* Set up our meta request list. */
    if (aws_array_list_init_dynamic(
            &vip_connection->meta_requests,
            options->allocator,
            s_meta_request_list_initial_capacity,
            sizeof(struct aws_s3_meta_request *))) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_VIP_CONNECTION,
            "id=%p Could not allocate meta requests list for aws_s3_vip_connection.",
            (void *)vip_connection);
        goto failed_alloc_meta_request_list;
    }

    /* Set up our meta request out-of-order removal list. */
    if (aws_array_list_init_dynamic(
            &vip_connection->meta_request_ooo_removals,
            options->allocator,
            s_out_of_order_removal_list_initial_capacity,
            sizeof(struct aws_s3_meta_request *))) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_VIP_CONNECTION,
            "id=%p Could not allocate out-of-order removals list for aws_s3_vip_connection.",
            (void *)vip_connection);
        goto failed_alloc_removals_list;
    }

    vip_connection->on_idle_callback = options->on_idle_callback;
    vip_connection->on_active_callback = options->on_active_callback;
    vip_connection->shutdown_callback = options->shutdown_callback;
    vip_connection->user_data = options->user_data;

    return vip_connection;

failed_alloc_removals_list:
    aws_array_list_clean_up(&vip_connection->meta_requests);

failed_alloc_meta_request_list:
    aws_string_destroy(vip_connection->region);

failed_alloc_region:
    aws_mem_release(allocator, vip_connection);

    return NULL;
}

void aws_s3_vip_connection_acquire(struct aws_s3_vip_connection *vip_connection) {
    AWS_PRECONDITION(vip_connection);
    aws_atomic_fetch_add(&vip_connection->ref_count, 1);
}

void aws_s3_vip_connection_release(struct aws_s3_vip_connection *vip_connection) {
    AWS_PRECONDITION(vip_connection);

    size_t prev_ref_count = aws_atomic_fetch_sub(&vip_connection->ref_count, 1);

    if (prev_ref_count > 1) {
        return;
    }

    /* Initiate clean up. */
    if (s_s3_vip_connection_async_action(vip_connection, S3_VIP_CONNECTION_ASYNC_ACTION_CLEAN_UP, NULL, NULL, 0)) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_VIP_CONNECTION,
            "id=%p Could not initiate clean up for s3 vip connection.",
            (void *)vip_connection);
    }
}

static size_t s_find_meta_request_index(
    const struct aws_array_list *meta_requests,
    const struct aws_s3_meta_request *meta_request) {

    size_t num_meta_requests = aws_array_list_length(meta_requests);

    for (size_t meta_request_index = 0; meta_request_index < num_meta_requests; ++meta_request_index) {

        struct aws_s3_meta_request *meta_request_item = NULL;

        aws_array_list_get_at(meta_requests, &meta_request_item, meta_request_index);

        if (meta_request_item == meta_request) {
            return meta_request_index;
        }
    }
    return (size_t)-1;
}

static bool s_s3_vip_connection_is_cleaning_up(const struct aws_s3_vip_connection *vip_connection) {
    return vip_connection->state >= AWS_S3_VIP_CONNECTION_STATE_CLEAN_UP &&
           vip_connection->state <= AWS_S3_VIP_CONNECTION_STATE_CLEAN_UP_FINISH;
}

/* Issue a client async action.  We enforce what values go to what parameters to simplify th actual work function. */
static int s_s3_vip_connection_async_action(
    struct aws_s3_vip_connection *vip_connection,
    enum s3_vip_connection_async_action action,
    struct aws_s3_meta_request *meta_request,
    void *extra,
    uint64_t time_offset_ns) {

    if (meta_request != NULL) {
        aws_s3_meta_request_acquire(meta_request);
    }

    struct aws_s3_async_work_options async_work_options = {.action = action,
                                                           .work_controller = &vip_connection->async_work_controller,
                                                           .params = {vip_connection, meta_request, extra},
                                                           .schedule_time_offset_ns = time_offset_ns};

    return aws_s3_async_work_dispatch(&async_work_options);
}

static void s_s3_vip_connection_process_async_work(struct aws_s3_async_work *async_work) {
    AWS_PRECONDITION(async_work);

    struct aws_s3_vip_connection *vip_connection = async_work->params[0];
    struct aws_s3_meta_request *meta_request = async_work->params[1];
    void *extra = async_work->params[2];

    (void)extra;

    AWS_PRECONDITION(vip_connection);

    aws_mutex_lock(&vip_connection->lock);
    if (s_s3_vip_connection_is_cleaning_up(vip_connection)) {
        aws_mutex_unlock(&vip_connection->lock);
        goto clean_up;
    }
    aws_mutex_unlock(&vip_connection->lock);

    switch (async_work->action) {
        case S3_VIP_CONNECTION_ASYNC_ACTION_PUSH_META_REQUEST: {
            aws_mutex_lock(&vip_connection->lock);

            size_t meta_request_index =
                s_find_meta_request_index(&vip_connection->meta_request_ooo_removals, meta_request);

            /* If we found this in our out of order removal list, we got a remove for this already (before it was even
             * added) due to out of order task evaluation. TODO (size_t)-1 should be a constant. */
            if (meta_request_index != (size_t)-1) {
                aws_array_list_erase(&vip_connection->meta_request_ooo_removals, meta_request_index);
                aws_s3_meta_request_release(meta_request);
            } else {
                /* Grab a reference to the meta request and push it into our list. */
                aws_s3_meta_request_acquire(meta_request);
                aws_array_list_push_back(&vip_connection->meta_requests, &meta_request);

                /* Make sure that we're processing meta requests. */
                s_s3_vip_connection_set_processing_state(vip_connection, AWS_S3_VIP_CONNECTION_PROCESSING_STATE_ACTIVE);
            }

            aws_mutex_unlock(&vip_connection->lock);
            break;
        }
        case S3_VIP_CONNECTION_ASYNC_ACTION_REMOVE_META_REQUEST: {
            aws_mutex_lock(&vip_connection->lock);
            size_t meta_request_index = s_find_meta_request_index(&vip_connection->meta_requests, meta_request);

            /* If it wasn't found, then this should mean that we received a remove before an add.  In that case, put it
            into our out-of-order list for the future. TODO find a better way of verifying this. Otherwise, we can go
            ahead and remove it from our normal list. */
            if (meta_request_index == (size_t)-1) {
                aws_s3_meta_request_acquire(meta_request);
                aws_array_list_push_back(&vip_connection->meta_request_ooo_removals, meta_request);
            } else {

                /* Update our next meta request index if needed. This is just to help keep processing order in tact, but
                 * might be unnecessary/overkill. */
                size_t *next_meta_request_index = &vip_connection->next_meta_request_index;

                if (meta_request_index < *next_meta_request_index) {
                    --(*next_meta_request_index);
                } else if (meta_request_index > *next_meta_request_index) {
                    *next_meta_request_index =
                        *next_meta_request_index % aws_array_list_length(&vip_connection->meta_requests);
                }

                aws_array_list_erase(&vip_connection->meta_requests, meta_request_index);
                aws_s3_meta_request_release(meta_request);
            }
            aws_mutex_unlock(&vip_connection->lock);
            break;
        }
        case S3_VIP_CONNECTION_ASYNC_ACTION_PROCESS_META_REQUESTS: {
            s_s3_vip_connection_process_meta_requests(vip_connection);
            break;
        }
        case S3_VIP_CONNECTION_ASYNC_ACTION_CLEAN_UP: {
            aws_mutex_lock(&vip_connection->lock);
            s_s3_vip_connection_set_state(vip_connection, AWS_S3_VIP_CONNECTION_STATE_CLEAN_UP);
            aws_mutex_unlock(&vip_connection->lock);
            break;
        }
        default:
            AWS_FATAL_ASSERT(false);
            break;
    }

clean_up:

    if (meta_request != NULL) {
        aws_s3_meta_request_release(meta_request);
    }
}

/* Updates the actual processing state. Calling any necessary callbacks. */
static void s_s3_vip_connection_set_processing_state(
    struct aws_s3_vip_connection *vip_connection,
    enum aws_s3_vip_connection_processing_state processing_state) {
    AWS_PRECONDITION(vip_connection);

    if (vip_connection->processing_state == processing_state) {
        return;
    }

    switch (processing_state) {
        case AWS_S3_VIP_CONNECTION_PROCESSING_STATE_IDLE: {
            /* We're not doing anything right now, s we can reset the next_meta_request_index to just be 0. */
            vip_connection->next_meta_request_index = 0;

            if (vip_connection->on_idle_callback != NULL) {
                vip_connection->on_idle_callback(vip_connection, vip_connection->user_data);
            }

            /* If clean up is waiting for us to be idle, tell them we're idle. */
            if (vip_connection->state == AWS_S3_VIP_CONNECTION_STATE_CLEAN_UP_WAIT_FOR_IDLE) {
                s_s3_vip_connection_set_state(vip_connection, AWS_S3_VIP_CONNECTION_STATE_CLEAN_UP_IDLE_FINISHED);
            }
            break;
        }
        case AWS_S3_VIP_CONNECTION_PROCESSING_STATE_ACTIVE: {
            if (vip_connection->on_active_callback != NULL) {
                vip_connection->on_active_callback(vip_connection, vip_connection->user_data);
            }

            /* Issue an async action to process requests. */
            s_s3_vip_connection_async_action(
                vip_connection, S3_VIP_CONNECTION_ASYNC_ACTION_PROCESS_META_REQUESTS, NULL, NULL, 0);

            break;
        }
        default:
            AWS_FATAL_ASSERT(false);
            break;
    }
}

/* State machine for the VIP connection.  Mainly used to manage clean ups tates.*/
static void s_s3_vip_connection_set_state(
    struct aws_s3_vip_connection *vip_connection,
    enum aws_s3_vip_connection_state state) {

    AWS_PRECONDITION(vip_connection);

    if (vip_connection->state == state) {
        return;
    }

    vip_connection->state = state;

    switch (state) {
        case AWS_S3_VIP_CONNECTION_STATE_ALIVE: {
            break;
        }
        case AWS_S3_VIP_CONNECTION_STATE_CLEAN_UP: {
            s_s3_vip_connection_set_state(vip_connection, AWS_S3_VIP_CONNECTION_STATE_CLEAN_UP_WAIT_FOR_IDLE);
            break;
        }
        case AWS_S3_VIP_CONNECTION_STATE_CLEAN_UP_WAIT_FOR_IDLE: {
            if (vip_connection->processing_state == AWS_S3_VIP_CONNECTION_PROCESSING_STATE_IDLE) {
                s_s3_vip_connection_set_state(vip_connection, AWS_S3_VIP_CONNECTION_STATE_CLEAN_UP_IDLE_FINISHED);
            }
            break;
        }
        case AWS_S3_VIP_CONNECTION_STATE_CLEAN_UP_IDLE_FINISHED: {
            s_s3_vip_connection_set_state(
                vip_connection, AWS_S3_VIP_CONNECTION_STATE_CLEAN_UP_WAIT_FOR_WORK_CONTROLLER);
            break;
        }
        case AWS_S3_VIP_CONNECTION_STATE_CLEAN_UP_WAIT_FOR_WORK_CONTROLLER: {
            aws_s3_async_work_controller_shutdown(&vip_connection->async_work_controller);
            break;
        }
        case AWS_S3_VIP_CONNECTION_STATE_CLEAN_UP_WORK_CONTROLLER_FINISHED: {
            s_s3_vip_connection_set_state(vip_connection, AWS_S3_VIP_CONNECTION_STATE_CLEAN_UP_FINISH);
            break;
        }
        case AWS_S3_VIP_CONNECTION_STATE_CLEAN_UP_FINISH: {
            if (vip_connection->credentials_provider != NULL) {
                aws_credentials_provider_release(vip_connection->credentials_provider);
                vip_connection->credentials_provider = NULL;
            }

            if (vip_connection->http_connection != NULL) {
                aws_http_connection_manager_release_connection(
                    vip_connection->http_connection_manager, vip_connection->http_connection);

                vip_connection->http_connection = NULL;
            }

            if (vip_connection->http_connection_manager != NULL) {
                aws_http_connection_manager_release(vip_connection->http_connection_manager);
                vip_connection->http_connection_manager = NULL;
            }

            if (vip_connection->region != NULL) {
                aws_string_destroy(vip_connection->region);
                vip_connection->region = NULL;
            }

            if (vip_connection->request != NULL) {
                aws_s3_request_release(vip_connection->request);
                vip_connection->request = NULL;
            }

            size_t num_meta_requests = aws_array_list_length(&vip_connection->meta_requests);

            /* Dump any meta requests we still have */
            for (size_t meta_request_index = 0; meta_request_index < num_meta_requests; ++meta_request_index) {
                struct aws_s3_meta_request *meta_request = NULL;
                aws_array_list_get_at(&vip_connection->meta_requests, &meta_request, meta_request_index);
                aws_s3_meta_request_release(meta_request);
            }

            aws_array_list_clean_up(&vip_connection->meta_requests);

            size_t num_ooo_removals = aws_array_list_length(&vip_connection->meta_request_ooo_removals);

            /* Dump any out of order meta requests we still have. */
            for (size_t meta_request_index = 0; meta_request_index < num_ooo_removals; ++meta_request_index) {
                struct aws_s3_meta_request *meta_request = NULL;
                aws_array_list_get_at(&vip_connection->meta_request_ooo_removals, &meta_request, meta_request_index);
                aws_s3_meta_request_release(meta_request);
            }

            aws_array_list_clean_up(&vip_connection->meta_request_ooo_removals);

            aws_mutex_clean_up(&vip_connection->lock);

            aws_s3_vip_connection_shutdown_complete_callback_fn *shutdown_callback = vip_connection->shutdown_callback;
            void *user_data = vip_connection->user_data;

            aws_mem_release(vip_connection->allocator, vip_connection);

            if (shutdown_callback != NULL) {
                shutdown_callback(user_data);
            }

            break;
        }
        default:
            AWS_FATAL_ASSERT(false);
            break;
    }
}

static void s_s3_work_controller_shutdown_callback(void *user_data) {
    AWS_PRECONDITION(user_data);

    struct aws_s3_vip_connection *vip_connection = user_data;

    aws_mutex_lock(&vip_connection->lock);
    s_s3_vip_connection_set_state(vip_connection, AWS_S3_VIP_CONNECTION_STATE_CLEAN_UP_WORK_CONTROLLER_FINISHED);
    aws_mutex_unlock(&vip_connection->lock);
}

int aws_s3_vip_connection_push_meta_request(
    struct aws_s3_vip_connection *vip_connection,
    struct aws_s3_meta_request *meta_request) {

    AWS_PRECONDITION(vip_connection);
    AWS_PRECONDITION(meta_request);

    return s_s3_vip_connection_async_action(
        vip_connection, S3_VIP_CONNECTION_ASYNC_ACTION_PUSH_META_REQUEST, meta_request, NULL, 0);
}

int aws_s3_vip_connection_remove_meta_request(
    struct aws_s3_vip_connection *vip_connection,
    struct aws_s3_meta_request *meta_request) {

    AWS_PRECONDITION(vip_connection);
    AWS_PRECONDITION(meta_request);

    return s_s3_vip_connection_async_action(
        vip_connection, S3_VIP_CONNECTION_ASYNC_ACTION_REMOVE_META_REQUEST, meta_request, NULL, 0);
}

static void s_s3_vip_connection_process_meta_requests(struct aws_s3_vip_connection *vip_connection) {
    AWS_PRECONDITION(vip_connection);

    // TODO a bit excessive?
    AWS_FATAL_ASSERT(vip_connection->request == NULL);

    aws_mutex_lock(&vip_connection->lock);

    size_t num_meta_requests = aws_array_list_length(&vip_connection->meta_requests);

    /* If we're trying to clean up, or we don't have anything to do, go back to idle state. */
    if (s_s3_vip_connection_is_cleaning_up(vip_connection) || num_meta_requests == 0) {
        s_s3_vip_connection_set_processing_state(vip_connection, AWS_S3_VIP_CONNECTION_PROCESSING_STATE_IDLE);
        return;
    }

    size_t next_meta_request_index = vip_connection->next_meta_request_index;

    /* Index that is relative to the value of next_meta_request_index.*/
    size_t relative_meta_request_index = 0;

    for (; relative_meta_request_index < num_meta_requests; ++relative_meta_request_index) {

        /* From our relative index, grab an actual index. */
        size_t meta_request_index = (relative_meta_request_index + next_meta_request_index) % num_meta_requests;

        struct aws_s3_meta_request *meta_request = NULL;
        aws_array_list_get_at(&vip_connection->meta_requests, &meta_request, meta_request_index);

        /* Try popping a request from this meta request. */
        vip_connection->request = aws_s3_meta_request_pop_request(meta_request);

        /* If we successfully got one, then go ahead and calculate a new next_meta_request_index value. */
        if (vip_connection->request != NULL) {
            next_meta_request_index = (meta_request_index + 1) % num_meta_requests;
            break;
        }
    }

    /* Store our new next_meta_reqest_index value if we have a request, or reset it if we couldn't find anything. */
    if (vip_connection->request != NULL) {
        vip_connection->next_meta_request_index = next_meta_request_index;
    } else {
        vip_connection->next_meta_request_index = 0;
    }

    aws_mutex_unlock(&vip_connection->lock);

    if (vip_connection->request != NULL) {
        s_s3_vip_connection_make_request(vip_connection); /* TODO needs error handling */
    } else {
        /* If there isn't an s3 request right now, don't completely shutdown--check back in a little bit to see if there
         * is additional work.*/
        uint64_t time_offset_ns = aws_timestamp_convert(
            s_vip_connection_processing_retry_offset_ms, AWS_TIMESTAMP_MILLIS, AWS_TIMESTAMP_NANOS, NULL);

        s_s3_vip_connection_async_action(
            vip_connection, S3_VIP_CONNECTION_ASYNC_ACTION_PROCESS_META_REQUESTS, NULL, NULL, time_offset_ns);
    }
}

static int s_s3_vip_connection_make_request(struct aws_s3_vip_connection *vip_connection) {
    AWS_PRECONDITION(vip_connection);

    struct aws_s3_request *request = vip_connection->request;
    AWS_PRECONDITION(request);

    /* If we already have an allocated signable, this is a retry, so skip the signing process. TODO add additional way
     * of verifying that this is a retry. */
    if (request->signable != NULL) {
        s_s3_vip_connection_acquire_connection(vip_connection);
    } else {
        request->signable = aws_signable_new_http_request(vip_connection->allocator, request->message);

        if (request->signable == NULL) {
            AWS_LOGF_ERROR(
                AWS_LS_S3_VIP_CONNECTION,
                "id=%p: Could not allocate signable for http request",
                (void *)vip_connection);
            return AWS_OP_ERR;
        }

        struct aws_date_time now;
        aws_date_time_init_now(&now);

        struct aws_byte_cursor service_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("s3");

        struct aws_signing_config_aws signing_config;
        AWS_ZERO_STRUCT(signing_config);
        signing_config.config_type = AWS_SIGNING_CONFIG_AWS;
        signing_config.algorithm = AWS_SIGNING_ALGORITHM_V4;
        signing_config.credentials_provider = vip_connection->credentials_provider;
        signing_config.region = aws_byte_cursor_from_array(vip_connection->region->bytes, vip_connection->region->len);
        signing_config.service = service_name;
        signing_config.date = now;
        signing_config.signed_body_value = AWS_SBVT_UNSIGNED_PAYLOAD;
        signing_config.signed_body_header = AWS_SBHT_X_AMZ_CONTENT_SHA256;

        if (aws_sign_request_aws(
                vip_connection->allocator,
                request->signable,
                (struct aws_signing_config_base *)&signing_config,
                s_s3_vip_connection_signing_complete,
                vip_connection)) {
            AWS_LOGF_ERROR(AWS_LS_S3_VIP_CONNECTION, "id=%p: Could not sign request", (void *)vip_connection);
            return AWS_OP_ERR;
        }
    }

    return AWS_OP_SUCCESS;
}

static void s_s3_vip_connection_signing_complete(struct aws_signing_result *result, int error_code, void *user_data) {

    struct aws_s3_vip_connection *vip_connection = user_data;
    AWS_PRECONDITION(vip_connection);

    struct aws_s3_request *request = vip_connection->request;
    AWS_PRECONDITION(request);

    if (error_code != AWS_OP_SUCCESS) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_VIP_CONNECTION,
            "id=%p: Could not sign request due to error_code %d",
            (void *)vip_connection,
            error_code);
        goto error_clean_up;
    }

    if (aws_apply_signing_result_to_http_request(request->message, vip_connection->allocator, result)) {
        error_code = aws_last_error();
        AWS_LOGF_ERROR(
            AWS_LS_S3_VIP_CONNECTION,
            "id=%p: Could not apply signing result to http request due to error %d",
            (void *)vip_connection,
            error_code);
        goto error_clean_up;
    }

    s_s3_vip_connection_acquire_connection(vip_connection);

    return;

error_clean_up:
    s_s3_vip_connection_finish_request(vip_connection, error_code);
}

static void s_s3_vip_connection_acquire_connection(struct aws_s3_vip_connection *vip_connection) {
    struct aws_http_connection *http_connection = vip_connection->http_connection;

    if (http_connection != NULL) {
        /* If we're at the max request count, set us up to get a new connection.  Also close the original connection so
         * that the connection manager doesn't reuse it.  TODO maybe find a more visible way of preventing the
         * connection from going back into the pool. */
        if (vip_connection->connection_request_count == s_s3_max_request_count_per_connection) {
            aws_http_connection_close(http_connection);
            aws_http_connection_manager_release_connection(vip_connection->http_connection_manager, http_connection);
            http_connection = NULL;
        } else if (!aws_http_connection_is_open(http_connection)) {
            /* If our connection is closed for some reason, also get rid of it.*/
            aws_http_connection_manager_release_connection(vip_connection->http_connection_manager, http_connection);
            http_connection = NULL;
        }
    }

    if (http_connection != NULL) {
        s_s3_vip_connection_on_acquire_connection(http_connection, AWS_ERROR_SUCCESS, vip_connection);
    } else {
        aws_http_connection_manager_acquire_connection(
            vip_connection->http_connection_manager, s_s3_vip_connection_on_acquire_connection, vip_connection);
    }
}

static void s_s3_vip_connection_on_acquire_connection(
    struct aws_http_connection *connection,
    int error_code,
    void *user_data) {
    struct aws_s3_vip_connection *vip_connection = user_data;
    AWS_PRECONDITION(vip_connection);

    struct aws_s3_request *request = vip_connection->request;
    AWS_PRECONDITION(request);

    if (error_code != AWS_ERROR_SUCCESS || connection == NULL) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_VIP_CONNECTION,
            "id=%p: Could not acquire connection due to error code %d (%s)",
            (void *)vip_connection,
            error_code,
            aws_error_str(error_code));
        goto error_clean_up;
    }

    /* If our cached connection is not equal to the one we just received, switch to the received one. */
    if (vip_connection->http_connection != connection) {
        if (vip_connection->http_connection != NULL) {
            aws_http_connection_manager_release_connection(
                vip_connection->http_connection_manager, vip_connection->http_connection);
            vip_connection->http_connection = NULL;
        }

        vip_connection->http_connection = connection;
    }

    struct aws_http_make_request_options options;
    AWS_ZERO_STRUCT(options);
    options.self_size = sizeof(struct aws_http_make_request_options);
    options.request = request->message;
    options.user_data = vip_connection;
    options.on_response_headers = s_s3_vip_connection_incoming_headers;
    options.on_response_header_block_done = s_s3_vip_connection_incoming_header_block_done;
    options.on_response_body = s_s3_vip_connection_incoming_body;
    options.on_complete = s_s3_vip_connection_stream_complete;

    request->stream = aws_http_connection_make_request(connection, &options);

    if (request->stream == NULL) {
        error_code = aws_last_error();
        AWS_LOGF_ERROR(AWS_LS_S3_VIP_CONNECTION, "id=%p: Could not make HTTP request", (void *)vip_connection);
        goto error_clean_up;
    }

    if (aws_http_stream_activate(request->stream) != AWS_OP_SUCCESS) {
        error_code = aws_last_error();
        AWS_LOGF_ERROR(AWS_LS_S3_VIP_CONNECTION, "id=%p: Could not activate HTTP stream", (void *)vip_connection);
        goto error_clean_up;
    }

    return;

error_clean_up:
    s_s3_vip_connection_finish_request(vip_connection, error_code);
}

static int s_s3_vip_connection_incoming_headers(
    struct aws_http_stream *stream,
    enum aws_http_header_block header_block,
    const struct aws_http_header *headers,
    size_t num_headers,
    void *user_data) {

    (void)stream;

    struct aws_s3_vip_connection *vip_connection = user_data;
    AWS_PRECONDITION(vip_connection);

    struct aws_s3_request *request = vip_connection->request;
    AWS_PRECONDITION(request);

    return aws_s3_request_incoming_headers(request, header_block, headers, num_headers);
}

static int s_s3_vip_connection_incoming_header_block_done(
    struct aws_http_stream *stream,
    enum aws_http_header_block header_block,
    void *user_data) {

    (void)stream;

    struct aws_s3_vip_connection *vip_connection = user_data;
    AWS_PRECONDITION(vip_connection);

    struct aws_s3_request *request = vip_connection->request;
    AWS_PRECONDITION(request);

    return aws_s3_request_incoming_header_block_done(request, header_block);
}

static int s_s3_vip_connection_incoming_body(
    struct aws_http_stream *stream,
    const struct aws_byte_cursor *data,
    void *user_data) {

    (void)stream;

    struct aws_s3_vip_connection *vip_connection = user_data;
    AWS_PRECONDITION(vip_connection);

    struct aws_s3_request *request = vip_connection->request;
    AWS_PRECONDITION(request);

    return aws_s3_request_incoming_body(request, data);
}

static void s_s3_vip_connection_stream_complete(struct aws_http_stream *stream, int error_code, void *user_data) {
    (void)stream;

    struct aws_s3_vip_connection *vip_connection = user_data;
    AWS_PRECONDITION(vip_connection);

    struct aws_s3_request *request = vip_connection->request;
    AWS_PRECONDITION(request);

    aws_s3_request_stream_complete(request, error_code);

    s_s3_vip_connection_finish_request(vip_connection, error_code);
}

static void s_s3_vip_connection_finish_request(struct aws_s3_vip_connection *vip_connection, int error_code) {
    AWS_PRECONDITION(vip_connection);

    struct aws_s3_request *request = vip_connection->request;
    AWS_PRECONDITION(request);

    aws_s3_request_finish(request, error_code);

    aws_s3_request_release(request);
    vip_connection->request = NULL;

    s_s3_vip_connection_process_meta_requests(vip_connection);
}
