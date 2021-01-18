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
#include <aws/common/environment.h>
#include <aws/common/string.h>
#include <aws/common/system_info.h>
#include <aws/http/connection.h>
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

enum aws_s3_meta_request_work_op {
    AWS_S3_META_REQUEST_WORK_OP_PUSH,
    AWS_S3_META_REQUEST_WORK_OP_REMOVE,
};

struct aws_s3_meta_request_work {
    struct aws_linked_list_node node;
    struct aws_s3_meta_request *meta_request;
    enum aws_s3_meta_request_work_op op;
};

/* TODO Provide analysis on origins of this value. */
static const double s_throughput_per_vip_gbps = 4.0;

/* TODO Provide more information on these values. */
static const size_t s_default_part_size = 8 * 1024 * 1024;
static const size_t s_default_max_part_size = 32 * 1024 * 1024;
static const double s_default_throughput_target_gbps = 10.0;
static const uint32_t s_default_max_retries = 5;

/* Called when ref count is 0. */
static void s_s3_client_start_destroy(void *user_data);

/* Called by aws_s3_client_check_for_shutdown when all shutdown criteria has been met. */
static void s_s3_client_finish_destroy(void *user_data);

/* Called when the body streaming elg shutdown has completed. */
static void s_s3_client_body_streaming_elg_shutdown(void *user_data);

/* Used to schedule a "work" for a meta request, be it processing of requests or removal from the processing list. */
static void s_s3_client_schedule_meta_request_work(
    struct aws_s3_client *client,
    struct aws_s3_meta_request *meta_request,
    enum aws_s3_meta_request_work_op op);

static void s_s3_client_process_request(struct aws_s3_client *client, struct aws_s3_vip_connection *vip_connection);

/* Default implementation for scheduling processing of work. */
static void s_s3_client_schedule_process_work_synced_default(struct aws_s3_client *client);

/* Actual task function that processes work. */
static void s_s3_client_process_work_task(struct aws_task *task, void *arg, enum aws_task_status task_status);

static void s_s3_client_process_work_default(struct aws_s3_client *client);

/* Task function for streaming body chunks back to the caller. */
static void s_s3_client_body_streaming_task(struct aws_task *task, void *arg, enum aws_task_status task_status);

/* Default factory function for creating a meta request. */
static struct aws_s3_meta_request *s_s3_client_meta_request_factory_default(
    struct aws_s3_client *client,
    const struct aws_s3_meta_request_options *options);

/* Default push-meta-request function to be used in the client vtable. */
static void s_s3_client_push_meta_request_default(
    struct aws_s3_client *client,
    struct aws_s3_meta_request *meta_request);

/* Default remove-meta-request function to be used in the client vtable. */
static void s_s3_client_remove_meta_request_default(
    struct aws_s3_client *client,
    struct aws_s3_meta_request *meta_request);

static struct aws_s3_client_vtable s_s3_client_default_vtable = {
    .meta_request_factory = s_s3_client_meta_request_factory_default,
    .push_meta_request = s_s3_client_push_meta_request_default,
    .remove_meta_request = s_s3_client_remove_meta_request_default,
    .add_vips = aws_s3_client_add_vips_default,
    .remove_vips = aws_s3_client_remove_vips_default,
    .schedule_process_work_synced = s_s3_client_schedule_process_work_synced_default,
    .process_work = s_s3_client_process_work_default,
};

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

    /* Store our client bootstrap. */
    client->client_bootstrap = client_config->client_bootstrap;
    aws_client_bootstrap_acquire(client_config->client_bootstrap);

    struct aws_event_loop_group *event_loop_group = client_config->client_bootstrap->event_loop_group;
    aws_event_loop_group_acquire(event_loop_group);

    client->process_work_event_loop = aws_event_loop_group_get_next_loop(event_loop_group);

    /* Set up body streaming ELG */
    {
        uint16_t num_event_loops =
            (uint16_t)aws_array_list_length(&client->client_bootstrap->event_loop_group->event_loops);
        uint16_t num_streaming_threads = num_event_loops / 2;

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

    aws_mutex_init(&client->synced_data.lock);

    aws_linked_list_init(&client->synced_data.vips);
    aws_linked_list_init(&client->synced_data.pending_vip_connection_updates);
    aws_linked_list_init(&client->synced_data.pending_meta_request_work);

    aws_linked_list_init(&client->threaded_data.idle_vip_connections);
    aws_linked_list_init(&client->threaded_data.meta_requests);

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

static void s_s3_client_reset_active_synced(struct aws_s3_client *client) {
    AWS_PRECONDITION(client);
    ASSERT_SYNCED_DATA_LOCK_HELD(client);
    client->synced_data.active = false;
}

static void s_s3_client_start_destroy(void *user_data) {
    struct aws_s3_client *client = user_data;
    AWS_PRECONDITION(client);

    AWS_LOGF_DEBUG(AWS_LS_S3_CLIENT, "id=%p Client starting destruction..", (void *)client);

    struct aws_linked_list local_vip_list;
    aws_linked_list_init(&local_vip_list);

    struct aws_host_listener *host_listener = NULL;

    aws_s3_client_lock_synced_data(client);

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

    aws_s3_client_check_for_shutdown(client, s_s3_client_reset_active_synced);

    aws_s3_client_lock_synced_data(client);
    aws_s3_client_schedule_process_work_synced(client);
    aws_s3_client_unlock_synced_data(client);
}

void aws_s3_client_check_for_shutdown(struct aws_s3_client *client, s3_client_update_synced_data_state_fn *update_fn) {
    (void)client;

    bool finish_destroy = false;

    aws_s3_client_lock_synced_data(client);

    if (update_fn != NULL) {
        update_fn(client);
    }

    /* This flag should never be set twice. If it was, that means a double-free could occur.*/
    AWS_ASSERT(!client->synced_data.finish_destroy);

    finish_destroy = client->synced_data.active == false && client->synced_data.allocated_vip_count == 0 &&
                     client->synced_data.host_listener_allocated == false &&
                     client->synced_data.body_streaming_elg_allocated == false &&
                     client->synced_data.process_work_task_scheduled == false &&
                     client->synced_data.process_work_task_in_progress == false;

    client->synced_data.finish_destroy = finish_destroy;

    aws_s3_client_unlock_synced_data(client);

    if (finish_destroy) {
        s_s3_client_finish_destroy(client);
    }
}

static void s_s3_client_finish_destroy(void *user_data) {
    AWS_PRECONDITION(user_data);
    struct aws_s3_client *client = user_data;

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

static void s_s3_client_set_body_streaming_elg_shutdown_synced(struct aws_s3_client *client) {
    AWS_PRECONDITION(client);
    ASSERT_SYNCED_DATA_LOCK_HELD(client);

    AWS_LOGF_DEBUG(AWS_LS_S3_CLIENT, "id=%p Client body streaming ELG shutdown.", (void *)client);

    client->synced_data.body_streaming_elg_allocated = false;
}

static void s_s3_client_body_streaming_elg_shutdown(void *user_data) {
    struct aws_s3_client *client = user_data;
    AWS_PRECONDITION(client);

    aws_s3_client_check_for_shutdown(client, s_s3_client_set_body_streaming_elg_shutdown_synced);
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

    if (aws_s3_client_start_resolving_addresses(client)) {
        AWS_LOGF_ERROR(AWS_LS_S3_CLIENT, "id=%p: Could not start resolving endpoint for meta request.", (void *)client);
        return NULL;
    }

    struct aws_s3_meta_request *meta_request = client->vtable->meta_request_factory(client, options);

    if (meta_request == NULL) {
        AWS_LOGF_ERROR(AWS_LS_S3_CLIENT, "id=%p: Could not create new meta request.", (void *)client);
        return NULL;
    }

    meta_request->client_data.body_streaming_event_loop =
        aws_event_loop_group_get_next_loop(client->body_streaming_elg);

    aws_s3_client_push_meta_request(client, meta_request);

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

        return aws_s3_meta_request_auto_ranged_put_new(client->allocator, client, part_size, num_parts, options);
    } else if (options->type == AWS_S3_META_REQUEST_TYPE_DEFAULT) {
        return aws_s3_meta_request_default_new(client->allocator, client, content_length, options);
    } else {
        AWS_FATAL_ASSERT(false);
    }

    return NULL;
}

static void s_s3_client_schedule_meta_request_work(
    struct aws_s3_client *client,
    struct aws_s3_meta_request *meta_request,
    enum aws_s3_meta_request_work_op op) {
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(meta_request);

    aws_s3_client_lock_synced_data(client);

    struct aws_s3_meta_request_work *meta_request_work =
        aws_mem_calloc(client->sba_allocator, 1, sizeof(struct aws_s3_meta_request_work));
    meta_request_work->meta_request = meta_request;
    meta_request_work->op = op;

    aws_s3_meta_request_acquire(meta_request);
    aws_linked_list_push_back(&client->synced_data.pending_meta_request_work, &meta_request_work->node);

    aws_s3_client_schedule_process_work_synced(client);
    aws_s3_client_unlock_synced_data(client);
}

void aws_s3_client_push_meta_request(struct aws_s3_client *client, struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(client->vtable);
    AWS_PRECONDITION(client->vtable->push_meta_request);

    client->vtable->push_meta_request(client, meta_request);
}

static void s_s3_client_push_meta_request_default(
    struct aws_s3_client *client,
    struct aws_s3_meta_request *meta_request) {
    s_s3_client_schedule_meta_request_work(client, meta_request, AWS_S3_META_REQUEST_WORK_OP_PUSH);
}

void aws_s3_client_remove_meta_request(struct aws_s3_client *client, struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(client->vtable);
    AWS_PRECONDITION(client->vtable->remove_meta_request);

    client->vtable->remove_meta_request(client, meta_request);
}

static void s_s3_client_remove_meta_request_default(
    struct aws_s3_client *client,
    struct aws_s3_meta_request *meta_request) {
    s_s3_client_schedule_meta_request_work(client, meta_request, AWS_S3_META_REQUEST_WORK_OP_REMOVE);
}

void aws_s3_client_schedule_process_work_synced(struct aws_s3_client *client) {
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

static void s_s3_client_reset_work_task_in_progress_synced(struct aws_s3_client *client) {
    AWS_PRECONDITION(client);
    ASSERT_SYNCED_DATA_LOCK_HELD(client);
    client->synced_data.process_work_task_in_progress = false;
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

    bool client_active = false;
    bool invalid_endpoint = false;

    struct aws_linked_list meta_request_work_list;
    aws_linked_list_init(&meta_request_work_list);

    struct aws_linked_list vip_connections_updates;
    aws_linked_list_init(&vip_connections_updates);

    /*******************/
    /* Step 1: Move everything into thread local memory. */
    /*******************/
    aws_s3_client_lock_synced_data(client);

    client_active = client->synced_data.active != 0;
    invalid_endpoint = client->synced_data.invalid_endpoint != 0;

    /* Once we exit this mutex, someone can reschedule this task. */
    client->synced_data.process_work_task_scheduled = false;
    client->synced_data.process_work_task_in_progress = true;

    aws_linked_list_swap_contents(&meta_request_work_list, &client->synced_data.pending_meta_request_work);
    aws_linked_list_swap_contents(&vip_connections_updates, &client->synced_data.pending_vip_connection_updates);

    /* TODO aws_sub_u32_checked */
    client->threaded_data.num_requests_in_flight -= client->synced_data.pending_request_count;
    client->synced_data.pending_request_count = 0;

    aws_s3_client_unlock_synced_data(client);

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
            } else {
                aws_s3_meta_request_release(meta_request);
                meta_request = NULL;
            }
        } else if (meta_request_work->op == AWS_S3_META_REQUEST_WORK_OP_REMOVE) {
            if (meta_request->client_process_work_threaded_data.scheduled) {

                if (client->threaded_data.next_meta_request == meta_request) {
                    client->threaded_data.next_meta_request =
                        s_s3_client_next_meta_request_threaded(client, meta_request);
                }

                s_s3_client_remove_meta_request_threaded(client, meta_request);
            } else {
                aws_s3_meta_request_release(meta_request);
                meta_request = NULL;
            }
        }

        aws_mem_release(client->sba_allocator, meta_request_work);
    }

    /* If we have an invalid endpoint, then finish up all of the meta requests. */
    /* TODO once we have multiple bucket support, this will should only stop meta requests attached to bad endpoints. */
    if (invalid_endpoint) {
        while (!aws_linked_list_empty(&client->threaded_data.meta_requests)) {
            struct aws_linked_list_node *node = aws_linked_list_begin(&client->threaded_data.meta_requests);
            struct aws_s3_meta_request *meta_request =
                AWS_CONTAINER_OF(node, struct aws_s3_meta_request, client_process_work_threaded_data);

            aws_s3_meta_request_finish(meta_request, NULL, 0, AWS_ERROR_S3_INVALID_ENDPOINT);
            s_s3_client_remove_meta_request_threaded(client, meta_request);
        }
    }

    /*******************/
    /* Step 3: Go through all VIP connections, cleaning up old ones, and assigning requests where possible. */
    /*******************/

    /* Move all idle connections into our local update list. */
    aws_linked_list_move_all_back(&vip_connections_updates, &client->threaded_data.idle_vip_connections);

    const uint32_t max_requests_multiplier = 4;
    const uint32_t max_requests_in_flight =
        client->ideal_vip_count * g_num_connections_per_vip * max_requests_multiplier;

    while (!aws_linked_list_empty(&vip_connections_updates)) {

        struct aws_linked_list_node *node = aws_linked_list_pop_front(&vip_connections_updates);
        struct aws_s3_vip_connection *vip_connection = AWS_CONTAINER_OF(node, struct aws_s3_vip_connection, node);

        /*
        TODO
        size_t owning_vip_active = aws_atomic_load_int(&vip_connection->owning_vip->active);

        if (!owning_vip_active &&
            (!client_active || (vip_connection->http_connection == NULL ||
                                !aws_http_connection_is_open(vip_connection->http_connection) ||
                                vip_connection->request_count >= s_s3_max_request_count_per_connection))) {
            aws_s3_vip_connection_destroy(client, vip_connection);
            continue;
        }
        */

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

            /* If the request is null, then this meta request is currently out of requests, so remove it. */
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
            vip_connection->is_retry = false;
            request->request_was_sent = true;
            ++client->threaded_data.num_requests_in_flight;
            s_s3_client_process_request(client, vip_connection);
        }
    }

    aws_s3_client_check_for_shutdown(client, s_s3_client_reset_work_task_in_progress_synced);
}

static void s_s3_client_acquired_retry_token(
    struct aws_retry_strategy *retry_strategy,
    int error_code,
    struct aws_retry_token *token,
    void *user_data);

static void s_s3_client_retry_ready(struct aws_retry_token *token, int error_code, void *user_data);

static void s_s3_client_process_request(struct aws_s3_client *client, struct aws_s3_vip_connection *vip_connection) {
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(vip_connection);

    struct aws_s3_request *request = vip_connection->request;
    AWS_PRECONDITION(request);

    struct aws_s3_meta_request *meta_request = request->meta_request;
    AWS_PRECONDITION(meta_request);

    if (vip_connection->retry_token != NULL) {
        s_s3_client_acquired_retry_token(
            client->retry_strategy, AWS_ERROR_SUCCESS, vip_connection->retry_token, vip_connection);
    } else {

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

    /* 
    TODO
    s_s3_client_acquire_http_connection(client, vip_connection);
    */

    /* TODO */
    // aws_s3_meta_request_make_request(meta_request, client, vip_connection);

    return;

error_clean_up:

    aws_s3_client_notify_connection_finished(
        client, vip_connection, error_code, AWS_S3_VIP_CONNECTION_FINISH_CODE_FAILED);
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

    /* ++vip_connection->request_count; TODO */

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
        aws_s3_meta_request_finish(meta_request, request, request->send_data.response_status, error_code);

        if (vip_connection->http_connection != NULL) {
            aws_http_connection_close(vip_connection->http_connection);
            ++vip_connection->request_count;
        }
    }

    /* Get rid of the attached request. */
    aws_s3_request_release(vip_connection->request);
    vip_connection->request = NULL;
    ++vip_connection->request_count;

    if (vip_connection->http_connection == NULL) {

        aws_s3_client_close_vip_connection(client, vip_connection);

    } else if (vip_connection->request_count >= vip_connection->max_request_count) {

        AWS_LOGF_INFO(
            AWS_LS_S3_CLIENT, "id=%p VIP Connection %p hit request limit.", (void *)client, (void *)vip_connection);

        aws_s3_client_close_vip_connection(client, vip_connection);

    } else if (!aws_http_connection_is_open(vip_connection->http_connection)) {
        AWS_LOGF_INFO(
            AWS_LS_S3_CLIENT,
            "id=%p VIP Connection %p being released because it is not open.",
            (void *)client,
            (void *)vip_connection);

        aws_s3_client_close_vip_connection(client, vip_connection);
    } else {
        /* Throw this VIP Connection structure back into the update list. */
        aws_s3_client_lock_synced_data(client);
        aws_linked_list_push_back(&client->synced_data.pending_vip_connection_updates, &vip_connection->node);
        aws_s3_client_schedule_process_work_synced(client);
        aws_s3_client_unlock_synced_data(client);
    }
}

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

    s_s3_client_process_request(client, vip_connection);

    return;

error_clean_up:

    aws_s3_client_notify_connection_finished(
        client, vip_connection, error_code, AWS_S3_VIP_CONNECTION_FINISH_CODE_FAILED);
}

/* Called by aws_s3_request when it has finished being destroyed */
void aws_s3_client_notify_request_destroyed(struct aws_s3_client *client, struct aws_s3_request *request) {
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(request);

    if (request->request_was_sent) {
        aws_s3_client_lock_synced_data(client);
        ++client->synced_data.pending_request_count;
        aws_s3_client_schedule_process_work_synced(client);
        aws_s3_client_unlock_synced_data(client);
    }
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

    AWS_LOGF_DEBUG(
        AWS_LS_S3_CLIENT,
        "id=%p Scheduling body streaming task for meta request %p.",
        (void *)client,
        (void *)meta_request);

    struct s3_streaming_body_payload *payload =
        aws_mem_calloc(client->sba_allocator, 1, sizeof(struct s3_streaming_body_payload));

    aws_s3_client_acquire(client);
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
        struct aws_s3_meta_request *meta_request = request->meta_request;

        bool active = aws_s3_meta_request_check_active(meta_request);

        if (!active) {
            AWS_LOGF_DEBUG(
                AWS_LS_S3_CLIENT,
                "id=%p meta request %p is not active, drop the body of s3 request %p.",
                (void *)client,
                (void *)meta_request,
                (void *)request);
            aws_s3_request_release(request);
            continue;
        }

        struct aws_byte_cursor body_buffer_byte_cursor = aws_byte_cursor_from_buf(&request->send_data.response_body);

        AWS_ASSERT(request->part_number >= 1);

        if (aws_s3_meta_request_is_finished(meta_request)) {
            aws_s3_request_release(request);
            continue;
        }

        uint64_t range_start = (request->part_number - 1) * meta_request->part_size;
        if (meta_request->body_callback != NULL) {
            if (meta_request->body_callback(
                    meta_request, &body_buffer_byte_cursor, range_start, meta_request->user_data)) {
                aws_s3_meta_request_finish(meta_request, NULL, 0, aws_last_error_or_unknown());
            }
        }

        aws_s3_request_release(request);
    }

    aws_mem_release(client->sba_allocator, payload);
    aws_s3_client_release(client);
}
