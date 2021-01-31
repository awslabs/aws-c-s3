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
    bool push_front;
};

static const uint32_t s_connection_timeout_ms = 3000;

/* TODO Provide analysis on origins of this value. */
static const double s_throughput_per_vip_gbps = 4.0;
static const uint32_t s_num_connections_per_vip = 10;

/* Max number of connections to open, avg per second */
/*
static const bool s_max_conn_open_rate_enabled = false;
static const uint32_t s_max_conn_open_count = 24;
static const uint64_t s_max_conn_open_rate = 1000;
*/

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

typedef void(s3_client_update_synced_data_state_fn)(struct aws_s3_client *client, void *user_data);

/* Used to atomically update client state during clean-up and check for finishing shutdown. */
static void s_s3_client_check_for_shutdown(
    struct aws_s3_client *client,
    s3_client_update_synced_data_state_fn *update_fn,
    void *user_data);

/* Called by s_s3_client_check_for_shutdown when all shutdown criteria has been met. */
static void s_s3_client_finish_destroy(void *user_data);

/* Called when the body streaming elg shutdown has completed. */
static void s_s3_client_body_streaming_elg_shutdown(void *user_data);

static void s_s3_client_process_request(struct aws_s3_client *client, struct aws_s3_vip_connection *vip_connection);

/* Callback which handles the HTTP connection retrieved by acquire_http_connection. */
static void s_s3_client_on_acquire_http_connection(
    struct aws_http_connection *http_connection,
    int error_code,
    void *user_data);

static void s_s3_client_push_meta_request_synced(
    struct aws_s3_client *client,
    struct aws_s3_meta_request *meta_request,
    bool push_front);

/* Schedule task for processing work. (Calls the corresponding vtable function.) */
static void s_s3_client_schedule_process_work_synced(struct aws_s3_client *client);

/* Default implementation for scheduling processing of work. */
static void s_s3_client_schedule_process_work_synced_default(struct aws_s3_client *client);

/* Actual task function that processes work. */
static void s_s3_client_process_work_task(struct aws_task *task, void *arg, enum aws_task_status task_status);

static void s_s3_client_process_work_default(struct aws_s3_client *client);

static void s_s3_vip_connection_move_new_connections(struct aws_s3_vip_connection *vip_connection);

static void s_s3_client_on_acquire_http_connection(
    struct aws_http_connection *incoming_http_connection,
    int error_code,
    void *user_data);

static void s_s3_client_on_acquire_http_connection_default(
    struct aws_http_connection *incoming_http_connection,
    int error_code,
    void *user_data);

/* Task function for streaming body chunks back to the caller. */
static void s_s3_client_body_streaming_task(struct aws_task *task, void *arg, enum aws_task_status task_status);

/* Ask the host resolver to start resolving addresses. */
static int s_s3_client_start_resolving_addresses(struct aws_s3_client *client);

/* Default factory function for creating a meta request. */
static struct aws_s3_meta_request *s_s3_client_meta_request_factory_default(
    struct aws_s3_client *client,
    const struct aws_s3_meta_request_options *options);

static struct aws_s3_client_vtable s_s3_client_default_vtable = {
    .meta_request_factory = s_s3_client_meta_request_factory_default,
    .schedule_process_work_synced = s_s3_client_schedule_process_work_synced_default,
    .process_work = s_s3_client_process_work_default,
    .on_acquire_http_connection = s_s3_client_on_acquire_http_connection_default,
};

void aws_s3_set_dns_ttl(size_t ttl) {
    s_dns_host_address_ttl_seconds = ttl;
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

    aws_linked_list_init(&client->synced_data.pending_meta_request_work);
    aws_linked_list_init(&client->threaded_data.meta_requests);
    aws_linked_list_init(&client->synced_data.pending_connections);
    aws_linked_list_init(&client->threaded_data.connections);

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

    /*
        aws_array_list_init_dynamic(
            &client->threaded_data.open_conn_timestamps_millis, client->allocator, s_max_conn_open_count,
       sizeof(uint64_t));
    */

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

static void s_s3_client_reset_active_synced(struct aws_s3_client *client, void *user_data) {
    AWS_PRECONDITION(client);
    (void)user_data;
    ASSERT_SYNCED_DATA_LOCK_HELD(client);
    client->synced_data.active = false;
    s_s3_client_schedule_process_work_synced(client);
}

static void s_s3_client_start_destroy(void *user_data) {
    struct aws_s3_client *client = user_data;
    AWS_PRECONDITION(client);

    AWS_LOGF_DEBUG(AWS_LS_S3_CLIENT, "id=%p Client starting destruction..", (void *)client);

    aws_event_loop_group_release(client->body_streaming_elg);
    client->body_streaming_elg = NULL;

    s_s3_client_check_for_shutdown(client, s_s3_client_reset_active_synced, NULL);
}

static void s_s3_client_check_for_shutdown(
    struct aws_s3_client *client,
    s3_client_update_synced_data_state_fn *update_fn,
    void *user_data) {
    (void)client;

    bool finish_destroy = false;

    struct aws_http_connection_manager *connection_manager = NULL;

    aws_s3_client_lock_synced_data(client);

    if (update_fn != NULL) {
        update_fn(client, user_data);
    }

    /* This flag should never be set twice. If it was, that means a double-free could occur.*/
    AWS_ASSERT(!client->synced_data.finish_destroy);

    if (client->synced_data.active) {
        goto unlock;
    }

    if (client->synced_data.body_streaming_elg_allocated) {
        goto unlock;
    }

    if (client->synced_data.process_work_task_scheduled) {
        goto unlock;
    }

    if (client->synced_data.process_work_task_in_progress) {
        goto unlock;
    }

    if (client->synced_data.connection_manager_active) {
        if (client->synced_data.connection_manager != NULL) {
            connection_manager = client->synced_data.connection_manager;
            client->synced_data.connection_manager = NULL;
        }

        goto unlock;
    }

    finish_destroy = true;
    client->synced_data.finish_destroy = true;

unlock:
    aws_s3_client_unlock_synced_data(client);

    if (connection_manager != NULL) {
        aws_http_connection_manager_release(connection_manager);
        connection_manager = NULL;
    }

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
    /*
        aws_array_list_clean_up(&client->threaded_data.open_conn_timestamps_millis);
    */
    if (client->tls_connection_options) {
        aws_tls_connection_options_clean_up(client->tls_connection_options);
        aws_mem_release(client->allocator, client->tls_connection_options);
        client->tls_connection_options = NULL;
    }

    aws_mutex_clean_up(&client->synced_data.lock);

    AWS_ASSERT(aws_linked_list_empty(&client->synced_data.pending_meta_request_work));
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

static void s_s3_client_set_body_streaming_elg_shutdown_synced(struct aws_s3_client *client, void *user_data) {
    AWS_PRECONDITION(client);
    (void)user_data;

    ASSERT_SYNCED_DATA_LOCK_HELD(client);

    AWS_LOGF_DEBUG(AWS_LS_S3_CLIENT, "id=%p Client body streaming ELG shutdown.", (void *)client);

    client->synced_data.body_streaming_elg_allocated = false;
}

static void s_s3_client_body_streaming_elg_shutdown(void *user_data) {
    struct aws_s3_client *client = user_data;
    AWS_PRECONDITION(client);

    s_s3_client_check_for_shutdown(client, s_s3_client_set_body_streaming_elg_shutdown_synced, NULL);
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
    bool resolve_endpoint = false;

    aws_s3_client_lock_synced_data(client);

    /* TODO This is temporary until we add multiple bucket support. */
    if (client->synced_data.endpoint == NULL) {
        client->synced_data.endpoint =
            aws_string_new_from_array(client->allocator, host_header_value.ptr, host_header_value.len);
        endpoint_matches = true;
        resolve_endpoint = true;
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

    if (resolve_endpoint && s_s3_client_start_resolving_addresses(client)) {
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

    aws_s3_client_lock_synced_data(client);
    s_s3_client_push_meta_request_synced(client, meta_request, false);
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

        return aws_s3_meta_request_auto_ranged_put_new(client->allocator, client, part_size, num_parts, options);
    } else if (options->type == AWS_S3_META_REQUEST_TYPE_DEFAULT) {
        return aws_s3_meta_request_default_new(client->allocator, client, content_length, options);
    } else {
        AWS_FATAL_ASSERT(false);
    }

    return NULL;
}

static void s_s3_client_push_meta_request_synced(
    struct aws_s3_client *client,
    struct aws_s3_meta_request *meta_request,
    bool push_front) {
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(meta_request);
    ASSERT_SYNCED_DATA_LOCK_HELD(client);

    struct aws_s3_meta_request_work *meta_request_work =
        aws_mem_calloc(client->sba_allocator, 1, sizeof(struct aws_s3_meta_request_work));

    aws_s3_meta_request_acquire(meta_request);
    meta_request_work->meta_request = meta_request;
    meta_request_work->push_front = push_front;
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

static void s_s3_client_reset_work_task_in_progress_synced(struct aws_s3_client *client, void *user_data) {
    AWS_PRECONDITION(client);
    (void)user_data;
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
    aws_linked_list_move_all_back(&client->threaded_data.connections, &client->synced_data.pending_connections);

    /* TODO aws_sub_u32_checked */
    client->threaded_data.num_requests_in_flight -= client->synced_data.pending_request_count;
    client->synced_data.pending_request_count = 0;

    struct aws_http_connection_manager *connection_manager = client->synced_data.connection_manager;

    if (connection_manager != NULL) {
        aws_http_connection_manager_acquire(connection_manager);
    }

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

        if (!meta_request->client_process_work_threaded_data.scheduled) {

            if (meta_request_work->push_front) {
                aws_linked_list_push_front(
                    &client->threaded_data.meta_requests, &meta_request->client_process_work_threaded_data.node);
            } else {
                aws_linked_list_push_back(
                    &client->threaded_data.meta_requests, &meta_request->client_process_work_threaded_data.node);
            }

            meta_request->client_process_work_threaded_data.scheduled = true;
        } else {
            aws_s3_meta_request_release(meta_request);
            meta_request = NULL;
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

            aws_s3_meta_request_next_request(
                meta_request, NULL, AWS_S3_META_REQUEST_NEXT_REQUEST_FLAG_NO_ENDPOINT_CONNECTIONS);

            s_s3_client_remove_meta_request_threaded(client, meta_request);
        }
    }

    /*******************/
    /* Step 3: Go through all VIP connections, cleaning up old ones, and assigning requests where possible. */
    /*******************/
    if (!client_active) {

        struct aws_linked_list_node *node = aws_linked_list_begin(&client->threaded_data.connections);

        while (!aws_linked_list_empty(&client->threaded_data.connections) &&
               node != aws_linked_list_end(&client->threaded_data.connections)) {

            struct aws_s3_vip_connection *vip_connection = AWS_CONTAINER_OF(node, struct aws_s3_vip_connection, node);
            node = aws_linked_list_next(node);

            s_s3_vip_connection_move_new_connections(vip_connection);

            if (vip_connection->num_pending_acquisition_count > 0) {
                continue;
            }

            for (uint32_t i = 0; i < vip_connection->num_connections; ++i) {
                struct aws_http_connection *connection = vip_connection->connections[i];

                if (connection != NULL) {
                    aws_http_connection_manager_release_connection(vip_connection->connection_manager, connection);
                    connection = NULL;
                }
            }

            if (vip_connection->connection_manager != NULL) {
                aws_http_connection_manager_release(vip_connection->connection_manager);
                vip_connection->connection_manager = NULL;
            }

            aws_linked_list_remove(&vip_connection->node);
            aws_mem_release(client->sba_allocator, vip_connection);
        }

        goto check_for_shutdown;
    }

    const uint32_t max_requests_multiplier = 4;
    const uint32_t max_meta_requests_process_depth = client->ideal_vip_count * s_num_connections_per_vip;
    const uint32_t max_requests_in_flight =
        client->ideal_vip_count * s_num_connections_per_vip * max_requests_multiplier;

    struct aws_s3_meta_request *current_meta_request = NULL;

    while (!aws_linked_list_empty(&client->threaded_data.connections) &&
           !aws_linked_list_empty(&client->threaded_data.meta_requests) &&
           client->threaded_data.num_requests_in_flight < max_requests_in_flight) {

        struct aws_linked_list_node *node = aws_linked_list_pop_front(&client->threaded_data.connections);
        struct aws_s3_vip_connection *vip_connection = AWS_CONTAINER_OF(node, struct aws_s3_vip_connection, node);

        struct aws_s3_request *request = NULL;
        uint32_t current_meta_request_index = 0;

        /* While we haven't found a request yet for our VIP Connection and there are still meta requests to look
         * through...*/
        while (request == NULL && !aws_linked_list_empty(&client->threaded_data.meta_requests)) {

            if (current_meta_request == NULL) {
                struct aws_linked_list_node *begin_node = aws_linked_list_begin(&client->threaded_data.meta_requests);
                current_meta_request =
                    AWS_CONTAINER_OF(begin_node, struct aws_s3_meta_request, client_process_work_threaded_data);
            }

            struct aws_s3_meta_request *next_meta_request = NULL;

            /* Figure out which meta request is next line in early so that we can remove the meta request if we need to.
             */
            struct aws_linked_list_node *next_node =
                aws_linked_list_next(&current_meta_request->client_process_work_threaded_data.node);

            /* If the next node is the end node, wrap around to the beginning node. */
            if (next_node != aws_linked_list_end(&client->threaded_data.meta_requests)) {
                next_meta_request =
                    AWS_CONTAINER_OF(next_node, struct aws_s3_meta_request, client_process_work_threaded_data);
            }

            /* Grab the next request from the meta request. */
            aws_s3_meta_request_next_request(current_meta_request, &request, 0);

            if (request == NULL) {
                s_s3_client_remove_meta_request_threaded(client, current_meta_request);
            } else {
                ++current_meta_request_index;
            }

            if ((current_meta_request_index + 1) < max_meta_requests_process_depth) {
                current_meta_request = next_meta_request;
            } else {
                current_meta_request_index = 0;
                current_meta_request = NULL;
            }
        }

        if (request != NULL) {
            request->client_data.request_was_sent = true;

            ++client->threaded_data.num_requests_in_flight;

            vip_connection->request = request;

            s_s3_client_process_request(client, vip_connection);

        } else {
            aws_linked_list_push_front(&client->threaded_data.connections, &vip_connection->node);
            break;
        }
    }

check_for_shutdown:

    if (connection_manager != NULL) {
        aws_http_connection_manager_release(connection_manager);
        connection_manager = NULL;
    }

    s_s3_client_check_for_shutdown(client, s_s3_client_reset_work_task_in_progress_synced, NULL);
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

static void s_s3_vip_connection_move_new_connections(struct aws_s3_vip_connection *vip_connection) {
    AWS_PRECONDITION(vip_connection);

    aws_s3_client_lock_synced_data(vip_connection->owning_client);

    for (uint32_t i = 0; i < vip_connection->synced_data.num_new_connections; ++i) {
        vip_connection->connections[vip_connection->num_connections++] = vip_connection->synced_data.new_connections[i];
        --vip_connection->num_pending_acquisition_count;
    }

    vip_connection->synced_data.num_new_connections = 0;

    aws_s3_client_unlock_synced_data(vip_connection->owning_client);
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

    struct aws_s3_client *client = vip_connection->owning_client;
    AWS_PRECONDITION(client);

    if (error_code != AWS_ERROR_SUCCESS) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_CLIENT,
            "id=%p Client could not get retry token for vip connection %p processing request %p due to error %d "
            "(%s)",
            (void *)client,
            (void *)vip_connection,
            (void *)request,
            error_code,
            aws_error_str(error_code));

        goto error_clean_up;
    }

    AWS_ASSERT(token);

    vip_connection->retry_token = token;

    s_s3_vip_connection_move_new_connections(vip_connection);

    uint32_t i = 0;

    while (i < vip_connection->num_connections) {
        struct aws_http_connection *connection = vip_connection->connections[i];

        /* Throw away connections that have closed out from underneath of us. */
        if (connection != NULL && !aws_http_connection_is_open(connection)) {
            aws_http_connection_manager_release_connection(vip_connection->connection_manager, connection);
            connection = NULL;
        }

        if (connection == NULL) {
            --vip_connection->num_connections;

            /* Keep all open connections at the beginning of the list */
            vip_connection->connections[i] = vip_connection->connections[vip_connection->num_connections];
            vip_connection->connections[vip_connection->num_connections] = NULL;
        } else {
            ++i;
        }
    }

    uint32_t num_connections_to_acquire =
        S3_NUM_HTTP_CONNECTIONS_PER_S3_CONNECTION - (vip_connection->num_connections + vip_connection->num_pending_acquisition_count);

    for (uint32_t i = 0; i < num_connections_to_acquire; ++i) {
        ++vip_connection->num_pending_acquisition_count;

        aws_http_connection_manager_acquire_connection(
            vip_connection->connection_manager, s_s3_client_on_acquire_http_connection, vip_connection);
    }

    if (vip_connection->num_connections > 0) {
        vip_connection->active_connection = vip_connection->connections[0];
        aws_s3_meta_request_make_request(request->meta_request, client, vip_connection);
    } else {
        aws_atomic_store_int(&vip_connection->waiting_for_active_connection, 1);
    }

    return;

error_clean_up:

    aws_s3_client_notify_connection_finished(
        client, vip_connection, error_code, AWS_S3_VIP_CONNECTION_FINISH_CODE_FAILED);
}

static void s_s3_client_on_acquire_http_connection(
    struct aws_http_connection *incoming_http_connection,
    int error_code,
    void *user_data) {

    struct aws_s3_vip_connection *vip_connection = user_data;
    AWS_PRECONDITION(vip_connection);

    struct aws_s3_client *client = vip_connection->owning_client;
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(client->vtable);
    AWS_PRECONDITION(client->vtable->on_acquire_http_connection);

    client->vtable->on_acquire_http_connection(incoming_http_connection, error_code, user_data);
}

static void s_s3_client_on_acquire_http_connection_default(
    struct aws_http_connection *incoming_http_connection,
    int error_code,
    void *user_data) {

    struct aws_s3_vip_connection *vip_connection = user_data;
    AWS_PRECONDITION(vip_connection);

    struct aws_s3_client *client = vip_connection->owning_client;
    AWS_PRECONDITION(client);

    bool first_connection = aws_atomic_exchange_int(&vip_connection->waiting_for_active_connection, 0) == 1;

    if (error_code != AWS_ERROR_SUCCESS) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_VIP_CONNECTION,
            "id=%p: Could not acquire connection due to error code %d (%s)",
            (void *)vip_connection,
            error_code,
            aws_error_str(error_code));

        if (first_connection) {
            aws_s3_client_notify_connection_finished(
                client, vip_connection, error_code, AWS_S3_VIP_CONNECTION_FINISH_CODE_RETRY);
        }

        AWS_ASSERT(incoming_http_connection == NULL);

    } else if (first_connection) {
        struct aws_s3_request *request = vip_connection->request;
        AWS_ASSERT(request);

        struct aws_s3_meta_request *meta_request = request->meta_request;
        AWS_ASSERT(meta_request);

        vip_connection->active_connection = incoming_http_connection;
        aws_s3_meta_request_make_request(meta_request, client, vip_connection);
    }

    aws_s3_client_lock_synced_data(client);
    vip_connection->synced_data.new_connections[vip_connection->synced_data.num_new_connections++] =
        incoming_http_connection;

    if (!first_connection && !client->synced_data.active) {
        s_s3_client_schedule_process_work_synced(client);
    }
    aws_s3_client_unlock_synced_data(client);
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

    if (vip_connection->active_connection != NULL) {
        if (finish_code != AWS_S3_VIP_CONNECTION_FINISH_CODE_SUCCESS) {
            aws_http_connection_close(vip_connection->active_connection);
        }
    }

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

    aws_s3_meta_request_finished_request(meta_request, request, error_code);

    /* Grab a reference to the meta request since we got it from the request, and we want to use after we release
     * the request.*/
    aws_s3_meta_request_acquire(meta_request);

    /* Get rid of the attached request. */
    aws_s3_request_release(vip_connection->request);
    vip_connection->request = NULL;

    aws_s3_client_lock_synced_data(client);
    aws_linked_list_push_back(&client->synced_data.pending_connections, &vip_connection->node);
    s_s3_client_push_meta_request_synced(client, meta_request, true);
    s_s3_client_schedule_process_work_synced(client);
    aws_s3_client_unlock_synced_data(client);

    aws_s3_meta_request_release(meta_request);
}

static void s_s3_client_retry_ready(struct aws_retry_token *token, int error_code, void *user_data) {
    AWS_PRECONDITION(token);
    (void)token;

    struct aws_s3_vip_connection *vip_connection = user_data;
    AWS_PRECONDITION(vip_connection);

    struct aws_s3_client *client = vip_connection->owning_client;
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

    if (request->client_data.request_was_sent) {
        aws_s3_client_lock_synced_data(client);
        ++client->synced_data.pending_request_count;
        s_s3_client_schedule_process_work_synced(client);
        aws_s3_client_unlock_synced_data(client);
    }
}

struct s3_streaming_body_payload {
    struct aws_s3_client *client;
    struct aws_s3_meta_request *meta_request;
    struct aws_linked_list requests;
    aws_s3_client_stream_response_body_callback_fn *callback;
    void *user_data;
    struct aws_task task;
};

void aws_s3_client_stream_response_body(
    struct aws_s3_client *client,
    struct aws_s3_meta_request *meta_request,
    struct aws_linked_list *requests,
    aws_s3_client_stream_response_body_callback_fn callback,
    void *user_data) {
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

    aws_s3_meta_request_acquire(meta_request);
    payload->meta_request = meta_request;

    aws_linked_list_init(&payload->requests);
    aws_linked_list_move_all_back(&payload->requests, requests);

    payload->callback = callback;
    payload->user_data = user_data;

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

    struct aws_linked_list completed_requests;
    aws_linked_list_init(&completed_requests);

    int error_code = AWS_ERROR_SUCCESS;
    uint32_t num_successful = 0;
    uint32_t num_failed = 0;

    while (!aws_linked_list_empty(&payload->requests)) {
        struct aws_linked_list_node *request_node = aws_linked_list_pop_front(&payload->requests);
        struct aws_s3_request *request = AWS_CONTAINER_OF(request_node, struct aws_s3_request, node);
        struct aws_s3_meta_request *meta_request = request->meta_request;

        struct aws_byte_cursor body_buffer_byte_cursor = aws_byte_cursor_from_buf(&request->send_data.response_body);

        AWS_ASSERT(request->part_number >= 1);

        uint64_t range_start = (request->part_number - 1) * meta_request->part_size;

        if (aws_s3_meta_request_is_finishing(meta_request)) {
            ++num_failed;
        } else {
            if (error_code == AWS_ERROR_SUCCESS && meta_request->body_callback &&
                meta_request->body_callback(
                    meta_request, &body_buffer_byte_cursor, range_start, meta_request->user_data)) {
                error_code = aws_last_error_or_unknown();
            }

            if (error_code == AWS_ERROR_SUCCESS) {
                ++num_successful;
            } else {
                ++num_failed;
            }
        }

        aws_s3_request_release(request);
    }

    if (payload->callback != NULL) {
        payload->callback(error_code, num_failed, num_successful, payload->user_data);
    }

    aws_s3_client_lock_synced_data(client);
    s_s3_client_push_meta_request_synced(client, payload->meta_request, true);
    s_s3_client_schedule_process_work_synced(client);
    aws_s3_client_unlock_synced_data(client);

    aws_s3_meta_request_release(payload->meta_request);
    aws_mem_release(client->sba_allocator, payload);
    aws_s3_client_release(client);
}

static void s_s3_client_set_connection_manager_inactive_synced(struct aws_s3_client *client, void *user_data) {
    AWS_PRECONDITION(client);
    (void)user_data;
    ASSERT_SYNCED_DATA_LOCK_HELD(client);

    AWS_LOGF_DEBUG(AWS_LS_S3_CLIENT, "id=%p Client connection manager shutdown.", (void *)client);

    client->synced_data.connection_manager_active = false;
}

static void s_s3_client_connection_manager_shutdown_callback(void *user_data) {
    struct aws_s3_client *client = user_data;
    AWS_PRECONDITION(client);

    s_s3_client_check_for_shutdown(client, s_s3_client_set_connection_manager_inactive_synced, NULL);
}

static void s_s3_client_on_host_resolver_address_resolved(
    struct aws_host_resolver *resolver,
    const struct aws_string *host_name,
    int err_code,
    const struct aws_array_list *host_addresses,
    void *user_data) {
    (void)resolver;
    (void)host_addresses;

    AWS_PRECONDITION(resolver);
    AWS_PRECONDITION(host_name);
    AWS_PRECONDITION(user_data);

    struct aws_s3_client *client = user_data;

    if (err_code != AWS_ERROR_SUCCESS) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_CLIENT,
            "id=%p Error when resolving endpoint '%s' due to error %d (%s)",
            (void *)client,
            (const char *)host_name->bytes,
            err_code,
            aws_error_str(err_code));

        aws_s3_client_lock_synced_data(client);
        client->synced_data.invalid_endpoint = true;
        s_s3_client_schedule_process_work_synced(client);
        aws_s3_client_unlock_synced_data(client);
        return;
    }

    const uint32_t max_active_connections = client->ideal_vip_count * s_num_connections_per_vip;

    struct aws_byte_cursor host_name_cursor = aws_byte_cursor_from_string(host_name);

    /* Try to set up the connection manager. */
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
    manager_options.host = host_name_cursor;
    manager_options.max_connections = max_active_connections * S3_NUM_HTTP_CONNECTIONS_PER_S3_CONNECTION;
    manager_options.shutdown_complete_callback = s_s3_client_connection_manager_shutdown_callback;
    manager_options.shutdown_complete_user_data = client;

    struct aws_uri proxy_uri;
    AWS_ZERO_STRUCT(proxy_uri);
    struct aws_http_proxy_options *proxy_options = NULL;
    struct aws_tls_connection_options *proxy_tls_options = NULL;
    struct aws_tls_connection_options *manager_tls_options = NULL;
    struct aws_http_connection_manager *connection_manager = NULL;

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

        aws_tls_connection_options_set_server_name(manager_tls_options, client->allocator, &host_name_cursor);

        manager_options.tls_connection_options = manager_tls_options;
        manager_options.port = s_https_port;
    } else {
        manager_options.port = s_http_port;
    }

    connection_manager = aws_http_connection_manager_new(client->allocator, &manager_options);

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

    if (connection_manager == NULL) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_CLIENT,
            "id=%p Could not set up connection manager for endpoint '%s' due to error %d (%s)",
            (void *)client,
            (const char *)host_name->bytes,
            err_code,
            aws_error_str(err_code));

        aws_s3_client_lock_synced_data(client);
        client->synced_data.invalid_endpoint = true;
        s_s3_client_schedule_process_work_synced(client);
        aws_s3_client_unlock_synced_data(client);
    } else {
        aws_s3_client_lock_synced_data(client);

        for (uint32_t i = 0; i < max_active_connections; ++i) {

            struct aws_s3_vip_connection *vip_connection =
                aws_mem_calloc(client->sba_allocator, 1, sizeof(struct aws_s3_vip_connection));
            aws_http_connection_manager_acquire(connection_manager);
            vip_connection->owning_client = client;
            vip_connection->connection_manager = connection_manager;
            vip_connection->num_connections = 0;

            aws_atomic_init_int(&vip_connection->waiting_for_active_connection, 0);

            aws_linked_list_push_back(&client->synced_data.pending_connections, &vip_connection->node);
        }

        client->synced_data.connection_manager_active = true;
        client->synced_data.connection_manager = connection_manager;
        s_s3_client_schedule_process_work_synced(client);
        aws_s3_client_unlock_synced_data(client);
    }
}

static int s_s3_client_start_resolving_addresses(struct aws_s3_client *client) {
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(client->client_bootstrap);
    AWS_PRECONDITION(client->client_bootstrap->host_resolver);

    struct aws_host_resolver *host_resolver = client->client_bootstrap->host_resolver;

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

        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}
