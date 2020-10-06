/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "s3_tester.h"
#include <aws/auth/credentials.h>
#include <aws/io/channel_bootstrap.h>
#include <aws/io/event_loop.h>
#include <aws/io/host_resolver.h>
#include <aws/testing/aws_test_harness.h>

/* Wait for the cleanup notification.  This, and the s_tester_notify_clean_up_signal function are meant to be used for
 * sequential clean up only, and should not overlap with the "finish" callback.  (Both currently use the same
 * mutex/signal.) */
static void s_s3_tester_wait_for_clean_up_signal(struct aws_s3_tester *tester);

/* Notify the tester that a particular clean up step has finished. */
static void s_tester_notify_clean_up_signal(void *user_data);

static bool s_s3_tester_has_received_finish_callback(void *user_data);

static bool s_s3_tester_has_clean_up_finished(void *user_data);

int aws_s3_tester_init(
    struct aws_allocator *allocator,
    struct aws_s3_tester *tester,
    const struct aws_byte_cursor bucket_name,
    const struct aws_byte_cursor region) {

    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(tester);

    (void)allocator;

    AWS_ZERO_STRUCT(*tester);

    tester->allocator = allocator;

    aws_s3_library_init(allocator);

    if (aws_mutex_init(&tester->lock)) {
        return AWS_OP_ERR;
    }

    if (aws_condition_variable_init(&tester->signal)) {
        goto condition_variable_failed;
    }

    /* Make a copy of the bucket name string. */
    tester->bucket_name = aws_string_new_from_array(allocator, bucket_name.ptr, bucket_name.len);

    if (tester->bucket_name == NULL) {
        goto bucket_name_failed;
    }

    /* Make a copy of the region string. */
    tester->region = aws_string_new_from_array(allocator, region.ptr, region.len);

    if (tester->region == NULL) {
        goto region_name_failed;
    }

    /* Compute an S3 endpoint given a bucket name and region. */
    {
        struct aws_byte_cursor endpoint_url_part0 = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL(".s3.");
        struct aws_byte_cursor endpoint_url_part1 = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL(".amazonaws.com");
        size_t endpoint_buffer_len =
            (tester->bucket_name->len) + endpoint_url_part0.len + (tester->region->len) + endpoint_url_part1.len + 1;
        char *endpoint_buffer = aws_mem_acquire(allocator, endpoint_buffer_len);

        if (endpoint_buffer == NULL) {
            return AWS_OP_ERR;
        }

        endpoint_buffer[0] = '\0';

        strncat(endpoint_buffer, aws_string_c_str(tester->bucket_name), tester->bucket_name->len);
        strncat(endpoint_buffer, (const char *)endpoint_url_part0.ptr, endpoint_url_part0.len);
        strncat(endpoint_buffer, aws_string_c_str(tester->region), tester->region->len);
        strncat(endpoint_buffer, (const char *)endpoint_url_part1.ptr, endpoint_url_part1.len);

        tester->endpoint = aws_string_new_from_c_str(allocator, endpoint_buffer);

        if (tester->endpoint == NULL) {
            goto endpoint_setup_failed;
        }

        aws_mem_release(allocator, endpoint_buffer);
    }

    /* Setup an event loop group and host resolver. */
    tester->el_group = aws_event_loop_group_new_default(allocator, 0, NULL);
    ASSERT_TRUE(tester->el_group != NULL);

    tester->host_resolver = aws_host_resolver_new_default(allocator, 10, tester->el_group, NULL);
    ASSERT_TRUE(tester->host_resolver != NULL);

    /* Setup the client boot strap. */
    {
        struct aws_client_bootstrap_options bootstrap_options;
        AWS_ZERO_STRUCT(bootstrap_options);
        bootstrap_options.event_loop_group = tester->el_group;
        bootstrap_options.host_resolver = tester->host_resolver;
        bootstrap_options.user_data = tester;

        tester->client_bootstrap = aws_client_bootstrap_new(allocator, &bootstrap_options);

        if (tester->client_bootstrap == NULL) {
            goto client_bootstrap_alloc_failed;
        }
    }

    /* Setup the credentials provider */
    {
        struct aws_credentials_provider_chain_default_options credentials_config;
        AWS_ZERO_STRUCT(credentials_config);
        credentials_config.bootstrap = tester->client_bootstrap;
        tester->credentials_provider = aws_credentials_provider_new_chain_default(allocator, &credentials_config);

        if (tester->credentials_provider == NULL) {
            goto credentials_provider_alloc_failed;
        }
    }

    return AWS_OP_SUCCESS;

credentials_provider_alloc_failed:

    if (tester->client_bootstrap != NULL) {
        aws_client_bootstrap_release(tester->client_bootstrap);
        tester->client_bootstrap = NULL;
    }

client_bootstrap_alloc_failed:

    if (tester->endpoint != NULL) {
        aws_string_destroy(tester->endpoint);
        tester->endpoint = NULL;
    }

endpoint_setup_failed:

    if (tester->region != NULL) {
        aws_string_destroy(tester->region);
        tester->region = NULL;
    }

region_name_failed:

    if (tester->bucket_name != NULL) {
        aws_string_destroy(tester->bucket_name);
        tester->bucket_name = NULL;
    }

bucket_name_failed:

    aws_condition_variable_clean_up(&tester->signal);

condition_variable_failed:

    aws_mutex_clean_up(&tester->lock);

    return AWS_OP_ERR;
}

void aws_s3_tester_wait_for_finish(struct aws_s3_tester *tester) {
    AWS_PRECONDITION(tester);
    aws_mutex_lock(&tester->lock);
    aws_condition_variable_wait_pred(&tester->signal, &tester->lock, s_s3_tester_has_received_finish_callback, tester);
    aws_mutex_unlock(&tester->lock);
}

void aws_s3_tester_notify_finished(struct aws_s3_tester *tester, int error_code) {
    AWS_PRECONDITION(tester);

    aws_mutex_lock(&tester->lock);
    tester->received_finish_callback = true;
    tester->finish_error_code = error_code;
    aws_mutex_unlock(&tester->lock);

    aws_condition_variable_notify_one(&tester->signal);
}

void aws_s3_tester_clean_up(struct aws_s3_tester *tester) {
    AWS_PRECONDITION(tester);

    if (tester->bound_to_client_shutdown) {
        s_s3_tester_wait_for_clean_up_signal(tester);
        tester->bound_to_client_shutdown = false;
    }

    if (tester->client_bootstrap != NULL) {
        aws_client_bootstrap_release(tester->client_bootstrap);
        tester->client_bootstrap = NULL;
    }

    if (tester->credentials_provider != NULL) {
        aws_credentials_provider_release(tester->credentials_provider);
        tester->credentials_provider = NULL;
    }

    if (tester->region != NULL) {
        aws_string_destroy(tester->region);
        tester->region = NULL;
    }

    if (tester->bucket_name != NULL) {
        aws_string_destroy(tester->bucket_name);
        tester->bucket_name = NULL;
    }

    if (tester->endpoint != NULL) {
        aws_string_destroy(tester->endpoint);
        tester->endpoint = NULL;
    }

    if (tester->host_resolver != NULL) {
        aws_host_resolver_release(tester->host_resolver);
        tester->host_resolver = NULL;
    }

    if (tester->el_group != NULL) {
        aws_event_loop_group_release(tester->el_group);
        tester->el_group = NULL;
    }

    aws_condition_variable_clean_up(&tester->signal);
    aws_mutex_clean_up(&tester->lock);

    aws_s3_library_clean_up();

    aws_global_thread_creator_shutdown_wait_for(10);
}

void aws_s3_create_test_buffer(struct aws_allocator *allocator, size_t buffer_size, struct aws_byte_buf *out_buf) {
    struct aws_byte_cursor test_string = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("This is an S3 test.");

    aws_byte_buf_init(out_buf, allocator, buffer_size);

    for (size_t buffer_pos = 0; buffer_pos < buffer_size; buffer_pos += test_string.len) {
        size_t buffer_size_remaining = buffer_size - buffer_pos;
        size_t string_copy_size = test_string.len;

        if (buffer_size_remaining < string_copy_size) {
            string_copy_size = buffer_size_remaining;
        }

        struct aws_byte_cursor from_byte_cursor = {.len = string_copy_size, .ptr = test_string.ptr};

        aws_byte_buf_append(out_buf, from_byte_cursor);
    }
}

void aws_s3_tester_bind_client_shutdown(struct aws_s3_tester *tester, struct aws_s3_client_config *config) {
    AWS_PRECONDITION(tester);
    AWS_PRECONDITION(config);

    AWS_FATAL_ASSERT(!tester->bound_to_client_shutdown && "Only one client supported for binding to shutdown");

    config->shutdown_callback = s_tester_notify_clean_up_signal;
    config->shutdown_callback_user_data = tester;
    tester->bound_to_client_shutdown = true;
}

static void s_tester_notify_clean_up_signal(void *user_data) {
    AWS_PRECONDITION(user_data);

    struct aws_s3_tester *tester = (struct aws_s3_tester *)user_data;

    aws_mutex_lock(&tester->lock);
    tester->clean_up_flag = true;
    aws_mutex_unlock(&tester->lock);

    aws_condition_variable_notify_one(&tester->signal);
}

static bool s_s3_tester_has_received_finish_callback(void *user_data) {
    AWS_PRECONDITION(user_data);
    struct aws_s3_tester *tester = (struct aws_s3_tester *)user_data;
    return tester->received_finish_callback;
}

static bool s_s3_tester_has_clean_up_finished(void *user_data) {
    AWS_PRECONDITION(user_data);
    struct aws_s3_tester *tester = (struct aws_s3_tester *)user_data;
    return tester->clean_up_flag;
}

static void s_s3_tester_wait_for_clean_up_signal(struct aws_s3_tester *tester) {
    AWS_PRECONDITION(tester);

    aws_mutex_lock(&tester->lock);
    aws_condition_variable_wait_pred(&tester->signal, &tester->lock, s_s3_tester_has_clean_up_finished, tester);

    /* Reset the clean up flag for any additional clean up steps */
    tester->clean_up_flag = false;

    aws_mutex_unlock(&tester->lock);
}
