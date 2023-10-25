/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */
#include <aws/common/hash_table.h>
#include <aws/s3/s3_platform_info.h>

#include <aws/auth/aws_imds_client.h>
#include <aws/common/clock.h>
#include <aws/common/condition_variable.h>
#include <aws/common/mutex.h>
#include <aws/common/system_info.h>
#include <aws/io/channel_bootstrap.h>
#include <aws/io/event_loop.h>
#include <aws/io/host_resolver.h>

struct aws_s3_compute_platform_info_loader {
    struct aws_allocator *allocator;
    struct aws_ref_count ref_count;
    struct {
        struct aws_string *detected_instance_type;
        struct aws_mutex lock;
    } lock_data;
    struct aws_system_environment *current_env;
};

static void s_destroy_loader(void *arg) {
    struct aws_s3_compute_platform_info_loader *loader = arg;

    aws_mutex_clean_up(&loader->lock_data.lock);

    if (loader->lock_data.detected_instance_type) {
        aws_string_destroy(loader->lock_data.detected_instance_type);
    }

    aws_system_environment_release(loader->current_env);
    aws_mem_release(loader->allocator, loader);
}

struct aws_s3_compute_platform_info_loader *aws_s3_compute_platform_info_loader_new(struct aws_allocator *allocator) {
    struct aws_s3_compute_platform_info_loader *loader =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_compute_platform_info_loader));

    loader->allocator = allocator;
    loader->current_env = aws_system_environment_load(allocator);
    AWS_FATAL_ASSERT(loader->current_env && "Failed to load system environment");
    aws_mutex_init(&loader->lock_data.lock);
    aws_ref_count_init(&loader->ref_count, loader, s_destroy_loader);

    return loader;
}

struct aws_s3_compute_platform_info_loader *aws_s3_compute_platform_info_loader_acquire(
    struct aws_s3_compute_platform_info_loader *loader) {
    aws_ref_count_acquire(&loader->ref_count);
    return loader;
}

struct aws_s3_compute_platform_info_loader *aws_s3_compute_platform_info_loader_release(
    struct aws_s3_compute_platform_info_loader *loader) {
    if (loader) {
        aws_ref_count_release(&loader->ref_count);
    }
    return NULL;
}

struct imds_callback_info {
    struct aws_allocator *allocator;
    struct aws_string *instance_type;
    struct aws_condition_variable c_var;
    int error_code;
    struct aws_s3_compute_platform_info_loader *loader;
    bool shutdown_completed;
    struct aws_mutex mutex;
};

static void s_imds_client_shutdown_completed(void *user_data) {
    struct imds_callback_info *info = user_data;
    aws_mutex_lock(&info->mutex);
    info->shutdown_completed = true;
    aws_mutex_unlock(&info->mutex);
    aws_condition_variable_notify_all(&info->c_var);
}

static bool s_client_shutdown_predicate(void *arg) {
    struct imds_callback_info *info = arg;
    return info->shutdown_completed;
}

static void s_imds_client_on_get_instance_info_callback(
    const struct aws_imds_instance_info *instance_info,
    int error_code,
    void *user_data) {
    struct imds_callback_info *info = user_data;

    aws_mutex_lock(&info->mutex);
    if (error_code) {
        info->error_code = error_code;
    } else {
        info->instance_type = aws_string_new_from_cursor(info->allocator, &instance_info->instance_type);
    }
    aws_mutex_unlock(&info->mutex);
    aws_condition_variable_notify_all(&info->c_var);
}

static bool s_completion_predicate(void *arg) {
    struct imds_callback_info *info = arg;
    return info->error_code != 0 || info->instance_type != NULL;
}

struct aws_byte_cursor aws_s3_get_ec2_instance_type(struct aws_s3_compute_platform_info_loader *loader) {
    aws_mutex_lock(&loader->lock_data.lock);
    if (loader->lock_data.detected_instance_type) {
        AWS_LOGF_TRACE(
            AWS_LS_S3_CLIENT,
            "id=%p: Instance type has already been determined to be %s. Returning cached version.",
            (void *)loader,
            aws_string_bytes(loader->lock_data.detected_instance_type));
        goto return_instance_and_unlock;
    }

    AWS_LOGF_TRACE(
        AWS_LS_S3_CLIENT,
        "id=%p: Instance type has not been determined, checking to see if running in EC2 nitro environment.",
        (void *)loader);

    if (aws_s3_is_running_on_ec2_nitro(loader)) {
        AWS_LOGF_INFO(
            AWS_LS_S3_CLIENT, "id=%p: Detected Amazon EC2 with nitro as the current environment.", (void *)loader);
        /* easy case not requiring any calls out to IMDS. If we detected we're running on ec2, then the dmi info is
         * correct, and we can use it if we have it. Otherwise call out to IMDS. */
        struct aws_byte_cursor product_name =
            aws_system_environment_get_virtualization_product_name(loader->current_env);

        if (product_name.len) {
            loader->lock_data.detected_instance_type = aws_string_new_from_cursor(loader->allocator, &product_name);

            AWS_LOGF_INFO(
                AWS_LS_S3_CLIENT,
                "id=%p: Determined instance type to be %s, from dmi info. Caching.",
                (void *)loader,
                aws_string_bytes(loader->lock_data.detected_instance_type));
            goto return_instance_and_unlock;
        }

        AWS_LOGF_DEBUG(
            AWS_LS_S3_CLIENT,
            "static: DMI info was insufficient to determine instance type. Making call to IMDS to determine");
        struct imds_callback_info callback_info = {
            .mutex = AWS_MUTEX_INIT,
            .c_var = AWS_CONDITION_VARIABLE_INIT,
            .allocator = loader->allocator,
            .loader = loader,
        };

        struct aws_event_loop_group *el_group = NULL;
        struct aws_host_resolver *resolver = NULL;
        struct aws_client_bootstrap *client_bootstrap = NULL;
        /* now call IMDS */
        el_group = aws_event_loop_group_new_default(loader->allocator, 1, NULL);

        if (!el_group) {
            goto tear_down;
        }

        struct aws_host_resolver_default_options resolver_options = {
            .max_entries = 1,
            .el_group = el_group,
        };

        resolver = aws_host_resolver_new_default(loader->allocator, &resolver_options);

        if (!resolver) {
            goto tear_down;
        }

        struct aws_client_bootstrap_options bootstrap_options = {
            .event_loop_group = el_group,
            .host_resolver = resolver,
        };

        client_bootstrap = aws_client_bootstrap_new(loader->allocator, &bootstrap_options);

        if (!client_bootstrap) {
            goto tear_down;
        }

        struct aws_imds_client_shutdown_options imds_shutdown_options = {
            .shutdown_callback = s_imds_client_shutdown_completed,
            .shutdown_user_data = &callback_info,
        };

        struct aws_imds_client_options imds_options = {
            .bootstrap = client_bootstrap,
            .imds_version = IMDS_PROTOCOL_V2,
            .shutdown_options = imds_shutdown_options,
        };

        struct aws_imds_client *imds_client = aws_imds_client_new(loader->allocator, &imds_options);

        if (!imds_client) {
            goto tear_down;
        }

        aws_mutex_lock(&callback_info.mutex);
        aws_imds_client_get_instance_info(imds_client, s_imds_client_on_get_instance_info_callback, &callback_info);
        aws_condition_variable_wait_for_pred(
            &callback_info.c_var, &callback_info.mutex, AWS_TIMESTAMP_SECS, s_completion_predicate, &callback_info);

        aws_condition_variable_wait_pred(
            &callback_info.c_var, &callback_info.mutex, s_client_shutdown_predicate, &callback_info);
        aws_mutex_unlock(&callback_info.mutex);
        aws_imds_client_release(imds_client);

        if (callback_info.error_code) {
            aws_raise_error(callback_info.error_code);
            AWS_LOGF_ERROR(
                AWS_LS_S3_CLIENT,
                "id=%p: IMDS call failed with error %s.",
                (void *)loader,
                aws_error_debug_str(callback_info.error_code));
        }

        if (callback_info.instance_type) {
            loader->lock_data.detected_instance_type = callback_info.instance_type;
            AWS_LOGF_INFO(
                AWS_LS_S3_CLIENT,
                "id=%p: Determined instance type to be %s, from IMDS. Caching.",
                (void *)loader,
                aws_string_bytes(loader->lock_data.detected_instance_type));
        }

    tear_down:
        if (client_bootstrap) {
            aws_client_bootstrap_release(client_bootstrap);
        }

        if (resolver) {
            aws_host_resolver_release(resolver);
        }

        if (el_group) {
            aws_event_loop_group_release(el_group);
        }
    }

    struct aws_byte_cursor return_cur;
    AWS_ZERO_STRUCT(return_cur);

return_instance_and_unlock:
    return_cur = aws_byte_cursor_from_string(loader->lock_data.detected_instance_type);
    aws_mutex_unlock(&loader->lock_data.lock);

    return return_cur;
}

bool aws_s3_is_running_on_ec2_nitro(struct aws_s3_compute_platform_info_loader *loader) {
    struct aws_byte_cursor system_virt_name = aws_system_environment_get_virtualization_vendor(loader->current_env);

    if (aws_byte_cursor_eq_c_str_ignore_case(&system_virt_name, "amazon ec2")) {
        return true;
    }

    return false;
}
