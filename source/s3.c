/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/s3/s3.h>

#include <aws/auth/auth.h>
#include <aws/auth/aws_imds_client.h>
#include <aws/common/clock.h>
#include <aws/common/condition_variable.h>
#include <aws/common/error.h>
#include <aws/common/hash_table.h>
#include <aws/common/mutex.h>
#include <aws/common/system_info.h>
#include <aws/http/http.h>
#include <aws/io/channel_bootstrap.h>
#include <aws/io/event_loop.h>

#define AWS_DEFINE_ERROR_INFO_S3(CODE, STR) AWS_DEFINE_ERROR_INFO(CODE, STR, "aws-c-s3")

/* clang-format off */
static struct aws_error_info s_errors[] = {
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_MISSING_CONTENT_RANGE_HEADER, "Response missing required Content-Range header."),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_INVALID_CONTENT_RANGE_HEADER, "Response contains invalid Content-Range header."),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_MISSING_CONTENT_LENGTH_HEADER, "Response missing required Content-Length header."),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_INVALID_CONTENT_LENGTH_HEADER, "Response contains invalid Content-Length header."),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_MISSING_ETAG, "Response missing required ETag header."),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_INTERNAL_ERROR, "Response code indicates internal server error"),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_SLOW_DOWN, "Response code indicates throttling"),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_INVALID_RESPONSE_STATUS, "Invalid response status from request"),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_MISSING_UPLOAD_ID, "Upload Id not found in create-multipart-upload response"),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_PROXY_PARSE_FAILED, "Could not parse proxy URI"),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_UNSUPPORTED_PROXY_SCHEME, "Given Proxy URI has an unsupported scheme"),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_CANCELED, "Request successfully cancelled"),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_INVALID_RANGE_HEADER, "Range header has invalid syntax"),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_MULTIRANGE_HEADER_UNSUPPORTED, "Range header specifies multiple ranges which is unsupported"),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_RESPONSE_CHECKSUM_MISMATCH, "response checksum header does not match calculated checksum"),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_CHECKSUM_CALCULATION_FAILED, "failed to calculate a checksum for the provided stream"),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_PAUSED, "Request successfully paused"),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_LIST_PARTS_PARSE_FAILED, "Failed to parse response from ListParts"),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_RESUMED_PART_CHECKSUM_MISMATCH, "Checksum does not match previously uploaded part"),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_RESUME_FAILED, "Resuming request failed"),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_OBJECT_MODIFIED, "The object modifed during download."),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_NON_RECOVERABLE_ASYNC_ERROR, "Async error received from S3 and not recoverable from retry."),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_METRIC_DATA_NOT_AVAILABLE, "The metric data is not available, the requests ends before the metric happens."),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_INCORRECT_CONTENT_LENGTH, "Request body length must match Content-Length header."),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_REQUEST_TIME_TOO_SKEWED, "RequestTimeTooSkewed error received from S3."),
};
/* clang-format on */

static struct aws_error_info_list s_error_list = {
    .error_list = s_errors,
    .count = AWS_ARRAY_SIZE(s_errors),
};

static struct aws_log_subject_info s_s3_log_subject_infos[] = {
    DEFINE_LOG_SUBJECT_INFO(AWS_LS_S3_GENERAL, "S3General", "Subject for aws-c-s3 logging that defies categorization."),
    DEFINE_LOG_SUBJECT_INFO(AWS_LS_S3_CLIENT, "S3Client", "Subject for aws-c-s3 logging from an aws_s3_client."),
    DEFINE_LOG_SUBJECT_INFO(
        AWS_LS_S3_CLIENT_STATS,
        "S3ClientStats",
        "Subject for aws-c-s3 logging for stats tracked by an aws_s3_client."),
    DEFINE_LOG_SUBJECT_INFO(AWS_LS_S3_REQUEST, "S3Request", "Subject for aws-c-s3 logging from an aws_s3_request."),
    DEFINE_LOG_SUBJECT_INFO(
        AWS_LS_S3_META_REQUEST,
        "S3MetaRequest",
        "Subject for aws-c-s3 logging from an aws_s3_meta_request."),
    DEFINE_LOG_SUBJECT_INFO(AWS_LS_S3_ENDPOINT, "S3Endpoint", "Subject for aws-c-s3 logging from an aws_s3_endpoint."),
};

static struct aws_log_subject_info_list s_s3_log_subject_list = {
    .subject_list = s_s3_log_subject_infos,
    .count = AWS_ARRAY_SIZE(s_s3_log_subject_infos),
};

/**** Configuration info for the c5n.18xlarge *****/
static struct aws_byte_cursor s_c5n_18xlarge_nic_array[] = {AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("eth0")};

static struct aws_s3_cpu_group_info s_c5n_18xlarge_cpu_group_info_array[] = {
    {
        .cpu_group = 0u,
        .nic_name_array = s_c5n_18xlarge_nic_array,
        .nic_name_array_length = AWS_ARRAY_SIZE(s_c5n_18xlarge_nic_array),
    },
    {
        .cpu_group = 1u,
        .nic_name_array = NULL,
        .nic_name_array_length = 0u,
    },
};

static struct aws_s3_compute_platform_info s_c5n_18xlarge_platform_info = {
    .instance_type = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("c5n.18xlarge"),
    .max_throughput_gbps = 100u,
    .cpu_group_info_array = s_c5n_18xlarge_cpu_group_info_array,
    .cpu_group_info_array_length = AWS_ARRAY_SIZE(s_c5n_18xlarge_cpu_group_info_array),
};
/****** End c5n.18xlarge *****/

static struct aws_hash_table s_compute_platform_info_table;

static bool s_library_initialized = false;
static struct aws_allocator *s_library_allocator = NULL;

void aws_s3_library_init(struct aws_allocator *allocator) {
    if (s_library_initialized) {
        return;
    }

    if (allocator) {
        s_library_allocator = allocator;
    } else {
        s_library_allocator = aws_default_allocator();
    }

    aws_auth_library_init(s_library_allocator);
    aws_http_library_init(s_library_allocator);

    aws_register_error_info(&s_error_list);
    aws_register_log_subject_info_list(&s_s3_log_subject_list);

    AWS_FATAL_ASSERT(
        !aws_hash_table_init(
            &s_compute_platform_info_table,
            allocator,
            32,
            aws_hash_byte_cursor_ptr_ignore_case,
            (bool (*)(const void *, const void *))aws_byte_cursor_eq_ignore_case,
            NULL,
            NULL) &&
        "Hash table init failed!");

    AWS_FATAL_ASSERT(
        !aws_hash_table_put(
            &s_compute_platform_info_table,
            &s_c5n_18xlarge_platform_info.instance_type,
            &s_c5n_18xlarge_platform_info,
            NULL) &&
        "hash table put failed!");

    s_library_initialized = true;
}

void aws_s3_library_clean_up(void) {
    if (!s_library_initialized) {
        return;
    }

    s_library_initialized = false;
    aws_thread_join_all_managed();

    aws_hash_table_clean_up(&s_compute_platform_info_table);
    aws_unregister_log_subject_info_list(&s_s3_log_subject_list);
    aws_unregister_error_info(&s_error_list);
    aws_http_library_clean_up();
    aws_auth_library_clean_up();
    s_library_allocator = NULL;
}

struct aws_s3_compute_platform_info *aws_s3_get_compute_platform_info_for_instance_type(
    const struct aws_byte_cursor instance_type_name) {
    AWS_LOGF_TRACE(
        AWS_LS_S3_GENERAL,
        "static: looking up compute platform info for instance type " PRInSTR,
        AWS_BYTE_CURSOR_PRI(instance_type_name));

    struct aws_hash_element *platform_info_element = NULL;
    aws_hash_table_find(&s_compute_platform_info_table, &instance_type_name, &platform_info_element);

    if (platform_info_element) {
        AWS_LOGF_INFO(
            AWS_LS_S3_GENERAL,
            "static: found compute platform info for instance type " PRInSTR,
            AWS_BYTE_CURSOR_PRI(instance_type_name));
        return platform_info_element->value;
    }

    AWS_LOGF_INFO(
        AWS_LS_S3_GENERAL,
        "static: compute platform info for instance type " PRInSTR " not found",
        AWS_BYTE_CURSOR_PRI(instance_type_name));
    return NULL;
}

static struct aws_byte_cursor s_instance_type_allow_list[] = {
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("p4d"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("p5"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("trn1")};

bool aws_s3_is_optimized_for_system_env(
    const struct aws_system_environment *env,
    const struct aws_byte_cursor *discovered_instance_type) {
    struct aws_byte_cursor product_name = aws_system_environment_get_virtualization_product_name(env);

    const struct aws_byte_cursor *to_compare = &product_name;

    if (discovered_instance_type && discovered_instance_type->len) {
        to_compare = discovered_instance_type;
    }

    for (size_t i = 0; i < AWS_ARRAY_SIZE(s_instance_type_allow_list); ++i) {
        if (aws_byte_cursor_starts_with_ignore_case(to_compare, &s_instance_type_allow_list[i])) {
            return true;
        }
    }

    return false;
}

struct imds_callback_info {
    struct aws_allocator *allocator;
    struct aws_string *instance_type;
    struct aws_condition_variable c_var;
    int error_code;
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

struct aws_string *aws_s3_get_ec2_instance_type(
    struct aws_allocator *allocator,
    const struct aws_system_environment *env) {
    if (aws_s3_is_running_on_ec2(env)) {
        /* easy case not requiring any calls out to IMDS. If we detected we're running on ec2, then the dmi info is
         * correct, and we can use it if we have it. Otherwise call out to IMDS. */
        struct aws_byte_cursor product_name = aws_system_environment_get_virtualization_product_name(env);

        if (product_name.len) {
            return aws_string_new_from_cursor(allocator, &product_name);
        }

        struct imds_callback_info callback_info = {
            .mutex = AWS_MUTEX_INIT,
            .c_var = AWS_CONDITION_VARIABLE_INIT,
            .allocator = allocator,
        };

        struct aws_event_loop_group *el_group = NULL;
        struct aws_host_resolver *resolver = NULL;
        struct aws_client_bootstrap *client_bootstrap = NULL;
        /* now call IMDS */
        el_group = aws_event_loop_group_new_default(allocator, 1, NULL);

        if (!el_group) {
            goto tear_down;
        }

        struct aws_host_resolver_default_options resolver_options = {
            .max_entries = 1,
            .el_group = el_group,
        };

        resolver = aws_host_resolver_new_default(allocator, &resolver_options);

        if (!resolver) {
            goto tear_down;
        }

        struct aws_client_bootstrap_options bootstrap_options = {
            .event_loop_group = el_group,
            .host_resolver = resolver,
        };

        client_bootstrap = aws_client_bootstrap_new(allocator, &bootstrap_options);

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

        struct aws_imds_client *imds_client = aws_imds_client_new(allocator, &imds_options);

        if (!imds_client) {
            goto tear_down;
        }

        aws_mutex_lock(&callback_info.mutex);
        aws_imds_client_get_instance_info(imds_client, s_imds_client_on_get_instance_info_callback, &callback_info);
        aws_condition_variable_wait_for_pred(
            &callback_info.c_var, &callback_info.mutex, AWS_TIMESTAMP_SECS, s_completion_predicate, &callback_info);

        aws_condition_variable_wait_pred(
            &callback_info.c_var, &callback_info.mutex, s_client_shutdown_predicate, &callback_info);

        aws_imds_client_release(imds_client);

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

        if (callback_info.instance_type) {
            return callback_info.instance_type;
        }

        if (callback_info.error_code) {
            aws_raise_error(callback_info.error_code);
        }
    }
    return NULL;
}

bool aws_s3_is_running_on_ec2(const struct aws_system_environment *env) {
    struct aws_byte_cursor system_virt_name = aws_system_environment_get_virtualization_vendor(env);

    if (aws_byte_cursor_eq_c_str_ignore_case(&system_virt_name, "amazon ec2")) {
        return true;
    }

    return false;
}
