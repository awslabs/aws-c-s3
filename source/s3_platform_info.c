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

#include <inttypes.h>

/**** Configuration info for the c5n.18xlarge *****/
static struct aws_byte_cursor s_c5n_nic_array[] = {AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("eth0")};

static struct aws_s3_cpu_group_info s_c5n_18xlarge_cpu_group_info_array[] = {
    {
        .cpu_group = 0u,
        .nic_name_array = s_c5n_nic_array,
        .nic_name_array_length = AWS_ARRAY_SIZE(s_c5n_nic_array),
        .cpus_in_group = 36,
    },
    {
        .cpu_group = 1u,
        .nic_name_array = NULL,
        .nic_name_array_length = 0u,
        .cpus_in_group = 36,
    },
};

static struct aws_s3_compute_platform_info s_c5n_18xlarge_platform_info = {
    .instance_type = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("c5n.18xlarge"),
    .max_throughput_gbps = 100u,
    .cpu_group_info_array = s_c5n_18xlarge_cpu_group_info_array,
    .cpu_group_info_array_length = AWS_ARRAY_SIZE(s_c5n_18xlarge_cpu_group_info_array),
    /** not yet **/
    .has_recommended_configuration = false,
};

static struct aws_s3_compute_platform_info s_c5n_metal_platform_info = {
    .instance_type = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("c5n.metal"),
    .max_throughput_gbps = 100u,
    .cpu_group_info_array = s_c5n_18xlarge_cpu_group_info_array,
    .cpu_group_info_array_length = AWS_ARRAY_SIZE(s_c5n_18xlarge_cpu_group_info_array),
    /** not yet **/
    .has_recommended_configuration = false,
};

/****** End c5n.18xlarge *****/

/****** Begin c5n.large ******/
static struct aws_s3_cpu_group_info s_c5n_9xlarge_cpu_group_info_array[] = {
    {
        .cpu_group = 0u,
        .nic_name_array = s_c5n_nic_array,
        .nic_name_array_length = AWS_ARRAY_SIZE(s_c5n_nic_array),
        .cpus_in_group = 36,
    },
};

static struct aws_s3_compute_platform_info s_c5n_9xlarge_platform_info = {
    .instance_type = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("c5n.9xlarge"),
    .max_throughput_gbps = 50u,
    .cpu_group_info_array = s_c5n_9xlarge_cpu_group_info_array,
    .cpu_group_info_array_length = AWS_ARRAY_SIZE(s_c5n_9xlarge_cpu_group_info_array),
    /** not yet **/
    .has_recommended_configuration = false,
};

/****** End c5n.9large *****/

/***** Begin p4d.24xlarge and p4de.24xlarge ****/
static struct aws_byte_cursor s_p4d_socket1_array[] = {
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("eth0"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("eth1")};

static struct aws_byte_cursor s_p4d_socket2_array[] = {
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("eth2"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("eth3")};

static struct aws_s3_cpu_group_info s_p4d_cpu_group_info_array[] = {
    {
        .cpu_group = 0u,
        .nic_name_array = s_p4d_socket1_array,
        .nic_name_array_length = AWS_ARRAY_SIZE(s_p4d_socket1_array),
        .cpus_in_group = 48,
    },
    {
        .cpu_group = 1u,
        .nic_name_array = s_p4d_socket2_array,
        .nic_name_array_length = AWS_ARRAY_SIZE(s_p4d_socket1_array),
        .cpus_in_group = 48,
    },
};

static struct aws_s3_compute_platform_info s_p4d_platform_info = {
    .instance_type = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("p4d.24xlarge"),
    .max_throughput_gbps = 400u,
    .cpu_group_info_array = s_p4d_cpu_group_info_array,
    .cpu_group_info_array_length = AWS_ARRAY_SIZE(s_p4d_cpu_group_info_array),
    .has_recommended_configuration = true,
};

static struct aws_s3_compute_platform_info s_p4de_platform_info = {
    .instance_type = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("p4de.24xlarge"),
    .max_throughput_gbps = 400u,
    .cpu_group_info_array = s_p4d_cpu_group_info_array,
    .cpu_group_info_array_length = AWS_ARRAY_SIZE(s_p4d_cpu_group_info_array),
    .has_recommended_configuration = true,
};

/***** End p4d.24xlarge and p4de.24xlarge ****/

/***** Begin p5.48xlarge ******/

/* note: the p5 is a stunningly massive instance type.
 * While the specs have 3.2 TB/s for the network bandwidth
 * not all of that is accessible from the CPU. From the CPU we'll
 * be able to get around 400 Gbps. Also note, 3.2 TB/s
 * with 2 sockets on a nitro instance inplies 16 NICs
 * per node. However, practically, due to the topology of this instance
 * as far as this client is concerned, there are two NICs per node, similar
 * to the p4d. The rest is for other things on the machine to use. */

struct aws_byte_cursor s_p5_socket1_array[] = {
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("eth0"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("eth1"),
};

static struct aws_byte_cursor s_p5_socket2_array[] = {
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("eth2"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("eth3"),
};

static struct aws_s3_cpu_group_info s_p5_cpu_group_info_array[] = {
    {
        .cpu_group = 0u,
        .nic_name_array = s_p5_socket1_array,
        .nic_name_array_length = AWS_ARRAY_SIZE(s_p5_socket1_array),
        .cpus_in_group = 96,
    },
    {
        .cpu_group = 1u,
        .nic_name_array = s_p5_socket2_array,
        .nic_name_array_length = AWS_ARRAY_SIZE(s_p5_socket2_array),
        .cpus_in_group = 96,
    },
};

struct aws_s3_compute_platform_info s_p5_platform_info = {
    .instance_type = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("p5.48xlarge"),
    .max_throughput_gbps = 400u,
    .cpu_group_info_array = s_p5_cpu_group_info_array,
    .cpu_group_info_array_length = AWS_ARRAY_SIZE(s_p5_cpu_group_info_array),
    .has_recommended_configuration = true,
};

/***** End p5.48xlarge *****/

/**** Begin trn1_32_large *****/
struct aws_byte_cursor s_trn1_n_socket1_array[] = {
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("ens32"),
};

static struct aws_s3_cpu_group_info s_trn1_n_cpu_group_info_array[] = {
    {
        .cpu_group = 0u,
        .nic_name_array = s_trn1_n_socket1_array,
        .nic_name_array_length = AWS_ARRAY_SIZE(s_trn1_n_socket1_array),
        .cpus_in_group = 64,
    },
    {
        .cpu_group = 1u,
        .nic_name_array = NULL,
        .nic_name_array_length = 0,
        .cpus_in_group = 64,
    },
};

static struct aws_s3_compute_platform_info s_trn1_n_platform_info = {
    .instance_type = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("trn1n.32xlarge"),
    /* not all of the advertised 1600 Gbps bandwidth can be hit from the cpu in user-space */
    .max_throughput_gbps = 800,
    .cpu_group_info_array = s_trn1_n_cpu_group_info_array,
    .cpu_group_info_array_length = AWS_ARRAY_SIZE(s_trn1_n_cpu_group_info_array),
    .has_recommended_configuration = true,
};

struct aws_byte_cursor s_trn1_socket1_array[] = {
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("ens32"),
};

static struct aws_s3_cpu_group_info s_trn1_cpu_group_info_array[] = {
    {
        .cpu_group = 0u,
        .nic_name_array = s_trn1_socket1_array,
        .nic_name_array_length = AWS_ARRAY_SIZE(s_trn1_socket1_array),
        .cpus_in_group = 64,
    },
    {
        .cpu_group = 1u,
        .nic_name_array = NULL,
        .nic_name_array_length = 0,
        .cpus_in_group = 64,
    },
};

static struct aws_s3_compute_platform_info s_trn1_platform_info = {
    .instance_type = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("trn1.32xlarge"),
    /* not all of the advertised 800 Gbps bandwidth can be hit from the cpu in user-space */
    .max_throughput_gbps = 600,
    .cpu_group_info_array = s_trn1_cpu_group_info_array,
    .cpu_group_info_array_length = AWS_ARRAY_SIZE(s_trn1_cpu_group_info_array),
    .has_recommended_configuration = true,
};

/**** End trn1.x32_large ******/

struct aws_s3_compute_platform_info_loader {
    struct aws_allocator *allocator;
    struct aws_ref_count ref_count;
    struct {
        struct aws_string *detected_instance_type;
        struct aws_s3_compute_platform_info current_env_platform_info;
        struct aws_hash_table compute_platform_info_table;
        struct aws_mutex lock;
    } lock_data;
    struct aws_system_environment *current_env;
};

void s_add_platform_info_to_table(
    struct aws_s3_compute_platform_info_loader *loader,
    struct aws_s3_compute_platform_info *info) {
    AWS_PRECONDITION(info->instance_type.len > 0);
    AWS_LOGF_TRACE(
        AWS_LS_S3_GENERAL,
        "id=%p: adding platform entry for \"" PRInSTR "\".",
        (void *)loader,
        AWS_BYTE_CURSOR_PRI(info->instance_type));

    struct aws_hash_element *platform_info_element = NULL;
    aws_hash_table_find(&loader->lock_data.compute_platform_info_table, &info->instance_type, &platform_info_element);
    if (platform_info_element) {
        AWS_LOGF_TRACE(
            AWS_LS_S3_GENERAL,
            "id=%p: existing entry for \"" PRInSTR "\" found, syncing the values.",
            (void *)loader,
            AWS_BYTE_CURSOR_PRI(info->instance_type));

        /* detected runtime NIC data is better than the pre-known config data but we don't always have it,
         * so copy over any better info than we have. Assume if info has NIC data, it was discovered at runtime.
         * The other data should be identical and we don't want to add complications to the memory model.
         * You're guaranteed only one instance of an instance type's info, the initial load is static memory */
        struct aws_s3_compute_platform_info *existing = platform_info_element->value;
        /* only do this if we were able to actually load numa info and it's the same as the pre-computed variants. */
        if (info->cpu_group_info_array_length == existing->cpu_group_info_array_length) {
            for (size_t i = 0; i < existing->cpu_group_info_array_length; ++i) {

                /* if we detected the same amount or more nics on a given node as the existing config, only copy over
                 * the names up to the existing NIC count. This is because the pre-made configs sometimes use less
                 * NICs than the system reports. */
                if (info->cpu_group_info_array[i].nic_name_array_length >= existing->cpu_group_info_array[i].nic_name_array_length) {
                    existing->cpu_group_info_array[i].nic_name_array = info->cpu_group_info_array[i].nic_name_array;
                    /* use the existing NIC count */
                    info->cpu_group_info_array[i].nic_name_array_length =
                        existing->cpu_group_info_array[i].nic_name_array_length;
                /* in this case, ignore what we detected and use the pre-configured config. */
                } else {
                    size_t existing_len = existing->cpu_group_info_array[i].nic_name_array_length;
                    /* info's nic arrays are dynamically allocated, and cleaned up at shutdown, so just reallocate
                     * them and copy. */
                    if (info->cpu_group_info_array[i].nic_name_array == NULL) {
                        info->cpu_group_info_array[i].nic_name_array =
                            aws_mem_calloc(loader->allocator, existing_len, sizeof(struct aws_byte_cursor));
                    } else {
                        aws_mem_release(loader->allocator, info->cpu_group_info_array[i].nic_name_array);
                        info->cpu_group_info_array[i].nic_name_array =
                            aws_mem_calloc(loader->allocator, existing_len, sizeof(struct aws_byte_cursor));
                    }
                    memcpy(info->cpu_group_info_array[i].nic_name_array, existing->cpu_group_info_array[i].nic_name_array, existing_len);
                }
            }
        }
        info->has_recommended_configuration = existing->has_recommended_configuration;
        /* always prefer a pre-known bandwidth, as we estimate low on EC2 by default for safety. */
        info->max_throughput_gbps = existing->max_throughput_gbps;
    } else if (info->max_throughput_gbps == 0) {
            size_t total_cpus = aws_system_environment_get_processor_count(loader->current_env);
            /* go ahead and set a default. */
            info->max_throughput_gbps = 5;

            if (aws_s3_is_running_on_ec2_nitro(loader)) {
                size_t bandwidth_factor = 4;

                /* check if we're on a nitro networking optimized instance. if we are we can use a factor of 25 */
                struct aws_byte_cursor nitro_name_id = aws_byte_cursor_from_c_str("n.");
                struct aws_byte_cursor output_var_not_used;
                AWS_ZERO_STRUCT(output_var_not_used);

                if (aws_byte_cursor_find_exact(
                        &loader->lock_data.current_env_platform_info.instance_type,
                        &nitro_name_id,
                        &output_var_not_used) == AWS_OP_SUCCESS) {
                    bandwidth_factor = 25;
                }

                /* these rules come from EC2's doc pages and we will only do this on EC2. It's pegged to the bottom range
                 * for the moment, so it's a best safe guess for the instance type based purely on CPU info. see:
                 * https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-network-bandwidth.html */

                /* the current rule we're using for in-region, multi-flow traffic is 16 CPUs per 5Gbps.
                 * 16CPU machines are usually "up-to" 10 Gbps, so just let them be 5. */
                if (total_cpus <= 16) {
                    loader->lock_data.current_env_platform_info.max_throughput_gbps = bandwidth_factor;
                } else {
                    size_t cpus_to_use_for_calc = total_cpus;
                    /* crazy things start happening past this point. */
                    if (total_cpus > 96) {
                        cpus_to_use_for_calc = 96;
                    }
                    size_t estimated_bandwidth =
                        (size_t)(((double)cpus_to_use_for_calc / (double)16) * (double)bandwidth_factor);
                    loader->lock_data.current_env_platform_info.max_throughput_gbps = estimated_bandwidth;
                    info->max_throughput_gbps = estimated_bandwidth;
                }

                AWS_LOGF_INFO(
                    AWS_LS_S3_GENERAL,
                    "id=%p: calculated %" PRIu16
                    "Gbps for target bandwidth based on Amazon EC2 instance type of " PRInSTR ".",
                    (void *)loader,
                    loader->lock_data.current_env_platform_info.max_throughput_gbps,
                    AWS_BYTE_CURSOR_PRI(loader->lock_data.current_env_platform_info.instance_type));
        }
    }

    AWS_FATAL_ASSERT(
        !aws_hash_table_put(
            &loader->lock_data.compute_platform_info_table, &info->instance_type, (void *)info, NULL) &&
        "hash table put failed!");
}

static void s_destroy_loader(void *arg) {
    struct aws_s3_compute_platform_info_loader *loader = arg;

    /* clean up the memory we allocated in init() */
    aws_hash_table_clean_up(&loader->lock_data.compute_platform_info_table);
    aws_mutex_clean_up(&loader->lock_data.lock);

    size_t cpu_group_arr_len = loader->lock_data.current_env_platform_info.cpu_group_info_array_length;
    for (size_t i = 0; i < cpu_group_arr_len; ++i) {
        struct aws_s3_cpu_group_info *group = &loader->lock_data.current_env_platform_info.cpu_group_info_array[i];

        if (group->nic_name_array) {
            aws_mem_release(loader->allocator, group->nic_name_array);
        }
    }

    aws_mem_release(loader->allocator, loader->lock_data.current_env_platform_info.cpu_group_info_array);

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

    /* we won't know an instance type, possibly ever, but it will be set if available before returning to the user. */
    loader->lock_data.current_env_platform_info.has_recommended_configuration = false;
    loader->lock_data.current_env_platform_info.cpu_group_info_array_length =
        aws_system_environment_get_cpu_group_count(loader->current_env);
    loader->lock_data.current_env_platform_info.cpu_group_info_array = aws_mem_calloc(
        allocator,
        loader->lock_data.current_env_platform_info.cpu_group_info_array_length,
        sizeof(struct aws_s3_cpu_group_info));

    for (size_t i = 0; i < loader->lock_data.current_env_platform_info.cpu_group_info_array_length; ++i) {
        struct aws_s3_cpu_group_info *group_info = &loader->lock_data.current_env_platform_info.cpu_group_info_array[i];
        group_info->cpu_group = i;
        group_info->cpus_in_group = aws_get_cpu_count_for_group(i);
    }

    size_t network_card_count = aws_system_environment_get_network_card_count(loader->current_env);
    /* this string array is attached to loader->current_env and does not need to be freed. */
    const struct aws_string **network_cards = aws_system_environment_get_network_cards(loader->current_env);

    for (size_t i = 0; i < network_card_count; ++i) {
        const struct aws_string *network_card_name = network_cards[i];
        uint16_t group_idx = aws_system_environment_get_cpu_group_for_network_card(loader->current_env, i);
        AWS_FATAL_ASSERT(group_idx < loader->lock_data.current_env_platform_info.cpu_group_info_array_length);

        if (loader->lock_data.current_env_platform_info.cpu_group_info_array[group_idx].nic_name_array) {
            size_t current_length =
                loader->lock_data.current_env_platform_info.cpu_group_info_array[group_idx].nic_name_array_length;
            loader->lock_data.current_env_platform_info.cpu_group_info_array[group_idx].nic_name_array[current_length] =
                aws_byte_cursor_from_string(network_card_name);
            loader->lock_data.current_env_platform_info.cpu_group_info_array[group_idx].nic_name_array_length += 1;
        } else {
            /* there aren't that many, and it saves us an extra loop. Just allocate the array to be large enough for all
             * NICs and be done with it. */
            loader->lock_data.current_env_platform_info.cpu_group_info_array[group_idx].nic_name_array =
                aws_mem_calloc(allocator, network_card_count, sizeof(struct aws_byte_cursor));
            loader->lock_data.current_env_platform_info.cpu_group_info_array[group_idx].nic_name_array[0] =
                aws_byte_cursor_from_string(network_card_name);
            loader->lock_data.current_env_platform_info.cpu_group_info_array[group_idx].nic_name_array_length = 1;
        }
    }

    /* we only make a bandwidth recommendation once we know instance type. Default it to 5. */
    loader->lock_data.current_env_platform_info.max_throughput_gbps = 5;

    AWS_FATAL_ASSERT(
        !aws_hash_table_init(
            &loader->lock_data.compute_platform_info_table,
            allocator,
            32,
            aws_hash_byte_cursor_ptr_ignore_case,
            (bool (*)(const void *, const void *))aws_byte_cursor_eq_ignore_case,
            NULL,
            NULL) &&
        "Hash table init failed!");

    s_add_platform_info_to_table(loader, &s_c5n_18xlarge_platform_info);
    s_add_platform_info_to_table(loader, &s_c5n_9xlarge_platform_info);
    s_add_platform_info_to_table(loader, &s_c5n_metal_platform_info);
    s_add_platform_info_to_table(loader, &s_p4d_platform_info);
    s_add_platform_info_to_table(loader, &s_p4de_platform_info);
    s_add_platform_info_to_table(loader, &s_p5_platform_info);
    s_add_platform_info_to_table(loader, &s_trn1_n_platform_info);
    s_add_platform_info_to_table(loader, &s_trn1_platform_info);

    return loader;
}

void aws_s3_compute_platform_info_loader_acquire(struct aws_s3_compute_platform_info_loader *loader) {
    aws_ref_count_acquire(&loader->ref_count);
}

void aws_s3_compute_platform_info_loader_release(struct aws_s3_compute_platform_info_loader *loader) {
    if (loader) {
        aws_ref_count_release(&loader->ref_count);
    }
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
        info->loader->lock_data.current_env_platform_info.instance_type =
            aws_byte_cursor_from_string(info->instance_type);
        s_add_platform_info_to_table(info->loader, &info->loader->lock_data.current_env_platform_info);
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
            loader->lock_data.current_env_platform_info.instance_type =
                aws_byte_cursor_from_string(loader->lock_data.detected_instance_type);
            s_add_platform_info_to_table(loader, &loader->lock_data.current_env_platform_info);

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
    return_cur = loader->lock_data.current_env_platform_info.instance_type;
    aws_mutex_unlock(&loader->lock_data.lock);

    return return_cur;
}

const struct aws_s3_compute_platform_info *aws_s3_get_compute_platform_info_for_current_environment(
    struct aws_s3_compute_platform_info_loader *loader) {
    /* getting the instance type will set it on the loader the first time if it can */
    aws_s3_get_ec2_instance_type(loader);
    /* will never be mutated after the above call. */
    return &loader->lock_data.current_env_platform_info;
}

const struct aws_s3_compute_platform_info *aws_s3_get_compute_platform_info_for_instance_type(
    struct aws_s3_compute_platform_info_loader *loader,
    struct aws_byte_cursor instance_type_name) {
    aws_mutex_lock(&loader->lock_data.lock);
    struct aws_hash_element *platform_info_element = NULL;
    aws_hash_table_find(&loader->lock_data.compute_platform_info_table, &instance_type_name, &platform_info_element);
    aws_mutex_unlock(&loader->lock_data.lock);

    if (platform_info_element) {
        return platform_info_element->value;
    }

    return NULL;
}

bool aws_s3_is_running_on_ec2_nitro(struct aws_s3_compute_platform_info_loader *loader) {
    struct aws_byte_cursor system_virt_name = aws_system_environment_get_virtualization_vendor(loader->current_env);

    if (aws_byte_cursor_eq_c_str_ignore_case(&system_virt_name, "amazon ec2")) {
        return true;
    }

    return false;
}