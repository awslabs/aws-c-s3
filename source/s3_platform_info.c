/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/auth/aws_imds_client.h>
#include <aws/common/clock.h>
#include <aws/common/condition_variable.h>
#include <aws/common/hash_table.h>
#include <aws/common/mutex.h>
#include <aws/common/system_info.h>
#include <aws/io/channel_bootstrap.h>
#include <aws/io/event_loop.h>
#include <aws/io/host_resolver.h>
#include <aws/s3/private/s3_platform_info.h>

/* note: setting `has_recommended_configuration` to true would mean crt client
 * can be loaded by default on the instance. We are not ready for this behavior
 * on certain instance families yet so this might be set to false. */

/**
 * EC2 instance family information for right-sizing S3 client resources.
 * Used to estimate NIC bandwidth when the exact instance type is not in
 * the pre-known hash table. The family prefix is matched against the
 * detected instance type (e.g., "c5n" from "c5n.18xlarge").
 *
 * Bandwidth estimation uses vCPU-proportional scaling (see
 * s_estimate_throughput_from_family for details and examples).
 *
 * Table entries MUST be ordered with longer prefixes first so that
 * "c5n" matches before "c5", "m5zn" before "m5n" before "m5", etc.
 *
 * Verified against AWS EC2 Instance Types documentation as of 2026-09-04.
 * In the future we should be able to get exact values per EC2 instance
 * via IMDS. Once that's available, we should replace this entire table.
 */
struct aws_s3_ec2_instance_type_info {
    const char *instance_type_prefix;
    double nic_bandwidth_baseline_gbps;
    uint32_t vcpus_at_max_size;
    bool has_recommended_configuration;
    /* Full instance type name for the max size in this family. Only populated
     * for families with has_recommended_configuration = true. Used by
     * aws_s3_get_recommended_platforms() to return full instance type names
     * for backward compatibility with consumers that do exact string matching. */
    const char *instance_type_max;
};

/* clang-format off */
static const struct aws_s3_ec2_instance_type_info s_ec2_instance_type_table[] = {
    /*                                                   baseline   vcpus
     *  prefix                                           gbps       @max    recommended  instance_type_max */

    /* === Accelerated / Training === */
    {"trn1n",                                            800.0,    128,        true,        "trn1n.32xlarge"},
    {"trn1",                                             600.0,    128,        true,        "trn1.32xlarge"},

    /* For p5e.48xlarge through p6-b300.48xlarge, the max_throughput_gbps values
     * are based on the maximum bandwidth offered from a single NIC. CRT clients
     * default to using a single NIC unless configured to use multiple NICs by
     * identifying the number of NICs and providing the names in an array (refer
     * s3_client.h: struct aws_s3_client_config). The max_throughput_gbps is only
     * a default that can be overridden by the user's client config.
     * TODO: Once we are able to auto-detect NICs and add them, default values
     * should be updated with maximum ENA network bandwidth for these instances. */
    {"p6-b300",                                          350.0,    192,        true,        "p6-b300.48xlarge"},
    {"p6-b200",                                          200.0,    192,        true,        "p6-b200.48xlarge"},
    {"p5en",                                             100.0,    192,        true,        "p5en.48xlarge"},
    {"p5e",                                              100.0,    192,        true,        "p5e.48xlarge"},

    /* The p5 is a stunningly massive instance type. While the specs show 3.2 Tb/s
     * for the network bandwidth, not all of that is accessible from the CPU. From
     * the CPU we can get around 400 Gbps. The 3.2 Tb/s with 2 sockets on a Nitro
     * instance implies 16 NICs per node. However, practically, due to the topology
     * of this instance, as far as this client is concerned there are two NICs per
     * node, similar to the p4d. The rest is for other things on the machine to use. */
    {"p5",                                               400.0,    192,        true,        "p5.48xlarge"},
    {"p4de",                                             400.0,     96,        true,        "p4de.24xlarge"},
    {"p4d",                                              400.0,     96,        true,        "p4d.24xlarge"},
    {"dl1",                                              400.0,     96,        false,       NULL},

    /* === Compute optimized === */
    {"c9gd",                                             100.0,    192,        false,       NULL},
    {"c9g",                                              100.0,    192,        false,       NULL},
    {"c8ine",                                             75.0,     48,        false,       NULL},
    {"c8in",                                             600.0,    384,        false,       NULL},
    {"c8ib",                                             400.0,    384,        false,       NULL},
    {"c8id",                                             100.0,    384,        false,       NULL},
    {"c8i-flex",                                          15.0,     64,        false,       NULL},
    {"c8i",                                              100.0,    384,        false,       NULL},
    {"c8gn",                                             600.0,    192,        false,       NULL},
    {"c8gb",                                             400.0,    192,        false,       NULL},
    {"c8gd",                                              50.0,    192,        false,       NULL},
    {"c8g",                                               50.0,    192,        false,       NULL},
    {"c8a",                                               75.0,    192,        false,       NULL},
    {"c7i-flex",                                          12.5,     64,        false,       NULL},
    {"c7i",                                               50.0,    192,        false,       NULL},
    {"c7gn",                                             200.0,     64,        false,       NULL},
    {"c7gd",                                              30.0,     64,        false,       NULL},
    {"c7g",                                               30.0,     64,        false,       NULL},
    {"c7a",                                               50.0,    192,        false,       NULL},
    {"c6in",                                             200.0,    128,        false,       NULL},
    {"c6id",                                              50.0,    128,        false,       NULL},
    {"c6i",                                               50.0,    128,        false,       NULL},
    {"c6gn",                                             100.0,     64,        false,       NULL},
    {"c6gd",                                              25.0,     64,        false,       NULL},
    {"c6g",                                               25.0,     64,        false,       NULL},
    {"c6a",                                               50.0,    192,        false,       NULL},
    {"c5n",                                              100.0,     72,        false,       NULL},
    {"c5ad",                                              20.0,     96,        false,       NULL},
    {"c5a",                                               20.0,     96,        false,       NULL},
    {"c5d",                                               25.0,     96,        false,       NULL},
    {"c5",                                                25.0,     96,        false,       NULL},

    /* === General purpose === */
    {"m8a",                                               75.0,    192,        false,       NULL},
    {"m8g",                                               50.0,    192,        false,       NULL},
    {"m7i-flex",                                          12.5,     64,        false,       NULL},
    {"m7i",                                               50.0,    192,        false,       NULL},
    {"m7a",                                               50.0,    192,        false,       NULL},
    {"m7g",                                               30.0,     64,        false,       NULL},
    {"m6in",                                             200.0,    128,        false,       NULL},
    {"m6idn",                                            200.0,    128,        false,       NULL},
    {"m6id",                                              50.0,    128,        false,       NULL},
    {"m6i",                                               50.0,    128,        false,       NULL},
    {"m6a",                                               50.0,    192,        false,       NULL},
    {"m6gd",                                              25.0,     64,        false,       NULL},
    {"m6g",                                               25.0,     64,        false,       NULL},
    {"m5zn",                                             100.0,     48,        false,       NULL},
    {"m5dn",                                             100.0,     96,        false,       NULL},
    {"m5n",                                              100.0,     96,        false,       NULL},
    {"m5ad",                                              20.0,     96,        false,       NULL},
    {"m5a",                                               20.0,     96,        false,       NULL},
    {"m5d",                                               25.0,     96,        false,       NULL},
    {"m5",                                                25.0,     96,        false,       NULL},

    /* === Memory optimized === */
    {"r7iz",                                              50.0,    128,        false,       NULL},
    {"r7i",                                               50.0,    192,        false,       NULL},
    {"r7a",                                               50.0,    192,        false,       NULL},
    {"r7g",                                               30.0,     64,        false,       NULL},
    {"r6in",                                             200.0,    128,        false,       NULL},
    {"r6idn",                                            200.0,    128,        false,       NULL},
    {"r6id",                                              50.0,    128,        false,       NULL},
    {"r6i",                                               50.0,    128,        false,       NULL},
    {"r6a",                                               50.0,    192,        false,       NULL},
    {"r6gd",                                              25.0,     64,        false,       NULL},
    {"r6g",                                               25.0,     64,        false,       NULL},
    {"r5dn",                                             100.0,     96,        false,       NULL},
    {"r5n",                                              100.0,     96,        false,       NULL},
    {"r5ad",                                              20.0,     96,        false,       NULL},
    {"r5a",                                               20.0,     96,        false,       NULL},
    {"r5d",                                               25.0,     96,        false,       NULL},
    {"r5",                                                25.0,     96,        false,       NULL},
    {"x2iedn",                                           100.0,    128,        false,       NULL},
    {"x2idn",                                            100.0,    128,        false,       NULL},
    {"x2iezn",                                           100.0,     48,        false,       NULL},
    {"x1e",                                               25.0,    128,        false,       NULL},
    {"x1",                                                25.0,    128,        false,       NULL},

    /* === Storage optimized === */
    {"i4i",                                               75.0,    128,        false,       NULL},
    {"i3en",                                             100.0,     96,        false,       NULL},
    {"i3",                                                25.0,     64,        false,       NULL},
    {"is4gen",                                            50.0,     32,        false,       NULL},
    {"im4gn",                                            100.0,     64,        false,       NULL},
    {"d3en",                                              75.0,     48,        false,       NULL},
    {"d3",                                                25.0,     32,        false,       NULL},
    {"h1",                                                25.0,     64,        false,       NULL},

    /* === Accelerated (GPU/inference) === */
    {"g6e",                                              400.0,    192,        false,       NULL},
    {"g6",                                               100.0,    192,        false,       NULL},
    {"g5g",                                               25.0,     64,        false,       NULL},
    {"g5",                                               100.0,    192,        false,       NULL},
    {"g4dn",                                             100.0,     96,        false,       NULL},
    {"inf2",                                             100.0,    192,        false,       NULL},
    {"inf1",                                             100.0,     96,        false,       NULL},

    /* === Burstable === */
    {"t4g",                                                5.0,      8,        false,       NULL},
    {"t3a",                                                5.0,      8,        false,       NULL},
    {"t3",                                                 5.0,      8,        false,       NULL},
    {"t2",                                                 1.0,      8,        false,       NULL},
};
/* clang-format on */

#define S_EC2_INSTANCE_TYPE_TABLE_SIZE (sizeof(s_ec2_instance_type_table) / sizeof(s_ec2_instance_type_table[0]))

/**
 * Look up the EC2 instance family info by prefix-matching the instance type string.
 * Returns NULL if no matching family is found.
 * The table is ordered longest-prefix-first, so the first match wins.
 */
static const struct aws_s3_ec2_instance_type_info *s_get_ec2_family_info(struct aws_byte_cursor instance_type) {
    for (size_t i = 0; i < S_EC2_INSTANCE_TYPE_TABLE_SIZE; ++i) {
        const struct aws_s3_ec2_instance_type_info *entry = &s_ec2_instance_type_table[i];
        size_t prefix_len = strlen(entry->instance_type_prefix);

        if (instance_type.len < prefix_len + 1) {
            /* instance type must be at least prefix + "." + size */
            continue;
        }

        /* Check if instance_type starts with the prefix followed by '.' */
        if (aws_byte_cursor_starts_with(
                &instance_type,
                &(struct aws_byte_cursor){.ptr = (uint8_t *)entry->instance_type_prefix, .len = prefix_len})) {
            /* Verify the character after the prefix is '.' to avoid "c5" matching "c5n.large" */
            if (instance_type.ptr[prefix_len] == '.') {
                return entry;
            }
        }
    }
    return NULL;
}

/**
 * Estimate NIC bandwidth for an instance type using the family table and vCPU count.
 * Returns 0.0 if the family is not found or vCPU count is not available.
 */
static double s_estimate_throughput_from_family(struct aws_byte_cursor instance_type) {
    const struct aws_s3_ec2_instance_type_info *family_info = s_get_ec2_family_info(instance_type);

    /* If we can't find the family, we'll revert to full default. */
    if (family_info == NULL) {
        return 0.0;
    }

    /* Get local vCPU count using aws-c-common's cross-platform API.
     *
     * On bare EC2 instances, this returns the instance's actual vCPU count.
     * In containers (Docker, ECS, Kubernetes), this returns the host's CPU
     * count, not the container's CPU limit. For example, a container limited
     * to 4 CPUs on a 96-vCPU host will report 96 here, which over-estimates
     * the throughput. This is safe (just less optimal). A future improvement
     * could read the container's CPU quota from /sys/fs/cgroup/ to get the
     * actual limit. */
    size_t local_vcpus = aws_system_info_processor_count();
    if (local_vcpus == 0) {
        local_vcpus = 1;
    }

    /*
     * Estimate this instance's NIC bandwidth using vCPU-proportional scaling.
     *
     * We only know the family prefix (e.g., "c5" from "c5.xlarge"), not the
     * exact instance size. AWS scales NIC bandwidth roughly proportional to
     * vCPU count within a family, so we use the local vCPU count as a proxy:
     *
     *   per_vcpu_rate = family_max_bandwidth / family_max_vcpus
     *   estimated     = per_vcpu_rate * local_vcpus
     *
     * Example: c5 family max is 25 Gbps at 96 vCPUs.
     *   c5.xlarge  (4 vCPU):  (25/96) * 4  = 1.04 Gbps -> 256 MiB pool
     *   c5.4xlarge (16 vCPU): (25/96) * 16 = 4.17 Gbps -> 512 MiB pool
     *   c5.24xlarge (96 vCPU): (25/96) * 96 = 25 Gbps  -> 4 GiB pool
     *
     * This avoids over-provisioning small instances (which would get the
     * family max) and doesn't require ec2:DescribeInstanceTypes IAM
     * permission, which many S3 workloads lack.
     *
     * This will be resolved when we can use IMDS to get the actual
     * NIC bandwidth allotted to an EC2 instance.
     */
    double per_vcpu_rate = family_info->nic_bandwidth_baseline_gbps / (double)family_info->vcpus_at_max_size;
    double estimated_gbps = per_vcpu_rate * (double)local_vcpus;

    /* Clamp to the family maximum (can't exceed the max-size instance's bandwidth) */
    if (estimated_gbps > family_info->nic_bandwidth_baseline_gbps) {
        estimated_gbps = family_info->nic_bandwidth_baseline_gbps;
    }

    AWS_LOGF_INFO(
        AWS_LS_S3_CLIENT,
        "EC2 family lookup: instance_type=" PRInSTR ", family=%s, local_vcpus=%zu, "
        "family_max=%.1f Gbps, estimated=%.1f Gbps",
        AWS_BYTE_CURSOR_PRI(instance_type),
        family_info->instance_type_prefix,
        local_vcpus,
        family_info->nic_bandwidth_baseline_gbps,
        estimated_gbps);

    return estimated_gbps;
}

struct aws_s3_platform_info_loader {
    struct aws_allocator *allocator;
    struct aws_ref_count ref_count;
    struct {
        struct aws_string *detected_instance_type;
        struct aws_s3_platform_info current_env_platform_info;
        /* aws_hash_table<aws_byte_cursor*, aws_s3_platform_info *>
         * the table does not "own" any of the data inside it. */
        struct aws_hash_table compute_platform_info_table;
        /* Tracks dynamically allocated platform_info entries and their backing
         * aws_string objects, so they can be freed when the loader is destroyed. */
        struct aws_array_list dynamic_platform_infos; /* aws_array_list<struct s_dynamic_platform_entry> */
        struct aws_mutex lock;
    } lock_data;
    struct aws_system_environment *current_env;
};

/* Tracks a dynamically allocated platform_info + its backing string for cleanup */
struct s_dynamic_platform_entry {
    struct aws_s3_platform_info *info;
    struct aws_string *instance_type_str;
};

void s_add_platform_info_to_table(struct aws_s3_platform_info_loader *loader, struct aws_s3_platform_info *info) {
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
        struct aws_s3_platform_info *existing = platform_info_element->value;
        // TODO: sync the cpu group and NIC data
        info->has_recommended_configuration = existing->has_recommended_configuration;
        /* always prefer a pre-known bandwidth, as we estimate low on EC2 by default for safety. */
        info->max_throughput_gbps = existing->max_throughput_gbps;
    } else {
        AWS_FATAL_ASSERT(
            !aws_hash_table_put(
                &loader->lock_data.compute_platform_info_table, &info->instance_type, (void *)info, NULL) &&
            "hash table put failed!");
    }
}

static void s_destroy_loader(void *arg) {
    struct aws_s3_platform_info_loader *loader = arg;

    aws_hash_table_clean_up(&loader->lock_data.compute_platform_info_table);

    /* Free dynamically allocated platform_info entries and their backing strings */
    for (size_t i = 0; i < aws_array_list_length(&loader->lock_data.dynamic_platform_infos); ++i) {
        struct s_dynamic_platform_entry entry;
        aws_array_list_get_at(&loader->lock_data.dynamic_platform_infos, &entry, i);
        aws_string_destroy(entry.instance_type_str);
        aws_mem_release(loader->allocator, entry.info);
    }
    aws_array_list_clean_up(&loader->lock_data.dynamic_platform_infos);

    aws_mutex_clean_up(&loader->lock_data.lock);

    if (loader->lock_data.detected_instance_type) {
        aws_string_destroy(loader->lock_data.detected_instance_type);
    }

    aws_system_environment_release(loader->current_env);
    aws_mem_release(loader->allocator, loader);
}

struct aws_s3_platform_info_loader *aws_s3_platform_info_loader_new(struct aws_allocator *allocator) {
    struct aws_s3_platform_info_loader *loader =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_platform_info_loader));

    loader->allocator = allocator;
    loader->current_env = aws_system_environment_load(allocator);
    AWS_FATAL_ASSERT(loader->current_env && "Failed to load system environment");
    aws_mutex_init(&loader->lock_data.lock);
    aws_ref_count_init(&loader->ref_count, loader, s_destroy_loader);

    /* TODO: Implement runtime CPU information retrieval from the system. Currently, Valgrind detects a memory leak
     * associated with the g_numa_node_of_cpu_ptr function (see: https://github.com/numactl/numactl/issues/3). This
     * issue was addressed in version v2.0.13 of libnuma (see: https://github.com/numactl/numactl/pull/43). However,
     * Amazon Linux 2 defaults to libnuma version v2.0.9, which lacks this fix. We need to suppress this
     * warning as a false positive in older versions of libnuma. In the future, however, we will probably eliminate the
     * use of numactl altogether. */

    AWS_FATAL_ASSERT(
        !aws_hash_table_init(
            &loader->lock_data.compute_platform_info_table,
            allocator,
            32,
            aws_hash_byte_cursor_ptr_ignore_case,
            (aws_hash_callback_eq_fn *)aws_byte_cursor_eq_ignore_case,
            NULL,
            NULL) &&
        "Hash table init failed!");

    aws_array_list_init_dynamic(
        &loader->lock_data.dynamic_platform_infos, allocator, 4, sizeof(struct s_dynamic_platform_entry));

    return loader;
}

struct aws_s3_platform_info_loader *aws_s3_platform_info_loader_acquire(struct aws_s3_platform_info_loader *loader) {
    aws_ref_count_acquire(&loader->ref_count);
    return loader;
}

struct aws_s3_platform_info_loader *aws_s3_platform_info_loader_release(struct aws_s3_platform_info_loader *loader) {
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
    bool shutdown_completed;
    struct aws_mutex mutex;
};

static void s_imds_client_shutdown_completed(void *user_data) {
    struct imds_callback_info *info = user_data;
    aws_mutex_lock(&info->mutex);
    info->shutdown_completed = true;
    aws_condition_variable_notify_all(&info->c_var);
    aws_mutex_unlock(&info->mutex);
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
    aws_condition_variable_notify_all(&info->c_var);
    aws_mutex_unlock(&info->mutex);
}

static bool s_completion_predicate(void *arg) {
    struct imds_callback_info *info = arg;
    return info->error_code != 0 || info->instance_type != NULL;
}

struct aws_string *s_query_imds_for_instance_type(struct aws_allocator *allocator) {

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

    if (aws_imds_client_get_instance_info(imds_client, s_imds_client_on_get_instance_info_callback, &callback_info)) {
        aws_condition_variable_wait_for_pred(
            &callback_info.c_var, &callback_info.mutex, AWS_TIMESTAMP_SECS, s_completion_predicate, &callback_info);
    }
    aws_imds_client_release(imds_client);
    aws_condition_variable_wait_pred(
        &callback_info.c_var, &callback_info.mutex, s_client_shutdown_predicate, &callback_info);

    aws_mutex_unlock(&callback_info.mutex);

    if (callback_info.error_code) {
        aws_raise_error(callback_info.error_code);
        AWS_LOGF_ERROR(
            AWS_LS_S3_CLIENT, "IMDS call failed with error %s.", aws_error_debug_str(callback_info.error_code));
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
    return callback_info.instance_type;
}

struct aws_byte_cursor aws_s3_get_ec2_instance_type(struct aws_s3_platform_info_loader *loader, bool cached_only) {
    aws_mutex_lock(&loader->lock_data.lock);
    struct aws_byte_cursor return_cur;
    AWS_ZERO_STRUCT(return_cur);

    if (loader->lock_data.detected_instance_type) {
        AWS_LOGF_TRACE(
            AWS_LS_S3_CLIENT,
            "id=%p: Instance type has already been determined to be %s. Returning cached version.",
            (void *)loader,
            aws_string_bytes(loader->lock_data.detected_instance_type));
        goto return_instance_and_unlock;
    }
    if (cached_only) {
        AWS_LOGF_TRACE(
            AWS_LS_S3_CLIENT,
            "id=%p: Instance type has not been cached. Returning without trying to determine instance type since "
            "cached_only is set.",
            (void *)loader);
        goto return_instance_and_unlock;
    }

    AWS_LOGF_TRACE(
        AWS_LS_S3_CLIENT,
        "id=%p: Instance type has not been determined, checking to see if running in EC2 nitro environment.",
        (void *)loader);
    /*
     * We want to only imds call if we know that we are on an ec2 instance. All new instances are Nitro and we don't
     * care about the old ones.
     */
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
        struct aws_string *instance_type = s_query_imds_for_instance_type(loader->allocator);
        if (instance_type) {
            loader->lock_data.detected_instance_type = instance_type;
            loader->lock_data.current_env_platform_info.instance_type = aws_byte_cursor_from_string(instance_type);
            s_add_platform_info_to_table(loader, &loader->lock_data.current_env_platform_info);
            AWS_LOGF_INFO(
                AWS_LS_S3_CLIENT,
                "id=%p: Determined instance type to be %s, from IMDS.",
                (void *)loader,
                aws_string_bytes(loader->lock_data.detected_instance_type));
        }
    }

return_instance_and_unlock:
    return_cur = loader->lock_data.current_env_platform_info.instance_type;
    aws_mutex_unlock(&loader->lock_data.lock);

    return return_cur;
}

const struct aws_s3_platform_info *aws_s3_get_platform_info_for_current_environment(
    struct aws_s3_platform_info_loader *loader) {
    /* getting the instance type will set it on the loader the first time if it can */
    aws_s3_get_ec2_instance_type(loader, false /*cached_only*/);

    /* If we detected an instance type but don't have throughput info for it yet,
     * try the per-family lookup table to estimate bandwidth from the family prefix
     * and local vCPU count. */
    if (loader->lock_data.current_env_platform_info.instance_type.len > 0 &&
        loader->lock_data.current_env_platform_info.max_throughput_gbps == 0) {

        double estimated = s_estimate_throughput_from_family(loader->lock_data.current_env_platform_info.instance_type);
        if (estimated > 0.0) {
            loader->lock_data.current_env_platform_info.max_throughput_gbps = estimated;

            /* Also propagate has_recommended_configuration from the family entry */
            const struct aws_s3_ec2_instance_type_info *family_info =
                s_get_ec2_family_info(loader->lock_data.current_env_platform_info.instance_type);
            if (family_info != NULL) {
                loader->lock_data.current_env_platform_info.has_recommended_configuration =
                    family_info->has_recommended_configuration;
            }
        }
    }

    /* will never be mutated after the above call. */
    return &loader->lock_data.current_env_platform_info;
}

struct aws_array_list aws_s3_get_recommended_platforms(struct aws_s3_platform_info_loader *loader) {
    struct aws_array_list array_list;
    (void)loader; /* loader not needed for static table iteration */
    aws_array_list_init_dynamic(&array_list, loader->allocator, 16, sizeof(struct aws_byte_cursor));
    /* Iterate the static family table and add full instance type names for entries
     * that have has_recommended_configuration == true. Returns the same set of
     * instance type names as the original pre-known entries for backward compatibility. */
    for (size_t i = 0; i < S_EC2_INSTANCE_TYPE_TABLE_SIZE; ++i) {
        if (s_ec2_instance_type_table[i].has_recommended_configuration &&
            s_ec2_instance_type_table[i].instance_type_max != NULL) {
            struct aws_byte_cursor instance_type_cursor =
                aws_byte_cursor_from_c_str(s_ec2_instance_type_table[i].instance_type_max);
            aws_array_list_push_back(&array_list, &instance_type_cursor);
        }
    }
    return array_list;
}

const struct aws_s3_platform_info *aws_s3_get_platform_info_for_instance_type(
    struct aws_s3_platform_info_loader *loader,
    struct aws_byte_cursor instance_type_name) {
    aws_mutex_lock(&loader->lock_data.lock);
    struct aws_hash_element *platform_info_element = NULL;
    aws_hash_table_find(&loader->lock_data.compute_platform_info_table, &instance_type_name, &platform_info_element);

    if (platform_info_element) {
        aws_mutex_unlock(&loader->lock_data.lock);
        return platform_info_element->value;
    }

    /* Not in hash table. Try the family lookup table to estimate throughput. */
    const struct aws_s3_ec2_instance_type_info *family_info = s_get_ec2_family_info(instance_type_name);
    if (family_info == NULL) {
        aws_mutex_unlock(&loader->lock_data.lock);
        return NULL;
    }

    /* Allocate a platform_info, populate it from the family entry, and cache it in the hash table
     * so subsequent lookups return the same pointer. Track the allocation for cleanup. */
    struct aws_s3_platform_info *new_info = aws_mem_calloc(loader->allocator, 1, sizeof(struct aws_s3_platform_info));

    struct aws_string *instance_type_str = aws_string_new_from_cursor(loader->allocator, &instance_type_name);
    new_info->instance_type = aws_byte_cursor_from_string(instance_type_str);
    new_info->max_throughput_gbps = family_info->nic_bandwidth_baseline_gbps;
    new_info->has_recommended_configuration = family_info->has_recommended_configuration;

    /* Track for cleanup when loader is destroyed */
    struct s_dynamic_platform_entry entry = {.info = new_info, .instance_type_str = instance_type_str};
    aws_array_list_push_back(&loader->lock_data.dynamic_platform_infos, &entry);

    s_add_platform_info_to_table(loader, new_info);
    aws_mutex_unlock(&loader->lock_data.lock);

    return new_info;
}

bool aws_s3_is_running_on_ec2_nitro(struct aws_s3_platform_info_loader *loader) {
    struct aws_byte_cursor system_virt_name = aws_system_environment_get_virtualization_vendor(loader->current_env);

    if (aws_byte_cursor_eq_c_str_ignore_case(&system_virt_name, "amazon ec2")) {
        return true;
    }

    return false;
}
