#ifndef AWS_S3_S3_PLATFORM_INFO_H
#define AWS_S3_S3_PLATFORM_INFO_H
/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */
#include <aws/s3/s3.h>

struct aws_s3_cpu_group_info {
    /* group index, this usually refers to a particular numa node */
    uint16_t cpu_group;
    /* array of network devices on this node */
    const struct aws_byte_cursor *nic_name_array;
    /* length of network devices array */
    size_t nic_name_array_length;
    size_t cpus_in_group;
};

#ifdef _MSC_VER
#    pragma warning(push)
#    pragma warning(disable : 4626) /* assignment operator was implicitly defined as deleted */
#    pragma warning(disable : 5027) /* move assignment operator was implicitly defined as deleted */
#endif

struct aws_s3_compute_platform_info {
    /* name of the instance-type: example c5n.18xlarge */
    struct aws_byte_cursor instance_type;
    /* max throughput for this instance type */
    uint16_t max_throughput_gbps;
    /* array of cpu group info. This will always have at least one entry. */
    struct aws_s3_cpu_group_info *cpu_group_info_array;
    /* length of cpu group info array */
    size_t cpu_group_info_array_length;

    /* The current build of this library specifically knows an optimal configuration for this
     * platform */
    bool has_recommended_configuration;
};

#ifdef _MSC_VER
#    pragma warning(pop)
#endif

struct aws_s3_compute_platform_info_loader;

AWS_EXTERN_C_BEGIN

/**
 * Initializes and returns a loader for querying the compute platform for information needed for making configuration
 * decisions.
 *
 * Returns NULL if an unrecoverable error occurs.
 */
AWS_S3_API
struct aws_s3_compute_platform_info_loader *aws_s3_compute_platform_info_loader_new(struct aws_allocator *allocator);

AWS_S3_API
void aws_s3_compute_platform_info_loader_acquire(struct aws_s3_compute_platform_info_loader *loader);

AWS_S3_API
void aws_s3_compute_platform_info_loader_release(struct aws_s3_compute_platform_info_loader *loader);

/**
 * Retrieves the pre-configured metadata for a given ec2 instance type. If no such pre-configuration exists, returns
 * NULL.
 */
AWS_S3_API
const struct aws_s3_compute_platform_info *aws_s3_get_compute_platform_info_for_instance_type(
    struct aws_s3_compute_platform_info_loader *loader,
    struct aws_byte_cursor instance_type_name);

/**
 * Retrieves the  metadata for the current environment. If EC2 instance type is unknown, or it is not an EC2 instance at
 * all, this value will still include the information about the system that could be determined. This value will never
 * be NULL.
 */
AWS_S3_API
const struct aws_s3_compute_platform_info *aws_s3_get_compute_platform_info_for_current_environment(
    struct aws_s3_compute_platform_info_loader *loader);

/**
 * Returns true if the current process is running on an Amazon EC2 instance powered by Nitro.
 */
AWS_S3_API
bool aws_s3_is_running_on_ec2_nitro(struct aws_s3_compute_platform_info_loader *loader);

/**
 * Returns an EC2 instance type assuming this executable is running on Amazon EC2 powered by nitro.
 *
 * First this function will check it's running on EC2 via. attempting to read DMI info to avoid making IMDS calls.
 *
 * If the function detects it's on EC2, and it was able to detect the instance type without a call to IMDS
 * it will return it.
 *
 * Finally, it will call IMDS and return the instance type from there.
 *
 * Note that in the case of the IMDS call, a new client stack is spun up using 1 background thread. The call is made
 * synchronously with a 1 second timeout: It's not cheap. To make this easier, the underlying result is cached
 * internally and will be freed when aws_s3_library_clean_up() is called.
 * @return byte_cursor containing the instance type. If this is empty, the instance type could not be determined.
 */
AWS_S3_API
struct aws_byte_cursor aws_s3_get_ec2_instance_type(struct aws_s3_compute_platform_info_loader *loader);

AWS_EXTERN_C_END

#endif /* AWS_S3_S3_PLATFORM_INFO_H */
