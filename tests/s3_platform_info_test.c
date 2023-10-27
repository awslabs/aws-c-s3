/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/s3/private/s3_platform_info.h>
#include <aws/s3/s3.h>

#include <aws/testing/aws_test_harness.h>

static int s_test_get_existing_platform_info(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_s3_library_init(allocator);

    struct aws_byte_cursor instance_type = aws_byte_cursor_from_c_str("c5n.18xlarge");
    struct aws_s3_platform_info_loader *loader = aws_s3_platform_info_loader_new(allocator);

    const struct aws_s3_platform_info *platform_info =
        aws_s3_get_platform_info_for_instance_type(loader, instance_type);
    ASSERT_NOT_NULL(platform_info);

    ASSERT_BIN_ARRAYS_EQUALS(
        instance_type.ptr, instance_type.len, platform_info->instance_type.ptr, platform_info->instance_type.len);
    ASSERT_UINT_EQUALS(100, (uintmax_t)platform_info->max_throughput_gbps);
    ASSERT_UINT_EQUALS(2, (uintmax_t)platform_info->cpu_group_info_array_length);
    ASSERT_NOT_NULL(platform_info->cpu_group_info_array);
    ASSERT_UINT_EQUALS(0, (uintmax_t)platform_info->cpu_group_info_array[0].cpu_group);
    ASSERT_NOT_NULL(platform_info->cpu_group_info_array[0].nic_name_array);
    ASSERT_UINT_EQUALS(1, (uintmax_t)platform_info->cpu_group_info_array[0].nic_name_array_length);

    struct aws_byte_cursor nic_name = aws_byte_cursor_from_c_str("eth0");
    ASSERT_BIN_ARRAYS_EQUALS(
        nic_name.ptr,
        nic_name.len,
        platform_info->cpu_group_info_array[0].nic_name_array[0].ptr,
        platform_info->cpu_group_info_array[0].nic_name_array[0].len);

    ASSERT_UINT_EQUALS(1, platform_info->cpu_group_info_array[1].cpu_group);
    ASSERT_NULL(platform_info->cpu_group_info_array[1].nic_name_array);
    ASSERT_UINT_EQUALS(0, platform_info->cpu_group_info_array[1].nic_name_array_length);

    aws_s3_platform_info_loader_release(loader);
    aws_s3_library_clean_up();
    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(test_get_existing_platform_info, s_test_get_existing_platform_info)

static int s_test_get_nonexistent_platform_info(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_s3_library_init(allocator);

    struct aws_s3_platform_info_loader *loader = aws_s3_platform_info_loader_new(allocator);

    struct aws_byte_cursor instance_type = aws_byte_cursor_from_c_str("non-existent");
    const struct aws_s3_platform_info *platform_info =
        aws_s3_get_platform_info_for_instance_type(loader, instance_type);
    ASSERT_NULL(platform_info);

    aws_s3_platform_info_loader_release(loader);
    aws_s3_library_clean_up();
    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(test_get_nonexistent_platform_info, s_test_get_nonexistent_platform_info)

static int s_load_platform_info_from_global_state_sanity_test(struct aws_allocator *allocator, void *arg) {
    (void)arg;
    aws_s3_library_init(allocator);

    const struct aws_s3_platform_info *platform_info = aws_s3_get_current_platform_info();
    ASSERT_NOT_NULL(platform_info);
    ASSERT_NOT_NULL(platform_info->cpu_group_info_array);
    ASSERT_TRUE(platform_info->cpu_group_info_array_length > 0);

    if (platform_info->instance_type.len) {
        struct aws_s3_platform_info_loader *loader = aws_s3_platform_info_loader_new(allocator);
        const struct aws_s3_platform_info *by_name_info =
            aws_s3_get_platform_info_for_instance_type(loader, platform_info->instance_type);
        if (by_name_info) {
            ASSERT_BIN_ARRAYS_EQUALS(
                platform_info->instance_type.ptr,
                platform_info->instance_type.len,
                by_name_info->instance_type.ptr,
                by_name_info->instance_type.len);
            ASSERT_UINT_EQUALS(platform_info->cpu_group_info_array_length, by_name_info->cpu_group_info_array_length);
            ASSERT_TRUE(platform_info->max_throughput_gbps == by_name_info->max_throughput_gbps);
        }

        aws_s3_platform_info_loader_release(loader);
    }

    aws_s3_library_clean_up();
    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(load_platform_info_from_global_state_sanity_test, s_load_platform_info_from_global_state_sanity_test)
