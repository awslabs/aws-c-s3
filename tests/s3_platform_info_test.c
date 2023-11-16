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
            ASSERT_TRUE(platform_info->max_throughput_gbps == by_name_info->max_throughput_gbps);
        }

        aws_s3_platform_info_loader_release(loader);
    }

    aws_s3_library_clean_up();
    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(load_platform_info_from_global_state_sanity_test, s_load_platform_info_from_global_state_sanity_test)

static int s_test_get_platforms_with_recommended_config(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_s3_library_init(allocator);

    struct aws_array_list recommended_platform_list = aws_s3_get_platforms_with_recommended_config();
    ASSERT_TRUE(aws_array_list_length(&recommended_platform_list) > 0);
    for (size_t i = 0; i < aws_array_list_length(&recommended_platform_list); ++i) {
        struct aws_byte_cursor cursor;
        aws_array_list_get_at(&recommended_platform_list, &cursor, i);
        ASSERT_TRUE(cursor.len > 0);
    }
    aws_array_list_clean_up(&recommended_platform_list);
    aws_s3_library_clean_up();
    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(test_get_platforms_with_recommended_config, s_test_get_platforms_with_recommended_config)
