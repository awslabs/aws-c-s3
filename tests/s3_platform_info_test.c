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
    /* c5n family has max_throughput_gbps = 100.0 */
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
        /* by_name_info may be NULL if running on an instance family not in the table.
         * If found, the throughput should be positive. */
        if (by_name_info) {
            ASSERT_TRUE(by_name_info->max_throughput_gbps > 0);
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

/* Test that the family lookup correctly matches prefixes with '.' separator */
static int s_test_family_lookup_prefix_matching(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    aws_s3_library_init(allocator);

    struct aws_s3_platform_info_loader *loader = aws_s3_platform_info_loader_new(allocator);

    /* "c5n.large" should match family "c5n" (100 Gbps), not "c5" (25 Gbps) */
    struct aws_byte_cursor c5n_large = aws_byte_cursor_from_c_str("c5n.large");
    const struct aws_s3_platform_info *c5n_info = aws_s3_get_platform_info_for_instance_type(loader, c5n_large);
    ASSERT_NOT_NULL(c5n_info);
    ASSERT_UINT_EQUALS(100, (uintmax_t)c5n_info->max_throughput_gbps);

    /* "c5.xlarge" should match family "c5" (25 Gbps), not "c5n" */
    struct aws_byte_cursor c5_xlarge = aws_byte_cursor_from_c_str("c5.xlarge");
    const struct aws_s3_platform_info *c5_info = aws_s3_get_platform_info_for_instance_type(loader, c5_xlarge);
    ASSERT_NOT_NULL(c5_info);
    ASSERT_UINT_EQUALS(25, (uintmax_t)c5_info->max_throughput_gbps);

    /* "t3.micro" should match family "t3" */
    struct aws_byte_cursor t3_micro = aws_byte_cursor_from_c_str("t3.micro");
    const struct aws_s3_platform_info *t3_info = aws_s3_get_platform_info_for_instance_type(loader, t3_micro);
    ASSERT_NOT_NULL(t3_info);
    ASSERT_UINT_EQUALS(5, (uintmax_t)t3_info->max_throughput_gbps);

    aws_s3_platform_info_loader_release(loader);
    aws_s3_library_clean_up();
    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(test_family_lookup_prefix_matching, s_test_family_lookup_prefix_matching)
