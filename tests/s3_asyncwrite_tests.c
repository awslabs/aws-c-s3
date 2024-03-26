/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */
#include "s3_tester.h"

#include <aws/checksums/crc.h>
#include <aws/common/byte_order.h>
#include <aws/common/clock.h>
#include <aws/common/device_random.h>
#include <aws/common/encoding.h>
#include <aws/s3/private/s3_util.h>
#include <aws/testing/aws_test_harness.h>

#define TIMEOUT_NANOS ((uint64_t)AWS_TIMESTAMP_NANOS * 10) /* 10secs */
#define PART_SIZE MB_TO_BYTES(5)

struct basic_asyncwrite_options {
    size_t object_size;
    size_t max_bytes_per_write;
    /* If true, EOF is passed in a separate final empty write() */
    bool eof_requires_extra_write;
};

/* Helper function for successful uploads using async write() */
static int s_basic_asyncwrite(struct aws_allocator *allocator, const struct basic_asyncwrite_options *options) {

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    /* Create S3 client */
    struct aws_s3_client_config client_config = {
        .part_size = PART_SIZE,
    };
    ASSERT_SUCCESS(aws_s3_tester_bind_client(
        &tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION | AWS_S3_TESTER_BIND_CLIENT_SIGNING));
    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);
    ASSERT_NOT_NULL(client);

    /* Create meta request */
    struct aws_s3_meta_request_test_results test_results;
    aws_s3_meta_request_test_results_init(&test_results, allocator);

    struct aws_string *host_name =
        aws_s3_tester_build_endpoint_string(allocator, &g_test_bucket_name, &g_test_s3_region);
    struct aws_byte_cursor host_name_cursor = aws_byte_cursor_from_string(host_name);
    struct aws_byte_buf object_path;
    ASSERT_SUCCESS(aws_s3_tester_upload_file_path_init(
        allocator, &object_path, aws_byte_cursor_from_c_str("/basic_asyncwrite.bin")));

    struct aws_http_message *message = aws_s3_test_put_object_request_new_without_body(
        allocator,
        &host_name_cursor,
        g_test_body_content_type,
        aws_byte_cursor_from_buf(&object_path),
        options->object_size,
        0 /*flags*/);

    /* erase content-length header, because async-write doesn't currently support it */
    aws_http_headers_erase(aws_http_message_get_headers(message), g_content_length_header_name);

    struct aws_s3_checksum_config checksum_config = {
        .checksum_algorithm = AWS_SCA_CRC32,
        .location = AWS_SCL_TRAILER,
    };

    struct aws_s3_meta_request_options meta_request_options = {
        .type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .message = message,
        .send_using_async_writes = true,
        .checksum_config = &checksum_config,
    };
    ASSERT_SUCCESS(aws_s3_tester_bind_meta_request(&tester, &meta_request_options, &test_results));

    struct aws_s3_meta_request *meta_request = aws_s3_client_make_meta_request(client, &meta_request_options);
    ASSERT_NOT_NULL(meta_request);

    /* Call write() */
    struct aws_byte_buf source_buf;
    aws_byte_buf_init(&source_buf, allocator, options->object_size);
    ASSERT_SUCCESS(aws_device_random_buffer(&source_buf));
    struct aws_byte_cursor source_cursor = aws_byte_cursor_from_buf(&source_buf);

    size_t max_bytes_per_write = options->max_bytes_per_write > 0 ? options->max_bytes_per_write : options->object_size;
    bool eof = false;
    while (source_cursor.len > 0) {
        size_t bytes_to_write = aws_min_size(max_bytes_per_write, source_cursor.len);
        struct aws_byte_cursor write_cursor = aws_byte_cursor_advance(&source_cursor, bytes_to_write);
        if (source_cursor.len == 0 && !options->eof_requires_extra_write) {
            eof = true;
        }

        /* use freshly allocated buffer for each write, so that we're likely to get memory violations
         * if this data is used wrong internally. */
        struct aws_byte_buf write_buf;
        aws_byte_buf_init_cache_and_update_cursors(&write_buf, allocator, &write_cursor, NULL);

        struct aws_future_void *write_future = aws_s3_meta_request_write(meta_request, write_cursor, eof);
        ASSERT_NOT_NULL(write_future);
        ASSERT_TRUE(aws_future_void_wait(write_future, TIMEOUT_NANOS));
        aws_byte_buf_clean_up(&write_buf);
        ASSERT_INT_EQUALS(0, aws_future_void_get_error(write_future));
        aws_future_void_release(write_future);
    }

    /* Ensure EOF is sent (eof_requires_extra_write, or object_size==0) */
    if (!eof) {
        struct aws_byte_cursor empty_cursor = {0};
        struct aws_future_void *write_future = aws_s3_meta_request_write(meta_request, empty_cursor, true /*eof*/);
        ASSERT_NOT_NULL(write_future);
        ASSERT_TRUE(aws_future_void_wait(write_future, TIMEOUT_NANOS));
        ASSERT_INT_EQUALS(0, aws_future_void_get_error(write_future));
        aws_future_void_release(write_future);
    }

    /* Wait for meta request to complete, then validate */
    aws_s3_tester_wait_for_meta_request_finish(&tester);
    ASSERT_SUCCESS(aws_s3_tester_validate_put_object_results(&test_results, 0 /*flags*/));

    /* Validate checksum */
    ASSERT_TRUE(test_results.upload_review.part_count > 0, "Update this code to handle whole-object checksum");
    source_cursor = aws_byte_cursor_from_buf(&source_buf);
    for (size_t part_i = 0; part_i < test_results.upload_review.part_count; ++part_i) {
        /* calculate checksum from source_buffer */
        size_t part_size = test_results.upload_review.part_sizes_array[part_i];
        ASSERT_TRUE(part_size <= source_cursor.len);
        uint32_t crc32_val = aws_checksums_crc32(source_cursor.ptr, part_size, 0x0 /*previousCrc32*/);
        aws_byte_cursor_advance(&source_cursor, part_size);

        uint32_t crc32_be_val = aws_hton32(crc32_val); /* to big-endian */
        struct aws_byte_cursor crc32_be_cursor = {.ptr = (uint8_t *)&crc32_be_val, .len = sizeof(crc32_be_val)};
        struct aws_byte_buf crc32_base64_buf;
        aws_byte_buf_init(&crc32_base64_buf, allocator, 16);
        ASSERT_SUCCESS(aws_base64_encode(&crc32_be_cursor, &crc32_base64_buf));

        /* compare to what got sent */
        struct aws_string *sent_checksum = test_results.upload_review.part_checksums_array[part_i];
        ASSERT_BIN_ARRAYS_EQUALS(
            crc32_base64_buf.buffer, crc32_base64_buf.len, sent_checksum->bytes, sent_checksum->len);

        aws_byte_buf_clean_up(&crc32_base64_buf);
    }

    /* Cleanup */
    meta_request = aws_s3_meta_request_release(meta_request);
    aws_s3_tester_wait_for_meta_request_shutdown(&tester);
    aws_s3_meta_request_test_results_clean_up(&test_results);
    aws_string_destroy(host_name);
    aws_byte_buf_clean_up(&object_path);
    aws_byte_buf_clean_up(&source_buf);
    aws_http_message_release(message);
    client = aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);
    return 0;
}

AWS_TEST_CASE(test_s3_asyncwrite_empty_file, s_test_s3_asyncwrite_empty_file)
static int s_test_s3_asyncwrite_empty_file(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    struct basic_asyncwrite_options options = {
        .object_size = 0,
    };
    ASSERT_SUCCESS(s_basic_asyncwrite(allocator, &options));
    return 0;
}

AWS_TEST_CASE(test_s3_asyncwrite_small_file_1_write, s_test_s3_asyncwrite_small_file_1_write)
static int s_test_s3_asyncwrite_small_file_1_write(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    struct basic_asyncwrite_options options = {
        .object_size = 100,
    };
    ASSERT_SUCCESS(s_basic_asyncwrite(allocator, &options));
    return 0;
}

AWS_TEST_CASE(test_s3_asyncwrite_small_file_1_write_then_eof, s_test_s3_asyncwrite_small_file_1_write_then_eof)
static int s_test_s3_asyncwrite_small_file_1_write_then_eof(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    struct basic_asyncwrite_options options = {
        .object_size = 100,
        .eof_requires_extra_write = true,
    };
    ASSERT_SUCCESS(s_basic_asyncwrite(allocator, &options));
    return 0;
}

AWS_TEST_CASE(test_s3_asyncwrite_small_file_many_writes, s_test_s3_asyncwrite_small_file_many_writes)
static int s_test_s3_asyncwrite_small_file_many_writes(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    struct basic_asyncwrite_options options = {
        .object_size = 100,
        .max_bytes_per_write = 1,
    };
    ASSERT_SUCCESS(s_basic_asyncwrite(allocator, &options));
    return 0;
}

AWS_TEST_CASE(test_s3_asyncwrite_1_part, s_test_s3_asyncwrite_1_part)
static int s_test_s3_asyncwrite_1_part(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    struct basic_asyncwrite_options options = {
        .object_size = PART_SIZE,
    };
    ASSERT_SUCCESS(s_basic_asyncwrite(allocator, &options));
    return 0;
}

AWS_TEST_CASE(test_s3_asyncwrite_1_part_many_writes, s_test_s3_asyncwrite_1_part_many_writes)
static int s_test_s3_asyncwrite_1_part_many_writes(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    struct basic_asyncwrite_options options = {.object_size = PART_SIZE, .max_bytes_per_write = PART_SIZE / 16};
    ASSERT_SUCCESS(s_basic_asyncwrite(allocator, &options));
    return 0;
}

AWS_TEST_CASE(test_s3_asyncwrite_1_part_then_empty_eof, s_test_s3_asyncwrite_1_part_then_empty_eof)
static int s_test_s3_asyncwrite_1_part_then_empty_eof(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    struct basic_asyncwrite_options options = {
        .object_size = PART_SIZE,
        .eof_requires_extra_write = true,
    };
    ASSERT_SUCCESS(s_basic_asyncwrite(allocator, &options));
    return 0;
}

AWS_TEST_CASE(test_s3_asyncwrite_2_parts_1_write, s_test_s3_asyncwrite_2_parts_1_write)
static int s_test_s3_asyncwrite_2_parts_1_write(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    struct basic_asyncwrite_options options = {
        .object_size = PART_SIZE * 2,
    };
    ASSERT_SUCCESS(s_basic_asyncwrite(allocator, &options));
    return 0;
}

AWS_TEST_CASE(test_s3_asyncwrite_2_parts_2_partsize_writes, s_test_s3_asyncwrite_2_parts_2_partsize_writes)
static int s_test_s3_asyncwrite_2_parts_2_partsize_writes(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    struct basic_asyncwrite_options options = {
        .object_size = PART_SIZE * 2,
        .max_bytes_per_write = PART_SIZE,
    };
    ASSERT_SUCCESS(s_basic_asyncwrite(allocator, &options));
    return 0;
}

AWS_TEST_CASE(
    test_s3_asyncwrite_2_parts_first_write_over_partsize,
    s_test_s3_asyncwrite_2_parts_first_write_over_partsize)
static int s_test_s3_asyncwrite_2_parts_first_write_over_partsize(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    struct basic_asyncwrite_options options = {
        .object_size = PART_SIZE * 2,
        .max_bytes_per_write = PART_SIZE + 100,
    };
    ASSERT_SUCCESS(s_basic_asyncwrite(allocator, &options));
    return 0;
}

AWS_TEST_CASE(
    test_s3_asyncwrite_2_parts_first_write_under_partsize,
    s_test_s3_asyncwrite_2_parts_first_write_under_partsize)
static int s_test_s3_asyncwrite_2_parts_first_write_under_partsize(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    struct basic_asyncwrite_options options = {
        .object_size = PART_SIZE * 2,
        .max_bytes_per_write = PART_SIZE - 100,
    };
    ASSERT_SUCCESS(s_basic_asyncwrite(allocator, &options));
    return 0;
}
