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

struct asyncwrite_tester {
    struct aws_allocator *allocator;
    struct aws_s3_tester s3_tester;
    struct aws_s3_client *client;
    struct aws_s3_meta_request *meta_request;
    struct aws_s3_meta_request_test_results test_results;
    struct aws_byte_buf source_buf;
};

static int s_asyncwrite_tester_init(
    struct asyncwrite_tester *tester,
    struct aws_allocator *allocator,
    size_t object_size) {

    AWS_ZERO_STRUCT(*tester);
    tester->allocator = allocator;

    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester->s3_tester));

    /* Create S3 client */
    struct aws_s3_client_config client_config = {
        .part_size = PART_SIZE,
    };
    ASSERT_SUCCESS(aws_s3_tester_bind_client(
        &tester->s3_tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION | AWS_S3_TESTER_BIND_CLIENT_SIGNING));
    tester->client = aws_s3_client_new(allocator, &client_config);
    ASSERT_NOT_NULL(tester->client);

    /* Create buffer of data to upload */
    aws_byte_buf_init(&tester->source_buf, allocator, object_size);
    ASSERT_SUCCESS(aws_device_random_buffer(&tester->source_buf));

    /* Create meta request */
    aws_s3_meta_request_test_results_init(&tester->test_results, allocator);

    struct aws_string *host_name =
        aws_s3_tester_build_endpoint_string(allocator, &g_test_bucket_name, &g_test_s3_region);
    struct aws_byte_cursor host_name_cursor = aws_byte_cursor_from_string(host_name);
    struct aws_byte_buf object_path;
    ASSERT_SUCCESS(
        aws_s3_tester_upload_file_path_init(allocator, &object_path, aws_byte_cursor_from_c_str("/asyncwrite.bin")));

    struct aws_http_message *message = aws_s3_test_put_object_request_new_without_body(
        allocator,
        &host_name_cursor,
        g_test_body_content_type,
        aws_byte_cursor_from_buf(&object_path),
        object_size,
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
    ASSERT_SUCCESS(aws_s3_tester_bind_meta_request(&tester->s3_tester, &meta_request_options, &tester->test_results));

    tester->meta_request = aws_s3_client_make_meta_request(tester->client, &meta_request_options);
    ASSERT_NOT_NULL(tester->meta_request);

    /* Clean up tmp variables */
    aws_string_destroy(host_name);
    aws_byte_buf_clean_up(&object_path);
    aws_http_message_release(message);
    return 0;
}

static int s_asyncwrite_tester_validate(struct asyncwrite_tester *tester) {
    ASSERT_SUCCESS(aws_s3_tester_validate_put_object_results(&tester->test_results, 0 /*flags*/));

    /* Validate the checksums, to be we uploaded what we meant to upload */
    ASSERT_TRUE(tester->test_results.upload_review.part_count > 0, "Update this code to handle whole-object checksum");
    struct aws_byte_cursor source_cursor = aws_byte_cursor_from_buf(&tester->source_buf);
    for (size_t part_i = 0; part_i < tester->test_results.upload_review.part_count; ++part_i) {
        /* calculate checksum of this part, from source_buffer */
        uint64_t part_size = tester->test_results.upload_review.part_sizes_array[part_i];
        ASSERT_TRUE(part_size <= source_cursor.len);
        ASSERT_TRUE(part_size < INT_MAX);
        uint32_t crc32_val = aws_checksums_crc32(source_cursor.ptr, (int)part_size, 0x0 /*previousCrc32*/);
        aws_byte_cursor_advance(&source_cursor, (size_t)part_size);

        /* base64-encode the big-endian representation of the CRC32 */
        uint32_t crc32_be_val = aws_hton32(crc32_val);
        struct aws_byte_cursor crc32_be_cursor = {.ptr = (uint8_t *)&crc32_be_val, .len = sizeof(crc32_be_val)};
        struct aws_byte_buf crc32_base64_buf;
        aws_byte_buf_init(&crc32_base64_buf, tester->allocator, 16);
        ASSERT_SUCCESS(aws_base64_encode(&crc32_be_cursor, &crc32_base64_buf));

        /* compare to what got sent */
        struct aws_string *sent_checksum = tester->test_results.upload_review.part_checksums_array[part_i];
        ASSERT_BIN_ARRAYS_EQUALS(
            crc32_base64_buf.buffer, crc32_base64_buf.len, sent_checksum->bytes, sent_checksum->len);

        aws_byte_buf_clean_up(&crc32_base64_buf);
    }
    return 0;
}

static int s_asyncwrite_tester_clean_up(struct asyncwrite_tester *tester) {
    tester->meta_request = aws_s3_meta_request_release(tester->meta_request);
    aws_s3_tester_wait_for_meta_request_shutdown(&tester->s3_tester);
    aws_s3_meta_request_test_results_clean_up(&tester->test_results);
    aws_byte_buf_clean_up(&tester->source_buf);
    tester->client = aws_s3_client_release(tester->client);
    aws_s3_tester_clean_up(&tester->s3_tester);
    return 0;
}

static int s_write(struct asyncwrite_tester *tester, struct aws_byte_cursor data, bool eof) {
    /* use freshly allocated buffer for each write, so that we're likely to get memory violations
     * if this data is used wrong internally. */
    struct aws_byte_buf write_buf;
    aws_byte_buf_init_cache_and_update_cursors(&write_buf, tester->allocator, &data, NULL);

    struct aws_future_void *write_future = aws_s3_meta_request_write(tester->meta_request, data, eof);
    ASSERT_NOT_NULL(write_future);
    ASSERT_TRUE(aws_future_void_wait(write_future, TIMEOUT_NANOS));
    aws_byte_buf_clean_up(&write_buf);
    ASSERT_INT_EQUALS(0, aws_future_void_get_error(write_future));
    aws_future_void_release(write_future);
    return 0;
}

struct basic_asyncwrite_options {
    /* Total size of object to upload */
    size_t object_size;
    /* Max bytes per write(). If zero, defaults to object_size */
    size_t max_bytes_per_write;
    /* If true, EOF is passed in a separate final empty write() */
    bool eof_requires_extra_write;
};

/* Common function for tests that do successful uploads, without too much weird stuff */
static int s_basic_asyncwrite(
    struct aws_allocator *allocator,
    void *ctx,
    const struct basic_asyncwrite_options *options) {

    (void)ctx;
    struct asyncwrite_tester tester;
    ASSERT_SUCCESS(s_asyncwrite_tester_init(&tester, allocator, options->object_size));

    size_t max_bytes_per_write = options->max_bytes_per_write > 0 ? options->max_bytes_per_write : options->object_size;
    bool eof = false;
    struct aws_byte_cursor source_cursor = aws_byte_cursor_from_buf(&tester.source_buf);
    while (source_cursor.len > 0) {
        size_t bytes_to_write = aws_min_size(max_bytes_per_write, source_cursor.len);
        struct aws_byte_cursor write_cursor = aws_byte_cursor_advance(&source_cursor, bytes_to_write);
        if (source_cursor.len == 0 && !options->eof_requires_extra_write) {
            eof = true;
        }

        ASSERT_SUCCESS(s_write(&tester, write_cursor, eof));
    }

    /* Ensure EOF is sent (eof_requires_extra_write, or object_size==0) */
    if (!eof) {
        ASSERT_SUCCESS(s_write(&tester, (struct aws_byte_cursor){0}, true /*eof*/));
    }

    /* Done */
    aws_s3_tester_wait_for_meta_request_finish(&tester.s3_tester);
    ASSERT_SUCCESS(s_asyncwrite_tester_validate(&tester));
    ASSERT_SUCCESS(s_asyncwrite_tester_clean_up(&tester));
    return 0;
};

AWS_TEST_CASE(test_s3_asyncwrite_empty_file, s_test_s3_asyncwrite_empty_file)
static int s_test_s3_asyncwrite_empty_file(struct aws_allocator *allocator, void *ctx) {
    struct basic_asyncwrite_options options = {
        .object_size = 0,
    };
    return s_basic_asyncwrite(allocator, ctx, &options);
}

AWS_TEST_CASE(test_s3_asyncwrite_small_file_1_write, s_test_s3_asyncwrite_small_file_1_write)
static int s_test_s3_asyncwrite_small_file_1_write(struct aws_allocator *allocator, void *ctx) {
    struct basic_asyncwrite_options options = {
        .object_size = 100,
    };
    return s_basic_asyncwrite(allocator, ctx, &options);
}

/* In this test, the 1st write must be buffered, since it's less than part-size */
AWS_TEST_CASE(test_s3_asyncwrite_small_file_1_write_then_eof, s_test_s3_asyncwrite_small_file_1_write_then_eof)
static int s_test_s3_asyncwrite_small_file_1_write_then_eof(struct aws_allocator *allocator, void *ctx) {
    struct basic_asyncwrite_options options = {
        .object_size = 100,
        .eof_requires_extra_write = true,
    };
    return s_basic_asyncwrite(allocator, ctx, &options);
}

/* In this test, we must buffer multiple writes, since their cumulative size is under part-size */
AWS_TEST_CASE(test_s3_asyncwrite_small_file_many_writes, s_test_s3_asyncwrite_small_file_many_writes)
static int s_test_s3_asyncwrite_small_file_many_writes(struct aws_allocator *allocator, void *ctx) {
    struct basic_asyncwrite_options options = {
        .object_size = 100,
        .max_bytes_per_write = 1,
    };
    return s_basic_asyncwrite(allocator, ctx, &options);
}

/* 1 part-sized write */
AWS_TEST_CASE(test_s3_asyncwrite_1_part, s_test_s3_asyncwrite_1_part)
static int s_test_s3_asyncwrite_1_part(struct aws_allocator *allocator, void *ctx) {
    struct basic_asyncwrite_options options = {
        .object_size = PART_SIZE,
    };
    return s_basic_asyncwrite(allocator, ctx, &options);
}

/* Send 1 full part, but spread across many writes.
 * This is just stressing data buffering.  */
AWS_TEST_CASE(test_s3_asyncwrite_1_part_many_writes, s_test_s3_asyncwrite_1_part_many_writes)
static int s_test_s3_asyncwrite_1_part_many_writes(struct aws_allocator *allocator, void *ctx) {
    struct basic_asyncwrite_options options = {
        .object_size = PART_SIZE,
        .max_bytes_per_write = PART_SIZE / 16,
    };
    return s_basic_asyncwrite(allocator, ctx, &options);
}

/* Send 1 full part, then a separate empty EOF write.
 * This probably results in a second (empty) part being uploaded. */
AWS_TEST_CASE(test_s3_asyncwrite_1_part_then_eof, s_test_s3_asyncwrite_1_part_then_eof)
static int s_test_s3_asyncwrite_1_part_then_eof(struct aws_allocator *allocator, void *ctx) {
    struct basic_asyncwrite_options options = {
        .object_size = PART_SIZE,
        .eof_requires_extra_write = true,
    };
    return s_basic_asyncwrite(allocator, ctx, &options);
}

/* Send 2 part-sized writes.
 * This stresses sending multiple parts. */
AWS_TEST_CASE(test_s3_asyncwrite_2_parts_2_partsize_writes, s_test_s3_asyncwrite_2_parts_2_partsize_writes)
static int s_test_s3_asyncwrite_2_parts_2_partsize_writes(struct aws_allocator *allocator, void *ctx) {
    struct basic_asyncwrite_options options = {
        .object_size = PART_SIZE * 2,
        .max_bytes_per_write = PART_SIZE,
    };
    return s_basic_asyncwrite(allocator, ctx, &options);
}

/* Send 1 write, with enough data for 2 full parts.
 * This stresses the case where a single write-future must persist while multiple
 * calls to poll_write() are made under the hood */
AWS_TEST_CASE(test_s3_asyncwrite_2_parts_1_write, s_test_s3_asyncwrite_2_parts_1_write)
static int s_test_s3_asyncwrite_2_parts_1_write(struct aws_allocator *allocator, void *ctx) {
    struct basic_asyncwrite_options options = {
        .object_size = PART_SIZE * 2,
    };
    return s_basic_asyncwrite(allocator, ctx, &options);
}

/* Send 2 full parts, but the first write is larger than part-size.
 * This tests the case where poll_write() can't handle all the data at once,
 * and poll() needs to send the remainder in further calls to poll_write(). */
AWS_TEST_CASE(
    test_s3_asyncwrite_2_parts_first_write_over_partsize,
    s_test_s3_asyncwrite_2_parts_first_write_over_partsize)
static int s_test_s3_asyncwrite_2_parts_first_write_over_partsize(struct aws_allocator *allocator, void *ctx) {
    struct basic_asyncwrite_options options = {
        .object_size = PART_SIZE * 2,
        .max_bytes_per_write = PART_SIZE + 100,
    };
    return s_basic_asyncwrite(allocator, ctx, &options);
}

/* Send 2 full parts, but the first write is less than part-size.
 * This tests the case where both parts contain data from multiple poll_write() calls */
AWS_TEST_CASE(
    test_s3_asyncwrite_2_parts_first_write_under_partsize,
    s_test_s3_asyncwrite_2_parts_first_write_under_partsize)
static int s_test_s3_asyncwrite_2_parts_first_write_under_partsize(struct aws_allocator *allocator, void *ctx) {
    struct basic_asyncwrite_options options = {
        .object_size = PART_SIZE * 2,
        .max_bytes_per_write = PART_SIZE - 100,
    };
    return s_basic_asyncwrite(allocator, ctx, &options);
}

/* We don't explicitly bar empty writes, since it's reasonable to do an empty write with the EOF at the end.
 * Let's make sure we can tolerate empty writes at other arbitrary points. */
AWS_TEST_CASE(test_s3_asyncwrite_tolerate_empty_writes, s_test_s3_asyncwrite_tolerate_empty_writes)
static int s_test_s3_asyncwrite_tolerate_empty_writes(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    struct asyncwrite_tester tester;
    ASSERT_SUCCESS(s_asyncwrite_tester_init(&tester, allocator, PART_SIZE /*object_size*/));

    struct aws_byte_cursor source_cursor = aws_byte_cursor_from_buf(&tester.source_buf);

    /* empty write at start */
    struct aws_byte_cursor empty_data = {0};
    ASSERT_SUCCESS(s_write(&tester, empty_data, false /*eof*/));

    /* write half the data */
    struct aws_byte_cursor next_chunk = aws_byte_cursor_advance(&source_cursor, PART_SIZE / 2);
    ASSERT_SUCCESS(s_write(&tester, next_chunk, false /*eof*/));

    /* empty write in the middle */
    ASSERT_SUCCESS(s_write(&tester, empty_data, false /*eof*/));

    /* write up till we're 1 byte short of a full part */
    next_chunk = aws_byte_cursor_advance(&source_cursor, (PART_SIZE / 2) - 1);
    ASSERT_SUCCESS(s_write(&tester, next_chunk, false /*eof*/));

    /* empty write when we're just 1 byte away from having a full part to send */
    ASSERT_SUCCESS(s_write(&tester, empty_data, false /*eof*/));

    /* write final byte, but don't send EOF yet */
    next_chunk = aws_byte_cursor_advance(&source_cursor, 1);
    ASSERT_SUCCESS(s_write(&tester, next_chunk, false /*eof*/));

    /* empty write at the end, but don't send EOF yet */
    ASSERT_SUCCESS(s_write(&tester, empty_data, false /*eof*/));

    /* OK, finally send EOF */
    ASSERT_SUCCESS(s_write(&tester, empty_data, true /*eof*/));

    /* Done */
    aws_s3_tester_wait_for_meta_request_finish(&tester.s3_tester);
    ASSERT_SUCCESS(s_asyncwrite_tester_validate(&tester));
    ASSERT_SUCCESS(s_asyncwrite_tester_clean_up(&tester));
    return 0;
}

struct asyncwrite_on_another_thread_ctx {
    struct asyncwrite_tester *tester;
    size_t max_bytes_per_write;
    struct aws_byte_cursor source_cursor;
    struct aws_future_void *write_future;
};

static void s_write_from_future_callback(void *user_data) {
    struct asyncwrite_on_another_thread_ctx *thread_ctx = user_data;

    /* If there was a previous write, assert it succeeded */
    if (thread_ctx->write_future != NULL) {
        AWS_FATAL_ASSERT(aws_future_void_get_error(thread_ctx->write_future) == 0);
        thread_ctx->write_future = aws_future_void_release(thread_ctx->write_future);
    }

    /* If that was the final write, we're done */
    if (thread_ctx->source_cursor.len == 0) {
        return;
    }

    /* Write next chunk */
    size_t bytes_to_write = aws_min_size(thread_ctx->source_cursor.len, thread_ctx->max_bytes_per_write);
    struct aws_byte_cursor next_chunk = aws_byte_cursor_advance(&thread_ctx->source_cursor, bytes_to_write);
    bool eof = thread_ctx->source_cursor.len == 0;
    thread_ctx->write_future = aws_s3_meta_request_write(thread_ctx->tester->meta_request, next_chunk, eof);

    /* Register this function to run again when write completes */
    aws_future_void_register_callback(thread_ctx->write_future, s_write_from_future_callback, thread_ctx);
}

/* This test tries to submit new writes from the write-future's completion callback,
 * which often fires on another thread. */
AWS_TEST_CASE(test_s3_asyncwrite_write_from_future_callback, s_test_s3_asyncwrite_write_from_future_callback)
static int s_test_s3_asyncwrite_write_from_future_callback(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    struct asyncwrite_tester tester;

    /* Have a few parts, so we get more chances to write from a callback on another thread */
    ASSERT_SUCCESS(s_asyncwrite_tester_init(&tester, allocator, PART_SIZE * 4 /*object_size*/));

    struct asyncwrite_on_another_thread_ctx on_another_thread_ctx = {
        .tester = &tester,
        .source_cursor = aws_byte_cursor_from_buf(&tester.source_buf),
        /* Use writes that don't divide nicely into part-size.
         * This way, we're passing some buffered data, and some unbuffered data, to each part.
         * And getting back some buffered leftovers when the part completes.
         * This pushes a lot of edge cases, and makes it likely we'll catch any threading bugs. */
        .max_bytes_per_write = (PART_SIZE / 2) + 1,
    };

    /* Kick off the recursive write-future completion callback loop */
    s_write_from_future_callback(&on_another_thread_ctx);

    /* Done */
    aws_s3_tester_wait_for_meta_request_finish(&tester.s3_tester);
    ASSERT_SUCCESS(s_asyncwrite_tester_validate(&tester));
    ASSERT_SUCCESS(s_asyncwrite_tester_clean_up(&tester));
    return 0;
}

/* This tests checks that, if the meta request fails before write() is called,
 * the the write-future fails with AWS_ERROR_S3_REQUEST_HAS_COMPLETED */
AWS_TEST_CASE(test_s3_asyncwrite_fails_if_request_has_completed, s_test_s3_asyncwrite_fails_if_request_has_completed)
static int s_test_s3_asyncwrite_fails_if_request_has_completed(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    struct asyncwrite_tester tester;

    ASSERT_SUCCESS(s_asyncwrite_tester_init(&tester, allocator, PART_SIZE /*object_size*/));

    /* Cancel meta request before write() call */
    aws_s3_meta_request_cancel(tester.meta_request);

    struct aws_future_void *write_future =
        aws_s3_meta_request_write(tester.meta_request, aws_byte_cursor_from_buf(&tester.source_buf), true /*eof*/);

    ASSERT_TRUE(aws_future_void_wait(write_future, TIMEOUT_NANOS));

    ASSERT_INT_EQUALS(AWS_ERROR_S3_REQUEST_HAS_COMPLETED, aws_future_void_get_error(write_future));
    write_future = aws_future_void_release(write_future);

    /* Done */
    aws_s3_tester_wait_for_meta_request_finish(&tester.s3_tester);

    /* The meta request's error-code should still be CANCELED, the failed write() shouldn't affect that */
    ASSERT_INT_EQUALS(AWS_ERROR_S3_CANCELED, tester.test_results.finished_error_code);

    ASSERT_SUCCESS(s_asyncwrite_tester_clean_up(&tester));
    return 0;
}

AWS_TEST_CASE(test_s3_asyncwrite_fails_if_write_after_eof, s_test_s3_asyncwrite_fails_if_write_after_eof)
static int s_test_s3_asyncwrite_fails_if_write_after_eof(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    struct asyncwrite_tester tester;

    ASSERT_SUCCESS(s_asyncwrite_tester_init(&tester, allocator, PART_SIZE /*object_size*/));

    /* Write the whole object, with EOF */
    ASSERT_SUCCESS(s_write(&tester, aws_byte_cursor_from_buf(&tester.source_buf), true /*eof*/));

    /* Any more writes should fail with INVALID_STATE error */
    struct aws_byte_cursor empty_cursor = {0};
    struct aws_future_void *write_future = aws_s3_meta_request_write(tester.meta_request, empty_cursor, true /*eof*/);
    ASSERT_TRUE(aws_future_void_wait(write_future, TIMEOUT_NANOS));
    ASSERT_INT_EQUALS(AWS_ERROR_INVALID_STATE, aws_future_void_get_error(write_future));
    write_future = aws_future_void_release(write_future);

    /* Done. Don't really care if the request completes successfully or not */
    aws_s3_tester_wait_for_meta_request_finish(&tester.s3_tester);
    ASSERT_SUCCESS(s_asyncwrite_tester_clean_up(&tester));
    return 0;
}

AWS_TEST_CASE(test_s3_asyncwrite_fails_if_writes_overlap, s_test_s3_asyncwrite_fails_if_writes_overlap)
static int s_test_s3_asyncwrite_fails_if_writes_overlap(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    struct asyncwrite_tester tester;

    /* Make it VERY likely that some writes will overlap by issuing a lot of them as fast as possible */
    enum { num_writes = 100 };
    ASSERT_SUCCESS(s_asyncwrite_tester_init(&tester, allocator, num_writes * PART_SIZE /*object_size*/));

    bool had_overlapping_write = false;
    struct aws_byte_cursor source_cursor = aws_byte_cursor_from_buf(&tester.source_buf);
    bool eof = false;
    while (!eof && !had_overlapping_write) {
        struct aws_byte_cursor write_cursor = aws_byte_cursor_advance(&source_cursor, PART_SIZE);
        eof = (source_cursor.len == 0);
        struct aws_future_void *write_future = aws_s3_meta_request_write(tester.meta_request, write_cursor, eof);
        int write_error_code = aws_future_void_is_done(write_future) ? aws_future_void_get_error(write_future) : 0;
        aws_future_void_release(write_future);

        if (write_error_code != 0) {
            /* INVALID_STATE is the error code for overlapping writes */
            ASSERT_INT_EQUALS(AWS_ERROR_INVALID_STATE, write_error_code);
            had_overlapping_write = true;
        }
    }

    ASSERT_TRUE(had_overlapping_write);

    /* Any error from the write() call should result in the meta request terminating with INVALID_STATE error */
    aws_s3_tester_wait_for_meta_request_finish(&tester.s3_tester);
    ASSERT_INT_EQUALS(AWS_ERROR_INVALID_STATE, tester.test_results.finished_error_code);

    ASSERT_SUCCESS(s_asyncwrite_tester_clean_up(&tester));
    return 0;
}

static int s_wait_for_sub_request_to_send(
    struct asyncwrite_tester *tester,
    enum aws_s3_request_type request_type,
    uint64_t timeout) {

    uint64_t now;
    ASSERT_SUCCESS(aws_high_res_clock_get_ticks(&now));
    const uint64_t timeout_timestamp = now + timeout;
    const uint64_t sleep_between_checks = aws_timestamp_convert(100, AWS_TIMESTAMP_MILLIS, AWS_TIMESTAMP_NANOS, NULL);

    bool request_sent = false;
    while (!request_sent) {
        aws_s3_tester_lock_synced_data(&tester->s3_tester);
        for (size_t i = 0; i < aws_array_list_length(&tester->test_results.synced_data.metrics); ++i) {
            struct aws_s3_request_metrics *metrics = NULL;
            ASSERT_SUCCESS(aws_array_list_get_at(&tester->test_results.synced_data.metrics, (void **)&metrics, i));
            enum aws_s3_request_type request_type_i;
            aws_s3_request_metrics_get_request_type(metrics, &request_type_i);
            if (request_type_i == request_type) {
                if (aws_s3_request_metrics_get_error_code(metrics) == 0) {
                    request_sent = true;
                }
            }
        }
        aws_s3_tester_unlock_synced_data(&tester->s3_tester);

        if (!request_sent) {
            /* Check for timeout, then sleep a bit before checking again */
            ASSERT_SUCCESS(aws_high_res_clock_get_ticks(&now));
            ASSERT_TRUE(
                now < timeout_timestamp,
                "Timed out waiting for %s to be sent",
                aws_s3_request_type_operation_name(request_type));
            aws_thread_current_sleep(sleep_between_checks);
        }
    }
    return 0;
}

/* Test that aws_s3_meta_request_cancel() will result in AbortMultipartUpload being sent.
 * This is a regression test: once upon a time cancel() forgot to trigger the client's update(),
 * and so the meta-request would hang until something else kicked the update loop. */
AWS_TEST_CASE(test_s3_asyncwrite_cancel_sends_abort, s_test_s3_asyncwrite_cancel_sends_abort)
static int s_test_s3_asyncwrite_cancel_sends_abort(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct asyncwrite_tester tester;
    ASSERT_SUCCESS(s_asyncwrite_tester_init(&tester, allocator, PART_SIZE * 3 /*object_size*/));

    const uint64_t one_sec_in_nanos = aws_timestamp_convert(1, AWS_TIMESTAMP_SECS, AWS_TIMESTAMP_NANOS, NULL);

    /* Wait for StartMultipartUpload to be sent */
    ASSERT_SUCCESS(s_wait_for_sub_request_to_send(
        &tester, AWS_S3_REQUEST_TYPE_CREATE_MULTIPART_UPLOAD, 10 * one_sec_in_nanos /*timeout*/));

    /* Sleep a bit to ensure the client isn't doing anything, then cancel() */
    aws_thread_current_sleep(one_sec_in_nanos);

    aws_s3_meta_request_cancel(tester.meta_request);

    /* Wait for AbortMultipartUpload to be sent.
     * Ugh if timeout is too long we risk some unrelated system updating the client and hiding this bug.
     * But if timeout is too short CI will randomly fail on super slow system. */
    ASSERT_SUCCESS(s_wait_for_sub_request_to_send(
        &tester, AWS_S3_REQUEST_TYPE_ABORT_MULTIPART_UPLOAD, 5 * one_sec_in_nanos /*timeout*/));

    /* Wait for meta request to complete */
    aws_s3_tester_wait_for_meta_request_finish(&tester.s3_tester);

    ASSERT_INT_EQUALS(AWS_ERROR_S3_CANCELED, tester.test_results.finished_error_code);

    ASSERT_SUCCESS(s_asyncwrite_tester_clean_up(&tester));
    return 0;
}
