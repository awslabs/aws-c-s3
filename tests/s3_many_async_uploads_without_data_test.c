/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */
#include "s3_tester.h"

#include <aws/common/clock.h>
#include <aws/s3/private/s3_util.h>
#include <aws/testing/aws_test_harness.h>

/**
 * Regression test for deadlock discovered by a user of Mountpoint (which wraps aws-c-s3
 * with a filesystem-like API). The user opened MANY files at once.
 * The user wrote data to some of the later files they opened,
 * and waited for those writes to complete.
 * But aws-c-s3 was waiting on data from the first few files.
 * Both sides were waiting on each other. It was a deadlock.
 *
 * This test starts N upload meta-requests.
 * Then, it only sends data to 1 meta-request at a time, starting with the last
 * meta-request it created, and working backwards to the first.
 * If the test times out, then we still suffer from the deadlock.
 */

/* Number of simultaneous upload meta-requests to create */
#define MANY_ASYNC_UPLOADS_COUNT 200

/* Number of bytes each meta-request should upload (small so this this doesn't take forever) */
#define MANY_ASYNC_UPLOADS_OBJECT_SIZE 100

/* Bytes per write */
#define MANY_ASYNC_UPLOADS_BYTES_PER_WRITE 10

/* How long to spend doing nothing, before assuming we're deadlocked */
#define SEND_DATA_TIMEOUT_NANOS ((uint64_t)AWS_TIMESTAMP_NANOS * 10) /* 10secs */

/* See top of file for full description of what's going on in this test. */
AWS_TEST_CASE(test_s3_many_async_uploads_without_data, s_test_s3_many_async_uploads_without_data)
static int s_test_s3_many_async_uploads_without_data(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    /* Set up */
    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client *client = NULL;
    struct aws_s3_tester_client_options client_options;
    AWS_ZERO_STRUCT(client_options);
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_s3_meta_request *meta_requests[MANY_ASYNC_UPLOADS_COUNT];
    struct aws_s3_meta_request_test_results meta_request_test_results[MANY_ASYNC_UPLOADS_COUNT];

    /* Create N upload meta-requests, each with an async-input-stream that
     * won't provide data until later in this test... */
    for (int i = 0; i < MANY_ASYNC_UPLOADS_COUNT; ++i) {
        aws_s3_meta_request_test_results_init(&meta_request_test_results[i], allocator);

        struct aws_string *host_name =
            aws_s3_tester_build_endpoint_string(allocator, &g_test_bucket_name, &g_test_s3_region);
        struct aws_byte_cursor host_name_cursor = aws_byte_cursor_from_string(host_name);

        char object_name[128] = {0};
        snprintf(object_name, sizeof(object_name), "/many-async-uploads-%d.txt", i);
        struct aws_byte_buf object_path;
        ASSERT_SUCCESS(
            aws_s3_tester_upload_file_path_init(allocator, &object_path, aws_byte_cursor_from_c_str(object_name)));

        struct aws_http_message *message = aws_s3_test_put_object_request_new_without_body(
            allocator,
            &host_name_cursor,
            g_test_body_content_type,
            aws_byte_cursor_from_buf(&object_path),
            MANY_ASYNC_UPLOADS_OBJECT_SIZE,
            0 /*flags*/);

        /* Erase content-length header, because Mountpoint always uploads with unknown content-length */
        aws_http_headers_erase(aws_http_message_get_headers(message), g_content_length_header_name);

        struct aws_s3_meta_request_options options = {
            .type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
            .message = message,
            .send_using_async_writes = true,
        };
        ASSERT_SUCCESS(aws_s3_tester_bind_meta_request(&tester, &options, &meta_request_test_results[i]));

        meta_requests[i] = aws_s3_client_make_meta_request(client, &options);

        /* Release stuff created in this loop */
        aws_string_destroy(host_name);
        aws_byte_buf_clean_up(&object_path);
        aws_http_message_release(message);
    }

    /* Starting at the end, and working backwards, only provide data to one meta-request at a time. */
    for (int i = MANY_ASYNC_UPLOADS_COUNT - 1; i >= 0; --i) {

        struct aws_s3_meta_request *meta_request_i = meta_requests[i];

        /* Perform sequential writes to meta_request_i, until EOF */
        size_t bytes_written = 0;
        bool eof = false;
        while (!eof) {
            size_t bytes_to_write =
                aws_min_size(MANY_ASYNC_UPLOADS_BYTES_PER_WRITE, MANY_ASYNC_UPLOADS_OBJECT_SIZE - bytes_written);

            eof = (bytes_written + bytes_to_write) == MANY_ASYNC_UPLOADS_OBJECT_SIZE;

            /* use freshly allocated buffer for each write, so that we're likely to get memory violations
             * if this data is used wrong internally. */
            struct aws_byte_buf tmp_data;
            aws_byte_buf_init(&tmp_data, allocator, bytes_to_write);
            aws_byte_buf_write_u8_n(&tmp_data, 'z', bytes_to_write);

            struct aws_future_void *write_future =
                aws_s3_meta_request_write(meta_request_i, aws_byte_cursor_from_buf(&tmp_data), eof);

            ASSERT_TRUE(
                aws_future_void_wait(write_future, SEND_DATA_TIMEOUT_NANOS),
                "Timed out waiting to send data on upload %d/%d."
                " After writing %zu bytes, timed out on write(data=%zu, eof=%d)",
                i + 1,
                MANY_ASYNC_UPLOADS_COUNT,
                bytes_written,
                bytes_to_write,
                eof);

            /* write complete! */
            aws_byte_buf_clean_up(&tmp_data);

            ASSERT_INT_EQUALS(0, aws_future_void_get_error(write_future));
            aws_future_void_release(write_future);

            bytes_written += bytes_to_write;
        }
    }

    /* Wait for everything to finish */
    for (int i = 0; i < MANY_ASYNC_UPLOADS_COUNT; ++i) {
        meta_requests[i] = aws_s3_meta_request_release(meta_requests[i]);
    }

    aws_s3_tester_wait_for_meta_request_finish(&tester);
    aws_s3_tester_wait_for_meta_request_shutdown(&tester);

    for (int i = 0; i < MANY_ASYNC_UPLOADS_COUNT; ++i) {
        aws_s3_tester_validate_put_object_results(&meta_request_test_results[i], 0 /*flags*/);
        aws_s3_meta_request_test_results_clean_up(&meta_request_test_results[i]);
    }

    /* Cleanup */
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return 0;
}
