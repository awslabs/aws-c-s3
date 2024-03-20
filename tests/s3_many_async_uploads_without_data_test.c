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
/* TODO: when we come up with a real fix, increase to 1000 for all cases.
 * But for now the memory_limit_in_bytes limits us, and it has a different default value for 32 and 64 bit */
#if SIZE_BITS == 32
#    define MANY_ASYNC_UPLOADS_COUNT 80
#else
#    define MANY_ASYNC_UPLOADS_COUNT 200
#endif

/* Number of bytes each meta-request should upload (small so this this doesn't take forever) */
#define MANY_ASYNC_UPLOADS_OBJECT_SIZE 1

/* How long to spend doing nothing, before assuming we're deadlocked */
#define SEND_DATA_TIMEOUT_NANOS ((uint64_t)AWS_TIMESTAMP_NANOS * 10) /* 10secs */

/* Singleton struct for this test, containing anything touched by helper functions.
 * Lock must be held while touching anything in here */
static struct many_async_uploads_test_data {
    struct aws_mutex mutex;

    /* This cvar is notified whenever async-input-stream read() is called
     * (at least one index of async_buffers[] or async_futures[] will be non-null) */
    struct aws_condition_variable cvar;

    /* The main thread waits on the cvar until async-input-stream read() is
     * called for this meta-request */
    int waiting_on_upload_i;

    /* For each upload i: dest buffer from any pending async-input-stream read() */
    struct aws_byte_buf *async_buffers[MANY_ASYNC_UPLOADS_COUNT];

    /* For each upload i: future from any pending async-input-stream read() */
    struct aws_future_bool *async_futures[MANY_ASYNC_UPLOADS_COUNT];

    /* For each upload i: bytes uploaded so far */
    uint64_t bytes_uploaded[MANY_ASYNC_UPLOADS_COUNT];

} s_many_async_uploads_test_data;

/* async-input-stream for this test */
struct many_async_uploads_stream {
    struct aws_async_input_stream base;
    int upload_i;
};

static void s_many_async_uploads_stream_destroy(struct aws_async_input_stream *stream) {
    struct many_async_uploads_stream *stream_impl = stream->impl;
    aws_mem_release(stream->alloc, stream_impl);
}

static struct aws_future_bool *s_many_async_uploads_stream_read(
    struct aws_async_input_stream *stream,
    struct aws_byte_buf *dest) {

    struct many_async_uploads_stream *stream_impl = stream->impl;
    struct aws_future_bool *future = aws_future_bool_new(stream->alloc);
    struct many_async_uploads_test_data *test_data = &s_many_async_uploads_test_data;

    /* Store the buffer and future */
    aws_mutex_lock(&test_data->mutex);

    AWS_FATAL_ASSERT(test_data->async_buffers[stream_impl->upload_i] == NULL);
    test_data->async_buffers[stream_impl->upload_i] = dest;

    AWS_FATAL_ASSERT(test_data->async_futures[stream_impl->upload_i] == NULL);
    test_data->async_futures[stream_impl->upload_i] = aws_future_bool_acquire(future);

    /* Alert the main thread that it may complete this async read */
    aws_condition_variable_notify_all(&test_data->cvar);
    aws_mutex_unlock(&s_many_async_uploads_test_data.mutex);

    return future;
}

static const struct aws_async_input_stream_vtable s_many_async_uploads_stream_vtable = {
    .destroy = s_many_async_uploads_stream_destroy,
    .read = s_many_async_uploads_stream_read,
};

static struct aws_async_input_stream *s_many_async_uploads_stream_new(struct aws_allocator *allocator, int upload_i) {
    struct many_async_uploads_stream *stream_impl =
        aws_mem_calloc(allocator, 1, sizeof(struct many_async_uploads_stream));
    aws_async_input_stream_init_base(&stream_impl->base, allocator, &s_many_async_uploads_stream_vtable, stream_impl);
    stream_impl->upload_i = upload_i;
    return &stream_impl->base;
}

/* Return true if the desired meta-request is able to send data */
static bool s_waiting_on_upload_i_predicate(void *user_data) {
    (void)user_data;
    struct many_async_uploads_test_data *test_data = &s_many_async_uploads_test_data;
    return test_data->async_buffers[test_data->waiting_on_upload_i] != NULL;
}

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

    struct many_async_uploads_test_data *test_data = &s_many_async_uploads_test_data;
    aws_mutex_init(&test_data->mutex);
    aws_condition_variable_init(&test_data->cvar);

    // struct aws_s3_meta_request *meta_requests[MANY_ASYNC_UPLOADS_COUNT];
    struct aws_s3_meta_request_test_results meta_request_test_results[MANY_ASYNC_UPLOADS_COUNT];

    /* Create N upload meta-requests, each with an async-input-stream that
     * won't provide data until later in this test... */
    for (int i = 0; i < MANY_ASYNC_UPLOADS_COUNT; ++i) {
        struct aws_async_input_stream *async_stream = s_many_async_uploads_stream_new(allocator, i);

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
            .send_async_stream = async_stream,
            /* TODO: come up with a real fix, this "internal_use_only" setting is just a temporary workaround.
             * that lets us deal with 200+ "stalled" meta-requests. The client still deadlocks if you
             * increase MANY_ASYNC_UPLOADS_COUNT to 1000. */
            .maximize_async_stream_reads_internal_use_only = true,
        };
        ASSERT_SUCCESS(aws_s3_tester_bind_meta_request(&tester, &options, &meta_request_test_results[i]));

        struct aws_s3_meta_request *meta_request = aws_s3_client_make_meta_request(client, &options);

        /* Release stuff created in this loop.
         * The s3_client will keep everything related to the meta-request alive until it completes */
        aws_string_destroy(host_name);
        aws_byte_buf_clean_up(&object_path);
        aws_http_message_release(message);
        aws_async_input_stream_release(async_stream);
        aws_s3_meta_request_release(meta_request);
    }

    /* Starting at the end, and working backwards, only provide data to one meta-request at a time. */
    for (int i = MANY_ASYNC_UPLOADS_COUNT - 1; i >= 0; --i) {
        bool upload_done = false;

        while (!upload_done) {
            aws_mutex_lock(&test_data->mutex);
            test_data->waiting_on_upload_i = i;

            /* Wait until meta-request i's async-input-stream read() is called */
            ASSERT_SUCCESS(
                aws_condition_variable_wait_for_pred(
                    &test_data->cvar,
                    &test_data->mutex,
                    SEND_DATA_TIMEOUT_NANOS,
                    s_waiting_on_upload_i_predicate,
                    NULL),
                "Timed out waiting to send data on upload %d/%d",
                i + 1,
                MANY_ASYNC_UPLOADS_COUNT);

            /* OK, send data for meta-request i */
            struct aws_byte_buf *dest = test_data->async_buffers[i];
            test_data->async_buffers[i] = NULL;

            struct aws_future_bool *future = test_data->async_futures[i];
            test_data->async_futures[i] = NULL;

            size_t space_available = dest->capacity - dest->len;
            uint64_t bytes_remaining = MANY_ASYNC_UPLOADS_OBJECT_SIZE - test_data->bytes_uploaded[i];
            size_t bytes_to_send = (size_t)aws_min_u64(space_available, bytes_remaining);
            ASSERT_TRUE(aws_byte_buf_write_u8_n(dest, 'z', bytes_to_send));
            test_data->bytes_uploaded[i] += bytes_to_send;
            upload_done = test_data->bytes_uploaded[i] == MANY_ASYNC_UPLOADS_OBJECT_SIZE;
            aws_mutex_unlock(&test_data->mutex);

            aws_future_bool_set_result(future, upload_done);
            aws_future_bool_release(future);
        }
    }

    /* Wait for everything to finish */
    aws_s3_tester_wait_for_meta_request_finish(&tester);
    aws_s3_tester_wait_for_meta_request_shutdown(&tester);

    for (int i = 0; i < MANY_ASYNC_UPLOADS_COUNT; ++i) {
        aws_s3_tester_validate_put_object_results(&meta_request_test_results[i], 0 /*flags*/);
        aws_s3_meta_request_test_results_clean_up(&meta_request_test_results[i]);
    }

    /* Cleanup */
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);
    aws_condition_variable_clean_up(&test_data->cvar);
    aws_mutex_clean_up(&test_data->mutex);

    return 0;
}
