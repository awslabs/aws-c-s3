/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_client_impl.h"
#include "aws/s3/private/s3_request.h"
#include "aws/s3/private/s3_util.h"
#include "s3_tester.h"

#include <aws/common/clock.h>
#include <aws/testing/aws_test_harness.h>

#define TEST_CASE(NAME)                                                                                                \
    AWS_TEST_CASE(NAME, s_test_##NAME);                                                                                \
    static int s_test_##NAME(struct aws_allocator *allocator, void *ctx)

#define DEFINE_HEADER(NAME, VALUE)                                                                                     \
    {                                                                                                                  \
        .name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL(NAME),                                                           \
        .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL(VALUE),                                                         \
    }

static void s_init_mock_s3_request_upload_part_timeout(
    struct aws_s3_request *mock_request,
    uint64_t original_upload_timeout_ms,
    uint64_t request_time_ns,
    uint64_t response_to_first_byte_time_ns) {
    mock_request->upload_timeout_ms = (size_t)original_upload_timeout_ms;
    struct aws_s3_request_metrics *metrics = mock_request->send_data.metrics;

    metrics->time_metrics.send_start_timestamp_ns = 0;
    metrics->time_metrics.send_end_timestamp_ns = 0;
    metrics->time_metrics.receive_end_timestamp_ns = request_time_ns;
    metrics->time_metrics.receive_start_timestamp_ns = response_to_first_byte_time_ns;
}

static int s_starts_upload_retry(struct aws_s3_client *client, struct aws_s3_request *mock_request) {
    uint64_t average_time_ns = aws_timestamp_convert(
        300, AWS_TIMESTAMP_MILLIS, AWS_TIMESTAMP_NANOS, NULL); /* 0.3 Secs, average for upload a part */
    AWS_ZERO_STRUCT(client->synced_data.upload_part_stats);

    s_init_mock_s3_request_upload_part_timeout(mock_request, 0, average_time_ns, average_time_ns);
    size_t init_count = client->ideal_connection_count;
    size_t p90_count = init_count / 10 + 1;
    for (size_t i = 0; i < init_count - p90_count; i++) {
        /* With 90% of the average request time. */
        aws_s3_client_update_upload_part_timeout(client, mock_request, AWS_ERROR_SUCCESS);
    }

    uint64_t one_sec_time_ns = aws_timestamp_convert(1, AWS_TIMESTAMP_SECS, AWS_TIMESTAMP_NANOS, NULL); /* 1 Secs */
    s_init_mock_s3_request_upload_part_timeout(mock_request, 0, one_sec_time_ns, one_sec_time_ns);
    for (size_t i = 0; i < p90_count; i++) {
        /* 10 percent of the request takes 1 sec */
        aws_s3_client_update_upload_part_timeout(client, mock_request, AWS_ERROR_SUCCESS);
    }
    /* Check that retry should be turned off */
    ASSERT_FALSE(client->synced_data.upload_part_stats.stop_timeout);
    size_t current_timeout_ms = aws_atomic_load_int(&client->upload_timeout_ms);
    /* We start the retry with a default 1 sec timeout */
    ASSERT_UINT_EQUALS(1000, current_timeout_ms);
    return AWS_OP_SUCCESS;
}

/* Test the aws_s3_client_update_upload_part_timeout works as expected */
TEST_CASE(client_update_upload_part_timeout) {
    (void)ctx;
    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_client *client = NULL;
    struct aws_s3_tester_client_options client_options = {
        .part_size = MB_TO_BYTES(8),
        .tls_usage = AWS_S3_TLS_DISABLED,
    };
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));
    struct aws_s3_request mock_request;
    struct aws_s3_request_metrics metrics;
    AWS_ZERO_STRUCT(mock_request);
    AWS_ZERO_STRUCT(metrics);
    mock_request.send_data.metrics = &metrics;

    uint64_t large_time_ns =
        aws_timestamp_convert(5500, AWS_TIMESTAMP_MILLIS, AWS_TIMESTAMP_NANOS, NULL); /* 5.5 Secs, larger than 5 secs */

    uint64_t average_time_ns = aws_timestamp_convert(
        250, AWS_TIMESTAMP_MILLIS, AWS_TIMESTAMP_NANOS, NULL); /* 0.25 Secs, close to average for upload a part */

    size_t init_count = client->ideal_connection_count;
    {
        /* 1. If the request time is larger than 5 secs, we don't do retry */
        AWS_ZERO_STRUCT(client->synced_data.upload_part_stats);
        s_init_mock_s3_request_upload_part_timeout(&mock_request, 0, large_time_ns, average_time_ns);

        /* If request timeout happened before the retry started, it has no effects. */
        aws_s3_client_update_upload_part_timeout(client, &mock_request, AWS_ERROR_HTTP_RESPONSE_FIRST_BYTE_TIMEOUT);
        for (size_t i = 0; i < init_count; i++) {
            /* Mock a number of requests completed with the large time for the request */
            aws_s3_client_update_upload_part_timeout(client, &mock_request, AWS_ERROR_SUCCESS);
        }

        ASSERT_TRUE(client->synced_data.upload_part_stats.stop_timeout);
        size_t current_timeout_ms = aws_atomic_load_int(&client->upload_timeout_ms);
        ASSERT_UINT_EQUALS(0, current_timeout_ms);
        /* clean up */
        if (client->synced_data.upload_part_stats.initial_request_time.collecting_p90) {
            aws_priority_queue_clean_up(&client->synced_data.upload_part_stats.initial_request_time.p90_samples);
            client->synced_data.upload_part_stats.initial_request_time.collecting_p90 = false;
        }
    }
    {
        /* 2.1. Test that the P90 of the init samples are used correctly and at least 1 sec */
        AWS_ZERO_STRUCT(client->synced_data.upload_part_stats);
        /* Hack around to set the ideal connection time for testing. */
        size_t test_init_connection = 1000;
        *(uint32_t *)(void *)&client->ideal_connection_count = (uint32_t)test_init_connection;
        for (size_t i = 0; i < test_init_connection; i++) {
            /* Mock a number of requests completed with the large time for the request */
            uint64_t time_ns = aws_timestamp_convert(i, AWS_TIMESTAMP_MILLIS, AWS_TIMESTAMP_NANOS, NULL);
            s_init_mock_s3_request_upload_part_timeout(&mock_request, 0, time_ns, time_ns);
            aws_s3_client_update_upload_part_timeout(client, &mock_request, AWS_ERROR_SUCCESS);
        }

        size_t current_timeout_ms = aws_atomic_load_int(&client->upload_timeout_ms);
        /* the P90 of the results is 900, but it has to be at least 1000 */
        ASSERT_UINT_EQUALS(1000, current_timeout_ms);
        /* clean up */
        if (client->synced_data.upload_part_stats.initial_request_time.collecting_p90) {
            aws_priority_queue_clean_up(&client->synced_data.upload_part_stats.initial_request_time.p90_samples);
            client->synced_data.upload_part_stats.initial_request_time.collecting_p90 = false;
        }
        /* Change it back */
        *(uint32_t *)(void *)&client->ideal_connection_count = (uint32_t)init_count;
    }
    {
        /* 2.2. Test that the P90 of the init samples are used correctly */
        AWS_ZERO_STRUCT(client->synced_data.upload_part_stats);
        /* Hack around to set the ideal connection time for testing. */
        size_t test_init_connection = 10000;
        *(uint32_t *)(void *)&client->ideal_connection_count = (uint32_t)test_init_connection;
        for (size_t i = 0; i < test_init_connection; i++) {
            /* Mock a number of requests completed with the large time for the request */
            uint64_t time_ns = aws_timestamp_convert(i, AWS_TIMESTAMP_MILLIS, AWS_TIMESTAMP_NANOS, NULL);
            s_init_mock_s3_request_upload_part_timeout(&mock_request, 0, time_ns, time_ns);
            aws_s3_client_update_upload_part_timeout(client, &mock_request, AWS_ERROR_SUCCESS);
        }

        size_t current_timeout_ms = aws_atomic_load_int(&client->upload_timeout_ms);
        /* P90 is 9000 */
        ASSERT_UINT_EQUALS(9000, current_timeout_ms);
        /* clean up */
        if (client->synced_data.upload_part_stats.initial_request_time.collecting_p90) {
            aws_priority_queue_clean_up(&client->synced_data.upload_part_stats.initial_request_time.p90_samples);
            client->synced_data.upload_part_stats.initial_request_time.collecting_p90 = false;
        }
        /* Change it back */
        *(uint32_t *)(void *)&client->ideal_connection_count = (uint32_t)init_count;
    }

    {
        ASSERT_SUCCESS(s_starts_upload_retry(client, &mock_request));
        /**
         * 3. Once a request finishes without timeout, use the average response_to_first_byte_time +
         *      g_expect_timeout_offset_ms as our expected timeout. (TODO: The real expected timeout should be a P99 of
         *      all the requests.)
         *  3.1 Adjust the current timeout against the expected timeout, via 0.99 * <current timeout> + 0.01 * <expected
         *      timeout> to get closer to the expected timeout.
         */
        s_init_mock_s3_request_upload_part_timeout(
            &mock_request,
            aws_timestamp_convert(1, AWS_TIMESTAMP_SECS, AWS_TIMESTAMP_MILLIS, NULL),
            average_time_ns,
            average_time_ns);

        /* After 1000 runs, we have the timeout match the "expected" (average time + g_expect_timeout_offset_ms) timeout
         */
        for (size_t i = 0; i < 1000; i++) {
            /* Mock a number of requests completed with the large time for the request */
            aws_s3_client_update_upload_part_timeout(client, &mock_request, AWS_ERROR_SUCCESS);
        }
        size_t current_timeout_ms = aws_atomic_load_int(&client->upload_timeout_ms);
        ASSERT_UINT_EQUALS(
            aws_timestamp_convert(average_time_ns, AWS_TIMESTAMP_NANOS, AWS_TIMESTAMP_MILLIS, NULL) +
                g_expect_timeout_offset_ms,
            current_timeout_ms);

        /* will not change after another 1k run */
        for (size_t i = 0; i < 1000; i++) {
            /* Mock a number of requests completed with the large time for the request */
            aws_s3_client_update_upload_part_timeout(client, &mock_request, AWS_ERROR_SUCCESS);
        }
        ASSERT_FALSE(client->synced_data.upload_part_stats.stop_timeout);
        current_timeout_ms = aws_atomic_load_int(&client->upload_timeout_ms);
        /* After 1000 runs, we have the timeout match the "expected" (average time + g_expect_timeout_offset_ms) timeout
         */
        ASSERT_UINT_EQUALS(
            aws_timestamp_convert(average_time_ns, AWS_TIMESTAMP_NANOS, AWS_TIMESTAMP_MILLIS, NULL) +
                g_expect_timeout_offset_ms,
            current_timeout_ms);
        /* clean up */
        if (client->synced_data.upload_part_stats.initial_request_time.collecting_p90) {
            aws_priority_queue_clean_up(&client->synced_data.upload_part_stats.initial_request_time.p90_samples);
            client->synced_data.upload_part_stats.initial_request_time.collecting_p90 = false;
        }
    }

    {
        ASSERT_SUCCESS(s_starts_upload_retry(client, &mock_request));
        /**
         *  4.1 If timeout rate is larger than 0.1%, we increase the timeout by 100ms (Check the timeout when the
         *      request was made, if the updated timeout is larger than the expected, skip update).
         */
        /* Set current timeout rate to be around 0.1% */
        client->synced_data.upload_part_stats.timeout_rate_tracking.num_completed = 800;
        client->synced_data.upload_part_stats.timeout_rate_tracking.num_failed = 1;

        /* Update the timeout as the rate is larger than 0.1% */
        s_init_mock_s3_request_upload_part_timeout(&mock_request, 1000 /*original_upload_timeout_ms*/, 0, 0);
        aws_s3_client_update_upload_part_timeout(client, &mock_request, AWS_ERROR_HTTP_RESPONSE_FIRST_BYTE_TIMEOUT);
        size_t current_timeout_ms = aws_atomic_load_int(&client->upload_timeout_ms);
        /* 1.1 secs */
        ASSERT_UINT_EQUALS(1100, current_timeout_ms);
        /* The same timeout applied to multiple requests made before, and the timeout happened right after we already
         * updated it. The timeout will not be updated again. */
        aws_s3_client_update_upload_part_timeout(client, &mock_request, AWS_ERROR_HTTP_RESPONSE_FIRST_BYTE_TIMEOUT);
        ASSERT_FALSE(client->synced_data.upload_part_stats.stop_timeout);
        current_timeout_ms = aws_atomic_load_int(&client->upload_timeout_ms);
        /* 1.1 secs, still */
        ASSERT_UINT_EQUALS(1100, current_timeout_ms);
        /* clean up */
        if (client->synced_data.upload_part_stats.initial_request_time.collecting_p90) {
            aws_priority_queue_clean_up(&client->synced_data.upload_part_stats.initial_request_time.p90_samples);
            client->synced_data.upload_part_stats.initial_request_time.collecting_p90 = false;
        }
    }

    {
        ASSERT_SUCCESS(s_starts_upload_retry(client, &mock_request));
        /**
         * 4.2 If timeout rate is larger than 1%, we increase the timeout by 1 secs (If needed). And clear the rate
         *      to get the exact rate with new timeout.
         */

        /* Assume our first batch requests all failed with the 1 sec timeout. As the request around 3 secs to
         * complete */

        uint64_t real_response_time_ns =
            aws_timestamp_convert(3000 - g_expect_timeout_offset_ms, AWS_TIMESTAMP_MILLIS, AWS_TIMESTAMP_NANOS, NULL);
        s_init_mock_s3_request_upload_part_timeout(
            &mock_request, 1000 /*original_upload_timeout_ms*/, real_response_time_ns, real_response_time_ns);

        /* First failure will not change the timeout, as we use the ceiling of 1% rate */
        aws_s3_client_update_upload_part_timeout(client, &mock_request, AWS_ERROR_HTTP_RESPONSE_FIRST_BYTE_TIMEOUT);
        size_t current_timeout_ms = aws_atomic_load_int(&client->upload_timeout_ms);
        ASSERT_UINT_EQUALS(1000, current_timeout_ms);

        /* Updated at the second timeout */
        aws_s3_client_update_upload_part_timeout(client, &mock_request, AWS_ERROR_HTTP_RESPONSE_FIRST_BYTE_TIMEOUT);
        current_timeout_ms = aws_atomic_load_int(&client->upload_timeout_ms);
        ASSERT_UINT_EQUALS(2000, current_timeout_ms);
        /* The rest of the batch failure will not affect the timeout */
        for (size_t i = 0; i < 10; i++) {
            aws_s3_client_update_upload_part_timeout(client, &mock_request, AWS_ERROR_HTTP_RESPONSE_FIRST_BYTE_TIMEOUT);
        }
        current_timeout_ms = aws_atomic_load_int(&client->upload_timeout_ms);
        /* still 2 secs */
        ASSERT_UINT_EQUALS(2000, current_timeout_ms);

        /* The 2 secs will still fail the whole batch */
        s_init_mock_s3_request_upload_part_timeout(
            &mock_request,
            current_timeout_ms /*original_upload_timeout_ms*/,
            real_response_time_ns,
            real_response_time_ns);
        for (size_t i = 0; i < 10; i++) {
            aws_s3_client_update_upload_part_timeout(client, &mock_request, AWS_ERROR_HTTP_RESPONSE_FIRST_BYTE_TIMEOUT);
        }
        current_timeout_ms = aws_atomic_load_int(&client->upload_timeout_ms);
        /* 3 secs now */
        ASSERT_UINT_EQUALS(3000, current_timeout_ms);

        /* 3 secs will result in around 0.1% failure, and we are okay with that */
        s_init_mock_s3_request_upload_part_timeout(
            &mock_request,
            current_timeout_ms /*original_upload_timeout_ms*/,
            real_response_time_ns,
            real_response_time_ns);
        /* 1 failure, and others all succeed */
        aws_s3_client_update_upload_part_timeout(client, &mock_request, AWS_ERROR_HTTP_RESPONSE_FIRST_BYTE_TIMEOUT);
        for (size_t i = 0; i < 10; i++) {
            aws_s3_client_update_upload_part_timeout(client, &mock_request, AWS_ERROR_SUCCESS);
        }
        /* still 3 secs */
        current_timeout_ms = aws_atomic_load_int(&client->upload_timeout_ms);
        ASSERT_UINT_EQUALS(3000, current_timeout_ms);
        ASSERT_FALSE(client->synced_data.upload_part_stats.stop_timeout);
        /* clean up */
        if (client->synced_data.upload_part_stats.initial_request_time.collecting_p90) {
            aws_priority_queue_clean_up(&client->synced_data.upload_part_stats.initial_request_time.p90_samples);
            client->synced_data.upload_part_stats.initial_request_time.collecting_p90 = false;
        }
    }

    {
        ASSERT_SUCCESS(s_starts_upload_retry(client, &mock_request));
        /* 4.3 Once the timeout is larger than 5 secs, we stop the process. */
        s_init_mock_s3_request_upload_part_timeout(&mock_request, 1000 /*original_upload_timeout_ms*/, 0, 0);

        for (size_t i = 0; i < 10; i++) {
            /* Make two continuous timeout request with updated timeout */
            aws_s3_client_update_upload_part_timeout(client, &mock_request, AWS_ERROR_HTTP_RESPONSE_FIRST_BYTE_TIMEOUT);
            aws_s3_client_update_upload_part_timeout(client, &mock_request, AWS_ERROR_HTTP_RESPONSE_FIRST_BYTE_TIMEOUT);
            size_t current_timeout_ms = aws_atomic_load_int(&client->upload_timeout_ms);
            s_init_mock_s3_request_upload_part_timeout(
                &mock_request, current_timeout_ms /*original_upload_timeout_ms*/, 0, 0);
        }
        /* Timeout stopped */
        size_t current_timeout_ms = aws_atomic_load_int(&client->upload_timeout_ms);
        ASSERT_UINT_EQUALS(0, current_timeout_ms);
        ASSERT_TRUE(client->synced_data.upload_part_stats.stop_timeout);
    }

    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);
    return AWS_OP_SUCCESS;
}

/* Test meta request can override the part size as expected */
TEST_CASE(client_meta_request_override_part_size) {
    (void)ctx;
    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_client *client = NULL;
    struct aws_s3_tester_client_options client_options = {
        .part_size = MB_TO_BYTES(8),
        .tls_usage = AWS_S3_TLS_DISABLED,
    };
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_string *host_name =
        aws_s3_tester_build_endpoint_string(allocator, &g_test_bucket_name, &g_test_s3_region);
    struct aws_byte_cursor host_cur = aws_byte_cursor_from_string(host_name);
    struct aws_byte_cursor test_object_path = aws_byte_cursor_from_c_str("/mytest");

    size_t override_part_size = MB_TO_BYTES(10);
    size_t content_length =
        MB_TO_BYTES(20); /* Let the content length larger than the override part size to make sure we do MPU */

    /* MPU put object */
    struct aws_input_stream_tester_options stream_options = {
        .autogen_length = content_length,
    };
    struct aws_input_stream *input_stream = aws_input_stream_new_tester(allocator, &stream_options);

    struct aws_http_message *put_messages = aws_s3_test_put_object_request_new(
        allocator, &host_cur, g_test_body_content_type, test_object_path, input_stream, 0 /*flags*/);

    struct aws_s3_meta_request_options meta_request_options = {
        .message = put_messages,
        .type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .part_size = override_part_size,
    };
    struct aws_s3_meta_request *put_meta_request = client->vtable->meta_request_factory(client, &meta_request_options);
    ASSERT_UINT_EQUALS(put_meta_request->part_size, override_part_size);

    /* auto ranged Get Object */
    struct aws_http_message *get_message = aws_s3_test_get_object_request_new(
        allocator, aws_byte_cursor_from_string(host_name), g_pre_existing_object_1MB);

    struct aws_s3_meta_request_options get_meta_request_options = {
        .message = get_message,
        .type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .part_size = override_part_size,
    };

    struct aws_s3_meta_request *get_meta_request =
        client->vtable->meta_request_factory(client, &get_meta_request_options);
    ASSERT_UINT_EQUALS(get_meta_request->part_size, override_part_size);

    aws_http_message_release(put_messages);
    aws_s3_meta_request_release(put_meta_request);
    aws_http_message_release(get_message);
    aws_s3_meta_request_release(get_meta_request);
    aws_string_destroy(host_name);
    aws_s3_client_release(client);
    aws_input_stream_release(input_stream);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

/* Test meta request can override the multipart upload threshold as expected */
TEST_CASE(client_meta_request_override_multipart_upload_threshold) {
    (void)ctx;
    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client_config client_config = {
        .part_size = MB_TO_BYTES(8),
        .multipart_upload_threshold = MB_TO_BYTES(15),
    };

    ASSERT_SUCCESS(aws_s3_tester_bind_client(
        &tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION | AWS_S3_TESTER_BIND_CLIENT_SIGNING));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    ASSERT_TRUE(client != NULL);

    struct aws_string *host_name =
        aws_s3_tester_build_endpoint_string(allocator, &g_test_bucket_name, &g_test_s3_region);
    struct aws_byte_cursor host_cur = aws_byte_cursor_from_string(host_name);
    struct aws_byte_cursor test_object_path = aws_byte_cursor_from_c_str("/mytest");

    size_t override_multipart_upload_threshold = MB_TO_BYTES(20);
    size_t content_length =
        MB_TO_BYTES(20); /* Let the content length larger than the override part size to make sure we do MPU */

    /* MPU put object */
    struct aws_input_stream_tester_options stream_options = {
        .autogen_length = content_length,
    };
    struct aws_input_stream *input_stream = aws_input_stream_new_tester(allocator, &stream_options);

    struct aws_http_message *put_messages = aws_s3_test_put_object_request_new(
        allocator, &host_cur, g_test_body_content_type, test_object_path, input_stream, 0 /*flags*/);

    {
        /* Content length is smaller than the override multipart_upload_threshold */
        struct aws_s3_meta_request_options meta_request_options = {
            .message = put_messages,
            .type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
            .multipart_upload_threshold = override_multipart_upload_threshold,
        };
        struct aws_s3_meta_request *put_meta_request =
            client->vtable->meta_request_factory(client, &meta_request_options);

        /* Part size will be 0, as we don't use MPU */
        ASSERT_UINT_EQUALS(put_meta_request->part_size, 0);
        aws_s3_meta_request_release(put_meta_request);
    }

    {
        /* meta request override the part size, so the override part size will be used as the multipart upload threshold
         */
        struct aws_s3_meta_request_options meta_request_options = {
            .message = put_messages,
            .type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
            .part_size = override_multipart_upload_threshold,
        };
        struct aws_s3_meta_request *put_meta_request =
            client->vtable->meta_request_factory(client, &meta_request_options);

        /* Part size will be 0, as we don't use MPU */
        ASSERT_UINT_EQUALS(put_meta_request->part_size, 0);
        aws_s3_meta_request_release(put_meta_request);
    }

    aws_http_message_release(put_messages);
    aws_string_destroy(host_name);
    aws_s3_client_release(client);
    aws_input_stream_release(input_stream);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}
