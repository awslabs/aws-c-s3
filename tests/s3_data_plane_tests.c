/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_client_impl.h"
#include "aws/s3/private/s3_meta_request_impl.h"
#include "aws/s3/private/s3_util.h"
#include "s3_tester.h"
#include <aws/common/byte_buf.h>
#include <aws/common/clock.h>
#include <aws/common/common.h>
#include <aws/common/ref_count.h>
#include <aws/http/request_response.h>
#include <aws/io/stream.h>
#include <aws/testing/aws_test_harness.h>
#include <inttypes.h>

AWS_TEST_CASE(test_s3_client_create_destroy, s_test_s3_client_create_destroy)
static int s_test_s3_client_create_destroy(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client_config client_config = {
        .region = g_test_s3_region,
    };

    ASSERT_SUCCESS(aws_s3_tester_bind_client(&tester, &client_config));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    ASSERT_TRUE(client != NULL);

    aws_s3_client_release(client);
    client = NULL;

    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(test_s3_request_create_destroy, s_test_s3_request_create_destroy)
static int s_test_s3_request_create_destroy(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    const int request_tag = 1234;
    const uint32_t part_number = 5678;

    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_meta_request *meta_request = aws_s3_tester_meta_request_new(&tester, NULL, NULL);
    ASSERT_TRUE(meta_request != NULL);

    struct aws_http_message *request_message = aws_s3_tester_dummy_http_request_new(&tester);
    ASSERT_TRUE(request_message != NULL);

    struct aws_s3_request *request =
        aws_s3_request_new(meta_request, request_tag, part_number, AWS_S3_REQUEST_DESC_RECORD_RESPONSE_HEADERS);

    ASSERT_TRUE(request != NULL);

    ASSERT_TRUE(request->meta_request == meta_request);
    ASSERT_TRUE(request->desc_data.part_number == part_number);
    ASSERT_TRUE(request->desc_data.request_tag == request_tag);
    ASSERT_TRUE(request->desc_data.record_response_headers == true);

    aws_s3_request_setup_send_data(request, request_message);

    ASSERT_TRUE(request->send_data.message != NULL);
    ASSERT_TRUE(request->send_data.response_headers == NULL);

    request->send_data.response_headers = aws_http_headers_new(allocator);
    ASSERT_TRUE(request->send_data.response_headers != NULL);

    aws_s3_request_clean_up_send_data(request);

    ASSERT_TRUE(request->send_data.message == NULL);
    ASSERT_TRUE(request->send_data.response_headers == NULL);
    ASSERT_TRUE(request->send_data.part_buffer == NULL);
    ASSERT_TRUE(request->send_data.response_status == 0);
    ASSERT_TRUE(request->send_data.finished_callback == 0);
    ASSERT_TRUE(request->send_data.user_data == 0);

    aws_s3_request_release(request);
    aws_http_message_release(request_message);
    aws_s3_meta_request_release(meta_request);

    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(test_s3_get_object, s_test_s3_get_object)
static int s_test_s3_get_object(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client_config client_config = {
        .region = g_test_s3_region,
        .part_size = 64 * 1024,
    };

    ASSERT_SUCCESS(aws_s3_tester_bind_client(&tester, &client_config));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    ASSERT_SUCCESS(aws_s3_tester_send_get_object_meta_request(&tester, client, true));

    aws_s3_client_release(client);
    client = NULL;

    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(test_s3_get_object_less_than_part_size, s_test_s3_get_object_less_than_part_size)
static int s_test_s3_get_object_less_than_part_size(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client_config client_config = {
        .region = g_test_s3_region,
        .part_size = 20 * 1024 * 1024,
    };

    ASSERT_SUCCESS(aws_s3_tester_bind_client(&tester, &client_config));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    ASSERT_SUCCESS(aws_s3_tester_send_get_object_meta_request(&tester, client, true));

    aws_s3_client_release(client);
    client = NULL;

    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(test_s3_get_object_multiple, s_test_s3_get_object_multiple)
static int s_test_s3_get_object_multiple(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    const struct aws_byte_cursor test_object_path = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("/get_object_test_1MB.txt");

    struct aws_s3_meta_request *meta_requests[4];
    struct aws_s3_meta_request_test_results meta_request_test_resultss[4];
    size_t num_meta_requests = sizeof(meta_requests) / sizeof(struct aws_s3_meta_request *);

    ASSERT_TRUE(
        num_meta_requests == (sizeof(meta_request_test_resultss) / sizeof(struct aws_s3_meta_request_test_results)));

    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client_config client_config = {
        .region = g_test_s3_region,
        .part_size = 64 * 1024,
    };

    aws_s3_tester_bind_client(&tester, &client_config);

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    struct aws_string *host_name =
        aws_s3_tester_build_endpoint_string(allocator, &g_test_bucket_name, &g_test_s3_region);

    /* Put together a simple S3 Get Object request. */
    struct aws_http_message *message =
        aws_s3_test_make_get_object_request(allocator, aws_byte_cursor_from_string(host_name), test_object_path);

    for (size_t i = 0; i < num_meta_requests; ++i) {
        struct aws_s3_meta_request_options options;
        AWS_ZERO_STRUCT(options);
        options.type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT;
        options.message = message;

        ASSERT_SUCCESS(aws_s3_tester_bind_meta_request(&tester, &options, &meta_request_test_resultss[i]));

        /* Trigger accelerating of our Get Object request. */
        meta_requests[i] = aws_s3_client_make_meta_request(client, &options);

        ASSERT_TRUE(meta_requests[i] != NULL);
    }

    /* Wait for the request to finish. */
    aws_s3_tester_wait_for_finish(&tester);

    aws_s3_tester_lock_synced_data(&tester);
    ASSERT_TRUE(tester.synced_data.finish_error_code == AWS_ERROR_SUCCESS);
    aws_s3_tester_unlock_synced_data(&tester);

    for (size_t i = 0; i < num_meta_requests; ++i) {
        aws_s3_tester_validate_get_object_results(&meta_request_test_resultss[i]);
        aws_s3_meta_request_test_results_clean_up(&meta_request_test_resultss[i]);
        aws_s3_meta_request_release(meta_requests[i]);
        meta_requests[i] = NULL;
    }

    aws_http_message_release(message);
    message = NULL;

    aws_string_destroy(host_name);
    host_name = NULL;

    aws_s3_client_release(client);
    client = NULL;

    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(test_s3_put_object, s_test_s3_put_object)
static int s_test_s3_put_object(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client_config client_config = {
        .region = g_test_s3_region,
        .part_size = 5 * 1024 * 1024,
    };

    ASSERT_SUCCESS(aws_s3_tester_bind_client(&tester, &client_config));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    ASSERT_TRUE(client != NULL);

    aws_s3_tester_send_put_object_meta_request(&tester, client, true);

    aws_s3_client_release(client);
    client = NULL;

    aws_s3_tester_clean_up(&tester);

    return 0;
}

AWS_TEST_CASE(test_s3_error_response, s_test_s3_error_response)
static int s_test_s3_error_response(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    const struct aws_byte_cursor test_object_path =
        AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("/non-existing-file12345.txt");

    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct aws_s3_client_config client_config = {
        .region = g_test_s3_region,
        .part_size = 64 * 1024,
    };

    ASSERT_SUCCESS(aws_s3_tester_bind_client(&tester, &client_config));

    struct aws_s3_client *client = aws_s3_client_new(allocator, &client_config);

    struct aws_string *host_name =
        aws_s3_tester_build_endpoint_string(allocator, &g_test_bucket_name, &g_test_s3_region);

    /* Put together a simple S3 Get Object request. */
    struct aws_http_message *message =
        aws_s3_test_make_get_object_request(allocator, aws_byte_cursor_from_string(host_name), test_object_path);

    struct aws_s3_meta_request_options options;
    AWS_ZERO_STRUCT(options);
    options.type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT;
    options.message = message;

    /* Trigger accelerating of our Get Object request. */
    struct aws_s3_meta_request_test_results meta_request_test_results;

    ASSERT_SUCCESS(aws_s3_tester_bind_meta_request(&tester, &options, &meta_request_test_results));

    struct aws_s3_meta_request *meta_request = aws_s3_client_make_meta_request(client, &options);

    ASSERT_TRUE(meta_request != NULL);

    /* Wait for the request to finish. */
    aws_s3_tester_wait_for_finish(&tester);

    aws_s3_tester_lock_synced_data(&tester);
    ASSERT_TRUE(tester.synced_data.finish_error_code != AWS_ERROR_SUCCESS);
    aws_s3_tester_unlock_synced_data(&tester);

    ASSERT_TRUE(meta_request_test_results.finished_response_status == 404);
    ASSERT_TRUE(meta_request_test_results.finished_error_code != AWS_ERROR_SUCCESS);

    ASSERT_TRUE(meta_request_test_results.error_response_headers != NULL);
    ASSERT_TRUE(meta_request_test_results.error_response_body.len > 0);

    aws_s3_meta_request_release(meta_request);
    aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);

    aws_http_message_release(message);
    message = NULL;

    aws_string_destroy(host_name);
    host_name = NULL;

    aws_s3_client_release(client);
    client = NULL;

    aws_s3_tester_clean_up(&tester);

    return 0;
}
