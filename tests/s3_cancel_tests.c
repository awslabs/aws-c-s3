/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_auto_ranged_get.h"
#include "aws/s3/private/s3_auto_ranged_put.h"
#include "aws/s3/private/s3_client_impl.h"
#include "aws/s3/private/s3_meta_request_impl.h"
#include "aws/s3/private/s3_util.h"
#include "aws/s3/s3_client.h"
#include "s3_tester.h"
#include <aws/testing/aws_test_harness.h>

enum s3_update_cancel_type {
    S3_UPDATE_CANCEL_TYPE_NO_CANCEL,

    S3_UPDATE_CANCEL_TYPE_MPU_CREATE_NOT_SENT,
    S3_UPDATE_CANCEL_TYPE_MPU_CREATE_COMPLETED,
    S3_UPDATE_CANCEL_TYPE_MPU_ONE_PART_COMPLETED,
    S3_UPDATE_CANCEL_TYPE_MPU_ALL_PARTS_COMPLETED,
    S3_UPDATE_CANCEL_TYPE_MPU_ONGOING_HTTP_REQUESTS,
    S3_UPDATE_CANCEL_TYPE_NUM_MPU_CANCEL_TYPES,

    S3_UPDATE_CANCEL_TYPE_MPD_NOTHING_SENT,
    S3_UPDATE_CANCEL_TYPE_MPD_HEAD_OBJECT_SENT,
    S3_UPDATE_CANCEL_TYPE_MPD_HEAD_OBJECT_COMPLETED,
    S3_UPDATE_CANCEL_TYPE_MPD_GET_EMPTY_OBJECT_WITH_PART_NUMBER_1_SENT,
    S3_UPDATE_CANCEL_TYPE_MPD_GET_EMPTY_OBJECT_WITH_PART_NUMBER_1_COMPLETED,
    S3_UPDATE_CANCEL_TYPE_MPD_ONE_PART_SENT,
    S3_UPDATE_CANCEL_TYPE_MPD_ONE_PART_COMPLETED,
    S3_UPDATE_CANCEL_TYPE_MPD_TWO_PARTS_COMPLETED,
    S3_UPDATE_CANCEL_TYPE_MPD_PENDING_STREAMING,
};

struct s3_cancel_test_user_data {
    enum s3_update_cancel_type type;
    bool pause;
    struct aws_s3_meta_request_resume_token *resume_token;
    bool abort_successful;
};

static bool s_s3_meta_request_update_cancel_test(
    struct aws_s3_meta_request *meta_request,
    uint32_t flags,
    struct aws_s3_request **out_request) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(out_request);

    struct aws_s3_meta_request_test_results *results = meta_request->user_data;
    struct aws_s3_tester *tester = results->tester;
    struct s3_cancel_test_user_data *cancel_test_user_data = tester->user_data;

    struct aws_s3_auto_ranged_put *auto_ranged_put = meta_request->impl;
    struct aws_s3_auto_ranged_get *auto_ranged_get = meta_request->impl;

    bool call_cancel_or_pause = false;
    bool block_update = false;

    aws_s3_meta_request_lock_synced_data(meta_request);

    switch (cancel_test_user_data->type) {
        case S3_UPDATE_CANCEL_TYPE_NO_CANCEL:
            break;

        case S3_UPDATE_CANCEL_TYPE_MPU_CREATE_NOT_SENT:
            call_cancel_or_pause = auto_ranged_put->synced_data.create_multipart_upload_sent != 0;
            break;
        case S3_UPDATE_CANCEL_TYPE_MPU_CREATE_COMPLETED:
            call_cancel_or_pause = auto_ranged_put->synced_data.create_multipart_upload_completed != 0;
            break;
        case S3_UPDATE_CANCEL_TYPE_MPU_ONE_PART_COMPLETED:
            call_cancel_or_pause = auto_ranged_put->synced_data.num_parts_completed == 1;
            block_update = !call_cancel_or_pause && auto_ranged_put->synced_data.num_parts_started == 1;
            break;
        case S3_UPDATE_CANCEL_TYPE_MPU_ALL_PARTS_COMPLETED:
            call_cancel_or_pause = auto_ranged_put->synced_data.num_parts_completed ==
                                   auto_ranged_put->total_num_parts_from_content_length;
            break;

        case S3_UPDATE_CANCEL_TYPE_MPU_ONGOING_HTTP_REQUESTS:
            call_cancel_or_pause = !aws_linked_list_empty(&meta_request->synced_data.cancellable_http_streams_list);
            break;

        case S3_UPDATE_CANCEL_TYPE_NUM_MPU_CANCEL_TYPES:
            AWS_ASSERT(false);
            break;

        case S3_UPDATE_CANCEL_TYPE_MPD_NOTHING_SENT:
            call_cancel_or_pause = auto_ranged_get->synced_data.num_parts_requested == 0;
            break;

        case S3_UPDATE_CANCEL_TYPE_MPD_HEAD_OBJECT_SENT:
            call_cancel_or_pause = auto_ranged_get->synced_data.head_object_sent != 0;
            break;

        case S3_UPDATE_CANCEL_TYPE_MPD_HEAD_OBJECT_COMPLETED:
            call_cancel_or_pause = auto_ranged_get->synced_data.head_object_completed != 0;
            break;

        case S3_UPDATE_CANCEL_TYPE_MPD_GET_EMPTY_OBJECT_WITH_PART_NUMBER_1_SENT:
            call_cancel_or_pause = auto_ranged_get->synced_data.object_range_known != 0 &&
                                   auto_ranged_get->synced_data.num_parts_requested > 0;
            break;

        case S3_UPDATE_CANCEL_TYPE_MPD_GET_EMPTY_OBJECT_WITH_PART_NUMBER_1_COMPLETED:
            call_cancel_or_pause = auto_ranged_get->synced_data.num_parts_completed > 0;
            break;

        case S3_UPDATE_CANCEL_TYPE_MPD_ONE_PART_SENT:
            call_cancel_or_pause = auto_ranged_get->synced_data.num_parts_requested == 1;
            break;

        case S3_UPDATE_CANCEL_TYPE_MPD_ONE_PART_COMPLETED:
            call_cancel_or_pause = auto_ranged_get->synced_data.num_parts_completed == 1;

            /* Prevent other parts from being queued while we wait for this one to complete. */
            block_update = !call_cancel_or_pause && auto_ranged_get->synced_data.num_parts_requested == 1;
            break;

        case S3_UPDATE_CANCEL_TYPE_MPD_TWO_PARTS_COMPLETED:
            call_cancel_or_pause = auto_ranged_get->synced_data.num_parts_completed == 2;

            /* Prevent other parts from being queued while we wait for these two to complete. */
            block_update = !call_cancel_or_pause && auto_ranged_get->synced_data.num_parts_requested == 2;
            break;

        case S3_UPDATE_CANCEL_TYPE_MPD_PENDING_STREAMING:
            call_cancel_or_pause =
                aws_priority_queue_size(&meta_request->synced_data.pending_body_streaming_requests) > 0;
            break;
    }

    aws_s3_meta_request_unlock_synced_data(meta_request);
    if (call_cancel_or_pause) {
        if (cancel_test_user_data->pause) {
            aws_s3_meta_request_pause(meta_request, &cancel_test_user_data->resume_token);
        } else {
            aws_s3_meta_request_cancel(meta_request);
        }
    }

    if (block_update) {
        return true;
    }

    struct aws_s3_meta_request_vtable *original_meta_request_vtable =
        aws_s3_tester_get_meta_request_vtable_patch(tester, 0)->original_vtable;

    return original_meta_request_vtable->update(meta_request, flags, out_request);
}

static void s_s3_meta_request_finished_request_cancel_test(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    int error_code) {
    AWS_ASSERT(meta_request);
    AWS_ASSERT(request);

    struct aws_s3_meta_request_test_results *results = meta_request->user_data;
    struct aws_s3_tester *tester = results->tester;
    struct s3_cancel_test_user_data *cancel_test_user_data = tester->user_data;

    if (meta_request->type == AWS_S3_META_REQUEST_TYPE_PUT_OBJECT &&
        request->request_tag == AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_ABORT_MULTIPART_UPLOAD) {

        cancel_test_user_data->abort_successful = error_code == AWS_ERROR_SUCCESS;
    }

    struct aws_s3_meta_request_vtable *original_meta_request_vtable =
        aws_s3_tester_get_meta_request_vtable_patch(tester, 0)->original_vtable;

    original_meta_request_vtable->finished_request(meta_request, request, error_code);
}

static struct aws_s3_meta_request *s_meta_request_factory_patch_update_cancel_test(
    struct aws_s3_client *client,
    const struct aws_s3_meta_request_options *options) {
    AWS_ASSERT(client != NULL);

    struct aws_s3_tester *tester = client->shutdown_callback_user_data;
    AWS_ASSERT(tester != NULL);

    struct aws_s3_client_vtable *original_client_vtable =
        aws_s3_tester_get_client_vtable_patch(tester, 0)->original_vtable;

    struct aws_s3_meta_request *meta_request = original_client_vtable->meta_request_factory(client, options);

    struct aws_s3_meta_request_vtable *patched_meta_request_vtable =
        aws_s3_tester_patch_meta_request_vtable(tester, meta_request, NULL);
    patched_meta_request_vtable->update = s_s3_meta_request_update_cancel_test;
    patched_meta_request_vtable->finished_request = s_s3_meta_request_finished_request_cancel_test;

    return meta_request;
}

static int s3_cancel_test_helper_ex(
    struct aws_allocator *allocator,
    enum s3_update_cancel_type cancel_type,
    bool async_input_stream,
    bool pause) {

    AWS_ASSERT(allocator);

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct s3_cancel_test_user_data test_user_data = {
        .type = cancel_type,
        .pause = pause,
    };

    tester.user_data = &test_user_data;

    size_t client_part_size = 0;

    if (cancel_type > S3_UPDATE_CANCEL_TYPE_NUM_MPU_CANCEL_TYPES) {
        client_part_size = 16 * 1024;
    }

    struct aws_s3_client *client = NULL;
    struct aws_s3_tester_client_options client_options = {
        .part_size = client_part_size,
    };

    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));
    struct aws_s3_client_vtable *patched_client_vtable = aws_s3_tester_patch_client_vtable(&tester, client, NULL);
    patched_client_vtable->meta_request_factory = s_meta_request_factory_patch_update_cancel_test;

    if (cancel_type < S3_UPDATE_CANCEL_TYPE_NUM_MPU_CANCEL_TYPES) {

        struct aws_s3_meta_request_test_results meta_request_test_results;
        aws_s3_meta_request_test_results_init(&meta_request_test_results, allocator);

        struct aws_s3_tester_meta_request_options options = {
            .allocator = allocator,
            .client = client,
            .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
            .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE,
            .put_options =
                {
                    .ensure_multipart = true,
                    .async_input_stream = async_input_stream,
                },
        };

        ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &options, &meta_request_test_results));
        int expected_error_code = pause ? AWS_ERROR_S3_PAUSED : AWS_ERROR_S3_CANCELED;
        ASSERT_INT_EQUALS(expected_error_code, meta_request_test_results.finished_error_code);

        if (cancel_type == S3_UPDATE_CANCEL_TYPE_MPU_ONGOING_HTTP_REQUESTS) {
            /* Check the metric and see we have at least a request completed with AWS_ERROR_S3_CANCELED */
            /* The meta request completed, we can access the synced data now. */
            struct aws_array_list *metrics_list = &meta_request_test_results.synced_data.metrics;
            bool cancelled_successfully = false;
            for (size_t i = 0; i < aws_array_list_length(metrics_list); ++i) {
                struct aws_s3_request_metrics *metrics = NULL;
                aws_array_list_get_at(metrics_list, (void **)&metrics, i);
                if (metrics->crt_info_metrics.error_code == expected_error_code) {
                    cancelled_successfully = true;
                    break;
                }
            }
            ASSERT_TRUE(cancelled_successfully);
        }

        aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);
        if (cancel_type != S3_UPDATE_CANCEL_TYPE_MPU_CREATE_NOT_SENT && !pause) {
            ASSERT_TRUE(test_user_data.abort_successful);
        }
        if (pause) {
            /* Resume the paused request. */
            ASSERT_NOT_NULL(test_user_data.resume_token);
            test_user_data.type = S3_UPDATE_CANCEL_TYPE_NO_CANCEL;
            struct aws_s3_tester_meta_request_options resume_options = {
                .allocator = allocator,
                .client = client,
                .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
                .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_SUCCESS,
                .put_options =
                    {
                        .ensure_multipart = true,
                        .async_input_stream = async_input_stream,
                        .resume_token = test_user_data.resume_token,
                    },
            };

            ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &resume_options, NULL));
            aws_s3_meta_request_resume_token_release(test_user_data.resume_token);
        }

        /* TODO: perform additional verification with list-multipart-uploads */

    } else {

        struct aws_s3_meta_request_test_results meta_request_test_results;
        aws_s3_meta_request_test_results_init(&meta_request_test_results, allocator);

        /* Specify a range without start-range to trigger HeadRequest */
        const struct aws_byte_cursor range = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("bytes=-32767");

        struct aws_s3_tester_meta_request_options options = {
            .allocator = allocator,
            .client = client,
            .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
            .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE,
            .get_options =
                {
                    /* Note 1: 10MB object with 16KB parts, so that tests have many requests in-flight.
                     * We want to try and stress stuff like parts arriving out of order. */
                    .object_path = g_pre_existing_object_10MB,
                },
        };

        switch (cancel_type) {
            case S3_UPDATE_CANCEL_TYPE_MPD_HEAD_OBJECT_SENT:
                options.get_options.object_range = range;
                break;

            case S3_UPDATE_CANCEL_TYPE_MPD_HEAD_OBJECT_COMPLETED:
                options.get_options.object_range = range;
                break;

            case S3_UPDATE_CANCEL_TYPE_MPD_GET_EMPTY_OBJECT_WITH_PART_NUMBER_1_SENT:
                options.get_options.object_path = g_pre_existing_empty_object;
                break;

            case S3_UPDATE_CANCEL_TYPE_MPD_GET_EMPTY_OBJECT_WITH_PART_NUMBER_1_COMPLETED:
                options.get_options.object_path = g_pre_existing_empty_object;
                break;

            default:
                break;
        }

        ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &options, &meta_request_test_results));
        ASSERT_TRUE(meta_request_test_results.finished_error_code == AWS_ERROR_S3_CANCELED);

        aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);
    }

    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

static int s3_cancel_test_helper(struct aws_allocator *allocator, enum s3_update_cancel_type cancel_type) {
    return s3_cancel_test_helper_ex(allocator, cancel_type, false /*async_input_stream*/, false /*pause*/);
}

static int s3_cancel_test_helper_fc(
    struct aws_allocator *allocator,
    enum s3_update_cancel_type cancel_type,
    struct aws_byte_cursor object_path,
    enum aws_s3_checksum_algorithm checksum_algorithm) {
    AWS_ASSERT(allocator);

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct s3_cancel_test_user_data test_user_data = {
        .type = cancel_type,
    };

    tester.user_data = &test_user_data;

    size_t client_part_size = 0;

    if (cancel_type > S3_UPDATE_CANCEL_TYPE_NUM_MPU_CANCEL_TYPES) {
        client_part_size = 16 * 1024;
    }

    struct aws_s3_client *client = NULL;
    struct aws_s3_tester_client_options client_options = {
        .part_size = client_part_size,
    };

    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));
    struct aws_s3_client_vtable *patched_client_vtable = aws_s3_tester_patch_client_vtable(&tester, client, NULL);
    patched_client_vtable->meta_request_factory = s_meta_request_factory_patch_update_cancel_test;

    if (cancel_type < S3_UPDATE_CANCEL_TYPE_NUM_MPU_CANCEL_TYPES) {

        struct aws_s3_meta_request_test_results meta_request_test_results;
        aws_s3_meta_request_test_results_init(&meta_request_test_results, allocator);

        struct aws_s3_tester_meta_request_options options = {
            .allocator = allocator,
            .client = client,
            .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
            .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE,
            .checksum_algorithm = checksum_algorithm,
            .validate_get_response_checksum = false,
            .put_options =
                {
                    .ensure_multipart = true,
                    .object_path_override = object_path,
                },
        };

        ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &options, &meta_request_test_results));
        ASSERT_TRUE(meta_request_test_results.finished_error_code == AWS_ERROR_S3_CANCELED);

        aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);

        if (cancel_type != S3_UPDATE_CANCEL_TYPE_MPU_CREATE_NOT_SENT) {
            ASSERT_TRUE(test_user_data.abort_successful);
        }

        /* TODO: perform additional verification with list-multipart-uploads */

    } else {

        struct aws_s3_meta_request_test_results meta_request_test_results;
        aws_s3_meta_request_test_results_init(&meta_request_test_results, allocator);

        // Range for the second 16k
        const struct aws_byte_cursor range = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("bytes=16384-32767");

        struct aws_s3_tester_meta_request_options options = {
            .allocator = allocator,
            .client = client,
            .validate_get_response_checksum = true,
            .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
            .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE,
            .get_options =
                {
                    .object_path = object_path,
                },
        };

        switch (cancel_type) {
            case S3_UPDATE_CANCEL_TYPE_MPD_HEAD_OBJECT_SENT:
                options.get_options.object_range = range;
                break;

            case S3_UPDATE_CANCEL_TYPE_MPD_HEAD_OBJECT_COMPLETED:
                options.get_options.object_range = range;
                break;

            case S3_UPDATE_CANCEL_TYPE_MPD_GET_EMPTY_OBJECT_WITH_PART_NUMBER_1_SENT:
                options.get_options.object_path = g_pre_existing_empty_object;
                break;

            case S3_UPDATE_CANCEL_TYPE_MPD_GET_EMPTY_OBJECT_WITH_PART_NUMBER_1_COMPLETED:
                options.get_options.object_path = g_pre_existing_empty_object;
                break;

            default:
                break;
        }

        ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &options, &meta_request_test_results));
        ASSERT_TRUE(meta_request_test_results.finished_error_code == AWS_ERROR_S3_CANCELED);

        aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);
    }

    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(test_s3_cancel_mpu_one_part_completed_fc, s_test_s3_cancel_mpu_one_part_completed_fc)
static int s_test_s3_cancel_mpu_one_part_completed_fc(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_byte_buf path_buf;
    AWS_ZERO_STRUCT(path_buf);

    ASSERT_SUCCESS(aws_s3_tester_upload_file_path_init(
        allocator, &path_buf, aws_byte_cursor_from_c_str("/prefix/cancel/upload_one_part_complete_fc.txt")));

    ASSERT_SUCCESS(s3_cancel_test_helper_fc(
        allocator, S3_UPDATE_CANCEL_TYPE_MPU_ONE_PART_COMPLETED, aws_byte_cursor_from_buf(&path_buf), AWS_SCA_CRC32));

    aws_byte_buf_clean_up(&path_buf);
    return 0;
}

AWS_TEST_CASE(test_s3_cancel_mpd_one_part_completed_fc, s_test_s3_cancel_mpd_one_part_completed_fc)
static int s_test_s3_cancel_mpd_one_part_completed_fc(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(s3_cancel_test_helper_fc(
        allocator, S3_UPDATE_CANCEL_TYPE_MPD_ONE_PART_COMPLETED, g_pre_existing_object_10MB, AWS_SCA_CRC32));

    return 0;
}

AWS_TEST_CASE(test_s3_cancel_mpu_create_not_sent, s_test_s3_cancel_mpu_create_not_sent)
static int s_test_s3_cancel_mpu_create_not_sent(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(s3_cancel_test_helper(allocator, S3_UPDATE_CANCEL_TYPE_MPU_CREATE_NOT_SENT));

    return 0;
}

AWS_TEST_CASE(test_s3_cancel_mpu_create_completed, s_test_s3_cancel_mpu_create_completed)
static int s_test_s3_cancel_mpu_create_completed(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(s3_cancel_test_helper(allocator, S3_UPDATE_CANCEL_TYPE_MPU_CREATE_COMPLETED));

    return 0;
}

AWS_TEST_CASE(test_s3_cancel_mpu_one_part_completed, s_test_s3_cancel_mpu_one_part_completed)
static int s_test_s3_cancel_mpu_one_part_completed(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(s3_cancel_test_helper(allocator, S3_UPDATE_CANCEL_TYPE_MPU_ONE_PART_COMPLETED));

    return 0;
}

AWS_TEST_CASE(test_s3_cancel_mpu_one_part_completed_async, s_test_s3_cancel_mpu_one_part_completed_async)
static int s_test_s3_cancel_mpu_one_part_completed_async(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(s3_cancel_test_helper_ex(
        allocator, S3_UPDATE_CANCEL_TYPE_MPU_ONE_PART_COMPLETED, true /*async_input_stream*/, false /*pause*/));

    return 0;
}

AWS_TEST_CASE(test_s3_cancel_mpu_all_parts_completed, s_test_s3_cancel_mpu_all_parts_completed)
static int s_test_s3_cancel_mpu_all_parts_completed(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(s3_cancel_test_helper(allocator, S3_UPDATE_CANCEL_TYPE_MPU_ALL_PARTS_COMPLETED));

    return 0;
}

AWS_TEST_CASE(test_s3_cancel_mpu_cancellable_requests, s_test_s3_cancel_mpu_cancellable_requests)
static int s_test_s3_cancel_mpu_cancellable_requests(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(s3_cancel_test_helper(allocator, S3_UPDATE_CANCEL_TYPE_MPU_ONGOING_HTTP_REQUESTS));

    return 0;
}

AWS_TEST_CASE(test_s3_pause_mpu_cancellable_requests, s_test_s3_pause_mpu_cancellable_requests)
static int s_test_s3_pause_mpu_cancellable_requests(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(s3_cancel_test_helper_ex(
        allocator, S3_UPDATE_CANCEL_TYPE_MPU_ONGOING_HTTP_REQUESTS, false /*async_input_stream*/, true /*pause*/));

    return 0;
}

AWS_TEST_CASE(test_s3_cancel_mpd_nothing_sent, s_test_s3_cancel_mpd_nothing_sent)
static int s_test_s3_cancel_mpd_nothing_sent(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(s3_cancel_test_helper(allocator, S3_UPDATE_CANCEL_TYPE_MPD_NOTHING_SENT));

    return 0;
}

AWS_TEST_CASE(test_s3_cancel_mpd_one_part_sent, s_test_s3_cancel_mpd_one_part_sent)
static int s_test_s3_cancel_mpd_one_part_sent(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(s3_cancel_test_helper(allocator, S3_UPDATE_CANCEL_TYPE_MPD_ONE_PART_SENT));

    return 0;
}

AWS_TEST_CASE(test_s3_cancel_mpd_one_part_completed, s_test_s3_cancel_mpd_one_part_completed)
static int s_test_s3_cancel_mpd_one_part_completed(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(s3_cancel_test_helper(allocator, S3_UPDATE_CANCEL_TYPE_MPD_ONE_PART_COMPLETED));

    return 0;
}

AWS_TEST_CASE(test_s3_cancel_mpd_two_parts_completed, s_test_s3_cancel_mpd_two_parts_completed)
static int s_test_s3_cancel_mpd_two_parts_completed(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(s3_cancel_test_helper(allocator, S3_UPDATE_CANCEL_TYPE_MPD_TWO_PARTS_COMPLETED));

    return 0;
}

AWS_TEST_CASE(test_s3_cancel_mpd_head_object_sent, s_test_s3_cancel_mpd_head_object_sent)
static int s_test_s3_cancel_mpd_head_object_sent(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(s3_cancel_test_helper(allocator, S3_UPDATE_CANCEL_TYPE_MPD_HEAD_OBJECT_SENT));

    return 0;
}

AWS_TEST_CASE(test_s3_cancel_mpd_head_object_completed, s_test_s3_cancel_mpd_head_object_completed)
static int s_test_s3_cancel_mpd_head_object_completed(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(s3_cancel_test_helper(allocator, S3_UPDATE_CANCEL_TYPE_MPD_HEAD_OBJECT_COMPLETED));

    return 0;
}

AWS_TEST_CASE(
    test_s3_cancel_mpd_empty_object_get_with_part_number_1_sent,
    s_test_s3_cancel_mpd_empty_object_get_with_part_number_1_sent)
static int s_test_s3_cancel_mpd_empty_object_get_with_part_number_1_sent(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(
        s3_cancel_test_helper(allocator, S3_UPDATE_CANCEL_TYPE_MPD_GET_EMPTY_OBJECT_WITH_PART_NUMBER_1_SENT));

    return 0;
}

AWS_TEST_CASE(
    test_s3_cancel_mpd_empty_object_get_with_part_number_1_completed,
    s_test_s3_cancel_mpd_empty_object_get_with_part_number_1_completed)
static int s_test_s3_cancel_mpd_empty_object_get_with_part_number_1_completed(
    struct aws_allocator *allocator,
    void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(
        s3_cancel_test_helper(allocator, S3_UPDATE_CANCEL_TYPE_MPD_GET_EMPTY_OBJECT_WITH_PART_NUMBER_1_COMPLETED));

    return 0;
}

AWS_TEST_CASE(test_s3_cancel_mpd_pending_streaming, s_test_s3_cancel_mpd_pending_streaming)
static int s_test_s3_cancel_mpd_pending_streaming(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(s3_cancel_test_helper(allocator, S3_UPDATE_CANCEL_TYPE_MPD_PENDING_STREAMING));

    return 0;
}

struct test_s3_cancel_prepare_user_data {
    uint32_t request_prepare_counters[AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_MAX];
};

/* Data for async cancel-prepare-meta-request job */
struct test_s3_cancel_prepare_meta_request_prepare_request_job {
    struct aws_allocator *allocator;
    struct aws_s3_request *request;
    struct aws_future_void *original_future; /* original future that we're intercepting and patching */
    struct aws_future_void *patched_future;  /* patched future to set when this job completes */
};

static void s_test_s3_cancel_prepare_meta_request_prepare_request_on_original_done(void *user_data);

static struct aws_future_void *s_test_s3_cancel_prepare_meta_request_prepare_request(struct aws_s3_request *request) {
    struct aws_s3_meta_request *meta_request = request->meta_request;
    AWS_ASSERT(meta_request != NULL);

    struct aws_s3_meta_request_test_results *results = meta_request->user_data;
    AWS_ASSERT(results != NULL);

    struct aws_s3_tester *tester = results->tester;
    AWS_ASSERT(tester != NULL);

    struct aws_future_void *patched_future = aws_future_void_new(meta_request->allocator);

    struct test_s3_cancel_prepare_meta_request_prepare_request_job *patched_prep = aws_mem_calloc(
        meta_request->allocator, 1, sizeof(struct test_s3_cancel_prepare_meta_request_prepare_request_job));
    patched_prep->allocator = meta_request->allocator;
    patched_prep->request = request;
    patched_prep->patched_future = aws_future_void_acquire(patched_future);

    struct aws_s3_meta_request_vtable *original_meta_request_vtable =
        aws_s3_tester_get_meta_request_vtable_patch(tester, 0)->original_vtable;

    patched_prep->original_future = original_meta_request_vtable->prepare_request(request);
    aws_future_void_register_callback(
        patched_prep->original_future,
        s_test_s3_cancel_prepare_meta_request_prepare_request_on_original_done,
        patched_prep);

    return patched_future;
}

static void s_test_s3_cancel_prepare_meta_request_prepare_request_on_original_done(void *user_data) {

    struct test_s3_cancel_prepare_meta_request_prepare_request_job *patched_prep = user_data;
    struct aws_s3_request *request = patched_prep->request;
    struct aws_s3_meta_request *meta_request = request->meta_request;
    struct aws_s3_meta_request_test_results *results = meta_request->user_data;
    struct aws_s3_tester *tester = results->tester;
    struct test_s3_cancel_prepare_user_data *test_user_data = tester->user_data;

    int error_code = aws_future_void_get_error(patched_prep->original_future);
    if (error_code != AWS_ERROR_SUCCESS) {
        aws_future_void_set_error(patched_prep->patched_future, error_code);
        goto finish;
    }

    ++test_user_data->request_prepare_counters[request->request_tag];

    /* Cancel after the first part is prepared, preventing any additional parts from being prepared. */
    if (request->request_tag == AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_PART &&
        test_user_data->request_prepare_counters[AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_PART] == 1) {
        aws_s3_meta_request_cancel(meta_request);
    }

    aws_future_void_set_result(patched_prep->patched_future);
finish:
    aws_future_void_release(patched_prep->original_future);
    aws_future_void_release(patched_prep->patched_future);
    aws_mem_release(patched_prep->allocator, patched_prep);
}

static struct aws_s3_meta_request *s_test_s3_cancel_prepare_meta_request_factory(
    struct aws_s3_client *client,
    const struct aws_s3_meta_request_options *options) {
    AWS_ASSERT(client != NULL);

    struct aws_s3_tester *tester = client->shutdown_callback_user_data;
    AWS_ASSERT(tester != NULL);

    struct aws_s3_client_vtable *original_client_vtable =
        aws_s3_tester_get_client_vtable_patch(tester, 0)->original_vtable;

    struct aws_s3_meta_request *meta_request = original_client_vtable->meta_request_factory(client, options);

    struct aws_s3_meta_request_vtable *patched_meta_request_vtable =
        aws_s3_tester_patch_meta_request_vtable(tester, meta_request, NULL);
    patched_meta_request_vtable->prepare_request = s_test_s3_cancel_prepare_meta_request_prepare_request;

    return meta_request;
}

AWS_TEST_CASE(test_s3_cancel_prepare, s_test_s3_cancel_prepare)
static int s_test_s3_cancel_prepare(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct test_s3_cancel_prepare_user_data test_user_data;
    AWS_ZERO_STRUCT(test_user_data);
    tester.user_data = &test_user_data;

    struct aws_s3_client *client = NULL;

    struct aws_s3_tester_client_options client_options;
    AWS_ZERO_STRUCT(client_options);

    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_s3_client_vtable *patched_client_vtable = aws_s3_tester_patch_client_vtable(&tester, client, NULL);
    patched_client_vtable->meta_request_factory = s_test_s3_cancel_prepare_meta_request_factory;

    {
        struct aws_s3_tester_meta_request_options options = {
            .allocator = allocator,
            .client = client,
            .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
            .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE,
            .put_options =
                {
                    .ensure_multipart = true,
                },
        };

        ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &options, NULL));
    }

    ASSERT_TRUE(
        test_user_data.request_prepare_counters[AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_CREATE_MULTIPART_UPLOAD] == 1);
    ASSERT_TRUE(test_user_data.request_prepare_counters[AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_PART] == 1);
    ASSERT_TRUE(
        test_user_data.request_prepare_counters[AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_ABORT_MULTIPART_UPLOAD] == 1);
    ASSERT_TRUE(
        test_user_data.request_prepare_counters[AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_COMPLETE_MULTIPART_UPLOAD] == 0);

    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return 0;
}
