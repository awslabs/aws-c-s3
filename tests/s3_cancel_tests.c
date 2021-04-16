/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_auto_ranged_get.h"
#include "aws/s3/private/s3_auto_ranged_put.h"
#include "aws/s3/private/s3_client_impl.h"
#include "aws/s3/private/s3_meta_request_impl.h"
#include "aws/s3/private/s3_util.h"
#include "s3_tester.h"
#include <aws/testing/aws_test_harness.h>

enum s3_update_cancel_type {
    S3_UPDATE_CANCEL_TYPE_MPU_CREATE_NOT_SENT,
    S3_UPDATE_CANCEL_TYPE_MPU_CREATE_COMPLETED,
    S3_UPDATE_CANCEL_TYPE_MPU_ONE_PART_COMPLETED,
    S3_UPDATE_CANCEL_TYPE_MPU_ALL_PARTS_COMPLETED,
    S3_UPDATE_CANCEL_TYPE_NUM_MPU_CANCEL_TYPES,

    S3_UPDATE_CANCEL_TYPE_MPD_NOTHING_SENT,
    S3_UPDATE_CANCEL_TYPE_MPD_ONE_PART_COMPLETED,
};

struct s3_cancel_test_user_data {
    enum s3_update_cancel_type type;
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

    bool call_cancel = false;
    bool block_update = false;

    aws_s3_meta_request_lock_synced_data(meta_request);

    switch (cancel_test_user_data->type) {
        case S3_UPDATE_CANCEL_TYPE_MPU_CREATE_NOT_SENT:
            call_cancel = auto_ranged_put->synced_data.create_multipart_upload_sent != 0;
            break;
        case S3_UPDATE_CANCEL_TYPE_MPU_CREATE_COMPLETED:
            call_cancel = auto_ranged_put->synced_data.create_multipart_upload_completed != 0;
            break;
        case S3_UPDATE_CANCEL_TYPE_MPU_ONE_PART_COMPLETED:
            call_cancel = auto_ranged_put->synced_data.num_parts_completed == 1;
            block_update = !call_cancel && auto_ranged_put->synced_data.num_parts_sent == 1;
            break;
        case S3_UPDATE_CANCEL_TYPE_MPU_ALL_PARTS_COMPLETED:
            call_cancel =
                auto_ranged_put->synced_data.num_parts_completed == auto_ranged_put->synced_data.total_num_parts;
            break;

        case S3_UPDATE_CANCEL_TYPE_NUM_MPU_CANCEL_TYPES:
            AWS_ASSERT(false);
            break;

        case S3_UPDATE_CANCEL_TYPE_MPD_NOTHING_SENT:
            call_cancel = auto_ranged_get->synced_data.num_parts_requested == 0;
            break;
        case S3_UPDATE_CANCEL_TYPE_MPD_ONE_PART_COMPLETED:
            call_cancel = auto_ranged_get->synced_data.num_parts_completed == 1;
            block_update = !call_cancel && auto_ranged_get->synced_data.num_parts_requested == 1;
            break;
    }

    aws_s3_meta_request_unlock_synced_data(meta_request);

    if (call_cancel) {
        aws_s3_meta_request_cancel(meta_request);
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

static int s3_cancel_test_helper(struct aws_allocator *allocator, enum s3_update_cancel_type cancel_type) {
    AWS_ASSERT(allocator);

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct s3_cancel_test_user_data test_user_data = {
        .type = cancel_type,
    };

    tester.user_data = &test_user_data;

    struct aws_s3_client *client = NULL;
    struct aws_s3_tester_client_options client_options = {
        .part_size = 64 * 1024 * 1024,
    };

    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));
    struct aws_s3_client_vtable *patched_client_vtable = aws_s3_tester_patch_client_vtable(&tester, client, NULL);
    patched_client_vtable->meta_request_factory = s_meta_request_factory_patch_update_cancel_test;

    if (cancel_type < S3_UPDATE_CANCEL_TYPE_NUM_MPU_CANCEL_TYPES) {

        struct aws_s3_meta_request_test_results meta_request_test_results;
        AWS_ZERO_STRUCT(meta_request_test_results);

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

        ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &options, &meta_request_test_results));
        ASSERT_TRUE(meta_request_test_results.finished_error_code == AWS_ERROR_S3_CANCELED);

        aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);

        if (cancel_type != S3_UPDATE_CANCEL_TYPE_MPU_CREATE_NOT_SENT) {
            ASSERT_TRUE(test_user_data.abort_successful);
        }

        /* TODO: perform additional verification with list-multipart-uploads */

    } else {

        struct aws_s3_meta_request_test_results meta_request_test_results;
        AWS_ZERO_STRUCT(meta_request_test_results);

        struct aws_s3_tester_meta_request_options options = {
            .allocator = allocator,
            .client = client,
            .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
            .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE,
            .get_options =
                {
                    .object_path = g_s3_path_get_object_test_1MB,
                },
        };

        ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &options, &meta_request_test_results));
        ASSERT_TRUE(meta_request_test_results.finished_error_code == AWS_ERROR_S3_CANCELED);

        aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);
    }

    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
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

AWS_TEST_CASE(test_s3_cancel_mpu_all_parts_completed, s_test_s3_cancel_mpu_all_parts_completed)
static int s_test_s3_cancel_mpu_all_parts_completed(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    ASSERT_SUCCESS(s3_cancel_test_helper(allocator, S3_UPDATE_CANCEL_TYPE_MPU_ALL_PARTS_COMPLETED));

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

    ASSERT_SUCCESS(s3_cancel_test_helper(allocator, S3_UPDATE_CANCEL_TYPE_MPD_ONE_PART_COMPLETED));

    return 0;
}
