/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "s3_tester.h"
#include "aws/s3/private/s3_auto_ranged_get.h"
#include "aws/s3/private/s3_checksums.h"
#include "aws/s3/private/s3_client_impl.h"
#include "aws/s3/private/s3_meta_request_impl.h"
#include "aws/s3/private/s3_util.h"
#include <aws/auth/credentials.h>
#include <aws/common/environment.h>
#include <aws/common/system_info.h>
#include <aws/common/uri.h>
#include <aws/http/request_response.h>
#include <aws/io/channel_bootstrap.h>
#include <aws/io/event_loop.h>
#include <aws/io/host_resolver.h>
#include <aws/io/stream.h>
#include <aws/io/tls_channel_handler.h>
#include <aws/testing/aws_test_harness.h>
#include <aws/testing/stream_tester.h>
#include <ctype.h>
#include <inttypes.h>
#include <stdlib.h>
#include <time.h>

#ifdef _MSC_VER
#    pragma warning(disable : 4232) /* function pointer to dll symbol */
#endif

const struct aws_byte_cursor g_mock_server_uri = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("http://localhost:8080/");

const struct aws_byte_cursor g_test_mrap_endpoint =
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("moujmk3izc19y.mrap.accesspoint.s3-global.amazonaws.com");

const struct aws_byte_cursor g_test_body_content_type = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("text/plain");
const struct aws_byte_cursor g_test_s3_region = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("us-west-2");

const struct aws_byte_cursor g_s3_sse_header = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-server-side-encryption");
const struct aws_byte_cursor g_s3_sse_c_alg_header =
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-server-side-encryption-customer-algorithm");
const struct aws_byte_cursor g_s3_sse_c_key_header =
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-server-side-encryption-customer-key");
const struct aws_byte_cursor g_s3_sse_c_key_md5_header =
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-server-side-encryption-customer-key-md5");

/* TODO populate these at the beginning of running tests with names that are unique to the test run. */

const struct aws_byte_cursor g_pre_existing_object_1MB = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("/pre-existing-1MB");
const struct aws_byte_cursor g_pre_existing_object_10MB = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("/pre-existing-10MB");
const struct aws_byte_cursor g_pre_existing_object_kms_10MB =
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("/pre-existing-10MB-kms");
const struct aws_byte_cursor g_pre_existing_object_aes256_10MB =
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("/pre-existing-10MB-aes256");
const struct aws_byte_cursor g_pre_existing_empty_object = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("/pre-existing-empty");
const struct aws_byte_cursor g_pre_existing_object_async_error_xml =
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("/pre-existing-async-error-xml");

const struct aws_byte_cursor g_put_object_prefix = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("/upload/put-object-test");
const struct aws_byte_cursor g_upload_folder = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("/upload");

/* If `$CRT_S3_TEST_BUCKET_NAME` environment variable is set, use that; otherwise, use aws-c-s3-test-bucket */
struct aws_byte_cursor g_test_bucket_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("aws-c-s3-test-bucket");
/* If `$CRT_S3_TEST_BUCKET_NAME` envrionment variable is set, use `$CRT_S3_TEST_BUCKET_NAME-public`; otherwise, use
 * aws-c-s3-test-bucket-public */
struct aws_byte_cursor g_test_public_bucket_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("aws-c-s3-test-bucket-public");
/* If `$CRT_S3_TEST_BUCKET_NAME` environment variable is set, use
 * `$CRT_S3_TEST_BUCKET_NAME--usw2-az1--x-s3.s3express-usw2-az1.us-west-2.amazonaws.com`; otherwise, use
 * aws-c-s3-test-bucket--usw2-az1--x-s3.s3express-usw2-az1.us-west-2.amazonaws.com */
struct aws_byte_cursor g_test_s3express_bucket_usw2_az1_endpoint = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL(
    "aws-c-s3-test-bucket--usw2-az1--x-s3.s3express-usw2-az1.us-west-2.amazonaws.com");
/* If `$CRT_S3_TEST_BUCKET_NAME` environment variable is set, use
 * `$CRT_S3_TEST_BUCKET_NAME--us1-az1--x-s3.s3express-use1-az4.us-east-1.amazonaws.com`; otherwise, use
 * aws-c-s3-test-bucket--use1-az4--x-s3.s3express-use1-az4.us-east-1.amazonaws.com */
struct aws_byte_cursor g_test_s3express_bucket_use1_az4_endpoint = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL(
    "aws-c-s3-test-bucket--use1-az4--x-s3.s3express-use1-az4.us-east-1.amazonaws.com");

#ifdef BYO_CRYPTO
/* Under BYO_CRYPTO, this function currently needs to be defined by the user. Defining a null implementation here so
 * that tests build, but it is not currently meant to be used by any tests. */
struct aws_byte_buf aws_tls_handler_protocol(struct aws_channel_handler *handler) {
    (void)handler;
    AWS_FATAL_ASSERT(false);
    struct aws_byte_buf byte_buf;
    AWS_ZERO_STRUCT(byte_buf);
    return byte_buf;
}
#endif

static int s_s3_test_meta_request_header_callback(
    struct aws_s3_meta_request *meta_request,
    const struct aws_http_headers *headers,
    int response_status,
    void *user_data) {
    (void)meta_request;

    struct aws_s3_meta_request_test_results *meta_request_test_results =
        (struct aws_s3_meta_request_test_results *)user_data;

    aws_http_headers_release(meta_request_test_results->response_headers);

    meta_request_test_results->response_headers = (struct aws_http_headers *)headers;
    aws_http_headers_acquire(meta_request_test_results->response_headers);

    meta_request_test_results->headers_response_status = response_status;

    if (meta_request_test_results->headers_callback != NULL) {
        return meta_request_test_results->headers_callback(meta_request, headers, response_status, user_data);
    }

    return AWS_OP_SUCCESS;
}

static int s_s3_test_meta_request_body_callback(
    struct aws_s3_meta_request *meta_request,
    const struct aws_byte_cursor *body,
    uint64_t range_start,
    void *user_data) {
    (void)meta_request;
    (void)body;
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(body);

    struct aws_s3_meta_request_test_results *meta_request_test_results = user_data;
    meta_request_test_results->received_body_size += body->len;
    aws_atomic_fetch_add(&meta_request_test_results->received_body_size_delta, body->len);

    AWS_LOGF_DEBUG(
        AWS_LS_S3_GENERAL,
        "Received range %" PRIu64 "-%" PRIu64 ". Expected range start: %" PRIu64,
        range_start,
        range_start + body->len - 1,
        meta_request_test_results->expected_range_start);

    uint64_t object_range_start = 0;

    /* If this is an auto-ranged-get meta request, then grab the object range start so that the expected_range_start can
     * be properly offset.*/
    if (meta_request->type == AWS_S3_META_REQUEST_TYPE_GET_OBJECT && meta_request->part_size != 0) {

        aws_s3_meta_request_lock_synced_data(meta_request);

        struct aws_s3_auto_ranged_get *auto_ranged_get = meta_request->impl;
        AWS_PRECONDITION(auto_ranged_get);

        bool object_range_known = auto_ranged_get->synced_data.object_range_known != 0;
        object_range_start = auto_ranged_get->synced_data.object_range_start;

        aws_s3_meta_request_unlock_synced_data(meta_request);

        ASSERT_TRUE(object_range_known);
    }

    ASSERT_TRUE((object_range_start + meta_request_test_results->expected_range_start) == range_start);
    meta_request_test_results->expected_range_start += body->len;

    if (meta_request_test_results->body_callback != NULL) {
        return meta_request_test_results->body_callback(meta_request, body, range_start, user_data);
    }

    return AWS_OP_SUCCESS;
}

static void s_s3_test_meta_request_finish(
    struct aws_s3_meta_request *meta_request,
    const struct aws_s3_meta_request_result *result,
    void *user_data) {
    (void)meta_request;

    struct aws_s3_meta_request_test_results *meta_request_test_results = user_data;
    struct aws_s3_tester *tester = meta_request_test_results->tester;

    meta_request_test_results->error_response_headers = result->error_response_headers;

    if (result->error_response_headers != NULL) {
        aws_http_headers_acquire(result->error_response_headers);
    }

    if (result->error_response_body != NULL) {
        aws_byte_buf_init_copy(
            &meta_request_test_results->error_response_body, tester->allocator, result->error_response_body);
    }

    if (result->error_response_operation_name != NULL) {
        meta_request_test_results->error_response_operation_name =
            aws_string_new_from_string(tester->allocator, result->error_response_operation_name);
    }

    meta_request_test_results->finished_response_status = result->response_status;
    meta_request_test_results->finished_error_code = result->error_code;

    if (meta_request_test_results->finish_callback != NULL) {
        meta_request_test_results->finish_callback(meta_request, result, user_data);
    }

    aws_s3_tester_notify_meta_request_finished(tester, result);
}

static void s_s3_test_meta_request_shutdown(void *user_data) {
    struct aws_s3_meta_request_test_results *meta_request_test_results = user_data;
    struct aws_s3_tester *tester = meta_request_test_results->tester;

    aws_s3_tester_notify_meta_request_shutdown(tester);
}
static void s_s3_test_meta_request_telemetry(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request_metrics *metrics,
    void *user_data) {
    (void)meta_request;
    struct aws_s3_meta_request_test_results *meta_request_test_results = user_data;
    struct aws_s3_tester *tester = meta_request_test_results->tester;
    uint64_t time_stamp = 0;
    aws_s3_request_metrics_get_start_timestamp_ns(metrics, &time_stamp);
    AWS_FATAL_ASSERT(time_stamp > 0);
    aws_s3_request_metrics_get_end_timestamp_ns(metrics, &time_stamp);
    AWS_FATAL_ASSERT(time_stamp > 0);
    aws_s3_request_metrics_get_total_duration_ns(metrics, &time_stamp);
    AWS_FATAL_ASSERT(time_stamp > 0);
    if (!aws_s3_request_metrics_get_send_end_timestamp_ns(metrics, &time_stamp)) {
        AWS_FATAL_ASSERT(time_stamp > 0);
        uint64_t start_time = 0;
        uint64_t end_time = 0;
        uint64_t during_time = 0;
        int error = 0;
        error |= aws_s3_request_metrics_get_send_start_timestamp_ns(metrics, &start_time);
        error |= aws_s3_request_metrics_get_send_end_timestamp_ns(metrics, &end_time);
        error |= aws_s3_request_metrics_get_sending_duration_ns(metrics, &during_time);
        AWS_FATAL_ASSERT(error == AWS_OP_SUCCESS);
        AWS_FATAL_ASSERT(during_time == (end_time - start_time));
    }
    if (!aws_s3_request_metrics_get_receive_end_timestamp_ns(metrics, &time_stamp)) {
        AWS_FATAL_ASSERT(time_stamp > 0);
        uint64_t start_time = 0;
        uint64_t end_time = 0;
        uint64_t during_time = 0;
        int error = 0;
        error |= aws_s3_request_metrics_get_receive_start_timestamp_ns(metrics, &start_time);
        error |= aws_s3_request_metrics_get_receive_end_timestamp_ns(metrics, &end_time);
        error |= aws_s3_request_metrics_get_receiving_duration_ns(metrics, &during_time);
        AWS_FATAL_ASSERT(error == AWS_OP_SUCCESS);
        AWS_FATAL_ASSERT(during_time == (end_time - start_time));
    }

    aws_s3_tester_lock_synced_data(tester);
    aws_array_list_push_back(&meta_request_test_results->synced_data.metrics, &metrics);
    aws_s3_request_metrics_acquire(metrics);
    aws_s3_tester_unlock_synced_data(tester);
}

static void s_s3_test_meta_request_progress(
    struct aws_s3_meta_request *meta_request,
    const struct aws_s3_meta_request_progress *progress,
    void *user_data) {

    (void)meta_request;
    AWS_ASSERT(meta_request);
    AWS_ASSERT(progress);
    AWS_ASSERT(user_data);

    struct aws_s3_meta_request_test_results *meta_request_test_results = user_data;

    meta_request_test_results->progress.total_bytes_transferred += progress->bytes_transferred;

    /* Once content_length is reported, it shouldn't change */
    if (meta_request_test_results->progress.content_length == 0) {
        meta_request_test_results->progress.content_length = progress->content_length;
    } else {
        AWS_FATAL_ASSERT(meta_request_test_results->progress.content_length == progress->content_length);
    }

    /* If content_length is known, we shouldn't go over it */
    if (progress->content_length != 0) {
        AWS_FATAL_ASSERT(meta_request_test_results->progress.total_bytes_transferred <= progress->content_length);
    }

    if (meta_request_test_results->progress_callback != NULL) {
        meta_request_test_results->progress_callback(meta_request, progress, user_data);
    }
}

static int s_s3_test_meta_request_upload_review(
    struct aws_s3_meta_request *meta_request,
    const struct aws_s3_upload_review *review,
    void *user_data) {

    struct aws_s3_meta_request_test_results *test_results = user_data;

    AWS_FATAL_ASSERT(test_results->upload_review.invoked_count == 0);
    test_results->upload_review.invoked_count++;

    test_results->upload_review.checksum_algorithm = review->checksum_algorithm;

    test_results->upload_review.part_count = review->part_count;
    if (test_results->upload_review.part_count > 0) {
        test_results->upload_review.part_sizes_array =
            aws_mem_calloc(test_results->allocator, review->part_count, sizeof(uint64_t));

        test_results->upload_review.part_checksums_array =
            aws_mem_calloc(test_results->allocator, review->part_count, sizeof(struct aws_string *));

        for (size_t i = 0; i < review->part_count; ++i) {
            test_results->upload_review.part_sizes_array[i] = review->part_array[i].size;
            test_results->upload_review.part_checksums_array[i] =
                aws_string_new_from_cursor(test_results->allocator, &review->part_array[i].checksum);
        }
    }

    if (test_results->upload_review_callback != NULL) {
        return test_results->upload_review_callback(meta_request, review, user_data);
    } else {
        return AWS_OP_SUCCESS;
    }
}

/* Notify the tester that a particular clean up step has finished. */
static void s_s3_test_client_shutdown(void *user_data);

static bool s_s3_tester_have_meta_requests_finished(void *user_data);

static bool s_s3_tester_has_client_shutdown(void *user_data);

struct aws_string *aws_s3_tester_build_endpoint_string(
    struct aws_allocator *allocator,
    const struct aws_byte_cursor *bucket_name,
    const struct aws_byte_cursor *region) {

    struct aws_byte_cursor endpoint_url_part0 = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL(".s3.");
    struct aws_byte_cursor endpoint_url_part1 = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL(".amazonaws.com");

    struct aws_byte_buf endpoint_buffer;
    aws_byte_buf_init(&endpoint_buffer, allocator, 128);

    aws_byte_buf_append_dynamic(&endpoint_buffer, bucket_name);
    aws_byte_buf_append_dynamic(&endpoint_buffer, &endpoint_url_part0);
    aws_byte_buf_append_dynamic(&endpoint_buffer, region);
    aws_byte_buf_append_dynamic(&endpoint_buffer, &endpoint_url_part1);

    struct aws_string *endpoint_string = aws_string_new_from_buf(allocator, &endpoint_buffer);

    aws_byte_buf_clean_up(&endpoint_buffer);

    return endpoint_string;
}

AWS_STATIC_STRING_FROM_LITERAL(s_bucket_name_env_var, "CRT_S3_TEST_BUCKET_NAME");

int aws_s3_tester_init(struct aws_allocator *allocator, struct aws_s3_tester *tester) {

    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(tester);

    (void)allocator;

    AWS_ZERO_STRUCT(*tester);

    tester->allocator = allocator;
    if (aws_get_environment_value(allocator, s_bucket_name_env_var, &tester->bucket_name) == AWS_OP_SUCCESS &&
        tester->bucket_name != NULL) {
        g_test_bucket_name = aws_byte_cursor_from_string(tester->bucket_name);
        char public_bucket_name_buffer[128] = "";
        snprintf(
            public_bucket_name_buffer,
            sizeof(public_bucket_name_buffer),
            "" PRInSTR "-public",
            AWS_BYTE_CURSOR_PRI(g_test_bucket_name));
        tester->public_bucket_name = aws_string_new_from_c_str(allocator, public_bucket_name_buffer);
        g_test_public_bucket_name = aws_byte_cursor_from_string(tester->public_bucket_name);

        char s3express_bucket_usw2_az1_endpoint_buffer[512] = "";
        snprintf(
            s3express_bucket_usw2_az1_endpoint_buffer,
            sizeof(s3express_bucket_usw2_az1_endpoint_buffer),
            "" PRInSTR "--usw2-az1--x-s3.s3express-usw2-az1.us-west-2.amazonaws.com",
            AWS_BYTE_CURSOR_PRI(g_test_bucket_name));
        tester->s3express_bucket_usw2_az1_endpoint =
            aws_string_new_from_c_str(allocator, s3express_bucket_usw2_az1_endpoint_buffer);
        g_test_s3express_bucket_usw2_az1_endpoint =
            aws_byte_cursor_from_string(tester->s3express_bucket_usw2_az1_endpoint);

        char s3express_bucket_use1_az4_name_buffer[128] = "";
        snprintf(
            s3express_bucket_use1_az4_name_buffer,
            sizeof(s3express_bucket_use1_az4_name_buffer),
            "" PRInSTR "--use1-az4--x-s3.s3express-use1-az4.us-east-1.amazonaws.com",
            AWS_BYTE_CURSOR_PRI(g_test_bucket_name));
        tester->s3express_bucket_use1_az4_endpoint =
            aws_string_new_from_c_str(allocator, s3express_bucket_use1_az4_name_buffer);
        g_test_s3express_bucket_use1_az4_endpoint =
            aws_byte_cursor_from_string(tester->s3express_bucket_use1_az4_endpoint);
    }

    aws_s3_library_init(allocator);

    if (aws_mutex_init(&tester->synced_data.lock)) {
        return AWS_OP_ERR;
    }

    if (aws_condition_variable_init(&tester->signal)) {
        goto condition_variable_failed;
    }

    ASSERT_SUCCESS(aws_array_list_init_dynamic(
        &tester->client_vtable_patches, tester->allocator, 4, sizeof(struct aws_s3_client_vtable_patch)));

    ASSERT_SUCCESS(aws_array_list_init_dynamic(
        &tester->meta_request_vtable_patches, tester->allocator, 4, sizeof(struct aws_s3_meta_request_vtable_patch)));

    /* Setup an event loop group and host resolver. */
    tester->el_group = aws_event_loop_group_new_default(allocator, 0, NULL);
    ASSERT_TRUE(tester->el_group != NULL);

    struct aws_host_resolver_default_options resolver_options = {
        .max_entries = 10,
        .el_group = tester->el_group,
    };
    tester->host_resolver = aws_host_resolver_new_default(allocator, &resolver_options);
    ASSERT_TRUE(tester->host_resolver != NULL);

    /* Setup the client boot strap. */
    {
        struct aws_client_bootstrap_options bootstrap_options;
        AWS_ZERO_STRUCT(bootstrap_options);
        bootstrap_options.event_loop_group = tester->el_group;
        bootstrap_options.host_resolver = tester->host_resolver;
        bootstrap_options.user_data = tester;

        tester->client_bootstrap = aws_client_bootstrap_new(allocator, &bootstrap_options);
    }

    tester->anonymous_creds = aws_credentials_new_anonymous(allocator);
    tester->anonymous_signing_config.credentials = tester->anonymous_creds;
#ifndef BYO_CRYPTO
    /* Setup the credentials provider */
    {
        struct aws_credentials_provider_chain_default_options credentials_config;
        AWS_ZERO_STRUCT(credentials_config);
        credentials_config.bootstrap = tester->client_bootstrap;
        tester->credentials_provider = aws_credentials_provider_new_chain_default(allocator, &credentials_config);
        aws_s3_init_default_signing_config(
            &tester->default_signing_config, g_test_s3_region, tester->credentials_provider);
    }
#else
    {
        tester->default_signing_config = tester->anonymous_signing_config;
    }
#endif

    return AWS_OP_SUCCESS;

condition_variable_failed:

    aws_mutex_clean_up(&tester->synced_data.lock);

    return AWS_OP_ERR;
}

int aws_s3_tester_bind_client(struct aws_s3_tester *tester, struct aws_s3_client_config *config, uint32_t flags) {
    AWS_PRECONDITION(tester);
    AWS_PRECONDITION(config);

    ASSERT_TRUE(!tester->bound_to_client);
    tester->bound_to_client = true;

    ASSERT_TRUE(config->client_bootstrap == NULL);
    config->client_bootstrap = tester->client_bootstrap;

    if (flags & AWS_S3_TESTER_BIND_CLIENT_SIGNING) {
        ASSERT_TRUE(config->signing_config == NULL);
        config->signing_config = &tester->default_signing_config;
    }
    if (flags & AWS_S3_TESTER_BIND_CLIENT_REGION) {
        ASSERT_TRUE(config->region.len == 0);
        config->region = g_test_s3_region;
    } else {
        if (config->signing_config) {
            config->signing_config->region = config->region;
        }
    }
    if (!config->signing_config) {
        config->signing_config = &tester->anonymous_signing_config;
    }

    ASSERT_TRUE(config->shutdown_callback == NULL);
    config->shutdown_callback = s_s3_test_client_shutdown;

    ASSERT_TRUE(config->shutdown_callback_user_data == NULL);
    config->shutdown_callback_user_data = tester;

    return AWS_OP_SUCCESS;
}

int aws_s3_tester_bind_meta_request(
    struct aws_s3_tester *tester,
    struct aws_s3_meta_request_options *options,
    struct aws_s3_meta_request_test_results *meta_request_test_results) {

    meta_request_test_results->tester = tester;

    aws_s3_tester_lock_synced_data(tester);
    ++tester->synced_data.desired_meta_request_finish_count;
    ++tester->synced_data.desired_meta_request_shutdown_count;
    aws_s3_tester_unlock_synced_data(tester);

    ASSERT_TRUE(options->headers_callback == NULL);
    options->headers_callback = s_s3_test_meta_request_header_callback;

    ASSERT_TRUE(options->body_callback == NULL);
    options->body_callback = s_s3_test_meta_request_body_callback;

    ASSERT_TRUE(options->finish_callback == NULL);
    options->finish_callback = s_s3_test_meta_request_finish;

    ASSERT_TRUE(options->shutdown_callback == NULL);
    options->shutdown_callback = s_s3_test_meta_request_shutdown;

    ASSERT_TRUE(options->telemetry_callback == NULL);
    options->telemetry_callback = s_s3_test_meta_request_telemetry;

    ASSERT_TRUE(options->progress_callback == NULL);
    options->progress_callback = s_s3_test_meta_request_progress;

    ASSERT_TRUE(options->upload_review_callback == NULL);
    options->upload_review_callback = s_s3_test_meta_request_upload_review;

    ASSERT_TRUE(options->user_data == NULL);
    options->user_data = meta_request_test_results;

    return AWS_OP_SUCCESS;
}

void aws_s3_meta_request_test_results_init(
    struct aws_s3_meta_request_test_results *test_meta_request,
    struct aws_allocator *allocator) {

    AWS_ZERO_STRUCT(*test_meta_request);
    test_meta_request->allocator = allocator;
    aws_atomic_init_int(&test_meta_request->received_body_size_delta, 0);
    aws_array_list_init_dynamic(
        &test_meta_request->synced_data.metrics, allocator, 4, sizeof(struct aws_s3_request_metrics *));
}

void aws_s3_meta_request_test_results_clean_up(struct aws_s3_meta_request_test_results *test_meta_request) {
    if (test_meta_request == NULL) {
        return;
    }

    aws_http_headers_release(test_meta_request->error_response_headers);
    aws_byte_buf_clean_up(&test_meta_request->error_response_body);
    aws_string_destroy(test_meta_request->error_response_operation_name);
    aws_http_headers_release(test_meta_request->response_headers);
    while (aws_array_list_length(&test_meta_request->synced_data.metrics) > 0) {
        struct aws_s3_request_metrics *metrics = NULL;
        aws_array_list_back(&test_meta_request->synced_data.metrics, (void **)&metrics);
        aws_array_list_pop_back(&test_meta_request->synced_data.metrics);
        aws_s3_request_metrics_release(metrics);
    }
    aws_array_list_clean_up(&test_meta_request->synced_data.metrics);

    for (size_t i = 0; i < test_meta_request->upload_review.part_count; ++i) {
        aws_string_destroy(test_meta_request->upload_review.part_checksums_array[i]);
    }
    aws_mem_release(test_meta_request->allocator, test_meta_request->upload_review.part_sizes_array);
    aws_mem_release(test_meta_request->allocator, test_meta_request->upload_review.part_checksums_array);

    AWS_ZERO_STRUCT(*test_meta_request);
}

void aws_s3_tester_notify_meta_request_finished(
    struct aws_s3_tester *tester,
    const struct aws_s3_meta_request_result *result) {
    AWS_PRECONDITION(tester);

    bool notify = false;

    aws_s3_tester_lock_synced_data(tester);
    ++tester->synced_data.meta_request_finish_count;

    int error_code = AWS_ERROR_SUCCESS;

    if (result != NULL) {
        error_code = result->error_code;
    }

    if (tester->synced_data.desired_meta_request_finish_count == 0 ||
        tester->synced_data.meta_request_finish_count == tester->synced_data.desired_meta_request_finish_count ||
        (error_code != AWS_ERROR_SUCCESS)) {

        tester->synced_data.meta_requests_finished = true;
        tester->synced_data.finish_error_code = error_code;

        notify = true;
    }

    aws_s3_tester_unlock_synced_data(tester);

    if (notify) {
        aws_condition_variable_notify_all(&tester->signal);
    }
}

static bool s_s3_tester_have_meta_requests_finished(void *user_data) {
    AWS_PRECONDITION(user_data);
    struct aws_s3_tester *tester = (struct aws_s3_tester *)user_data;

    return tester->synced_data.meta_requests_finished > 0;
}

void aws_s3_tester_wait_for_meta_request_finish(struct aws_s3_tester *tester) {
    AWS_PRECONDITION(tester);

    aws_s3_tester_lock_synced_data(tester);
    aws_condition_variable_wait_pred(
        &tester->signal, &tester->synced_data.lock, s_s3_tester_have_meta_requests_finished, tester);

    tester->synced_data.meta_requests_finished = false;
    aws_s3_tester_unlock_synced_data(tester);
}

void aws_s3_tester_notify_meta_request_shutdown(struct aws_s3_tester *tester) {
    bool notify = false;

    aws_s3_tester_lock_synced_data(tester);
    ++tester->synced_data.meta_request_shutdown_count;

    if (tester->synced_data.desired_meta_request_shutdown_count == 0 ||
        tester->synced_data.meta_request_shutdown_count == tester->synced_data.desired_meta_request_shutdown_count) {

        tester->synced_data.meta_requests_shutdown = true;
        notify = true;
    }

    aws_s3_tester_unlock_synced_data(tester);

    if (notify) {
        aws_condition_variable_notify_all(&tester->signal);
    }
}

static bool s_s3_tester_have_meta_requests_shutdown(void *user_data) {
    AWS_PRECONDITION(user_data);
    struct aws_s3_tester *tester = (struct aws_s3_tester *)user_data;

    return tester->synced_data.meta_requests_shutdown > 0;
}

void aws_s3_tester_wait_for_meta_request_shutdown(struct aws_s3_tester *tester) {
    AWS_PRECONDITION(tester);

    aws_s3_tester_lock_synced_data(tester);
    aws_condition_variable_wait_pred(
        &tester->signal, &tester->synced_data.lock, s_s3_tester_have_meta_requests_shutdown, tester);

    tester->synced_data.meta_requests_shutdown = false;
    aws_s3_tester_unlock_synced_data(tester);
}

static bool s_s3_tester_counters_equal_desired(void *user_data) {
    AWS_PRECONDITION(user_data);
    struct aws_s3_tester *tester = (struct aws_s3_tester *)user_data;

    return tester->synced_data.counter1 == tester->synced_data.desired_counter1 &&
           tester->synced_data.counter2 == tester->synced_data.desired_counter2;
}

void aws_s3_tester_wait_for_signal(struct aws_s3_tester *tester) {
    aws_s3_tester_lock_synced_data(tester);
    aws_condition_variable_wait(&tester->signal, &tester->synced_data.lock);
    aws_s3_tester_unlock_synced_data(tester);
}

void aws_s3_tester_notify_signal(struct aws_s3_tester *tester) {
    aws_condition_variable_notify_all(&tester->signal);
}

void aws_s3_tester_wait_for_counters(struct aws_s3_tester *tester) {
    aws_s3_tester_lock_synced_data(tester);
    aws_condition_variable_wait_pred(
        &tester->signal, &tester->synced_data.lock, s_s3_tester_counters_equal_desired, tester);
    aws_s3_tester_unlock_synced_data(tester);
}

size_t aws_s3_tester_inc_counter1(struct aws_s3_tester *tester) {
    aws_s3_tester_lock_synced_data(tester);
    size_t result = ++tester->synced_data.counter1;
    aws_s3_tester_unlock_synced_data(tester);

    aws_condition_variable_notify_all(&tester->signal);

    return result;
}

size_t aws_s3_tester_inc_counter2(struct aws_s3_tester *tester) {
    aws_s3_tester_lock_synced_data(tester);
    size_t result = ++tester->synced_data.counter2;
    aws_s3_tester_unlock_synced_data(tester);

    aws_condition_variable_notify_all(&tester->signal);

    return result;
}

void aws_s3_tester_reset_counter1(struct aws_s3_tester *tester) {
    aws_s3_tester_lock_synced_data(tester);
    tester->synced_data.counter1 = 0;
    aws_s3_tester_unlock_synced_data(tester);
}

void aws_s3_tester_reset_counter2(struct aws_s3_tester *tester) {
    aws_s3_tester_lock_synced_data(tester);
    tester->synced_data.counter2 = 0;
    aws_s3_tester_unlock_synced_data(tester);
}

void aws_s3_tester_set_counter1_desired(struct aws_s3_tester *tester, size_t value) {
    aws_s3_tester_lock_synced_data(tester);
    tester->synced_data.desired_counter1 = value;
    aws_s3_tester_unlock_synced_data(tester);
}

void aws_s3_tester_set_counter2_desired(struct aws_s3_tester *tester, size_t value) {
    aws_s3_tester_lock_synced_data(tester);
    tester->synced_data.desired_counter2 = value;
    aws_s3_tester_unlock_synced_data(tester);
}

void aws_s3_tester_clean_up(struct aws_s3_tester *tester) {
    AWS_PRECONDITION(tester);

    if (tester->bound_to_client) {
        aws_s3_tester_wait_for_client_shutdown(tester);
        tester->bound_to_client = false;
    }
    aws_string_destroy(tester->bucket_name);
    aws_string_destroy(tester->public_bucket_name);
    aws_string_destroy(tester->s3express_bucket_usw2_az1_endpoint);
    aws_string_destroy(tester->s3express_bucket_use1_az4_endpoint);

    aws_credentials_release(tester->anonymous_creds);

    aws_array_list_clean_up(&tester->client_vtable_patches);
    aws_array_list_clean_up(&tester->meta_request_vtable_patches);

    aws_client_bootstrap_release(tester->client_bootstrap);
    tester->client_bootstrap = NULL;

    aws_credentials_provider_release(tester->credentials_provider);
    tester->credentials_provider = NULL;

    aws_host_resolver_release(tester->host_resolver);
    tester->host_resolver = NULL;

    aws_event_loop_group_release(tester->el_group);
    tester->el_group = NULL;

    aws_s3_library_clean_up();

    aws_condition_variable_clean_up(&tester->signal);
    aws_mutex_clean_up(&tester->synced_data.lock);
}

void aws_s3_tester_lock_synced_data(struct aws_s3_tester *tester) {
    AWS_PRECONDITION(tester);
    aws_mutex_lock(&tester->synced_data.lock);
}

void aws_s3_tester_unlock_synced_data(struct aws_s3_tester *tester) {
    AWS_PRECONDITION(tester);

    aws_mutex_unlock(&tester->synced_data.lock);
}

struct aws_s3_meta_request *s_s3_client_meta_request_factory_empty(
    struct aws_s3_client *client,
    const struct aws_s3_meta_request_options *options) {
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(options);

    (void)client;
    (void)options;

    return NULL;
}

void s_s3_client_create_connection_for_request_empty(struct aws_s3_client *client, struct aws_s3_request *request) {
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(request);

    (void)client;
    (void)request;
}

static void s_s3_client_acquire_http_connection_empty(
    struct aws_http_connection_manager *conn_manager,
    aws_http_connection_manager_on_connection_setup_fn *on_connection_acquired_callback,
    void *user_data) {
    (void)conn_manager;
    (void)on_connection_acquired_callback;
    (void)user_data;
}

size_t s_s3_client_get_host_address_count_empty(
    struct aws_host_resolver *host_resolver,
    const struct aws_string *host_name,
    uint32_t flags) {
    (void)host_resolver;
    (void)host_name;
    (void)flags;
    return 0;
}

static void s_s3_client_schedule_process_work_synced_empty(struct aws_s3_client *client) {
    (void)client;
}

static void s_s3_client_process_work_empty(struct aws_s3_client *client) {
    AWS_PRECONDITION(client);
    (void)client;
}

static void s_s3_client_endpoint_shutdown_callback_empty(struct aws_s3_client *client) {
    AWS_PRECONDITION(client);
    (void)client;
}

static void s_s3_client_finish_destroy_empty(struct aws_s3_client *client) {
    AWS_PRECONDITION(client);
    (void)client;
}

struct aws_s3_client_vtable g_aws_s3_client_mock_vtable = {
    .meta_request_factory = s_s3_client_meta_request_factory_empty,
    .create_connection_for_request = s_s3_client_create_connection_for_request_empty,
    .acquire_http_connection = s_s3_client_acquire_http_connection_empty,
    .get_host_address_count = s_s3_client_get_host_address_count_empty,
    .schedule_process_work_synced = s_s3_client_schedule_process_work_synced_empty,
    .process_work = s_s3_client_process_work_empty,
    .endpoint_shutdown_callback = s_s3_client_endpoint_shutdown_callback_empty,
    .finish_destroy = s_s3_client_finish_destroy_empty,
};

static void s_s3_mock_client_start_destroy(void *user_data) {
    struct aws_s3_client *client = user_data;
    AWS_ASSERT(client);

    aws_s3_buffer_pool_destroy(client->buffer_pool);
    aws_mem_release(client->allocator, client);
}

struct aws_s3_client *aws_s3_tester_mock_client_new(struct aws_s3_tester *tester) {
    struct aws_allocator *allocator = tester->allocator;
    struct aws_s3_client *mock_client = aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_client));

    mock_client->allocator = allocator;
    mock_client->buffer_pool = aws_s3_buffer_pool_new(allocator, MB_TO_BYTES(8), GB_TO_BYTES(1));
    mock_client->vtable = &g_aws_s3_client_mock_vtable;

    aws_ref_count_init(
        &mock_client->ref_count, mock_client, (aws_simple_completion_callback *)s_s3_mock_client_start_destroy);

    aws_mutex_init(&mock_client->synced_data.lock);

    aws_atomic_init_int(&mock_client->stats.num_requests_in_flight, 0);

    for (uint32_t i = 0; i < (uint32_t)AWS_S3_META_REQUEST_TYPE_MAX; ++i) {
        aws_atomic_init_int(&mock_client->stats.num_requests_network_io[i], 0);
    }

    aws_atomic_init_int(&mock_client->stats.num_requests_stream_queued_waiting, 0);
    aws_atomic_init_int(&mock_client->stats.num_requests_streaming_response, 0);

    return mock_client;
}

struct aws_http_message *aws_s3_tester_dummy_http_request_new(struct aws_s3_tester *tester) {
    AWS_PRECONDITION(tester);

    struct aws_http_message *message = aws_http_message_new_request(tester->allocator);
    aws_http_message_set_request_method(message, aws_http_method_get);
    aws_http_message_set_request_path(message, aws_byte_cursor_from_c_str("/dummy_test"));

    struct aws_http_header host_header = {
        .name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Host"),
        .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("dummy_host"),
    };

    aws_http_message_add_header(message, host_header);

    return message;
}

static bool s_s3_meta_request_update_empty(
    struct aws_s3_meta_request *meta_request,
    uint32_t flags,
    struct aws_s3_request **out_request) {
    (void)meta_request;
    (void)flags;
    (void)out_request;
    return false;
}

void s_s3_meta_request_send_request_finish_empty(
    struct aws_s3_connection *connection,
    struct aws_http_stream *stream,
    int error_code) {
    (void)connection;
    (void)stream;
    (void)error_code;
}

static void s_s3_meta_request_finished_request_empty(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    int error_code) {
    (void)meta_request;
    (void)request;
    (void)error_code;
}

static void s_s3_meta_request_schedule_prepare_request_empty(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    aws_s3_meta_request_prepare_request_callback_fn *callback,
    void *user_data) {
    (void)meta_request;
    (void)request;
    (void)callback;
    (void)user_data;
}

static struct aws_future_void *s_s3_meta_request_prepare_request_async_empty(struct aws_s3_request *request) {
    struct aws_future_void *future = aws_future_void_new(request->allocator);
    aws_future_void_set_error(future, AWS_ERROR_UNKNOWN);
    return future;
}

static void s_s3_meta_request_init_signing_date_time_empty(
    struct aws_s3_meta_request *meta_request,
    struct aws_date_time *date_time) {
    (void)meta_request;
    (void)date_time;
}

static void s_s3_meta_request_sign_request_empty(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    aws_signing_complete_fn *on_signing_complete,
    void *user_data) {
    (void)meta_request;
    (void)request;
    (void)on_signing_complete;
    (void)user_data;
}

static void s_s3_mock_meta_request_destroy(struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);

    aws_mem_release(meta_request->allocator, meta_request->impl);
}

static struct aws_s3_meta_request_vtable s_s3_mock_meta_request_vtable = {
    .update = s_s3_meta_request_update_empty,
    .send_request_finish = s_s3_meta_request_send_request_finish_empty,
    .schedule_prepare_request = s_s3_meta_request_schedule_prepare_request_empty,
    .prepare_request = s_s3_meta_request_prepare_request_async_empty,
    .finished_request = s_s3_meta_request_finished_request_empty,
    .init_signing_date_time = s_s3_meta_request_init_signing_date_time_empty,
    .sign_request = s_s3_meta_request_sign_request_empty,
    .destroy = s_s3_mock_meta_request_destroy,
};

struct aws_s3_empty_meta_request {
    struct aws_s3_meta_request base;
};

static void s_s3_mock_endpoint_acquire(struct aws_s3_endpoint *endpoint, bool already_holding_lock) {
    (void)already_holding_lock;
    ++endpoint->client_synced_data.ref_count;
}

static void s_s3_mock_endpoint_release(struct aws_s3_endpoint *endpoint) {
    if (--endpoint->client_synced_data.ref_count == 0) {
        aws_string_destroy(endpoint->host_name);
        aws_mem_release(endpoint->allocator, endpoint);
    }
}

static struct aws_s3_endpoint_system_vtable s_s3_mock_endpoint_vtable = {
    .acquire = s_s3_mock_endpoint_acquire,
    .release = s_s3_mock_endpoint_release,
};

struct aws_s3_endpoint *aws_s3_tester_mock_endpoint_new(struct aws_s3_tester *tester) {
    aws_s3_endpoint_set_system_vtable(&s_s3_mock_endpoint_vtable);

    struct aws_s3_endpoint *endpoint = aws_mem_calloc(tester->allocator, 1, sizeof(struct aws_s3_endpoint));
    endpoint->allocator = tester->allocator;
    endpoint->client_synced_data.ref_count = 1;

    struct aws_byte_cursor empty_cursor = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("");
    endpoint->host_name = aws_string_new_from_cursor(tester->allocator, &empty_cursor);

    return endpoint;
}

/* Mock request defaults to GET request */
struct aws_s3_meta_request *aws_s3_tester_mock_meta_request_new(struct aws_s3_tester *tester) {
    AWS_PRECONDITION(tester);

    struct aws_s3_empty_meta_request *empty_meta_request =
        aws_mem_calloc(tester->allocator, 1, sizeof(struct aws_s3_empty_meta_request));

    struct aws_http_message *dummy_http_message = aws_s3_tester_dummy_http_request_new(tester);

    struct aws_s3_meta_request_options options = {
        .message = dummy_http_message,
        .type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
    };

    aws_s3_meta_request_init_base(
        tester->allocator,
        NULL,
        0,
        false,
        &options,
        empty_meta_request,
        &s_s3_mock_meta_request_vtable,
        &empty_meta_request->base);

    aws_http_message_release(dummy_http_message);

    return &empty_meta_request->base;
}

void aws_s3_create_test_buffer(struct aws_allocator *allocator, size_t buffer_size, struct aws_byte_buf *out_buf) {
    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(out_buf);

    struct aws_byte_cursor test_string = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("This is an S3 test.");

    aws_byte_buf_init(out_buf, allocator, buffer_size);

    for (size_t buffer_pos = 0; buffer_pos < buffer_size; buffer_pos += test_string.len) {
        size_t buffer_size_remaining = buffer_size - buffer_pos;
        size_t string_copy_size = test_string.len;

        if (buffer_size_remaining < string_copy_size) {
            string_copy_size = buffer_size_remaining;
        }

        struct aws_byte_cursor from_byte_cursor = {.len = string_copy_size, .ptr = test_string.ptr};

        aws_byte_buf_append(out_buf, &from_byte_cursor);
    }
}

static void s_s3_test_client_shutdown(void *user_data) {
    AWS_PRECONDITION(user_data);

    struct aws_s3_tester *tester = (struct aws_s3_tester *)user_data;

    aws_s3_tester_lock_synced_data(tester);
    tester->synced_data.client_shutdown = true;
    aws_s3_tester_unlock_synced_data(tester);

    aws_condition_variable_notify_all(&tester->signal);
}

static bool s_s3_tester_has_client_shutdown(void *user_data) {
    AWS_PRECONDITION(user_data);
    struct aws_s3_tester *tester = (struct aws_s3_tester *)user_data;

    return tester->synced_data.client_shutdown > 0;
}

void aws_s3_tester_wait_for_client_shutdown(struct aws_s3_tester *tester) {
    AWS_PRECONDITION(tester);

    aws_s3_tester_lock_synced_data(tester);
    aws_condition_variable_wait_pred(
        &tester->signal, &tester->synced_data.lock, s_s3_tester_has_client_shutdown, tester);

    tester->synced_data.client_shutdown = false;
    aws_s3_tester_unlock_synced_data(tester);
}
struct aws_http_message *aws_s3_test_get_object_request_new(
    struct aws_allocator *allocator,
    struct aws_byte_cursor host,
    struct aws_byte_cursor key) {

    struct aws_http_message *message = aws_http_message_new_request(allocator);

    if (message == NULL) {
        return NULL;
    }

    struct aws_http_header host_header = {.name = g_host_header_name, .value = host};

    if (aws_http_message_add_header(message, host_header)) {
        goto error_clean_up_message;
    }

    if (aws_http_message_set_request_method(message, aws_http_method_get)) {
        goto error_clean_up_message;
    }

    if (aws_http_message_set_request_path(message, key)) {
        goto error_clean_up_message;
    }

    return message;

error_clean_up_message:

    if (message != NULL) {
        aws_http_message_release(message);
        message = NULL;
    }

    return NULL;
}

struct aws_s3_client_vtable *aws_s3_tester_patch_client_vtable(
    struct aws_s3_tester *tester,
    struct aws_s3_client *client,
    size_t *out_index) {

    struct aws_s3_client_vtable_patch patch;
    AWS_ZERO_STRUCT(patch);

    /* Push a new vtable patch into the array. */
    aws_array_list_push_back(&tester->client_vtable_patches, (void *)&patch);

    /* Get a pointer to the new vtable patch. */
    size_t index = aws_array_list_length(&tester->client_vtable_patches) - 1;
    struct aws_s3_client_vtable_patch *patch_array_ptr = aws_s3_tester_get_client_vtable_patch(tester, index);

    /* Cache a pointer to the original vtable. */
    patch_array_ptr->original_vtable = client->vtable;

    /* Copy the original vtable contents into the patched vtable. */
    memcpy(&patch_array_ptr->patched_vtable, patch_array_ptr->original_vtable, sizeof(struct aws_s3_client_vtable));

    /* Point the client at the new vtable. */
    client->vtable = &patch_array_ptr->patched_vtable;

    if (out_index) {
        *out_index = index;
    }

    return &patch_array_ptr->patched_vtable;
}

struct aws_s3_client_vtable_patch *aws_s3_tester_get_client_vtable_patch(struct aws_s3_tester *tester, size_t index) {
    struct aws_s3_client_vtable_patch *patch = NULL;
    aws_array_list_get_at_ptr(&tester->client_vtable_patches, (void **)&patch, index);
    return patch;
}

struct aws_s3_meta_request_vtable *aws_s3_tester_patch_meta_request_vtable(
    struct aws_s3_tester *tester,
    struct aws_s3_meta_request *meta_request,
    size_t *out_index) {

    struct aws_s3_meta_request_vtable_patch patch;
    AWS_ZERO_STRUCT(patch);

    /* Push a new vtable patch into the array. */
    aws_array_list_push_back(&tester->meta_request_vtable_patches, (void *)&patch);

    /* Get a pointer to the new vtable patch. */
    size_t index = aws_array_list_length(&tester->meta_request_vtable_patches) - 1;
    struct aws_s3_meta_request_vtable_patch *patch_array_ptr =
        aws_s3_tester_get_meta_request_vtable_patch(tester, index);

    /* Cache a pointer to the original vtable. */
    patch_array_ptr->original_vtable = meta_request->vtable;

    /* Copy the original vtable contents into the patched vtable. */
    memcpy(
        &patch_array_ptr->patched_vtable, patch_array_ptr->original_vtable, sizeof(struct aws_s3_meta_request_vtable));

    /* Point the meta request at the new vtable. */
    meta_request->vtable = &patch_array_ptr->patched_vtable;

    if (out_index) {
        *out_index = index;
    }

    return &patch_array_ptr->patched_vtable;
}

struct aws_s3_meta_request_vtable_patch *aws_s3_tester_get_meta_request_vtable_patch(
    struct aws_s3_tester *tester,
    size_t index) {
    struct aws_s3_meta_request_vtable_patch *patch = NULL;
    aws_array_list_get_at_ptr(&tester->meta_request_vtable_patches, (void **)&patch, index);
    return patch;
}
struct aws_http_message *aws_s3_test_put_object_request_new_without_body(
    struct aws_allocator *allocator,
    struct aws_byte_cursor *host,
    struct aws_byte_cursor content_type,
    struct aws_byte_cursor key,
    uint64_t content_length,
    uint32_t flags) {

    AWS_PRECONDITION(allocator);

    struct aws_http_message *message = aws_http_message_new_request(allocator);

    if (message == NULL) {
        return NULL;
    }
    if (host) {
        struct aws_http_header host_header = {.name = g_host_header_name, .value = *host};
        if (aws_http_message_add_header(message, host_header)) {
            goto error_clean_up_message;
        }
    }
    struct aws_http_header content_type_header = {.name = g_content_type_header_name, .value = content_type};

    char content_length_buffer[64] = "";
    snprintf(content_length_buffer, sizeof(content_length_buffer), "%" PRIu64 "", content_length);

    struct aws_http_header content_length_header = {
        .name = g_content_length_header_name,
        .value = aws_byte_cursor_from_c_str(content_length_buffer),
    };

    struct aws_http_header sse_kms_header = {.name = g_s3_sse_header, .value = aws_byte_cursor_from_c_str("aws:kms")};
    struct aws_http_header sse_aes256_header = {.name = g_s3_sse_header, .value = aws_byte_cursor_from_c_str("AES256")};
    struct aws_http_header acl_public_read_header = {
        .name = g_acl_header_name,
        .value = aws_byte_cursor_from_c_str("bucket-owner-read"),
    };

    if (aws_http_message_add_header(message, content_type_header)) {
        goto error_clean_up_message;
    }

    if (aws_http_message_add_header(message, content_length_header)) {
        goto error_clean_up_message;
    }

    if (flags & AWS_S3_TESTER_SEND_META_REQUEST_SSE_KMS) {
        if (aws_http_message_add_header(message, sse_kms_header)) {
            goto error_clean_up_message;
        }
    }

    if (flags & AWS_S3_TESTER_SEND_META_REQUEST_SSE_AES256) {
        if (aws_http_message_add_header(message, sse_aes256_header)) {
            goto error_clean_up_message;
        }
    }

    if (flags & AWS_S3_TESTER_SEND_META_REQUEST_SSE_C_AES256) {
        struct aws_http_header sse_c_alg_header = {
            .name = g_s3_sse_c_alg_header,
            .value = aws_byte_cursor_from_c_str("AES256"),
        };
        struct aws_http_header sse_c_key_header = {
            .name = g_s3_sse_c_key_header,
            .value = aws_byte_cursor_from_c_str("MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDE="),
        };
        struct aws_http_header sse_c_key_md5_header = {
            .name = g_s3_sse_c_key_md5_header,
            .value = aws_byte_cursor_from_c_str("5inxltxhJQnqmmHfVSQjoA=="),
        };
        if (aws_http_message_add_header(message, sse_c_alg_header) ||
            aws_http_message_add_header(message, sse_c_key_header) ||
            aws_http_message_add_header(message, sse_c_key_md5_header)) {
            goto error_clean_up_message;
        }
    }

    if (flags & AWS_S3_TESTER_SEND_META_REQUEST_PUT_ACL) {
        if (aws_http_message_add_header(message, acl_public_read_header)) {
            goto error_clean_up_message;
        }
    }

    if (aws_http_message_set_request_method(message, aws_http_method_put)) {
        goto error_clean_up_message;
    }

    if (aws_http_message_set_request_path(message, key)) {
        goto error_clean_up_message;
    }

    return message;

error_clean_up_message:

    if (message != NULL) {
        aws_http_message_release(message);
        message = NULL;
    }

    return NULL;
}

struct aws_http_message *aws_s3_test_put_object_request_new(
    struct aws_allocator *allocator,
    struct aws_byte_cursor *host,
    struct aws_byte_cursor key,
    struct aws_byte_cursor content_type,
    struct aws_input_stream *body_stream,
    uint32_t flags) {

    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(body_stream);

    int64_t body_stream_length = 0;
    if (aws_input_stream_get_length(body_stream, &body_stream_length)) {
        return NULL;
    }

    struct aws_http_message *message = aws_s3_test_put_object_request_new_without_body(
        allocator, host, content_type, key, (uint64_t)body_stream_length, flags);
    if (!message) {
        return NULL;
    }

    aws_http_message_set_body_stream(message, body_stream);
    return message;
}

int aws_s3_tester_client_new(
    struct aws_s3_tester *tester,
    struct aws_s3_tester_client_options *options,
    struct aws_s3_client **out_client) {
    ASSERT_TRUE(tester != NULL);
    ASSERT_TRUE(options != NULL);
    ASSERT_TRUE(out_client != NULL);

    struct aws_s3_client_config client_config = {
        .part_size = options->part_size,
        .max_part_size = options->max_part_size,
    };
    struct aws_http_proxy_options proxy_options = {
        .connection_type = AWS_HPCT_HTTP_FORWARD,
        .host = aws_byte_cursor_from_c_str("localhost"),
        .port = 8899,
    };
    if (options->use_proxy) {
        client_config.proxy_options = &proxy_options;
    }

    struct aws_tls_connection_options tls_connection_options;
    AWS_ZERO_STRUCT(tls_connection_options);

#ifndef BYO_CRYPTO
    struct aws_tls_ctx_options tls_context_options;
    aws_tls_ctx_options_init_default_client(&tls_context_options, tester->allocator);

    struct aws_tls_ctx *context = aws_tls_client_ctx_new(tester->allocator, &tls_context_options);
    aws_tls_connection_options_init_from_ctx(&tls_connection_options, context);
#endif

    struct aws_string *endpoint =
        aws_s3_tester_build_endpoint_string(tester->allocator, &g_test_bucket_name, &g_test_s3_region);
    struct aws_byte_cursor endpoint_cursor = aws_byte_cursor_from_string(endpoint);

    tls_connection_options.server_name = aws_string_new_from_cursor(tester->allocator, &endpoint_cursor);

    switch (options->tls_usage) {
        case AWS_S3_TLS_ENABLED:
            client_config.tls_mode = AWS_MR_TLS_ENABLED;
            client_config.tls_connection_options = &tls_connection_options;
            break;
        case AWS_S3_TLS_DISABLED:
            client_config.tls_mode = AWS_MR_TLS_DISABLED;
            break;
        default:
            break;
    }

    ASSERT_SUCCESS(aws_s3_tester_bind_client(
        tester, &client_config, AWS_S3_TESTER_BIND_CLIENT_REGION | AWS_S3_TESTER_BIND_CLIENT_SIGNING));

    *out_client = aws_s3_client_new(tester->allocator, &client_config);

    aws_string_destroy(endpoint);

#ifndef BYO_CRYPTO
    aws_tls_ctx_release(context);
    aws_tls_ctx_options_clean_up(&tls_context_options);
#endif

    aws_tls_connection_options_clean_up(&tls_connection_options);

    return AWS_OP_SUCCESS;
}

/* Disable tsan as we hack into the client threaded data */
AWS_SUPPRESS_TSAN
static int s_tester_check_client_thread_data(struct aws_s3_client *client) {
    ASSERT_UINT_EQUALS(0, client->threaded_data.num_requests_being_prepared);
    ASSERT_UINT_EQUALS(0, client->threaded_data.request_queue_size);
    return AWS_OP_SUCCESS;
}

int aws_s3_tester_send_meta_request_with_options(
    struct aws_s3_tester *tester,
    struct aws_s3_tester_meta_request_options *options,
    struct aws_s3_meta_request_test_results *out_results) {
    ASSERT_TRUE(options != NULL);

    struct aws_allocator *allocator = options->allocator;

    struct aws_string *filepath_str = NULL;

    struct aws_s3_tester local_tester;
    AWS_ZERO_STRUCT(local_tester);
    bool clean_up_local_tester = false;

    if (tester == NULL) {
        ASSERT_TRUE(options->allocator);
        ASSERT_SUCCESS(aws_s3_tester_init(options->allocator, &local_tester));
        tester = &local_tester;
        clean_up_local_tester = true;
    } else if (allocator == NULL) {
        allocator = tester->allocator;
    }

    struct aws_s3_client *client = options->client;

    struct aws_uri mock_server;
    ASSERT_SUCCESS(aws_uri_init_parse(&mock_server, allocator, &g_mock_server_uri));
    if (client == NULL) {

        if (options->client_options != NULL) {
            ASSERT_SUCCESS(aws_s3_tester_client_new(tester, options->client_options, &client));
        } else {
            struct aws_s3_tester_client_options client_options;
            AWS_ZERO_STRUCT(client_options);
            ASSERT_SUCCESS(aws_s3_tester_client_new(tester, &client_options, &client));
        }

    } else {
        aws_s3_client_acquire(client);
    }

    bool disable_trailing_checksum =
        options->checksum_algorithm == AWS_SCA_NONE || options->disable_put_trailing_checksum;
    struct aws_s3_checksum_config checksum_config = {
        .checksum_algorithm = options->checksum_algorithm,
        .validate_response_checksum = options->validate_get_response_checksum,
        .location = disable_trailing_checksum ? AWS_SCL_NONE : AWS_SCL_TRAILER,
        .validate_checksum_algorithms = options->validate_checksum_algorithms,
    };

    struct aws_s3_meta_request_options meta_request_options = {
        .type = options->meta_request_type,
        .operation_name = options->default_type_options.operation_name,
        .message = options->message,
        .checksum_config = &checksum_config,
        .resume_token = options->put_options.resume_token,
        .object_size_hint = options->object_size_hint,
    };

    if (options->mock_server) {
        meta_request_options.endpoint = &mock_server;
    }

    if (options->signing_config) {
        meta_request_options.signing_config = options->signing_config;
    }
    struct aws_signing_config_aws signing_config = {
        .algorithm = AWS_SIGNING_ALGORITHM_V4_S3EXPRESS,
        .service = g_s3express_service_name,
    };
    meta_request_options.signing_config =
        options->use_s3express_signing ? &signing_config : meta_request_options.signing_config;

    struct aws_byte_buf input_stream_buffer;
    AWS_ZERO_STRUCT(input_stream_buffer);

    struct aws_input_stream *input_stream = NULL;
    struct aws_async_input_stream *async_stream = NULL;
    size_t upload_size_bytes = 0;

    if (meta_request_options.message == NULL) {
        const struct aws_byte_cursor *bucket_name = options->bucket_name;

        if (bucket_name == NULL) {
            bucket_name = &g_test_bucket_name;
        }

        struct aws_string *host_name = NULL;
        if (options->mock_server) {
            const struct aws_byte_cursor *host_cursor = aws_uri_authority(&mock_server);
            host_name = aws_string_new_from_cursor(allocator, host_cursor);
        } else if (options->mrap_test) {
            host_name = aws_string_new_from_cursor(allocator, &g_test_mrap_endpoint);
        } else {
            host_name = aws_s3_tester_build_endpoint_string(allocator, bucket_name, &g_test_s3_region);
        }
        if (meta_request_options.type == AWS_S3_META_REQUEST_TYPE_GET_OBJECT ||
            (meta_request_options.type == AWS_S3_META_REQUEST_TYPE_DEFAULT &&
             options->default_type_options.mode == AWS_S3_TESTER_DEFAULT_TYPE_MODE_GET)) {

            struct aws_http_message *message = aws_s3_test_get_object_request_new(
                allocator, aws_byte_cursor_from_string(host_name), options->get_options.object_path);
            ASSERT_SUCCESS(aws_s3_message_util_set_multipart_request_path(
                allocator,
                NULL /*upload_id*/,
                options->get_options.part_number,
                false /*append_uploads_suffix*/,
                message));

            if (options->get_options.object_range.ptr != NULL) {
                struct aws_http_header range_header = {
                    .name = g_range_header_name,
                    .value = options->get_options.object_range,
                };

                aws_http_message_add_header(message, range_header);
            }

            meta_request_options.message = message;

        } else if (
            meta_request_options.type == AWS_S3_META_REQUEST_TYPE_PUT_OBJECT ||
            (meta_request_options.type == AWS_S3_META_REQUEST_TYPE_DEFAULT &&
             options->default_type_options.mode == AWS_S3_TESTER_DEFAULT_TYPE_MODE_PUT)) {

            uint32_t object_size_mb = options->put_options.object_size_mb;
            upload_size_bytes = (size_t)object_size_mb * 1024ULL * 1024ULL;

            /* This doesn't do what we think it should because
             * g_min_upload_part_size overrides client->part_size */
            if (options->put_options.ensure_multipart) {
                if (upload_size_bytes == 0) {
                    upload_size_bytes = client->part_size * 2;
                    object_size_mb = (uint32_t)(upload_size_bytes / 1024 / 1024);
                }

                ASSERT_TRUE(upload_size_bytes > client->part_size);
            }

            struct aws_byte_buf object_path_buffer;
            aws_byte_buf_init(&object_path_buffer, allocator, 128);

            if (options->put_options.object_path_override.ptr != NULL) {
                aws_byte_buf_append_dynamic(&object_path_buffer, &options->put_options.object_path_override);
            } else {
                char object_path_sprintf_buffer[128] = "";

                switch (options->sse_type) {
                    case AWS_S3_TESTER_SSE_NONE:
                        snprintf(
                            object_path_sprintf_buffer,
                            sizeof(object_path_sprintf_buffer),
                            "" PRInSTR "-%uMB.txt",
                            AWS_BYTE_CURSOR_PRI(g_put_object_prefix),
                            object_size_mb);
                        break;
                    case AWS_S3_TESTER_SSE_KMS:
                        snprintf(
                            object_path_sprintf_buffer,
                            sizeof(object_path_sprintf_buffer),
                            "" PRInSTR "-kms-%uMB.txt",
                            AWS_BYTE_CURSOR_PRI(g_put_object_prefix),
                            object_size_mb);
                        break;
                    case AWS_S3_TESTER_SSE_AES256:
                        snprintf(
                            object_path_sprintf_buffer,
                            sizeof(object_path_sprintf_buffer),
                            "" PRInSTR "-aes256-%uMB.txt",
                            AWS_BYTE_CURSOR_PRI(g_put_object_prefix),
                            object_size_mb);
                        break;
                    case AWS_S3_TESTER_SSE_C_AES256:
                        snprintf(
                            object_path_sprintf_buffer,
                            sizeof(object_path_sprintf_buffer),
                            "" PRInSTR "-aes256-c-%uMB.txt",
                            AWS_BYTE_CURSOR_PRI(g_put_object_prefix),
                            object_size_mb);
                        break;

                    default:
                        break;
                }

                struct aws_byte_cursor sprintf_buffer_cursor = aws_byte_cursor_from_c_str(object_path_sprintf_buffer);
                aws_byte_buf_append_dynamic(&object_path_buffer, &sprintf_buffer_cursor);
            }

            struct aws_byte_cursor test_object_path = aws_byte_cursor_from_buf(&object_path_buffer);
            struct aws_byte_cursor host_cur = aws_byte_cursor_from_string(host_name);

            /* Create "tester" stream with appropriate options */
            struct aws_async_input_stream_tester_options stream_options = {
                .base =
                    {
                        .autogen_length = upload_size_bytes,
                        .eof_requires_extra_read = options->put_options.eof_requires_extra_read,
                        .max_bytes_per_read = options->put_options.max_bytes_per_read,
                    },
            };
            if (options->put_options.invalid_input_stream) {
                stream_options.base.fail_on_nth_read = 1;
                stream_options.base.fail_with_error_code = AWS_IO_STREAM_READ_FAILED;
            }

            if (options->put_options.async_input_stream) {
                stream_options.completion_strategy = options->put_options.async_read_strategy;
                stream_options.read_duration_ns = MS_TO_NS(100); /* have async reads take a bit of time. */

                async_stream = aws_async_input_stream_new_tester(allocator, &stream_options);
                ASSERT_NOT_NULL(async_stream);
                meta_request_options.send_async_stream = async_stream;
            } else {
                input_stream = aws_input_stream_new_tester(allocator, &stream_options.base);
                ASSERT_NOT_NULL(input_stream);
            }

            /* if uploading via filepath, write input_stream out as tmp file on disk, and then upload that */
            if (options->put_options.file_on_disk) {
                ASSERT_NOT_NULL(input_stream);
                filepath_str = aws_s3_tester_create_file(allocator, test_object_path, input_stream);
                meta_request_options.send_filepath = aws_byte_cursor_from_string(filepath_str);
                input_stream = aws_input_stream_release(input_stream);
            }

            /* Put together a simple S3 Put Object request. */
            struct aws_http_message *message;
            if (input_stream != NULL) {
                message = aws_s3_test_put_object_request_new(
                    allocator, &host_cur, test_object_path, g_test_body_content_type, input_stream, options->sse_type);
            } else {
                message = aws_s3_test_put_object_request_new_without_body(
                    allocator,
                    &host_cur,
                    g_test_body_content_type,
                    test_object_path,
                    upload_size_bytes,
                    options->sse_type);
            }

            if (options->put_options.content_length) {
                /* make a invalid request */
                char content_length_buffer[64] = "";
                snprintf(
                    content_length_buffer, sizeof(content_length_buffer), "%zu", options->put_options.content_length);

                struct aws_http_headers *headers = aws_http_message_get_headers(message);
                aws_http_headers_set(
                    headers, g_content_length_header_name, aws_byte_cursor_from_c_str(content_length_buffer));
            }

            if (options->put_options.skip_content_length) {
                struct aws_http_headers *headers = aws_http_message_get_headers(message);
                aws_http_headers_erase(headers, g_content_length_header_name);
            }

            if (options->put_options.invalid_request) {
                /* make a invalid request */
                aws_http_message_set_request_path(message, aws_byte_cursor_from_c_str("invalid_path"));
            }

            if (options->put_options.content_encoding.ptr != NULL) {
                struct aws_http_header content_encoding_header = {
                    .name = g_content_encoding_header_name,
                    .value = options->put_options.content_encoding,
                };
                aws_http_message_add_header(message, content_encoding_header);
            }

            meta_request_options.message = message;
            aws_byte_buf_clean_up(&object_path_buffer);
        }

        ASSERT_TRUE(meta_request_options.message != NULL);

        aws_string_destroy(host_name);
    } else {
        aws_http_message_acquire(meta_request_options.message);

        if (options->meta_request_type == AWS_S3_META_REQUEST_TYPE_PUT_OBJECT) {
            /* Figure out how much is being uploaded from pre-existing message */
            struct aws_input_stream *mystery_stream = aws_http_message_get_body_stream(meta_request_options.message);
            int64_t mystery_stream_length = 0;
            if (mystery_stream != NULL) {
                ASSERT_SUCCESS(aws_input_stream_get_length(mystery_stream, (int64_t *)&mystery_stream_length));
            }
            upload_size_bytes = (size_t)mystery_stream_length;
        }
    }

    struct aws_s3_meta_request_test_results meta_request_test_results;
    aws_s3_meta_request_test_results_init(&meta_request_test_results, allocator);

    if (out_results == NULL) {
        out_results = &meta_request_test_results;
    }

    out_results->headers_callback = options->headers_callback;
    out_results->body_callback = options->body_callback;
    out_results->finish_callback = options->finish_callback;
    out_results->progress_callback = options->progress_callback;
    out_results->upload_review_callback = options->upload_review_callback;

    out_results->algorithm = options->expected_validate_checksum_alg;

    ASSERT_SUCCESS(aws_s3_tester_bind_meta_request(tester, &meta_request_options, out_results));

    struct aws_s3_meta_request *meta_request = aws_s3_client_make_meta_request(client, &meta_request_options);

    if (meta_request == NULL) {
        out_results->finished_error_code = aws_last_error();
    }

    aws_http_message_release(meta_request_options.message);
    meta_request_options.message = NULL;

    if (meta_request != NULL) {
        /* Wait for the request to finish. */
        aws_s3_tester_wait_for_meta_request_finish(tester);
        ASSERT_TRUE(aws_s3_meta_request_is_finished(meta_request));
    }

    switch (options->validate_type) {
        case AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_SUCCESS:
            ASSERT_INT_EQUALS(AWS_ERROR_SUCCESS, out_results->finished_error_code);

            if (meta_request_options.type == AWS_S3_META_REQUEST_TYPE_GET_OBJECT) {
                ASSERT_SUCCESS(aws_s3_tester_validate_get_object_results(out_results, options->sse_type));
            } else if (meta_request_options.type == AWS_S3_META_REQUEST_TYPE_PUT_OBJECT) {
                ASSERT_SUCCESS(aws_s3_tester_validate_put_object_results(out_results, options->sse_type));

                /* Expected number of bytes should have been read from stream, and reported via progress callbacks */
                if (input_stream != NULL) {
                    ASSERT_UINT_EQUALS(upload_size_bytes, aws_input_stream_tester_total_bytes_read(input_stream));
                } else if (async_stream != NULL) {
                    ASSERT_UINT_EQUALS(upload_size_bytes, aws_async_input_stream_tester_total_bytes_read(async_stream));
                }

                ASSERT_UINT_EQUALS(upload_size_bytes, out_results->progress.total_bytes_transferred);
                if (!options->put_options.skip_content_length) {
                    ASSERT_UINT_EQUALS(upload_size_bytes, out_results->progress.content_length);
                }
            }
            ASSERT_UINT_EQUALS(0, aws_atomic_load_int(&client->stats.num_requests_in_flight));
            ASSERT_UINT_EQUALS(0, aws_atomic_load_int(&client->stats.num_requests_stream_queued_waiting));
            ASSERT_UINT_EQUALS(0, aws_atomic_load_int(&client->stats.num_requests_streaming_response));
            ASSERT_SUCCESS(s_tester_check_client_thread_data(client));
            break;
        case AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE:
            ASSERT_FALSE(out_results->finished_error_code == AWS_ERROR_SUCCESS);
            break;
        case AWS_S3_TESTER_VALIDATE_TYPE_NO_VALIDATE:
            break;
        default:
            ASSERT_TRUE(false);
            break;
    }

    if (meta_request != NULL) {
        out_results->part_size = meta_request->part_size;
        meta_request = aws_s3_meta_request_release(meta_request);

        if (!options->dont_wait_for_shutdown) {
            aws_s3_tester_wait_for_meta_request_shutdown(tester);
        }
    }

    aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);

    aws_s3_client_release(client);

    aws_input_stream_release(input_stream);
    input_stream = NULL;

    async_stream = aws_async_input_stream_release(async_stream);

    aws_byte_buf_clean_up(&input_stream_buffer);

    if (clean_up_local_tester) {
        aws_s3_tester_clean_up(&local_tester);
    }
    aws_uri_clean_up(&mock_server);

    if (filepath_str) {
        aws_file_delete(filepath_str);
        aws_string_destroy(filepath_str);
    }

    return AWS_OP_SUCCESS;
}

int aws_s3_tester_send_meta_request(
    struct aws_s3_tester *tester,
    struct aws_s3_client *client,
    struct aws_s3_meta_request_options *options,
    struct aws_s3_meta_request_test_results *test_results,
    uint32_t flags) {

    ASSERT_SUCCESS(aws_s3_tester_bind_meta_request(tester, options, test_results));

    struct aws_s3_meta_request *meta_request = aws_s3_client_make_meta_request(client, options);

    ASSERT_TRUE(meta_request != NULL);

    if (flags & AWS_S3_TESTER_SEND_META_REQUEST_CANCEL) {
        /* take a random sleep from 0-1 ms. */
        srand((uint32_t)time(NULL));
        aws_thread_current_sleep(rand() % MS_TO_NS(1));
        aws_s3_meta_request_cancel(meta_request);
    }

    /* Wait for the request to finish. */
    aws_s3_tester_wait_for_meta_request_finish(tester);

    ASSERT_TRUE(aws_s3_meta_request_is_finished(meta_request));

    aws_s3_tester_lock_synced_data(tester);

    if (flags & AWS_S3_TESTER_SEND_META_REQUEST_EXPECT_SUCCESS) {
        ASSERT_TRUE(tester->synced_data.finish_error_code == AWS_ERROR_SUCCESS);
    } else if (flags & AWS_S3_TESTER_SEND_META_REQUEST_CANCEL) {
        ASSERT_TRUE(tester->synced_data.finish_error_code == AWS_ERROR_S3_CANCELED);
    } else {
        ASSERT_FALSE(tester->synced_data.finish_error_code == AWS_ERROR_SUCCESS);
    }

    aws_s3_tester_unlock_synced_data(tester);

    test_results->part_size = meta_request->part_size;

    aws_s3_meta_request_release(meta_request);

    if ((flags & AWS_S3_TESTER_SEND_META_REQUEST_DONT_WAIT_FOR_SHUTDOWN) == 0) {
        aws_s3_tester_wait_for_meta_request_shutdown(tester);
    }

    return AWS_OP_SUCCESS;
}

int aws_s3_tester_send_get_object_meta_request(
    struct aws_s3_tester *tester,
    struct aws_s3_client *client,
    struct aws_byte_cursor s3_path,
    uint32_t flags,
    struct aws_s3_meta_request_test_results *out_results) {

    struct aws_string *host_name =
        aws_s3_tester_build_endpoint_string(tester->allocator, &g_test_bucket_name, &g_test_s3_region);

    /* Put together a simple S3 Get Object request. */
    struct aws_http_message *message =
        aws_s3_test_get_object_request_new(tester->allocator, aws_byte_cursor_from_string(host_name), s3_path);

    struct aws_s3_meta_request_options options;
    AWS_ZERO_STRUCT(options);
    options.type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT;
    options.message = message;

    /* Trigger accelerating of our Get Object request. */
    struct aws_s3_meta_request_test_results meta_request_test_results;
    aws_s3_meta_request_test_results_init(&meta_request_test_results, tester->allocator);

    if (out_results == NULL) {
        out_results = &meta_request_test_results;
    }

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request(tester, client, &options, out_results, flags));

    if (flags & AWS_S3_TESTER_SEND_META_REQUEST_EXPECT_SUCCESS) {
        ASSERT_SUCCESS(aws_s3_tester_validate_get_object_results(out_results, flags));
    }

    aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);

    aws_http_message_release(message);
    aws_string_destroy(host_name);

    return AWS_OP_SUCCESS;
}

int aws_s3_tester_validate_get_object_results(
    struct aws_s3_meta_request_test_results *meta_request_test_results,
    uint32_t flags) {
    AWS_PRECONDITION(meta_request_test_results);
    AWS_PRECONDITION(meta_request_test_results->tester);

    ASSERT_TRUE(meta_request_test_results->response_headers != NULL);

    if (aws_http_headers_has(
            meta_request_test_results->response_headers, aws_byte_cursor_from_c_str("Content-Range"))) {
        ASSERT_TRUE(meta_request_test_results->finished_response_status == 206);
    } else {
        ASSERT_TRUE(meta_request_test_results->finished_response_status == 200);
    }

    ASSERT_TRUE(
        meta_request_test_results->finished_response_status == meta_request_test_results->headers_response_status);
    ASSERT_TRUE(meta_request_test_results->finished_error_code == AWS_ERROR_SUCCESS);

    ASSERT_TRUE(meta_request_test_results->error_response_headers == NULL);
    ASSERT_TRUE(meta_request_test_results->error_response_body.len == 0);
    ASSERT_NULL(meta_request_test_results->error_response_operation_name);

    struct aws_byte_cursor sse_byte_cursor;

    if (flags & AWS_S3_TESTER_SEND_META_REQUEST_SSE_KMS) {
        ASSERT_SUCCESS(
            aws_http_headers_get(meta_request_test_results->response_headers, g_s3_sse_header, &sse_byte_cursor));
        ASSERT_TRUE(aws_byte_cursor_eq_c_str(&sse_byte_cursor, "aws:kms"));
    }

    if (flags & AWS_S3_TESTER_SEND_META_REQUEST_SSE_AES256) {
        ASSERT_SUCCESS(
            aws_http_headers_get(meta_request_test_results->response_headers, g_s3_sse_header, &sse_byte_cursor));
        ASSERT_TRUE(aws_byte_cursor_eq_c_str(&sse_byte_cursor, "AES256"));
    }

    if (flags & AWS_S3_TESTER_SEND_META_REQUEST_SSE_C_AES256) {
        ASSERT_SUCCESS(
            aws_http_headers_get(meta_request_test_results->response_headers, g_s3_sse_c_alg_header, &sse_byte_cursor));
        ASSERT_TRUE(aws_byte_cursor_eq_c_str(&sse_byte_cursor, "AES256"));
    }

    uint64_t content_length = 0;
    ASSERT_SUCCESS(aws_s3_tester_get_content_length(meta_request_test_results->response_headers, &content_length));

    AWS_LOGF_DEBUG(
        AWS_LS_S3_GENERAL,
        "Content length in header is %" PRIu64 " and received body size is %" PRIu64,
        content_length,
        meta_request_test_results->received_body_size);

    ASSERT_TRUE(content_length == meta_request_test_results->received_body_size);
    ASSERT_UINT_EQUALS(content_length, meta_request_test_results->progress.total_bytes_transferred);
    ASSERT_UINT_EQUALS(content_length, meta_request_test_results->progress.content_length);

    return AWS_OP_SUCCESS;
}

/* Avoid using this function as it will soon go away.  Use aws_s3_tester_send_meta_request_with_options instead.*/
int aws_s3_tester_send_put_object_meta_request(
    struct aws_s3_tester *tester,
    struct aws_s3_client *client,
    uint32_t file_size_mb,
    uint32_t flags,
    struct aws_s3_meta_request_test_results *out_results) {
    ASSERT_TRUE(tester != NULL);
    ASSERT_TRUE(client != NULL);

    struct aws_allocator *allocator = tester->allocator;

    struct aws_byte_buf test_buffer;
    aws_s3_create_test_buffer(allocator, (size_t)file_size_mb * 1024ULL * 1024ULL, &test_buffer);

    struct aws_byte_cursor test_body_cursor = aws_byte_cursor_from_buf(&test_buffer);
    struct aws_input_stream *input_stream = aws_input_stream_new_from_cursor(allocator, &test_body_cursor);

    struct aws_string *host_name =
        aws_s3_tester_build_endpoint_string(allocator, &g_test_bucket_name, &g_test_s3_region);

    char object_path_buffer[128] = "";

    if (flags & AWS_S3_TESTER_SEND_META_REQUEST_PUT_ACL) {
        snprintf(
            object_path_buffer,
            sizeof(object_path_buffer),
            "" PRInSTR "-acl-public-read-%uMB.txt",
            AWS_BYTE_CURSOR_PRI(g_put_object_prefix),
            file_size_mb);
    } else {
        snprintf(
            object_path_buffer,
            sizeof(object_path_buffer),
            "" PRInSTR "-%uMB.txt",
            AWS_BYTE_CURSOR_PRI(g_put_object_prefix),
            file_size_mb);
    }
    struct aws_byte_cursor test_object_path = aws_byte_cursor_from_c_str(object_path_buffer);

    struct aws_byte_cursor host_cur = aws_byte_cursor_from_string(host_name);
    /* Put together a simple S3 Put Object request. */
    struct aws_http_message *message = aws_s3_test_put_object_request_new(
        allocator, &host_cur, test_object_path, g_test_body_content_type, input_stream, flags);

    if (flags & AWS_S3_TESTER_SEND_META_REQUEST_WITH_CORRECT_CONTENT_MD5) {
        ASSERT_SUCCESS(aws_s3_message_util_add_content_md5_header(allocator, &test_buffer, message));
    } else if (flags & AWS_S3_TESTER_SEND_META_REQUEST_WITH_INCORRECT_CONTENT_MD5) {
        struct aws_http_header content_md5_header = {
            .name = g_content_md5_header_name,
            .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("dummy_content_md5"),
        };
        ASSERT_SUCCESS(aws_http_message_add_header(message, content_md5_header));
    }

    struct aws_s3_meta_request_options options;
    AWS_ZERO_STRUCT(options);
    options.type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT;
    options.message = message;

    struct aws_s3_meta_request_test_results meta_request_test_results;
    aws_s3_meta_request_test_results_init(&meta_request_test_results, allocator);

    if (out_results == NULL) {
        out_results = &meta_request_test_results;
    }

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request(tester, client, &options, out_results, flags));

    if (flags & AWS_S3_TESTER_SEND_META_REQUEST_EXPECT_SUCCESS) {
        ASSERT_SUCCESS(aws_s3_tester_validate_put_object_results(out_results, flags));
    }

    aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);

    aws_http_message_release(message);
    message = NULL;

    aws_string_destroy(host_name);
    host_name = NULL;

    aws_input_stream_release(input_stream);
    input_stream = NULL;

    aws_byte_buf_clean_up(&test_buffer);

    return AWS_OP_SUCCESS;
}

/* Avoid using this function as it will soon go away.  Use aws_s3_tester_send_meta_request_with_options instead.*/
int aws_s3_tester_validate_put_object_results(
    struct aws_s3_meta_request_test_results *meta_request_test_results,
    uint32_t flags) {
    ASSERT_TRUE(meta_request_test_results->finished_response_status == 200);
    ASSERT_TRUE(
        meta_request_test_results->finished_response_status == meta_request_test_results->headers_response_status);
    ASSERT_TRUE(meta_request_test_results->finished_error_code == AWS_ERROR_SUCCESS);

    ASSERT_TRUE(meta_request_test_results->error_response_headers == NULL);
    ASSERT_TRUE(meta_request_test_results->error_response_body.len == 0);
    ASSERT_NULL(meta_request_test_results->error_response_operation_name);

    struct aws_byte_cursor etag_byte_cursor;
    AWS_ZERO_STRUCT(etag_byte_cursor);
    ASSERT_SUCCESS(
        aws_http_headers_get(meta_request_test_results->response_headers, g_etag_header_name, &etag_byte_cursor));
    struct aws_byte_cursor sse_byte_cursor;
    if (flags & AWS_S3_TESTER_SEND_META_REQUEST_SSE_KMS) {
        ASSERT_SUCCESS(
            aws_http_headers_get(meta_request_test_results->response_headers, g_s3_sse_header, &sse_byte_cursor));
        ASSERT_TRUE(aws_byte_cursor_eq_c_str(&sse_byte_cursor, "aws:kms"));
    }
    if (flags & AWS_S3_TESTER_SEND_META_REQUEST_SSE_AES256) {
        ASSERT_SUCCESS(
            aws_http_headers_get(meta_request_test_results->response_headers, g_s3_sse_header, &sse_byte_cursor));
        ASSERT_TRUE(aws_byte_cursor_eq_c_str(&sse_byte_cursor, "AES256"));
    }
    ASSERT_TRUE(etag_byte_cursor.len > 0);

    if (flags & AWS_S3_TESTER_SEND_META_REQUEST_SSE_C_AES256) {
        ASSERT_SUCCESS(
            aws_http_headers_get(meta_request_test_results->response_headers, g_s3_sse_c_alg_header, &sse_byte_cursor));
        ASSERT_TRUE(aws_byte_cursor_eq_c_str(&sse_byte_cursor, "AES256"));
    }

    struct aws_byte_cursor quote_entity = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("&quot;");

    if (etag_byte_cursor.len >= quote_entity.len) {
        for (size_t i = 0; i < (etag_byte_cursor.len - quote_entity.len + 1); ++i) {
            ASSERT_TRUE(
                strncmp((const char *)&etag_byte_cursor.ptr[i], (const char *)quote_entity.ptr, quote_entity.len) != 0);
        }
    }

    return AWS_OP_SUCCESS;
}

int aws_s3_tester_upload_file_path_init(
    struct aws_allocator *allocator,
    struct aws_byte_buf *out_path_buffer,
    struct aws_byte_cursor file_path) {

    ASSERT_SUCCESS(aws_byte_buf_init_copy_from_cursor(out_path_buffer, allocator, g_upload_folder));
    ASSERT_SUCCESS(aws_byte_buf_append_dynamic(out_path_buffer, &file_path));

    return AWS_OP_SUCCESS;
}

int aws_s3_tester_get_content_length(const struct aws_http_headers *headers, uint64_t *out_content_length) {
    struct aws_byte_cursor value_cursor;
    AWS_ZERO_STRUCT(value_cursor);
    ASSERT_SUCCESS(aws_http_headers_get(headers, aws_byte_cursor_from_c_str("Content-Length"), &value_cursor));

    ASSERT_SUCCESS(aws_byte_cursor_utf8_parse_u64(value_cursor, out_content_length));
    return AWS_OP_SUCCESS;
}

/* The default mock response is:
<?xml version="1.0" encoding="UTF-8"?>
<CreateSessionResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Credentials>
    <SessionToken>sessionToken</SessionToken>
    <SecretAccessKey>secretKey</SecretAccessKey>
    <AccessKeyId>accessKeyId</AccessKeyId>
    <Expiration>2023-06-26T17:33:30Z</Expiration>
  </Credentials>
</CreateSessionResult>
 */
int aws_s3_tester_check_s3express_creds_for_default_mock_response(struct aws_credentials *credentials) {

    struct aws_byte_cursor result;
    bool match = true;
    result = aws_credentials_get_access_key_id(credentials);
    match &= aws_byte_cursor_eq_c_str(&result, "accessKeyId");
    result = aws_credentials_get_secret_access_key(credentials);
    match &= aws_byte_cursor_eq_c_str(&result, "secretKey");
    result = aws_credentials_get_session_token(credentials);
    match &= aws_byte_cursor_eq_c_str(&result, "sessionToken");

    uint64_t expiration_secs = aws_credentials_get_expiration_timepoint_seconds(credentials);
    ASSERT_UINT_EQUALS(1687800810, expiration_secs);

    ASSERT_TRUE(match);
    return AWS_OP_SUCCESS;
}

struct aws_string *aws_s3_tester_create_file(
    struct aws_allocator *allocator,
    struct aws_byte_cursor test_object_path,
    struct aws_input_stream *input_stream) {

    struct aws_byte_buf filepath_buf;
    aws_byte_buf_init(&filepath_buf, allocator, 128);
    struct aws_byte_cursor filepath_prefix = aws_byte_cursor_from_c_str("tmp");
    aws_byte_buf_append_dynamic(&filepath_buf, &filepath_prefix);
    aws_byte_buf_append_dynamic(&filepath_buf, &test_object_path);
    for (size_t i = 0; i < filepath_buf.len; ++i) {
        if (!isalnum(filepath_buf.buffer[i])) {
            filepath_buf.buffer[i] = '_'; /* sanitize filename */
        }
    }
    struct aws_string *filepath_str = aws_string_new_from_buf(allocator, &filepath_buf);
    aws_byte_buf_clean_up(&filepath_buf);

    FILE *file = aws_fopen(aws_string_c_str(filepath_str), "wb");
    AWS_FATAL_ASSERT(file != NULL);

    int64_t stream_length = 0;
    AWS_FATAL_ASSERT(aws_input_stream_get_length(input_stream, &stream_length) == AWS_OP_SUCCESS);

    struct aws_byte_buf data_buf;
    AWS_FATAL_ASSERT(aws_byte_buf_init(&data_buf, allocator, (size_t)stream_length) == AWS_OP_SUCCESS);
    AWS_FATAL_ASSERT(aws_input_stream_read(input_stream, &data_buf) == AWS_OP_SUCCESS);
    AWS_FATAL_ASSERT((size_t)stream_length == data_buf.len);
    AWS_FATAL_ASSERT(data_buf.len == fwrite(data_buf.buffer, 1, data_buf.len, file));
    fclose(file);
    aws_byte_buf_clean_up(&data_buf);

    return filepath_str;
}
