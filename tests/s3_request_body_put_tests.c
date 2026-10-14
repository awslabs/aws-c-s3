/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

/* request_body on PUT_OBJECT: upload straight from caller-owned memory. */

#include "s3_tester.h"

#include <aws/s3/private/s3_auto_ranged_put.h>
#include <aws/s3/private/s3_client_impl.h>
#include <aws/s3/private/s3_default_buffer_pool.h>
#include <aws/s3/private/s3_meta_request_impl.h>
#include <aws/s3/private/s3_util.h>
#include <aws/s3/s3_client.h>

#include <aws/common/byte_buf.h>
#include <aws/http/request_response.h>
#include <aws/testing/aws_test_harness.h>

/* A part count that isn't a whole number of parts, so the last part is short. */
#define RBP_MULTIPART_SIZE (MB_TO_BYTES(20) + 1234)

/* Deterministic, non-repeating-per-part bytes, so a part landing at the wrong offset shows up. */
static void s_rbp_fill(uint8_t *data, size_t len) {
    for (size_t i = 0; i < len; ++i) {
        data[i] = (uint8_t)((i * 31u) ^ (i >> 11));
    }
}

/* ---------------------------------------------------------------------------------------------------
 * Custom pools: a wrapper over the default pool, with or without pre-allocated buffer support
 * ------------------------------------------------------------------------------------------------ */

struct rbp_wrapper_pool {
    struct aws_allocator *allocator;
    struct aws_s3_buffer_pool *inner;
};

static struct {
    int num_adds;
} s_rbp_pool_stats;

static struct aws_future_s3_buffer_ticket *s_rbp_wrapper_reserve(
    struct aws_s3_buffer_pool *pool,
    struct aws_s3_buffer_pool_reserve_meta meta) {
    struct rbp_wrapper_pool *impl = pool->impl;
    return aws_s3_buffer_pool_reserve(impl->inner, meta);
}

static void s_rbp_wrapper_trim(struct aws_s3_buffer_pool *pool) {
    struct rbp_wrapper_pool *impl = pool->impl;
    aws_s3_buffer_pool_trim(impl->inner);
}

static int s_rbp_wrapper_add_preallocated(
    struct aws_s3_buffer_pool *pool,
    struct aws_s3_meta_request *meta_request,
    const struct aws_byte_buf *buffer) {
    struct rbp_wrapper_pool *impl = pool->impl;
    ++s_rbp_pool_stats.num_adds;
    return aws_s3_buffer_pool_add_preallocated_buffer(impl->inner, meta_request, buffer);
}

static void s_rbp_wrapper_remove_preallocated(
    struct aws_s3_buffer_pool *pool,
    struct aws_s3_meta_request *meta_request) {
    struct rbp_wrapper_pool *impl = pool->impl;
    aws_s3_buffer_pool_remove_preallocated_buffer(impl->inner, meta_request);
}

static void s_rbp_wrapper_destroy(void *data) {
    struct aws_s3_buffer_pool *pool = data;
    struct rbp_wrapper_pool *impl = pool->impl;
    struct aws_allocator *allocator = impl->allocator;
    aws_s3_buffer_pool_release(impl->inner);
    aws_mem_release(allocator, impl);
    aws_mem_release(allocator, pool);
}

static struct aws_s3_buffer_pool_vtable s_rbp_vtable_no_preallocated = {
    .reserve = s_rbp_wrapper_reserve,
    .trim = s_rbp_wrapper_trim,
};

static struct aws_s3_buffer_pool_vtable s_rbp_vtable_preallocated = {
    .reserve = s_rbp_wrapper_reserve,
    .trim = s_rbp_wrapper_trim,
    .add_preallocated_buffer = s_rbp_wrapper_add_preallocated,
    .remove_preallocated_buffer = s_rbp_wrapper_remove_preallocated,
};

static struct aws_s3_buffer_pool *s_rbp_wrapper_pool_new(
    struct aws_allocator *allocator,
    struct aws_s3_buffer_pool_config config,
    struct aws_s3_buffer_pool_vtable *vtable) {
    struct aws_s3_buffer_pool *pool = aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_buffer_pool));
    struct rbp_wrapper_pool *impl = aws_mem_calloc(allocator, 1, sizeof(struct rbp_wrapper_pool));
    impl->allocator = allocator;
    impl->inner = aws_s3_default_buffer_pool_new(allocator, config);
    pool->impl = impl;
    pool->vtable = vtable;
    aws_ref_count_init(&pool->ref_count, pool, s_rbp_wrapper_destroy);
    return pool;
}

static struct aws_s3_buffer_pool *s_rbp_pool_without_preallocated(
    struct aws_allocator *allocator,
    struct aws_s3_buffer_pool_config config,
    void *user_data) {
    (void)user_data;
    return s_rbp_wrapper_pool_new(allocator, config, &s_rbp_vtable_no_preallocated);
}

static struct aws_s3_buffer_pool *s_rbp_pool_with_preallocated(
    struct aws_allocator *allocator,
    struct aws_s3_buffer_pool_config config,
    void *user_data) {
    (void)user_data;
    return s_rbp_wrapper_pool_new(allocator, config, &s_rbp_vtable_preallocated);
}

/* ---------------------------------------------------------------------------------------------------
 * Helpers
 * ------------------------------------------------------------------------------------------------ */

struct rbp_env {
    struct aws_s3_tester tester;
    struct aws_s3_client *client;
    struct aws_string *host_name;
};

static int s_rbp_env_init_with_connections(
    struct aws_allocator *allocator,
    struct rbp_env *env,
    aws_s3_buffer_pool_factory_fn *buffer_pool_factory_fn,
    uint32_t max_active_connections) {

    AWS_ZERO_STRUCT(*env);
    AWS_ZERO_STRUCT(s_rbp_pool_stats);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &env->tester));
    struct aws_s3_client_config config = {
        .part_size = MB_TO_BYTES(8),
        .buffer_pool_factory_fn = buffer_pool_factory_fn,
        .max_active_connections_override = max_active_connections,
    };
    ASSERT_SUCCESS(aws_s3_tester_bind_client(
        &env->tester, &config, AWS_S3_TESTER_BIND_CLIENT_REGION | AWS_S3_TESTER_BIND_CLIENT_SIGNING));
    env->client = aws_s3_client_new(allocator, &config);
    ASSERT_NOT_NULL(env->client);
    env->host_name = aws_s3_tester_build_endpoint_string(allocator, &g_test_bucket_name, &g_test_s3_region);
    return AWS_OP_SUCCESS;
}

static int s_rbp_env_init(
    struct aws_allocator *allocator,
    struct rbp_env *env,
    aws_s3_buffer_pool_factory_fn *buffer_pool_factory_fn) {
    return s_rbp_env_init_with_connections(allocator, env, buffer_pool_factory_fn, 0 /*client default*/);
}

static void s_rbp_env_clean_up(struct rbp_env *env) {
    aws_string_destroy(env->host_name);
    env->client = aws_s3_client_release(env->client);
    aws_s3_tester_clean_up(&env->tester);
}

/* Upload `body` to `key` with request_body. Returns the meta request's finish error code in `out_error`. */
static int s_rbp_put(
    struct aws_allocator *allocator,
    struct rbp_env *env,
    struct aws_byte_cursor key,
    struct aws_byte_cursor body,
    enum aws_s3_meta_request_type type,
    enum aws_s3_checksum_algorithm checksum_algorithm,
    struct aws_s3_meta_request_resume_token *resume_token,
    int *out_error) {

    struct aws_byte_cursor host = aws_byte_cursor_from_string(env->host_name);
    struct aws_http_message *message =
        aws_s3_test_put_object_request_new_without_body(allocator, &host, g_test_body_content_type, key, body.len, 0);
    ASSERT_NOT_NULL(message);

    struct aws_s3_checksum_config checksum_config = {
        .location = AWS_SCL_TRAILER,
        .checksum_algorithm = checksum_algorithm,
    };
    struct aws_s3_meta_request_options options = {
        .type = type,
        .message = message,
        .request_body = body,
        .resume_token = resume_token,
    };
    if (type == AWS_S3_META_REQUEST_TYPE_DEFAULT) {
        options.operation_name = aws_byte_cursor_from_c_str("PutObject");
    }
    if (checksum_algorithm != AWS_SCA_NONE) {
        options.checksum_config = &checksum_config;
    }

    struct aws_s3_meta_request_test_results results;
    aws_s3_meta_request_test_results_init(&results, allocator);
    ASSERT_SUCCESS(aws_s3_tester_bind_meta_request(&env->tester, &options, &results));

    struct aws_s3_meta_request *meta_request = aws_s3_client_make_meta_request(env->client, &options);
    ASSERT_NOT_NULL(meta_request);
    aws_s3_tester_wait_for_meta_request_finish(&env->tester);
    *out_error = results.finished_error_code;

    aws_s3_meta_request_release(meta_request);
    aws_s3_tester_wait_for_meta_request_shutdown(&env->tester);
    aws_s3_meta_request_test_results_clean_up(&results);
    aws_http_message_release(message);
    return AWS_OP_SUCCESS;
}

/* Download `key` and check it's exactly `expected`. */
static int s_rbp_expect_object(
    struct aws_allocator *allocator,
    struct rbp_env *env,
    struct aws_byte_cursor key,
    struct aws_byte_cursor expected) {

    struct aws_http_message *message =
        aws_s3_test_get_object_request_new(allocator, aws_byte_cursor_from_string(env->host_name), key);
    ASSERT_NOT_NULL(message);

    struct aws_byte_buf downloaded;
    ASSERT_SUCCESS(aws_byte_buf_init(&downloaded, allocator, expected.len + 1));
    struct aws_s3_meta_request_options options = {
        .type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .message = message,
        .recv_buffer = &downloaded,
    };
    struct aws_s3_meta_request_test_results results;
    aws_s3_meta_request_test_results_init(&results, allocator);
    ASSERT_SUCCESS(aws_s3_tester_bind_meta_request(&env->tester, &options, &results));
    options.body_callback = NULL;

    struct aws_s3_meta_request *meta_request = aws_s3_client_make_meta_request(env->client, &options);
    ASSERT_NOT_NULL(meta_request);
    aws_s3_tester_wait_for_meta_request_finish(&env->tester);
    ASSERT_INT_EQUALS(AWS_ERROR_SUCCESS, results.finished_error_code);
    ASSERT_BIN_ARRAYS_EQUALS(expected.ptr, expected.len, downloaded.buffer, downloaded.len);

    aws_s3_meta_request_release(meta_request);
    aws_s3_tester_wait_for_meta_request_shutdown(&env->tester);
    aws_s3_meta_request_test_results_clean_up(&results);
    aws_byte_buf_clean_up(&downloaded);
    aws_http_message_release(message);
    return AWS_OP_SUCCESS;
}

/* Upload `size` bytes from request_body to a fresh key, then download it and compare. */
static int s_rbp_round_trip(
    struct aws_allocator *allocator,
    struct rbp_env *env,
    const char *name,
    size_t size,
    enum aws_s3_meta_request_type type,
    enum aws_s3_checksum_algorithm checksum_algorithm) {

    struct aws_byte_buf key;
    ASSERT_SUCCESS(aws_s3_tester_upload_file_path_init(allocator, &key, aws_byte_cursor_from_c_str(name)));
    uint8_t *data = aws_mem_acquire(allocator, size);
    s_rbp_fill(data, size);
    struct aws_byte_cursor body = aws_byte_cursor_from_array(data, size);

    int error_code = AWS_ERROR_UNKNOWN;
    ASSERT_SUCCESS(s_rbp_put(
        allocator,
        env,
        aws_byte_cursor_from_buf(&key),
        body,
        type,
        checksum_algorithm,
        NULL /*resume_token*/,
        &error_code));
    ASSERT_INT_EQUALS(AWS_ERROR_SUCCESS, error_code);
    ASSERT_SUCCESS(s_rbp_expect_object(allocator, env, aws_byte_cursor_from_buf(&key), body));

    aws_mem_release(allocator, data);
    aws_byte_buf_clean_up(&key);
    return AWS_OP_SUCCESS;
}

/* ---------------------------------------------------------------------------------------------------
 * Tests
 * ------------------------------------------------------------------------------------------------ */

/* Multipart upload from request_body through a pool that supports pre-allocated buffers: zero-copy. */
AWS_TEST_CASE(test_s3_request_body_put_multipart, s_test_s3_request_body_put_multipart)
static int s_test_s3_request_body_put_multipart(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    struct rbp_env env;
    ASSERT_SUCCESS(s_rbp_env_init(allocator, &env, s_rbp_pool_with_preallocated));
    ASSERT_SUCCESS(s_rbp_round_trip(
        allocator,
        &env,
        "/request-body-put-multipart.bin",
        RBP_MULTIPART_SIZE,
        AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        AWS_SCA_NONE));
    /* The upload registered request_body with the pool (the download's recv_buffer is the second add). */
    ASSERT_INT_EQUALS(2, s_rbp_pool_stats.num_adds);
    s_rbp_env_clean_up(&env);
    return 0;
}

/* Same, with a checksum on every part: checksums are computed over the parts' views of request_body. */
AWS_TEST_CASE(test_s3_request_body_put_multipart_checksum, s_test_s3_request_body_put_multipart_checksum)
static int s_test_s3_request_body_put_multipart_checksum(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    struct rbp_env env;
    ASSERT_SUCCESS(s_rbp_env_init(allocator, &env, NULL));
    ASSERT_SUCCESS(s_rbp_round_trip(
        allocator,
        &env,
        "/request-body-put-multipart-checksum.bin",
        RBP_MULTIPART_SIZE,
        AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        AWS_SCA_CRC32));
    s_rbp_env_clean_up(&env);
    return 0;
}

/* At or below the multipart threshold, PUT_OBJECT sends one PUT through the DEFAULT path, which already
 * supports request_body; the buffer pool isn't involved. */
AWS_TEST_CASE(test_s3_request_body_put_single_part, s_test_s3_request_body_put_single_part)
static int s_test_s3_request_body_put_single_part(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    struct rbp_env env;
    ASSERT_SUCCESS(s_rbp_env_init(allocator, &env, s_rbp_pool_with_preallocated));
    ASSERT_SUCCESS(s_rbp_round_trip(
        allocator,
        &env,
        "/request-body-put-single-part.bin",
        MB_TO_BYTES(1),
        AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        AWS_SCA_NONE));
    /* Only the download's recv_buffer was added: the single PUT didn't touch the pool. */
    ASSERT_INT_EQUALS(1, s_rbp_pool_stats.num_adds);
    s_rbp_env_clean_up(&env);
    return 0;
}

/* A custom pool without pre-allocated buffer support: a multipart request_body upload is rejected at creation with
 * AWS_ERROR_UNSUPPORTED_OPERATION, as recv_buffer is. A small one (single PUT) still works, since it never uses
 * the pool. */
AWS_TEST_CASE(
    test_s3_request_body_put_multipart_pool_unsupported,
    s_test_s3_request_body_put_multipart_pool_unsupported)
static int s_test_s3_request_body_put_multipart_pool_unsupported(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    struct rbp_env env;
    ASSERT_SUCCESS(s_rbp_env_init(allocator, &env, s_rbp_pool_without_preallocated));

    struct aws_byte_buf key;
    ASSERT_SUCCESS(aws_s3_tester_upload_file_path_init(
        allocator, &key, aws_byte_cursor_from_c_str("/request-body-put-pool-unsupported.bin")));
    uint8_t *data = aws_mem_acquire(allocator, RBP_MULTIPART_SIZE);
    s_rbp_fill(data, RBP_MULTIPART_SIZE);

    /* Multipart: rejected at creation. */
    struct aws_byte_cursor host = aws_byte_cursor_from_string(env.host_name);
    struct aws_http_message *message = aws_s3_test_put_object_request_new_without_body(
        allocator, &host, g_test_body_content_type, aws_byte_cursor_from_buf(&key), RBP_MULTIPART_SIZE, 0);
    ASSERT_NOT_NULL(message);
    struct aws_s3_meta_request_options options = {
        .type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .message = message,
        .request_body = aws_byte_cursor_from_array(data, RBP_MULTIPART_SIZE),
    };
    ASSERT_NULL(aws_s3_client_make_meta_request(env.client, &options));
    ASSERT_INT_EQUALS(AWS_ERROR_UNSUPPORTED_OPERATION, aws_last_error());
    ASSERT_INT_EQUALS(0, s_rbp_pool_stats.num_adds);
    aws_http_message_release(message);

    /* Single PUT (at or below the multipart threshold): works without the pool. */
    struct aws_byte_cursor small = aws_byte_cursor_from_array(data, MB_TO_BYTES(1));
    int error_code = AWS_ERROR_UNKNOWN;
    ASSERT_SUCCESS(s_rbp_put(
        allocator,
        &env,
        aws_byte_cursor_from_buf(&key),
        small,
        AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        AWS_SCA_NONE,
        NULL /*resume_token*/,
        &error_code));
    ASSERT_INT_EQUALS(AWS_ERROR_SUCCESS, error_code);
    s_rbp_env_clean_up(&env);

    /* Verify with a client on the default pool. */
    struct rbp_env download_env;
    ASSERT_SUCCESS(s_rbp_env_init(allocator, &download_env, NULL));
    ASSERT_SUCCESS(s_rbp_expect_object(allocator, &download_env, aws_byte_cursor_from_buf(&key), small));
    s_rbp_env_clean_up(&download_env);

    aws_mem_release(allocator, data);
    aws_byte_buf_clean_up(&key);
    return 0;
}

/* Existing behavior: DEFAULT request_body works with a custom pool that doesn't support pre-allocated
 * buffers, since that path never uses the pool. */
AWS_TEST_CASE(test_s3_request_body_default_custom_pool, s_test_s3_request_body_default_custom_pool)
static int s_test_s3_request_body_default_custom_pool(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    struct rbp_env upload_env;
    ASSERT_SUCCESS(s_rbp_env_init(allocator, &upload_env, s_rbp_pool_without_preallocated));

    struct aws_byte_buf key;
    ASSERT_SUCCESS(aws_s3_tester_upload_file_path_init(
        allocator, &key, aws_byte_cursor_from_c_str("/request-body-default-custom-pool.bin")));
    uint8_t *data = aws_mem_acquire(allocator, MB_TO_BYTES(1));
    s_rbp_fill(data, MB_TO_BYTES(1));
    struct aws_byte_cursor body = aws_byte_cursor_from_array(data, MB_TO_BYTES(1));

    int error_code = AWS_ERROR_UNKNOWN;
    ASSERT_SUCCESS(s_rbp_put(
        allocator,
        &upload_env,
        aws_byte_cursor_from_buf(&key),
        body,
        AWS_S3_META_REQUEST_TYPE_DEFAULT,
        AWS_SCA_NONE,
        NULL /*resume_token*/,
        &error_code));
    ASSERT_INT_EQUALS(AWS_ERROR_SUCCESS, error_code);
    s_rbp_env_clean_up(&upload_env);

    struct rbp_env download_env;
    ASSERT_SUCCESS(s_rbp_env_init(allocator, &download_env, NULL));
    ASSERT_SUCCESS(s_rbp_expect_object(allocator, &download_env, aws_byte_cursor_from_buf(&key), body));
    s_rbp_env_clean_up(&download_env);

    aws_mem_release(allocator, data);
    aws_byte_buf_clean_up(&key);
    return 0;
}

/* Fails part 2's first attempt with a 500 after it was sent, so it's retried from its view of request_body. */
static struct {
    int num_injected;
} s_rbp_retry;

static void s_rbp_send_request_finish_fail_part_2_once(
    struct aws_s3_connection *connection,
    struct aws_http_stream *stream,
    int error_code) {

    struct aws_s3_request *request = connection->request;
    struct aws_s3_tester *tester = request->meta_request->client->shutdown_callback_user_data;

    if (request->request_tag == AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_PART && request->part_number == 2 &&
        request->send_data.response_status == AWS_HTTP_STATUS_CODE_200_OK && s_rbp_retry.num_injected == 0) {
        request->send_data.response_status = AWS_HTTP_STATUS_CODE_500_INTERNAL_SERVER_ERROR;
        ++s_rbp_retry.num_injected;
    }

    aws_s3_tester_get_meta_request_vtable_patch(tester, 0)->original_vtable->send_request_finish(
        connection, stream, error_code);
}

static struct aws_s3_meta_request *s_rbp_meta_request_factory_fail_part_2_once(
    struct aws_s3_client *client,
    const struct aws_s3_meta_request_options *options) {

    struct aws_s3_tester *tester = client->shutdown_callback_user_data;
    struct aws_s3_meta_request *meta_request =
        aws_s3_tester_get_client_vtable_patch(tester, 0)->original_vtable->meta_request_factory(client, options);
    if (meta_request != NULL && options->type == AWS_S3_META_REQUEST_TYPE_PUT_OBJECT) {
        aws_s3_tester_patch_meta_request_vtable(tester, meta_request, NULL)->send_request_finish =
            s_rbp_send_request_finish_fail_part_2_once;
    }
    return meta_request;
}

/* A part fails once and is retried: it's re-sent from the same view of request_body, and the object is right. */
AWS_TEST_CASE(test_s3_request_body_put_multipart_retry, s_test_s3_request_body_put_multipart_retry)
static int s_test_s3_request_body_put_multipart_retry(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    AWS_ZERO_STRUCT(s_rbp_retry);
    struct rbp_env env;
    ASSERT_SUCCESS(s_rbp_env_init(allocator, &env, NULL));
    aws_s3_tester_patch_client_vtable(&env.tester, env.client, NULL)->meta_request_factory =
        s_rbp_meta_request_factory_fail_part_2_once;

    ASSERT_SUCCESS(s_rbp_round_trip(
        allocator,
        &env,
        "/request-body-put-multipart-retry.bin",
        RBP_MULTIPART_SIZE,
        AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        AWS_SCA_NONE));
    ASSERT_INT_EQUALS(1, s_rbp_retry.num_injected);
    s_rbp_env_clean_up(&env);
    return 0;
}

/* Pauses the upload as soon as its first part finishes, keeping the resume token. */
static struct {
    bool pause_requested;
    int pause_result;
    struct aws_s3_meta_request_resume_token *resume_token;
} s_rbp_pause;

static void s_rbp_finished_request_pause_after_first_part(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    int error_code) {

    struct aws_s3_tester *tester = meta_request->client->shutdown_callback_user_data;
    if (error_code == AWS_ERROR_SUCCESS && request->request_tag == AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_PART &&
        !s_rbp_pause.pause_requested) {
        s_rbp_pause.pause_requested = true;
        s_rbp_pause.pause_result = aws_s3_meta_request_pause(meta_request, &s_rbp_pause.resume_token);
    }
    aws_s3_tester_get_meta_request_vtable_patch(tester, 0)->original_vtable->finished_request(
        meta_request, request, error_code);
}

static struct aws_s3_meta_request *s_rbp_meta_request_factory_pause_after_first_part(
    struct aws_s3_client *client,
    const struct aws_s3_meta_request_options *options) {

    struct aws_s3_tester *tester = client->shutdown_callback_user_data;
    struct aws_s3_meta_request *meta_request =
        aws_s3_tester_get_client_vtable_patch(tester, 0)->original_vtable->meta_request_factory(client, options);
    if (meta_request != NULL && options->type == AWS_S3_META_REQUEST_TYPE_PUT_OBJECT) {
        aws_s3_tester_patch_meta_request_vtable(tester, meta_request, NULL)->finished_request =
            s_rbp_finished_request_pause_after_first_part;
    }
    return meta_request;
}

/* Pause a multipart request_body upload after its first part, then resume it from the same memory: the part
 * that's already uploaded is re-read from its view of request_body and its checksum compared, and the rest
 * are uploaded. The object comes out right. */
AWS_TEST_CASE(test_s3_request_body_put_pause_resume, s_test_s3_request_body_put_pause_resume)
static int s_test_s3_request_body_put_pause_resume(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    AWS_ZERO_STRUCT(s_rbp_pause);

    struct aws_byte_buf key;
    ASSERT_SUCCESS(aws_s3_tester_upload_file_path_init(
        allocator, &key, aws_byte_cursor_from_c_str("/request-body-put-pause-resume.bin")));
    uint8_t *data = aws_mem_acquire(allocator, RBP_MULTIPART_SIZE);
    s_rbp_fill(data, RBP_MULTIPART_SIZE);
    struct aws_byte_cursor body = aws_byte_cursor_from_array(data, RBP_MULTIPART_SIZE);
    int error_code = AWS_ERROR_UNKNOWN;

    /* --- Upload on 1 connection, paused after the first part --- */
    struct rbp_env pause_env;
    ASSERT_SUCCESS(s_rbp_env_init_with_connections(allocator, &pause_env, NULL, 1));
    aws_s3_tester_patch_client_vtable(&pause_env.tester, pause_env.client, NULL)->meta_request_factory =
        s_rbp_meta_request_factory_pause_after_first_part;
    ASSERT_SUCCESS(s_rbp_put(
        allocator,
        &pause_env,
        aws_byte_cursor_from_buf(&key),
        body,
        AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        AWS_SCA_CRC32,
        NULL /*resume_token*/,
        &error_code));
    s_rbp_env_clean_up(&pause_env);
    ASSERT_TRUE(s_rbp_pause.pause_requested);
    ASSERT_INT_EQUALS(AWS_OP_SUCCESS, s_rbp_pause.pause_result);
    ASSERT_NOT_NULL(s_rbp_pause.resume_token);
    ASSERT_INT_EQUALS(AWS_ERROR_S3_PAUSED, error_code);

    /* --- Resume from the token with the same memory, then check the object --- */
    struct rbp_env resume_env;
    ASSERT_SUCCESS(s_rbp_env_init(allocator, &resume_env, NULL));
    ASSERT_SUCCESS(s_rbp_put(
        allocator,
        &resume_env,
        aws_byte_cursor_from_buf(&key),
        body,
        AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        AWS_SCA_CRC32,
        s_rbp_pause.resume_token,
        &error_code));
    ASSERT_INT_EQUALS(AWS_ERROR_SUCCESS, error_code);
    ASSERT_SUCCESS(s_rbp_expect_object(allocator, &resume_env, aws_byte_cursor_from_buf(&key), body));
    s_rbp_env_clean_up(&resume_env);

    aws_s3_meta_request_resume_token_release(s_rbp_pause.resume_token);
    aws_mem_release(allocator, data);
    aws_byte_buf_clean_up(&key);
    return 0;
}
