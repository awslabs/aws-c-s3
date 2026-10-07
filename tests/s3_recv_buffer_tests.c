/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

/* Tests for aws_s3_meta_request_options.recv_buffer: downloading straight into a caller-provided buffer. */

#include "s3_tester.h"

#include <aws/s3/private/s3_default_buffer_pool.h>
#include <aws/s3/private/s3_util.h>
#include <aws/s3/s3_client.h>

#include <aws/checksums/crc.h>
#include <aws/common/byte_buf.h>
#include <aws/http/request_response.h>
#include <aws/testing/aws_test_harness.h>

/* Every caller buffer is filled with this byte before a download, and gets extra guard bytes past its
 * capacity. After the download, everything past `len` (and every guard byte) must still be the marker,
 * which proves nothing was written where it shouldn't be. */
#define RB_MARKER 0xAB
#define RB_GUARD 64

struct rb_buffer {
    struct aws_allocator *allocator;
    uint8_t *mem;
    size_t capacity;
    struct aws_byte_buf buf;
};

static void s_rb_buffer_init(struct aws_allocator *allocator, struct rb_buffer *b, size_t capacity) {
    b->allocator = allocator;
    b->capacity = capacity;
    b->mem = aws_mem_acquire(allocator, capacity + RB_GUARD);
    memset(b->mem, RB_MARKER, capacity + RB_GUARD);
    b->buf = aws_byte_buf_from_empty_array(b->mem, capacity);
}

static void s_rb_buffer_clean_up(struct rb_buffer *b) {
    aws_mem_release(b->allocator, b->mem);
    AWS_ZERO_STRUCT(*b);
}

/* Bytes from `from` to the end of the guard area are still the marker. */
static int s_rb_check_untouched(const struct rb_buffer *b, size_t from) {
    for (size_t i = from; i < b->capacity + RB_GUARD; ++i) {
        if (b->mem[i] != RB_MARKER) {
            fprintf(stderr, "byte %zu was written (capacity %zu, guard %d)\n", i, b->capacity, RB_GUARD);
            return AWS_OP_ERR;
        }
    }
    return AWS_OP_SUCCESS;
}

/* `length` bytes at `data` are the test-object pattern starting at `object_offset`. */
static int s_rb_check_pattern(const uint8_t *data, uint64_t object_offset, size_t length) {
    uint64_t actual = aws_checksums_crc64nvme_ex(data, length, 0);
    uint64_t expected = aws_s3_tester_pattern_crc64nvme(object_offset, length);
    ASSERT_TRUE(
        actual == expected,
        "bytes do not match the object pattern at [%" PRIu64 ", %" PRIu64 ")",
        object_offset,
        object_offset + length);
    return AWS_OP_SUCCESS;
}

/* A client set up the way each test needs. */
struct rb_env {
    struct aws_s3_tester tester;
    struct aws_s3_client *client;
};

struct rb_env_options {
    uint64_t part_size;
    uint64_t backpressure_window; /* 0 = backpressure off */
    aws_s3_buffer_pool_factory_fn *buffer_pool_factory_fn;
    uint32_t max_active_connections; /* 0 = client default */
};

static int s_rb_env_init(struct aws_allocator *allocator, struct rb_env *env, const struct rb_env_options *opts) {
    AWS_ZERO_STRUCT(*env);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &env->tester));

    struct aws_s3_client_config config = {
        .part_size = opts->part_size,
        .buffer_pool_factory_fn = opts->buffer_pool_factory_fn,
    };
    if (opts->backpressure_window > 0) {
        config.enable_read_backpressure = true;
        config.initial_read_window = (size_t)opts->backpressure_window;
    }
    config.max_active_connections_override = opts->max_active_connections;
    ASSERT_SUCCESS(aws_s3_tester_bind_client(
        &env->tester, &config, AWS_S3_TESTER_BIND_CLIENT_REGION | AWS_S3_TESTER_BIND_CLIENT_SIGNING));
    env->client = aws_s3_client_new(allocator, &config);
    ASSERT_NOT_NULL(env->client);
    return AWS_OP_SUCCESS;
}

static void s_rb_env_clean_up(struct rb_env *env) {
    env->client = aws_s3_client_release(env->client);
    aws_s3_tester_clean_up(&env->tester);
}

/* One GET into a recv_buffer. */
struct rb_get {
    struct aws_byte_cursor key;
    const char *range; /* e.g. "bytes=0-99"; NULL for a full-object GET */
    const uint64_t *object_size_hint;
    uint64_t part_size; /* per-request override; 0 = client default */
    bool validate_checksum;
    bool cancel_right_away;
};

struct rb_get_result {
    int error_code;
    int response_status;
    bool did_validate;
};

static struct aws_http_message *s_rb_get_message(
    struct aws_allocator *allocator,
    struct aws_string *host_name,
    const struct rb_get *get) {

    struct aws_http_message *message =
        aws_s3_test_get_object_request_new(allocator, aws_byte_cursor_from_string(host_name), get->key);
    if (message != NULL && get->range != NULL) {
        struct aws_http_header range_header = {
            .name = g_range_header_name,
            .value = aws_byte_cursor_from_c_str(get->range),
        };
        aws_http_message_add_header(message, range_header);
    }
    return message;
}

static int s_rb_download(
    struct aws_allocator *allocator,
    struct rb_env *env,
    const struct rb_get *get,
    struct aws_byte_buf *recv_buffer,
    struct rb_get_result *out) {

    struct aws_string *host_name =
        aws_s3_tester_build_endpoint_string(allocator, &g_test_bucket_name, &g_test_s3_region);
    struct aws_http_message *message = s_rb_get_message(allocator, host_name, get);
    ASSERT_NOT_NULL(message);

    struct aws_s3_checksum_config checksum_config = {
        .validate_response_checksum = true,
    };
    struct aws_s3_meta_request_options options = {
        .type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .message = message,
        .recv_buffer = recv_buffer,
        .object_size_hint = get->object_size_hint,
        .part_size = get->part_size,
    };
    if (get->validate_checksum) {
        options.checksum_config = &checksum_config;
    }

    struct aws_s3_meta_request_test_results results;
    aws_s3_meta_request_test_results_init(&results, allocator);
    ASSERT_SUCCESS(aws_s3_tester_bind_meta_request(&env->tester, &options, &results));
    /* recv_buffer can't be combined with a body callback; the tester's other callbacks still apply. */
    options.body_callback = NULL;

    struct aws_s3_meta_request *meta_request = aws_s3_client_make_meta_request(env->client, &options);
    ASSERT_NOT_NULL(meta_request);
    if (get->cancel_right_away) {
        aws_s3_meta_request_cancel(meta_request);
    }

    aws_s3_tester_wait_for_meta_request_finish(&env->tester);
    out->error_code = results.finished_error_code;
    out->response_status = results.finished_response_status;
    out->did_validate = results.did_validate;

    aws_s3_meta_request_release(meta_request);
    aws_s3_tester_wait_for_meta_request_shutdown(&env->tester);

    aws_s3_meta_request_test_results_clean_up(&results);
    aws_http_message_release(message);
    aws_string_destroy(host_name);
    return AWS_OP_SUCCESS;
}

/* Download into a fresh buffer of `capacity` and check the result: success with `expected_len` bytes of
 * pattern starting at `expected_object_offset`, or failure with `expected_error`. Nothing past `len` is
 * ever written. */
static int s_rb_expect(
    struct aws_allocator *allocator,
    const struct rb_env_options *env_opts,
    const struct rb_get *get,
    size_t capacity,
    int expected_error,
    uint64_t expected_object_offset,
    size_t expected_len) {

    struct rb_env env;
    ASSERT_SUCCESS(s_rb_env_init(allocator, &env, env_opts));
    struct rb_buffer b;
    s_rb_buffer_init(allocator, &b, capacity);

    struct rb_get_result result;
    ASSERT_SUCCESS(s_rb_download(allocator, &env, get, &b.buf, &result));

    ASSERT_INT_EQUALS(expected_error, result.error_code);
    if (expected_error == AWS_ERROR_SUCCESS) {
        ASSERT_UINT_EQUALS(expected_len, b.buf.len);
        if (expected_len > 0) {
            ASSERT_SUCCESS(s_rb_check_pattern(b.mem, expected_object_offset, expected_len));
        }
        ASSERT_SUCCESS(s_rb_check_untouched(&b, expected_len));
    } else {
        /* len is only set on success */
        ASSERT_UINT_EQUALS(0, b.buf.len);
        ASSERT_SUCCESS(s_rb_check_untouched(&b, b.capacity));
    }

    s_rb_buffer_clean_up(&b);
    s_rb_env_clean_up(&env);
    return AWS_OP_SUCCESS;
}

/* ---------------------------------------------------------------------------------------------------
 * Rejected at creation
 * ------------------------------------------------------------------------------------------------ */

/* A custom buffer pool that wraps the default pool. It talks to the client only through the pool
 * interface (the vtable, including add/remove_preallocated_buffer), the way a customer's pool
 * would, and comes in two variants: one without pre-allocated buffer
 * support, which can't serve recv_buffer, and one that supports it by passing the calls through. */
struct rb_wrapper_pool {
    struct aws_allocator *allocator;
    struct aws_s3_buffer_pool *inner;
};

static struct aws_future_s3_buffer_ticket *s_rb_wrapper_reserve(
    struct aws_s3_buffer_pool *pool,
    struct aws_s3_buffer_pool_reserve_meta meta) {
    struct rb_wrapper_pool *impl = pool->impl;
    return aws_s3_buffer_pool_reserve(impl->inner, meta);
}

static void s_rb_wrapper_trim(struct aws_s3_buffer_pool *pool) {
    struct rb_wrapper_pool *impl = pool->impl;
    aws_s3_buffer_pool_trim(impl->inner);
}

static int s_rb_wrapper_add_preallocated(
    struct aws_s3_buffer_pool *pool,
    struct aws_s3_meta_request *meta_request,
    struct aws_byte_buf *buffer) {
    struct rb_wrapper_pool *impl = pool->impl;
    return aws_s3_buffer_pool_add_preallocated_buffer(impl->inner, meta_request, buffer);
}

static void s_rb_wrapper_remove_preallocated(
    struct aws_s3_buffer_pool *pool,
    struct aws_s3_meta_request *meta_request) {
    struct rb_wrapper_pool *impl = pool->impl;
    aws_s3_buffer_pool_remove_preallocated_buffer(impl->inner, meta_request);
}

static void s_rb_wrapper_destroy(void *data) {
    struct aws_s3_buffer_pool *pool = data;
    struct rb_wrapper_pool *impl = pool->impl;
    struct aws_allocator *allocator = impl->allocator;
    aws_s3_buffer_pool_release(impl->inner);
    aws_mem_release(allocator, impl);
    aws_mem_release(allocator, pool);
}

static struct aws_s3_buffer_pool_vtable s_rb_wrapper_vtable_no_preallocated = {
    .reserve = s_rb_wrapper_reserve,
    .trim = s_rb_wrapper_trim,
};

static struct aws_s3_buffer_pool_vtable s_rb_wrapper_vtable_preallocated = {
    .reserve = s_rb_wrapper_reserve,
    .trim = s_rb_wrapper_trim,
    .add_preallocated_buffer = s_rb_wrapper_add_preallocated,
    .remove_preallocated_buffer = s_rb_wrapper_remove_preallocated,
};

static struct aws_s3_buffer_pool *s_rb_wrapper_pool_new(
    struct aws_allocator *allocator,
    struct aws_s3_buffer_pool_config config,
    struct aws_s3_buffer_pool_vtable *vtable) {
    struct aws_s3_buffer_pool *pool = aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_buffer_pool));
    struct rb_wrapper_pool *impl = aws_mem_calloc(allocator, 1, sizeof(struct rb_wrapper_pool));
    impl->allocator = allocator;
    impl->inner = aws_s3_default_buffer_pool_new(allocator, config);
    pool->impl = impl;
    pool->vtable = vtable;
    aws_ref_count_init(&pool->ref_count, pool, s_rb_wrapper_destroy);
    return pool;
}

static struct aws_s3_buffer_pool *s_rb_custom_pool_without_preallocated(
    struct aws_allocator *allocator,
    struct aws_s3_buffer_pool_config config,
    void *user_data) {
    (void)user_data;
    return s_rb_wrapper_pool_new(allocator, config, &s_rb_wrapper_vtable_no_preallocated);
}

static struct aws_s3_buffer_pool *s_rb_custom_pool_with_preallocated(
    struct aws_allocator *allocator,
    struct aws_s3_buffer_pool_config config,
    void *user_data) {
    (void)user_data;
    return s_rb_wrapper_pool_new(allocator, config, &s_rb_wrapper_vtable_preallocated);
}

static int s_rb_expect_create_fails(
    struct aws_allocator *allocator,
    struct aws_s3_meta_request_options *options,
    aws_s3_buffer_pool_factory_fn *factory,
    int expected_error) {

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_tester_client_options client_options = {
        .buffer_pool_factory_fn = factory,
    };
    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_s3_meta_request *meta_request = aws_s3_client_make_meta_request(client, options);
    ASSERT_NULL(meta_request);
    ASSERT_INT_EQUALS(expected_error, aws_last_error());

    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);
    return AWS_OP_SUCCESS;
}

static int s_rb_dummy_body_callback(
    struct aws_s3_meta_request *meta_request,
    const struct aws_byte_cursor *body,
    uint64_t range_start,
    void *user_data) {
    (void)meta_request;
    (void)body;
    (void)range_start;
    (void)user_data;
    return AWS_OP_SUCCESS;
}

static int s_rb_dummy_body_callback_ex(
    struct aws_s3_meta_request *meta_request,
    const struct aws_byte_cursor *body,
    const struct aws_s3_meta_request_receive_body_extra_info info,
    void *user_data) {
    (void)meta_request;
    (void)body;
    (void)info;
    (void)user_data;
    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(test_s3_recv_buffer_create_errors, s_test_s3_recv_buffer_create_errors)
static int s_test_s3_recv_buffer_create_errors(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_byte_cursor host_name = aws_byte_cursor_from_c_str("dummy_host");
    struct aws_byte_cursor key = aws_byte_cursor_from_c_str("/dummy_key");
    uint8_t mem[64];
    struct aws_byte_buf recv_buffer = aws_byte_buf_from_empty_array(mem, sizeof(mem));
    struct aws_byte_buf empty_recv_buffer = aws_byte_buf_from_empty_array(mem, 0);

    struct aws_http_message *get = aws_s3_test_get_object_request_new(allocator, host_name, key);
    ASSERT_NOT_NULL(get);
    /* A PUT that is otherwise valid (it has a body), so it can only fail because of recv_buffer. */
    struct aws_input_stream *put_body = aws_s3_test_input_stream_new(allocator, 64);
    struct aws_http_message *put =
        aws_s3_test_put_object_request_new(allocator, &host_name, key, g_test_body_content_type, put_body, 0);
    ASSERT_NOT_NULL(put);

    /* Upload: recv_buffer is a download destination only. */
    {
        struct aws_s3_meta_request_options o = {
            .type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT, .message = put, .recv_buffer = &recv_buffer};
        ASSERT_SUCCESS(s_rb_expect_create_fails(allocator, &o, NULL, AWS_ERROR_INVALID_ARGUMENT));
    }
    /* Combined with another destination. */
    {
        struct aws_s3_meta_request_options o = {
            .type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
            .message = get,
            .recv_buffer = &recv_buffer,
            .body_callback = s_rb_dummy_body_callback};
        ASSERT_SUCCESS(s_rb_expect_create_fails(allocator, &o, NULL, AWS_ERROR_INVALID_ARGUMENT));
    }
    {
        struct aws_s3_meta_request_options o = {
            .type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
            .message = get,
            .recv_buffer = &recv_buffer,
            .body_callback_ex = s_rb_dummy_body_callback_ex};
        ASSERT_SUCCESS(s_rb_expect_create_fails(allocator, &o, NULL, AWS_ERROR_INVALID_ARGUMENT));
    }
    {
        struct aws_s3_meta_request_options o = {
            .type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
            .message = get,
            .recv_buffer = &recv_buffer,
            .recv_filepath = aws_byte_cursor_from_c_str("dummy_file")};
        ASSERT_SUCCESS(s_rb_expect_create_fails(allocator, &o, NULL, AWS_ERROR_INVALID_ARGUMENT));
    }
    /* Not empty: len must be 0. */
    {
        struct aws_byte_buf used_recv_buffer = aws_byte_buf_from_empty_array(mem, sizeof(mem));
        used_recv_buffer.len = 1;
        struct aws_s3_meta_request_options o = {
            .type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT, .message = get, .recv_buffer = &used_recv_buffer};
        ASSERT_SUCCESS(s_rb_expect_create_fails(allocator, &o, NULL, AWS_ERROR_INVALID_ARGUMENT));
    }
    /* Zero capacity: can't hold even the first request. */
    {
        struct aws_s3_meta_request_options o = {
            .type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT, .message = get, .recv_buffer = &empty_recv_buffer};
        ASSERT_SUCCESS(s_rb_expect_create_fails(allocator, &o, NULL, AWS_ERROR_SHORT_BUFFER));
    }
    /* A custom buffer pool without pre-allocated buffer support can't serve recv_buffer. */
    {
        struct aws_s3_meta_request_options o = {
            .type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT, .message = get, .recv_buffer = &recv_buffer};
        ASSERT_SUCCESS(s_rb_expect_create_fails(
            allocator, &o, s_rb_custom_pool_without_preallocated, AWS_ERROR_UNSUPPORTED_OPERATION));
    }

    aws_http_message_release(get);
    aws_http_message_release(put);
    aws_input_stream_release(put_body);
    return 0;
}

/* ---------------------------------------------------------------------------------------------------
 * Successful downloads
 * ------------------------------------------------------------------------------------------------ */

static const struct rb_env_options s_rb_8mb_parts = {.part_size = MB_TO_BYTES(8)};
static const struct rb_env_options s_rb_1mb_parts = {.part_size = MB_TO_BYTES(1)};

/* Full GET, several parts, buffer exactly the object size. */
AWS_TEST_CASE(test_s3_recv_buffer_full_object, s_test_s3_recv_buffer_full_object)
static int s_test_s3_recv_buffer_full_object(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    struct rb_get get = {.key = g_pre_existing_object_10MB};
    ASSERT_SUCCESS(
        s_rb_expect(allocator, &s_rb_1mb_parts, &get, MB_TO_BYTES(10), AWS_ERROR_SUCCESS, 0, MB_TO_BYTES(10)));
    return 0;
}

/* Buffer bigger than the object: len is the object size, the rest is untouched. */
AWS_TEST_CASE(test_s3_recv_buffer_larger_than_object, s_test_s3_recv_buffer_larger_than_object)
static int s_test_s3_recv_buffer_larger_than_object(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    struct rb_get get = {.key = g_pre_existing_object_10MB};
    ASSERT_SUCCESS(
        s_rb_expect(allocator, &s_rb_1mb_parts, &get, MB_TO_BYTES(20), AWS_ERROR_SUCCESS, 0, MB_TO_BYTES(10)));
    return 0;
}

/* Range inside the object, unaligned and crossing part boundaries: data starts at buffer[0]. */
AWS_TEST_CASE(test_s3_recv_buffer_range, s_test_s3_recv_buffer_range)
static int s_test_s3_recv_buffer_range(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    struct rb_get get = {.key = g_pre_existing_object_10MB, .range = "bytes=1234567-9876543"};
    size_t len = 9876543 - 1234567 + 1;
    ASSERT_SUCCESS(s_rb_expect(allocator, &s_rb_1mb_parts, &get, len, AWS_ERROR_SUCCESS, 1234567, len));
    return 0;
}

/* Start-only range (bytes=A-). */
AWS_TEST_CASE(test_s3_recv_buffer_range_start_only, s_test_s3_recv_buffer_range_start_only)
static int s_test_s3_recv_buffer_range_start_only(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    struct rb_get get = {.key = g_pre_existing_object_10MB, .range = "bytes=3000000-"};
    size_t len = MB_TO_BYTES(10) - 3000000;
    ASSERT_SUCCESS(s_rb_expect(allocator, &s_rb_1mb_parts, &get, len, AWS_ERROR_SUCCESS, 3000000, len));
    return 0;
}

/* Range running past the end of the object: cut short, len reports it. */
AWS_TEST_CASE(test_s3_recv_buffer_range_past_end, s_test_s3_recv_buffer_range_past_end)
static int s_test_s3_recv_buffer_range_past_end(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    struct rb_get get = {.key = g_pre_existing_object_10MB, .range = "bytes=9437184-20971519"}; /* 9 MiB - 20 MiB */
    size_t requested = 20971519 - 9437184 + 1;
    ASSERT_SUCCESS(s_rb_expect(
        allocator, &s_rb_1mb_parts, &get, requested, AWS_ERROR_SUCCESS, 9437184, MB_TO_BYTES(10) - 9437184));
    return 0;
}

/* Suffix range smaller than the object: the last N bytes, at buffer[0]. */
AWS_TEST_CASE(test_s3_recv_buffer_suffix_range, s_test_s3_recv_buffer_suffix_range)
static int s_test_s3_recv_buffer_suffix_range(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    struct rb_get get = {.key = g_pre_existing_object_10MB, .range = "bytes=-3000000"};
    ASSERT_SUCCESS(
        s_rb_expect(allocator, &s_rb_1mb_parts, &get, 3000000, AWS_ERROR_SUCCESS, MB_TO_BYTES(10) - 3000000, 3000000));
    return 0;
}

/* Suffix range bigger than the object: the whole object. */
AWS_TEST_CASE(
    test_s3_recv_buffer_suffix_range_larger_than_object,
    s_test_s3_recv_buffer_suffix_range_larger_than_object)
static int s_test_s3_recv_buffer_suffix_range_larger_than_object(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    struct rb_get get = {.key = g_pre_existing_object_10MB, .range = "bytes=-20971520"};
    ASSERT_SUCCESS(
        s_rb_expect(allocator, &s_rb_1mb_parts, &get, MB_TO_BYTES(20), AWS_ERROR_SUCCESS, 0, MB_TO_BYTES(10)));
    return 0;
}

/* Buffer smaller than a part, object fits, no size hint: the first request is sized down. */
AWS_TEST_CASE(test_s3_recv_buffer_smaller_than_part, s_test_s3_recv_buffer_smaller_than_part)
static int s_test_s3_recv_buffer_smaller_than_part(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    struct rb_get get = {.key = g_pre_existing_object_1MB};
    ASSERT_SUCCESS(s_rb_expect(allocator, &s_rb_8mb_parts, &get, MB_TO_BYTES(1), AWS_ERROR_SUCCESS, 0, MB_TO_BYTES(1)));
    return 0;
}

/* Same, with checksum validation on. */
AWS_TEST_CASE(test_s3_recv_buffer_smaller_than_part_checksum, s_test_s3_recv_buffer_smaller_than_part_checksum)
static int s_test_s3_recv_buffer_smaller_than_part_checksum(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    struct rb_get get = {.key = g_pre_existing_object_1MB, .validate_checksum = true};
    ASSERT_SUCCESS(s_rb_expect(allocator, &s_rb_8mb_parts, &get, MB_TO_BYTES(1), AWS_ERROR_SUCCESS, 0, MB_TO_BYTES(1)));
    return 0;
}

/* Same, with a size hint (which would normally pick the partNumber=1 path). */
AWS_TEST_CASE(test_s3_recv_buffer_smaller_than_part_size_hint, s_test_s3_recv_buffer_smaller_than_part_size_hint)
static int s_test_s3_recv_buffer_smaller_than_part_size_hint(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    uint64_t hint = MB_TO_BYTES(1);
    struct rb_get get = {.key = g_pre_existing_object_1MB, .object_size_hint = &hint};
    ASSERT_SUCCESS(s_rb_expect(allocator, &s_rb_8mb_parts, &get, MB_TO_BYTES(1), AWS_ERROR_SUCCESS, 0, MB_TO_BYTES(1)));
    return 0;
}

/* Empty object, with a normal buffer and with a 1-byte buffer. */
AWS_TEST_CASE(test_s3_recv_buffer_empty_object, s_test_s3_recv_buffer_empty_object)
static int s_test_s3_recv_buffer_empty_object(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    struct rb_get get = {.key = g_pre_existing_empty_object};
    ASSERT_SUCCESS(s_rb_expect(allocator, &s_rb_8mb_parts, &get, MB_TO_BYTES(1), AWS_ERROR_SUCCESS, 0, 0));
    ASSERT_SUCCESS(s_rb_expect(allocator, &s_rb_8mb_parts, &get, 1, AWS_ERROR_SUCCESS, 0, 0));
    return 0;
}

/* Object exactly one part, with capacity part-1, part, and part+1. */
AWS_TEST_CASE(test_s3_recv_buffer_part_size_boundaries, s_test_s3_recv_buffer_part_size_boundaries)
static int s_test_s3_recv_buffer_part_size_boundaries(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    size_t part = MB_TO_BYTES(1);
    struct rb_get get = {.key = g_pre_existing_object_1MB};
    ASSERT_SUCCESS(s_rb_expect(allocator, &s_rb_1mb_parts, &get, part - 1, AWS_ERROR_SHORT_BUFFER, 0, 0));
    ASSERT_SUCCESS(s_rb_expect(allocator, &s_rb_1mb_parts, &get, part, AWS_ERROR_SUCCESS, 0, part));
    ASSERT_SUCCESS(s_rb_expect(allocator, &s_rb_1mb_parts, &get, part + 1, AWS_ERROR_SUCCESS, 0, part));
    return 0;
}

/* Wrong size hint (says 1 MiB, object is 10 MiB) with a big buffer: the partNumber=1 request is too
 * small, gets cancelled, and the client falls back to ranged gets. */
AWS_TEST_CASE(test_s3_recv_buffer_wrong_size_hint, s_test_s3_recv_buffer_wrong_size_hint)
static int s_test_s3_recv_buffer_wrong_size_hint(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    uint64_t hint = MB_TO_BYTES(1);
    struct rb_get get = {.key = g_pre_existing_object_10MB, .object_size_hint = &hint};
    ASSERT_SUCCESS(
        s_rb_expect(allocator, &s_rb_8mb_parts, &get, MB_TO_BYTES(10), AWS_ERROR_SUCCESS, 0, MB_TO_BYTES(10)));
    return 0;
}

/* Per-request part_size override. */
AWS_TEST_CASE(test_s3_recv_buffer_part_size_override, s_test_s3_recv_buffer_part_size_override)
static int s_test_s3_recv_buffer_part_size_override(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    struct rb_get get = {.key = g_pre_existing_object_10MB, .part_size = MB_TO_BYTES(2)};
    ASSERT_SUCCESS(
        s_rb_expect(allocator, &s_rb_8mb_parts, &get, MB_TO_BYTES(10), AWS_ERROR_SUCCESS, 0, MB_TO_BYTES(10)));
    return 0;
}

/* Checksum validation on a multipart download of an object that carries a checksum. */
AWS_TEST_CASE(test_s3_recv_buffer_checksum, s_test_s3_recv_buffer_checksum)
static int s_test_s3_recv_buffer_checksum(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    struct rb_get get = {.key = g_pre_existing_object_10MB, .validate_checksum = true};
    ASSERT_SUCCESS(
        s_rb_expect(allocator, &s_rb_1mb_parts, &get, MB_TO_BYTES(10), AWS_ERROR_SUCCESS, 0, MB_TO_BYTES(10)));
    return 0;
}

/* Backpressure on, with a small initial window: the client must open the window itself (the caller
 * gets no body callbacks to do it), or the download stalls. */
AWS_TEST_CASE(test_s3_recv_buffer_backpressure, s_test_s3_recv_buffer_backpressure)
static int s_test_s3_recv_buffer_backpressure(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    struct rb_env_options opts = {.part_size = MB_TO_BYTES(1), .backpressure_window = MB_TO_BYTES(1)};
    struct rb_get get = {.key = g_pre_existing_object_10MB};
    ASSERT_SUCCESS(s_rb_expect(allocator, &opts, &get, MB_TO_BYTES(10), AWS_ERROR_SUCCESS, 0, MB_TO_BYTES(10)));
    return 0;
}

/* The same buffer reused for two downloads in a row, resetting len in between. */
AWS_TEST_CASE(test_s3_recv_buffer_reuse, s_test_s3_recv_buffer_reuse)
static int s_test_s3_recv_buffer_reuse(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    struct rb_env env;
    ASSERT_SUCCESS(s_rb_env_init(allocator, &env, &s_rb_1mb_parts));
    struct rb_buffer b;
    s_rb_buffer_init(allocator, &b, MB_TO_BYTES(10));
    struct rb_get_result result;

    struct rb_get first = {.key = g_pre_existing_object_10MB};
    ASSERT_SUCCESS(s_rb_download(allocator, &env, &first, &b.buf, &result));
    ASSERT_INT_EQUALS(AWS_ERROR_SUCCESS, result.error_code);
    ASSERT_UINT_EQUALS(MB_TO_BYTES(10), b.buf.len);

    b.buf.len = 0;
    struct rb_get second = {.key = g_pre_existing_object_10MB, .range = "bytes=5000000-5999999"};
    ASSERT_SUCCESS(s_rb_download(allocator, &env, &second, &b.buf, &result));
    ASSERT_INT_EQUALS(AWS_ERROR_SUCCESS, result.error_code);
    ASSERT_UINT_EQUALS(1000000, b.buf.len);
    ASSERT_SUCCESS(s_rb_check_pattern(b.mem, 5000000, 1000000));

    s_rb_buffer_clean_up(&b);
    s_rb_env_clean_up(&env);
    return 0;
}

/* Several downloads at once on one client, each with its own buffer, plus a normal body_callback
 * download on the same client: every buffer gets the right bytes, and the other download is unaffected. */
AWS_TEST_CASE(test_s3_recv_buffer_concurrent, s_test_s3_recv_buffer_concurrent)
static int s_test_s3_recv_buffer_concurrent(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    struct rb_env env;
    ASSERT_SUCCESS(s_rb_env_init(allocator, &env, &s_rb_1mb_parts));
    struct aws_string *host_name =
        aws_s3_tester_build_endpoint_string(allocator, &g_test_bucket_name, &g_test_s3_region);

    struct {
        struct rb_get get;
        size_t capacity;
        uint64_t expected_offset;
        size_t expected_len;
    } cases[] = {
        {{.key = g_pre_existing_object_10MB}, MB_TO_BYTES(10), 0, MB_TO_BYTES(10)},
        {{.key = g_pre_existing_object_1MB}, MB_TO_BYTES(1), 0, MB_TO_BYTES(1)},
        {{.key = g_pre_existing_object_10MB, .range = "bytes=1234567-5678900"},
         5678900 - 1234567 + 1,
         1234567,
         5678900 - 1234567 + 1},
        {{.key = g_pre_existing_object_10MB, .range = "bytes=-2500000"}, 2500000, MB_TO_BYTES(10) - 2500000, 2500000},
    };
    enum { N = AWS_ARRAY_SIZE(cases) };
    struct rb_buffer buffers[N];
    struct aws_http_message *messages[N + 1];
    struct aws_s3_meta_request *meta_requests[N + 1];
    struct aws_s3_meta_request_test_results results[N + 1];

    for (size_t i = 0; i < N + 1; ++i) {
        aws_s3_meta_request_test_results_init(&results[i], allocator);
        struct rb_get plain = {.key = g_pre_existing_object_10MB};
        messages[i] = s_rb_get_message(allocator, host_name, i < N ? &cases[i].get : &plain);
        ASSERT_NOT_NULL(messages[i]);
        struct aws_s3_meta_request_options options = {
            .type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
            .message = messages[i],
        };
        ASSERT_SUCCESS(aws_s3_tester_bind_meta_request(&env.tester, &options, &results[i]));
        if (i < N) {
            s_rb_buffer_init(allocator, &buffers[i], cases[i].capacity);
            options.recv_buffer = &buffers[i].buf;
            options.body_callback = NULL;
        } /* else: the last one is a normal body_callback download */
        meta_requests[i] = aws_s3_client_make_meta_request(env.client, &options);
        ASSERT_NOT_NULL(meta_requests[i]);
    }

    aws_s3_tester_wait_for_meta_request_finish(&env.tester);

    for (size_t i = 0; i < N; ++i) {
        ASSERT_INT_EQUALS(AWS_ERROR_SUCCESS, results[i].finished_error_code);
        ASSERT_UINT_EQUALS(cases[i].expected_len, buffers[i].buf.len);
        ASSERT_SUCCESS(s_rb_check_pattern(buffers[i].mem, cases[i].expected_offset, cases[i].expected_len));
        ASSERT_SUCCESS(s_rb_check_untouched(&buffers[i], cases[i].expected_len));
    }
    ASSERT_INT_EQUALS(AWS_ERROR_SUCCESS, results[N].finished_error_code);
    ASSERT_UINT_EQUALS(MB_TO_BYTES(10), results[N].received_body_size);

    for (size_t i = 0; i < N + 1; ++i) {
        aws_s3_meta_request_release(meta_requests[i]);
    }
    aws_s3_tester_wait_for_meta_request_shutdown(&env.tester);
    for (size_t i = 0; i < N + 1; ++i) {
        aws_s3_meta_request_test_results_clean_up(&results[i]);
        aws_http_message_release(messages[i]);
        if (i < N) {
            s_rb_buffer_clean_up(&buffers[i]);
        }
    }
    aws_string_destroy(host_name);
    s_rb_env_clean_up(&env);
    return 0;
}

/* A custom buffer pool that implements add/remove_preallocated_buffer can serve recv_buffer downloads. */
AWS_TEST_CASE(test_s3_recv_buffer_custom_pool_with_preallocated, s_test_s3_recv_buffer_custom_pool_with_preallocated)
static int s_test_s3_recv_buffer_custom_pool_with_preallocated(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    struct rb_env_options opts = {
        .part_size = MB_TO_BYTES(1),
        .buffer_pool_factory_fn = s_rb_custom_pool_with_preallocated,
    };
    struct rb_get get = {.key = g_pre_existing_object_10MB};
    ASSERT_SUCCESS(s_rb_expect(allocator, &opts, &get, MB_TO_BYTES(10), AWS_ERROR_SUCCESS, 0, MB_TO_BYTES(10)));
    return 0;
}

/* ---------------------------------------------------------------------------------------------------
 * Failures
 * ------------------------------------------------------------------------------------------------ */

/* Buffer 1 byte smaller than the object. */
AWS_TEST_CASE(test_s3_recv_buffer_one_byte_short, s_test_s3_recv_buffer_one_byte_short)
static int s_test_s3_recv_buffer_one_byte_short(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    struct rb_get get = {.key = g_pre_existing_object_10MB};
    ASSERT_SUCCESS(s_rb_expect(allocator, &s_rb_1mb_parts, &get, MB_TO_BYTES(10) - 1, AWS_ERROR_SHORT_BUFFER, 0, 0));
    return 0;
}

/* Buffer smaller than a part, object much bigger: fails after the first (sized-down) request. */
AWS_TEST_CASE(test_s3_recv_buffer_small_buffer_big_object, s_test_s3_recv_buffer_small_buffer_big_object)
static int s_test_s3_recv_buffer_small_buffer_big_object(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    struct rb_get get = {.key = g_pre_existing_object_10MB};
    ASSERT_SUCCESS(s_rb_expect(allocator, &s_rb_8mb_parts, &get, MB_TO_BYTES(1), AWS_ERROR_SHORT_BUFFER, 0, 0));
    return 0;
}

/* Range starting past the end of the object: S3 rejects it; len is not set. */
AWS_TEST_CASE(test_s3_recv_buffer_range_starts_past_end, s_test_s3_recv_buffer_range_starts_past_end)
static int s_test_s3_recv_buffer_range_starts_past_end(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    struct rb_env env;
    ASSERT_SUCCESS(s_rb_env_init(allocator, &env, &s_rb_1mb_parts));
    struct rb_buffer b;
    s_rb_buffer_init(allocator, &b, MB_TO_BYTES(1));

    struct rb_get get = {.key = g_pre_existing_object_10MB, .range = "bytes=20971520-"};
    struct rb_get_result result;
    ASSERT_SUCCESS(s_rb_download(allocator, &env, &get, &b.buf, &result));
    ASSERT_TRUE(result.error_code != AWS_ERROR_SUCCESS);
    ASSERT_UINT_EQUALS(0, b.buf.len);
    ASSERT_SUCCESS(s_rb_check_untouched(&b, 0));

    s_rb_buffer_clean_up(&b);
    s_rb_env_clean_up(&env);
    return 0;
}

/* Cancel right after starting, with many parts in flight; the caller frees the buffer as soon as the
 * request finishes (before shutdown). Nothing may write to it after the finish callback. */
AWS_TEST_CASE(test_s3_recv_buffer_cancel, s_test_s3_recv_buffer_cancel)
static int s_test_s3_recv_buffer_cancel(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    struct rb_env env;
    ASSERT_SUCCESS(s_rb_env_init(allocator, &env, &s_rb_1mb_parts));
    struct aws_string *host_name =
        aws_s3_tester_build_endpoint_string(allocator, &g_test_bucket_name, &g_test_s3_region);
    struct rb_get get = {.key = g_pre_existing_object_10MB};
    struct aws_http_message *message = s_rb_get_message(allocator, host_name, &get);

    struct rb_buffer b;
    s_rb_buffer_init(allocator, &b, MB_TO_BYTES(10));
    struct aws_s3_meta_request_options options = {
        .type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .message = message,
        .recv_buffer = &b.buf,
    };
    struct aws_s3_meta_request_test_results results;
    aws_s3_meta_request_test_results_init(&results, allocator);
    ASSERT_SUCCESS(aws_s3_tester_bind_meta_request(&env.tester, &options, &results));
    options.body_callback = NULL;

    struct aws_s3_meta_request *meta_request = aws_s3_client_make_meta_request(env.client, &options);
    ASSERT_NOT_NULL(meta_request);
    aws_s3_meta_request_cancel(meta_request);

    aws_s3_tester_wait_for_meta_request_finish(&env.tester);
    ASSERT_INT_EQUALS(AWS_ERROR_S3_CANCELED, results.finished_error_code);
    ASSERT_UINT_EQUALS(0, b.buf.len);
    /* Allowed as soon as the finish callback has fired. */
    s_rb_buffer_clean_up(&b);

    aws_s3_meta_request_release(meta_request);
    aws_s3_tester_wait_for_meta_request_shutdown(&env.tester);
    aws_s3_meta_request_test_results_clean_up(&results);
    aws_http_message_release(message);
    aws_string_destroy(host_name);
    s_rb_env_clean_up(&env);
    return 0;
}

/* ---------------------------------------------------------------------------------------------------
 * Pause, then resume manually
 * ------------------------------------------------------------------------------------------------ */

/* aws-c-s3 can't resume a download from a token, but the token's continuous_downloaded_bytes tells the
 * caller how much of the buffer already holds correct data. The caller resumes with a ranged GET from
 * that offset into a recv_buffer that views the rest of the same array. */
static struct {
    struct aws_mutex mutex;
    uint64_t bytes_seen;
    bool pause_requested;
    bool pause_completed;
    int pause_error_code;
    struct aws_s3_meta_request_resume_token *token;
} s_rb_pause;

static void s_rb_pause_complete(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_meta_request_resume_token *resume_token,
    int error_code,
    void *user_data) {
    (void)meta_request;
    (void)user_data;
    aws_mutex_lock(&s_rb_pause.mutex);
    s_rb_pause.pause_completed = true;
    s_rb_pause.pause_error_code = error_code;
    if (resume_token != NULL) {
        s_rb_pause.token = aws_s3_meta_request_resume_token_acquire(resume_token);
    }
    aws_mutex_unlock(&s_rb_pause.mutex);
}

/* Pause once a couple of MiB have arrived. */
static void s_rb_pause_progress(
    struct aws_s3_meta_request *meta_request,
    const struct aws_s3_meta_request_progress *progress,
    void *user_data) {
    (void)user_data;
    aws_mutex_lock(&s_rb_pause.mutex);
    s_rb_pause.bytes_seen += progress->bytes_transferred;
    bool pause_now = !s_rb_pause.pause_requested && s_rb_pause.bytes_seen >= MB_TO_BYTES(2);
    if (pause_now) {
        s_rb_pause.pause_requested = true;
    }
    aws_mutex_unlock(&s_rb_pause.mutex);
    if (pause_now) {
        aws_s3_meta_request_pause_async(meta_request, s_rb_pause_complete, NULL);
    }
}

AWS_TEST_CASE(test_s3_recv_buffer_pause_then_resume_with_range, s_test_s3_recv_buffer_pause_then_resume_with_range)
static int s_test_s3_recv_buffer_pause_then_resume_with_range(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    const size_t object_size = MB_TO_BYTES(10);

    AWS_ZERO_STRUCT(s_rb_pause);
    aws_mutex_init(&s_rb_pause.mutex);

    /* One connection, so the pause lands partway through. */
    struct rb_env_options opts = {.part_size = MB_TO_BYTES(1), .max_active_connections = 1};
    struct rb_env env;
    ASSERT_SUCCESS(s_rb_env_init(allocator, &env, &opts));
    struct aws_string *host_name =
        aws_s3_tester_build_endpoint_string(allocator, &g_test_bucket_name, &g_test_s3_region);
    struct rb_buffer b;
    s_rb_buffer_init(allocator, &b, object_size);

    /* --- First download, paused partway --- */
    struct rb_get get = {.key = g_pre_existing_object_10MB};
    struct aws_http_message *message = s_rb_get_message(allocator, host_name, &get);
    struct aws_s3_meta_request_options options = {
        .type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .message = message,
        .recv_buffer = &b.buf,
    };
    struct aws_s3_meta_request_test_results results;
    aws_s3_meta_request_test_results_init(&results, allocator);
    ASSERT_SUCCESS(aws_s3_tester_bind_meta_request(&env.tester, &options, &results));
    options.body_callback = NULL;
    options.progress_callback = s_rb_pause_progress;

    struct aws_s3_meta_request *meta_request = aws_s3_client_make_meta_request(env.client, &options);
    ASSERT_NOT_NULL(meta_request);
    aws_s3_tester_wait_for_meta_request_finish(&env.tester);
    ASSERT_INT_EQUALS(AWS_ERROR_S3_PAUSED, results.finished_error_code);
    ASSERT_UINT_EQUALS(0, b.buf.len); /* len is only set on success */
    aws_s3_meta_request_release(meta_request);
    aws_s3_tester_wait_for_meta_request_shutdown(&env.tester);
    aws_s3_meta_request_test_results_clean_up(&results);
    aws_http_message_release(message);

    aws_mutex_lock(&s_rb_pause.mutex);
    ASSERT_TRUE(s_rb_pause.pause_completed);
    ASSERT_INT_EQUALS(AWS_ERROR_SUCCESS, s_rb_pause.pause_error_code);
    ASSERT_NOT_NULL(s_rb_pause.token);
    struct aws_s3_meta_request_resume_token *token = s_rb_pause.token;
    aws_mutex_unlock(&s_rb_pause.mutex);

    uint64_t range_start = aws_s3_meta_request_resume_token_object_range_start(token);
    uint64_t done = aws_s3_meta_request_resume_token_continuous_downloaded_bytes(token);
    ASSERT_UINT_EQUALS(0, range_start);
    ASSERT_UINT_EQUALS(object_size - 1, aws_s3_meta_request_resume_token_object_range_end(token));
    ASSERT_TRUE(done > 0 && done < object_size, "pause should land partway (done=%" PRIu64 ")", done);
    /* What the token says is done really is in the buffer. */
    ASSERT_SUCCESS(s_rb_check_pattern(b.mem, range_start, (size_t)done));

    /* --- Resume: ranged GET for the rest, into a view of the rest of the same array --- */
    char range[64];
    snprintf(range, sizeof(range), "bytes=%" PRIu64 "-", range_start + done);
    struct rb_get rest = {.key = g_pre_existing_object_10MB, .range = range};
    struct aws_byte_buf rest_view = aws_byte_buf_from_empty_array(b.mem + done, object_size - (size_t)done);
    struct rb_get_result result;
    ASSERT_SUCCESS(s_rb_download(allocator, &env, &rest, &rest_view, &result));
    ASSERT_INT_EQUALS(AWS_ERROR_SUCCESS, result.error_code);
    ASSERT_UINT_EQUALS(object_size - done, rest_view.len);

    /* The whole array now holds the whole object, and nothing past it was written. */
    ASSERT_SUCCESS(s_rb_check_pattern(b.mem, 0, object_size));
    ASSERT_SUCCESS(s_rb_check_untouched(&b, object_size));

    aws_s3_meta_request_resume_token_release(token);
    aws_mutex_clean_up(&s_rb_pause.mutex);
    s_rb_buffer_clean_up(&b);
    aws_string_destroy(host_name);
    s_rb_env_clean_up(&env);
    return 0;
}
