/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_auto_ranged_get.h"
#include "aws/s3/private/s3_auto_ranged_put.h"
#include "aws/s3/private/s3_bitmap.h"
#include "aws/s3/private/s3_client_impl.h"
#include "aws/s3/private/s3_copy_object.h"
#include "aws/s3/private/s3_util.h"
#include "aws/s3/s3_client.h"
#include "s3_tester.h"

#include <aws/common/file.h>
#include <aws/io/stream.h>
#include <aws/s3/s3_client.h>
#include <aws/testing/aws_test_harness.h>
#include <inttypes.h>

#define TEST_CASE(NAME)                                                                                                \
    AWS_TEST_CASE(NAME, s_test_##NAME);                                                                                \
    static int s_test_##NAME(struct aws_allocator *allocator, void *ctx)

#define DEFINE_HEADER(NAME, VALUE)                                                                                     \
    {                                                                                                                  \
        .name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL(NAME),                                                           \
        .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL(VALUE),                                                         \
    }

TEST_CASE(meta_request_auto_ranged_get_new_error_handling) {
    (void)ctx;

    struct aws_http_message *message = aws_http_message_new_request(allocator);
    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_client *client = NULL;
    struct aws_s3_tester_client_options client_options = {
        .part_size = 5 * 1024 * 1024,
    };
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_s3_meta_request_options options = {
        .message = message,
        .type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
    };
    struct aws_s3_meta_request *meta_request =
        aws_s3_meta_request_auto_ranged_get_new(allocator, client, SIZE_MAX, false, &options);

    ASSERT_NULL(meta_request);
    aws_http_message_release(message);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

TEST_CASE(meta_request_auto_ranged_put_new_error_handling) {
    (void)ctx;

    struct aws_http_message *message = aws_http_message_new_request(allocator);
    struct aws_byte_cursor body = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("write more tests");
    struct aws_input_stream *body_stream = aws_input_stream_new_from_cursor(allocator, &body);
    aws_http_message_set_body_stream(message, body_stream);

    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_client *client = NULL;
    struct aws_s3_tester_client_options client_options = {
        .part_size = 5 * 1024 * 1024,
    };
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    /* First: Fail from the aws_s3_meta_request_init_base */
    struct aws_s3_meta_request_options options = {
        .message = message,
        .type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
    };
    struct aws_s3_meta_request *meta_request =
        aws_s3_meta_request_auto_ranged_put_new(allocator, client, SIZE_MAX, true, MB_TO_BYTES(10), 2, &options);

    ASSERT_NULL(meta_request);

    /* Second: Fail from the s_try_update_part_info_from_resume_token */
    struct aws_s3_meta_request_resume_token *token = aws_s3_meta_request_resume_token_new(allocator);
    token->part_size = 1; /* Less than g_s3_min_upload_part_size */
    options.resume_token = token;
    meta_request =
        aws_s3_meta_request_auto_ranged_put_new(allocator, client, MB_TO_BYTES(8), true, MB_TO_BYTES(10), 2, &options);
    ASSERT_NULL(meta_request);
    aws_s3_meta_request_resume_token_release(token);

    /* Third: Fail from the s_try_init_resume_state_from_persisted_data */
    struct aws_s3_upload_resume_token_options token_options = {
        .upload_id = aws_byte_cursor_from_c_str("upload_id"),
        .part_size = MB_TO_BYTES(8),
        .total_num_parts = 2,
        .num_parts_completed = 1,
    };
    token = aws_s3_meta_request_resume_token_new_upload(allocator, &token_options);
    options.resume_token = token;
    ASSERT_UINT_EQUALS(AWS_S3_META_REQUEST_TYPE_PUT_OBJECT, aws_s3_meta_request_resume_token_type(token));
    ASSERT_UINT_EQUALS(token_options.part_size, aws_s3_meta_request_resume_token_part_size(token));
    ASSERT_UINT_EQUALS(token_options.total_num_parts, aws_s3_meta_request_resume_token_total_num_parts(token));
    ASSERT_UINT_EQUALS(token_options.num_parts_completed, aws_s3_meta_request_resume_token_num_parts_completed(token));
    meta_request =
        aws_s3_meta_request_auto_ranged_put_new(allocator, client, MB_TO_BYTES(8), true, MB_TO_BYTES(10), 2, &options);

    ASSERT_NULL(meta_request);

    aws_input_stream_release(body_stream);
    aws_http_message_release(message);
    aws_s3_meta_request_resume_token_release(token);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

TEST_CASE(bad_request_error_handling) {
    /* The original request without method and path. */
    (void)ctx;
    struct aws_http_message *message = aws_http_message_new_request(allocator);
    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_client *client = NULL;
    struct aws_s3_tester_client_options client_options = {
        .part_size = 5 * 1024 * 1024,
    };
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_http_header host_header = {
        .name = g_host_header_name,
        .value = aws_byte_cursor_from_c_str("s3.us-east-1.amazonaws.com"),
    };
    ASSERT_SUCCESS(aws_http_message_add_header(message, host_header));

    struct aws_s3_meta_request_options options;
    AWS_ZERO_STRUCT(options);
    options.type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT;
    options.message = message;

    struct aws_s3_meta_request_test_results meta_request_test_results;
    aws_s3_meta_request_test_results_init(&meta_request_test_results, allocator);

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request(
        &tester, client, &options, &meta_request_test_results, 0 /* Not expect success */));

    ASSERT_UINT_EQUALS(AWS_ERROR_HTTP_DATA_NOT_AVAILABLE, meta_request_test_results.finished_error_code);

    aws_s3_meta_request_test_results_clean_up(&meta_request_test_results);

    aws_http_message_release(message);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return 0;
}

TEST_CASE(make_meta_request_error_handling) {
    /* The original request without method and path. */
    (void)ctx;
    struct aws_http_message *message = aws_http_message_new_request(allocator);
    ASSERT_SUCCESS(aws_http_message_set_request_method(message, aws_http_method_get));
    ASSERT_SUCCESS(aws_http_message_set_request_path(message, aws_byte_cursor_from_c_str("/")));
    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_client *client = NULL;
    struct aws_s3_tester_client_options client_options = {
        .part_size = 5 * 1024 * 1024,
    };

    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    /* 1. Bad options type */
    struct aws_s3_meta_request_options options;
    AWS_ZERO_STRUCT(options);
    options.type = AWS_S3_META_REQUEST_TYPE_MAX;

    struct aws_s3_meta_request *meta_request = aws_s3_client_make_meta_request(client, &options);
    ASSERT_NULL(meta_request);
    /* 2. No message */
    options.type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT;

    meta_request = aws_s3_client_make_meta_request(client, &options);
    ASSERT_NULL(meta_request);

    /* 3. No message header */
    options.type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT;
    options.message = message;

    meta_request = aws_s3_client_make_meta_request(client, &options);
    ASSERT_NULL(meta_request);

    /* 4. Bad host name */
    struct aws_http_header host_header = {
        .name = g_host_header_name,
        .value = aws_byte_cursor_from_c_str("invalid:/s3.us-east-1.amazonaws.com"),
    };
    ASSERT_SUCCESS(aws_http_message_add_header(message, host_header));

    options.type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT;
    options.message = message;

    meta_request = aws_s3_client_make_meta_request(client, &options);
    ASSERT_NULL(meta_request);

    aws_http_message_release(message);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return 0;
}

TEST_CASE(download_resume_token_create_and_getters) {
    (void)ctx;

    uint32_t completed_parts[] = {1, 2, 3, 6, 7, 10};
    struct aws_s3_download_resume_token_options options = {
        .etag = aws_byte_cursor_from_c_str("\"abc123\""),
        .version_id = aws_byte_cursor_from_c_str("v1"),
        .s3_object_last_modified = aws_byte_cursor_from_c_str("Wed, 09 Oct 2024 22:28:00 GMT"),
        .part_size = 8 * 1024 * 1024,
        .first_part_size = 8 * 1024 * 1024,
        .object_range_start = 0,
        .object_range_end = 99999999,
        .object_size = 100000000,
        .total_num_parts = 13,
        .completed_parts = completed_parts,
        .num_completed_parts = 6,
        .file_last_modified_epoch_ns = 1728511680000000000ULL,
    };

    struct aws_s3_meta_request_resume_token *token =
        aws_s3_meta_request_resume_token_new_download(allocator, &options);
    ASSERT_NOT_NULL(token);

    /* Verify type */
    ASSERT_UINT_EQUALS(AWS_S3_META_REQUEST_TYPE_GET_OBJECT, aws_s3_meta_request_resume_token_type(token));

    /* Verify all getters */
    struct aws_byte_cursor etag = aws_s3_meta_request_resume_token_etag(token);
    ASSERT_TRUE(aws_byte_cursor_eq_c_str(&etag, "\"abc123\""));

    struct aws_byte_cursor version_id = aws_s3_meta_request_resume_token_version_id(token);
    ASSERT_TRUE(aws_byte_cursor_eq_c_str(&version_id, "v1"));

    struct aws_byte_cursor last_modified = aws_s3_meta_request_resume_token_s3_object_last_modified(token);
    ASSERT_TRUE(aws_byte_cursor_eq_c_str(&last_modified, "Wed, 09 Oct 2024 22:28:00 GMT"));

    ASSERT_UINT_EQUALS(8 * 1024 * 1024, aws_s3_meta_request_resume_token_part_size(token));
    ASSERT_UINT_EQUALS(8 * 1024 * 1024, aws_s3_meta_request_resume_token_first_part_size(token));
    ASSERT_UINT_EQUALS(0, aws_s3_meta_request_resume_token_object_range_start(token));
    ASSERT_UINT_EQUALS(99999999, aws_s3_meta_request_resume_token_object_range_end(token));
    ASSERT_UINT_EQUALS(100000000, aws_s3_meta_request_resume_token_object_size(token));
    ASSERT_UINT_EQUALS(13, aws_s3_meta_request_resume_token_total_num_parts(token));

    /* Verify completed parts bitmap */
    struct aws_byte_cursor bitmap = aws_s3_meta_request_resume_token_completed_parts_bitmap(token);
    /* 13 parts → 2 bytes. Parts 1,2,3,6,7,10 are set */
    ASSERT_UINT_EQUALS(2, bitmap.len);
    /* Byte 0: bits 0-7 = parts 1-8. Parts 1,2,3,6,7 → bits 0,1,2,5,6 → 0b01100111 = 0x67 */
    ASSERT_UINT_EQUALS(0x67, bitmap.ptr[0]);
    /* Byte 1: bits 8-12 = parts 9-13. Part 10 → bit 1 → 0b00000010 = 0x02 */
    ASSERT_UINT_EQUALS(0x02, bitmap.ptr[1]);

    aws_s3_meta_request_resume_token_release(token);
    return AWS_OP_SUCCESS;
}

TEST_CASE(download_resume_token_invalid_inputs) {
    (void)ctx;

    /* Missing etag should fail */
    struct aws_s3_download_resume_token_options options = {
        .etag = aws_byte_cursor_from_c_str(""),
        .part_size = 8 * 1024 * 1024,
        .first_part_size = 8 * 1024 * 1024,
        .object_range_end = 99,
        .object_size = 100,
        .total_num_parts = 1,
        .completed_parts = NULL,
        .num_completed_parts = 0,
    };
    struct aws_s3_meta_request_resume_token *token =
        aws_s3_meta_request_resume_token_new_download(allocator, &options);
    ASSERT_NULL(token);

    /* Zero part_size should fail */
    options.etag = aws_byte_cursor_from_c_str("\"abc\"");
    options.part_size = 0;
    token = aws_s3_meta_request_resume_token_new_download(allocator, &options);
    ASSERT_NULL(token);

    return AWS_OP_SUCCESS;
}


TEST_CASE(s3_bitmap_set_and_get) {
    (void)ctx;

    struct aws_byte_buf bitmap;
    aws_s3_bitmap_init(&bitmap, allocator, 16);

    /* Initially all bits are unset */
    for (uint32_t i = 1; i <= 16; ++i) {
        ASSERT_FALSE(aws_s3_bitmap_get(&bitmap, i));
    }

    /* Set some bits with gaps */
    aws_s3_bitmap_set(&bitmap, 1);
    aws_s3_bitmap_set(&bitmap, 3);
    aws_s3_bitmap_set(&bitmap, 5);
    aws_s3_bitmap_set(&bitmap, 16);

    ASSERT_TRUE(aws_s3_bitmap_get(&bitmap, 1));
    ASSERT_FALSE(aws_s3_bitmap_get(&bitmap, 2));
    ASSERT_TRUE(aws_s3_bitmap_get(&bitmap, 3));
    ASSERT_FALSE(aws_s3_bitmap_get(&bitmap, 4));
    ASSERT_TRUE(aws_s3_bitmap_get(&bitmap, 5));
    ASSERT_TRUE(aws_s3_bitmap_get(&bitmap, 16));

    /* Out of range returns false */
    ASSERT_FALSE(aws_s3_bitmap_get(&bitmap, 0));
    ASSERT_FALSE(aws_s3_bitmap_get(&bitmap, 17));

    /* Setting 0 or out of range is a no-op */
    aws_s3_bitmap_set(&bitmap, 0);
    aws_s3_bitmap_set(&bitmap, 17);

    aws_byte_buf_clean_up(&bitmap);
    return AWS_OP_SUCCESS;
}

TEST_CASE(s3_bitmap_init_from_array) {
    (void)ctx;

    struct aws_byte_buf bitmap;

    /* Valid array with gaps */
    uint32_t parts[] = {1, 3, 5, 13};
    ASSERT_SUCCESS(aws_s3_bitmap_init_from_array(&bitmap, allocator, 13, parts, AWS_ARRAY_SIZE(parts)));
    ASSERT_TRUE(aws_s3_bitmap_get(&bitmap, 1));
    ASSERT_FALSE(aws_s3_bitmap_get(&bitmap, 2));
    ASSERT_TRUE(aws_s3_bitmap_get(&bitmap, 3));
    ASSERT_FALSE(aws_s3_bitmap_get(&bitmap, 4));
    ASSERT_TRUE(aws_s3_bitmap_get(&bitmap, 5));
    ASSERT_TRUE(aws_s3_bitmap_get(&bitmap, 13));
    aws_byte_buf_clean_up(&bitmap);

    /* Empty array produces an all-zero bitmap */
    ASSERT_SUCCESS(aws_s3_bitmap_init_from_array(&bitmap, allocator, 8, NULL, 0));
    for (uint32_t i = 1; i <= 8; ++i) {
        ASSERT_FALSE(aws_s3_bitmap_get(&bitmap, i));
    }
    aws_byte_buf_clean_up(&bitmap);

    /* Index 0 is invalid */
    uint32_t zero_part[] = {0};
    ASSERT_ERROR(
        AWS_ERROR_INVALID_ARGUMENT, aws_s3_bitmap_init_from_array(&bitmap, allocator, 8, zero_part, 1));

    /* Index above capacity is invalid */
    uint32_t oob_part[] = {9};
    ASSERT_ERROR(
        AWS_ERROR_INVALID_ARGUMENT, aws_s3_bitmap_init_from_array(&bitmap, allocator, 8, oob_part, 1));

    return AWS_OP_SUCCESS;
}

TEST_CASE(s3_bitmap_to_array_round_trip) {
    (void)ctx;

    struct aws_byte_buf bitmap;
    uint32_t parts[] = {1, 2, 3, 6, 7, 10};
    ASSERT_SUCCESS(aws_s3_bitmap_init_from_array(&bitmap, allocator, 13, parts, AWS_ARRAY_SIZE(parts)));

    /* Convert back to an array and expect the same 1-based indices in ascending order */
    struct aws_array_list out_parts;
    ASSERT_SUCCESS(aws_array_list_init_dynamic(&out_parts, allocator, 0, sizeof(uint32_t)));
    ASSERT_SUCCESS(aws_s3_bitmap_to_array(&bitmap, 13, &out_parts));

    ASSERT_UINT_EQUALS(AWS_ARRAY_SIZE(parts), aws_array_list_length(&out_parts));
    for (size_t i = 0; i < AWS_ARRAY_SIZE(parts); ++i) {
        uint32_t part_num = 0;
        ASSERT_SUCCESS(aws_array_list_get_at(&out_parts, &part_num, i));
        ASSERT_UINT_EQUALS(parts[i], part_num);
    }

    aws_array_list_clean_up(&out_parts);
    aws_byte_buf_clean_up(&bitmap);

    /* Empty bitmap converts to an empty array */
    ASSERT_SUCCESS(aws_s3_bitmap_init(&bitmap, allocator, 8));
    ASSERT_SUCCESS(aws_array_list_init_dynamic(&out_parts, allocator, 0, sizeof(uint32_t)));
    ASSERT_SUCCESS(aws_s3_bitmap_to_array(&bitmap, 8, &out_parts));
    ASSERT_UINT_EQUALS(0, aws_array_list_length(&out_parts));

    aws_array_list_clean_up(&out_parts);
    aws_byte_buf_clean_up(&bitmap);
    return AWS_OP_SUCCESS;
}

/*******************************************************************************
 * Download resume validation tests (local, no network requests are sent).
 ******************************************************************************/

#define RESUME_TEST_OBJECT_SIZE MB_TO_BYTES(100)
#define RESUME_TEST_PART_SIZE MB_TO_BYTES(8)
#define RESUME_TEST_TOTAL_NUM_PARTS 13
#define RESUME_TEST_FILE_MTIME_NS 1728511680000000000ULL

/* Build a valid set of download resume token options. Individual tests override fields. */
static struct aws_s3_download_resume_token_options s_valid_download_token_options(
    const uint32_t *completed_parts,
    size_t num_completed_parts) {

    struct aws_s3_download_resume_token_options options = {
        .etag = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("\"resume-test-etag\""),
        .part_size = RESUME_TEST_PART_SIZE,
        .first_part_size = RESUME_TEST_PART_SIZE,
        .object_size = RESUME_TEST_OBJECT_SIZE,
        .total_num_parts = RESUME_TEST_TOTAL_NUM_PARTS,
        .completed_parts = completed_parts,
        .num_completed_parts = num_completed_parts,
        .file_last_modified_epoch_ns = RESUME_TEST_FILE_MTIME_NS,
    };
    return options;
}

TEST_CASE(download_resume_token_range_normalization) {
    (void)ctx;
    uint32_t completed_parts[] = {1, 2, 3};

    /* Range not set: normalized to the full object */
    struct aws_s3_download_resume_token_options options =
        s_valid_download_token_options(completed_parts, AWS_ARRAY_SIZE(completed_parts));
    struct aws_s3_meta_request_resume_token *token = aws_s3_meta_request_resume_token_new_download(allocator, &options);
    ASSERT_NOT_NULL(token);
    ASSERT_UINT_EQUALS(0, aws_s3_meta_request_resume_token_object_range_start(token));
    ASSERT_UINT_EQUALS(RESUME_TEST_OBJECT_SIZE - 1, aws_s3_meta_request_resume_token_object_range_end(token));
    aws_s3_meta_request_resume_token_release(token);

    /* Explicit range: preserved as given */
    options = s_valid_download_token_options(completed_parts, AWS_ARRAY_SIZE(completed_parts));
    options.object_range_start = 1000;
    options.object_range_end = 999999;
    token = aws_s3_meta_request_resume_token_new_download(allocator, &options);
    ASSERT_NOT_NULL(token);
    ASSERT_UINT_EQUALS(1000, aws_s3_meta_request_resume_token_object_range_start(token));
    ASSERT_UINT_EQUALS(999999, aws_s3_meta_request_resume_token_object_range_end(token));
    aws_s3_meta_request_resume_token_release(token);

    /* Zero object_size is invalid: nothing to resume */
    options = s_valid_download_token_options(completed_parts, AWS_ARRAY_SIZE(completed_parts));
    options.object_size = 0;
    token = aws_s3_meta_request_resume_token_new_download(allocator, &options);
    ASSERT_NULL(token);
    ASSERT_UINT_EQUALS(AWS_ERROR_INVALID_ARGUMENT, aws_last_error());

    return AWS_OP_SUCCESS;
}

/* Shared harness for creating an auto-ranged GET with a resume token against a local client. */
struct get_resume_creation_test {
    struct aws_s3_tester tester;
    struct aws_s3_client *client;
    struct aws_http_message *message;
    struct aws_s3_meta_request_resume_token *token;
};

static int s_get_resume_creation_test_init(
    struct get_resume_creation_test *fixture,
    struct aws_allocator *allocator,
    struct aws_byte_cursor request_path,
    const struct aws_s3_download_resume_token_options *token_options) {

    AWS_ZERO_STRUCT(*fixture);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &fixture->tester));
    struct aws_s3_tester_client_options client_options = {
        .part_size = RESUME_TEST_PART_SIZE,
    };
    ASSERT_SUCCESS(aws_s3_tester_client_new(&fixture->tester, &client_options, &fixture->client));

    fixture->message = aws_s3_test_get_object_request_new(
        allocator, aws_byte_cursor_from_c_str("test-bucket.s3.us-west-2.amazonaws.com"), request_path);
    ASSERT_NOT_NULL(fixture->message);

    if (token_options != NULL) {
        fixture->token = aws_s3_meta_request_resume_token_new_download(allocator, token_options);
        ASSERT_NOT_NULL(fixture->token);
    }
    return AWS_OP_SUCCESS;
}

static void s_get_resume_creation_test_clean_up(struct get_resume_creation_test *fixture) {
    aws_s3_meta_request_resume_token_release(fixture->token);
    aws_http_message_release(fixture->message);
    aws_s3_client_release(fixture->client);
    aws_s3_tester_clean_up(&fixture->tester);
}

/* Attempt meta request creation with the fixture's message/token and given file options. */
static struct aws_s3_meta_request *s_get_resume_creation_test_create(
    struct get_resume_creation_test *fixture,
    struct aws_allocator *allocator,
    const char *recv_filepath,
    enum aws_s3_recv_file_options recv_file_option) {

    struct aws_s3_meta_request_options options = {
        .message = fixture->message,
        .type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .resume_token = fixture->token,
        .recv_file_option = recv_file_option,
    };
    if (recv_filepath != NULL) {
        options.recv_filepath = aws_byte_cursor_from_c_str(recv_filepath);
    }
    return aws_s3_meta_request_auto_ranged_get_new(
        allocator, fixture->client, RESUME_TEST_PART_SIZE, false, &options);
}

TEST_CASE(get_resume_invalid_recv_file_option) {
    (void)ctx;
    uint32_t completed_parts[] = {1, 2, 3};
    struct aws_s3_download_resume_token_options token_options =
        s_valid_download_token_options(completed_parts, AWS_ARRAY_SIZE(completed_parts));

    struct get_resume_creation_test fixture;
    ASSERT_SUCCESS(
        s_get_resume_creation_test_init(&fixture, allocator, aws_byte_cursor_from_c_str("/key"), &token_options));

    const char *filepath = "get_resume_invalid_recv_file_option_tmp";

    /* Resume token + filepath + any non-RESUME option is invalid configuration */
    enum aws_s3_recv_file_options bad_options[] = {
        AWS_S3_RECV_FILE_CREATE_OR_REPLACE,
        AWS_S3_RECV_FILE_CREATE_NEW,
        AWS_S3_RECV_FILE_CREATE_OR_APPEND,
        AWS_S3_RECV_FILE_WRITE_TO_POSITION,
    };
    for (size_t i = 0; i < AWS_ARRAY_SIZE(bad_options); ++i) {
        struct aws_s3_meta_request *meta_request =
            s_get_resume_creation_test_create(&fixture, allocator, filepath, bad_options[i]);
        ASSERT_NULL(meta_request);
        ASSERT_UINT_EQUALS(AWS_ERROR_INVALID_ARGUMENT, aws_last_error());
    }

    /* RESUME without a resume token is also invalid */
    struct aws_s3_meta_request_resume_token *token = fixture.token;
    fixture.token = NULL;
    struct aws_s3_meta_request *meta_request =
        s_get_resume_creation_test_create(&fixture, allocator, filepath, AWS_S3_RECV_FILE_RESUME);
    ASSERT_NULL(meta_request);
    ASSERT_UINT_EQUALS(AWS_ERROR_INVALID_ARGUMENT, aws_last_error());
    fixture.token = token;

    s_get_resume_creation_test_clean_up(&fixture);
    return AWS_OP_SUCCESS;
}

TEST_CASE(get_resume_version_id_validation) {
    (void)ctx;
    uint32_t completed_parts[] = {1, 2, 3};
    struct aws_s3_download_resume_token_options token_options =
        s_valid_download_token_options(completed_parts, AWS_ARRAY_SIZE(completed_parts));
    token_options.version_id = aws_byte_cursor_from_c_str("abc123");

    /* Token pins a version but request path has no versionId: invalid */
    struct get_resume_creation_test fixture;
    ASSERT_SUCCESS(
        s_get_resume_creation_test_init(&fixture, allocator, aws_byte_cursor_from_c_str("/key"), &token_options));
    struct aws_s3_meta_request *meta_request =
        s_get_resume_creation_test_create(&fixture, allocator, NULL, AWS_S3_RECV_FILE_CREATE_OR_REPLACE);
    ASSERT_NULL(meta_request);
    ASSERT_UINT_EQUALS(AWS_ERROR_INVALID_ARGUMENT, aws_last_error());
    s_get_resume_creation_test_clean_up(&fixture);

    /* Request pins a different version: invalid */
    ASSERT_SUCCESS(s_get_resume_creation_test_init(
        &fixture, allocator, aws_byte_cursor_from_c_str("/key?versionId=xyz"), &token_options));
    meta_request = s_get_resume_creation_test_create(&fixture, allocator, NULL, AWS_S3_RECV_FILE_CREATE_OR_REPLACE);
    ASSERT_NULL(meta_request);
    ASSERT_UINT_EQUALS(AWS_ERROR_INVALID_ARGUMENT, aws_last_error());
    s_get_resume_creation_test_clean_up(&fixture);

    /* Request pins the same version: valid */
    ASSERT_SUCCESS(s_get_resume_creation_test_init(
        &fixture, allocator, aws_byte_cursor_from_c_str("/key?versionId=abc123"), &token_options));
    meta_request = s_get_resume_creation_test_create(&fixture, allocator, NULL, AWS_S3_RECV_FILE_CREATE_OR_REPLACE);
    ASSERT_NOT_NULL(meta_request);
    aws_s3_meta_request_release(meta_request);
    s_get_resume_creation_test_clean_up(&fixture);

    /* Token without a version: request's versionId is not validated */
    token_options.version_id = aws_byte_cursor_from_c_str("");
    ASSERT_SUCCESS(s_get_resume_creation_test_init(
        &fixture, allocator, aws_byte_cursor_from_c_str("/key?versionId=xyz"), &token_options));
    meta_request = s_get_resume_creation_test_create(&fixture, allocator, NULL, AWS_S3_RECV_FILE_CREATE_OR_REPLACE);
    ASSERT_NOT_NULL(meta_request);
    aws_s3_meta_request_release(meta_request);
    s_get_resume_creation_test_clean_up(&fixture);

    return AWS_OP_SUCCESS;
}

static int s_get_resume_range_case(
    struct aws_allocator *allocator,
    const struct aws_s3_download_resume_token_options *token_options,
    const char *range_header_value, /* NULL for no Range header */
    bool expect_success) {

    struct get_resume_creation_test fixture;
    ASSERT_SUCCESS(
        s_get_resume_creation_test_init(&fixture, allocator, aws_byte_cursor_from_c_str("/key"), token_options));

    if (range_header_value != NULL) {
        struct aws_http_header range_header = {
            .name = g_range_header_name,
            .value = aws_byte_cursor_from_c_str(range_header_value),
        };
        ASSERT_SUCCESS(aws_http_message_add_header(fixture.message, range_header));
    }

    struct aws_s3_meta_request *meta_request =
        s_get_resume_creation_test_create(&fixture, allocator, NULL, AWS_S3_RECV_FILE_CREATE_OR_REPLACE);
    if (expect_success) {
        ASSERT_NOT_NULL(meta_request);
        aws_s3_meta_request_release(meta_request);
    } else {
        ASSERT_NULL(meta_request);
        ASSERT_UINT_EQUALS(AWS_ERROR_INVALID_ARGUMENT, aws_last_error());
    }
    s_get_resume_creation_test_clean_up(&fixture);
    return AWS_OP_SUCCESS;
}

TEST_CASE(get_resume_range_validation) {
    (void)ctx;
    uint32_t completed_parts[] = {1, 2, 3};

    /* Full-object token (range normalized to 0..object_size-1) */
    struct aws_s3_download_resume_token_options full_object =
        s_valid_download_token_options(completed_parts, AWS_ARRAY_SIZE(completed_parts));

    /* no Range header: OK */
    ASSERT_SUCCESS(s_get_resume_range_case(allocator, &full_object, NULL, true /*expect_success*/));
    /* explicit full range: equivalent to no range, OK */
    ASSERT_SUCCESS(s_get_resume_range_case(allocator, &full_object, "bytes=0-104857599", true));
    /* subrange request against full-object token: mismatch */
    ASSERT_SUCCESS(s_get_resume_range_case(allocator, &full_object, "bytes=0-999", false));

    /* Subrange token: 8MiB..24MiB-1 */
    struct aws_s3_download_resume_token_options subrange =
        s_valid_download_token_options(completed_parts, AWS_ARRAY_SIZE(completed_parts));
    subrange.object_range_start = MB_TO_BYTES(8);
    subrange.object_range_end = MB_TO_BYTES(24) - 1;

    /* no Range header against subrange token: mismatch */
    ASSERT_SUCCESS(s_get_resume_range_case(allocator, &subrange, NULL, false));
    /* exact match: OK */
    ASSERT_SUCCESS(s_get_resume_range_case(allocator, &subrange, "bytes=8388608-25165823", true));
    /* different range: mismatch */
    ASSERT_SUCCESS(s_get_resume_range_case(allocator, &subrange, "bytes=8388608-16777215", false));
    /* open-ended range: token end is not object_size-1, mismatch */
    ASSERT_SUCCESS(s_get_resume_range_case(allocator, &subrange, "bytes=8388608-", false));

    /* Tail subrange token: 8MiB..object_size-1 (what an open-ended request resolves to) */
    struct aws_s3_download_resume_token_options tail =
        s_valid_download_token_options(completed_parts, AWS_ARRAY_SIZE(completed_parts));
    tail.object_range_start = MB_TO_BYTES(8);
    tail.object_range_end = RESUME_TEST_OBJECT_SIZE - 1;

    /* open-ended: OK */
    ASSERT_SUCCESS(s_get_resume_range_case(allocator, &tail, "bytes=8388608-", true));
    /* suffix range of the same length: OK (suffix len = object_size - 8MiB) */
    ASSERT_SUCCESS(s_get_resume_range_case(allocator, &tail, "bytes=-96468992", true));
    /* suffix range of a different length: mismatch */
    ASSERT_SUCCESS(s_get_resume_range_case(allocator, &tail, "bytes=-1000", false));

    return AWS_OP_SUCCESS;
}

TEST_CASE(get_resume_file_modified_restarts) {
    (void)ctx;
    uint32_t completed_parts[] = {1, 2, 3};
    const char *filepath = "get_resume_file_modified_restarts_tmp";

    /* Create the partially-downloaded file and capture its real mtime */
    FILE *file = aws_fopen(filepath, "wb");
    ASSERT_NOT_NULL(file);
    ASSERT_TRUE(fprintf(file, "partial download contents") > 0);
    ASSERT_INT_EQUALS(0, fclose(file));

    file = aws_fopen(filepath, "rb");
    ASSERT_NOT_NULL(file);
    uint64_t real_mtime_ns = 0;
    ASSERT_SUCCESS(aws_file_get_last_modified_epoch(file, &real_mtime_ns));
    ASSERT_INT_EQUALS(0, fclose(file));

    /* Case 1: token mtime matches the file: resume state is kept */
    struct aws_s3_download_resume_token_options token_options =
        s_valid_download_token_options(completed_parts, AWS_ARRAY_SIZE(completed_parts));
    token_options.file_last_modified_epoch_ns = real_mtime_ns;

    struct get_resume_creation_test fixture;
    ASSERT_SUCCESS(
        s_get_resume_creation_test_init(&fixture, allocator, aws_byte_cursor_from_c_str("/key"), &token_options));
    struct aws_s3_meta_request *meta_request =
        s_get_resume_creation_test_create(&fixture, allocator, filepath, AWS_S3_RECV_FILE_RESUME);
    ASSERT_NOT_NULL(meta_request);
    struct aws_s3_auto_ranged_get *auto_ranged_get = meta_request->impl;
    ASSERT_NOT_NULL(auto_ranged_get->resume_token);
    ASSERT_TRUE(auto_ranged_get->delivered_parts_bitmap.len > 0);
    ASSERT_TRUE(aws_s3_bitmap_get(&auto_ranged_get->delivered_parts_bitmap, 1));
    aws_s3_meta_request_release(meta_request);
    s_get_resume_creation_test_clean_up(&fixture);

    /* Case 2: token mtime differs: resume state silently discarded, fresh download */
    token_options.file_last_modified_epoch_ns = real_mtime_ns + 12345;
    ASSERT_SUCCESS(
        s_get_resume_creation_test_init(&fixture, allocator, aws_byte_cursor_from_c_str("/key"), &token_options));
    meta_request = s_get_resume_creation_test_create(&fixture, allocator, filepath, AWS_S3_RECV_FILE_RESUME);
    ASSERT_NOT_NULL(meta_request);
    auto_ranged_get = meta_request->impl;
    ASSERT_NULL(auto_ranged_get->resume_token);
    ASSERT_UINT_EQUALS(0, auto_ranged_get->delivered_parts_bitmap.len);
    aws_s3_meta_request_release(meta_request);
    s_get_resume_creation_test_clean_up(&fixture);

    /* Case 3: file missing: resume state silently discarded, fresh download */
    struct aws_string *filepath_str = aws_string_new_from_c_str(allocator, filepath);
    ASSERT_SUCCESS(aws_file_delete(filepath_str));
    aws_string_destroy(filepath_str);
    token_options.file_last_modified_epoch_ns = real_mtime_ns;
    ASSERT_SUCCESS(
        s_get_resume_creation_test_init(&fixture, allocator, aws_byte_cursor_from_c_str("/key"), &token_options));
    meta_request = s_get_resume_creation_test_create(&fixture, allocator, filepath, AWS_S3_RECV_FILE_RESUME);
    ASSERT_NOT_NULL(meta_request);
    auto_ranged_get = meta_request->impl;
    ASSERT_NULL(auto_ranged_get->resume_token);
    aws_s3_meta_request_release(meta_request);
    s_get_resume_creation_test_clean_up(&fixture);

    remove(filepath);
    return AWS_OP_SUCCESS;
}

static void s_pause_noop_callback(
    struct aws_s3_meta_request *meta_request,
    const struct aws_s3_meta_request_resume_token *resume_token,
    int error_code,
    void *user_data) {
    (void)meta_request;
    (void)resume_token;
    (void)error_code;
    (void)user_data;
}

TEST_CASE(pause_async_unsupported_for_copy) {
    (void)ctx;

    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_client *client = NULL;
    struct aws_s3_tester_client_options client_options = {
        .part_size = 64 * 1024 * 1024,
    };
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_http_message *message = aws_http_message_new_request(allocator);
    aws_http_message_set_request_method(message, aws_http_method_put);
    aws_http_message_set_request_path(message, aws_byte_cursor_from_c_str("/dest-key"));
    struct aws_http_header host_header = {
        .name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Host"),
        .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("test-bucket.s3.us-west-2.amazonaws.com"),
    };
    aws_http_message_add_header(message, host_header);
    struct aws_http_header copy_source_header = {
        .name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-copy-source"),
        .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("/src-bucket/src-key"),
    };
    aws_http_message_add_header(message, copy_source_header);

    struct aws_s3_meta_request_options options = {
        .message = message,
        .type = AWS_S3_META_REQUEST_TYPE_COPY_OBJECT,
    };
    struct aws_s3_meta_request *meta_request = aws_s3_meta_request_copy_object_new(allocator, client, &options);
    ASSERT_NOT_NULL(meta_request);

    /* pause_async on a COPY meta request should fail synchronously with UNSUPPORTED_OPERATION */
    int result = aws_s3_meta_request_pause_async(meta_request, s_pause_noop_callback, NULL);
    ASSERT_INT_EQUALS(AWS_OP_ERR, result);
    ASSERT_UINT_EQUALS(AWS_ERROR_UNSUPPORTED_OPERATION, aws_last_error());

    aws_s3_meta_request_release(meta_request);
    aws_http_message_release(message);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}
