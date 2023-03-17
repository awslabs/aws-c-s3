/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_util.h"
#include "aws/s3/s3_client.h"
#include "s3_tester.h"
#include <aws/io/stream.h>
#include <aws/io/uri.h>
#include <aws/testing/aws_test_harness.h>
#include <inttypes.h>

#define TEST_CASE(NAME)                                                                                                \
    AWS_TEST_CASE(NAME, s_test_##NAME);                                                                                \
    static int s_test_##NAME(struct aws_allocator *allocator, void *ctx)

#define DEFINE_HEADER(NAME, VALUE)                                                                                     \
    { .name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL(NAME), .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL(VALUE), }

TEST_CASE(multipart_upload_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_tester_client_options client_options = {
        .part_size = MB_TO_BYTES(5),
        .tls_usage = AWS_S3_TLS_DISABLED,
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/default");

    struct aws_s3_tester_meta_request_options put_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .client = client,
        .checksum_algorithm = AWS_SCA_CRC32,
        .validate_get_response_checksum = false,
        .put_options =
            {
                .object_size_mb = 10,
                .object_path_override = object_path,
            },
        .mock_server = true,
    };

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &put_options, NULL));

    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

TEST_CASE(async_internal_error_from_complete_multipart_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_tester_client_options client_options = {
        .part_size = MB_TO_BYTES(5),
        .tls_usage = AWS_S3_TLS_DISABLED,
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    /* Checkout the ./mock_s3_server/CompleteMultipartUpload/async_internal_error.json for the response details */
    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/async_internal_error");

    struct aws_s3_tester_meta_request_options put_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .client = client,
        .checksum_algorithm = AWS_SCA_CRC32,
        .validate_get_response_checksum = false,
        .put_options =
            {
                .object_size_mb = 10,
                .object_path_override = object_path,
            },
        .mock_server = true,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE,
    };
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &put_options, &out_results));

    /* Internal error will be retried and failed with internal error. */
    ASSERT_UINT_EQUALS(out_results.finished_error_code, AWS_ERROR_S3_INTERNAL_ERROR);

    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

TEST_CASE(async_access_denied_from_complete_multipart_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_tester_client_options client_options = {
        .part_size = MB_TO_BYTES(5),
        .tls_usage = AWS_S3_TLS_DISABLED,
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    /* Checkout the ./mock_s3_server/CompleteMultipartUpload/async_access_denied_error.json for the response details */
    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/async_access_denied_error");

    struct aws_s3_tester_meta_request_options put_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .client = client,
        .checksum_algorithm = AWS_SCA_CRC32,
        .validate_get_response_checksum = false,
        .put_options =
            {
                .object_size_mb = 10,
                .object_path_override = object_path,
            },
        .mock_server = true,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE,
    };
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &put_options, &out_results));

    ASSERT_UINT_EQUALS(out_results.finished_error_code, AWS_ERROR_S3_NON_RECOVERABLE_ASYNC_ERROR);
    ASSERT_UINT_EQUALS(out_results.finished_response_status, AWS_S3_RESPONSE_STATUS_SUCCESS);
    ASSERT_TRUE(out_results.error_response_body.len != 0);

    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

TEST_CASE(get_object_modified_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_tester_client_options client_options = {
        .part_size = 64 * 1024,
        .tls_usage = AWS_S3_TLS_DISABLED,
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    /* Check the mock server README/GetObject Response for the response that will be received. */
    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/get_object_modified");

    struct aws_s3_tester_meta_request_options get_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .client = client,
        .get_options =
            {
                .object_path = object_path,
            },
        .mock_server = true,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE,
    };
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &get_options, &out_results));

    ASSERT_UINT_EQUALS(out_results.finished_error_code, AWS_ERROR_S3_OBJECT_MODIFIED);
    ASSERT_UINT_EQUALS(out_results.finished_response_status, AWS_HTTP_STATUS_CODE_412_PRECONDITION_FAILED);

    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

TEST_CASE(get_object_invalid_responses_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_tester_client_options client_options = {
        .part_size = 64 * 1024,
        .tls_usage = AWS_S3_TLS_DISABLED,
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    /* 1 - Mock server will response without Content-Range */
    struct aws_byte_cursor object_path =
        aws_byte_cursor_from_c_str("/get_object_invalid_response_missing_content_range");

    struct aws_s3_tester_meta_request_options get_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .client = client,
        .get_options =
            {
                .object_path = object_path,
            },
        .mock_server = true,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE,
    };
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &get_options, &out_results));

    ASSERT_UINT_EQUALS(out_results.finished_error_code, AWS_ERROR_S3_MISSING_CONTENT_RANGE_HEADER);

    /* 2 - Mock server will response without Etags */
    object_path = aws_byte_cursor_from_c_str("/get_object_invalid_response_missing_etags");
    get_options.get_options.object_path = object_path;
    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &get_options, &out_results));
    ASSERT_UINT_EQUALS(out_results.finished_error_code, AWS_ERROR_S3_MISSING_ETAG);

    /* 3 -  Mock server will response without Content-Range response for HEAD request */
    object_path = aws_byte_cursor_from_c_str("/get_object_invalid_response_missing_content_range");
    /* Put together a simple S3 Get Object request. */
    struct aws_uri mock_server;
    ASSERT_SUCCESS(aws_uri_init_parse(&mock_server, allocator, &g_mock_server_uri));
    struct aws_http_message *message =
        aws_s3_test_get_object_request_new(allocator, *aws_uri_authority(&mock_server), object_path);
    struct aws_http_header range_header = {
        .name = g_range_header_name,
        .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("bytes=0-1"),
    };
    ASSERT_SUCCESS(aws_http_message_add_header(message, range_header));
    get_options.get_options.object_path = object_path;
    get_options.message = message;
    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &get_options, &out_results));
    ASSERT_UINT_EQUALS(out_results.finished_error_code, AWS_ERROR_S3_MISSING_CONTENT_RANGE_HEADER);
    aws_uri_clean_up(&mock_server);
    aws_http_message_destroy(message);

    /* Clean up */
    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

TEST_CASE(get_object_missmatch_checksum_responses_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_tester_client_options client_options = {
        .part_size = 64 * 1024,
        .tls_usage = AWS_S3_TLS_DISABLED,
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    /* Mock server will response without fake checksum for the body */
    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/get_object_unmatch_checksum_crc32");

    struct aws_s3_tester_meta_request_options get_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .client = client,
        .expected_validate_checksum_alg = AWS_SCA_CRC32,
        .validate_get_response_checksum = true,
        .get_options =
            {
                .object_path = object_path,
            },
        .default_type_options =
            {
                .mode = AWS_S3_TESTER_DEFAULT_TYPE_MODE_GET,
            },
        .mock_server = true,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE,
    };
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &get_options, &out_results));

    ASSERT_UINT_EQUALS(out_results.finished_error_code, AWS_ERROR_S3_RESPONSE_CHECKSUM_MISMATCH);
    ASSERT_UINT_EQUALS(out_results.algorithm, AWS_SCA_CRC32);

    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

/* Test that the HTTP throughput monitoring's default settings can detect dead (or absurdly slow) connections.
 * We trigger this by having the mock server delay 60 seconds before sending the response. */
TEST_CASE(get_object_throughput_failure_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_tester_client_options client_options = {
        .part_size = 64 * 1024,
        .tls_usage = AWS_S3_TLS_DISABLED,
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/get_object_delay_60s");

    struct aws_s3_tester_meta_request_options get_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_GET_OBJECT,
        .client = client,
        .get_options =
            {
                .object_path = object_path,
            },
        .default_type_options =
            {
                .mode = AWS_S3_TESTER_DEFAULT_TYPE_MODE_GET,
            },
        .mock_server = true,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE,
    };
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &get_options, &out_results));

    ASSERT_UINT_EQUALS(out_results.finished_error_code, AWS_ERROR_HTTP_CHANNEL_THROUGHPUT_FAILURE);

    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

TEST_CASE(upload_part_invalid_response_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_tester_client_options client_options = {
        .part_size = MB_TO_BYTES(5),
        .tls_usage = AWS_S3_TLS_DISABLED,
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/missing_etag");

    struct aws_s3_tester_meta_request_options put_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .client = client,
        .checksum_algorithm = AWS_SCA_CRC32,
        .validate_get_response_checksum = false,
        .put_options =
            {
                .object_size_mb = 10,
                .object_path_override = object_path,
            },
        .mock_server = true,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE,
    };
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &put_options, &out_results));
    ASSERT_UINT_EQUALS(out_results.finished_error_code, AWS_ERROR_S3_MISSING_ETAG);

    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

static void s_resume_meta_request_progress(
    struct aws_s3_meta_request *meta_request,
    const struct aws_s3_meta_request_progress *progress,
    void *user_data) {

    (void)meta_request;
    AWS_ASSERT(meta_request);
    AWS_ASSERT(progress);
    AWS_ASSERT(user_data);

    struct aws_s3_meta_request_test_results *out_results = user_data;

    aws_atomic_fetch_add(&out_results->total_bytes_uploaded, (size_t)progress->bytes_transferred);
}

/* Fake a MPU with 4 parts and the 2nd and 3rd have already completed and resume works fine */
TEST_CASE(resume_first_part_not_completed_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    size_t num_parts = 4;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_tester_client_options client_options = {
        .part_size = MB_TO_BYTES(8),
        .tls_usage = AWS_S3_TLS_DISABLED,
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    /* ListParts from mock server will return Etags for the 2nd and 3rd parts */
    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/resume_first_part_not_completed");
    struct aws_s3_upload_resume_token_options token_options = {
        .upload_id = aws_byte_cursor_from_c_str("upload_id"),
        .part_size = client_options.part_size,
        .total_num_parts = num_parts,
    };
    struct aws_s3_meta_request_resume_token *token =
        aws_s3_meta_request_resume_token_new_upload(allocator, &token_options);

    struct aws_s3_tester_meta_request_options put_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .client = client,
        .checksum_algorithm = AWS_SCA_CRC32,
        .validate_get_response_checksum = false,
        .put_options =
            {
                .object_size_mb = (uint32_t)num_parts * 8, /* Make sure we have exactly 4 parts */
                .object_path_override = object_path,
                .resume_token = token,
            },
        .mock_server = true,
    };
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);
    out_results.progress_callback = s_resume_meta_request_progress;

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &put_options, &out_results));
    /* Make Sure we only uploaded 2 parts. */
    size_t total_bytes_uploaded = aws_atomic_load_int(&out_results.total_bytes_uploaded);
    ASSERT_UINT_EQUALS(2 * MB_TO_BYTES(8), total_bytes_uploaded);

    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_meta_request_resume_token_release(token);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

/* Fake a MPU with 4 parts and the 2nd and 3rd have already completed and resume works fine with two response of
 * ListParts
 */
TEST_CASE(resume_mutli_page_list_parts_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    size_t num_parts = 4;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_tester_client_options client_options = {
        .part_size = MB_TO_BYTES(8),
        .tls_usage = AWS_S3_TLS_DISABLED,
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    /* ListParts from mock server will return NextPartNumberMarker */
    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/multiple_list_parts");
    struct aws_s3_upload_resume_token_options token_options = {
        .upload_id = aws_byte_cursor_from_c_str("upload_id"),
        .part_size = client_options.part_size,
        .total_num_parts = num_parts,
    };
    struct aws_s3_meta_request_resume_token *token =
        aws_s3_meta_request_resume_token_new_upload(allocator, &token_options);

    struct aws_s3_tester_meta_request_options put_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .client = client,
        .checksum_algorithm = AWS_SCA_CRC32,
        .validate_get_response_checksum = false,
        .put_options =
            {
                .object_size_mb = (uint32_t)num_parts * 8, /* Make sure we have exactly 4 parts */
                .object_path_override = object_path,
                .resume_token = token,
            },
        .mock_server = true,
    };
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);
    out_results.progress_callback = s_resume_meta_request_progress;

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &put_options, &out_results));
    /* Make Sure we only uploaded 2 parts. */
    size_t total_bytes_uploaded = aws_atomic_load_int(&out_results.total_bytes_uploaded);
    ASSERT_UINT_EQUALS(2 * MB_TO_BYTES(8), total_bytes_uploaded);

    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_meta_request_resume_token_release(token);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

TEST_CASE(resume_list_parts_failed_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    size_t num_parts = 4;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_tester_client_options client_options = {
        .part_size = MB_TO_BYTES(8),
        .tls_usage = AWS_S3_TLS_DISABLED,
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/non-exist");
    struct aws_s3_upload_resume_token_options token_options = {
        .upload_id = aws_byte_cursor_from_c_str("upload_id"),
        .part_size = client_options.part_size,
        .total_num_parts = num_parts,
    };
    struct aws_s3_meta_request_resume_token *token =
        aws_s3_meta_request_resume_token_new_upload(allocator, &token_options);

    struct aws_s3_tester_meta_request_options put_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .client = client,
        .checksum_algorithm = AWS_SCA_CRC32,
        .validate_get_response_checksum = false,
        .put_options =
            {
                .object_size_mb = (uint32_t)num_parts * 8, /* Make sure we have exactly 4 parts */
                .object_path_override = object_path,
                .resume_token = token,
            },
        .mock_server = true,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE,
    };
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &put_options, &out_results));
    ASSERT_UINT_EQUALS(out_results.finished_error_code, AWS_ERROR_S3_INVALID_RESPONSE_STATUS);
    ASSERT_UINT_EQUALS(out_results.finished_response_status, AWS_HTTP_STATUS_CODE_404_NOT_FOUND);

    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_meta_request_resume_token_release(token);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

TEST_CASE(resume_after_finished_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    size_t num_parts = 4;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_tester_client_options client_options = {
        .part_size = MB_TO_BYTES(8),
        .tls_usage = AWS_S3_TLS_DISABLED,
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/non-exist");
    struct aws_s3_upload_resume_token_options token_options = {
        .upload_id = aws_byte_cursor_from_c_str("upload_id"),
        .part_size = client_options.part_size,
        .total_num_parts = num_parts,
        .num_parts_completed = num_parts,
    };
    struct aws_s3_meta_request_resume_token *token =
        aws_s3_meta_request_resume_token_new_upload(allocator, &token_options);

    struct aws_s3_tester_meta_request_options put_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .client = client,
        .checksum_algorithm = AWS_SCA_CRC32,
        .validate_get_response_checksum = false,
        .put_options =
            {
                .object_size_mb = (uint32_t)num_parts * 8, /* Make sure we have exactly 4 parts */
                .object_path_override = object_path,
                .resume_token = token,
            },
        .mock_server = true,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_NO_VALIDATE,
    };
    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);
    out_results.progress_callback = s_resume_meta_request_progress;

    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &put_options, &out_results));
    /* The error code should be success, but there are no headers and stuff as no request was made. */
    ASSERT_UINT_EQUALS(out_results.finished_error_code, AWS_ERROR_SUCCESS);
    size_t total_bytes_uploaded = aws_atomic_load_int(&out_results.total_bytes_uploaded);
    ASSERT_UINT_EQUALS(0, total_bytes_uploaded);

    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_meta_request_resume_token_release(token);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

TEST_CASE(multipart_upload_proxy_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_tester_client_options client_options = {
        .part_size = MB_TO_BYTES(5),
        .tls_usage = AWS_S3_TLS_DISABLED,
        .use_proxy = true,
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/default");

    struct aws_s3_tester_meta_request_options put_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .client = client,
        .checksum_algorithm = AWS_SCA_CRC32,
        .validate_get_response_checksum = false,
        .put_options =
            {
                .object_size_mb = 10,
                .object_path_override = object_path,
            },
        .mock_server = true,
        .validate_type = AWS_S3_TESTER_VALIDATE_TYPE_NO_VALIDATE,
    };

    /* The request can fail if proxy is unavailable. */
    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &put_options, NULL));

    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}

TEST_CASE(endpoint_override_mock_server) {
    (void)ctx;

    struct aws_s3_tester tester;
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));
    struct aws_s3_tester_client_options client_options = {
        .part_size = MB_TO_BYTES(5),
        .tls_usage = AWS_S3_TLS_DISABLED,
    };

    struct aws_s3_client *client = NULL;
    ASSERT_SUCCESS(aws_s3_tester_client_new(&tester, &client_options, &client));

    /* 1 - Mock server will response without Content-Range */
    struct aws_byte_cursor object_path = aws_byte_cursor_from_c_str("/default");

    struct aws_s3_tester_meta_request_options put_options = {
        .allocator = allocator,
        .meta_request_type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,
        .client = client,
        .put_options =
            {
                .object_size_mb = 5, /* Make sure we have exactly 4 parts */
                .object_path_override = object_path,
            },
        .mock_server = true,
    };

    struct aws_s3_meta_request_test_results out_results;
    aws_s3_meta_request_test_results_init(&out_results, allocator);

    /* Put together a simple S3 Put Object request. */
    struct aws_input_stream *input_stream =
        aws_s3_test_input_stream_new(allocator, put_options.put_options.object_size_mb);
    struct aws_http_message *message =
        aws_s3_test_put_object_request_new(allocator, NULL, object_path, g_test_body_content_type, input_stream, 0);
    ASSERT_NOT_NULL(message);

    /* 1. Create request without host and use endpoint override for the host info */
    put_options.message = message;
    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &put_options, &out_results));

    /* 2. Create request with host info missmatch endpoint override */
    struct aws_http_header host_header = {
        .name = g_host_header_name,
        .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("bad_host"),
    };
    ASSERT_SUCCESS(aws_http_message_add_header(message, host_header));
    put_options.message = message;
    put_options.validate_type = AWS_S3_TESTER_VALIDATE_TYPE_EXPECT_FAILURE;
    ASSERT_SUCCESS(aws_s3_tester_send_meta_request_with_options(&tester, &put_options, &out_results));

    /* Clean up */
    aws_http_message_destroy(message);
    aws_input_stream_release(input_stream);
    aws_s3_meta_request_test_results_clean_up(&out_results);
    aws_s3_client_release(client);
    aws_s3_tester_clean_up(&tester);

    return AWS_OP_SUCCESS;
}
