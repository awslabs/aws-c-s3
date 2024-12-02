/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_client_impl.h"
#include "aws/s3/private/s3_meta_request_impl.h"
#include "aws/s3/private/s3_request_messages.h"
#include "aws/s3/private/s3_util.h"
#include "s3_tester.h"
#include <aws/common/atomics.h>
#include <aws/common/byte_buf.h>
#include <aws/common/clock.h>
#include <aws/common/common.h>
#include <aws/common/ref_count.h>
#include <aws/common/xml_parser.h>
#include <aws/http/request_response.h>
#include <aws/io/stream.h>
#include <aws/io/tls_channel_handler.h>
#include <aws/testing/aws_test_harness.h>
#include <inttypes.h>

static const struct aws_http_header get_object_test_headers[] = {
    {
        .name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Host"),
        .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("HostValue"),
    },
    {
        .name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("If-Match"),
        .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("If-MatchValue"),
    },
    {
        .name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("If-Modified-Since"),
        .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("If-Modified-SinceValue"),
    },
    {
        .name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("If-None-Match"),
        .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("If-None-MatchValue"),
    },
    {
        .name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Range"),
        .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("RangeValue"),
    },
    {
        .name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-server-side-encryption-customer-algorithm"),
        .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-server-side-encryption-customer-algorithmValue"),
    },
    {
        .name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-server-side-encryption-customer-key"),
        .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-server-side-encryption-customer-keyValue"),
    },
    {
        .name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-server-side-encryption-customer-key-MD5"),
        .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-server-side-encryption-customer-key-MD5Value"),
    },
    {
        .name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-request-payer"),
        .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-request-payerValue"),
    },
    {
        .name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-expected-bucket-owner"),
        .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-expected-bucket-ownerValue"),
    },
};

static const struct aws_http_header s_put_object_test_headers[] = {
    {
        .name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-acl"),
        .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("ACLValue"),
    },
    {
        .name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Cache-Control"),
        .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("CacheControlValue"),
    },
    {
        .name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Content-Disposition"),
        .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("ContentDispositionValue"),
    },
    {
        .name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Content-Encoding"),
        .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("ContentEncodingValue"),
    },
    {
        .name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Content-Language"),
        .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("ContentLanguageValue"),
    },
    {
        .name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Content-Length"),
        .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("ContentLengthValue"),
    },
    {
        .name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Content-MD5"),
        .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("ContentMD5Value"),
    },
    {
        .name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Content-Type"),
        .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("ContentTypeValue"),
    },
    {
        .name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Expires"),
        .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("ExpiresValue"),
    },
    {
        .name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-grant-full-control"),
        .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("GrantFullControlValue"),
    },
    {
        .name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-grant-read"),
        .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("GrantReadValue"),
    },
    {
        .name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-grant-read-acp"),
        .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("GrantReadACPValue"),
    },
    {
        .name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-grant-write-acp"),
        .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("GrantWriteACPValue"),
    },
    {
        .name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-server-side-encryption"),
        .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("ServerSideEncryptionValue"),
    },
    {
        .name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-storage-class"),
        .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("StorageClassValue"),
    },
    {
        .name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-website-redirect-location"),
        .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("WebsiteRedirectLocationValue"),
    },
    {
        .name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-server-side-encryption-customer-algorithm"),
        .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("SSECustomerAlgorithmValue"),
    },
    {
        .name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-server-side-encryption-customer-key"),
        .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("SSECustomerKeyValue"),
    },
    {
        .name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-server-side-encryption-customer-key-MD5"),
        .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("SSECustomerKeyMD5Value"),
    },
    {
        .name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-server-side-encryption-aws-kms-key-id"),
        .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("SSEKMSKeyIdValue"),
    },
    {
        .name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-server-side-encryption-context"),
        .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("SSEKMSEncryptionContextValue"),
    },
    {
        .name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-server-side-encryption-bucket-key-enabled"),
        .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("BucketKeyEnabledValue"),
    },
    {
        .name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-request-payer"),
        .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("RequestPayerValue"),
    },
    {
        .name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-tagging"),
        .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("TaggingValue"),
    },
    {
        .name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-object-lock-mode"),
        .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("ObjectLockModeValue"),
    },
    {
        .name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-object-lock-retain-until-date"),
        .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("ObjectLockRetainUntilDateValue"),
    },
    {
        .name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-object-lock-legal-hold"),
        .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("ObjectLockLegalHoldStatusValue"),
    },
    {
        .name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-expected-bucket-owner"),
        .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("ExpectedBucketOwnerValue"),
    },
};

static int s_fill_byte_buf(struct aws_byte_buf *buffer, struct aws_allocator *allocator, size_t buffer_size) {
    ASSERT_TRUE(buffer != NULL);
    ASSERT_TRUE(allocator != NULL);
    ASSERT_TRUE(buffer_size > 0);

    ASSERT_SUCCESS(aws_byte_buf_init(buffer, allocator, buffer_size));

    srand(0);

    for (size_t i = 0; i < buffer_size; ++i) {
        const char single_char = (char)(rand() % (int)('z' - 'a') + (int)'a');

        struct aws_byte_cursor single_char_cursor = {
            .ptr = (uint8_t *)&single_char,
            .len = 1,
        };

        ASSERT_SUCCESS(aws_byte_buf_append(buffer, &single_char_cursor));
    }

    return AWS_OP_SUCCESS;
}

static int s_test_http_headers_match(
    struct aws_allocator *allocator,
    const struct aws_http_message *message0,
    const struct aws_http_message *message1,

    /* Headers that we know are in message0, but should NOT be in message1 */
    const struct aws_byte_cursor *excluded_message0_headers,
    size_t excluded_message0_headers_count,

    /* Headers in message1 that are okay to be in message1 even if they are in the excluded list or are not in
       message0.*/
    const struct aws_byte_cursor *message1_header_exceptions,
    size_t message1_header_exceptions_count) {
    ASSERT_TRUE(message0 != NULL);
    ASSERT_TRUE(message1 != NULL);
    ASSERT_TRUE(excluded_message0_headers != NULL || excluded_message0_headers_count == 0);
    ASSERT_TRUE(message1_header_exceptions != NULL || message1_header_exceptions_count == 0);

    const struct aws_http_headers *message0_headers = aws_http_message_get_const_headers(message0);
    ASSERT_TRUE(message0_headers != NULL);

    const struct aws_http_headers *message1_headers = aws_http_message_get_const_headers(message1);
    ASSERT_TRUE(message1_headers != NULL);

    struct aws_http_headers *expected_message0_headers = aws_http_headers_new(allocator);

    /* Copy message1 headers to expected_message0_headers. With upcoming adds/removes, it should transform back into
     * message0.
     */
    for (size_t i = 0; i < aws_http_headers_count(message1_headers); ++i) {
        struct aws_http_header message1_header;
        AWS_ZERO_STRUCT(message1_header);
        ASSERT_SUCCESS(aws_http_headers_get_index(message1_headers, i, &message1_header));
        ASSERT_SUCCESS(aws_http_headers_add(expected_message0_headers, message1_header.name, message1_header.value));
    }

    /* Go through all of the headers that were originally removed from message1 after it was copied from message0. */
    for (size_t i = 0; i < excluded_message0_headers_count; ++i) {
        const struct aws_byte_cursor *excluded_header_name = &excluded_message0_headers[i];

        bool header_existence_is_valid = false;

        /* If the header is in the exception list, it's okay for message1 to have. (It may have been re-added.) */
        for (size_t j = 0; j < message1_header_exceptions_count; ++j) {
            if (aws_byte_cursor_eq(excluded_header_name, &message1_header_exceptions[j])) {
                header_existence_is_valid = true;
                break;
            }
        }

        /* Try to get the header from message1. */
        struct aws_byte_cursor message1_header_value;
        AWS_ZERO_STRUCT(message1_header_value);
        int result = aws_http_headers_get(message1_headers, *excluded_header_name, &message1_header_value);

        if (header_existence_is_valid) {

            /* If this header is allowed to exist in message1, then we don't need to assert on its existence or
             * non-existence.  But we do want to erase it from the expected_message0_headers, since its value may be
             * different from that in message0. */
            if (result == AWS_OP_SUCCESS) {
                ASSERT_SUCCESS(aws_http_headers_erase(expected_message0_headers, *excluded_header_name));
            }

        } else {
            /* In this case, message1 should not have the header. */
            ASSERT_TRUE(result == AWS_OP_ERR && aws_last_error() == AWS_ERROR_HTTP_HEADER_NOT_FOUND);
        }

        /* At this point, expected_message0_headers should not have the excluded header in it. Add a copy of the header
         * from message0 to expected_message0_headers to further transform it toward being a copy of message0 headers.
         */
        struct aws_byte_cursor message0_header_value;
        AWS_ZERO_STRUCT(message0_header_value);
        if (aws_http_headers_get(message0_headers, *excluded_header_name, &message0_header_value) == AWS_OP_SUCCESS) {
            ASSERT_SUCCESS(
                aws_http_headers_add(expected_message0_headers, *excluded_header_name, message0_header_value));
        }
    }

    /* message0_headers should now match expected_message0_headers */
    {
        ASSERT_TRUE(aws_http_headers_count(message0_headers) == aws_http_headers_count(expected_message0_headers));

        for (size_t i = 0; i < aws_http_headers_count(message0_headers); ++i) {
            struct aws_http_header message0_header;
            AWS_ZERO_STRUCT(message0_header);
            ASSERT_SUCCESS(aws_http_headers_get_index(message0_headers, i, &message0_header));

            struct aws_byte_cursor expected_message0_header_value;
            AWS_ZERO_STRUCT(expected_message0_header_value);
            ASSERT_SUCCESS(
                aws_http_headers_get(expected_message0_headers, message0_header.name, &expected_message0_header_value));

            ASSERT_TRUE(aws_byte_cursor_eq(&message0_header.value, &expected_message0_header_value));
        }
    }

    aws_http_headers_release(expected_message0_headers);

    return AWS_OP_SUCCESS;
}

static int s_test_http_messages_match(
    struct aws_allocator *allocator,
    const struct aws_http_message *message0,
    const struct aws_http_message *message1,
    const struct aws_byte_cursor *excluded_headers,
    size_t excluded_headers_count) {
    ASSERT_TRUE(message0 != NULL);
    ASSERT_TRUE(message1 != NULL);
    ASSERT_TRUE(excluded_headers != NULL || excluded_headers_count == 0);

    struct aws_byte_cursor request_path;
    AWS_ZERO_STRUCT(request_path);
    ASSERT_SUCCESS(aws_http_message_get_request_path(message0, &request_path));

    struct aws_byte_cursor copied_request_path;
    AWS_ZERO_STRUCT(copied_request_path);
    ASSERT_SUCCESS(aws_http_message_get_request_path(message1, &copied_request_path));

    ASSERT_TRUE(aws_byte_cursor_eq(&request_path, &copied_request_path));

    struct aws_byte_cursor request_method;
    AWS_ZERO_STRUCT(request_method);
    ASSERT_SUCCESS(aws_http_message_get_request_method(message0, &request_method));

    struct aws_byte_cursor copied_request_method;
    AWS_ZERO_STRUCT(copied_request_method);
    ASSERT_SUCCESS(aws_http_message_get_request_method(message1, &copied_request_method));

    ASSERT_TRUE(aws_byte_cursor_eq(&request_method, &copied_request_method));

    ASSERT_SUCCESS(
        s_test_http_headers_match(allocator, message0, message1, excluded_headers, excluded_headers_count, NULL, 0));

    return AWS_OP_SUCCESS;
}

static struct aws_http_header s_http_header_from_c_str(const char *name, const char *value) {
    struct aws_http_header header = {
        .name = aws_byte_cursor_from_c_str(name),
        .value = aws_byte_cursor_from_c_str(value),
    };
    return header;
}

static int s_test_http_message_request_path(
    struct aws_http_message *message,
    const struct aws_byte_cursor *request_path) {

    struct aws_byte_cursor message_request_path;
    AWS_ZERO_STRUCT(message_request_path);
    ASSERT_SUCCESS(aws_http_message_get_request_path(message, &message_request_path));

    ASSERT_TRUE(aws_byte_cursor_eq(&message_request_path, request_path));

    return AWS_OP_SUCCESS;
}

static int s_test_http_message_request_method(struct aws_http_message *message, const char *method) {

    struct aws_byte_cursor message_request_method;
    AWS_ZERO_STRUCT(message_request_method);
    ASSERT_SUCCESS(aws_http_message_get_request_method(message, &message_request_method));

    struct aws_byte_cursor method_cursor = aws_byte_cursor_from_c_str(method);

    ASSERT_TRUE(aws_byte_cursor_eq(&message_request_method, &method_cursor));

    return AWS_OP_SUCCESS;
}

static int s_test_http_message_body_stream(
    struct aws_allocator *allocator,
    struct aws_http_message *derived_message,
    struct aws_byte_buf *expected_stream_contents) {
    ASSERT_TRUE(derived_message != NULL);
    ASSERT_TRUE(expected_stream_contents != NULL);

    struct aws_http_headers *headers = aws_http_message_get_headers(derived_message);
    ASSERT_TRUE(headers != NULL);

    struct aws_input_stream *body_stream = aws_http_message_get_body_stream(derived_message);
    ASSERT_TRUE(body_stream != NULL);

    /* Check for the content length header. */
    uint64_t content_length = 0;
    ASSERT_SUCCESS(aws_s3_tester_get_content_length(headers, &content_length));
    ASSERT_TRUE(content_length == expected_stream_contents->len);

    /* Check that the stream data is equal to the original buffer data. */
    struct aws_byte_buf stream_read_buffer;
    ASSERT_SUCCESS(aws_byte_buf_init(&stream_read_buffer, allocator, expected_stream_contents->len));
    ASSERT_SUCCESS(aws_input_stream_read(body_stream, &stream_read_buffer));
    ASSERT_TRUE(aws_byte_buf_eq(expected_stream_contents, &stream_read_buffer));
    aws_byte_buf_clean_up(&stream_read_buffer);

    /* There should be no data left in the stream. */
    struct aws_byte_buf stream_overread_buffer;
    ASSERT_SUCCESS(aws_byte_buf_init(&stream_overread_buffer, allocator, expected_stream_contents->len));
    ASSERT_SUCCESS(aws_input_stream_read(body_stream, &stream_overread_buffer));
    ASSERT_TRUE(stream_overread_buffer.len == 0);
    aws_byte_buf_clean_up(&stream_overread_buffer);

    return AWS_OP_SUCCESS;
}

int s_create_get_object_message(
    struct aws_allocator *allocator,
    const struct aws_byte_cursor *path,
    struct aws_http_message **out_message) {
    ASSERT_TRUE(out_message != NULL);
    ASSERT_TRUE(*out_message == NULL);

    struct aws_http_message *message = aws_http_message_new_request(allocator);
    ASSERT_TRUE(message != NULL);

    ASSERT_SUCCESS(aws_http_message_set_request_path(message, *path));
    ASSERT_SUCCESS(aws_http_message_set_request_method(message, aws_byte_cursor_from_c_str("GET")));

    for (size_t i = 0; i < AWS_ARRAY_SIZE(get_object_test_headers); ++i) {
        ASSERT_SUCCESS(aws_http_message_add_header(message, get_object_test_headers[i]));
    }

    *out_message = message;

    return AWS_OP_SUCCESS;
}

int s_create_put_object_message(
    struct aws_allocator *allocator,
    const struct aws_byte_cursor *path,
    struct aws_http_message **out_message) {
    ASSERT_TRUE(out_message != NULL);
    ASSERT_TRUE(*out_message == NULL);

    struct aws_http_message *message = aws_http_message_new_request(allocator);
    ASSERT_TRUE(message != NULL);

    ASSERT_SUCCESS(aws_http_message_set_request_path(message, *path));
    ASSERT_SUCCESS(aws_http_message_set_request_method(message, aws_byte_cursor_from_c_str("PUT")));

    for (size_t i = 0; i < AWS_ARRAY_SIZE(s_put_object_test_headers); ++i) {
        ASSERT_SUCCESS(aws_http_message_add_header(message, s_put_object_test_headers[i]));
    }

    *out_message = message;

    return AWS_OP_SUCCESS;
    ;
}

AWS_TEST_CASE(test_s3_copy_http_message, s_test_s3_copy_http_message)
static int s_test_s3_copy_http_message(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    const struct aws_byte_cursor request_method = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("RequestMethod");
    const struct aws_byte_cursor request_path = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("RequestPath");

    const struct aws_http_header original_headers[] = {
        s_http_header_from_c_str("IncludedHeader", "IncludedHeaderValue"),
        s_http_header_from_c_str("ExcludedHeader", "ExcludedHeaderValue"),
        s_http_header_from_c_str("x-amz-meta-MyMetadata", "MyMetadataValue"),
    };

    const struct aws_byte_cursor excluded_headers[] = {
        aws_byte_cursor_from_c_str("ExcludedHeader"),
    };

    struct aws_http_message *message = aws_http_message_new_request(allocator);
    ASSERT_TRUE(message != NULL);
    ASSERT_SUCCESS(aws_http_message_set_request_method(message, request_method));
    ASSERT_SUCCESS(aws_http_message_set_request_path(message, request_path));
    ASSERT_SUCCESS(aws_http_message_add_header_array(message, original_headers, AWS_ARRAY_SIZE(original_headers)));

    { /* copy message, include "x-amz-meta-" */
        struct aws_http_message *copied_message = aws_s3_message_util_copy_http_message_no_body_filter_headers(
            allocator, message, excluded_headers, AWS_ARRAY_SIZE(excluded_headers), false /*exclude_x_amz_meta*/);
        ASSERT_TRUE(copied_message != NULL);

        ASSERT_SUCCESS(s_test_http_messages_match(
            allocator, message, copied_message, excluded_headers, AWS_ARRAY_SIZE(excluded_headers)));

        aws_http_message_release(copied_message);
    }

    { /* copy message, exclude "x-amz-meta-" */
        struct aws_http_message *copied_message = aws_s3_message_util_copy_http_message_no_body_filter_headers(
            allocator, message, excluded_headers, AWS_ARRAY_SIZE(excluded_headers), true /*exclude_x_amz_meta*/);
        ASSERT_TRUE(copied_message != NULL);

        const struct aws_byte_cursor expected_excluded_headers[] = {
            aws_byte_cursor_from_c_str("ExcludedHeader"),
            aws_byte_cursor_from_c_str("x-amz-meta-MyMetadata"),
        };

        ASSERT_SUCCESS(s_test_http_messages_match(
            allocator, message, copied_message, expected_excluded_headers, AWS_ARRAY_SIZE(expected_excluded_headers)));

        aws_http_message_release(copied_message);
    }

    aws_http_message_release(message);

    return 0;
}

AWS_TEST_CASE(test_s3_message_util_assign_body, s_test_s3_message_util_assign_body)
static int s_test_s3_message_util_assign_body(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_http_message *message = aws_http_message_new_request(allocator);
    aws_http_message_set_request_method(message, aws_http_method_get);

    const size_t test_buffer_size = 42;
    struct aws_byte_buf test_buffer;
    ASSERT_SUCCESS(s_fill_byte_buf(&test_buffer, allocator, test_buffer_size));

    struct aws_input_stream *input_stream =
        aws_s3_message_util_assign_body(allocator, &test_buffer, message, NULL, NULL);
    ASSERT_TRUE(input_stream != NULL);

    ASSERT_TRUE(aws_http_message_get_body_stream(message) == input_stream);
    ASSERT_SUCCESS(s_test_http_message_body_stream(allocator, message, &test_buffer));

    aws_byte_buf_clean_up(&test_buffer);
    aws_http_message_release(message);

    return 0;
}

AWS_TEST_CASE(test_s3_ranged_get_object_message_new, s_test_s3_ranged_get_object_message_new)
static int s_test_s3_ranged_get_object_message_new(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    const struct aws_byte_cursor test_path = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("/TestPath");

    struct aws_http_message *original_message = NULL;
    ASSERT_SUCCESS(s_create_get_object_message(allocator, &test_path, &original_message));
    ASSERT_TRUE(original_message != NULL);

    {
        char expected_range_value_buffer[128] = "bytes=42-83";
        struct aws_byte_cursor expected_range_value_cursor = aws_byte_cursor_from_c_str(expected_range_value_buffer);

        struct aws_http_message *get_object_message =
            aws_s3_ranged_get_object_message_new(allocator, original_message, 42, 83);
        ASSERT_TRUE(get_object_message != NULL);

        struct aws_http_headers *headers = aws_http_message_get_headers(get_object_message);
        ASSERT_TRUE(headers != NULL);

        struct aws_byte_cursor range_header_value;
        AWS_ZERO_STRUCT(range_header_value);
        ASSERT_SUCCESS(aws_http_headers_get(headers, g_range_header_name, &range_header_value));

        ASSERT_TRUE(aws_byte_cursor_eq(&range_header_value, &expected_range_value_cursor));

        s_test_http_message_request_method(get_object_message, "GET");

        aws_http_message_release(get_object_message);
    }

    aws_http_message_release(original_message);

    return 0;
}

AWS_TEST_CASE(test_s3_set_multipart_request_path, s_test_s3_set_multipart_request_path)
static int s_test_s3_set_multipart_request_path(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

#define TEST_PATH "/TestPath"
#define TEST_PATH_WITH_PARAMS "/TestPath?arg=value"
#define UPLOAD_ID "test_upload_id"
#define UPLOAD_ID_PARAM "uploadId=test_upload_id"
#define PART_NUMBER 4
#define UPLOADS_PARAM "uploads"

    const struct aws_byte_cursor test_path = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL(TEST_PATH);
    const struct aws_byte_cursor test_path_with_params = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL(TEST_PATH_WITH_PARAMS);

    struct aws_byte_cursor test_path_permutations[] = {
        AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("/TestPath"),
        AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("/TestPath?uploads"),
        AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("/TestPath?partNumber=4"),
        AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("/TestPath?partNumber=4&uploads"),
        AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("/TestPath?uploadId=test_upload_id"),
        AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("/TestPath?uploadId=test_upload_id&uploads"),
        AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("/TestPath?partNumber=4&uploadId=test_upload_id"),
        AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("/TestPath?partNumber=4&uploadId=test_upload_id&uploads"),
    };

    struct aws_byte_cursor test_path_with_params_permutations[] = {
        AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("/TestPath?arg=value"),
        AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("/TestPath?arg=value&uploads"),
        AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("/TestPath?arg=value&partNumber=4"),
        AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("/TestPath?arg=value&partNumber=4&uploads"),
        AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("/TestPath?arg=value&uploadId=test_upload_id"),
        AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("/TestPath?arg=value&uploadId=test_upload_id&uploads"),
        AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("/TestPath?arg=value&partNumber=4&uploadId=test_upload_id"),
        AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("/TestPath?arg=value&partNumber=4&uploadId=test_upload_id&uploads"),
    };

    const uint32_t num_permutations = 8;

    for (uint32_t i = 0; i < num_permutations; ++i) {
        struct aws_string *upload_id = NULL;
        uint32_t part_number = 0;
        bool uploads_param = false;

        if (i & 0x4) {
            upload_id = aws_string_new_from_c_str(allocator, UPLOAD_ID);
        }

        if (i & 0x2) {
            part_number = PART_NUMBER;
        }

        if (i & 0x1) {
            uploads_param = true;
        }

        {
            struct aws_http_message *message = NULL;
            ASSERT_SUCCESS(s_create_put_object_message(allocator, &test_path, &message));

            ASSERT_SUCCESS(aws_s3_message_util_set_multipart_request_path(
                allocator, upload_id, part_number, uploads_param, message));

            ASSERT_SUCCESS(s_test_http_message_request_path(message, &test_path_permutations[i]));

            aws_http_message_release(message);
        }

        {
            struct aws_http_message *message_with_params = NULL;
            ASSERT_SUCCESS(s_create_put_object_message(allocator, &test_path_with_params, &message_with_params));

            ASSERT_SUCCESS(aws_s3_message_util_set_multipart_request_path(
                allocator, upload_id, part_number, uploads_param, message_with_params));

            ASSERT_SUCCESS(
                s_test_http_message_request_path(message_with_params, &test_path_with_params_permutations[i]));

            aws_http_message_release(message_with_params);
        }

        aws_string_destroy(upload_id);
    }

#undef TEST_PATH
#undef TEST_PATH_WITH_PARAMS
#undef UPLOAD_ID
#undef UPLOAD_ID_PARAM
#undef PART_NUMBER
#undef UPLOADS_PARAM

    return 0;
}

AWS_TEST_CASE(test_s3_create_multipart_upload_message_new, s_test_s3_create_multipart_upload_message_new)
static int s_test_s3_create_multipart_upload_message_new(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_byte_cursor path = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("/TestPath");
    struct aws_byte_cursor expected_create_path = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("/TestPath?uploads");

    struct aws_http_message *original_message = NULL;
    ASSERT_SUCCESS(s_create_put_object_message(allocator, &path, &original_message));
    ASSERT_TRUE(original_message != NULL);

    struct aws_http_message *create_multipart_upload_message =
        aws_s3_create_multipart_upload_message_new(allocator, original_message, NULL);
    ASSERT_TRUE(create_multipart_upload_message != NULL);

    ASSERT_SUCCESS(s_test_http_message_request_method(create_multipart_upload_message, "POST"));
    ASSERT_SUCCESS(s_test_http_message_request_path(create_multipart_upload_message, &expected_create_path));

    const struct aws_byte_cursor header_exclude_exceptions[] = {
        AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Content-Length"),
    };
    ASSERT_SUCCESS(s_test_http_headers_match(
        allocator,
        original_message,
        create_multipart_upload_message,
        g_s3_create_multipart_upload_excluded_headers,
        g_s3_create_multipart_upload_excluded_headers_count,
        header_exclude_exceptions,
        AWS_ARRAY_SIZE(header_exclude_exceptions)));

    aws_http_message_release(create_multipart_upload_message);
    aws_http_message_release(original_message);

    return 0;
}

AWS_TEST_CASE(test_s3_upload_part_message_new, s_test_s3_upload_part_message_new)
static int s_test_s3_upload_part_message_new(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

#define STRINGIFY_HELPER(x) #x
#define STRINGIFY(x) STRINGIFY_HELPER(x)
#define TEST_PATH "/TestPath"
#define UPLOAD_ID "test_upload_id"
#define PART_NUMBER 4
#define PART_NUMBER_STR "?partNumber=" STRINGIFY(PART_NUMBER)
#define EXPECTED_UPLOAD_PART_PATH TEST_PATH PART_NUMBER_STR "&uploadId=" UPLOAD_ID

    const struct aws_byte_cursor header_exclude_exceptions[] = {
        AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Content-Length"),
    };

    struct aws_byte_cursor path = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL(TEST_PATH);
    struct aws_byte_cursor expected_create_path = aws_byte_cursor_from_c_str(EXPECTED_UPLOAD_PART_PATH);

    struct aws_http_message *original_message = NULL;
    ASSERT_SUCCESS(s_create_put_object_message(allocator, &path, &original_message));
    ASSERT_TRUE(original_message != NULL);

    const size_t part_buffer_size = 42;
    struct aws_byte_buf part_buffer;
    AWS_ZERO_STRUCT(part_buffer);
    s_fill_byte_buf(&part_buffer, allocator, part_buffer_size);

    struct aws_string *upload_id = aws_string_new_from_c_str(allocator, UPLOAD_ID);

    struct aws_http_message *upload_part_message = aws_s3_upload_part_message_new(
        allocator, original_message, &part_buffer, PART_NUMBER, upload_id, false, NULL, NULL);
    ASSERT_TRUE(upload_part_message != NULL);

    ASSERT_SUCCESS(s_test_http_message_request_method(upload_part_message, "PUT"));
    ASSERT_SUCCESS(s_test_http_message_request_path(upload_part_message, &expected_create_path));
    ASSERT_SUCCESS(s_test_http_headers_match(
        allocator,
        original_message,
        upload_part_message,
        g_s3_upload_part_excluded_headers,
        g_s3_upload_part_excluded_headers_count,
        header_exclude_exceptions,
        AWS_ARRAY_SIZE(header_exclude_exceptions)));

    ASSERT_SUCCESS(s_test_http_message_body_stream(allocator, upload_part_message, &part_buffer));

    aws_string_destroy(upload_id);
    aws_byte_buf_clean_up(&part_buffer);

    aws_http_message_release(upload_part_message);
    aws_http_message_release(original_message);

#undef STRINGIFY_HELPER
#undef STRINGIFY
#undef TEST_PATH
#undef UPLOAD_ID
#undef PART_NUMBER
#undef PART_NUMBER_STR
#undef EXPECTED_UPLOAD_PART_PATH

    return 0;
}

AWS_TEST_CASE(test_s3_upload_part_message_fail, s_test_s3_upload_part_message_fail)
static int s_test_s3_upload_part_message_fail(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

#define UPLOAD_ID "test_upload_id"
#define PART_NUMBER 4

    struct aws_http_message *original_message = aws_http_message_new_request(allocator);
    ASSERT_NOT_NULL(original_message);

    const size_t part_buffer_size = 42;
    struct aws_byte_buf part_buffer;
    AWS_ZERO_STRUCT(part_buffer);
    s_fill_byte_buf(&part_buffer, allocator, part_buffer_size);

    struct aws_string *upload_id = aws_string_new_from_c_str(allocator, UPLOAD_ID);

    struct aws_http_message *upload_part_message = aws_s3_upload_part_message_new(
        allocator, original_message, &part_buffer, PART_NUMBER, upload_id, false, NULL, NULL);
    ASSERT_NULL(upload_part_message);

    aws_string_destroy(upload_id);
    aws_byte_buf_clean_up(&part_buffer);

    aws_http_message_release(upload_part_message);
    aws_http_message_release(original_message);

#undef UPLOAD_ID
#undef PART_NUMBER

    return 0;
}

struct complete_multipart_upload_xml_test_data {
    struct aws_byte_cursor etag_value;
    struct aws_byte_cursor part_number_value;
    bool found_etag;
    bool found_part_number;
};

static int s_complete_multipart_upload_traverse_xml_node(struct aws_xml_node *node, void *user_data) {

    const struct aws_byte_cursor complete_multipar_upload_tag_name =
        AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("CompleteMultipartUpload");
    const struct aws_byte_cursor part_tag_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Part");
    const struct aws_byte_cursor etag_tag_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("ETag");
    const struct aws_byte_cursor part_number_tag_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("PartNumber");

    struct complete_multipart_upload_xml_test_data *test_data = user_data;

    struct aws_byte_cursor node_name = aws_xml_node_get_name(node);
    if (aws_byte_cursor_eq(&node_name, &complete_multipar_upload_tag_name)) {
        if (aws_xml_node_traverse(node, s_complete_multipart_upload_traverse_xml_node, user_data)) {
            return AWS_OP_ERR;
        }
    } else if (aws_byte_cursor_eq(&node_name, &part_tag_name)) {
        if (aws_xml_node_traverse(node, s_complete_multipart_upload_traverse_xml_node, user_data)) {
            return AWS_OP_ERR;
        }
    } else if (aws_byte_cursor_eq(&node_name, &etag_tag_name)) {

        struct aws_byte_cursor node_body;
        AWS_ZERO_STRUCT(node_body);
        if (aws_xml_node_as_body(node, &node_body)) {
            return AWS_OP_ERR;
        }

        test_data->found_etag = aws_byte_cursor_eq(&node_body, &test_data->etag_value);
    } else if (aws_byte_cursor_eq(&node_name, &part_number_tag_name)) {

        struct aws_byte_cursor node_body;
        AWS_ZERO_STRUCT(node_body);
        if (aws_xml_node_as_body(node, &node_body)) {
            return AWS_OP_ERR;
        }

        test_data->found_part_number = aws_byte_cursor_eq(&node_body, &test_data->part_number_value);
    }

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(test_s3_complete_multipart_message_new, s_test_s3_complete_multipart_message_new)
static int s_test_s3_complete_multipart_message_new(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

#define TEST_PATH "/TestPath"
#define UPLOAD_ID "test_upload_id"
#define EXPECTED_UPLOAD_PART_PATH TEST_PATH "?uploadId=" UPLOAD_ID
#define ETAG_VALUE "etag_value"

    struct aws_array_list parts;
    ASSERT_SUCCESS(aws_array_list_init_dynamic(&parts, allocator, 1, sizeof(struct aws_s3_mpu_part_info *)));
    struct aws_s3_mpu_part_info *part = aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_mpu_part_info));
    part->etag = aws_string_new_from_c_str(allocator, ETAG_VALUE);
    ASSERT_SUCCESS(aws_array_list_push_back(&parts, &part));

    const struct aws_byte_cursor header_exclude_exceptions[] = {
        AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Content-Length"),
    };

    struct aws_byte_cursor path = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL(TEST_PATH);
    struct aws_byte_cursor expected_create_path = aws_byte_cursor_from_c_str(EXPECTED_UPLOAD_PART_PATH);

    struct aws_http_message *original_message = NULL;
    ASSERT_SUCCESS(s_create_put_object_message(allocator, &path, &original_message));
    ASSERT_TRUE(original_message != NULL);

    struct aws_string *upload_id = aws_string_new_from_c_str(allocator, UPLOAD_ID);

    struct aws_byte_buf body_buffer;
    aws_byte_buf_init(&body_buffer, allocator, 64);

    struct aws_http_message *complete_multipart_message =
        aws_s3_complete_multipart_message_new(allocator, original_message, &body_buffer, upload_id, &parts, NULL);

    ASSERT_SUCCESS(s_test_http_message_request_method(complete_multipart_message, "POST"));
    ASSERT_SUCCESS(s_test_http_message_request_path(complete_multipart_message, &expected_create_path));
    ASSERT_SUCCESS(s_test_http_headers_match(
        allocator,
        original_message,
        complete_multipart_message,
        g_s3_complete_multipart_upload_excluded_headers,
        g_s3_complete_multipart_upload_excluded_headers_count,
        header_exclude_exceptions,
        AWS_ARRAY_SIZE(header_exclude_exceptions)));

    {
        struct complete_multipart_upload_xml_test_data xml_user_data = {
            .etag_value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL(ETAG_VALUE),
            .part_number_value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("1"),
            .found_etag = false,
            .found_part_number = false,
        };

        struct aws_xml_parser_options parser_options = {
            .doc = aws_byte_cursor_from_buf(&body_buffer),
            .on_root_encountered = s_complete_multipart_upload_traverse_xml_node,
            .user_data = &xml_user_data,
        };
        ASSERT_SUCCESS(aws_xml_parse(allocator, &parser_options));

        ASSERT_TRUE(xml_user_data.found_etag);
        ASSERT_TRUE(xml_user_data.found_part_number);
    }

    aws_byte_buf_clean_up(&body_buffer);
    aws_string_destroy(upload_id);

    aws_http_message_release(complete_multipart_message);
    aws_http_message_release(original_message);

    aws_string_destroy(part->etag);
    aws_mem_release(allocator, part);
    aws_array_list_clean_up(&parts);
#undef TEST_PATH
#undef UPLOAD_ID
#undef EXPECTED_UPLOAD_PART_PATH
#undef ETAG_VALUE

    return 0;
}

AWS_TEST_CASE(test_s3_abort_multipart_upload_message_new, s_test_s3_abort_multipart_upload_message_newt)
static int s_test_s3_abort_multipart_upload_message_newt(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

#define TEST_PATH "/TestPath"
#define UPLOAD_ID "test_upload_id"
#define EXPECTED_UPLOAD_PART_PATH TEST_PATH "?uploadId=" UPLOAD_ID

    struct aws_byte_cursor path = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL(TEST_PATH);
    struct aws_byte_cursor expected_create_path = aws_byte_cursor_from_c_str(EXPECTED_UPLOAD_PART_PATH);

    struct aws_http_message *original_message = NULL;
    ASSERT_SUCCESS(s_create_put_object_message(allocator, &path, &original_message));
    ASSERT_TRUE(original_message != NULL);

    struct aws_string *upload_id = aws_string_new_from_c_str(allocator, UPLOAD_ID);

    struct aws_http_message *abort_upload_message =
        aws_s3_abort_multipart_upload_message_new(allocator, original_message, upload_id);
    ASSERT_TRUE(abort_upload_message != NULL);

    ASSERT_SUCCESS(s_test_http_message_request_method(abort_upload_message, "DELETE"));
    ASSERT_SUCCESS(s_test_http_message_request_path(abort_upload_message, &expected_create_path));
    ASSERT_SUCCESS(s_test_http_headers_match(
        allocator,
        original_message,
        abort_upload_message,
        g_s3_abort_multipart_upload_excluded_headers,
        g_s3_abort_multipart_upload_excluded_headers_count,
        NULL,
        0));

    aws_string_destroy(upload_id);

    aws_http_message_release(abort_upload_message);
    aws_http_message_release(original_message);

#undef TEST_PATH
#undef UPLOAD_ID
#undef EXPECTED_UPLOAD_PART_PATH

    return 0;
}
