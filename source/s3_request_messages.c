/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_request_messages.h"
#include "aws/s3/private/s3_checksums.h"
#include "aws/s3/private/s3_client_impl.h"
#include "aws/s3/private/s3_meta_request_impl.h"
#include "aws/s3/private/s3_util.h"
#include <aws/cal/hash.h>
#include <aws/common/byte_buf.h>
#include <aws/common/encoding.h>
#include <aws/common/string.h>
#include <aws/http/request_response.h>
#include <aws/io/stream.h>
#include <aws/io/uri.h>
#include <aws/s3/s3.h>
#include <inttypes.h>

const struct aws_byte_cursor g_s3_create_multipart_upload_excluded_headers[] = {
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Content-Length"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Content-MD5"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-copy-source"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-copy-source-range"),
};

const size_t g_s3_create_multipart_upload_excluded_headers_count =
    AWS_ARRAY_SIZE(g_s3_create_multipart_upload_excluded_headers);

const struct aws_byte_cursor g_s3_upload_part_excluded_headers[] = {
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-acl"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Cache-Control"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Content-Disposition"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Content-Encoding"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Content-Language"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Content-Length"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Content-MD5"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Content-Type"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Expires"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-grant-full-control"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-grant-read"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-grant-read-acp"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-grant-write-acp"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-server-side-encryption"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-storage-class"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-website-redirect-location"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-server-side-encryption-aws-kms-key-id"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-server-side-encryption-context"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-server-side-encryption-bucket-key-enabled"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-tagging"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-object-lock-mode"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-object-lock-retain-until-date"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-object-lock-legal-hold"),
};

const size_t g_s3_upload_part_excluded_headers_count = AWS_ARRAY_SIZE(g_s3_upload_part_excluded_headers);

const struct aws_byte_cursor g_s3_complete_multipart_upload_excluded_headers[] = {
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-acl"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Cache-Control"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Content-Disposition"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Content-Encoding"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Content-Language"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Content-Length"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Content-MD5"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Content-Type"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Expires"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-grant-full-control"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-grant-read"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-grant-read-acp"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-grant-write-acp"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-server-side-encryption"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-storage-class"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-website-redirect-location"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-server-side-encryption-customer-algorithm"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-server-side-encryption-customer-key"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-server-side-encryption-customer-key-MD5"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-server-side-encryption-aws-kms-key-id"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-server-side-encryption-context"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-server-side-encryption-bucket-key-enabled"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-tagging"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-object-lock-mode"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-object-lock-retain-until-date"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-object-lock-legal-hold"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-copy-source"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-copy-source-range"),
};

const size_t g_s3_complete_multipart_upload_excluded_headers_count =
    AWS_ARRAY_SIZE(g_s3_complete_multipart_upload_excluded_headers);

const struct aws_byte_cursor g_s3_abort_multipart_upload_excluded_headers[] = {
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-acl"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Cache-Control"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Content-Disposition"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Content-Encoding"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Content-Language"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Content-Length"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Content-MD5"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Content-Type"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Expires"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-grant-full-control"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-grant-read"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-grant-read-acp"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-grant-write-acp"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-server-side-encryption"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-storage-class"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-website-redirect-location"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-server-side-encryption-customer-algorithm"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-server-side-encryption-customer-key"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-server-side-encryption-customer-key-MD5"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-server-side-encryption-aws-kms-key-id"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-server-side-encryption-context"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-server-side-encryption-bucket-key-enabled"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-tagging"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-object-lock-mode"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-object-lock-retain-until-date"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-object-lock-legal-hold"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-copy-source"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-copy-source-range"),
};

const size_t g_s3_abort_multipart_upload_excluded_headers_count =
    AWS_ARRAY_SIZE(g_s3_abort_multipart_upload_excluded_headers);

static int s_s3_message_util_add_content_range_header(
    uint64_t part_range_start,
    uint64_t part_range_end,
    struct aws_http_message *out_message);

/* Create a new get object request from an existing get object request. Currently just adds an optional ranged header.
 */
struct aws_http_message *aws_s3_ranged_get_object_message_new(
    struct aws_allocator *allocator,
    struct aws_http_message *base_message,
    uint64_t range_start,
    uint64_t range_end) {
    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(base_message);

    struct aws_http_message *message = aws_s3_message_util_copy_http_message_no_body(allocator, base_message, NULL, 0);

    if (message == NULL) {
        return NULL;
    }

    if (s_s3_message_util_add_content_range_header(range_start, range_end, message)) {
        goto error_clean_up;
    }

    return message;

error_clean_up:

    if (message != NULL) {
        aws_http_message_release(message);
        message = NULL;
    }

    return NULL;
}

/* Creates a create-multipart-upload request from a given put objet request. */
struct aws_http_message *aws_s3_create_multipart_upload_message_new(
    struct aws_allocator *allocator,
    struct aws_http_message *base_message,
    enum aws_s3_checksum_algorithm algorithm) {
    AWS_PRECONDITION(allocator);

    /* For multipart upload, sse related headers should only be shown in create-multipart request */
    struct aws_http_message *message = aws_s3_message_util_copy_http_message_no_body(
        allocator,
        base_message,
        g_s3_create_multipart_upload_excluded_headers,
        AWS_ARRAY_SIZE(g_s3_create_multipart_upload_excluded_headers));

    if (message == NULL) {
        goto error_clean_up;
    }

    if (aws_s3_message_util_set_multipart_request_path(allocator, NULL, 0, true, message)) {
        goto error_clean_up;
    }

    struct aws_http_headers *headers = aws_http_message_get_headers(message);

    if (headers == NULL) {
        goto error_clean_up;
    }

    if (aws_http_headers_erase(headers, g_content_md5_header_name)) {
        if (aws_last_error_or_unknown() != AWS_ERROR_HTTP_HEADER_NOT_FOUND) {
            goto error_clean_up;
        }
    }
    if (algorithm) {
        if (aws_http_headers_set(
                headers,
                g_create_mpu_checksum_header_name,
                *aws_get_create_mpu_header_name_from_algorithm(algorithm))) {
            goto error_clean_up;
        }
    }

    aws_http_message_set_request_method(message, g_post_method);
    aws_http_message_set_body_stream(message, NULL);

    return message;

error_clean_up:

    if (message != NULL) {
        aws_http_message_release(message);
        message = NULL;
    }

    return NULL;
}

/* Create a new put object request from an existing put object request.  Currently just optionally adds part information
 * for a multipart upload. */
struct aws_http_message *aws_s3_upload_part_message_new(
    struct aws_allocator *allocator,
    struct aws_http_message *base_message,
    struct aws_byte_buf *buffer,
    uint32_t part_number,
    const struct aws_string *upload_id,
    bool should_compute_content_md5,
    const enum aws_s3_checksum_algorithm checksum_algorithm,
    struct aws_byte_buf *encoded_checksum_output) {
    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(base_message);
    AWS_PRECONDITION(part_number > 0);

    struct aws_http_message *message = aws_s3_message_util_copy_http_message_no_body(
        allocator, base_message, g_s3_upload_part_excluded_headers, AWS_ARRAY_SIZE(g_s3_upload_part_excluded_headers));

    if (message == NULL) {
        goto error_clean_up;
    }

    if (aws_s3_message_util_set_multipart_request_path(allocator, upload_id, part_number, false, message)) {
        goto error_clean_up;
    }

    if (buffer != NULL) {

        if (aws_s3_message_util_assign_body(allocator, buffer, message, checksum_algorithm, encoded_checksum_output) ==
            NULL) {
            goto error_clean_up;
        }
        if (checksum_algorithm == AWS_SCA_NONE && should_compute_content_md5) {
            if (aws_s3_message_util_add_content_md5_header(allocator, buffer, message)) {
                goto error_clean_up;
            }
        }
    } else {
        goto error_clean_up;
    }

    return message;

error_clean_up:

    if (message != NULL) {
        aws_http_message_release(message);
        message = NULL;
    }

    return NULL;
}

struct aws_http_message *aws_s3_upload_part_copy_message_new(
    struct aws_allocator *allocator,
    struct aws_http_message *base_message,
    struct aws_byte_buf *buffer,
    uint32_t part_number,
    uint64_t range_start,
    uint64_t range_end,
    const struct aws_string *upload_id,
    bool should_compute_content_md5) {
    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(base_message);
    AWS_PRECONDITION(part_number > 0);

    struct aws_http_message *message = aws_s3_message_util_copy_http_message_no_body(
        allocator, base_message, g_s3_upload_part_excluded_headers, AWS_ARRAY_SIZE(g_s3_upload_part_excluded_headers));

    if (message == NULL) {
        goto error_clean_up;
    }

    if (aws_s3_message_util_set_multipart_request_path(allocator, upload_id, part_number, false, message)) {
        goto error_clean_up;
    }

    if (buffer != NULL) {
        /* part copy does not have a ChecksumAlgorithm member, it will use the same algorithm as the create
         * multipart upload request specifies */
        if (aws_s3_message_util_assign_body(allocator, buffer, message, AWS_SCA_NONE, NULL /* out_checksum */) ==
            NULL) {
            goto error_clean_up;
        }

        if (should_compute_content_md5) {
            if (aws_s3_message_util_add_content_md5_header(allocator, buffer, message)) {
                goto error_clean_up;
            }
        }
    }

    char source_range[1024];
    snprintf(source_range, sizeof(source_range), "bytes=%" PRIu64 "-%" PRIu64, range_start, range_end);

    struct aws_http_header source_range_header = {
        .name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-copy-source-range"),
        .value = aws_byte_cursor_from_c_str(source_range),
    };

    struct aws_http_headers *headers = aws_http_message_get_headers(message);
    aws_http_headers_add_header(headers, &source_range_header);

    return message;

error_clean_up:

    if (message != NULL) {
        aws_http_message_release(message);
        message = NULL;
    }

    return NULL;
}

/* Creates a HEAD GetObject request to get the size of the specified object. */
struct aws_http_message *aws_s3_get_object_size_message_new(
    struct aws_allocator *allocator,
    struct aws_http_message *base_message,
    struct aws_byte_cursor source_bucket,
    struct aws_byte_cursor source_key) {

    (void)base_message;

    AWS_PRECONDITION(allocator);

    struct aws_http_message *message = aws_http_message_new_request(allocator);

    if (message == NULL) {
        return NULL;
    }

    const struct aws_byte_cursor head_operation = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("HEAD");
    if (aws_http_message_set_request_method(message, head_operation)) {
        goto error_clean_up;
    }

    char destination_path[1024];
    snprintf(destination_path, sizeof(destination_path), "/%.*s", (int)source_key.len, source_key.ptr);
    /* TODO: url encode */

    if (aws_http_message_set_request_path(message, aws_byte_cursor_from_c_str(destination_path))) {
        goto error_clean_up;
    }

    char host_header_value[1024];
    snprintf(
        host_header_value,
        sizeof(host_header_value),
        "%.*s.s3.us-west-2.amazonaws.com",
        (int)source_bucket.len,
        source_bucket.ptr);
    struct aws_http_header host_header = {
        .name = g_host_header_name,
        .value = aws_byte_cursor_from_c_str(host_header_value),
    };
    aws_http_message_add_header(message, host_header);

    aws_http_message_set_body_stream(message, NULL);

    return message;

error_clean_up:

    if (message != NULL) {
        aws_http_message_release(message);
        message = NULL;
    }

    return NULL;
}

/* Creates a HEAD GetObject sub-request to get the size of the source object of a Copy meta request. */
struct aws_http_message *aws_s3_get_source_object_size_message_new(
    struct aws_allocator *allocator,
    struct aws_http_message *base_message) {
    AWS_PRECONDITION(allocator);

    struct aws_http_message *message = NULL;

    /* find the x-amz-copy-source header */
    struct aws_http_headers *headers = aws_http_message_get_headers(base_message);

    struct aws_byte_cursor source_header_value;
    AWS_ZERO_STRUCT(source_header_value);

    const struct aws_byte_cursor copy_source_header = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-copy-source");
    if (aws_http_headers_get(headers, copy_source_header, &source_header_value) != AWS_OP_SUCCESS) {
        AWS_LOGF_ERROR(AWS_LS_S3_GENERAL, "CopyRequest is missing the x-amz-copy-source header");
        return NULL;
    }

    /* url decode */
    struct aws_byte_buf decode_buffer;
    AWS_ZERO_STRUCT(decode_buffer);
    aws_byte_buf_init(&decode_buffer, allocator, 0);
    if (aws_byte_buf_append_decoding_uri(&decode_buffer, &source_header_value) != AWS_OP_SUCCESS) {
        goto error_cleanup;
    }

    /* extracts source bucket and key */
    struct aws_byte_cursor source_bucket = aws_byte_cursor_from_buf(&decode_buffer);

    /* x-amz-copy-source might have an optional leading slash. if so let's skip it */
    if (decode_buffer.len > 1 && decode_buffer.buffer[0] == '/') {
        /* skip the leading slash */
        aws_byte_cursor_advance(&source_bucket, 1);
    }

    /* as we skipped the optional leading slash, from this point source format is always {bucket}/{key}. split them.
     */
    struct aws_byte_cursor source_key = source_bucket;

    while (source_key.len > 0) {
        if (*source_key.ptr == '/') {
            source_bucket.len = source_key.ptr - source_bucket.ptr;
            aws_byte_cursor_advance(&source_key, 1); /* skip the / between bucket and key */
            break;
        }
        aws_byte_cursor_advance(&source_key, 1);
    }

    if (source_bucket.len == 0 || source_key.len == 0) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_GENERAL,
            "The CopyRequest x-amz-copy-source header must contain the bucket and object key separated by a slash");
        goto error_cleanup;
    }

    message = aws_s3_get_object_size_message_new(allocator, base_message, source_bucket, source_key);

error_cleanup:
    aws_byte_buf_clean_up(&decode_buffer);
    return message;
}

static const struct aws_byte_cursor s_complete_payload_begin = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL(
    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
    "<CompleteMultipartUpload xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n");

static const struct aws_byte_cursor s_complete_payload_end =
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("</CompleteMultipartUpload>");

static const struct aws_byte_cursor s_part_section_string_0 = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("    <Part>\n"
                                                                                                    "        <ETag>");

static const struct aws_byte_cursor s_part_section_string_1 =
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("</ETag>\n"
                                          "         <PartNumber>");

static const struct aws_byte_cursor s_close_part_number_tag = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("</PartNumber>\n");
static const struct aws_byte_cursor s_close_part_tag = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("    </Part>\n");
static const struct aws_byte_cursor s_open_start_bracket = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("         <");
static const struct aws_byte_cursor s_open_end_bracket = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("</");
static const struct aws_byte_cursor s_close_bracket = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL(">");
static const struct aws_byte_cursor s_close_bracket_new_line = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL(">\n");
/* Create a complete-multipart message, which includes an XML payload of all completed parts. */
struct aws_http_message *aws_s3_complete_multipart_message_new(
    struct aws_allocator *allocator,
    struct aws_http_message *base_message,
    struct aws_byte_buf *body_buffer,
    const struct aws_string *upload_id,
    const struct aws_array_list *etags,
    struct aws_byte_buf *checksums,
    enum aws_s3_checksum_algorithm algorithm) {
    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(base_message);
    AWS_PRECONDITION(body_buffer);
    AWS_PRECONDITION(upload_id);
    AWS_PRECONDITION(etags);

    const struct aws_byte_cursor *mpu_algorithm_checksum_name = aws_get_complete_mpu_name_from_algorithm(algorithm);

    struct aws_http_message *message = aws_s3_message_util_copy_http_message_no_body(
        allocator,
        base_message,
        g_s3_complete_multipart_upload_excluded_headers,
        AWS_ARRAY_SIZE(g_s3_complete_multipart_upload_excluded_headers));

    struct aws_http_headers *headers = NULL;

    if (message == NULL) {
        goto error_clean_up;
    }

    if (aws_s3_message_util_set_multipart_request_path(allocator, upload_id, 0, false, message)) {
        goto error_clean_up;
    }

    aws_http_message_set_request_method(message, g_post_method);

    headers = aws_http_message_get_headers(message);

    if (headers == NULL) {
        goto error_clean_up;
    }

    /* Create XML payload with all of the etags of finished parts */
    {
        aws_byte_buf_reset(body_buffer, false);

        if (aws_byte_buf_append_dynamic(body_buffer, &s_complete_payload_begin)) {
            goto error_clean_up;
        }

        for (size_t etag_index = 0; etag_index < aws_array_list_length(etags); ++etag_index) {
            struct aws_string *etag = NULL;

            aws_array_list_get_at(etags, &etag, etag_index);

            AWS_FATAL_ASSERT(etag != NULL);

            if (aws_byte_buf_append_dynamic(body_buffer, &s_part_section_string_0)) {
                goto error_clean_up;
            }

            struct aws_byte_cursor etag_byte_cursor = aws_byte_cursor_from_string(etag);

            if (aws_byte_buf_append_dynamic(body_buffer, &etag_byte_cursor)) {
                goto error_clean_up;
            }

            if (aws_byte_buf_append_dynamic(body_buffer, &s_part_section_string_1)) {
                goto error_clean_up;
            }

            char part_number_buffer[32] = "";
            int part_number = (int)(etag_index + 1);
            int part_number_num_char = snprintf(part_number_buffer, sizeof(part_number_buffer), "%d", part_number);
            struct aws_byte_cursor part_number_byte_cursor =
                aws_byte_cursor_from_array(part_number_buffer, part_number_num_char);

            if (aws_byte_buf_append_dynamic(body_buffer, &part_number_byte_cursor)) {
                goto error_clean_up;
            }

            if (aws_byte_buf_append_dynamic(body_buffer, &s_close_part_number_tag)) {
                goto error_clean_up;
            }
            if (mpu_algorithm_checksum_name) {
                struct aws_byte_cursor checksum = aws_byte_cursor_from_buf(&checksums[etag_index]);

                if (aws_byte_buf_append_dynamic(body_buffer, &s_open_start_bracket)) {
                    goto error_clean_up;
                }
                if (aws_byte_buf_append_dynamic(body_buffer, mpu_algorithm_checksum_name)) {
                    goto error_clean_up;
                }
                if (aws_byte_buf_append_dynamic(body_buffer, &s_close_bracket)) {
                    goto error_clean_up;
                }
                if (aws_byte_buf_append_dynamic(body_buffer, &checksum)) {
                    goto error_clean_up;
                }
                if (aws_byte_buf_append_dynamic(body_buffer, &s_open_end_bracket)) {
                    goto error_clean_up;
                }
                if (aws_byte_buf_append_dynamic(body_buffer, mpu_algorithm_checksum_name)) {
                    goto error_clean_up;
                }
                if (aws_byte_buf_append_dynamic(body_buffer, &s_close_bracket_new_line)) {
                    goto error_clean_up;
                }
            }
            if (aws_byte_buf_append_dynamic(body_buffer, &s_close_part_tag)) {
                goto error_clean_up;
            }
        }

        if (aws_byte_buf_append_dynamic(body_buffer, &s_complete_payload_end)) {
            goto error_clean_up;
        }

        aws_s3_message_util_assign_body(allocator, body_buffer, message, AWS_SCA_NONE, NULL /* out_checksum */);
    }

    return message;

error_clean_up:

    AWS_LOGF_ERROR(AWS_LS_S3_GENERAL, "Could not create complete multipart message");

    if (message != NULL) {
        aws_http_message_release(message);
        message = NULL;
    }

    return NULL;
}

struct aws_http_message *aws_s3_abort_multipart_upload_message_new(
    struct aws_allocator *allocator,
    struct aws_http_message *base_message,
    const struct aws_string *upload_id) {

    struct aws_http_message *message = aws_s3_message_util_copy_http_message_no_body(
        allocator,
        base_message,
        g_s3_abort_multipart_upload_excluded_headers,
        AWS_ARRAY_SIZE(g_s3_abort_multipart_upload_excluded_headers));

    if (aws_s3_message_util_set_multipart_request_path(allocator, upload_id, 0, false, message)) {
        goto error_clean_up;
    }
    aws_http_message_set_request_method(message, g_delete_method);

    return message;

error_clean_up:

    AWS_LOGF_ERROR(AWS_LS_S3_GENERAL, "Could not create abort multipart upload message");

    if (message != NULL) {
        aws_http_message_release(message);
        message = NULL;
    }

    return NULL;
}

/* Assign a buffer to an HTTP message, creating a stream and setting the content-length header */
struct aws_input_stream *aws_s3_message_util_assign_body(
    struct aws_allocator *allocator,
    struct aws_byte_buf *byte_buf,
    struct aws_http_message *out_message,
    enum aws_s3_checksum_algorithm algorithm,
    struct aws_byte_buf *out_checksum) {
    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(out_message);
    AWS_PRECONDITION(byte_buf);

    struct aws_byte_cursor buffer_byte_cursor = aws_byte_cursor_from_buf(byte_buf);
    struct aws_http_headers *headers = aws_http_message_get_headers(out_message);

    if (headers == NULL) {
        return NULL;
    }

    struct aws_input_stream *input_stream = aws_input_stream_new_from_cursor(allocator, &buffer_byte_cursor);

    if (input_stream == NULL) {
        goto error_clean_up;
    }

    if (algorithm) {
        /* set Content-Encoding header */
        if (aws_http_headers_set(headers, g_content_encoding_header_name, g_content_encoding_header_aws_chunked)) {
            goto error_clean_up;
        }
        /* set x-amz-trailer header */
        if (aws_http_headers_set(headers, g_trailer_header_name, *aws_get_http_header_name_from_algorithm(algorithm))) {
            goto error_clean_up;
        }
        /* set x-amz-decoded-content-length header */
        char decoded_content_length_buffer[64] = "";
        snprintf(
            decoded_content_length_buffer,
            sizeof(decoded_content_length_buffer),
            "%" PRIu64,
            (uint64_t)buffer_byte_cursor.len);
        struct aws_byte_cursor decode_content_length_cursor =
            aws_byte_cursor_from_array(decoded_content_length_buffer, strlen(decoded_content_length_buffer));
        if (aws_http_headers_set(headers, g_decoded_content_length_header_name, decode_content_length_cursor)) {
            goto error_clean_up;
        }
        /* set input stream to chunk stream */
        input_stream = aws_chunk_stream_new(allocator, input_stream, algorithm, out_checksum);
        if (!input_stream) {
            goto error_clean_up;
        }
    }
    int64_t stream_length = 0;
    if (aws_input_stream_get_length(input_stream, &stream_length)) {
        goto error_clean_up;
    }
    char content_length_buffer[64] = "";
    snprintf(content_length_buffer, sizeof(content_length_buffer), "%" PRIu64, (uint64_t)stream_length);
    struct aws_byte_cursor content_length_cursor =
        aws_byte_cursor_from_array(content_length_buffer, strlen(content_length_buffer));
    if (aws_http_headers_set(headers, g_content_length_header_name, content_length_cursor)) {
        goto error_clean_up;
    }

    aws_http_message_set_body_stream(out_message, input_stream);

    return input_stream;

error_clean_up:
    AWS_LOGF_ERROR(AWS_LS_S3_CLIENT, "Failed to assign body for s3 request http message, from body buffer .");
    aws_input_stream_destroy(input_stream);
    return NULL;
}

/* Add a content-md5 header. */
int aws_s3_message_util_add_content_md5_header(
    struct aws_allocator *allocator,
    struct aws_byte_buf *input_buf,
    struct aws_http_message *out_message) {

    AWS_PRECONDITION(out_message);

    /* Compute MD5 */
    struct aws_byte_cursor md5_input = aws_byte_cursor_from_buf(input_buf);
    uint8_t md5_output[AWS_MD5_LEN];
    struct aws_byte_buf md5_output_buf = aws_byte_buf_from_empty_array(md5_output, sizeof(md5_output));
    if (aws_md5_compute(allocator, &md5_input, &md5_output_buf, 0)) {
        return AWS_OP_ERR;
    }

    /* Compute Base64 encoding of MD5 */
    struct aws_byte_cursor base64_input = aws_byte_cursor_from_buf(&md5_output_buf);
    size_t base64_output_size = 0;
    if (aws_base64_compute_encoded_len(md5_output_buf.len, &base64_output_size)) {
        return AWS_OP_ERR;
    }
    struct aws_byte_buf base64_output_buf;
    if (aws_byte_buf_init(&base64_output_buf, allocator, base64_output_size)) {
        return AWS_OP_ERR;
    }
    if (aws_base64_encode(&base64_input, &base64_output_buf)) {
        goto error_clean_up;
    }

    struct aws_http_headers *headers = aws_http_message_get_headers(out_message);
    if (aws_http_headers_set(headers, g_content_md5_header_name, aws_byte_cursor_from_buf(&base64_output_buf))) {
        goto error_clean_up;
    }

    aws_byte_buf_clean_up(&base64_output_buf);
    return AWS_OP_SUCCESS;

error_clean_up:

    aws_byte_buf_clean_up(&base64_output_buf);
    return AWS_OP_ERR;
}

/* Copy an existing HTTP message's headers, method, and path. */
struct aws_http_message *aws_s3_message_util_copy_http_message_no_body(
    struct aws_allocator *allocator,
    struct aws_http_message *base_message,
    const struct aws_byte_cursor *excluded_header_array,
    size_t excluded_header_array_size) {
    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(base_message);

    struct aws_http_message *message = aws_http_message_new_request(allocator);

    if (message == NULL) {
        return NULL;
    }

    struct aws_byte_cursor request_method;
    if (aws_http_message_get_request_method(base_message, &request_method)) {
        goto error_clean_up;
    }

    if (aws_http_message_set_request_method(message, request_method)) {
        goto error_clean_up;
    }

    struct aws_byte_cursor request_path;
    if (aws_http_message_get_request_path(base_message, &request_path)) {
        goto error_clean_up;
    }

    if (aws_http_message_set_request_path(message, request_path)) {
        goto error_clean_up;
    }

    size_t num_headers = aws_http_message_get_header_count(base_message);

    for (size_t header_index = 0; header_index < num_headers; ++header_index) {
        struct aws_http_header header;

        if (aws_http_message_get_header(base_message, &header, header_index)) {
            goto error_clean_up;
        }

        if (excluded_header_array && excluded_header_array_size > 0) {
            bool exclude_header = false;

            for (size_t exclude_index = 0; exclude_index < excluded_header_array_size; ++exclude_index) {
                if (aws_byte_cursor_eq_ignore_case(&header.name, &excluded_header_array[exclude_index])) {
                    exclude_header = true;
                    break;
                }
            }

            if (exclude_header) {
                continue;
            }
        }

        if (aws_http_message_add_header(message, header)) {
            goto error_clean_up;
        }
    }

    return message;

error_clean_up:

    if (message != NULL) {
        aws_http_message_release(message);
        message = NULL;
    }

    return NULL;
}

/* Add a content-range header.*/
static int s_s3_message_util_add_content_range_header(
    uint64_t part_range_start,
    uint64_t part_range_end,
    struct aws_http_message *out_message) {
    AWS_PRECONDITION(out_message);

    /* ((2^64)-1 = 20 characters;  2*20 + length-of("bytes=-") < 128) */
    char range_value_buffer[128] = "";
    snprintf(
        range_value_buffer, sizeof(range_value_buffer), "bytes=%" PRIu64 "-%" PRIu64, part_range_start, part_range_end);

    struct aws_http_header range_header;
    AWS_ZERO_STRUCT(range_header);
    range_header.name = g_range_header_name;
    range_header.value = aws_byte_cursor_from_c_str(range_value_buffer);

    struct aws_http_headers *headers = aws_http_message_get_headers(out_message);
    AWS_ASSERT(headers != NULL);

    int erase_result = aws_http_headers_erase(headers, range_header.name);
    AWS_ASSERT(erase_result == AWS_OP_SUCCESS || aws_last_error() == AWS_ERROR_HTTP_HEADER_NOT_FOUND)
    (void)erase_result;

    if (aws_http_message_add_header(out_message, range_header)) {
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}

/* Handle setting up the multipart request path for a message. */
int aws_s3_message_util_set_multipart_request_path(
    struct aws_allocator *allocator,
    const struct aws_string *upload_id,
    uint32_t part_number,
    bool append_uploads_suffix,
    struct aws_http_message *message) {

    const struct aws_byte_cursor question_mark = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("?");
    const struct aws_byte_cursor ampersand = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("&");

    const struct aws_byte_cursor uploads_suffix = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("uploads");
    const struct aws_byte_cursor part_number_arg = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("partNumber=");
    const struct aws_byte_cursor upload_id_arg = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("uploadId=");

    struct aws_byte_buf request_path_buf;
    struct aws_byte_cursor request_path;

    if (aws_http_message_get_request_path(message, &request_path)) {
        return AWS_OP_ERR;
    }

    if (aws_byte_buf_init(&request_path_buf, allocator, request_path.len)) {
        return AWS_OP_ERR;
    }

    if (aws_byte_buf_append_dynamic(&request_path_buf, &request_path)) {
        goto error_clean_up;
    }

    bool has_existing_query_parameters = false;

    for (size_t i = 0; i < request_path.len; ++i) {
        if (request_path.ptr[i] == '?') {
            has_existing_query_parameters = true;
            break;
        }
    }

    if (part_number > 0) {
        if (aws_byte_buf_append_dynamic(
                &request_path_buf, has_existing_query_parameters ? &ampersand : &question_mark)) {
            goto error_clean_up;
        }

        if (aws_byte_buf_append_dynamic(&request_path_buf, &part_number_arg)) {
            goto error_clean_up;
        }

        char part_number_buffer[32] = "";
        snprintf(part_number_buffer, sizeof(part_number_buffer), "%d", part_number);
        struct aws_byte_cursor part_number_cursor =
            aws_byte_cursor_from_array(part_number_buffer, strlen(part_number_buffer));

        if (aws_byte_buf_append_dynamic(&request_path_buf, &part_number_cursor)) {
            goto error_clean_up;
        }

        has_existing_query_parameters = true;
    }

    if (upload_id != NULL) {

        struct aws_byte_cursor upload_id_cursor = aws_byte_cursor_from_string(upload_id);

        if (aws_byte_buf_append_dynamic(
                &request_path_buf, has_existing_query_parameters ? &ampersand : &question_mark)) {
            goto error_clean_up;
        }

        if (aws_byte_buf_append_dynamic(&request_path_buf, &upload_id_arg)) {
            goto error_clean_up;
        }

        if (aws_byte_buf_append_dynamic(&request_path_buf, &upload_id_cursor)) {
            goto error_clean_up;
        }

        has_existing_query_parameters = true;
    }

    if (append_uploads_suffix) {
        if (aws_byte_buf_append_dynamic(
                &request_path_buf, has_existing_query_parameters ? &ampersand : &question_mark)) {
            goto error_clean_up;
        }

        if (aws_byte_buf_append_dynamic(&request_path_buf, &uploads_suffix)) {
            goto error_clean_up;
        }

        has_existing_query_parameters = true;
    }

    struct aws_byte_cursor new_request_path = aws_byte_cursor_from_buf(&request_path_buf);

    if (aws_http_message_set_request_path(message, new_request_path)) {
        goto error_clean_up;
    }

    aws_byte_buf_clean_up(&request_path_buf);
    return AWS_OP_SUCCESS;

error_clean_up:

    aws_byte_buf_clean_up(&request_path_buf);

    return AWS_OP_ERR;
}
