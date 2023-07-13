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
#include <aws/common/environment.h>
#include <aws/common/ref_count.h>
#include <aws/http/request_response.h>
#include <aws/io/channel_bootstrap.h>
#include <aws/io/event_loop.h>
#include <aws/io/host_resolver.h>
#include <aws/io/stream.h>
#include <aws/io/tls_channel_handler.h>
#include <aws/testing/aws_test_harness.h>
#include <inttypes.h>
#include <stdio.h>

AWS_TEST_CASE(test_s3_replace_quote_entities, s_test_s3_replace_quote_entities)
static int s_test_s3_replace_quote_entities(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct test_case {
        struct aws_byte_cursor test_string;
        const char *expected_result;
    };

    struct test_case test_cases[] = {
        {
            .test_string = aws_byte_cursor_from_c_str("&quot;testtest"),
            .expected_result = "\"testtest",
        },
        {
            .test_string = aws_byte_cursor_from_c_str("testtest&quot;"),
            .expected_result = "testtest\"",
        },
        {
            .test_string = aws_byte_cursor_from_c_str("&quot;&quot;"),
            .expected_result = "\"\"",
        },
        {
            .test_string = aws_byte_cursor_from_c_str("testtest"),
            .expected_result = "testtest",
        },
        {
            .test_string = aws_byte_cursor_from_c_str(""),
            .expected_result = "",
        },
    };

    for (size_t i = 0; i < AWS_ARRAY_SIZE(test_cases); ++i) {
        struct test_case *test_case = &test_cases[i];

        struct aws_byte_buf result_byte_buf = aws_replace_quote_entities(allocator, test_case->test_string);

        struct aws_byte_cursor result_byte_cursor = aws_byte_cursor_from_buf(&result_byte_buf);

        ASSERT_CURSOR_VALUE_CSTRING_EQUALS(result_byte_cursor, test_case->expected_result);

        aws_byte_buf_clean_up(&result_byte_buf);
    }

    return 0;
}

AWS_TEST_CASE(test_s3_strip_quotes, s_test_s3_strip_quotes)
static int s_test_s3_strip_quotes(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct test_case {
        struct aws_byte_cursor test_cursor;
        struct aws_byte_cursor expected_result;
    };

    struct test_case test_cases[] = {
        {
            .test_cursor = aws_byte_cursor_from_c_str("\"test\""),
            .expected_result = aws_byte_cursor_from_c_str("test"),
        },
        {
            .test_cursor = aws_byte_cursor_from_c_str("test\""),
            .expected_result = aws_byte_cursor_from_c_str("test\""),
        },
        {
            .test_cursor = aws_byte_cursor_from_c_str("\"test"),
            .expected_result = aws_byte_cursor_from_c_str("\"test"),
        },
        {
            .test_cursor = aws_byte_cursor_from_c_str("test"),
            .expected_result = aws_byte_cursor_from_c_str("test"),
        },
        {
            .test_cursor = aws_byte_cursor_from_c_str(""),
            .expected_result = aws_byte_cursor_from_c_str(""),
        },
    };

    for (size_t i = 0; i < AWS_ARRAY_SIZE(test_cases); ++i) {
        struct test_case *test_case = &test_cases[i];

        struct aws_byte_buf result_byte_buf;
        AWS_ZERO_STRUCT(result_byte_buf);

        struct aws_string *result = aws_strip_quotes(allocator, test_case->test_cursor);

        struct aws_byte_cursor result_byte_cursor = aws_byte_cursor_from_string(result);

        ASSERT_TRUE(aws_byte_cursor_eq(&test_case->expected_result, &result_byte_cursor));

        aws_byte_buf_clean_up(&result_byte_buf);
        aws_string_destroy(result);
    }

    return 0;
}

AWS_TEST_CASE(test_s3_parse_content_range_response_header, s_test_s3_parse_content_range_response_header)
static int s_test_s3_parse_content_range_response_header(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    const struct aws_http_header content_range_header = {
        .name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Content-Range"),
        .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("bytes 55-100/12345"),
    };

    const struct aws_http_header invalid_content_range_header = {
        .name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Content-Range"),
        .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("bytes 55-100/"),
    };

    struct aws_http_headers *response_headers = aws_http_headers_new(allocator);

    /* Try to parse a header that isn't there. */
    {
        uint64_t object_size = 0ULL;

        ASSERT_FAILS(aws_s3_parse_content_range_response_header(allocator, response_headers, NULL, NULL, &object_size));
        ASSERT_TRUE(aws_last_error() == AWS_ERROR_S3_MISSING_CONTENT_RANGE_HEADER);
    }

    aws_http_headers_add_header(response_headers, &content_range_header);

    /* Parse all of the data from a valid header. */
    {
        uint64_t object_size = 0ULL;
        uint64_t range_start = 0ULL;
        uint64_t range_end = 0ULL;

        ASSERT_SUCCESS(aws_s3_parse_content_range_response_header(
            allocator, response_headers, &range_start, &range_end, &object_size));
        ASSERT_TRUE(range_start == 55ULL);
        ASSERT_TRUE(range_end == 100ULL);
        ASSERT_TRUE(object_size == 12345ULL);
    }

    /* Range-end and range-start are optional output arguments. */
    {
        uint64_t object_size = 0ULL;

        ASSERT_SUCCESS(
            aws_s3_parse_content_range_response_header(allocator, response_headers, NULL, NULL, &object_size));
        ASSERT_TRUE(object_size == 12345ULL);
    }

    aws_http_headers_set(response_headers, invalid_content_range_header.name, invalid_content_range_header.value);

    /* Try to parse an invalid header. */
    {
        uint64_t object_size = 0ULL;
        ASSERT_FAILS(aws_s3_parse_content_range_response_header(allocator, response_headers, NULL, NULL, &object_size));
        ASSERT_TRUE(aws_last_error() == AWS_ERROR_S3_INVALID_CONTENT_RANGE_HEADER);
    }

    aws_http_headers_release(response_headers);

    return 0;
}

AWS_TEST_CASE(test_s3_parse_content_length_response_header, s_test_s3_parse_content_length_response_header)
static int s_test_s3_parse_content_length_response_header(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_http_headers *response_headers = aws_http_headers_new(allocator);

    const struct aws_http_header valid_content_length_header = {
        .name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Content-Length"),
        .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("12345"),
    };

    const struct aws_http_header invalid_content_length_header = {
        .name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Content-Length"),
        .value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL(""),
    };

    /* Try to parse a header that isn't there. */
    {
        uint64_t content_length = 0ULL;
        ASSERT_FAILS(aws_s3_parse_content_length_response_header(allocator, response_headers, &content_length));
        ASSERT_TRUE(aws_last_error() == AWS_ERROR_S3_MISSING_CONTENT_LENGTH_HEADER);
    }

    aws_http_headers_add_header(response_headers, &valid_content_length_header);

    /* Parse a valid header. */
    {
        uint64_t content_length = 0ULL;
        ASSERT_SUCCESS(aws_s3_parse_content_length_response_header(allocator, response_headers, &content_length));
        ASSERT_TRUE(content_length == 12345ULL);
    }

    aws_http_headers_set(response_headers, invalid_content_length_header.name, invalid_content_length_header.value);

    /* Try to parse an invalid header. */
    {
        uint64_t content_length = 0ULL;
        ASSERT_FAILS(aws_s3_parse_content_length_response_header(allocator, response_headers, &content_length));
        ASSERT_TRUE(aws_last_error() == AWS_ERROR_S3_INVALID_CONTENT_LENGTH_HEADER);
    }

    aws_http_headers_release(response_headers);

    return 0;
}

static int s_validate_part_ranges(
    uint64_t object_range_start,
    uint64_t object_range_end,
    size_t part_size,
    uint32_t num_parts,
    const uint64_t *part_ranges) {
    ASSERT_TRUE(part_ranges != NULL);

    for (uint32_t i = 0; i < num_parts; ++i) {
        uint64_t part_range_start = 0ULL;
        uint64_t part_range_end = 0ULL;

        aws_s3_get_part_range(
            object_range_start, object_range_end, part_size, i + 1, &part_range_start, &part_range_end);

        ASSERT_TRUE(part_range_start == part_ranges[i * 2]);
        ASSERT_TRUE(part_range_end == part_ranges[i * 2 + 1]);
    }

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(test_s3_get_num_parts_and_get_part_range, s_test_s3_get_num_parts_and_get_part_range)
static int s_test_s3_get_num_parts_and_get_part_range(struct aws_allocator *allocator, void *ctx) {
    (void)allocator;
    (void)ctx;

    const size_t part_size = 16 * 1024;

    /* Perfectly aligned on part boundaries. */
    {
        const uint32_t expected_num_parts = 2;
        const uint64_t object_range_start = 0;
        const uint64_t object_range_end =
            (object_range_start + (uint64_t)part_size * (uint64_t)expected_num_parts) - 1ULL;

        const uint64_t part_ranges[] = {
            0,
            (uint64_t)part_size - 1ULL,

            (uint64_t)part_size,
            (uint64_t)part_size * 2ULL - 1ULL,
        };

        ASSERT_TRUE(aws_s3_get_num_parts(part_size, object_range_start, object_range_end) == expected_num_parts);

        ASSERT_SUCCESS(
            s_validate_part_ranges(object_range_start, object_range_end, part_size, expected_num_parts, part_ranges));
    }

    /* Range-start unaligned on part boundaries, but range-end aligned. */
    {
        const uint32_t expected_num_parts = 3;
        const uint64_t half_part_size = part_size >> 1ULL;
        const uint64_t object_range_start = half_part_size;
        const uint64_t object_range_end = (object_range_start + half_part_size + (uint64_t)part_size * 2ULL) - 1ULL;

        const uint64_t part_ranges[] = {
            object_range_start,
            object_range_start + half_part_size - 1,

            object_range_start + half_part_size,
            object_range_start + half_part_size + (uint64_t)part_size - 1ULL,

            object_range_start + half_part_size + (uint64_t)part_size,
            object_range_start + half_part_size + (uint64_t)part_size * 2ULL - 1ULL,
        };

        ASSERT_TRUE(aws_s3_get_num_parts(part_size, object_range_start, object_range_end) == expected_num_parts);

        ASSERT_SUCCESS(
            s_validate_part_ranges(object_range_start, object_range_end, part_size, expected_num_parts, part_ranges));
    }

    /* Range-start and range-end both unaligned on part boundaries. */
    {
        const uint32_t expected_num_parts = 4;
        const uint64_t half_part_size = part_size >> 1ULL;
        const uint64_t object_range_start = half_part_size;
        const uint64_t object_range_end =
            (object_range_start + half_part_size + (uint64_t)part_size * 2ULL + half_part_size) - 1ULL;

        const uint64_t part_ranges[] = {
            object_range_start,
            object_range_start + half_part_size - 1,

            object_range_start + half_part_size,
            object_range_start + half_part_size + (uint64_t)part_size - 1ULL,

            object_range_start + half_part_size + (uint64_t)part_size,
            object_range_start + half_part_size + (uint64_t)part_size * 2ULL - 1ULL,

            object_range_start + half_part_size + (uint64_t)part_size * 2ULL,
            object_range_start + half_part_size + (uint64_t)part_size * 2ULL + half_part_size - 1ULL,
        };

        ASSERT_TRUE(aws_s3_get_num_parts(part_size, object_range_start, object_range_end) == expected_num_parts);

        ASSERT_SUCCESS(
            s_validate_part_ranges(object_range_start, object_range_end, part_size, expected_num_parts, part_ranges));
    }

    /* 1 byte range corner case. */
    {
        const uint32_t expected_num_parts = 1;
        const uint64_t object_range_start = 8;
        const uint64_t object_range_end = 8;

        const uint64_t part_ranges[] = {8, 8};

        ASSERT_TRUE(aws_s3_get_num_parts(part_size, object_range_start, object_range_end) == expected_num_parts);

        ASSERT_SUCCESS(
            s_validate_part_ranges(object_range_start, object_range_end, part_size, expected_num_parts, part_ranges));
    }

    return 0;
}

struct s3_request_part_config_example {
    const char *name;
    uint64_t content_length;
    size_t client_part_size;
    uint64_t client_max_part_size;
    size_t expected_part_size;
    uint32_t expected_num_parts;
};

AWS_TEST_CASE(test_s3_mpu_get_part_size_and_num_parts, s_test_s3_mpu_get_part_size_and_num_parts)
static int s_test_s3_mpu_get_part_size_and_num_parts(struct aws_allocator *allocator, void *ctx) {
    (void)allocator;
    (void)ctx;
    uint64_t default_max_part_size = 5368709120ULL;

    const struct s3_request_part_config_example valid_request_part_config[] = {
        {
            .name = "simple case",
            .content_length = MB_TO_BYTES((uint64_t)10000),
            .client_part_size = MB_TO_BYTES(5),
            .client_max_part_size = default_max_part_size,
            .expected_part_size = 5242880,
            .expected_num_parts = 2000,
        },
        {
            .name = "large content length with small part size",
            .content_length = MB_TO_BYTES((uint64_t)990000),
            .client_part_size = MB_TO_BYTES(5),
            .client_max_part_size = default_max_part_size,
            .expected_part_size = 103809024,
            .expected_num_parts = 10000,
        },
        {

            .name = "large content length with large part size",
            .content_length = MB_TO_BYTES((uint64_t)1000000),
            .client_part_size = MB_TO_BYTES(500),
            .client_max_part_size = default_max_part_size,
            .expected_part_size = MB_TO_BYTES(500),
            .expected_num_parts = 2000,
        },
        {
            .name = "large odd content length",
            .content_length = 1044013645824,
            .client_part_size = 5242880,
            .client_max_part_size = default_max_part_size,
            .expected_part_size = 104401365,
            .expected_num_parts = 10000,
        },
        {
            .name = "10k parts",
            .content_length = MB_TO_BYTES((uint64_t)50000),
            .client_part_size = MB_TO_BYTES(5),
            .client_max_part_size = default_max_part_size,
            .expected_part_size = MB_TO_BYTES(5),
            .expected_num_parts = 10000,
        },
        {
            .name = "10k - 1 parts",
            .content_length = 49995,
            .client_part_size = 5,
            .client_max_part_size = default_max_part_size,
            .expected_part_size = 5,
            .expected_num_parts = 9999,
        },
        {
            .name = "10k with small last part",
            .content_length = 49998,
            .client_part_size = 5,
            .client_max_part_size = default_max_part_size,
            .expected_part_size = 5,
            .expected_num_parts = 10000,
        },
        {
            .name = "10k + 1 parts",
            .content_length = 50001,
            .client_part_size = 5,
            .client_max_part_size = default_max_part_size,
            .expected_part_size = 6,
            .expected_num_parts = 8334,

        },
        {
            .name = "bump content length",
            .content_length = 100000,
            .client_part_size = 5,
            .client_max_part_size = default_max_part_size,
            .expected_part_size = 10,
            .expected_num_parts = 10000,
        },
        {
            .name = "bump content length with non-zero mod",
            .content_length = 999999,
            .client_part_size = 5,
            .client_max_part_size = default_max_part_size,
            .expected_part_size = 100,
            .expected_num_parts = 10000,
        },
        {
            .name = "5 tb content length",
            .content_length = MB_TO_BYTES((uint64_t)5 * 1024 * 1024),
            .client_part_size = MB_TO_BYTES((uint64_t)5),
            .client_max_part_size = default_max_part_size,
            .expected_part_size = 549755814,
            .expected_num_parts = 10000,
        },
    };
    for (size_t i = 0; i < AWS_ARRAY_SIZE(valid_request_part_config); ++i) {
        AWS_LOGF_INFO(AWS_LS_S3_GENERAL, "valid example [%zu]: %s\n", i, valid_request_part_config[i].name);

        uint64_t content_length = valid_request_part_config[i].content_length;
        size_t part_size;
        uint32_t num_parts;

        ASSERT_SUCCESS(aws_s3_calculate_optimal_mpu_part_size_and_num_parts(
            content_length,
            valid_request_part_config[i].client_part_size,
            valid_request_part_config[i].client_max_part_size,
            &part_size,
            &num_parts));
        ASSERT_INT_EQUALS(valid_request_part_config[i].expected_part_size, part_size);
        ASSERT_INT_EQUALS(valid_request_part_config[i].expected_num_parts, num_parts);
    }

    /* Invalid cases */
    const struct s3_request_part_config_example invalid_request_part_config[] = {{
        .name = "max part < required part size",
        .content_length = 900000,
        .client_part_size = 5,
        .client_max_part_size = 10,
    }};

    for (size_t i = 0; i < AWS_ARRAY_SIZE(invalid_request_part_config); ++i) {
        printf("invalid example [%zu]: %s\n", i, invalid_request_part_config[i].name);
        size_t part_size;
        uint32_t num_parts;
        ASSERT_FAILS(aws_s3_calculate_optimal_mpu_part_size_and_num_parts(
            invalid_request_part_config[i].content_length,
            invalid_request_part_config[i].client_part_size,
            invalid_request_part_config[i].client_max_part_size,
            &part_size,
            &num_parts));
    }
    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE(test_s3_aws_xml_get_body_at_path, s_test_s3_aws_xml_get_body_at_path)
static int s_test_s3_aws_xml_get_body_at_path(struct aws_allocator *allocator, void *ctx) {
    (void)allocator;
    (void)ctx;

    struct aws_byte_cursor example_error_body = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        "<Error>\n"
        "<Code>AccessDenied</Code>\n"
        "<Message>Access Denied</Message>\n"
        "<RequestId>656c76696e6727732072657175657374</RequestId>\n"
        "<HostId>Uuag1LuByRx9e6j5Onimru9pO4ZVKnJ2Qz7/C1NPcfTWAtRPfTaOFg==</HostId>\n"
        "</Error>");

    /* Ensure we can successfully look up <Error><Code> */
    {
        struct aws_byte_cursor error_code = {0};
        const char *xml_path[] = {"Error", "Code", NULL};

        ASSERT_SUCCESS(aws_xml_get_body_at_path(allocator, example_error_body, xml_path, &error_code));
        ASSERT_CURSOR_VALUE_CSTRING_EQUALS(error_code, "AccessDenied");
    }

    /* Ensure we fail if the beginning of the path doesn't match */
    {
        struct aws_byte_cursor error_code = {0};
        const char *xml_path[] = {"ObviouslyInvalidName", "Code", NULL};
        ASSERT_ERROR(
            AWS_ERROR_STRING_MATCH_NOT_FOUND,
            aws_xml_get_body_at_path(allocator, example_error_body, xml_path, &error_code));
    }

    /* Ensure we fail if the end of the path doesn't match */
    {
        struct aws_byte_cursor error_code = {0};
        const char *xml_path[] = {"Error", "ObviouslyInvalidName", NULL};
        ASSERT_ERROR(
            AWS_ERROR_STRING_MATCH_NOT_FOUND,
            aws_xml_get_body_at_path(allocator, example_error_body, xml_path, &error_code));
    }

    /* Ensure we fail if the document isn't valid XML */
    {
        struct aws_byte_cursor error_code = {0};
        const char *xml_path[] = {"Error", "Code", NULL};
        ASSERT_ERROR(
            AWS_ERROR_INVALID_XML,
            aws_xml_get_body_at_path(
                allocator, aws_byte_cursor_from_c_str("Obviously invalid XML document"), xml_path, &error_code));
    }
    return AWS_OP_SUCCESS;
}
