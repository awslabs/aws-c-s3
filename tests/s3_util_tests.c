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

struct replace_quote_entities_test_case {
    struct aws_string *test_string;
    struct aws_byte_cursor expected_result;
};

AWS_TEST_CASE(test_s3_replace_quote_entities, s_test_s3_replace_quote_entities)
static int s_test_s3_replace_quote_entities(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct replace_quote_entities_test_case test_cases[] = {
        {
            .test_string = aws_string_new_from_c_str(allocator, "&quot;testtest"),
            .expected_result = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("\"testtest"),
        },
        {
            .test_string = aws_string_new_from_c_str(allocator, "testtest&quot;"),
            .expected_result = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("testtest\""),
        },
        {
            .test_string = aws_string_new_from_c_str(allocator, "&quot;&quot;"),
            .expected_result = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("\"\""),
        },
        {
            .test_string = aws_string_new_from_c_str(allocator, "testtest"),
            .expected_result = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("testtest"),
        },
        {
            .test_string = aws_string_new_from_c_str(allocator, ""),
            .expected_result = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL(""),
        },
    };

    for (size_t i = 0; i < (sizeof(test_cases) / sizeof(struct replace_quote_entities_test_case)); ++i) {
        struct replace_quote_entities_test_case *test_case = &test_cases[i];

        struct aws_byte_buf result_byte_buf;
        AWS_ZERO_STRUCT(result_byte_buf);

        replace_quote_entities(allocator, test_case->test_string, &result_byte_buf);

        struct aws_byte_cursor result_byte_cursor = aws_byte_cursor_from_buf(&result_byte_buf);

        ASSERT_TRUE(aws_byte_cursor_eq(&test_case->expected_result, &result_byte_cursor));

        aws_byte_buf_clean_up(&result_byte_buf);
        aws_string_destroy(test_case->test_string);
        test_case->test_string = NULL;
    }

    aws_s3_tester_clean_up(&tester);

    return 0;
}

struct ranged_header_test_case {
    struct aws_byte_cursor header_value;
    struct aws_s3_range_header_values expected_values;
    int expected_error;
};

AWS_TEST_CASE(test_s3_ranged_header_parsing, s_test_s3_ranged_header_parsing)
static int s_test_s3_ranged_header_parsing(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;

    struct aws_s3_tester tester;
    AWS_ZERO_STRUCT(tester);
    ASSERT_SUCCESS(aws_s3_tester_init(allocator, &tester));

    struct ranged_header_test_case test_cases[] = {
        {
            .header_value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("bytes=123-456"),
            .expected_values =
                {
                    .range_start = 123,
                    .range_end = 456,
                    .range_start_found = true,
                    .range_end_found = true,
                },
        },
        {
            .header_value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("bytes=123-"),
            .expected_values =
                {
                    .range_start = 123,
                    .range_start_found = true,
                },
        },
        {
            .header_value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("bytes=-123"),
            .expected_values =
                {
                    .range_suffix = 123,
                    .range_suffix_found = true,
                },
        },
        {
            .header_value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("invalid=123"),
            .expected_error = AWS_ERROR_S3_INVALID_RANGE_HEADER,
        },
        {
            .header_value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("bytes=-123-35"),
            .expected_error = AWS_ERROR_S3_INVALID_RANGE_HEADER,
        },
        {
            .header_value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("bytes=123-,"),
            .expected_error = AWS_ERROR_S3_MULTIRANGE_HEADER_UNSUPPORTED,
        },
        {
            .header_value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("bytes=123-, 456-789"),
            .expected_error = AWS_ERROR_S3_MULTIRANGE_HEADER_UNSUPPORTED,
        },
    };

    for (size_t i = 0; i < sizeof(test_cases) / sizeof(test_cases[0]); ++i) {
        struct aws_s3_range_header_values *expected_values = &test_cases[i].expected_values;

        struct aws_s3_range_header_values values;
        int parse_result = aws_s3_parse_range_header_value(allocator, &test_cases[i].header_value, &values);

        if (test_cases[i].expected_error == 0) {
            ASSERT_SUCCESS(parse_result);

            ASSERT_TRUE(values.range_start == expected_values->range_start);
            ASSERT_TRUE(values.range_end == expected_values->range_end);
            ASSERT_TRUE(values.range_suffix == expected_values->range_suffix);
            ASSERT_TRUE(values.range_start_found == expected_values->range_start_found);
            ASSERT_TRUE(values.range_end_found == expected_values->range_end_found);
            ASSERT_TRUE(values.range_suffix_found == expected_values->range_suffix_found);

        } else {
            ASSERT_FAILS(parse_result);

            ASSERT_TRUE(aws_last_error() == test_cases[i].expected_error);
        }
    }

    aws_s3_tester_clean_up(&tester);

    return 0;
}
