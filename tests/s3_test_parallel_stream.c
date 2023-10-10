/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_parallel_input_stream.h"
#include "s3_tester.h"
#include <aws/common/atomics.h>

struct aws_parallel_input_stream_from_file_failure_impl {
    struct aws_parallel_input_stream base;

    struct aws_atomic_var number_read;
};

static void s_para_from_file_failure_destroy(struct aws_parallel_input_stream *stream) {
    struct aws_parallel_input_stream_from_file_failure_impl *impl = stream->impl;

    aws_mem_release(stream->alloc, impl);
}

struct aws_future_bool *s_para_from_file_failure_read(
    struct aws_parallel_input_stream *stream,
    uint64_t offset,
    struct aws_byte_buf *dest) {
    (void)offset;

    struct aws_future_bool *future = aws_future_bool_new(stream->alloc);
    struct aws_parallel_input_stream_from_file_failure_impl *impl = stream->impl;
    size_t previous_number_read = aws_atomic_fetch_add(&impl->number_read, 1);
    if (previous_number_read == 1) {
        /* TODO: make the failure configurable */
        aws_future_bool_set_error(future, AWS_ERROR_UNIMPLEMENTED);
    } else {

        struct aws_byte_cursor test_string = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("This is an S3 test.");
        while (dest->len < dest->capacity) {
            size_t remaining_in_buffer = dest->capacity - dest->len;
            if (remaining_in_buffer < test_string.len) {
                test_string.len = remaining_in_buffer;
            }
            aws_byte_buf_append(dest, &test_string);
        }
        aws_future_bool_set_result(future, false);
    }
    return future;
}

static struct aws_parallel_input_stream_vtable s_parallel_input_stream_from_file_failure_vtable = {
    .destroy = s_para_from_file_failure_destroy,
    .read = s_para_from_file_failure_read,
};

struct aws_parallel_input_stream *aws_parallel_input_stream_new_from_file_failure_tester(
    struct aws_allocator *allocator,
    struct aws_byte_cursor file_name) {
    (void)file_name;

    struct aws_parallel_input_stream_from_file_failure_impl *impl =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_parallel_input_stream_from_file_failure_impl));
    aws_parallel_input_stream_init_base(
        &impl->base, allocator, &s_parallel_input_stream_from_file_failure_vtable, impl);

    aws_atomic_init_int(&impl->number_read, 0);
    return &impl->base;
}
