#include "s3_tester.h"
#include <aws/io/stream.h>

struct aws_s3_test_input_stream_impl {
    struct aws_input_stream base;
    size_t position;
    size_t length;
    struct aws_allocator *allocator;
};

static int s_aws_s3_test_input_stream_seek(
    struct aws_input_stream *stream,
    int64_t offset,
    enum aws_stream_seek_basis basis) {
    (void)stream;
    (void)offset;
    (void)basis;

    /* Stream should never be seeked; all reads should be sequential. */
    aws_raise_error(AWS_ERROR_UNKNOWN);
    return AWS_OP_ERR;
}

static int s_aws_s3_test_input_stream_read(
    struct aws_input_stream *stream,
    struct aws_byte_buf *dest,
    struct aws_byte_cursor *test_string) {
    (void)stream;
    (void)dest;

    struct aws_s3_test_input_stream_impl *test_input_stream =
        AWS_CONTAINER_OF(stream, struct aws_s3_test_input_stream_impl, base);

    while (dest->len < dest->capacity && test_input_stream->position < test_input_stream->length) {
        size_t buffer_pos = test_input_stream->position % test_string->len;

        struct aws_byte_cursor source_byte_cursor = {
            .len = test_string->len - buffer_pos,
            .ptr = test_string->ptr + buffer_pos,
        };

        size_t remaining_in_stream = test_input_stream->length - test_input_stream->position;
        if (remaining_in_stream < source_byte_cursor.len) {
            source_byte_cursor.len = remaining_in_stream;
        }

        size_t remaining_in_buffer = dest->capacity - dest->len;

        if (remaining_in_buffer < source_byte_cursor.len) {
            source_byte_cursor.len = remaining_in_buffer;
        }

        aws_byte_buf_append(dest, &source_byte_cursor);

        test_input_stream->position += source_byte_cursor.len;
    }

    return AWS_OP_SUCCESS;
}

static int s_aws_s3_test_input_stream_read_1(struct aws_input_stream *stream, struct aws_byte_buf *dest) {
    struct aws_byte_cursor test_string = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("This is an S3 test.");
    return s_aws_s3_test_input_stream_read(stream, dest, &test_string);
}

static int s_aws_s3_test_input_stream_read_2(struct aws_input_stream *stream, struct aws_byte_buf *dest) {
    struct aws_byte_cursor test_string = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Different S3 test value.");
    return s_aws_s3_test_input_stream_read(stream, dest, &test_string);
}

static int s_aws_s3_test_input_stream_get_status(struct aws_input_stream *stream, struct aws_stream_status *status) {
    (void)stream;
    (void)status;

    struct aws_s3_test_input_stream_impl *test_input_stream =
        AWS_CONTAINER_OF(stream, struct aws_s3_test_input_stream_impl, base);

    status->is_end_of_stream = test_input_stream->position == test_input_stream->length;
    status->is_valid = true;

    return AWS_OP_SUCCESS;
}

static int s_aws_s3_test_input_stream_get_length(struct aws_input_stream *stream, int64_t *out_length) {
    AWS_ASSERT(stream != NULL);
    struct aws_s3_test_input_stream_impl *test_input_stream =
        AWS_CONTAINER_OF(stream, struct aws_s3_test_input_stream_impl, base);
    *out_length = (int64_t)test_input_stream->length;
    return AWS_OP_SUCCESS;
}

static void s_aws_s3_test_input_stream_destroy(struct aws_s3_test_input_stream_impl *test_input_stream) {
    aws_mem_release(test_input_stream->allocator, test_input_stream);
}

static struct aws_input_stream_vtable s_aws_s3_test_input_stream_vtable_1 = {
    .seek = s_aws_s3_test_input_stream_seek,
    .read = s_aws_s3_test_input_stream_read_1,
    .get_status = s_aws_s3_test_input_stream_get_status,
    .get_length = s_aws_s3_test_input_stream_get_length,
};

static struct aws_input_stream_vtable s_aws_s3_test_input_stream_vtable_2 = {
    .seek = s_aws_s3_test_input_stream_seek,
    .read = s_aws_s3_test_input_stream_read_2,
    .get_status = s_aws_s3_test_input_stream_get_status,
    .get_length = s_aws_s3_test_input_stream_get_length,
};

struct aws_input_stream *aws_s3_test_input_stream_new_with_value_type(
    struct aws_allocator *allocator,
    size_t stream_length,
    enum aws_s3_test_stream_value stream_value) {

    struct aws_s3_test_input_stream_impl *test_input_stream =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_test_input_stream_impl));

    test_input_stream->base.vtable = stream_value == TEST_STREAM_VALUE_1 ? &s_aws_s3_test_input_stream_vtable_1
                                                                         : &s_aws_s3_test_input_stream_vtable_2;

    aws_ref_count_init(
        &test_input_stream->base.ref_count,
        test_input_stream,
        (aws_simple_completion_callback *)s_aws_s3_test_input_stream_destroy);

    struct aws_input_stream *input_stream = &test_input_stream->base;

    test_input_stream->position = 0;
    test_input_stream->length = stream_length;
    test_input_stream->allocator = allocator;

    return input_stream;
}

struct aws_input_stream *aws_s3_test_input_stream_new(struct aws_allocator *allocator, size_t stream_length) {
    return aws_s3_test_input_stream_new_with_value_type(allocator, stream_length, TEST_STREAM_VALUE_1);
}
