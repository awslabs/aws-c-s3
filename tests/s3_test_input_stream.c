#include "s3_tester.h"
#include <aws/io/stream.h>

struct aws_s3_test_input_stream_impl {
    size_t position;
    size_t length;
};

#define MAX_TEST_STRINGS 65536

struct aws_s3_test_input_stream_global {
    size_t test_string_size;
    struct aws_byte_cursor test_string_cursors[MAX_TEST_STRINGS];
    struct aws_byte_buf test_strings_buffer;
};

struct aws_s3_test_input_stream_global g_s3_test_input_stream_global;

void aws_s3_init_test_input_stream_look_up(struct aws_s3_tester *tester) {
    AWS_ASSERT(tester);
    AWS_ASSERT(MAX_TEST_STRINGS > 0);

    srand((unsigned int)time(NULL));

    uint32_t num_digits = 0;
    uint32_t max_contents_buffers_temp = MAX_TEST_STRINGS;

    while (max_contents_buffers_temp > 0) {
        ++num_digits;
        max_contents_buffers_temp /= 10;
    }

    char format_string[64];
    snprintf(format_string, sizeof(format_string), "This is is an S3 test. %%%uu ", num_digits);

    char test_format_string[64];
    snprintf(test_format_string, sizeof(test_format_string), format_string, 0);

    g_s3_test_input_stream_global.test_string_size = strlen(test_format_string);

    aws_byte_buf_init(
        &g_s3_test_input_stream_global.test_strings_buffer,
        tester->allocator,
        g_s3_test_input_stream_global.test_string_size * MAX_TEST_STRINGS);

    for (size_t i = 0; i < MAX_TEST_STRINGS; ++i) {
        char current_string_buffer[64] = "";

        sprintf(current_string_buffer, format_string, rand() % MAX_TEST_STRINGS);

        AWS_ASSERT(strlen(current_string_buffer) == g_s3_test_input_stream_global.test_string_size);

        struct aws_byte_cursor current_string_cursor = {
            .ptr = (uint8_t *)current_string_buffer,
            .len = g_s3_test_input_stream_global.test_string_size,
        };

        aws_byte_buf_append(&g_s3_test_input_stream_global.test_strings_buffer, &current_string_cursor);

        struct aws_byte_cursor *test_string_cursor = &g_s3_test_input_stream_global.test_string_cursors[i];
        test_string_cursor->ptr = g_s3_test_input_stream_global.test_strings_buffer.buffer +
                                  g_s3_test_input_stream_global.test_strings_buffer.len - current_string_cursor.len;
        test_string_cursor->len = current_string_cursor.len;
    }
}

void aws_s3_destroy_test_input_stream_look_up(struct aws_s3_tester *tester) {
    aws_byte_buf_clean_up(&g_s3_test_input_stream_global.test_strings_buffer);
}

static int s_aws_s3_test_input_stream_seek(
    struct aws_input_stream *stream,
    aws_off_t offset,
    enum aws_stream_seek_basis basis) {
    (void)stream;
    (void)offset;
    (void)basis;

    /* Stream should never be seeked; all reads should be sequential. */
    aws_raise_error(AWS_ERROR_UNKNOWN);
    return AWS_OP_ERR;
}

static int s_aws_s3_test_input_stream_read(struct aws_input_stream *stream, struct aws_byte_buf *dest) {
    (void)stream;
    (void)dest;

    struct aws_s3_test_input_stream_impl *test_input_stream = stream->impl;

    if (dest->capacity > (test_input_stream->length - test_input_stream->position)) {
        aws_raise_error(AWS_IO_STREAM_READ_FAILED);
        return AWS_OP_ERR;
    }

    while (dest->len < dest->capacity) {
        uint32_t test_string_index = (uint32_t)(
            (test_input_stream->position / g_s3_test_input_stream_global.test_string_size) %
            (uint64_t)MAX_TEST_STRINGS);

        struct aws_byte_cursor *test_string = &g_s3_test_input_stream_global.test_string_cursors[test_string_index];
        size_t buffer_pos = test_input_stream->position % test_string->len;

        struct aws_byte_cursor source_byte_cursor = {
            .len = test_string->len - buffer_pos,
            .ptr = test_string->ptr + buffer_pos,
        };

        size_t remaining_in_buffer = dest->capacity - dest->len;

        if (remaining_in_buffer < source_byte_cursor.len) {
            source_byte_cursor.len = remaining_in_buffer;
        }

        aws_byte_buf_append(dest, &source_byte_cursor);
        buffer_pos += source_byte_cursor.len;

        test_input_stream->position += source_byte_cursor.len;
    }

    return AWS_OP_SUCCESS;
}

static int s_aws_s3_test_input_stream_get_status(struct aws_input_stream *stream, struct aws_stream_status *status) {
    (void)stream;
    (void)status;

    struct aws_s3_test_input_stream_impl *test_input_stream = stream->impl;

    status->is_end_of_stream = test_input_stream->position == test_input_stream->length;
    status->is_valid = true;

    return AWS_OP_SUCCESS;
}

static int s_aws_s3_test_input_stream_get_length(struct aws_input_stream *stream, int64_t *out_length) {
    AWS_ASSERT(stream != NULL);
    struct aws_s3_test_input_stream_impl *test_input_stream = stream->impl;
    *out_length = (int64_t)test_input_stream->length;
    return AWS_OP_SUCCESS;
}

static void s_aws_s3_test_input_stream_destroy(struct aws_input_stream *stream) {
    (void)stream;
    aws_mem_release(stream->allocator, stream);
}

static struct aws_input_stream_vtable s_aws_s3_test_input_stream_vtable = {
    .seek = s_aws_s3_test_input_stream_seek,
    .read = s_aws_s3_test_input_stream_read,
    .get_status = s_aws_s3_test_input_stream_get_status,
    .get_length = s_aws_s3_test_input_stream_get_length,
    .destroy = s_aws_s3_test_input_stream_destroy,
};

struct aws_input_stream *aws_s3_test_input_stream_new(struct aws_allocator *allocator, size_t stream_length) {
    AWS_ASSERT(allocator);

    struct aws_input_stream *input_stream = NULL;
    struct aws_s3_test_input_stream_impl *test_input_stream = NULL;

    aws_mem_acquire_many(
        allocator,
        2,
        &input_stream,
        sizeof(struct aws_input_stream),
        &test_input_stream,
        sizeof(struct aws_s3_test_input_stream_impl));

    input_stream->allocator = allocator;
    input_stream->vtable = &s_aws_s3_test_input_stream_vtable;
    input_stream->impl = test_input_stream;

    test_input_stream->position = 0;
    test_input_stream->length = stream_length;

    return input_stream;
}
