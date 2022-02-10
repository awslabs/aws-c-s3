#include "s3_tester.h"
#include <aws/io/stream.h>

struct aws_s3_bad_input_stream_impl {
    size_t length;
    struct aws_allocator *allocator;
};

static int s_aws_s3_bad_input_stream_seek(
    struct aws_input_stream *stream,
    aws_off_t offset,
    enum aws_stream_seek_basis basis) {
    (void)stream;
    (void)offset;
    (void)basis;
    aws_raise_error(AWS_ERROR_UNKNOWN);
    return AWS_OP_ERR;
}

static int s_aws_s3_bad_input_stream_read(struct aws_input_stream *stream, struct aws_byte_buf *dest) {
    (void)stream;
    (void)dest;
    aws_raise_error(AWS_IO_STREAM_READ_FAILED);
    return AWS_OP_ERR;
}

static int s_aws_s3_bad_input_stream_get_status(struct aws_input_stream *stream, struct aws_stream_status *status) {
    (void)stream;
    (void)status;
    aws_raise_error(AWS_ERROR_UNKNOWN);
    return AWS_OP_ERR;
}

static int s_aws_s3_bad_input_stream_get_length(struct aws_input_stream *stream, int64_t *out_length) {
    AWS_ASSERT(stream != NULL);
    struct aws_s3_bad_input_stream_impl *bad_input_stream = aws_input_stream_get_impl(stream);
    *out_length = (int64_t)bad_input_stream->length;
    return AWS_OP_SUCCESS;
}

static void s_aws_s3_bad_input_stream_destroy(struct aws_input_stream *stream) {
    (void)stream;
    struct aws_s3_bad_input_stream_impl *bad_input_stream = aws_input_stream_get_impl(stream);
    aws_mem_release(bad_input_stream->allocator, bad_input_stream);
}

static struct aws_input_stream_vtable s_aws_s3_bad_input_stream_vtable = {
    .seek = s_aws_s3_bad_input_stream_seek,
    .read = s_aws_s3_bad_input_stream_read,
    .get_status = s_aws_s3_bad_input_stream_get_status,
    .get_length = s_aws_s3_bad_input_stream_get_length,
    .impl_destroy = s_aws_s3_bad_input_stream_destroy,
};

struct aws_input_stream *aws_s3_bad_input_stream_new(struct aws_allocator *allocator, size_t stream_length) {
    AWS_ASSERT(allocator);

    struct aws_s3_bad_input_stream_impl *bad_input_stream =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_bad_input_stream_impl));

    struct aws_input_stream_options options = {
        .allocator = allocator,
        .vtable = &s_aws_s3_bad_input_stream_vtable,
        .impl = bad_input_stream,
    };

    struct aws_input_stream *input_stream = aws_input_stream_new(&options);

    bad_input_stream->length = stream_length;

    return input_stream;
}
