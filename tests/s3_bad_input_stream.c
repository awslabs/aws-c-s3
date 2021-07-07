#include "s3_tester.h"
#include <aws/io/stream.h>

struct aws_s3_bad_input_stream_impl {
    size_t length;
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
    struct aws_s3_bad_input_stream_impl *bad_input_stream = stream->impl;
    *out_length = (int64_t)bad_input_stream->length;
    return AWS_OP_SUCCESS;
}

static void s_aws_s3_bad_input_stream_destroy(struct aws_input_stream *stream) {
    (void)stream;
    aws_mem_release(stream->allocator, stream);
}

static struct aws_input_stream_vtable s_aws_s3_bad_input_stream_vtable = {
    .seek = s_aws_s3_bad_input_stream_seek,
    .read = s_aws_s3_bad_input_stream_read,
    .get_status = s_aws_s3_bad_input_stream_get_status,
    .get_length = s_aws_s3_bad_input_stream_get_length,
    .destroy = s_aws_s3_bad_input_stream_destroy,
};

struct aws_input_stream *aws_s3_bad_input_stream_new(struct aws_allocator *allocator, size_t stream_length) {
    AWS_ASSERT(allocator);

    struct aws_input_stream *input_stream = NULL;
    struct aws_s3_bad_input_stream_impl *bad_input_stream = NULL;

    aws_mem_acquire_many(
        allocator,
        2,
        &input_stream,
        sizeof(struct aws_input_stream),
        &bad_input_stream,
        sizeof(struct aws_s3_bad_input_stream_impl));

    input_stream->allocator = allocator;
    input_stream->vtable = &s_aws_s3_bad_input_stream_vtable;
    input_stream->impl = bad_input_stream;

    bad_input_stream->length = stream_length;

    return input_stream;
}
