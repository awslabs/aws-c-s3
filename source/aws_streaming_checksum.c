/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/cal/hash.h>
#include <aws/s3/private/s3_checksum.h>
#include <aws/s3/s3_streaming_checksum.h>

void aws_checksum_destroy(struct aws_checksum *checksum) {
    checksum->vtable->destroy(checksum);
}

int aws_checksum_update(struct aws_checksum *checksum, const struct aws_byte_cursor *buffer) {
    return checksum->vtable->update(checksum, buffer);
}

int aws_checksum_finalize(struct aws_checksum *checksum, struct aws_byte_buf *output, size_t truncate_to) {

    if (truncate_to && truncate_to < checksum->digest_size) {
        size_t available_buffer = output->capacity - output->len;
        if (available_buffer < truncate_to) {
            return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
        }

        uint8_t tmp_output[128] = {0};
        AWS_ASSERT(sizeof(tmp_output) >= checksum->digest_size);

        struct aws_byte_buf tmp_out_buf = aws_byte_buf_from_array(tmp_output, sizeof(tmp_output));
        tmp_out_buf.len = 0;

        if (checksum->vtable->finalize(checksum, &tmp_out_buf)) {
            return AWS_OP_ERR;
        }

        memcpy(output->buffer + output->len, tmp_output, truncate_to);
        output->len += truncate_to;
        return AWS_OP_SUCCESS;
    }

    return checksum->vtable->finalize(checksum, output);
}

int compute_checksum(
    struct aws_checksum *checksum,
    const struct aws_byte_cursor *input,
    struct aws_byte_buf *output,
    size_t truncate_to) {
    if (!checksum) {
        return AWS_OP_ERR;
    }

    if (aws_checksum_update(checksum, input)) {
        aws_checksum_destroy(checksum);
        return AWS_OP_ERR;
    }

    if (aws_checksum_finalize(checksum, output, truncate_to)) {
        aws_checksum_destroy(checksum);
        return AWS_OP_ERR;
    }

    aws_checksum_destroy(checksum);
    return AWS_OP_SUCCESS;
}
