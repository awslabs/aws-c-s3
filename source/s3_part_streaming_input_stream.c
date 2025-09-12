/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/s3/private/s3_meta_request_impl.h>
#include <aws/s3/private/s3_parallel_input_stream.h>
#include <aws/s3/private/s3_part_streaming_input_stream.h>
#include <aws/s3/private/s3_util.h>

#include <aws/common/atomics.h>
#include <aws/common/clock.h>
#include <aws/common/file.h>
#include <aws/common/logging.h>
#include <aws/common/string.h>
#include <aws/common/system_info.h>
#include <aws/common/task_scheduler.h>
#include <aws/common/thread.h>

#include <aws/io/event_loop.h>
#include <aws/io/future.h>
#include <aws/io/stream.h>

#include <inttypes.h>

/* 60 secs. */
static const uint64_t s_max_timeout_ns = 60 * (uint64_t)AWS_TIMESTAMP_NANOS;

struct aws_s3_part_streaming_input_stream_impl {
    struct aws_input_stream base;
    struct aws_allocator *allocator;

    struct aws_s3_buffer_ticket *ticket;
    struct aws_parallel_input_stream *para_stream;

    /* Settings won't be changed during read. */
    size_t page_size;
    uint64_t offset;
    size_t total_length;
    size_t chunk_load_size;

    /* The reading counters */
    /* Incase the `offset` is not aligned with page size. The offset - page_aligned_offset will be aligned the page size
     * to load the chunk. And read from the chunk can start from this `page_aligned_offset` */
    size_t page_aligned_offset;
    /* The offset of the chunk in the `reading_chunk_buf` that will start reading. */
    size_t in_chunk_offset;
    /* So far, how many bytes has been read */
    size_t total_length_read;

    /* The pointer to the buffer that is reading from */
    struct aws_byte_buf *reading_chunk_buf;
    /* The pointer to the buffer that is loading in the background. And will be the next chunk to read from. */
    struct aws_byte_buf *loading_chunk_buf;
    /* The future when the loading_chunk_buf is read. */
    struct aws_future_bool *loading_future;

    /* Stores two buffers for reading and loading. */
    struct aws_byte_buf chunk_buf_1;
    struct aws_byte_buf chunk_buf_2;

    /* End of stream has been load from the `para_stream` */
    bool eos_loaded_from_para_stream;
    /* End of stream reached for this input stream */
    bool eos_reached;
};

static int s_part_streaming_input_stream_seek(
    struct aws_input_stream *stream,
    int64_t offset,
    enum aws_stream_seek_basis basis) {
    (void)stream;
    (void)offset;
    (void)basis;

    AWS_LOGF_ERROR(AWS_LS_S3_GENERAL, "Seek operation not supported on part streaming input stream");

    return aws_raise_error(AWS_ERROR_UNSUPPORTED_OPERATION);
}

static void s_kick_off_next_load(struct aws_s3_part_streaming_input_stream_impl *impl) {

    size_t length_after_chunk_read = impl->total_length_read + (impl->reading_chunk_buf->len - impl->in_chunk_offset);

    uint64_t new_offset = impl->offset + length_after_chunk_read;
    impl->page_aligned_offset = new_offset % impl->page_size;
    new_offset -= impl->page_aligned_offset;

    size_t remaining_length = impl->total_length - length_after_chunk_read + impl->page_aligned_offset;

    if (remaining_length > 0 && !impl->eos_loaded_from_para_stream) {
        /* Align the remaining length with the page size. */
        if (remaining_length < impl->chunk_load_size) {
            size_t aligned_remaining_length = remaining_length % impl->page_size;
            /* Read more tha needed to align with the page size. */
            if (aligned_remaining_length > 0) {
                remaining_length = remaining_length + impl->page_size - aligned_remaining_length;
                AWS_LOGF_TRACE(
                    AWS_LS_S3_GENERAL,
                    "id=%p: Aligned remaining length to page boundary: %zu bytes",
                    (void *)impl,
                    remaining_length);
            }
        }

        size_t load_size = aws_min_size(remaining_length, impl->chunk_load_size);
        AWS_LOGF_TRACE(
            AWS_LS_S3_GENERAL,
            "id=%p: Starting background load - offset=%" PRIu64 ", size=%zu",
            (void *)impl,
            new_offset,
            load_size);

        /* Kick off loading the next chunk. */
        impl->loading_future =
            aws_parallel_input_stream_read(impl->para_stream, new_offset, load_size, impl->loading_chunk_buf);
    }
}

/* The input stream read can only be invoked serially. No concurrent read supported. */
static int s_part_streaming_input_stream_read(struct aws_input_stream *stream, struct aws_byte_buf *dest) {
    struct aws_s3_part_streaming_input_stream_impl *impl =
        AWS_CONTAINER_OF(stream, struct aws_s3_part_streaming_input_stream_impl, base);
    size_t read_length = aws_min_size(dest->capacity - dest->len, impl->total_length - impl->total_length_read);

    if (impl->reading_chunk_buf->len == 0) {
        /* The reading buf is not ready. Block until the loading buf is available. */
        if (impl->loading_future == NULL) {
            /* Nothing to read */
            AWS_LOGF_TRACE(AWS_LS_S3_GENERAL, "id=%p: Nothing to read, EOS reached", (void *)impl);
            AWS_ASSERT(impl->eos_reached);
            return AWS_OP_SUCCESS;
        }

        AWS_LOGF_TRACE(AWS_LS_S3_GENERAL, "id=%p: Waiting for background load to complete", (void *)impl);
        /* TODO: the HTTP interface doesn't support async streaming, we have to block the thread here. */
        if (!aws_future_bool_wait(impl->loading_future, s_max_timeout_ns)) {
            /* Timeout */
            AWS_LOGF_ERROR(AWS_LS_S3_GENERAL, "id=%p: Timeout waiting for background load", (void *)impl);
            return AWS_OP_ERR;
        }

        int read_error = aws_future_bool_get_error(impl->loading_future);
        if (read_error != AWS_ERROR_SUCCESS) {
            /* Read failed. */
            AWS_LOGF_ERROR(
                AWS_LS_S3_GENERAL,
                "id=%p: Background load failed with error %s",
                (void *)impl,
                aws_error_debug_str(read_error));
            return aws_raise_error(read_error);
        }

        impl->eos_loaded_from_para_stream = aws_future_bool_get_result(impl->loading_future);
        impl->loading_future = aws_future_bool_release(impl->loading_future);

        AWS_LOGF_TRACE(
            AWS_LS_S3_GENERAL,
            "id=%p: Background load completed - loaded_bytes=%zu, eos_loaded=%s",
            (void *)impl,
            impl->loading_chunk_buf->len,
            impl->eos_loaded_from_para_stream ? "true" : "false");

        /* Swap the reading the loading pointer. */
        struct aws_byte_buf *tmp = impl->reading_chunk_buf;
        impl->reading_chunk_buf = impl->loading_chunk_buf;
        impl->loading_chunk_buf = tmp;

        if (impl->page_aligned_offset > 0) {
            impl->in_chunk_offset = impl->page_aligned_offset;
            impl->page_aligned_offset = 0;
        } else {
            impl->in_chunk_offset = 0;
        }
        s_kick_off_next_load(impl);
    }

    read_length = aws_min_size(read_length, impl->reading_chunk_buf->len - impl->in_chunk_offset);

    struct aws_byte_cursor chunk_cursor = aws_byte_cursor_from_buf(impl->reading_chunk_buf);
    aws_byte_cursor_advance(&chunk_cursor, impl->in_chunk_offset);
    chunk_cursor.len = read_length;
    aws_byte_buf_append(dest, &chunk_cursor);
    impl->in_chunk_offset += read_length;
    impl->total_length_read += read_length;

    AWS_LOGF_TRACE(
        AWS_LS_S3_GENERAL,
        "id=%p: Read completed - bytes_read=%zu, total_read=%zu, in_chunk_offset=%zu",
        (void *)impl,
        read_length,
        impl->total_length_read,
        impl->in_chunk_offset);

    /* We finished reading the chunk or we reached the expected EOS. */
    if (impl->in_chunk_offset == impl->reading_chunk_buf->len || impl->total_length_read == impl->total_length) {
        /* We finished reading the reading buffer, reset it. */

        if (impl->eos_loaded_from_para_stream || impl->total_length_read == impl->total_length) {
            /* We reached the end of the stream. */
            impl->eos_reached = true;
            AWS_LOGF_DEBUG(AWS_LS_S3_GENERAL, "id=%p: End of stream reached", (void *)impl);
            AWS_ASSERT(impl->total_length_read <= impl->total_length);
        }
        aws_byte_buf_reset(impl->reading_chunk_buf, false);
        impl->in_chunk_offset = 0;
    }

    return AWS_OP_SUCCESS;
}

static int s_part_streaming_input_stream_get_status(struct aws_input_stream *stream, struct aws_stream_status *status) {
    (void)stream;
    (void)status;

    struct aws_s3_part_streaming_input_stream_impl *impl =
        AWS_CONTAINER_OF(stream, struct aws_s3_part_streaming_input_stream_impl, base);

    status->is_end_of_stream = (impl->total_length_read == impl->total_length) || impl->eos_reached;
    status->is_valid = true;

    return AWS_OP_SUCCESS;
}

static int s_part_streaming_input_stream_get_length(struct aws_input_stream *stream, int64_t *out_length) {
    AWS_ASSERT(stream != NULL);
    struct aws_s3_part_streaming_input_stream_impl *impl =
        AWS_CONTAINER_OF(stream, struct aws_s3_part_streaming_input_stream_impl, base);
    *out_length = (int64_t)impl->total_length;
    return AWS_OP_SUCCESS;
}

static void s_part_streaming_input_stream_destroy(void *user_data) {
    struct aws_s3_part_streaming_input_stream_impl *impl = user_data;

    if (impl->loading_future) {
        AWS_LOGF_DEBUG(AWS_LS_S3_GENERAL, "id=%p: Waiting for pending load to complete before destroy", (void *)impl);
        if (aws_future_bool_is_done(impl->loading_future)) {
            aws_future_bool_release(impl->loading_future);
        } else {
            /* If there is a loading future, wait for it to complete.
             * Don't block the thead, to avoid dead lock when the future needs the thread to complete.*/
            /* TODO: probably better to cancel the future, but we don't support cancel yet */
            aws_future_bool_register_callback(impl->loading_future, s_part_streaming_input_stream_destroy, impl);
            return;
        }
    }
    AWS_LOGF_DEBUG(
        AWS_LS_S3_GENERAL,
        "id=%p: Destroying part streaming input stream - total_read=%zu, total_length=%zu",
        (void *)impl,
        impl->total_length_read,
        impl->total_length);
    aws_parallel_input_stream_release(impl->para_stream);
    aws_s3_buffer_ticket_release(impl->ticket);
    aws_mem_release(impl->allocator, impl);
}

static struct aws_input_stream_vtable s_part_streaming_input_stream_vtable = {
    .seek = s_part_streaming_input_stream_seek,
    .read = s_part_streaming_input_stream_read,
    .get_status = s_part_streaming_input_stream_get_status,
    .get_length = s_part_streaming_input_stream_get_length,
};

struct aws_input_stream *aws_part_streaming_input_stream_new(
    struct aws_allocator *allocator,
    struct aws_parallel_input_stream *para_stream,
    struct aws_s3_buffer_ticket *buffer_ticket,
    uint64_t offset,
    size_t request_body_size,
    bool page_aligned) {
    AWS_PRECONDITION(para_stream);
    AWS_PRECONDITION(buffer_ticket);

    struct aws_s3_part_streaming_input_stream_impl *impl =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_part_streaming_input_stream_impl));

    aws_ref_count_init(
        &impl->base.ref_count, impl, (aws_simple_completion_callback *)s_part_streaming_input_stream_destroy);
    impl->allocator = allocator;
    impl->base.vtable = &s_part_streaming_input_stream_vtable;

    if (page_aligned) {
        impl->page_size = aws_system_info_page_size();
    } else {
        /* Disable page alignment by using 1 as the page size */
        impl->page_size = 1;
    }
    impl->offset = offset;
    int64_t para_stream_total_length = 0;
    if (aws_parallel_input_stream_get_length(para_stream, &para_stream_total_length)) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_GENERAL,
            "id=%p: Failed to create part_streaming_input_stream: get length from parallel input stream with error %s",
            (void *)impl,
            aws_error_debug_str(aws_last_error()));
        goto error;
    }
    uint64_t total_available_length = aws_sub_u64_saturating((uint64_t)para_stream_total_length, offset);
    impl->total_length = (size_t)aws_min_u64((uint64_t)request_body_size, total_available_length);

    impl->para_stream = aws_parallel_input_stream_acquire(para_stream);
    impl->ticket = aws_s3_buffer_ticket_acquire(buffer_ticket);

    struct aws_byte_buf buffer = aws_s3_buffer_ticket_claim(impl->ticket);
    if (buffer.capacity % 2 != 0) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_GENERAL,
            "id=%p: Failed to create part_streaming_input_stream: Only supports even length of buffer from the ticket.",
            (void *)impl);
        aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        goto error;
    }
    if (buffer.buffer == NULL || buffer.capacity == 0) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_GENERAL,
            "id=%p: Failed to create part_streaming_input_stream: The buffer from ticket is invalid.",
            (void *)impl);
        aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        goto error;
    }
    impl->chunk_load_size = buffer.capacity / 2;

    if (impl->chunk_load_size < impl->page_size) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_GENERAL,
            "id=%p: Failed to create part_streaming_input_stream: The buffer from ticket is smaller than the two times "
            "of page size. Cannot align the page.",
            (void *)impl);
        aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        goto error;
    }

    /* Split the buffer to the first and second half. */
    /* There is no need to clean up the buffer acquired from the ticket, only need to release the ticket itself. */
    impl->chunk_buf_1 = aws_byte_buf_from_array(buffer.buffer, impl->chunk_load_size);
    impl->chunk_buf_2 = aws_byte_buf_from_array(buffer.buffer + impl->chunk_load_size, impl->chunk_load_size);

    aws_byte_buf_reset(&impl->chunk_buf_1, false);
    aws_byte_buf_reset(&impl->chunk_buf_2, false);

    impl->loading_chunk_buf = &impl->chunk_buf_1;
    impl->reading_chunk_buf = &impl->chunk_buf_2;

    AWS_LOGF_TRACE(
        AWS_LS_S3_GENERAL,
        "id=%p: Created part streaming input stream - offset=%" PRIu64
        ", total_length_to_read=%zu, chunk_load_size=%zu",
        (void *)impl,
        offset,
        impl->total_length,
        impl->chunk_load_size);

    /* Handle zero-length request case */
    if (impl->total_length == 0) {
        impl->eos_reached = true;
        AWS_LOGF_TRACE(AWS_LS_S3_GENERAL, "id=%p: Zero-length request, immediately setting EOS", (void *)impl);
    } else {
        /* Start to load into the loading buffer. Cannot fail the create function after this. */
        s_kick_off_next_load(impl);
    }
    return &impl->base;
error:
    aws_parallel_input_stream_release(impl->para_stream);
    aws_s3_buffer_ticket_release(impl->ticket);
    aws_mem_release(allocator, impl);
    return NULL;
}
