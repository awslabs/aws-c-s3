/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/s3/private/s3_meta_request_impl.h>
#include <aws/s3/private/s3_parallel_input_stream.h>

#include <aws/common/atomics.h>
#include <aws/common/clock.h>
#include <aws/common/file.h>
#include <aws/common/string.h>
#include <aws/common/task_scheduler.h>
#include <aws/common/thread.h>

#include <aws/io/event_loop.h>
#include <aws/io/future.h>
#include <aws/io/stream.h>

#include <errno.h>

#define ONE_SEC_IN_NS_P ((uint64_t)AWS_TIMESTAMP_NANOS)
#define MAX_TIMEOUT_NS_P (600 * ONE_SEC_IN_NS_P)

void aws_parallel_input_stream_init_base(
    struct aws_parallel_input_stream *stream,
    struct aws_allocator *alloc,
    const struct aws_parallel_input_stream_vtable *vtable,
    void *impl) {

    AWS_ZERO_STRUCT(*stream);
    stream->alloc = alloc;
    stream->vtable = vtable;
    stream->impl = impl;
    aws_ref_count_init(&stream->ref_count, stream, (aws_simple_completion_callback *)vtable->destroy);
}

struct aws_parallel_input_stream *aws_parallel_input_stream_acquire(struct aws_parallel_input_stream *stream) {
    if (stream != NULL) {
        aws_ref_count_acquire(&stream->ref_count);
    }
    return stream;
}

struct aws_parallel_input_stream *aws_parallel_input_stream_release(struct aws_parallel_input_stream *stream) {
    if (stream != NULL) {
        aws_ref_count_release(&stream->ref_count);
    }
    return NULL;
}

struct aws_future_bool *aws_parallel_input_stream_read(
    struct aws_parallel_input_stream *stream,
    uint64_t offset,
    size_t max_length,
    struct aws_byte_buf *dest) {
    /* Ensure the buffer has space available */
    if (dest->len == dest->capacity) {
        struct aws_future_bool *future = aws_future_bool_new(stream->alloc);
        aws_future_bool_set_error(future, AWS_ERROR_SHORT_BUFFER);
        return future;
    }

    struct aws_future_bool *future = stream->vtable->read(stream, offset, max_length, dest);
    AWS_POSTCONDITION(future != NULL);
    return future;
}

struct aws_parallel_input_stream_from_file_impl {
    struct aws_parallel_input_stream base;

    struct aws_string *file_path;
    struct aws_event_loop_group *reading_elg;

    FILE **file_stream;
    size_t elg_num;
    struct aws_atomic_var current_index;
};

static void s_para_from_file_destroy(struct aws_parallel_input_stream *stream) {
    struct aws_parallel_input_stream_from_file_impl *impl =
        AWS_CONTAINER_OF(stream, struct aws_parallel_input_stream_from_file_impl, base);

    aws_string_destroy(impl->file_path);
    size_t elg_num = aws_event_loop_group_get_loop_count(impl->reading_elg);
    for (size_t i = 0; i < elg_num; i++) {
        fclose(impl->file_stream[i]);
    }
    aws_event_loop_group_release(impl->reading_elg);

    aws_mem_release(stream->alloc, impl);

    return;
}

struct read_task_impl {
    struct aws_parallel_input_stream_from_file_impl *para_impl;

    struct aws_future_bool *end_future;
    uint64_t offset;
    size_t length;
    struct aws_byte_buf *dest;
    FILE *file_stream;
};

static void s_s3_parallel_from_file_read_task(struct aws_task *task, void *arg, enum aws_task_status task_status) {
    (void)task_status;
    struct read_task_impl *read_task = arg;
    struct aws_parallel_input_stream_from_file_impl *impl = read_task->para_impl;
    struct aws_future_bool *end_future = read_task->end_future;
    FILE *file_stream = read_task->file_stream;
    int error_code = AWS_ERROR_SUCCESS;
    size_t actually_read = 0;
    /* seek to the right position and then read */
    if (aws_fseek(file_stream, (int64_t)read_task->offset, SEEK_SET)) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_GENERAL,
            "id=%p: Failed to seek to position %llu in file %s",
            (void *)&impl->base,
            (unsigned long long)read_task->offset,
            aws_string_c_str(impl->file_path));
        error_code = aws_last_error();
        goto cleanup;
    }

    actually_read = fread(read_task->dest->buffer + read_task->dest->len, 1, read_task->length, file_stream);
    if (actually_read == 0 && ferror(file_stream)) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_GENERAL,
            "id=%p: Failed to read %zu bytes from file %s",
            (void *)&impl->base,
            read_task->length,
            aws_string_c_str(impl->file_path));
        error_code = aws_translate_and_raise_io_error(errno);
        goto cleanup;
    }

    read_task->dest->len += actually_read;

    AWS_LOGF_TRACE(
        AWS_LS_S3_GENERAL,
        "id=%p: Successfully read %zu bytes from file %s at position %llu",
        (void *)&impl->base,
        actually_read,
        aws_string_c_str(impl->file_path),
        (unsigned long long)read_task->offset);

cleanup:

    if (error_code != AWS_ERROR_SUCCESS) {
        aws_future_bool_set_error(end_future, error_code);
    } else {
        /* Return true if we reached EOF */
        bool eof_reached = (actually_read < read_task->length);
        AWS_ASSERT(!eof_reached);
        aws_future_bool_set_result(end_future, eof_reached);
    }

    aws_future_bool_release(end_future);
    aws_mem_release(impl->base.alloc, task);
    aws_mem_release(impl->base.alloc, read_task);
}

struct aws_future_bool *s_para_from_file_read(
    struct aws_parallel_input_stream *stream,
    uint64_t offset,
    size_t max_length,
    struct aws_byte_buf *dest) {

    struct aws_future_bool *future = aws_future_bool_new(stream->alloc);
    struct aws_parallel_input_stream_from_file_impl *impl =
        AWS_CONTAINER_OF(stream, struct aws_parallel_input_stream_from_file_impl, base);

    /* Calculate how much we can read based on available buffer space and max_length */
    size_t available_space = dest->capacity - dest->len;
    size_t length = aws_min_size(available_space, max_length);

    if (length == 0) {
        /* Nothing to read. Complete the read with success. */
        aws_future_bool_set_result(future, false);
        return future;
    }

    struct read_task_impl *read_task = aws_mem_calloc(impl->base.alloc, 1, sizeof(struct read_task_impl));

    AWS_LOGF_TRACE(
        AWS_LS_S3_GENERAL,
        "id=%p: Read %zu bytes from offset %llu",
        (void *)stream,
        length,
        (unsigned long long)offset);

    /* Initialize for one read */
    read_task->dest = dest;
    read_task->offset = offset;
    read_task->length = length;
    read_task->end_future = aws_future_bool_acquire(future);
    /* May need to keep the impl alive */
    read_task->para_impl = impl;

    int index = aws_atomic_fetch_add_int(&impl->current_index, 1);
    index %= impl->elg_num;
    /*This is not 100% thread safe, but, it's fine. We only need to make sure the index is the same for ELG and fstream.
     */
    aws_atomic_store_int(&impl->current_index, index);

    struct aws_event_loop *loop = aws_event_loop_group_get_loop_at(impl->reading_elg, index);
    read_task->file_stream = impl->file_stream[index];

    struct aws_task *task = aws_mem_calloc(impl->base.alloc, 1, sizeof(struct aws_task));
    aws_task_init(task, s_s3_parallel_from_file_read_task, read_task, "s3_parallel_read_task");
    aws_event_loop_schedule_task_now(loop, task);
    return future;
}

static struct aws_parallel_input_stream_vtable s_parallel_input_stream_from_file_vtable = {
    .destroy = s_para_from_file_destroy,
    .read = s_para_from_file_read,
};

struct aws_parallel_input_stream *aws_parallel_input_stream_new_from_file(
    struct aws_allocator *allocator,
    struct aws_byte_cursor file_name,
    struct aws_event_loop_group *reading_elg) {

    struct aws_parallel_input_stream_from_file_impl *impl =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_parallel_input_stream_from_file_impl));

    aws_parallel_input_stream_init_base(&impl->base, allocator, &s_parallel_input_stream_from_file_vtable, impl);
    impl->file_path = aws_string_new_from_cursor(allocator, &file_name);
    impl->reading_elg = aws_event_loop_group_acquire(reading_elg);
    size_t elg_num = aws_event_loop_group_get_loop_count(impl->reading_elg);
    impl->file_stream = aws_mem_calloc(allocator, elg_num, sizeof(FILE *));

    if (!aws_path_exists(impl->file_path)) {
        /* If file path not exists, raise error from errno. */
        aws_translate_and_raise_io_error(errno);
        s_para_from_file_destroy(&impl->base);
        return NULL;
    }
    for (size_t i = 0; i < elg_num; i++) {
        impl->file_stream[i] = aws_fopen(aws_string_c_str(impl->file_path), "rb");
    }
    aws_atomic_init_int(&impl->current_index, 0);
    return &impl->base;
}

struct aws_s3_mmap_part_streaming_input_stream_impl {
    struct aws_input_stream base;
    struct aws_allocator *allocator;

    struct aws_parallel_input_stream *stream;
    size_t offset;

    size_t chunk_load_size;
    void *page_address;
    size_t in_chunk_offset;

    size_t total_length;
    size_t total_length_read;

    struct aws_byte_buf *reading_chunk_buf;
    struct aws_byte_buf *loading_chunk_buf;
    struct aws_future_bool *loading_future;

    struct aws_byte_buf chunk_buf_1;
    struct aws_byte_buf chunk_buf_2;

    bool eos_loaded;
    bool eos_reached;

    struct s3_data_read_metrics metrics;

    struct aws_s3_meta_request *meta_request;
    struct aws_s3_request *request;
};

static int s_aws_s3_mmap_part_streaming_input_stream_seek(
    struct aws_input_stream *stream,
    int64_t offset,
    enum aws_stream_seek_basis basis) {
    (void)stream;
    (void)offset;
    (void)basis;
    return aws_raise_error(AWS_ERROR_UNSUPPORTED_OPERATION);
}

static int s_aws_s3_mmap_part_streaming_input_stream_read(struct aws_input_stream *stream, struct aws_byte_buf *dest) {
    struct aws_s3_mmap_part_streaming_input_stream_impl *impl =
        AWS_CONTAINER_OF(stream, struct aws_s3_mmap_part_streaming_input_stream_impl, base);
    /* Map the content */
    size_t read_length = aws_min_size(dest->capacity - dest->len, impl->total_length - impl->total_length_read);
    uint64_t time = 0;

    aws_s3_meta_request_lock_synced_data(impl->meta_request);
    if (impl->request->send_data.metrics->time_metrics.body_read_start_timestamp_ns == -1) {
        aws_high_res_clock_get_ticks(&time);
        impl->request->send_data.metrics->time_metrics.body_read_start_timestamp_ns = time;
    }
    aws_s3_meta_request_unlock_synced_data(impl->meta_request);
    aws_high_res_clock_get_ticks(&impl->metrics.start_timestamp);
    impl->metrics.thread_id = aws_thread_current_thread_id();

    if (impl->in_chunk_offset == SIZE_MAX) {
        /* The reading buf is invalid. Block until the loading buf is available. */
        if (impl->loading_future == NULL) {
            /* Nothing to read */
            AWS_ASSERT(impl->eos_reached);
            return AWS_OP_SUCCESS;
        }
        aws_future_bool_wait(impl->loading_future, MAX_TIMEOUT_NS_P);
        int read_error = aws_future_bool_get_error(impl->loading_future);
        if (read_error != 0) {
            /* Read failed. */
            return aws_raise_error(read_error);
        }
        impl->eos_loaded = aws_future_bool_get_result(impl->loading_future);
        impl->loading_future = aws_future_bool_release(impl->loading_future);
        /* Swap the reading the loading pointer. */
        AWS_ASSERT(impl->reading_chunk_buf->len == 0);
        struct aws_byte_buf *tmp = impl->reading_chunk_buf;
        impl->reading_chunk_buf = impl->loading_chunk_buf;
        impl->loading_chunk_buf = tmp;
        size_t new_offset = impl->offset + impl->total_length_read + impl->reading_chunk_buf->len;
        size_t new_load_length = aws_min_size(
            impl->chunk_load_size, impl->total_length - impl->total_length_read - impl->reading_chunk_buf->len);
        if (new_load_length > 0 && !impl->eos_loaded) {
            /* Kick off loading the next chunk. */
            impl->loading_future =
                aws_parallel_input_stream_read(impl->stream, new_offset, new_load_length, impl->loading_chunk_buf);
        }
        impl->in_chunk_offset = 0;
    }
    read_length = aws_min_size(read_length, impl->reading_chunk_buf->len - impl->in_chunk_offset);
    impl->metrics.offset = impl->offset + impl->total_length_read;
    impl->metrics.size = read_length;
    impl->metrics.request_ptr = impl->request;

    struct aws_byte_cursor chunk_cursor = aws_byte_cursor_from_buf(impl->reading_chunk_buf);
    aws_byte_cursor_advance(&chunk_cursor, impl->in_chunk_offset);
    chunk_cursor.len = read_length;
    aws_byte_buf_append(dest, &chunk_cursor);
    impl->in_chunk_offset += read_length;
    impl->total_length_read += read_length;
    aws_high_res_clock_get_ticks(&impl->metrics.end_timestamp);
    int64_t duration = impl->metrics.end_timestamp - impl->metrics.start_timestamp;

    /* BEGIN CRITICAL SECTION */
    aws_s3_meta_request_lock_synced_data(impl->meta_request);
    aws_array_list_push_back(&impl->meta_request->read_metrics_list, &impl->metrics);
    if (impl->request->send_data.metrics->time_metrics.body_read_total_ns == -1) {
        impl->request->send_data.metrics->time_metrics.body_read_total_ns = duration;
    } else {
        impl->request->send_data.metrics->time_metrics.body_read_total_ns += duration;
    }
    if (impl->request->send_data.metrics->time_metrics.body_read_total_without_reset_ns == -1) {
        impl->request->send_data.metrics->time_metrics.body_read_total_without_reset_ns = duration;
    } else {
        impl->request->send_data.metrics->time_metrics.body_read_total_without_reset_ns += duration;
    }
    aws_s3_meta_request_unlock_synced_data(impl->meta_request);
    /* END CRITICAL SECTION */
    if (impl->in_chunk_offset == impl->reading_chunk_buf->len) {
        /* We finished reading the reading buffer, reset it. */
        aws_byte_buf_reset(impl->reading_chunk_buf, false);
        impl->in_chunk_offset = SIZE_MAX;
        if (impl->eos_loaded || impl->total_length_read == impl->total_length) {
            /* We reached the end of the stream. */
            impl->eos_reached = true;
            AWS_ASSERT(impl->total_length_read == impl->total_length);
            aws_s3_meta_request_lock_synced_data(impl->meta_request);
            if (impl->request->send_data.metrics->time_metrics.body_read_end_timestamp_ns == -1) {
                aws_high_res_clock_get_ticks(&time);
                impl->request->send_data.metrics->time_metrics.body_read_end_timestamp_ns = time;
            }
            impl->request->send_data.metrics->time_metrics.body_read_duration_ns =
                impl->request->send_data.metrics->time_metrics.body_read_end_timestamp_ns -
                impl->request->send_data.metrics->time_metrics.body_read_start_timestamp_ns;
            aws_s3_meta_request_unlock_synced_data(impl->meta_request);
        }
    }

    return AWS_OP_SUCCESS;
}

static int s_aws_s3_mmap_part_streaming_input_stream_get_status(
    struct aws_input_stream *stream,
    struct aws_stream_status *status) {
    (void)stream;
    (void)status;

    struct aws_s3_mmap_part_streaming_input_stream_impl *impl =
        AWS_CONTAINER_OF(stream, struct aws_s3_mmap_part_streaming_input_stream_impl, base);

    status->is_end_of_stream = (impl->total_length_read == impl->total_length) || impl->eos_reached;
    status->is_valid = true;

    return AWS_OP_SUCCESS;
}

static int s_aws_s3_mmap_part_streaming_input_stream_get_length(struct aws_input_stream *stream, int64_t *out_length) {
    AWS_ASSERT(stream != NULL);
    struct aws_s3_mmap_part_streaming_input_stream_impl *impl =
        AWS_CONTAINER_OF(stream, struct aws_s3_mmap_part_streaming_input_stream_impl, base);
    *out_length = (int64_t)impl->total_length;
    return AWS_OP_SUCCESS;
}

static void s_aws_s3_mmap_part_streaming_input_stream_destroy(
    struct aws_s3_mmap_part_streaming_input_stream_impl *impl) {
    aws_parallel_input_stream_release(impl->stream);
    aws_byte_buf_clean_up(&impl->chunk_buf_1);
    aws_byte_buf_clean_up(&impl->chunk_buf_2);
    aws_mem_release(impl->allocator, impl);
}

static struct aws_input_stream_vtable s_aws_s3_mmap_part_streaming_input_stream_vtable = {
    .seek = s_aws_s3_mmap_part_streaming_input_stream_seek,
    .read = s_aws_s3_mmap_part_streaming_input_stream_read,
    .get_status = s_aws_s3_mmap_part_streaming_input_stream_get_status,
    .get_length = s_aws_s3_mmap_part_streaming_input_stream_get_length,
};

void aws_streaming_input_stream_reset(struct aws_input_stream *stream) {
    struct aws_s3_mmap_part_streaming_input_stream_impl *impl =
        AWS_CONTAINER_OF(stream, struct aws_s3_mmap_part_streaming_input_stream_impl, base);
    if (impl->loading_future) {
        /* If there is a loading future, wait for it to complete. */
        /* TODO: probably better to cancel the future, but we don't support cancel yet */
        aws_future_bool_wait(impl->loading_future, MAX_TIMEOUT_NS_P);
        aws_future_bool_release(impl->loading_future);
    }
    impl->total_length_read = 0;
    impl->eos_loaded = false;
    impl->eos_reached = false;
    impl->in_chunk_offset = SIZE_MAX;
    aws_byte_buf_reset(&impl->chunk_buf_1, false);
    aws_byte_buf_reset(&impl->chunk_buf_2, false);
    size_t new_load_length = aws_min_size(impl->chunk_load_size, impl->total_length);
    /* Start to load into the loading buffer. */
    impl->loading_future =
        aws_parallel_input_stream_read(impl->stream, impl->offset, new_load_length, impl->loading_chunk_buf);
}

struct aws_input_stream *aws_input_stream_new_from_parallel_stream(
    struct aws_allocator *allocator,
    struct aws_parallel_input_stream *stream,
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    uint64_t offset,
    size_t request_body_size) {

    struct aws_s3_mmap_part_streaming_input_stream_impl *impl =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_mmap_part_streaming_input_stream_impl));
    aws_ref_count_init(
        &impl->base.ref_count,
        impl,
        (aws_simple_completion_callback *)s_aws_s3_mmap_part_streaming_input_stream_destroy);
    impl->allocator = allocator;
    impl->base.vtable = &s_aws_s3_mmap_part_streaming_input_stream_vtable;

    impl->total_length = request_body_size;
    impl->offset = offset;
    impl->chunk_load_size = 8 * 1024 * 1024;

    impl->stream = aws_parallel_input_stream_acquire(stream);
    aws_byte_buf_init(&impl->chunk_buf_1, allocator, impl->chunk_load_size);
    aws_byte_buf_init(&impl->chunk_buf_2, allocator, impl->chunk_load_size);
    impl->loading_chunk_buf = &impl->chunk_buf_1;
    impl->reading_chunk_buf = &impl->chunk_buf_2;
    impl->meta_request = meta_request;
    impl->request = request;
    if (impl->request->send_data.metrics && impl->request->send_data.metrics->time_metrics.body_read_total_ns != -1) {
        impl->request->send_data.metrics->time_metrics.body_read_total_ns = -1;
    }

    /* Reset the input stream to start */
    aws_streaming_input_stream_reset(&impl->base);
    return &impl->base;
}
