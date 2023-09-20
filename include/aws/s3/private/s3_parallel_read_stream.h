/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#ifndef AWS_S3_PARALLEL_READ_STREAM_H
#define AWS_S3_PARALLEL_READ_STREAM_H

#include <aws/s3/s3.h>

#include <aws/common/ref_count.h>

AWS_PUSH_SANE_WARNING_LEVEL

struct aws_byte_buf;
struct aws_future_bool;
struct aws_input_stream;

struct aws_event_loop_group;

struct aws_parallel_input_stream {
    const struct aws_parallel_input_stream_vtable *vtable;
    struct aws_allocator *alloc;
    struct aws_ref_count ref_count;

    void *impl;
};

struct aws_parallel_input_stream_vtable {
    /**
     * Destroy the stream, its refcount has reached 0.
     */
    void (*destroy)(struct aws_parallel_input_stream *stream);

    /**
     * Read into the buffer in parallel.
     * The stream will split into parts and read each part in parallel.
     *
     * TODO: error handling:
     * - The stream reach end before all parts
     * - Failed to create a elg task to read
     * - Asyc related stuff as the original async stream (If we cannot fill the dest, we wait?)
     * - etc.
     */
    struct aws_future_bool *(*read)(
        struct aws_parallel_input_stream *stream,
        size_t start_position,
        size_t end_position,
        struct aws_byte_buf *dest);
};

AWS_EXTERN_C_BEGIN

/**
 * Initialize aws_parallel_input_stream "base class"
 */
AWS_S3_API
void aws_parallel_input_stream_init_base(
    struct aws_parallel_input_stream *stream,
    struct aws_allocator *alloc,
    const struct aws_parallel_input_stream_vtable *vtable,
    void *impl);

/**
 * Increment reference count.
 * You may pass in NULL (has no effect).
 * Returns whatever pointer was passed in.
 */
AWS_S3_API
struct aws_parallel_input_stream *aws_parallel_input_stream_acquire(struct aws_parallel_input_stream *stream);

/**
 * Decrement reference count.
 * You may pass in NULL (has no effect).
 * Always returns NULL.
 */
AWS_S3_API
struct aws_parallel_input_stream *aws_parallel_input_stream_release(struct aws_parallel_input_stream *stream);

/**
 * WARNING: Do not read again until the previous read is complete.
 *
 * @param stream
 * @param start_position
 * @param end_position
 * @param dest
 * @param split_num
 * @return AWS_S3_API struct*
 */
AWS_S3_API
struct aws_future_bool *aws_parallel_input_stream_read(
    struct aws_parallel_input_stream *stream,
    size_t start_position,
    size_t end_position,
    struct aws_byte_buf *dest);

AWS_S3_API
struct aws_parallel_input_stream *aws_parallel_input_stream_new_from_file(
    struct aws_allocator *allocator,
    struct aws_byte_cursor file_name,
    struct aws_event_loop_group *reading_elg,
    size_t num_workers);

AWS_EXTERN_C_END
AWS_POP_SANE_WARNING_LEVEL

#endif /* AWS_S3_PARALLEL_READ_STREAM_H */
