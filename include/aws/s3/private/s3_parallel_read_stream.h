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
     * The implementation needs to support this to be invoked concurrently from multiple threads
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
 * Read from the [start_position, end_position).
 * It's thread safe to be called from multiple threads without waiting for other read to complete
 *
 * @param stream            The stream to read from
 * @param start_position    The start_position in the stream to
 * @param end_position      Read until end_position of the stream
 * @param dest              The output buffer read to
 * @return a future to be solved when the current read finishes
 */
AWS_S3_API
struct aws_future_bool *aws_parallel_input_stream_read(
    struct aws_parallel_input_stream *stream,
    size_t start_position,
    size_t end_position,
    struct aws_byte_buf *dest);

/**
 * Create a new file based parallel input stream implementation.
 *
 * This implementation will take `num_workers` event loops from the `reading_elg` to schedule read jobs to.
 * And will open `num_workers` FILE * to read from the file concurrently.
 *
 * Note: we may result in less than the `num_workers` threads to use.
 *
 * @param allocator         memory allocator
 * @param file_name         The file path to read from
 * @param reading_elg       The eventloop group to assign read work to
 * @param num_workers       The number of worker to read from file
 * @return aws_parallel_input_stream
 */
AWS_S3_API
struct aws_parallel_input_stream *aws_parallel_input_stream_new_from_file(
    struct aws_allocator *allocator,
    const char *file_name);

AWS_EXTERN_C_END
AWS_POP_SANE_WARNING_LEVEL

#endif /* AWS_S3_PARALLEL_READ_STREAM_H */
