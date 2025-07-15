/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#ifndef AWS_S3_PARALLEL_INPUT_STREAM_H
#define AWS_S3_PARALLEL_INPUT_STREAM_H

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
     * Read from the offset until fill the dest, or EOF reached.
     * It's thread safe to be called from multiple threads without waiting for other read to complete
     *
     * @param stream            The stream to read from
     * @param offset            The offset in the stream from beginning to start reading
     * @param max_length        The maximum number of bytes to read
     * @param dest              The output buffer read to
     * @return                  a future, which will contain an error code if something went wrong,
     *                          or a result bool indicating whether EOF has been reached.
     */
    struct aws_future_bool *(*read)(
        struct aws_parallel_input_stream *stream,
        uint64_t offset,
        size_t max_length,
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
 * Read from the offset until fill the dest, or EOF reached.
 * It's thread safe to be called from multiple threads without waiting for other read to complete
 *
 * @param stream            The stream to read from
 * @param offset            The offset in the stream from beginning to start reading
 * @param max_length        The maximum number of bytes to read
 * @param dest              The output buffer read to
 * @return                  a future, which will contain an error code if something went wrong,
 *                          or a result bool indicating whether EOF has been reached.
 */
AWS_S3_API
struct aws_future_bool *aws_parallel_input_stream_read(
    struct aws_parallel_input_stream *stream,
    uint64_t offset,
    size_t max_length,
    struct aws_byte_buf *dest);

/**
 * Creates a new parallel input stream that reads from a file.
 * This stream uses an event loop group to perform file I/O operations asynchronously.
 *
 * @param allocator The allocator to use for memory allocation
 * @param file_name The name of the file to read from
 * @param reading_elg The event loop group to use for file I/O operations
 * @return A new parallel input stream that reads from the specified file
 */
AWS_S3_API
struct aws_parallel_input_stream *aws_parallel_input_stream_new_from_file(
    struct aws_allocator *allocator,
    struct aws_byte_cursor file_name,
    struct aws_event_loop_group *reading_elg);

/****************** Adapters to convert parallel input stream to standard input stream ***************************/

/**
 * Creates a new streaming input stream that reads from a parallel input stream.
 * This adapter allows using a parallel input stream with APIs that expect a standard input stream.
 * The adapter uses double-buffering to read ahead and provide efficient streaming.
 *
 * @param allocator The allocator to use for memory allocation
 * @param stream The parallel input stream to read from
 * @param offset The starting offset in the stream
 * @param request_body_size The total size to read
 * @return A new input stream that reads from the parallel input stream
 */
struct aws_input_stream *aws_input_stream_new_from_parallel_stream(
    struct aws_allocator *allocator,
    struct aws_parallel_input_stream *stream,
    uint64_t offset,
    size_t request_body_size);

/**
 * Resets the streaming input stream to start reading from the beginning again.
 * This allows reusing the same stream for multiple read operations.
 *
 * @param stream The streaming input stream to reset
 */
void aws_streaming_input_stream_reset(struct aws_input_stream *stream);

AWS_EXTERN_C_END
AWS_POP_SANE_WARNING_LEVEL

#endif /* AWS_S3_PARALLEL_INPUT_STREAM_H */
