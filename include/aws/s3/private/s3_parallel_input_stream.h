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
struct aws_future_void;
struct aws_input_stream;
struct aws_s3_meta_request;
struct aws_s3_request;

struct aws_event_loop_group;

/**
 * This should be private, but keep it public for providing your own implementation.
 */
struct aws_parallel_input_stream {
    const struct aws_parallel_input_stream_vtable *vtable;
    struct aws_allocator *alloc;
    struct aws_ref_count ref_count;
    struct aws_future_void *shutdown_future;

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
    struct aws_future_bool *(
        *read)(struct aws_parallel_input_stream *stream, uint64_t offset, size_t max_length, struct aws_byte_buf *dest);

    /**
     * Get the length of the stream.
     *
     * @param stream            The stream to get length from
     * @param out_length        The output length
     * @return                  AWS_OP_SUCCESS if success, otherwise AWS_OP_ERR
     */
    int (*get_length)(struct aws_parallel_input_stream *stream, int64_t *out_length);
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
 * Get the total length of the parallel input stream.
 *
 * @param stream
 * @param out_length
 * @return AWS_S3_API
 */
AWS_S3_API
int aws_parallel_input_stream_get_length(struct aws_parallel_input_stream *stream, int64_t *out_length);

/**
 * Creates a new parallel input stream that reads from a file.
 * This stream uses an event loop group to perform file I/O operations asynchronously.
 *
 * Notes for direct_io_read:
 * - checking `aws_file_path_read_from_offset_direct_io` for detail
 * - For `AWS_ERROR_UNSUPPORTED_OPERATION`, fallback to reading with cache with warnings, instead of fail.
 * - If alignment required, it's callers' responsibility to align with the page size.
 *
 * @param allocator The allocator to use for memory allocation
 * @param file_name The name of the file to read from
 * @param reading_elg The event loop group to use for file I/O operations
 * @param direct_io_read Whether to use direct I/O for reading the file.
 *
 * @return A new parallel input stream that reads from the specified file
 */
AWS_S3_API
struct aws_parallel_input_stream *aws_parallel_input_stream_new_from_file(
    struct aws_allocator *allocator,
    struct aws_byte_cursor file_name,
    struct aws_event_loop_group *reading_elg,
    bool direct_io_read);

/**
 * Get the shutdown future from the parallel input stream.
 * The future will be completed when every refcount on the stream has been released.
 * And all the resource has been released.
 * Don't hold any refcount of the stream while waiting on the future, otherwise, deadlock can happen.
 * You need to release the future after using it.
 */
AWS_S3_API
struct aws_future_void *aws_parallel_input_stream_get_shutdown_future(struct aws_parallel_input_stream *stream);

AWS_EXTERN_C_END
AWS_POP_SANE_WARNING_LEVEL

#endif /* AWS_S3_PARALLEL_INPUT_STREAM_H */
