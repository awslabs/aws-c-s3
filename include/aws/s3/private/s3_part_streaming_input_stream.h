/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#ifndef AWS_S3_PART_STREAMING_INPUT_STREAM_H
#define AWS_S3_PART_STREAMING_INPUT_STREAM_H

#include <aws/s3/s3.h>

struct aws_input_stream;
struct aws_s3_buffer_ticket;
struct aws_parallel_input_stream;

AWS_PUSH_SANE_WARNING_LEVEL
AWS_EXTERN_C_BEGIN

/**
 * Creates a new streaming input stream that reads from a parallel input stream.
 * This adapter allows using a parallel input stream with APIs that expect a standard input stream.
 * The adapter uses double-buffering to read ahead and provide efficient streaming.
 *
 * Note: The input stream only provides a blocking API to read, if reading from the same thread that
 *      will be used from `para_stream` to handle the parallel read, it's subjected to a dead lock!!
 *      Make sure the input stream is read from a different thread/thread pool that executes the `para_stream` reads.
 *
 * @param allocator             The allocator to use for memory allocation
 * @param para_stream           The parallel input stream to read from
 * @param buffer_ticket         The buffer pool ticket to use for buffering
 * @param offset                The starting offset in the stream
 * @param request_body_size     The total size to read
 * @param page_aligned          Whether the input stream only read from the para_stream on that
 *                              aligned with the page size, required for direct I/O in linux.
 * @return A new input stream that reads from the parallel input stream
 */
AWS_S3_API
struct aws_input_stream *aws_part_streaming_input_stream_new(
    struct aws_allocator *allocator,
    struct aws_parallel_input_stream *para_stream,
    struct aws_s3_buffer_ticket *buffer_ticket,
    uint64_t offset,
    size_t request_body_size,
    bool page_aligned);

AWS_EXTERN_C_END
AWS_POP_SANE_WARNING_LEVEL

#endif /* AWS_S3_PART_STREAMING_INPUT_STREAM_H */
