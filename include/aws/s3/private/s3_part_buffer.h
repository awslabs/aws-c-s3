#ifndef S3_PART_BUFFER_H
#define S3_PART_BUFFER_H

#include <aws/common/byte_buf.h>
#include <aws/common/linked_list.h>
#include <aws/common/ref_count.h>
#include <aws/common/task_scheduler.h>
#include <inttypes.h>

struct aws_s3_client;

/* Pre-allocated buffer that is the size of a single part.*/
struct aws_s3_part_buffer {
    struct aws_linked_list_node node;

    /* Reference to the owning client so that it can easily be released back. */
    struct aws_s3_client *client;

    /* What part of the overall file transfer this part is currently designated to. */
    uint64_t range_start;

    /* Re-usable byte buffer. */
    struct aws_byte_buf buffer;
};

/* Pool of pre-allocated part buffers. */
struct aws_s3_part_buffer_pool {
    size_t num_allocated;
    struct aws_linked_list free_list;
};

#endif /* S3_PART_BUFFER_H */
