#ifndef AWS_S3_CHECKSUM_H
#define AWS_S3_CHECKSUM_H

#include <aws/common/byte_buf.h>
#include <aws/common/common.h>
#include <aws/s3/s3.h>

enum checksum_type {
    Md5,
    Sha1,
    Sha256,
    Crc32,
    Crc32c,
};

AWS_S3_API
struct aws_checksum *aws_sha256_new(struct aws_allocator *allocator);
/**
 * Allocates and initializes a sha1 checksum instance.
 */
AWS_S3_API
struct aws_checksum *aws_sha1_new(struct aws_allocator *allocator);
/**
 * Allocates and initializes an md5 checksum instance.
 */
AWS_S3_API
struct aws_checksum *aws_md5_new(struct aws_allocator *allocator);
/**
 * Allocates and initializes a crc32 checksum instance
 */
AWS_S3_API
struct aws_checksum *aws_crc32_new(struct aws_allocator *allocator);
/**
 * Allocates and initializes a crc32c checksum instance
 */
AWS_S3_API
struct aws_checksum *aws_crc32c_new(struct aws_allocator *allocator);
/**
 * Cleans up and deallocates checksum.
 */
AWS_S3_API
void aws_checksum_destroy(struct aws_checksum *checksum);
/**
 * Updates the running checksum with to_checksum. this can be called multiple times.
 */
AWS_S3_API
int aws_checksum_update(struct aws_checksum *checksum, const struct aws_byte_cursor *to_checksum);

/**
 * Completes the checksum computation and writes the final digest to output.
 * Allocation of output is the caller's responsibility. If you specify
 * truncate_to to something other than 0, the output will be truncated to that
 * number of bytes. For example if you want a SHA256 digest as the first 16
 * bytes, set truncate_to to 16. If you want the full digest size, just set this
 * to 0.
 */
AWS_S3_API
int aws_checksum_finalize(struct aws_checksum *checksum, struct aws_byte_buf *output, size_t truncate_to);

/**
 * Computes the md5 checksum over input and writes the digest output to 'output'.
 * Use this if you don't need to stream the data you're checksuming and you can load
 * the entire input to checksum into memory.
 */
AWS_S3_API
int aws_md5_compute(
    struct aws_allocator *allocator,
    const struct aws_byte_cursor *input,
    struct aws_byte_buf *output,
    size_t truncate_to);

/**
 * Computes the sha256 checksum over input and writes the digest output to 'output'.
 * Use this if you don't need to stream the data you're checksuming and you can load
 * the entire input to checksum into memory. If you specify truncate_to to something
 * other than 0, the output will be truncated to that  number of bytes. For
 * example if you want a SHA256 digest as the first 16 bytes, set truncate_to
 * to 16. If you want the full digest size, just set this to 0.
 */
AWS_S3_API
int aws_sha256_compute(
    struct aws_allocator *allocator,
    const struct aws_byte_cursor *input,
    struct aws_byte_buf *output,
    size_t truncate_to);

/**
 * Computes the sha1 checksum over input and writes the digest output to 'output'.
 * Use this if you don't need to stream the data you're checksuming and you can load
 * the entire input to checksum into memory. If you specify truncate_to to something
 * other than 0, the output will be truncated to that  number of bytes. For
 * example if you want a SHA1 digest as the first 16 bytes, set truncate_to
 * to 16. If you want the full digest size, just set this to 0.
 */
AWS_S3_API
int aws_sha1_compute(
    struct aws_allocator *allocator,
    const struct aws_byte_cursor *input,
    struct aws_byte_buf *output,
    size_t truncate_to);

#endif /* #define AWS_S3_H */
