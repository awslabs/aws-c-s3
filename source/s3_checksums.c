#include "aws/s3/private/s3_checksums.h"
#include <aws/checksums/crc.h>

size_t digest_size_from_algorithm(enum aws_s3_checksum_algorithm algorithm) {
    switch (algorithm) {
        case AWS_CRC32C:
            return AWS_CRC32C_LEN;
        case AWS_CRC32:
            return AWS_CRC32_LEN;
        case AWS_SHA1:
            return AWS_SHA1_LEN;
        case AWS_SHA256:
            return AWS_SHA256_LEN;
        case AWS_MD5:
            return AWS_MD5_LEN;
        default:
            AWS_ASSERT(false);
            return 0;
    }
}

void s3_hash_destroy(struct aws_checksum *checksum) {
    struct aws_hash *hash = (struct aws_hash *)checksum->impl;
    aws_hash_destroy(hash);
    aws_mem_release(checksum->allocator, checksum->vtable);
    aws_mem_release(checksum->allocator, checksum);
}

int s3_hash_update(struct aws_checksum *checksum, const struct aws_byte_cursor *to_checksum) {
    struct aws_hash *hash = (struct aws_hash *)checksum->impl;
    return aws_hash_update(hash, to_checksum);
}

int s3_hash_finalize(struct aws_checksum *checksum, struct aws_byte_buf *output, size_t truncate_to) {
    struct aws_hash *hash = (struct aws_hash *)checksum->impl;
    checksum->good = false;
    return aws_hash_finalize(hash, output, truncate_to);
}

struct aws_checksum *aws_hash_new(struct aws_allocator *allocator, aws_hash_new_fn hash_fn) {
    struct aws_checksum *checksum = aws_mem_acquire(allocator, sizeof(struct aws_checksum));
    struct aws_checksum_vtable *vtable = aws_mem_acquire(allocator, sizeof(struct aws_checksum_vtable));
    struct aws_hash *hash = hash_fn(allocator);
    checksum->impl = (void *)hash;
    checksum->allocator = allocator;
    vtable->update = s3_hash_update;
    vtable->finalize = s3_hash_finalize;
    vtable->destroy = s3_hash_destroy;
    checksum->vtable = vtable;
    checksum->good = true;
    checksum->digest_size = hash->digest_size;
    return checksum;
}

struct aws_checksum *aws_sha256_checksum_new(struct aws_allocator *allocator) {
    return aws_hash_new(allocator, aws_sha256_new);
}

struct aws_checksum *aws_sha1_checksum_new(struct aws_allocator *allocator) {
    return aws_hash_new(allocator, aws_sha1_new);
}

struct aws_checksum *aws_md5_checksum_new(struct aws_allocator *allocator) {
    return aws_hash_new(allocator, aws_md5_new);
}

struct aws_checksum *aws_checksum_new(struct aws_allocator *allocator, enum aws_s3_checksum_algorithm algorithm) {
    switch (algorithm) {
        case AWS_CRC32C:
            return aws_crc32c_checksum_new(allocator);
        case AWS_CRC32:
            return aws_crc32_checksum_new(allocator);
        case AWS_SHA1:
            return aws_sha1_checksum_new(allocator);
        case AWS_SHA256:
            return aws_sha256_checksum_new(allocator);
        case AWS_MD5:
            return aws_md5_checksum_new(allocator);
        default:
            return NULL;
    }
}

void aws_crc_destroy(struct aws_checksum *checksum) {
    aws_mem_release(checksum->allocator, checksum->vtable);
    aws_mem_release(checksum->allocator, checksum);
}

uint32_t aws_crc32_common(
    uint32_t previous,
    const struct aws_byte_cursor *buf,
    uint32_t (*checksum_fn)(const uint8_t *, int, uint32_t)) {

    size_t length = buf->len;
    uint8_t *buffer = buf->ptr;
    uint32_t val = previous;
    while (length > INT_MAX) {
        val = checksum_fn(buffer, INT_MAX, val);
        buffer += (size_t)INT_MAX;
        length -= (size_t)INT_MAX;
    }
    return checksum_fn(buffer, (int)length, val);
}

int aws_crc32_checksum_update(struct aws_checksum *checksum, const struct aws_byte_cursor *buf) {
    if (!checksum->good) {
        return aws_raise_error(AWS_ERROR_INVALID_STATE);
    }
    checksum->impl = (void *)(uintptr_t)aws_crc32_common((uint32_t)(uintptr_t)checksum->impl, buf, aws_checksums_crc32);
    return AWS_OP_SUCCESS;
}

int aws_crc32c_checksum_update(struct aws_checksum *checksum, const struct aws_byte_cursor *buf) {
    if (!checksum->good) {
        return aws_raise_error(AWS_ERROR_INVALID_STATE);
    }
    checksum->impl =
        (void *)(uintptr_t)aws_crc32_common((uint32_t)(uintptr_t)checksum->impl, buf, aws_checksums_crc32c);
    return AWS_OP_SUCCESS;
}

int aws_crc_finalize(struct aws_checksum *checksum, struct aws_byte_buf *out, size_t truncate_to) {
    if (!checksum->good) {
        return aws_raise_error(AWS_ERROR_INVALID_STATE);
    }
    checksum->good = false;
    size_t available_buffer = out->capacity - out->len;
    size_t len = checksum->digest_size;
    if (truncate_to && truncate_to < len) {
        len = truncate_to;
    }
    if (available_buffer < len) {
        return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
    }
    AWS_PRECONDITION(aws_byte_buf_is_valid(out));
    uint32_t tmp = aws_hton32((uint32_t)(uintptr_t)checksum->impl);
    if (aws_byte_buf_write(out, (uint8_t *)&tmp, len)) {
        return AWS_OP_SUCCESS;
    }
    return AWS_OP_ERR;
}

struct aws_checksum *aws_crc32_checksum_new(struct aws_allocator *allocator) {
    struct aws_checksum *checksum = aws_mem_acquire(allocator, sizeof(struct aws_checksum));
    struct aws_checksum_vtable *vtable = aws_mem_acquire(allocator, sizeof(struct aws_checksum_vtable));
    vtable->update = aws_crc32_checksum_update;
    vtable->finalize = aws_crc_finalize;
    vtable->destroy = aws_crc_destroy;
    checksum->allocator = allocator;
    checksum->impl = (void *)0;
    checksum->vtable = vtable;
    checksum->good = true;
    checksum->digest_size = AWS_CRC32_LEN;

    return checksum;
}

struct aws_checksum *aws_crc32c_checksum_new(struct aws_allocator *allocator) {
    struct aws_checksum *checksum = aws_mem_acquire(allocator, sizeof(struct aws_checksum));
    struct aws_checksum_vtable *vtable = aws_mem_acquire(allocator, sizeof(struct aws_checksum_vtable));
    vtable->update = aws_crc32c_checksum_update;
    vtable->finalize = aws_crc_finalize;
    vtable->destroy = aws_crc_destroy;
    checksum->allocator = allocator;
    checksum->impl = (void *)0;
    checksum->vtable = vtable;
    checksum->good = true;
    checksum->digest_size = AWS_CRC32_LEN;
    return checksum;
}

int aws_checksum_compute_fn(
    struct aws_allocator *allocator,
    const struct aws_byte_cursor *input,
    struct aws_byte_buf *output,
    struct aws_checksum *(*aws_crc_new)(struct aws_allocator *),
    size_t truncate_to) {
    struct aws_checksum *checksum = aws_crc_new(allocator);
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

void aws_checksum_destroy(struct aws_checksum *checksum) {
    checksum->vtable->destroy(checksum);
}

int aws_checksum_update(struct aws_checksum *checksum, const struct aws_byte_cursor *to_checksum) {
    return checksum->vtable->update(checksum, to_checksum);
}

int aws_checksum_finalize(struct aws_checksum *checksum, struct aws_byte_buf *output, size_t truncate_to) {
    return checksum->vtable->finalize(checksum, output, truncate_to);
}

int aws_checksum_compute(
    struct aws_allocator *allocator,
    enum aws_s3_checksum_algorithm algorithm,
    const struct aws_byte_cursor *input,
    struct aws_byte_buf *output,
    size_t truncate_to) {

    switch (algorithm) {
        case AWS_SHA1:
            return aws_sha1_compute(allocator, input, output, truncate_to);
        case AWS_SHA256:
            return aws_sha256_compute(allocator, input, output, truncate_to);
        case AWS_MD5:
            return aws_md5_compute(allocator, input, output, truncate_to);
        case AWS_CRC32:
            return aws_checksum_compute_fn(allocator, input, output, aws_crc32_checksum_new, truncate_to);
        case AWS_CRC32C:
            return aws_checksum_compute_fn(allocator, input, output, aws_crc32c_checksum_new, truncate_to);
        default:
            return AWS_OP_ERR;
    }
}
