#include "aws/s3/private/s3_checksums.h"
#include "aws/s3/private/s3_util.h"
#include <aws/cal/hash.h>
#include <aws/checksums/crc.h>

#define AWS_CRC32_LEN sizeof(uint32_t)
#define AWS_CRC32C_LEN sizeof(uint32_t)
#define AWS_CRC64_LEN sizeof(uint64_t)

static const struct aws_byte_cursor s_crc64nvme_algorithm_value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("CRC64NVME");
static const struct aws_byte_cursor s_crc32c_algorithm_value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("CRC32C");
static const struct aws_byte_cursor s_crc32_algorithm_value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("CRC32");
static const struct aws_byte_cursor s_sha1_algorithm_value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("SHA1");
static const struct aws_byte_cursor s_sha256_algorithm_value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("SHA256");

static const struct aws_byte_cursor s_crc64nvme_header_name =
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-checksum-crc64nvme");
static const struct aws_byte_cursor s_crc32c_header_name =
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-checksum-crc32c");
static const struct aws_byte_cursor s_crc32_header_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-checksum-crc32");
static const struct aws_byte_cursor s_sha1_header_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-checksum-sha1");
static const struct aws_byte_cursor s_sha256_header_name =
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-checksum-sha256");

static const struct aws_byte_cursor s_crc64nvme_completed_part_name =
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("ChecksumCRC64NVME");
static const struct aws_byte_cursor s_crc32c_completed_part_name =
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("ChecksumCRC32C");
static const struct aws_byte_cursor s_crc32_completed_part_name =
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("ChecksumCRC32");
static const struct aws_byte_cursor s_sha1_completed_part_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("ChecksumSHA1");
static const struct aws_byte_cursor s_sha256_completed_part_name =
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("ChecksumSHA256");
static const struct aws_byte_cursor s_empty_cursor = {
    .len = 0,
    .ptr = NULL,
};

size_t aws_get_digest_size_from_algorithm(enum aws_s3_checksum_algorithm algorithm) {
    switch (algorithm) {
        case AWS_SCA_CRC64NVME:
            return AWS_CRC64_LEN;
        case AWS_SCA_CRC32C:
            return AWS_CRC32C_LEN;
        case AWS_SCA_CRC32:
            return AWS_CRC32_LEN;
        case AWS_SCA_SHA1:
            return AWS_SHA1_LEN;
        case AWS_SCA_SHA256:
            return AWS_SHA256_LEN;
        default:
            return 0;
    }
}

struct aws_byte_cursor aws_get_http_header_name_from_algorithm(enum aws_s3_checksum_algorithm algorithm) {
    switch (algorithm) {
        case AWS_SCA_CRC64NVME:
            return s_crc64nvme_header_name;
        case AWS_SCA_CRC32C:
            return s_crc32c_header_name;
        case AWS_SCA_CRC32:
            return s_crc32_header_name;
        case AWS_SCA_SHA1:
            return s_sha1_header_name;
        case AWS_SCA_SHA256:
            return s_sha256_header_name;
        default:
            return s_empty_cursor;
    }
}

struct aws_byte_cursor aws_get_algorithm_value_from_algorithm(enum aws_s3_checksum_algorithm algorithm) {
    switch (algorithm) {
        case AWS_SCA_CRC64NVME:
            return s_crc64nvme_algorithm_value;
        case AWS_SCA_CRC32C:
            return s_crc32c_algorithm_value;
        case AWS_SCA_CRC32:
            return s_crc32_algorithm_value;
        case AWS_SCA_SHA1:
            return s_sha1_algorithm_value;
        case AWS_SCA_SHA256:
            return s_sha256_algorithm_value;
        default:
            return s_empty_cursor;
    }
}

struct aws_byte_cursor aws_get_completed_part_name_from_algorithm(enum aws_s3_checksum_algorithm algorithm) {
    switch (algorithm) {
        case AWS_SCA_CRC64NVME:
            return s_crc64nvme_completed_part_name;
        case AWS_SCA_CRC32C:
            return s_crc32c_completed_part_name;
        case AWS_SCA_CRC32:
            return s_crc32_completed_part_name;
        case AWS_SCA_SHA1:
            return s_sha1_completed_part_name;
        case AWS_SCA_SHA256:
            return s_sha256_completed_part_name;
        default:
            return s_empty_cursor;
    }
}

void s3_hash_destroy(struct aws_s3_checksum *checksum) {
    struct aws_hash *hash = (struct aws_hash *)checksum->impl;
    aws_hash_destroy(hash);
    aws_mem_release(checksum->allocator, checksum);
}

int s3_hash_update(struct aws_s3_checksum *checksum, const struct aws_byte_cursor *to_checksum) {
    struct aws_hash *hash = (struct aws_hash *)checksum->impl;
    return aws_hash_update(hash, to_checksum);
}

int s3_hash_finalize(struct aws_s3_checksum *checksum, struct aws_byte_buf *output, size_t truncate_to) {
    struct aws_hash *hash = (struct aws_hash *)checksum->impl;
    checksum->good = false;
    return aws_hash_finalize(hash, output, truncate_to);
}

static int s_crc_finalize_helper(struct aws_s3_checksum *checksum, struct aws_byte_buf *out, size_t truncate_to) {
    AWS_PRECONDITION(aws_byte_buf_is_valid(out));

    if (!checksum->good) {
        return aws_raise_error(AWS_ERROR_INVALID_STATE);
    }
    checksum->good = false;
    size_t len = checksum->digest_size;
    if (truncate_to && truncate_to < len) {
        len = truncate_to;
    }
    if (out->capacity - out->len < len) {
        return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
    }
    if (checksum->digest_size == AWS_CRC32_LEN) {
        if (aws_byte_buf_write_be32(out, *(uint32_t *)checksum->impl)) {
            return AWS_OP_SUCCESS;
        }
    } else {
        if (aws_byte_buf_write_be64(out, *(uint64_t *)checksum->impl)) {
            return AWS_OP_SUCCESS;
        }
    }
    return aws_raise_error(AWS_ERROR_INVALID_BUFFER_SIZE);
}

int aws_crc32_finalize(struct aws_s3_checksum *checksum, struct aws_byte_buf *out, size_t truncate_to) {
    return s_crc_finalize_helper(checksum, out, truncate_to);
}

int aws_crc64_finalize(struct aws_s3_checksum *checksum, struct aws_byte_buf *out, size_t truncate_to) {
    return s_crc_finalize_helper(checksum, out, truncate_to);
}

int aws_crc32_checksum_update(struct aws_s3_checksum *checksum, const struct aws_byte_cursor *buf) {
    if (!checksum->good) {
        return aws_raise_error(AWS_ERROR_INVALID_STATE);
    }
    *(uint32_t *)checksum->impl = aws_checksums_crc32_ex(buf->ptr, buf->len, *(uint32_t *)checksum->impl);
    return AWS_OP_SUCCESS;
}

int aws_crc32c_checksum_update(struct aws_s3_checksum *checksum, const struct aws_byte_cursor *buf) {
    if (!checksum->good) {
        return aws_raise_error(AWS_ERROR_INVALID_STATE);
    }
    *(uint32_t *)checksum->impl = aws_checksums_crc32c_ex(buf->ptr, buf->len, *(uint32_t *)checksum->impl);
    return AWS_OP_SUCCESS;
}

int aws_crc64nvme_checksum_update(struct aws_s3_checksum *checksum, const struct aws_byte_cursor *buf) {
    if (!checksum->good) {
        return aws_raise_error(AWS_ERROR_INVALID_STATE);
    }
    *(uint64_t *)checksum->impl = aws_checksums_crc64nvme_ex(buf->ptr, buf->len, *(uint64_t *)checksum->impl);
    return AWS_OP_SUCCESS;
}

void aws_crc_destroy(struct aws_s3_checksum *checksum) {
    aws_mem_release(checksum->allocator, checksum);
}

static struct aws_checksum_vtable hash_vtable = {
    .update = s3_hash_update,
    .finalize = s3_hash_finalize,
    .destroy = s3_hash_destroy,
};

static struct aws_checksum_vtable crc32_vtable = {
    .update = aws_crc32_checksum_update,
    .finalize = aws_crc32_finalize,
    .destroy = aws_crc_destroy,
};
static struct aws_checksum_vtable crc32c_vtable = {
    .update = aws_crc32c_checksum_update,
    .finalize = aws_crc32_finalize,
    .destroy = aws_crc_destroy,
};
static struct aws_checksum_vtable crc64nvme_vtable = {
    .update = aws_crc64nvme_checksum_update,
    .finalize = aws_crc64_finalize,
    .destroy = aws_crc_destroy,
};

struct aws_s3_checksum *aws_hash_new(struct aws_allocator *allocator, aws_hash_new_fn hash_fn) {
    struct aws_s3_checksum *checksum = aws_mem_acquire(allocator, sizeof(struct aws_s3_checksum));
    struct aws_hash *hash = hash_fn(allocator);
    checksum->impl = (void *)hash;
    checksum->allocator = allocator;
    checksum->vtable = &hash_vtable;
    checksum->good = true;
    checksum->digest_size = hash->digest_size;
    return checksum;
}

struct aws_s3_checksum *aws_crc32_checksum_new(struct aws_allocator *allocator) {
    struct aws_s3_checksum *checksum = NULL;
    uint32_t *crc32_val = NULL;
    aws_mem_acquire_many(allocator, 2, &checksum, sizeof(struct aws_s3_checksum), &crc32_val, sizeof(uint32_t));
    *crc32_val = 0;
    checksum->vtable = &crc32_vtable;
    checksum->allocator = allocator;
    checksum->impl = crc32_val;
    checksum->good = true;
    checksum->digest_size = AWS_CRC32_LEN;

    return checksum;
}

struct aws_s3_checksum *aws_crc32c_checksum_new(struct aws_allocator *allocator) {
    struct aws_s3_checksum *checksum = NULL;
    uint32_t *crc32_val = NULL;
    aws_mem_acquire_many(allocator, 2, &checksum, sizeof(struct aws_s3_checksum), &crc32_val, sizeof(uint32_t));
    *crc32_val = 0;
    checksum->vtable = &crc32c_vtable;
    checksum->allocator = allocator;
    checksum->impl = crc32_val;
    checksum->good = true;
    checksum->digest_size = AWS_CRC32C_LEN;
    return checksum;
}

struct aws_s3_checksum *aws_crc64nvme_checksum_new(struct aws_allocator *allocator) {
    struct aws_s3_checksum *checksum = NULL;
    uint64_t *crc64_val = NULL;
    aws_mem_acquire_many(allocator, 2, &checksum, sizeof(struct aws_s3_checksum), &crc64_val, sizeof(uint64_t));
    *crc64_val = 0;
    checksum->vtable = &crc64nvme_vtable;
    checksum->allocator = allocator;
    checksum->impl = crc64_val;
    checksum->good = true;
    checksum->digest_size = AWS_CRC64_LEN;
    return checksum;
}

struct aws_s3_checksum *aws_checksum_new(struct aws_allocator *allocator, enum aws_s3_checksum_algorithm algorithm) {
    struct aws_s3_checksum *checksum = NULL;
    switch (algorithm) {
        case AWS_SCA_CRC64NVME:
            checksum = aws_crc64nvme_checksum_new(allocator);
            break;
        case AWS_SCA_CRC32C:
            checksum = aws_crc32c_checksum_new(allocator);
            break;
        case AWS_SCA_CRC32:
            checksum = aws_crc32_checksum_new(allocator);
            break;
        case AWS_SCA_SHA1:
            checksum = aws_hash_new(allocator, aws_sha1_new);
            break;
        case AWS_SCA_SHA256:
            checksum = aws_hash_new(allocator, aws_sha256_new);
            break;
        default:
            return NULL;
    }
    checksum->algorithm = algorithm;
    return checksum;
}

int aws_checksum_compute_fn(
    struct aws_allocator *allocator,
    const struct aws_byte_cursor *input,
    struct aws_byte_buf *output,
    struct aws_s3_checksum *(*aws_crc_new)(struct aws_allocator *),
    size_t truncate_to) {
    struct aws_s3_checksum *checksum = aws_crc_new(allocator);
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

void aws_checksum_destroy(struct aws_s3_checksum *checksum) {
    if (checksum != NULL) {
        checksum->vtable->destroy(checksum);
    }
}

int aws_checksum_update(struct aws_s3_checksum *checksum, const struct aws_byte_cursor *to_checksum) {
    return checksum->vtable->update(checksum, to_checksum);
}

int aws_checksum_finalize(struct aws_s3_checksum *checksum, struct aws_byte_buf *output, size_t truncate_to) {
    return checksum->vtable->finalize(checksum, output, truncate_to);
}

int aws_checksum_compute(
    struct aws_allocator *allocator,
    enum aws_s3_checksum_algorithm algorithm,
    const struct aws_byte_cursor *input,
    struct aws_byte_buf *output,
    size_t truncate_to) {

    switch (algorithm) {
        case AWS_SCA_SHA1:
            return aws_sha1_compute(allocator, input, output, truncate_to);
        case AWS_SCA_SHA256:
            return aws_sha256_compute(allocator, input, output, truncate_to);
        case AWS_SCA_CRC64NVME:
            return aws_checksum_compute_fn(allocator, input, output, aws_crc64nvme_checksum_new, truncate_to);
        case AWS_SCA_CRC32:
            return aws_checksum_compute_fn(allocator, input, output, aws_crc32_checksum_new, truncate_to);
        case AWS_SCA_CRC32C:
            return aws_checksum_compute_fn(allocator, input, output, aws_crc32c_checksum_new, truncate_to);
        default:
            return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
    }
}

void checksum_config_init(struct checksum_config *internal_config, const struct aws_s3_checksum_config *config) {
    AWS_ZERO_STRUCT(*internal_config);
    if (!config) {
        return;
    }
    internal_config->checksum_algorithm = config->checksum_algorithm;
    internal_config->location = config->location;
    internal_config->validate_response_checksum = config->validate_response_checksum;

    if (config->validate_checksum_algorithms) {
        const size_t count = aws_array_list_length(config->validate_checksum_algorithms);
        for (size_t i = 0; i < count; ++i) {
            enum aws_s3_checksum_algorithm algorithm;
            aws_array_list_get_at(config->validate_checksum_algorithms, &algorithm, i);
            switch (algorithm) {
                case AWS_SCA_CRC64NVME:
                    internal_config->response_checksum_algorithms.crc64nvme = true;
                    break;
                case AWS_SCA_CRC32C:
                    internal_config->response_checksum_algorithms.crc32c = true;
                    break;
                case AWS_SCA_CRC32:
                    internal_config->response_checksum_algorithms.crc32 = true;
                    break;
                case AWS_SCA_SHA1:
                    internal_config->response_checksum_algorithms.sha1 = true;
                    break;
                case AWS_SCA_SHA256:
                    internal_config->response_checksum_algorithms.sha256 = true;
                    break;
                default:
                    break;
            }
        }

    } else if (config->validate_response_checksum) {
        internal_config->response_checksum_algorithms.crc64nvme = true;
        internal_config->response_checksum_algorithms.crc32 = true;
        internal_config->response_checksum_algorithms.crc32c = true;
        internal_config->response_checksum_algorithms.sha1 = true;
        internal_config->response_checksum_algorithms.sha256 = true;
    }
}
