#include "aws/s3/private/s3_checksums.h"
#include "aws/s3/private/s3_util.h"
#include <aws/cal/hash.h>
#include <aws/checksums/crc.h>
#include <aws/checksums/xxhash.h>
#include <aws/http/request_response.h>

#define AWS_CRC32_LEN sizeof(uint32_t)
#define AWS_CRC32C_LEN sizeof(uint32_t)
#define AWS_CRC64_LEN sizeof(uint64_t)

enum {
    AWS_XXHASH64_LEN = 8,
    AWS_XXHASH3_64_LEN = 8,
    AWS_XXHASH3_128_LEN = 16,
};

static const struct aws_byte_cursor s_crc64nvme_algorithm_value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("CRC64NVME");
static const struct aws_byte_cursor s_crc32c_algorithm_value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("CRC32C");
static const struct aws_byte_cursor s_crc32_algorithm_value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("CRC32");
static const struct aws_byte_cursor s_sha1_algorithm_value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("SHA1");
static const struct aws_byte_cursor s_sha256_algorithm_value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("SHA256");
static const struct aws_byte_cursor s_sha512_algorithm_value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("SHA512");
static const struct aws_byte_cursor s_xxhash64_algorithm_value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("XXHASH64");
static const struct aws_byte_cursor s_xxhash3_64_algorithm_value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("XXHASH3");
static const struct aws_byte_cursor s_xxhash3_128_algorithm_value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("XXHASH128");

static const struct aws_byte_cursor s_crc64nvme_header_name =
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-checksum-crc64nvme");
static const struct aws_byte_cursor s_crc32c_header_name =
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-checksum-crc32c");
static const struct aws_byte_cursor s_crc32_header_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-checksum-crc32");
static const struct aws_byte_cursor s_sha1_header_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-checksum-sha1");
static const struct aws_byte_cursor s_sha256_header_name =
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-checksum-sha256");
static const struct aws_byte_cursor s_sha512_header_name =
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-checksum-sha512");
static const struct aws_byte_cursor s_xxhash64_header_name =
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-checksum-xxhash64");
static const struct aws_byte_cursor s_xxhash3_64_header_name =
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-checksum-xxhash3");
static const struct aws_byte_cursor s_xxhash3_128_header_name =
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-checksum-xxhash128");

static const struct aws_byte_cursor s_crc64nvme_completed_part_name =
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("ChecksumCRC64NVME");
static const struct aws_byte_cursor s_crc32c_completed_part_name =
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("ChecksumCRC32C");
static const struct aws_byte_cursor s_crc32_completed_part_name =
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("ChecksumCRC32");
static const struct aws_byte_cursor s_sha1_completed_part_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("ChecksumSHA1");
static const struct aws_byte_cursor s_sha256_completed_part_name =
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("ChecksumSHA256");
static const struct aws_byte_cursor s_sha512_completed_part_name =
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("ChecksumSHA512");
static const struct aws_byte_cursor s_xxhash64_completed_part_name =
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("ChecksumXXHASH64");
static const struct aws_byte_cursor s_xxhash3_64_completed_part_name =
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("ChecksumXXHASH3");
static const struct aws_byte_cursor s_xxhash3_128_completed_part_name =
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("ChecksumXXHASH128");
static const struct aws_byte_cursor s_empty_cursor = {
    .len = 0,
    .ptr = NULL,
};

size_t aws_get_digest_size_from_checksum_algorithm(enum aws_s3_checksum_algorithm algorithm) {
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
        case AWS_SCA_SHA512:
            return AWS_SHA512_LEN;
        case AWS_SCA_XXHASH64:
            return AWS_XXHASH64_LEN;
        case AWS_SCA_XXHASH3_64:
            return AWS_XXHASH3_64_LEN;
        case AWS_SCA_XXHASH3_128:
            return AWS_XXHASH3_128_LEN;
        default:
            return 0;
    }
}

struct aws_byte_cursor aws_get_http_header_name_from_checksum_algorithm(enum aws_s3_checksum_algorithm algorithm) {
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
        case AWS_SCA_SHA512:
            return s_sha512_header_name;
        case AWS_SCA_XXHASH64:
            return s_xxhash64_header_name;
        case AWS_SCA_XXHASH3_64:
            return s_xxhash3_64_header_name;
        case AWS_SCA_XXHASH3_128:
            return s_xxhash3_128_header_name;
        default:
            return s_empty_cursor;
    }
}

struct aws_byte_cursor aws_get_checksum_algorithm_name(enum aws_s3_checksum_algorithm algorithm) {
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
        case AWS_SCA_SHA512:
            return s_sha512_algorithm_value;
        case AWS_SCA_XXHASH64:
            return s_xxhash64_algorithm_value;
        case AWS_SCA_XXHASH3_64:
            return s_xxhash3_64_algorithm_value;
        case AWS_SCA_XXHASH3_128:
            return s_xxhash3_128_algorithm_value;
        default:
            return s_empty_cursor;
    }
}

struct aws_byte_cursor aws_get_completed_part_name_from_checksum_algorithm(enum aws_s3_checksum_algorithm algorithm) {
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
        case AWS_SCA_SHA512:
            return s_sha512_completed_part_name;
        case AWS_SCA_XXHASH64:
            return s_xxhash64_completed_part_name;
        case AWS_SCA_XXHASH3_64:
            return s_xxhash3_64_completed_part_name;
        case AWS_SCA_XXHASH3_128:
            return s_xxhash3_128_completed_part_name;
        default:
            return s_empty_cursor;
    }
}

static void s_hash_destroy(struct aws_s3_checksum *checksum) {
    struct aws_hash *hash = checksum->impl.hash;
    aws_hash_destroy(hash);
    aws_mem_release(checksum->allocator, checksum);
}

static int s_hash_update(struct aws_s3_checksum *checksum, const struct aws_byte_cursor *to_checksum) {
    return aws_hash_update(checksum->impl.hash, to_checksum);
}

static int s_hash_finalize(struct aws_s3_checksum *checksum, struct aws_byte_buf *output) {
    checksum->good = false;
    return aws_hash_finalize(checksum->impl.hash, output, 0);
}

static void s_xxhash_destroy(struct aws_s3_checksum *checksum) {
    struct aws_xxhash *hash = checksum->impl.xxhash;
    aws_xxhash_destroy(hash);
    aws_mem_release(checksum->allocator, checksum);
}

static int s_xxhash_update(struct aws_s3_checksum *checksum, const struct aws_byte_cursor *to_checksum) {
    return aws_xxhash_update(checksum->impl.xxhash, *to_checksum);
}

static int s_xxhash_finalize(struct aws_s3_checksum *checksum, struct aws_byte_buf *output) {
    checksum->good = false;
    return aws_xxhash_finalize(checksum->impl.xxhash, output);
}

static int s_crc_finalize_helper(struct aws_s3_checksum *checksum, struct aws_byte_buf *out) {
    AWS_PRECONDITION(aws_byte_buf_is_valid(out));

    if (!checksum->good) {
        return aws_raise_error(AWS_ERROR_INVALID_STATE);
    }
    checksum->good = false;
    size_t len = checksum->digest_size;
    if (out->capacity - out->len < len) {
        return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
    }
    if (checksum->digest_size == AWS_CRC32_LEN) {
        if (aws_byte_buf_write_be32(out, checksum->impl.crc_val_32bit)) {
            return AWS_OP_SUCCESS;
        }
    } else {
        if (aws_byte_buf_write_be64(out, checksum->impl.crc_val_64bit)) {
            return AWS_OP_SUCCESS;
        }
    }
    return aws_raise_error(AWS_ERROR_INVALID_BUFFER_SIZE);
}

static int s_crc32_finalize(struct aws_s3_checksum *checksum, struct aws_byte_buf *out) {
    return s_crc_finalize_helper(checksum, out);
}

static int s_crc64_finalize(struct aws_s3_checksum *checksum, struct aws_byte_buf *out) {
    return s_crc_finalize_helper(checksum, out);
}

static int s_crc32_checksum_update(struct aws_s3_checksum *checksum, const struct aws_byte_cursor *buf) {
    if (!checksum->good) {
        return aws_raise_error(AWS_ERROR_INVALID_STATE);
    }
    checksum->impl.crc_val_32bit = aws_checksums_crc32_ex(buf->ptr, buf->len, checksum->impl.crc_val_32bit);
    return AWS_OP_SUCCESS;
}

static int s_crc32c_checksum_update(struct aws_s3_checksum *checksum, const struct aws_byte_cursor *buf) {
    if (!checksum->good) {
        return aws_raise_error(AWS_ERROR_INVALID_STATE);
    }
    checksum->impl.crc_val_32bit = aws_checksums_crc32c_ex(buf->ptr, buf->len, checksum->impl.crc_val_32bit);
    return AWS_OP_SUCCESS;
}

static int s_crc64nvme_checksum_update(struct aws_s3_checksum *checksum, const struct aws_byte_cursor *buf) {
    if (!checksum->good) {
        return aws_raise_error(AWS_ERROR_INVALID_STATE);
    }
    checksum->impl.crc_val_64bit = aws_checksums_crc64nvme_ex(buf->ptr, buf->len, checksum->impl.crc_val_64bit);
    return AWS_OP_SUCCESS;
}

static void s_crc_destroy(struct aws_s3_checksum *checksum) {
    aws_mem_release(checksum->allocator, checksum);
}

static struct aws_checksum_vtable hash_vtable = {
    .update = s_hash_update,
    .finalize = s_hash_finalize,
    .destroy = s_hash_destroy,
};

static struct aws_checksum_vtable s_xxhash_vtable = {
    .update = s_xxhash_update,
    .finalize = s_xxhash_finalize,
    .destroy = s_xxhash_destroy,
};

static struct aws_checksum_vtable crc32_vtable = {
    .update = s_crc32_checksum_update,
    .finalize = s_crc32_finalize,
    .destroy = s_crc_destroy,
};
static struct aws_checksum_vtable crc32c_vtable = {
    .update = s_crc32c_checksum_update,
    .finalize = s_crc32_finalize,
    .destroy = s_crc_destroy,
};
static struct aws_checksum_vtable crc64nvme_vtable = {
    .update = s_crc64nvme_checksum_update,
    .finalize = s_crc64_finalize,
    .destroy = s_crc_destroy,
};

struct aws_s3_checksum *aws_hash_new(struct aws_allocator *allocator, aws_hash_new_fn hash_fn) {
    struct aws_s3_checksum *checksum = aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_checksum));
    struct aws_hash *hash = hash_fn(allocator);
    if (!hash) {
        aws_mem_release(allocator, checksum);
        aws_raise_error(aws_last_error_or_unknown());
        return NULL;
    }
    checksum->impl.hash = hash;
    checksum->allocator = allocator;
    checksum->vtable = &hash_vtable;
    checksum->good = true;
    checksum->digest_size = hash->digest_size;
    return checksum;
}

typedef struct aws_xxhash *(aws_xxhash_new_fn)(struct aws_allocator *allocator, uint64_t seed);

struct aws_s3_checksum *s_aws_xxhash_new(
    struct aws_allocator *allocator,
    aws_xxhash_new_fn hash_fn,
    size_t digest_size) {
    struct aws_s3_checksum *checksum = aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_checksum));
    struct aws_xxhash *hash = hash_fn(allocator, 0);
    if (!hash) {
        aws_mem_release(allocator, checksum);
        aws_raise_error(aws_last_error_or_unknown());
        return NULL;
    }
    checksum->impl.xxhash = hash;
    checksum->allocator = allocator;
    checksum->vtable = &s_xxhash_vtable;
    checksum->good = true;
    checksum->digest_size = digest_size;
    return checksum;
}

static struct aws_s3_checksum *s_crc32_checksum_new(struct aws_allocator *allocator) {
    struct aws_s3_checksum *checksum = aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_checksum));
    checksum->vtable = &crc32_vtable;
    checksum->allocator = allocator;
    checksum->impl.crc_val_32bit = 0;
    checksum->good = true;
    checksum->digest_size = AWS_CRC32_LEN;

    return checksum;
}

static struct aws_s3_checksum *s_crc32c_checksum_new(struct aws_allocator *allocator) {
    struct aws_s3_checksum *checksum = aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_checksum));
    checksum->vtable = &crc32c_vtable;
    checksum->allocator = allocator;
    checksum->impl.crc_val_32bit = 0;
    checksum->good = true;
    checksum->digest_size = AWS_CRC32C_LEN;
    return checksum;
}

static struct aws_s3_checksum *s_crc64nvme_checksum_new(struct aws_allocator *allocator) {
    struct aws_s3_checksum *checksum = aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_checksum));
    checksum->vtable = &crc64nvme_vtable;
    checksum->allocator = allocator;
    checksum->impl.crc_val_64bit = 0;
    checksum->good = true;
    checksum->digest_size = AWS_CRC64_LEN;
    return checksum;
}

struct aws_s3_checksum *aws_checksum_new(struct aws_allocator *allocator, enum aws_s3_checksum_algorithm algorithm) {
    struct aws_s3_checksum *checksum = NULL;
    switch (algorithm) {
        case AWS_SCA_CRC64NVME:
            checksum = s_crc64nvme_checksum_new(allocator);
            break;
        case AWS_SCA_CRC32C:
            checksum = s_crc32c_checksum_new(allocator);
            break;
        case AWS_SCA_CRC32:
            checksum = s_crc32_checksum_new(allocator);
            break;
        case AWS_SCA_SHA1:
            checksum = aws_hash_new(allocator, aws_sha1_new);
            break;
        case AWS_SCA_SHA256:
            checksum = aws_hash_new(allocator, aws_sha256_new);
            break;
        case AWS_SCA_SHA512:
            checksum = aws_hash_new(allocator, aws_sha512_new);
            break;
        case AWS_SCA_XXHASH64:
            checksum = s_aws_xxhash_new(allocator, aws_xxhash64_new, AWS_XXHASH64_LEN);
            break;
        case AWS_SCA_XXHASH3_64:
            checksum = s_aws_xxhash_new(allocator, aws_xxhash3_64_new, AWS_XXHASH3_64_LEN);
            break;
        case AWS_SCA_XXHASH3_128:
            checksum = s_aws_xxhash_new(allocator, aws_xxhash3_128_new, AWS_XXHASH3_128_LEN);
            break;
        default:
            return NULL;
    }

    if (checksum != NULL) {
        checksum->algorithm = algorithm;
    }
    return checksum;
}

void aws_checksum_destroy(struct aws_s3_checksum *checksum) {
    if (checksum != NULL) {
        checksum->vtable->destroy(checksum);
    }
}

int aws_checksum_update(struct aws_s3_checksum *checksum, const struct aws_byte_cursor *to_checksum) {
    AWS_PRECONDITION(checksum);
    return checksum->vtable->update(checksum, to_checksum);
}

int aws_checksum_finalize(struct aws_s3_checksum *checksum, struct aws_byte_buf *output) {
    AWS_PRECONDITION(checksum);
    return checksum->vtable->finalize(checksum, output);
}

static int s_checksum_compute_fn(
    struct aws_allocator *allocator,
    const struct aws_byte_cursor *input,
    struct aws_byte_buf *output,
    struct aws_s3_checksum *(*s_crc_new)(struct aws_allocator *)) {
    struct aws_s3_checksum *checksum = s_crc_new(allocator);
    if (aws_checksum_update(checksum, input)) {
        aws_checksum_destroy(checksum);
        return AWS_OP_ERR;
    }
    if (aws_checksum_finalize(checksum, output)) {
        aws_checksum_destroy(checksum);
        return AWS_OP_ERR;
    }
    aws_checksum_destroy(checksum);
    return AWS_OP_SUCCESS;
}

int aws_checksum_compute(
    struct aws_allocator *allocator,
    enum aws_s3_checksum_algorithm algorithm,
    const struct aws_byte_cursor *input,
    struct aws_byte_buf *output) {

    switch (algorithm) {
        case AWS_SCA_SHA1:
            return aws_sha1_compute(allocator, input, output, 0);
        case AWS_SCA_SHA256:
            return aws_sha256_compute(allocator, input, output, 0);
        case AWS_SCA_SHA512:
            return aws_sha512_compute(allocator, input, output, 0);
        case AWS_SCA_XXHASH64:
            return aws_xxhash64_compute(0, *input, output);
        case AWS_SCA_XXHASH3_64:
            return aws_xxhash3_64_compute(0, *input, output);
        case AWS_SCA_XXHASH3_128:
            return aws_xxhash3_128_compute(0, *input, output);
        case AWS_SCA_CRC64NVME:
            return s_checksum_compute_fn(allocator, input, output, s_crc64nvme_checksum_new);
        case AWS_SCA_CRC32:
            return s_checksum_compute_fn(allocator, input, output, s_crc32_checksum_new);
        case AWS_SCA_CRC32C:
            return s_checksum_compute_fn(allocator, input, output, s_crc32c_checksum_new);
        default:
            return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
    }
}

static const struct aws_byte_cursor s_checksum_prefix = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-checksum-");

static int s_init_and_verify_checksum_config_from_headers(
    struct aws_s3_meta_request_checksum_config_storage *checksum_config,
    const struct aws_http_message *message,
    const void *log_id) {
    /* Check if the checksum header was set from the message */
    struct aws_http_headers *headers = aws_http_message_get_headers(message);
    enum aws_s3_checksum_algorithm header_algo = AWS_SCA_NONE;
    struct aws_byte_cursor header_value;
    AWS_ZERO_STRUCT(header_value);

    bool has_checksum_header = false;
    for (size_t i = 0; i < aws_http_headers_count(headers); ++i) {
        struct aws_http_header header;
        if (aws_http_headers_get_index(headers, i, &header)) {
            return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        }

        if (aws_byte_cursor_starts_with_ignore_case(&header->name, &s_checksum_prefix)) {
            has_checksum_header = true;
            break;
        }
    }

    if (has_checksum_header) {
        for (size_t i = 0; i < AWS_ARRAY_SIZE(s_checksum_algo_priority_list); i++) {
            enum aws_s3_checksum_algorithm algorithm = s_checksum_algo_priority_list[i];
            const struct aws_byte_cursor algorithm_header_name =
                aws_get_http_header_name_from_checksum_algorithm(algorithm);
            if (aws_http_headers_get(headers, algorithm_header_name, &header_value) == AWS_OP_SUCCESS) {
                if (header_algo == AWS_SCA_NONE) {
                    header_algo = algorithm;
                } else {
                    /* If there are multiple checksum headers set, it's malformed request */
                    AWS_LOGF_ERROR(
                        AWS_LS_S3_META_REQUEST,
                        "id=%p Could not create auto-ranged-put meta request; multiple checksum headers has been set",
                        log_id);
                    return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
                }
            }
        }
    }

    if (!has_checksum_header) {
        /* No checksum header found, done */
        return AWS_OP_SUCCESS;
    }

    if (header_algo == AWS_SCA_NONE) {
        header_algo = AWS_SCA_UNKNOWN;
    }

    if (checksum_config->has_full_object_checksum) {
        /* If the full object checksum has been set, it's malformed request */
        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST,
            "id=%p: Could not create auto-ranged-put meta request; full object checksum is set from multiple ways.",
            log_id);
        return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
    }

    AWS_LOGF_DEBUG(
        AWS_LS_S3_META_REQUEST,
        "id=%p Setting the full-object checksum from header; algorithm: " PRInSTR ", value: " PRInSTR ".",
        log_id,
        AWS_BYTE_CURSOR_PRI(aws_get_checksum_algorithm_name(header_algo)),
        AWS_BYTE_CURSOR_PRI(header_value));
    /* Set algo */
    checksum_config->checksum_algorithm = header_algo;
    /**
     * Set the location to NONE to avoid adding extra checksums from client.
     *
     * Notes: The multipart upload will set the location to trailer to add parts level checksums.
     **/
    checksum_config->location = AWS_SCL_NONE;

    /* Set full object checksum from the header value. */
    aws_byte_buf_init_copy_from_cursor(
        &checksum_config->full_object_checksum, checksum_config->allocator, header_value);
    checksum_config->has_full_object_checksum = true;
    return AWS_OP_SUCCESS;
}

int aws_s3_meta_request_checksum_config_storage_init(
    struct aws_allocator *allocator,
    struct aws_s3_meta_request_checksum_config_storage *internal_config,
    const struct aws_s3_checksum_config *config,
    const struct aws_http_message *message,
    const void *log_id) {
    AWS_ZERO_STRUCT(*internal_config);
    /* Zero out the struct and set the allocator regardless. */
    internal_config->allocator = allocator;

    if (!config) {
        return AWS_OP_SUCCESS;
    }

    struct aws_http_headers *headers = aws_http_message_get_headers(message);
    if (config->location == AWS_SCL_TRAILER) {
        struct aws_byte_cursor existing_encoding;
        AWS_ZERO_STRUCT(existing_encoding);
        if (aws_http_headers_get(headers, g_content_encoding_header_name, &existing_encoding) == AWS_OP_SUCCESS) {
            if (aws_byte_cursor_find_exact(&existing_encoding, &g_content_encoding_header_aws_chunked, NULL) ==
                AWS_OP_SUCCESS) {
                AWS_LOGF_ERROR(
                    AWS_LS_S3_META_REQUEST,
                    "id=%p Cannot create meta s3 request; for trailer checksum, the original request cannot be "
                    "aws-chunked encoding. The client will encode the request instead.",
                    (void *)log_id);
                aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
                return AWS_OP_ERR;
            }
        }
    }
    if (config->location != AWS_SCL_NONE && config->checksum_algorithm == AWS_SCA_NONE) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST,
            "id=%p Cannot create meta s3 request; checksum location is set, but no checksum algorithm selected.",
            (void *)log_id);
        aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        return AWS_OP_ERR;
    }

    internal_config->checksum_algorithm = config->checksum_algorithm;
    internal_config->location = config->location;
    internal_config->validate_response_checksum = config->validate_response_checksum;

    internal_config->full_object_checksum_callback = config->full_object_checksum_callback;
    internal_config->user_data = config->user_data;
    if (internal_config->full_object_checksum_callback) {
        /* allocate the full object checksum when the callback was set. */
        internal_config->has_full_object_checksum = true;
    }

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
                case AWS_SCA_SHA512:
                    internal_config->response_checksum_algorithms.sha512 = true;
                    break;
                case AWS_SCA_XXHASH64:
                    internal_config->response_checksum_algorithms.xxhash64 = true;
                    break;
                case AWS_SCA_XXHASH3_64:
                    internal_config->response_checksum_algorithms.xxhash3_64 = true;
                    break;
                case AWS_SCA_XXHASH3_128:
                    internal_config->response_checksum_algorithms.xxhash3_128 = true;
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
        internal_config->response_checksum_algorithms.sha512 = true;
        internal_config->response_checksum_algorithms.xxhash64 = true;
        internal_config->response_checksum_algorithms.xxhash3_64 = true;
        internal_config->response_checksum_algorithms.xxhash3_128 = true;
    }

    /* After applying settings from config, check the message header to override the corresponding settings. */
    if (s_init_and_verify_checksum_config_from_headers(internal_config, message, log_id)) {
        return AWS_OP_ERR;
    }
    /* Anything fail afterward will need to cleanup the storage. */

    return AWS_OP_SUCCESS;
}

void aws_s3_meta_request_checksum_config_storage_cleanup(
    struct aws_s3_meta_request_checksum_config_storage *internal_config) {
    if (internal_config->has_full_object_checksum) {
        aws_byte_buf_clean_up(&internal_config->full_object_checksum);
    }
}
