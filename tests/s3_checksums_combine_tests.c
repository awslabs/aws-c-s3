/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */
#include "aws/s3/private/s3_checksums.h"
#include <aws/common/byte_buf.h>
#include <aws/testing/aws_test_harness.h>

/* Computes checksum(input) in one shot, for comparison against a combined result. */
static int s_compute_whole(
    struct aws_allocator *allocator,
    enum aws_s3_checksum_algorithm algorithm,
    struct aws_byte_cursor input,
    struct aws_byte_buf *out) {

    aws_byte_buf_init(out, allocator, aws_get_digest_size_from_checksum_algorithm(algorithm));
    return aws_checksum_compute(allocator, algorithm, &input, out);
}

/* Folds `tail` into `head` the way the download path does: finalize the tail to get its digest, then
 * fold that digest in. Consumes `tail`. */
static int s_combine_checksums(struct aws_s3_checksum *head, struct aws_s3_checksum *tail, uint64_t tail_length) {
    uint8_t digest_storage[AWS_S3_COMBINABLE_DIGEST_MAX_LEN] = {0};
    struct aws_byte_buf tail_digest = aws_byte_buf_from_empty_array(digest_storage, sizeof(digest_storage));
    if (aws_checksum_finalize(tail, &tail_digest)) {
        return AWS_OP_ERR;
    }
    return aws_checksum_combine_digest(head, aws_byte_cursor_from_buf(&tail_digest), tail_length);
}

/* Splits input at `split`, checksums each half independently, combines them, and asserts the
 * result equals the checksum of the whole input. */
static int s_verify_combine_at_split(
    struct aws_allocator *allocator,
    enum aws_s3_checksum_algorithm algorithm,
    struct aws_byte_cursor input,
    size_t split) {

    AWS_FATAL_ASSERT(split <= input.len);

    struct aws_byte_cursor head_bytes = aws_byte_cursor_from_array(input.ptr, split);
    struct aws_byte_cursor tail_bytes = aws_byte_cursor_from_array(input.ptr + split, input.len - split);

    struct aws_s3_checksum *head = aws_checksum_new(allocator, algorithm);
    struct aws_s3_checksum *tail = aws_checksum_new(allocator, algorithm);
    ASSERT_NOT_NULL(head);
    ASSERT_NOT_NULL(tail);

    ASSERT_SUCCESS(aws_checksum_update(head, &head_bytes));
    ASSERT_SUCCESS(aws_checksum_update(tail, &tail_bytes));
    ASSERT_SUCCESS(s_combine_checksums(head, tail, tail_bytes.len));

    struct aws_byte_buf combined;
    aws_byte_buf_init(&combined, allocator, aws_get_digest_size_from_checksum_algorithm(algorithm));
    ASSERT_SUCCESS(aws_checksum_finalize(head, &combined));

    struct aws_byte_buf expected;
    ASSERT_SUCCESS(s_compute_whole(allocator, algorithm, input, &expected));

    ASSERT_BIN_ARRAYS_EQUALS(expected.buffer, expected.len, combined.buffer, combined.len);

    aws_byte_buf_clean_up(&expected);
    aws_byte_buf_clean_up(&combined);
    aws_checksum_destroy(tail);
    aws_checksum_destroy(head);

    return AWS_OP_SUCCESS;
}

/* Exercises every split offset, including the degenerate 0 and input.len cases. */
static int s_verify_combine_all_splits(
    struct aws_allocator *allocator,
    enum aws_s3_checksum_algorithm algorithm,
    struct aws_byte_cursor input) {

    for (size_t split = 0; split <= input.len; ++split) {
        if (s_verify_combine_at_split(allocator, algorithm, input, split)) {
            return AWS_OP_ERR;
        }
    }
    return AWS_OP_SUCCESS;
}

static struct aws_byte_cursor s_combine_test_input(void) {
    return aws_byte_cursor_from_c_str("abcdefghbcdefghicdefghijdefghijkefghijklfghijklmghijklmn"
                                      "hijklmnoijklmnopjklmnopqklmnopqrlmnopqrsmnopqrstnopqrstu");
}

static int s_checksum_combine_crc32_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    aws_s3_library_init(allocator);
    ASSERT_SUCCESS(s_verify_combine_all_splits(allocator, AWS_SCA_CRC32, s_combine_test_input()));
    aws_s3_library_clean_up();
    return AWS_OP_SUCCESS;
}
AWS_TEST_CASE(checksum_combine_crc32, s_checksum_combine_crc32_fn)

static int s_checksum_combine_crc32c_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    aws_s3_library_init(allocator);
    ASSERT_SUCCESS(s_verify_combine_all_splits(allocator, AWS_SCA_CRC32C, s_combine_test_input()));
    aws_s3_library_clean_up();
    return AWS_OP_SUCCESS;
}
AWS_TEST_CASE(checksum_combine_crc32c, s_checksum_combine_crc32c_fn)

static int s_checksum_combine_crc64nvme_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    aws_s3_library_init(allocator);
    ASSERT_SUCCESS(s_verify_combine_all_splits(allocator, AWS_SCA_CRC64NVME, s_combine_test_input()));
    aws_s3_library_clean_up();
    return AWS_OP_SUCCESS;
}
AWS_TEST_CASE(checksum_combine_crc64nvme, s_checksum_combine_crc64nvme_fn)

/* Combining N blocks left-to-right must match the whole-buffer checksum. This is the shape the
 * download path uses: one accumulator folded forward part by part. */
static int s_checksum_combine_many_blocks_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    aws_s3_library_init(allocator);

    const enum aws_s3_checksum_algorithm algorithms[] = {AWS_SCA_CRC32, AWS_SCA_CRC32C, AWS_SCA_CRC64NVME};
    /* Deliberately uneven block sizes, mirroring a final short part. */
    const size_t block_sizes[] = {1, 7, 16, 3, 64, 21};

    uint8_t input_bytes[112];
    for (size_t i = 0; i < sizeof(input_bytes); ++i) {
        input_bytes[i] = (uint8_t)(i * 31 + 7);
    }
    struct aws_byte_cursor input = aws_byte_cursor_from_array(input_bytes, sizeof(input_bytes));

    for (size_t algo_i = 0; algo_i < AWS_ARRAY_SIZE(algorithms); ++algo_i) {
        enum aws_s3_checksum_algorithm algorithm = algorithms[algo_i];
        size_t digest_size = aws_get_digest_size_from_checksum_algorithm(algorithm);

        struct aws_s3_checksum *accumulator = aws_checksum_new(allocator, algorithm);
        ASSERT_NOT_NULL(accumulator);

        struct aws_byte_cursor remaining = input;
        for (size_t block_i = 0; block_i < AWS_ARRAY_SIZE(block_sizes) && remaining.len > 0; ++block_i) {
            size_t block_len = aws_min_size(block_sizes[block_i], remaining.len);
            struct aws_byte_cursor block = aws_byte_cursor_from_array(remaining.ptr, block_len);

            struct aws_s3_checksum *block_sum = aws_checksum_new(allocator, algorithm);
            ASSERT_NOT_NULL(block_sum);
            ASSERT_SUCCESS(aws_checksum_update(block_sum, &block));
            ASSERT_SUCCESS(s_combine_checksums(accumulator, block_sum, block_len));
            aws_checksum_destroy(block_sum);

            aws_byte_cursor_advance(&remaining, block_len);
        }
        /* Whatever the block sizes did not cover, fold in as one final block. */
        if (remaining.len > 0) {
            struct aws_s3_checksum *block_sum = aws_checksum_new(allocator, algorithm);
            ASSERT_NOT_NULL(block_sum);
            ASSERT_SUCCESS(aws_checksum_update(block_sum, &remaining));
            ASSERT_SUCCESS(s_combine_checksums(accumulator, block_sum, remaining.len));
            aws_checksum_destroy(block_sum);
        }

        struct aws_byte_buf combined;
        aws_byte_buf_init(&combined, allocator, digest_size);
        ASSERT_SUCCESS(aws_checksum_finalize(accumulator, &combined));

        struct aws_byte_buf expected;
        ASSERT_SUCCESS(s_compute_whole(allocator, algorithm, input, &expected));
        ASSERT_BIN_ARRAYS_EQUALS(expected.buffer, expected.len, combined.buffer, combined.len);

        aws_byte_buf_clean_up(&expected);
        aws_byte_buf_clean_up(&combined);
        aws_checksum_destroy(accumulator);
    }

    aws_s3_library_clean_up();
    return AWS_OP_SUCCESS;
}
AWS_TEST_CASE(checksum_combine_many_blocks, s_checksum_combine_many_blocks_fn)

/* A fresh accumulator is the identity element, so folding a block into it must yield that
 * block's own checksum. The download path relies on this for the first part. */
static int s_checksum_combine_identity_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    aws_s3_library_init(allocator);

    struct aws_byte_cursor input = s_combine_test_input();
    const enum aws_s3_checksum_algorithm algorithms[] = {AWS_SCA_CRC32, AWS_SCA_CRC32C, AWS_SCA_CRC64NVME};

    for (size_t i = 0; i < AWS_ARRAY_SIZE(algorithms); ++i) {
        enum aws_s3_checksum_algorithm algorithm = algorithms[i];

        struct aws_s3_checksum *accumulator = aws_checksum_new(allocator, algorithm);
        struct aws_s3_checksum *block_sum = aws_checksum_new(allocator, algorithm);
        ASSERT_NOT_NULL(accumulator);
        ASSERT_NOT_NULL(block_sum);

        ASSERT_SUCCESS(aws_checksum_update(block_sum, &input));
        ASSERT_SUCCESS(s_combine_checksums(accumulator, block_sum, input.len));

        struct aws_byte_buf combined;
        aws_byte_buf_init(&combined, allocator, aws_get_digest_size_from_checksum_algorithm(algorithm));
        ASSERT_SUCCESS(aws_checksum_finalize(accumulator, &combined));

        struct aws_byte_buf expected;
        ASSERT_SUCCESS(s_compute_whole(allocator, algorithm, input, &expected));
        ASSERT_BIN_ARRAYS_EQUALS(expected.buffer, expected.len, combined.buffer, combined.len);

        aws_byte_buf_clean_up(&expected);
        aws_byte_buf_clean_up(&combined);
        aws_checksum_destroy(block_sum);
        aws_checksum_destroy(accumulator);
    }

    aws_s3_library_clean_up();
    return AWS_OP_SUCCESS;
}
AWS_TEST_CASE(checksum_combine_identity, s_checksum_combine_identity_fn)

/* Combining a zero-length block must leave the accumulator untouched. Empty part responses take
 * this path. */
static int s_checksum_combine_empty_tail_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    aws_s3_library_init(allocator);

    struct aws_byte_cursor input = s_combine_test_input();
    const enum aws_s3_checksum_algorithm algorithms[] = {AWS_SCA_CRC32, AWS_SCA_CRC32C, AWS_SCA_CRC64NVME};

    for (size_t i = 0; i < AWS_ARRAY_SIZE(algorithms); ++i) {
        enum aws_s3_checksum_algorithm algorithm = algorithms[i];

        struct aws_s3_checksum *accumulator = aws_checksum_new(allocator, algorithm);
        struct aws_s3_checksum *empty = aws_checksum_new(allocator, algorithm);
        ASSERT_NOT_NULL(accumulator);
        ASSERT_NOT_NULL(empty);

        ASSERT_SUCCESS(aws_checksum_update(accumulator, &input));
        ASSERT_SUCCESS(s_combine_checksums(accumulator, empty, 0));

        struct aws_byte_buf combined;
        aws_byte_buf_init(&combined, allocator, aws_get_digest_size_from_checksum_algorithm(algorithm));
        ASSERT_SUCCESS(aws_checksum_finalize(accumulator, &combined));

        struct aws_byte_buf expected;
        ASSERT_SUCCESS(s_compute_whole(allocator, algorithm, input, &expected));
        ASSERT_BIN_ARRAYS_EQUALS(expected.buffer, expected.len, combined.buffer, combined.len);

        aws_byte_buf_clean_up(&expected);
        aws_byte_buf_clean_up(&combined);
        aws_checksum_destroy(empty);
        aws_checksum_destroy(accumulator);
    }

    aws_s3_library_clean_up();
    return AWS_OP_SUCCESS;
}
AWS_TEST_CASE(checksum_combine_empty_tail, s_checksum_combine_empty_tail_fn)

/* Non-CRC algorithms have no combine identity, so the API must reject them rather than produce a
 * silently wrong digest. */
static int s_checksum_combine_unsupported_algorithms_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    aws_s3_library_init(allocator);

    const enum aws_s3_checksum_algorithm combinable[] = {AWS_SCA_CRC32, AWS_SCA_CRC32C, AWS_SCA_CRC64NVME};
    const enum aws_s3_checksum_algorithm not_combinable[] = {
        AWS_SCA_SHA1,
        AWS_SCA_SHA256,
        AWS_SCA_SHA512,
        AWS_SCA_XXHASH64,
        AWS_SCA_XXHASH3_64,
        AWS_SCA_XXHASH3_128,
    };

    for (size_t i = 0; i < AWS_ARRAY_SIZE(combinable); ++i) {
        ASSERT_TRUE(aws_checksum_algorithm_is_combinable(combinable[i]));
    }

    struct aws_byte_cursor input = s_combine_test_input();
    for (size_t i = 0; i < AWS_ARRAY_SIZE(not_combinable); ++i) {
        if (i <= AWS_SCA_SHA512) {
#ifdef BYO_CRYPTO
            /* Skip SHA based algo for BYO_CRYPTO since they are libcrypto based. */
            continue;
#endif
        }
        enum aws_s3_checksum_algorithm algorithm = not_combinable[i];
        ASSERT_FALSE(aws_checksum_algorithm_is_combinable(algorithm));

        struct aws_s3_checksum *head = aws_checksum_new(allocator, algorithm);
        struct aws_s3_checksum *tail = aws_checksum_new(allocator, algorithm);
        ASSERT_NOT_NULL(head);
        ASSERT_NOT_NULL(tail);
        ASSERT_SUCCESS(aws_checksum_update(head, &input));
        ASSERT_SUCCESS(aws_checksum_update(tail, &input));
        uint8_t unused_digest[AWS_S3_COMBINABLE_DIGEST_MAX_LEN] = {0};
        ASSERT_ERROR(
            AWS_ERROR_UNSUPPORTED_OPERATION,
            aws_checksum_combine_digest(
                head, aws_byte_cursor_from_array(unused_digest, sizeof(unused_digest)), input.len));
        aws_checksum_destroy(tail);
        aws_checksum_destroy(head);
    }

    aws_s3_library_clean_up();
    return AWS_OP_SUCCESS;
}
AWS_TEST_CASE(checksum_combine_unsupported_algorithms, s_checksum_combine_unsupported_algorithms_fn)

/* Once finalized, a checksum is spent. Combining into or from one must fail loudly. */
static int s_checksum_combine_invalid_state_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    aws_s3_library_init(allocator);

    struct aws_byte_cursor input = s_combine_test_input();
    size_t digest_size = aws_get_digest_size_from_checksum_algorithm(AWS_SCA_CRC32);

    /* Finalized head. */
    {
        struct aws_s3_checksum *head = aws_checksum_new(allocator, AWS_SCA_CRC32);
        struct aws_s3_checksum *tail = aws_checksum_new(allocator, AWS_SCA_CRC32);
        struct aws_byte_buf digest;
        aws_byte_buf_init(&digest, allocator, digest_size);

        ASSERT_SUCCESS(aws_checksum_update(head, &input));
        ASSERT_SUCCESS(aws_checksum_finalize(head, &digest));
        ASSERT_ERROR(AWS_ERROR_INVALID_STATE, s_combine_checksums(head, tail, 0));

        aws_byte_buf_clean_up(&digest);
        aws_checksum_destroy(tail);
        aws_checksum_destroy(head);
    }

    /* Finalized tail. */
    {
        struct aws_s3_checksum *head = aws_checksum_new(allocator, AWS_SCA_CRC32);
        struct aws_s3_checksum *tail = aws_checksum_new(allocator, AWS_SCA_CRC32);
        struct aws_byte_buf digest;
        aws_byte_buf_init(&digest, allocator, digest_size);

        ASSERT_SUCCESS(aws_checksum_update(tail, &input));
        ASSERT_SUCCESS(aws_checksum_finalize(tail, &digest));
        ASSERT_ERROR(AWS_ERROR_INVALID_STATE, s_combine_checksums(head, tail, input.len));

        aws_byte_buf_clean_up(&digest);
        aws_checksum_destroy(tail);
        aws_checksum_destroy(head);
    }

    aws_s3_library_clean_up();
    return AWS_OP_SUCCESS;
}
AWS_TEST_CASE(checksum_combine_invalid_state, s_checksum_combine_invalid_state_fn)
