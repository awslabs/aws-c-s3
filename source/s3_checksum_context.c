/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_checksum_context.h"
#include "aws/s3/private/s3_checksums.h"
#include <aws/common/encoding.h>
#include <aws/common/logging.h>

static void s_lock_synced_data(struct aws_s3_upload_request_checksum_context *context) {
    aws_mutex_lock(&context->synced_data.lock);
}

static void s_unlock_synced_data(struct aws_s3_upload_request_checksum_context *context) {
    aws_mutex_unlock(&context->synced_data.lock);
}

static void s_aws_s3_upload_request_checksum_context_destroy(void *context) {
    struct aws_s3_upload_request_checksum_context *checksum_context = context;
    aws_byte_buf_clean_up(&checksum_context->synced_data.base64_checksum);
    aws_mutex_clean_up(&checksum_context->synced_data.lock);
    aws_mem_release(checksum_context->allocator, checksum_context);
}

static struct aws_s3_upload_request_checksum_context *s_s3_upload_request_checksum_context_new_base(
    struct aws_allocator *allocator,
    const struct aws_s3_meta_request_checksum_config_storage *checksum_config) {
    AWS_PRECONDITION(allocator);

    struct aws_s3_upload_request_checksum_context *context =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_upload_request_checksum_context));

    aws_ref_count_init(&context->ref_count, context, s_aws_s3_upload_request_checksum_context_destroy);
    context->allocator = allocator;
    if (aws_mutex_init(&context->synced_data.lock)) {
        aws_s3_upload_request_checksum_context_release(context);
        return NULL;
    }
    /* Handle case where no checksum config is provided */
    if (!checksum_config || checksum_config->checksum_algorithm == AWS_SCA_NONE) {
        context->algorithm = AWS_SCA_NONE;
        context->encoded_checksum_size = 0;
        return context;
    }

    /* Extract configuration */
    context->algorithm = checksum_config->checksum_algorithm;
    context->location = checksum_config->location;
    context->encoded_checksum_size = aws_get_digest_size_from_checksum_algorithm(context->algorithm);

    /* Convert to base64 encoded size */
    size_t encoded_size = 0;
    if (aws_base64_compute_encoded_len(context->encoded_checksum_size, &encoded_size)) {
        AWS_LOGF_ERROR(AWS_LS_S3_GENERAL, "Failed to compute base64 encoded length for checksum");
        aws_s3_upload_request_checksum_context_release(context);
        return NULL;
    }
    context->encoded_checksum_size = encoded_size;
    return context;
}

struct aws_s3_upload_request_checksum_context *aws_s3_upload_request_checksum_context_new(
    struct aws_allocator *allocator,
    const struct aws_s3_meta_request_checksum_config_storage *checksum_config) {
    struct aws_s3_upload_request_checksum_context *context =
        s_s3_upload_request_checksum_context_new_base(allocator, checksum_config);
    if (context && context->encoded_checksum_size > 0) {
        /* Initial the buffer for checksum */
        aws_byte_buf_init(&context->synced_data.base64_checksum, allocator, context->encoded_checksum_size);
    }
    return context;
}

struct aws_s3_upload_request_checksum_context *aws_s3_upload_request_checksum_context_new_with_existing_base64_checksum(
    struct aws_allocator *allocator,
    const struct aws_s3_meta_request_checksum_config_storage *checksum_config,
    struct aws_byte_cursor existing_base64_checksum) {
    struct aws_s3_upload_request_checksum_context *context =
        s_s3_upload_request_checksum_context_new_base(allocator, checksum_config);
    if (context) {
        /* Initial the buffer for checksum from the exist checksum */
        if (context->encoded_checksum_size != existing_base64_checksum.len) {
            struct aws_byte_cursor algo_name = aws_get_checksum_algorithm_name(context->algorithm);
            AWS_LOGF_ERROR(
                AWS_LS_S3_GENERAL,
                "Encoded checksum size mismatch during creating the context for algorithm " PRInSTR
                ": expected %zu bytes, got %zu bytes",
                AWS_BYTE_CURSOR_PRI(algo_name),
                context->encoded_checksum_size,
                existing_base64_checksum.len);
            aws_s3_upload_request_checksum_context_release(context);
            return NULL;
        }
        aws_byte_buf_init_copy_from_cursor(&context->synced_data.base64_checksum, allocator, existing_base64_checksum);
        context->synced_data.checksum_calculated = true;
    }
    return context;
}

struct aws_s3_upload_request_checksum_context *aws_s3_upload_request_checksum_context_acquire(
    struct aws_s3_upload_request_checksum_context *context) {
    if (context) {
        aws_ref_count_acquire(&context->ref_count);
    }
    return context;
}

struct aws_s3_upload_request_checksum_context *aws_s3_upload_request_checksum_context_release(
    struct aws_s3_upload_request_checksum_context *context) {
    if (context) {
        aws_ref_count_release(&context->ref_count);
    }
    return NULL;
}

bool aws_s3_upload_request_checksum_context_should_calculate(struct aws_s3_upload_request_checksum_context *context) {
    if (!context || context->algorithm == AWS_SCA_NONE) {
        return false;
    }

    bool should_calculate = false;
    s_lock_synced_data(context);
    /* If not previous calculated */
    should_calculate = !context->synced_data.checksum_calculated;
    s_unlock_synced_data(context);

    return should_calculate;
}

bool aws_s3_upload_request_checksum_context_should_add_header(
    const struct aws_s3_upload_request_checksum_context *context) {
    if (!context) {
        return false;
    }

    return context->location == AWS_SCL_HEADER && context->algorithm != AWS_SCA_NONE;
}

bool aws_s3_upload_request_checksum_context_should_add_trailer(
    const struct aws_s3_upload_request_checksum_context *context) {
    if (!context) {
        return false;
    }

    return context->location == AWS_SCL_TRAILER && context->algorithm != AWS_SCA_NONE;
}

int aws_s3_upload_request_checksum_context_finalize_checksum(
    struct aws_s3_upload_request_checksum_context *context,
    struct aws_byte_cursor raw_checksum_cursor) {
    if (!context) {
        return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
    }
    s_lock_synced_data(context);
    /* If not previous calculated */
    if (!context->synced_data.checksum_calculated) {
        AWS_ASSERT(context->synced_data.base64_checksum.len == 0);

        if (aws_base64_encode(&raw_checksum_cursor, &context->synced_data.base64_checksum)) {
            aws_byte_buf_reset(&context->synced_data.base64_checksum, false);
            AWS_LOGF_ERROR(
                AWS_LS_S3_GENERAL,
                "Failed to base64 encode for the checksum. Raw checksum length: %zu. Output buffer capacity: %zu "
                "length %zu",
                raw_checksum_cursor.len,
                context->synced_data.base64_checksum.capacity,
                context->synced_data.base64_checksum.len);
            s_unlock_synced_data(context);
            return AWS_OP_ERR;
        }
        context->synced_data.checksum_calculated = true;
    }
    s_unlock_synced_data(context);
    return AWS_OP_SUCCESS;
}

struct aws_byte_cursor aws_s3_upload_request_checksum_context_get_checksum_cursor(
    struct aws_s3_upload_request_checksum_context *context) {
    struct aws_byte_cursor checksum_cursor = {0};
    if (!context) {
        return checksum_cursor;
    }
    s_lock_synced_data(context);
    /* If not previous calculated */
    if (context->synced_data.checksum_calculated) {
        checksum_cursor = aws_byte_cursor_from_buf(&context->synced_data.base64_checksum);
    }
    s_unlock_synced_data(context);
    return checksum_cursor;
}
