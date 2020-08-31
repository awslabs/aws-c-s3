#include "aws/s3/private/s3_client_impl.h"
#include "aws/s3/private/s3_meta_request_impl.h"
#include "aws/s3/private/s3_request_messages.h"
#include "aws/s3/private/s3_util.h"
#include <inttypes.h>

enum aws_s3_auto_ranged_get_state {
    AWS_S3_AUTO_RANGED_GET_STATE_START,
    AWS_S3_AUTO_RANGED_GET_STATE_WAITING_FOR_FIRST_PART,
    AWS_S3_AUTO_RANGED_GET_STATE_ALL_REQUESTS
};

enum aws_s3_auto_ranged_get_request_type {
    AWS_S3_AUTO_RANGE_GET_REQUEST_TYPE_ENTIRE_OBJECT,
    AWS_S3_AUTO_RANGE_GET_REQUEST_TYPE_FIRST_PART,
    AWS_S3_AUTO_RANGE_GET_REQUEST_TYPE_PART
};

struct aws_s3_auto_ranged_get {
    struct aws_s3_meta_request base;

    struct {
        enum aws_s3_auto_ranged_get_state state;

        uint32_t next_part_number;
        uint32_t total_num_parts;
        uint32_t num_parts_completed;
        size_t total_object_size;

    } synced_data;
};

static void s_s3_auto_ranged_get_lock_synced_data(struct aws_s3_auto_ranged_get *auto_ranged_get);
static void s_s3_auto_ranged_get_unlock_synced_data(struct aws_s3_auto_ranged_get *auto_ranged_get);

static void s_s3_meta_request_auto_ranged_get_destroy(struct aws_s3_meta_request *meta_request);

static int s_s3_auto_ranged_get_next_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request_desc **out_request_desc);

struct aws_s3_request *s_s3_auto_ranged_get_request_factory(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_client *client,
    struct aws_s3_request_desc *request_desc);

static int s_s3_auto_ranged_get_incoming_headers(
    struct aws_http_stream *stream,
    enum aws_http_header_block header_block,
    const struct aws_http_header *headers,
    size_t headers_count,
    void *user_data);

static int s_s3_auto_ranged_get_incoming_body(
    struct aws_http_stream *stream,
    const struct aws_byte_cursor *data,
    void *user_data);

static void s_s3_auto_ranged_get_stream_complete(struct aws_http_stream *stream, int error_code, void *user_data);

static struct aws_s3_meta_request_vtable s_s3_auto_ranged_get_vtable = {
    .next_request = s_s3_auto_ranged_get_next_request,
    .request_factory = s_s3_auto_ranged_get_request_factory,
    .incoming_headers = s_s3_auto_ranged_get_incoming_headers,
    .incoming_headers_block_done = NULL,
    .incoming_body = s_s3_auto_ranged_get_incoming_body,
    .stream_complete = s_s3_auto_ranged_get_stream_complete,
    .destroy = s_s3_meta_request_auto_ranged_get_destroy};

static void s_s3_auto_ranged_get_lock_synced_data(struct aws_s3_auto_ranged_get *auto_ranged_get) {
    AWS_PRECONDITION(auto_ranged_get);

    aws_mutex_lock(&auto_ranged_get->base.synced_data.lock);
}

static void s_s3_auto_ranged_get_unlock_synced_data(struct aws_s3_auto_ranged_get *auto_ranged_get) {
    AWS_PRECONDITION(auto_ranged_get);

    aws_mutex_unlock(&auto_ranged_get->base.synced_data.lock);
}

struct aws_s3_meta_request *aws_s3_meta_request_auto_ranged_get_new(
    struct aws_allocator *allocator,
    const struct aws_s3_meta_request_internal_options *options) {
    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(options);

    struct aws_s3_auto_ranged_get *auto_ranged_get =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_auto_ranged_get));

    if (auto_ranged_get == NULL) {
        AWS_LOGF_ERROR(AWS_LS_S3_META_REQUEST, "Could not allocate Auto-Ranged-Get Meta Request.");
        return NULL;
    }

    if (aws_s3_meta_request_init_base(
            allocator, options, auto_ranged_get, &s_s3_auto_ranged_get_vtable, &auto_ranged_get->base)) {

        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST,
            "id=%p Could not initialize base type for Auto-Ranged-Get Meta Request.",
            (void *)auto_ranged_get);
        goto error_clean_up;
    }

    return &auto_ranged_get->base;

error_clean_up:

    aws_s3_meta_request_release(&auto_ranged_get->base);
    auto_ranged_get = NULL;

    return NULL;
}

static void s_s3_meta_request_auto_ranged_get_destroy(struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(meta_request->impl);

    struct aws_s3_auto_ranged_get *auto_ranged_get = meta_request->impl;
    aws_mem_release(meta_request->allocator, auto_ranged_get);
}

static int s_s3_auto_ranged_get_next_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request_desc **out_request_desc) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(out_request_desc);

    struct aws_s3_auto_ranged_get *auto_ranged_get = meta_request->impl;
    struct aws_s3_request_desc *request_desc = NULL;

    s_s3_auto_ranged_get_lock_synced_data(auto_ranged_get);

    switch (auto_ranged_get->synced_data.state) {
        case AWS_S3_AUTO_RANGED_GET_STATE_START: {
            struct aws_http_message *initial_message = meta_request->initial_request_message;
            AWS_FATAL_ASSERT(initial_message != NULL);

            struct aws_http_headers *initial_message_headers = aws_http_message_get_headers(initial_message);

            if (initial_message_headers == NULL) {
                s_s3_auto_ranged_get_unlock_synced_data(auto_ranged_get);
                goto error_clean_up;
            }

            struct aws_byte_cursor range_header_value;

            /* TODO If we already have a ranged header, we can break the range up into parts too.  However,
             * this requires additional parsing of this header value, so for now, we just send the message. */
            if (!aws_http_headers_get(initial_message_headers, g_range_header_name, &range_header_value)) {
                request_desc =
                    aws_s3_request_desc_new(meta_request, AWS_S3_AUTO_RANGE_GET_REQUEST_TYPE_ENTIRE_OBJECT, 0);

                if (request_desc == NULL) {
                    s_s3_auto_ranged_get_unlock_synced_data(auto_ranged_get);
                    goto error_clean_up;
                }

                auto_ranged_get->synced_data.state = AWS_S3_AUTO_RANGED_GET_STATE_ALL_REQUESTS;

            } else {
                /* We initially queue just one ranged get that is the size of a single part.  The headers from this
                 * first get will tell us the size of the object, and we can spin up additional gets if necessary. */
                request_desc = aws_s3_request_desc_new(meta_request, AWS_S3_AUTO_RANGE_GET_REQUEST_TYPE_FIRST_PART, 1);

                if (request_desc == NULL) {
                    s_s3_auto_ranged_get_unlock_synced_data(auto_ranged_get);
                    goto error_clean_up;
                }

                auto_ranged_get->synced_data.next_part_number = 2;
                auto_ranged_get->synced_data.state = AWS_S3_AUTO_RANGED_GET_STATE_WAITING_FOR_FIRST_PART;
            }

            break;
        }
        case AWS_S3_AUTO_RANGED_GET_STATE_WAITING_FOR_FIRST_PART: {
            break;
        }
        case AWS_S3_AUTO_RANGED_GET_STATE_ALL_REQUESTS: {

            if (auto_ranged_get->synced_data.next_part_number <= auto_ranged_get->synced_data.total_num_parts) {

                request_desc = aws_s3_request_desc_new(
                    meta_request,
                    AWS_S3_AUTO_RANGE_GET_REQUEST_TYPE_PART,
                    auto_ranged_get->synced_data.next_part_number);

                if (request_desc == NULL) {
                    s_s3_auto_ranged_get_unlock_synced_data(auto_ranged_get);
                    goto error_clean_up;
                }

                ++auto_ranged_get->synced_data.next_part_number;
            }

            break;
        }
        default:
            AWS_FATAL_ASSERT(false);
            break;
    }

    if (request_desc != NULL) {
        AWS_LOGF_INFO(
            AWS_LS_S3_META_REQUEST,
            "id=%p: Returning request desc for part %d of %d",
            (void *)meta_request,
            request_desc->part_number,
            auto_ranged_get->synced_data.total_num_parts);
    }

    s_s3_auto_ranged_get_unlock_synced_data(auto_ranged_get);

    *out_request_desc = request_desc;

    return AWS_OP_SUCCESS;

error_clean_up:

    return AWS_OP_ERR;
}

struct aws_s3_request *s_s3_auto_ranged_get_request_factory(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_client *client,
    struct aws_s3_request_desc *request_desc) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(request_desc);

    struct aws_http_message *message = NULL;
    struct aws_s3_part_buffer *part_buffer = NULL;

    switch (request_desc->request_tag) {
        case AWS_S3_AUTO_RANGE_GET_REQUEST_TYPE_ENTIRE_OBJECT: {
            message =
                aws_s3_get_object_message_new(meta_request->allocator, meta_request->initial_request_message, 0, 0);

            if (message == NULL) {
                AWS_LOGF_ERROR(
                    AWS_LS_S3_META_REQUEST,
                    "id=%p Could not create message for request with tag %d for auto-ranged-get meta request.",
                    (void *)meta_request,
                    request_desc->request_tag);
                goto message_alloc_failed;
            }

            break;
        }
        case AWS_S3_AUTO_RANGE_GET_REQUEST_TYPE_FIRST_PART:
        case AWS_S3_AUTO_RANGE_GET_REQUEST_TYPE_PART: {
            message = aws_s3_get_object_message_new(
                meta_request->allocator,
                meta_request->initial_request_message,
                request_desc->part_number,
                meta_request->part_size);

            if (message == NULL) {
                AWS_LOGF_ERROR(
                    AWS_LS_S3_META_REQUEST,
                    "id=%p Could not create message for request with tag %d for auto-ranged-get meta request.",
                    (void *)meta_request,
                    request_desc->request_tag);
                goto message_alloc_failed;
            }

            part_buffer = aws_s3_client_get_part_buffer(client, request_desc->part_number);

            if (part_buffer == NULL) {
                AWS_LOGF_WARN(
                    AWS_LS_S3_META_REQUEST,
                    "id=%p Could not get part buffer for request with tag %d for auto-ranged-get meta request.",
                    (void *)meta_request,
                    request_desc->request_tag);

                aws_raise_error(AWS_ERROR_S3_NO_PART_BUFFER);

                goto part_buffer_get_failed;
            }

            break;
        }
    }

    struct aws_s3_request *request = aws_s3_request_new(meta_request, request_desc, message);

    if (request == NULL) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST,
            "id=%p Could not not allocate request with tag %d for Auto-Ranged-Get Meta Request.",
            (void *)meta_request,
            request_desc->request_tag);
        goto request_alloc_failed;
    }

    request->part_buffer = part_buffer;
    aws_http_message_release(message);

    AWS_LOGF_INFO(
        AWS_LS_S3_META_REQUEST,
        "id=%p: Created request %p for part %d",
        (void *)meta_request,
        (void *)request,
        request_desc->part_number);

    return request;

request_alloc_failed:

    if (part_buffer != NULL) {
        aws_s3_part_buffer_release(part_buffer);
        part_buffer = NULL;
    }

part_buffer_get_failed:

    if (message != NULL) {
        aws_http_message_release(message);
        message = NULL;
    }

message_alloc_failed:

    return NULL;
}
static int s_s3_auto_ranged_get_incoming_headers(
    struct aws_http_stream *stream,
    enum aws_http_header_block header_block,
    const struct aws_http_header *headers,
    size_t headers_count,
    void *user_data) {

    AWS_PRECONDITION(stream);
    AWS_PRECONDITION(user_data);

    struct aws_s3_send_request_work *work = user_data;
    AWS_PRECONDITION(work->request_desc);

    if (work->request_desc->request_tag != AWS_S3_AUTO_RANGE_GET_REQUEST_TYPE_FIRST_PART) {
        return AWS_OP_SUCCESS;
    }

    struct aws_s3_meta_request *meta_request = work->meta_request;
    AWS_PRECONDITION(meta_request);

    struct aws_s3_auto_ranged_get *auto_ranged_get = meta_request->impl;
    AWS_PRECONDITION(auto_ranged_get);

    (void)stream;
    (void)header_block;

    /* Find the Content-Range header and extract the object size. */
    for (size_t i = 0; i < headers_count; ++i) {
        const struct aws_byte_cursor *name = &headers[i].name;
        const struct aws_byte_cursor *value = &headers[i].value;

        if (!aws_http_header_name_eq(*name, g_content_range_header_name)) {
            continue;
        }

        uint64_t range_start = 0;
        uint64_t range_end = 0;
        uint64_t total_object_size = 0;

        /* Format of header is: "bytes StartByte-EndByte/TotalObjectSize" */
        sscanf(
            (const char *)value->ptr,
            "bytes %" PRIu64 "-%" PRIu64 "/%" PRIu64,
            &range_start,
            &range_end,
            &total_object_size);

        if (total_object_size == 0) {
            AWS_LOGF_ERROR(AWS_LS_S3_META_REQUEST, "id=%p Get Object has invalid content range.", (void *)meta_request);
            aws_raise_error(AWS_ERROR_S3_MISSING_CONTENT_RANGE_HEADER);
            return AWS_OP_ERR;
        }

        s_s3_auto_ranged_get_lock_synced_data(auto_ranged_get);

        size_t num_parts = total_object_size / meta_request->part_size;

        if (total_object_size % meta_request->part_size) {
            ++num_parts;
        }

        auto_ranged_get->synced_data.state = AWS_S3_AUTO_RANGED_GET_STATE_ALL_REQUESTS;
        auto_ranged_get->synced_data.total_num_parts = num_parts;

        s_s3_auto_ranged_get_unlock_synced_data(auto_ranged_get);
    }

    return AWS_OP_SUCCESS;
}

static int s_s3_auto_ranged_get_incoming_body(
    struct aws_http_stream *stream,
    const struct aws_byte_cursor *data,
    void *user_data) {

    AWS_PRECONDITION(stream);

    struct aws_s3_send_request_work *work = user_data;
    AWS_PRECONDITION(work);

    struct aws_s3_meta_request *meta_request = work->meta_request;
    AWS_PRECONDITION(meta_request);

    struct aws_s3_request *request = work->request;
    AWS_PRECONDITION(request);

    struct aws_s3_request_desc *request_desc = work->request_desc;
    AWS_PRECONDITION(request_desc);

    struct aws_s3_part_buffer *part_buffer = request->part_buffer;
    AWS_PRECONDITION(part_buffer);

    if (request_desc->request_tag == AWS_S3_AUTO_RANGE_GET_REQUEST_TYPE_ENTIRE_OBJECT) {

        if (meta_request->body_callback != NULL) {
            meta_request->body_callback(meta_request, data, 0, 0, meta_request->user_data);
        }

    } else {
        if (aws_byte_buf_append(&part_buffer->buffer, data)) {
            return AWS_OP_ERR;
        }
    }

    return AWS_OP_SUCCESS;
}

static void s_s3_auto_ranged_get_stream_complete(struct aws_http_stream *stream, int error_code, void *user_data) {
    AWS_PRECONDITION(stream);

    struct aws_s3_send_request_work *work = user_data;
    AWS_PRECONDITION(work);
    AWS_PRECONDITION(work->request_desc);

    struct aws_s3_meta_request *meta_request = work->meta_request;
    AWS_PRECONDITION(meta_request);

    struct aws_s3_auto_ranged_get *auto_ranged_get = meta_request->impl;
    AWS_PRECONDITION(auto_ranged_get);

    if (error_code != AWS_ERROR_SUCCESS) {

        if (error_code == AWS_ERROR_S3_INTERNAL_ERROR || error_code == AWS_ERROR_S3_NO_PART_BUFFER) {
            if (aws_s3_meta_request_queue_retry(meta_request, &work->request_desc)) {
                aws_s3_meta_request_finish(meta_request, aws_last_error());
            }
        } else {
            aws_s3_meta_request_finish(meta_request, error_code);
        }

        return;
    }

    struct aws_s3_request *request = work->request;
    AWS_PRECONDITION(request);

    struct aws_s3_part_buffer *part_buffer = request->part_buffer;
    AWS_PRECONDITION(part_buffer);

    if (aws_s3_meta_request_write_part_buffer_to_caller(meta_request, part_buffer) == AWS_OP_SUCCESS) {
        request->part_buffer = NULL;
    }

    s_s3_auto_ranged_get_lock_synced_data(auto_ranged_get);
    ++auto_ranged_get->synced_data.num_parts_completed;

    AWS_LOGF_INFO(
        AWS_LS_S3_META_REQUEST,
        "id=%p: %d out of %d parts have completed.",
        (void *)meta_request,
        auto_ranged_get->synced_data.num_parts_completed,
        auto_ranged_get->synced_data.total_num_parts);

    bool finished = auto_ranged_get->synced_data.num_parts_completed == auto_ranged_get->synced_data.total_num_parts;

    s_s3_auto_ranged_get_unlock_synced_data(auto_ranged_get);

    if (finished) {
        aws_s3_meta_request_finish(meta_request, AWS_ERROR_SUCCESS);
    }
}
