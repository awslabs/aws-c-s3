#include "aws/s3/private/s3_client_impl.h"
#include "aws/s3/private/s3_meta_request_impl.h"
#include "aws/s3/private/s3_request_messages.h"
#include "aws/s3/private/s3_util.h"
#include <aws/common/string.h>
#include <inttypes.h>

#ifdef _MSC_VER
/* sscanf warning (not currently scanning for strings) */
#    pragma warning(disable : 4996)
#endif

enum aws_s3_auto_ranged_get_state {
    AWS_S3_AUTO_RANGED_GET_STATE_START,
    AWS_S3_AUTO_RANGED_GET_STATE_WAITING_FOR_FIRST_PART,
    AWS_S3_AUTO_RANGED_GET_STATE_ALL_REQUESTS,
    AWS_S3_AUTO_RANGED_GET_STATE_ALL_REQUESTS_MADE
};

enum aws_s3_auto_ranged_get_request_type {
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

static bool s_s3_auto_ranged_get_has_work(const struct aws_s3_meta_request *meta_request);

static int s_s3_auto_ranged_get_next_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request **out_request);

static int s_s3_auto_ranged_get_prepare_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_client *client,
    struct aws_s3_request *request);

static int s_s3_auto_ranged_get_header_block_done(
    struct aws_http_stream *stream,
    enum aws_http_header_block header_block,
    struct aws_s3_vip_connection *vip_connection);

static int s_s3_auto_ranged_get_incoming_body(
    struct aws_http_stream *stream,
    const struct aws_byte_cursor *data,
    struct aws_s3_vip_connection *vip_connection);

static int s_s3_auto_ranged_get_stream_complete(
    struct aws_http_stream *stream,
    struct aws_s3_vip_connection *vip_connection);

static void s_s3_auto_ranged_get_write_body_callback(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request);

static struct aws_s3_meta_request_vtable s_s3_auto_ranged_get_vtable = {
    .has_work = s_s3_auto_ranged_get_has_work,
    .next_request = s_s3_auto_ranged_get_next_request,
    .prepare_request = s_s3_auto_ranged_get_prepare_request,
    .incoming_headers = NULL,
    .incoming_headers_block_done = s_s3_auto_ranged_get_header_block_done,
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

/* Allocate a new auto-ranged-get meta request. */
struct aws_s3_meta_request *aws_s3_meta_request_auto_ranged_get_new(
    struct aws_allocator *allocator,
    const struct aws_s3_meta_request_internal_options *options) {
    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(options && options->options && options->options->message);

    struct aws_http_headers *initial_message_headers = aws_http_message_get_headers(options->options->message);

    /* TODO If we already have a ranged header, we can break the range up into parts too.  However,
     * this requires some additional logic.  For now just return NULL. */
    if (initial_message_headers && aws_http_headers_has(initial_message_headers, g_range_header_name)) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST,
            "Could not create Meta Request with message %p. Ranged requests not currently supported.",
            (void *)options->options->message);
        aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        return NULL;
    }

    struct aws_s3_auto_ranged_get *auto_ranged_get =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_auto_ranged_get));

    /* Try to initialize the base type. */
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

static bool s_s3_auto_ranged_state_has_work(enum aws_s3_auto_ranged_get_state state) {
    return state == AWS_S3_AUTO_RANGED_GET_STATE_START || state == AWS_S3_AUTO_RANGED_GET_STATE_ALL_REQUESTS;
}

static bool s_s3_auto_ranged_get_has_work(const struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(meta_request->impl);

    struct aws_s3_auto_ranged_get *auto_ranged_get = meta_request->impl;

    bool has_work = false;

    s_s3_auto_ranged_get_lock_synced_data((struct aws_s3_auto_ranged_get *)auto_ranged_get);
    has_work = s_s3_auto_ranged_state_has_work(auto_ranged_get->synced_data.state);
    s_s3_auto_ranged_get_unlock_synced_data((struct aws_s3_auto_ranged_get *)auto_ranged_get);

    return has_work;
}

/* Try to get the next request that should be processed. */
static int s_s3_auto_ranged_get_next_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request **out_request) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(out_request);

    struct aws_s3_auto_ranged_get *auto_ranged_get = meta_request->impl;
    struct aws_s3_request *request = NULL;

    s_s3_auto_ranged_get_lock_synced_data(auto_ranged_get);

    switch (auto_ranged_get->synced_data.state) {
        /* This state means we haven't sent anything yet */
        case AWS_S3_AUTO_RANGED_GET_STATE_START: {

            /* We initially queue just one ranged get that is the size of a single part.  The headers from this
             * first get will tell us the size of the object, and we can spin up additional gets if necessary. */
            request = aws_s3_request_new(
                meta_request,
                AWS_S3_AUTO_RANGE_GET_REQUEST_TYPE_FIRST_PART,
                1,
                AWS_S3_REQUEST_DESC_RECORD_RESPONSE_HEADERS);

            auto_ranged_get->synced_data.next_part_number = 2;

            /* Wait for the first part's headers so that we can discover the object's total size. */
            auto_ranged_get->synced_data.state = AWS_S3_AUTO_RANGED_GET_STATE_WAITING_FOR_FIRST_PART;

            break;
        }
        case AWS_S3_AUTO_RANGED_GET_STATE_WAITING_FOR_FIRST_PART: {
            break;
        }
        case AWS_S3_AUTO_RANGED_GET_STATE_ALL_REQUESTS: {

            /* Keep returning returning requests until we've returned up to the total amount of parts. */
            if (auto_ranged_get->synced_data.next_part_number <= auto_ranged_get->synced_data.total_num_parts) {

                request = aws_s3_request_new(
                    meta_request,
                    AWS_S3_AUTO_RANGE_GET_REQUEST_TYPE_PART,
                    auto_ranged_get->synced_data.next_part_number,
                    0);

                ++auto_ranged_get->synced_data.next_part_number;

                if (auto_ranged_get->synced_data.next_part_number > auto_ranged_get->synced_data.total_num_parts) {
                    auto_ranged_get->synced_data.state = AWS_S3_AUTO_RANGED_GET_STATE_ALL_REQUESTS_MADE;
                }
            }

            break;
        }
        case AWS_S3_AUTO_RANGED_GET_STATE_ALL_REQUESTS_MADE: {

        } break;
        default:
            AWS_FATAL_ASSERT(false);
            break;
    }

    if (request != NULL) {
        AWS_LOGF_TRACE(
            AWS_LS_S3_META_REQUEST,
            "id=%p: Returning request %p for part %d of %d",
            (void *)meta_request,
            (void *)request,
            request->desc_data.part_number,
            auto_ranged_get->synced_data.total_num_parts);
    }

    s_s3_auto_ranged_get_unlock_synced_data(auto_ranged_get);

    *out_request = request;

    return AWS_OP_SUCCESS;
}

/* Given a request, prepare it for sending based on its description. */
static int s_s3_auto_ranged_get_prepare_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_client *client,
    struct aws_s3_request *request) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(request);

    struct aws_http_message *message = NULL;
    struct aws_s3_part_buffer *part_buffer = NULL;

    switch (request->desc_data.request_tag) {
        case AWS_S3_AUTO_RANGE_GET_REQUEST_TYPE_FIRST_PART:
            /* FALLTHROUGH */
        case AWS_S3_AUTO_RANGE_GET_REQUEST_TYPE_PART: {

            /* Generate a new ranged get request based on the original message. */
            message = aws_s3_get_object_message_new(
                meta_request->allocator,
                meta_request->initial_request_message,
                request->desc_data.part_number,
                meta_request->part_size);

            if (message == NULL) {
                AWS_LOGF_ERROR(
                    AWS_LS_S3_META_REQUEST,
                    "id=%p Could not create message for request with tag %d for auto-ranged-get meta request.",
                    (void *)meta_request,
                    request->desc_data.request_tag);
                goto message_alloc_failed;
            }

            /* Grab a part buffer that we can write the contents to and pass back to the user. */
            part_buffer = aws_s3_client_get_part_buffer(client, request->desc_data.part_number);

            if (part_buffer == NULL) {
                AWS_LOGF_ERROR(
                    AWS_LS_S3_META_REQUEST,
                    "id=%p Could not get part buffer for request with tag %d for auto-ranged-get meta request.",
                    (void *)meta_request,
                    request->desc_data.request_tag);

                aws_raise_error(AWS_ERROR_S3_NO_PART_BUFFER);

                goto part_buffer_get_failed;
            }

            break;
        }
    }

    aws_s3_request_setup_send_data(request, message);

    request->send_data.part_buffer = part_buffer;
    aws_http_message_release(message);

    AWS_LOGF_TRACE(
        AWS_LS_S3_META_REQUEST,
        "id=%p: Created request %p for part %d",
        (void *)meta_request,
        (void *)request,
        request->desc_data.part_number);

    return AWS_OP_SUCCESS;

part_buffer_get_failed:

    if (message != NULL) {
        aws_http_message_release(message);
        message = NULL;
    }

message_alloc_failed:

    return AWS_OP_ERR;
}

static int s_s3_auto_ranged_get_header_block_done(
    struct aws_http_stream *stream,
    enum aws_http_header_block header_block,
    struct aws_s3_vip_connection *vip_connection) {

    (void)stream;
    (void)header_block;

    AWS_PRECONDITION(stream);
    AWS_PRECONDITION(vip_connection);

    struct aws_s3_request *request = vip_connection->work_data.request;
    AWS_PRECONDITION(request);

    struct aws_s3_meta_request *meta_request = request->meta_request;
    AWS_ASSERT(meta_request);

    struct aws_s3_auto_ranged_get *auto_ranged_get = meta_request->impl;
    AWS_ASSERT(auto_ranged_get);

    if (request->desc_data.request_tag != AWS_S3_AUTO_RANGE_GET_REQUEST_TYPE_FIRST_PART) {
        return AWS_OP_SUCCESS;
    }

    struct aws_byte_cursor content_range_header_value;

    if (aws_http_headers_get(request->send_data.response_headers, g_content_range_header_name, &content_range_header_value)) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST,
            "id=%p Could not find content range header for request %p",
            (void *)meta_request,
            (void *)request);
        aws_raise_error(AWS_ERROR_S3_MISSING_CONTENT_RANGE_HEADER);
        return AWS_OP_ERR;
    }

    uint64_t range_start = 0;
    uint64_t range_end = 0;
    uint64_t total_object_size = 0;

    /* The memory the byte cursor refers to should be valid, but if it's referring to a buffer that was previously used,
     * the null terminating character may not be where we expect. We copy to a string to ensure that our null
     * terminating character placement corresponds with the length. */
    struct aws_string *content_range_header_value_str =
        aws_string_new_from_cursor(meta_request->allocator, &content_range_header_value);

    /* Format of header is: "bytes StartByte-EndByte/TotalObjectSize" */
    sscanf(
        (const char *)content_range_header_value_str->bytes,
        "bytes %" PRIu64 "-%" PRIu64 "/%" PRIu64,
        &range_start,
        &range_end,
        &total_object_size);

    aws_string_destroy(content_range_header_value_str);
    content_range_header_value_str = NULL;

    if (total_object_size == 0) {
        AWS_LOGF_ERROR(AWS_LS_S3_META_REQUEST, "id=%p Get Object has invalid content range.", (void *)meta_request);
        aws_raise_error(AWS_ERROR_S3_MISSING_CONTENT_RANGE_HEADER);
        return AWS_OP_ERR;
    }

    /* TODO add additional error checking for this value going out above service-side part limits. */
    uint32_t num_parts = (uint32_t)(total_object_size / meta_request->part_size);

    if (total_object_size % meta_request->part_size) {
        ++num_parts;
    }

    AWS_LOGF_TRACE(
        AWS_LS_S3_META_REQUEST,
        "id=%p Object being requested is %" PRIu64 " bytes which will have %d parts based off of a %" PRIu64
        " part size.",
        (void *)meta_request,
        total_object_size,
        num_parts,
        meta_request->part_size);

    s_s3_auto_ranged_get_lock_synced_data(auto_ranged_get);

    if (num_parts == 1) {
        auto_ranged_get->synced_data.state = AWS_S3_AUTO_RANGED_GET_STATE_ALL_REQUESTS_MADE;
    } else {
        auto_ranged_get->synced_data.state = AWS_S3_AUTO_RANGED_GET_STATE_ALL_REQUESTS;
    }

    auto_ranged_get->synced_data.total_num_parts = num_parts;
    s_s3_auto_ranged_get_unlock_synced_data(auto_ranged_get);

    if (meta_request->headers_callback != NULL) {
        struct aws_http_headers *response_headers = aws_http_headers_new(meta_request->allocator);

        copy_http_headers(request->send_data.response_headers, response_headers);

        aws_http_headers_erase(response_headers, g_accept_ranges_header_name);
        aws_http_headers_erase(response_headers, g_content_range_header_name);

        char content_length_buffer[64] = "";
        snprintf(content_length_buffer, sizeof(content_length_buffer), "%" PRIu64, total_object_size);
        aws_http_headers_set(
            response_headers, g_content_length_header_name, aws_byte_cursor_from_c_str(content_length_buffer));

        meta_request->headers_callback(
            meta_request, response_headers, AWS_S3_RESPONSE_STATUS_SUCCESS, meta_request->user_data);

        aws_http_headers_release(response_headers);
    }

    if (num_parts > 1) {
        aws_s3_meta_request_schedule_work(meta_request);
    }

    return AWS_OP_SUCCESS;
}

static int s_s3_auto_ranged_get_incoming_body(
    struct aws_http_stream *stream,
    const struct aws_byte_cursor *data,
    struct aws_s3_vip_connection *vip_connection) {
    (void)stream;

    AWS_PRECONDITION(vip_connection);

    struct aws_s3_request *request = vip_connection->work_data.request;
    AWS_PRECONDITION(request);

    struct aws_s3_part_buffer *part_buffer = request->send_data.part_buffer;
    AWS_PRECONDITION(part_buffer);

    /* Store the contents in our part buffer */
    if (aws_byte_buf_append(&part_buffer->buffer, data)) {
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}

static int s_s3_auto_ranged_get_stream_complete(
    struct aws_http_stream *stream,
    struct aws_s3_vip_connection *vip_connection) {
    (void)stream;

    AWS_PRECONDITION(vip_connection);

    struct aws_s3_request *request = vip_connection->work_data.request;
    AWS_PRECONDITION(request);
    AWS_PRECONDITION(request->send_data.part_buffer);

    struct aws_s3_meta_request *meta_request = request->meta_request;
    AWS_PRECONDITION(meta_request);

    aws_s3_meta_request_internal_acquire(meta_request);

    /* Schedule the part buffer to be sent back to the user so that they can process it. */
    aws_s3_meta_request_write_body_to_caller(request, s_s3_auto_ranged_get_write_body_callback);

    return AWS_OP_SUCCESS;
}

static void s_s3_auto_ranged_get_write_body_callback(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(meta_request->impl);

    (void)request;

    struct aws_s3_auto_ranged_get *auto_ranged_get = meta_request->impl;

    s_s3_auto_ranged_get_lock_synced_data(auto_ranged_get);
    ++auto_ranged_get->synced_data.num_parts_completed;

    AWS_LOGF_DEBUG(
        AWS_LS_S3_META_REQUEST,
        "id=%p: %d out of %d parts have completed.",
        (void *)meta_request,
        auto_ranged_get->synced_data.num_parts_completed,
        auto_ranged_get->synced_data.total_num_parts);

    /* If we have now grabbed all parts for this transfer, then we are done with this meta request. */
    bool finished = auto_ranged_get->synced_data.num_parts_completed == auto_ranged_get->synced_data.total_num_parts;

    s_s3_auto_ranged_get_unlock_synced_data(auto_ranged_get);

    if (finished) {
        aws_s3_meta_request_finish(meta_request, NULL, AWS_S3_RESPONSE_STATUS_SUCCESS, AWS_ERROR_SUCCESS);
    }

    aws_s3_meta_request_internal_release(meta_request);
}
