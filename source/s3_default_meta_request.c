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

enum aws_s3_meta_request_default_state {
    AWS_S3_META_REQUEST_DEFAULT_STATE_START,
    AWS_S3_META_REQUEST_DEFAULT_WAITING_FOR_REQUEST,
};

struct aws_s3_meta_request_default {
    struct aws_s3_meta_request base;

    bool is_get_request;
    size_t content_length;

    struct {
        enum aws_s3_meta_request_default_state state;
    } synced_data;
};

static void s_s3_meta_request_default_lock_synced_data(struct aws_s3_meta_request_default *meta_request_default);
static void s_s3_meta_request_default_unlock_synced_data(struct aws_s3_meta_request_default *meta_request_default);

static void s_s3_meta_request_default_destroy(struct aws_s3_meta_request *meta_request);

static int s_s3_meta_request_default_next_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request **out_request);

static int s_s3_meta_request_default_prepare_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_client *client,
    struct aws_s3_vip_connection *vip_connection,
    bool is_initial_prepare);

static int s_s3_meta_request_default_header_block_done(
    struct aws_http_stream *stream,
    enum aws_http_header_block header_block,
    struct aws_s3_vip_connection *vip_connection);

static void s_s3_meta_request_default_notify_request_destroyed(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request);

static struct aws_s3_meta_request_vtable s_s3_meta_request_default_vtable = {
    .next_request = s_s3_meta_request_default_next_request,
    .send_request_finish = aws_s3_meta_request_send_request_finish_default,
    .prepare_request = s_s3_meta_request_default_prepare_request,
    .init_signing_date_time = aws_s3_meta_request_init_signing_date_time_default,
    .sign_request = aws_s3_meta_request_sign_request_default,
    .incoming_headers = NULL,
    .incoming_headers_block_done = s_s3_meta_request_default_header_block_done,
    .incoming_body = NULL,
    .stream_complete = NULL,
    .notify_request_destroyed = s_s3_meta_request_default_notify_request_destroyed,
    .destroy = s_s3_meta_request_default_destroy,
    .finish = aws_s3_meta_request_finish_default,
};

static void s_s3_meta_request_default_lock_synced_data(struct aws_s3_meta_request_default *meta_request_default) {
    AWS_PRECONDITION(meta_request_default);

    aws_mutex_lock(&meta_request_default->base.synced_data.lock);
}

static void s_s3_meta_request_default_unlock_synced_data(struct aws_s3_meta_request_default *meta_request_default) {
    AWS_PRECONDITION(meta_request_default);

    aws_mutex_unlock(&meta_request_default->base.synced_data.lock);
}

/* Allocate a new default meta request. */
struct aws_s3_meta_request *aws_s3_meta_request_default_new(
    struct aws_allocator *allocator,
    struct aws_s3_client *client,
    uint64_t content_length,
    const struct aws_s3_meta_request_options *options) {
    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(options);
    AWS_PRECONDITION(options->message);

    struct aws_byte_cursor request_method;
    if (aws_http_message_get_request_method(options->message, &request_method)) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST,
            "Could not create Default Meta request; could not get request method from message.");

        aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        return NULL;
    }

    if (content_length > SIZE_MAX) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST,
            "Could not create Default Meta request; content length of %" PRIu64 " bytes is too large for platform.",
            content_length);

        aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        return NULL;
    }

    struct aws_s3_meta_request_default *meta_request_default =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_meta_request_default));

    /* Try to initialize the base type. */
    if (aws_s3_meta_request_init_base(
            allocator,
            client,
            0,
            options,
            meta_request_default,
            &s_s3_meta_request_default_vtable,
            &meta_request_default->base)) {

        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST,
            "id=%p Could not initialize base type for Default Meta Request.",
            (void *)meta_request_default);
        goto error_clean_up;
    }

    meta_request_default->content_length = (size_t)content_length;
    meta_request_default->is_get_request = aws_byte_cursor_eq_ignore_case(&request_method, &aws_http_method_get);

    AWS_LOGF_DEBUG(AWS_LS_S3_META_REQUEST, "id=%p Created new Default Meta Request.", (void *)meta_request_default);

    return &meta_request_default->base;

error_clean_up:

    aws_s3_meta_request_release(&meta_request_default->base);
    meta_request_default = NULL;

    return NULL;
}

static void s_s3_meta_request_default_destroy(struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(meta_request->impl);

    struct aws_s3_meta_request_default *meta_request_default = meta_request->impl;
    aws_mem_release(meta_request->allocator, meta_request_default);
}

/* Try to get the next request that should be processed. */
static int s_s3_meta_request_default_next_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request **out_request) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(out_request);

    struct aws_s3_meta_request_default *meta_request_default = meta_request->impl;
    struct aws_s3_request *request = NULL;

    bool create_request = false;

    s_s3_meta_request_default_lock_synced_data(meta_request_default);

    /* We only have one request to potentially create, which is for the original message as-is. */
    if (meta_request_default->synced_data.state == AWS_S3_META_REQUEST_DEFAULT_STATE_START) {
        create_request = true;
        meta_request_default->synced_data.state = AWS_S3_META_REQUEST_DEFAULT_WAITING_FOR_REQUEST;
    }

    s_s3_meta_request_default_unlock_synced_data(meta_request_default);

    if (create_request) {
        uint32_t request_flags = AWS_S3_REQUEST_DESC_RECORD_RESPONSE_HEADERS;

        if (meta_request_default->is_get_request) {
            request_flags |= AWS_S3_REQUEST_DESC_STREAM_RESPONSE_BODY;
        }

        const uint32_t part_number = 1;

        request = aws_s3_request_new(meta_request, 0, part_number, request_flags);

        AWS_LOGF_DEBUG(
            AWS_LS_S3_META_REQUEST, "id=%p: Meta Request created request %p", (void *)meta_request, (void *)request);
    }

    *out_request = request;

    return AWS_OP_SUCCESS;
}

/* Given a request, prepare it for sending based on its description. */
static int s_s3_meta_request_default_prepare_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_client *client,
    struct aws_s3_vip_connection *vip_connection,
    bool is_initial_prepare) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(vip_connection);
    (void)client;

    struct aws_s3_request *request = vip_connection->request;
    AWS_PRECONDITION(request);

    struct aws_s3_meta_request_default *meta_request_default = meta_request->impl;
    AWS_PRECONDITION(meta_request_default);

    struct aws_http_message *message = aws_s3_message_util_copy_http_message(
        meta_request->allocator, meta_request->initial_request_message, AWS_S3_COPY_MESSAGE_INCLUDE_SSE);

    if (is_initial_prepare && meta_request_default->content_length > 0) {
        aws_byte_buf_init(&request->request_body, meta_request->allocator, meta_request_default->content_length);
        aws_s3_meta_request_read_body(meta_request, &request->request_body);
    }

    aws_s3_message_util_assign_body(meta_request->allocator, &request->request_body, message);

    aws_s3_request_setup_send_data(request, message);

    aws_http_message_release(message);

    AWS_LOGF_DEBUG(
        AWS_LS_S3_META_REQUEST, "id=%p: Meta Request prepared request %p", (void *)meta_request, (void *)request);

    return AWS_OP_SUCCESS;
}

static int s_s3_meta_request_default_header_block_done(
    struct aws_http_stream *stream,
    enum aws_http_header_block header_block,
    struct aws_s3_vip_connection *vip_connection) {

    (void)stream;
    (void)header_block;

    AWS_PRECONDITION(stream);
    AWS_PRECONDITION(vip_connection);

    struct aws_s3_request *request = vip_connection->request;
    AWS_PRECONDITION(request);

    struct aws_s3_meta_request *meta_request = request->meta_request;
    AWS_ASSERT(meta_request);

    if (meta_request->headers_callback != NULL && meta_request->headers_callback(
                                                      meta_request,
                                                      request->send_data.response_headers,
                                                      request->send_data.response_status,
                                                      meta_request->user_data)) {

        aws_s3_meta_request_finish(meta_request, NULL, 0, aws_last_error_or_unknown());
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}

static void s_s3_meta_request_default_notify_request_destroyed(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(request);

    aws_s3_meta_request_finish(meta_request, NULL, request->send_data.response_status, AWS_ERROR_SUCCESS);
}
