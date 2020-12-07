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

    struct {
        enum aws_s3_meta_request_default_state state;
    } synced_data;
};

static void s_s3_meta_request_default_lock_synced_data(struct aws_s3_meta_request_default *meta_request_default);
static void s_s3_meta_request_default_unlock_synced_data(struct aws_s3_meta_request_default *meta_request_default);

static void s_s3_meta_request_default_destroy(struct aws_s3_meta_request *meta_request);

static bool s_s3_meta_request_default_has_work(const struct aws_s3_meta_request *meta_request);

static int s_s3_meta_request_default_next_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request **out_request);

static int s_s3_meta_request_default_prepare_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_client *client,
    struct aws_s3_request *request);

static int s_s3_meta_request_default_header_block_done(
    struct aws_http_stream *stream,
    enum aws_http_header_block header_block,
    struct aws_s3_vip_connection *vip_connection);

static int s_s3_meta_request_default_incoming_body(
    struct aws_http_stream *stream,
    const struct aws_byte_cursor *data,
    struct aws_s3_vip_connection *vip_connection);

static int s_s3_meta_request_default_stream_complete(
    struct aws_http_stream *stream,
    struct aws_s3_vip_connection *vip_connection);

static void s_s3_meta_request_default_write_body_callback(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request);

static struct aws_s3_meta_request_vtable s_s3_meta_request_default_vtable = {
    .has_work = s_s3_meta_request_default_has_work,
    .next_request = s_s3_meta_request_default_next_request,
    .send_request_finish = aws_s3_meta_request_send_request_finish_default,
    .prepare_request = s_s3_meta_request_default_prepare_request,
    .init_signing_date_time = aws_s3_meta_request_sign_request_default,
    .sign_request = aws_s3_meta_request_sign_request_default,
    .incoming_headers = NULL,
    .incoming_headers_block_done = s_s3_meta_request_default_header_block_done,
    .incoming_body = s_s3_meta_request_default_incoming_body,
    .stream_complete = s_s3_meta_request_default_stream_complete,
    .destroy = s_s3_meta_request_default_destroy};

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
    const struct aws_s3_meta_request_options *options) {
    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(options);
    AWS_PRECONDITION(options->message);

    struct aws_s3_meta_request_default *meta_request_default =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_meta_request_default));

    /* Try to initialize the base type. */
    if (aws_s3_meta_request_init_base(
            allocator,
            client,
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

static bool s_s3_auto_ranged_state_has_work(enum aws_s3_meta_request_default_state state) {
    return state == AWS_S3_META_REQUEST_DEFAULT_STATE_START;
}

static bool s_s3_meta_request_default_has_work(const struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(meta_request->impl);

    struct aws_s3_meta_request_default *meta_request_default = meta_request->impl;

    bool has_work = false;

    s_s3_meta_request_default_lock_synced_data((struct aws_s3_meta_request_default *)meta_request_default);
    has_work = s_s3_auto_ranged_state_has_work(meta_request_default->synced_data.state);
    s_s3_meta_request_default_unlock_synced_data((struct aws_s3_meta_request_default *)meta_request_default);

    return has_work;
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
        request = aws_s3_request_new(
            meta_request,
            0,
            0,
            AWS_S3_REQUEST_DESC_RECORD_RESPONSE_HEADERS | AWS_S3_REQUEST_DESC_USE_INITIAL_BODY_STREAM);

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
    struct aws_s3_request *request) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(request);

    struct aws_s3_part_buffer *part_buffer = aws_s3_client_get_part_buffer(client, request->desc_data.part_number);

    if (part_buffer == NULL) {
        return AWS_OP_ERR;
    }

    struct aws_http_message *message =
        aws_s3_message_util_copy_http_message(meta_request->allocator, meta_request->initial_request_message);

    aws_s3_request_setup_send_data(request, message, part_buffer);

    aws_http_message_release(message);

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

    struct aws_s3_request *request = vip_connection->work_data.request;
    AWS_PRECONDITION(request);

    struct aws_s3_meta_request *meta_request = request->meta_request;
    AWS_ASSERT(meta_request);

    if (meta_request->headers_callback != NULL) {
        meta_request->headers_callback(
            meta_request,
            request->send_data.response_headers,
            request->send_data.response_status,
            meta_request->user_data);
    }

    return AWS_OP_SUCCESS;
}

static int s_s3_meta_request_default_incoming_body(
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

static int s_s3_meta_request_default_stream_complete(
    struct aws_http_stream *stream,
    struct aws_s3_vip_connection *vip_connection) {
    (void)stream;

    AWS_PRECONDITION(vip_connection);

    struct aws_s3_request *request = vip_connection->work_data.request;
    AWS_PRECONDITION(request);
    AWS_PRECONDITION(request->send_data.part_buffer);

    /* Schedule the part buffer to be sent back to the user so that they can process it. */
    aws_s3_meta_request_write_body_to_caller(request, s_s3_meta_request_default_write_body_callback);

    return AWS_OP_SUCCESS;
}

static void s_s3_meta_request_default_write_body_callback(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(meta_request->impl);
    AWS_PRECONDITION(request);

    aws_s3_meta_request_finish(meta_request, NULL, request->send_data.response_status, AWS_ERROR_SUCCESS);
}
