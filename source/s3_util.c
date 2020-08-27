#include "aws/s3/private/s3_util.h"
#include <aws/http/request_response.h>
#include <aws/s3/s3.h>

const struct aws_byte_cursor g_host_header_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Host");
const struct aws_byte_cursor g_range_header_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Range");
const struct aws_byte_cursor g_etag_header_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("ETag");
const struct aws_byte_cursor g_content_length_header_name_name =
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Content-Length");
const struct aws_byte_cursor g_content_range_header_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Content-Range");
const struct aws_byte_cursor g_content_type_header_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Content-Type");
const struct aws_byte_cursor g_content_length_header_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Content-Length");

/* Checks response status against any successful response statuses. */
bool aws_s3_is_response_status_success(int response_status) {
    return response_status == AWS_S3_RESPONSE_STATUS_SUCCESS || response_status == AWS_S3_RESPONSE_STATUS_RANGE_SUCCESS;
}

/* Polls the response status and checks to make sure that status is a success status. */
int aws_s3_is_stream_response_status_success(struct aws_http_stream *stream, bool *out_is_success) {
    AWS_PRECONDITION(stream);
    AWS_PRECONDITION(out_is_success);

    int response_status = 0;

    if (aws_http_stream_get_incoming_response_status(stream, &response_status)) {
        AWS_LOGF_ERROR(AWS_LS_S3_GENERAL, "Could not get response status for s3 request.");

        return AWS_OP_ERR;
    }

    *out_is_success = aws_s3_is_response_status_success(response_status);

    return AWS_OP_SUCCESS;
}

/*
static const size_t s_meta_request_list_initial_capacity = 16;
static const size_t s_out_of_order_removal_list_initial_capacity = 16;
static const uint64_t s_vip_connection_processing_retry_offset_ms = 50;

static size_t s_s3_find_meta_request(
    const struct aws_array_list *meta_requests,
    const struct aws_s3_meta_request *meta_request) {
    size_t num_meta_requests = aws_array_list_length(meta_requests);

    for (size_t meta_request_index = 0; meta_request_index < num_meta_requests; ++meta_request_index) {

        struct aws_s3_meta_request *meta_request_item = NULL;

        aws_array_list_get_at(meta_requests, &meta_request_item, meta_request_index);

        if (meta_request_item == meta_request) {
            return meta_request_index;
        }
    }
    return (size_t)-1;
}
*/
