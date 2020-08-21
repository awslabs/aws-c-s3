#include "aws/s3/private/s3_util.h"
#include <aws/http/request_response.h>
#include <aws/s3/s3.h>

const struct aws_byte_cursor g_host_header_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Host");
const struct aws_byte_cursor g_range_header_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Range");
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
