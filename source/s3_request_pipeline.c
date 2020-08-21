#include "aws/s3/private/s3_request_pipeline.h"
#include "aws/s3/private/s3_client_impl.h"
#include "aws/s3/private/s3_request.h"
#include "aws/s3/private/s3_util.h"

#include <aws/auth/credentials.h>
#include <aws/auth/signable.h>
#include <aws/auth/signing.h>
#include <aws/auth/signing_config.h>
#include <aws/auth/signing_result.h>
#include <aws/common/assert.h>
#include <aws/common/string.h>
#include <aws/http/connection.h>
#include <aws/http/connection_manager.h>
#include <aws/http/request_response.h>

enum pipeline_listener_slot {
    PIPELINE_LISTENER_SLOT_CREATOR,
    PIPELINE_LISTENER_SLOT_EXECUTOR,
    PIPELINE_LISTENER_SLOT_MAX
};

/* Context for a request being sent. */
struct aws_s3_request_pipeline {

    struct aws_allocator *allocator;

    struct aws_credentials_provider *credentials_provider;
    struct aws_http_connection_manager *http_connection_manager;
    struct aws_string *region;

    /* Request count for the connection on this pipeline.  Connections with S3 have a maximum amount of requests which
     * we need to take into account. */
    int32_t connection_request_count;

    /* Current meta request that is being processed. */
    struct aws_s3_meta_request *meta_request;

    /* Current request that is being processed. */
    struct aws_s3_request *request;

    /* HTTP connection currently in use by this pipeline.  A single connection is re-used until connection_request_count
     * is hit. */
    struct aws_http_connection *http_connection;

    /* Current HTTP stream for this transfer. */
    struct aws_http_stream *stream;

    /* Array of listeners (a number of callbacks with user data) */
    struct aws_s3_request_pipeline_listener listeners[(uint32_t)PIPELINE_LISTENER_SLOT_MAX];
};

static const int32_t s_s3_max_request_count_per_connection = 100;

static void s_s3_request_pipeline_signing_complete(struct aws_signing_result *result, int error_code, void *user_data);
static void s_s3_request_pipeline_acquire_connection(struct aws_s3_request_pipeline *pipeline);

static void s_s3_request_pipeline_on_acquire_connection(
    struct aws_http_connection *http_connection,
    int error_code,
    void *user_data);

static int s_s3_request_pipeline_incoming_headers(
    struct aws_http_stream *stream,
    enum aws_http_header_block header_block,
    const struct aws_http_header *headers,
    size_t headers_count,
    void *user_data);

static int s_s3_request_pipeline_incoming_header_block_done(
    struct aws_http_stream *stream,
    enum aws_http_header_block header_block,
    void *user_data);

static int s_s3_request_pipeline_incoming_body(
    struct aws_http_stream *stream,
    const struct aws_byte_cursor *data,
    void *user_data);

static void s_s3_request_pipeline_stream_complete(struct aws_http_stream *stream, int error_code, void *user_data);
static void s_s3_request_pipeline_finish_request(struct aws_s3_request_pipeline *pipeline, int error_code);

/* Allocate a new request pipeline. */
struct aws_s3_request_pipeline *aws_s3_request_pipeline_new(
    struct aws_allocator *allocator,
    struct aws_s3_request_pipeline_options *options) {
    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(options);
    AWS_PRECONDITION(options->credentials_provider);
    AWS_PRECONDITION(options->http_connection_manager);

    struct aws_s3_request_pipeline *pipeline = aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_request_pipeline));

    if (pipeline == NULL) {
        AWS_LOGF_ERROR(AWS_LS_S3_REQUEST_PIPELINE, "Could not allocate aws_s3_request_pipeline.");
        return NULL;
    }

    pipeline->allocator = allocator;

    pipeline->credentials_provider = options->credentials_provider;
    aws_credentials_provider_acquire(options->credentials_provider);

    pipeline->http_connection_manager = options->http_connection_manager;
    aws_http_connection_manager_acquire(pipeline->http_connection_manager);

    pipeline->region = aws_string_new_from_array(allocator, options->region.ptr, options->region.len);

    if (pipeline->region == NULL) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_REQUEST_PIPELINE,
            "id=%p Could not allocate region string for aws_s3_request_pipeline.",
            (void *)pipeline);
        goto failed_alloc_region;
    }

    pipeline->listeners[(uint32_t)PIPELINE_LISTENER_SLOT_CREATOR] = options->listener;

    return pipeline;

failed_alloc_region:

    aws_s3_request_pipeline_destroy(pipeline);

    return NULL;
}

/* Destroy this request pipeline. */
void aws_s3_request_pipeline_destroy(struct aws_s3_request_pipeline *request_pipeline) {
    AWS_PRECONDITION(request_pipeline);

    if (request_pipeline->credentials_provider != NULL) {
        aws_credentials_provider_release(request_pipeline->credentials_provider);
        request_pipeline->credentials_provider = NULL;
    }

    if (request_pipeline->http_connection != NULL) {
        aws_http_connection_manager_release_connection(
            request_pipeline->http_connection_manager, request_pipeline->http_connection);
        request_pipeline->http_connection = NULL;
    }

    if (request_pipeline->http_connection_manager != NULL) {
        aws_http_connection_manager_release(request_pipeline->http_connection_manager);
        request_pipeline->http_connection_manager = NULL;
    }

    if (request_pipeline->region != NULL) {
        aws_string_destroy(request_pipeline->region);
        request_pipeline->region = NULL;
    }

    aws_mem_release(request_pipeline->allocator, request_pipeline);
    request_pipeline = NULL;
}

/* Setup for a pipeline execution.  This provides all necessary options for processing a request, which includes the
 * request itself and its owning meta request.  Currently, it is not allowed to setup a new pipeline execution until a
 * the previously set up execution has been processed. */
void aws_s3_request_pipeline_setup_execute(
    struct aws_s3_request_pipeline *pipeline,
    struct aws_s3_request_pipeline_exec_options *options) {

    AWS_PRECONDITION(pipeline);
    AWS_PRECONDITION(options);
    AWS_PRECONDITION(options->meta_request != NULL);
    AWS_PRECONDITION(options->request != NULL);

    aws_s3_meta_request_acquire(options->meta_request);
    pipeline->meta_request = options->meta_request;
    pipeline->request = options->request;

    pipeline->listeners[(uint32_t)PIPELINE_LISTENER_SLOT_EXECUTOR] = options->listener;
}

/* Start executing the pipeline.  It is the responsibility of the caller to not call this again until a clean-up
 * callback has been triggered. */
void aws_s3_request_pipeline_execute(struct aws_s3_request_pipeline *pipeline) {
    AWS_PRECONDITION(pipeline);

    AWS_LOGF_DEBUG(AWS_LS_S3_REQUEST_PIPELINE, "id=%p Start triggered for request pipeline.", (void *)pipeline);

    struct aws_s3_meta_request *meta_request = pipeline->meta_request;
    struct aws_s3_request *request = pipeline->request;
    int error_code = AWS_ERROR_SUCCESS;

    if (meta_request == NULL) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_REQUEST_PIPELINE,
            "id=%p: Trying to execute request pipeline with a null meta request.",
            (void *)pipeline);

        error_code = AWS_ERROR_S3_MISSING_META_REQUEST;

        goto error_finish;
    }

    if (request == NULL) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_REQUEST_PIPELINE,
            "id=%p: Trying to execute request pipeline with a null request.",
            (void *)pipeline);

        error_code = AWS_ERROR_S3_MISSING_REQUEST;

        goto error_finish;
    }

    /* If we already have an allocated signable, this has already been signed (possible for a retried request), so skip
     * the signing process.*/
    if (request->signable != NULL) {

        s_s3_request_pipeline_acquire_connection(pipeline);

    } else {
        request->signable = aws_signable_new_http_request(pipeline->allocator, request->message);

        if (request->signable == NULL) {
            AWS_LOGF_ERROR(
                AWS_LS_S3_REQUEST_PIPELINE, "id=%p: Could not allocate signable for http request", (void *)pipeline);
            error_code = aws_last_error();
            goto error_finish;
        }

        struct aws_date_time now;
        aws_date_time_init_now(&now);

        struct aws_byte_cursor service_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("s3");

        struct aws_signing_config_aws signing_config;
        AWS_ZERO_STRUCT(signing_config);
        signing_config.config_type = AWS_SIGNING_CONFIG_AWS;
        signing_config.algorithm = AWS_SIGNING_ALGORITHM_V4;
        signing_config.credentials_provider = pipeline->credentials_provider;
        signing_config.region = aws_byte_cursor_from_array(pipeline->region->bytes, pipeline->region->len);
        signing_config.service = service_name;
        signing_config.date = now;
        signing_config.signed_body_value = g_aws_signed_body_value_unsigned_payload;
        signing_config.signed_body_header = AWS_SBHT_X_AMZ_CONTENT_SHA256;

        if (aws_sign_request_aws(
                pipeline->allocator,
                request->signable,
                (struct aws_signing_config_base *)&signing_config,
                s_s3_request_pipeline_signing_complete,
                pipeline)) {
            AWS_LOGF_ERROR(AWS_LS_S3_REQUEST_PIPELINE, "id=%p: Could not sign request", (void *)pipeline);
            error_code = aws_last_error();
            goto error_finish;
        }
    }

    return;

error_finish:

    s_s3_request_pipeline_finish_request(pipeline, error_code);
}

static void s_s3_request_pipeline_signing_complete(struct aws_signing_result *result, int error_code, void *user_data) {

    struct aws_s3_request_pipeline *pipeline = user_data;
    AWS_PRECONDITION(pipeline);

    struct aws_s3_request *request = pipeline->request;
    AWS_PRECONDITION(request);

    if (error_code != AWS_OP_SUCCESS) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_REQUEST_PIPELINE,
            "id=%p: Could not sign request due to error_code %d",
            (void *)pipeline,
            error_code);

        goto error_finish;
    }

    if (aws_apply_signing_result_to_http_request(request->message, pipeline->allocator, result)) {
        error_code = aws_last_error();
        AWS_LOGF_ERROR(
            AWS_LS_S3_REQUEST_PIPELINE,
            "id=%p: Could not apply signing result to http request due to error %d",
            (void *)pipeline,
            error_code);

        goto error_finish;
    }

    s_s3_request_pipeline_acquire_connection(pipeline);

    return;

error_finish:

    s_s3_request_pipeline_finish_request(pipeline, error_code);
}

static void s_s3_request_pipeline_acquire_connection(struct aws_s3_request_pipeline *pipeline) {
    AWS_PRECONDITION(pipeline);

    struct aws_http_connection *http_connection = pipeline->http_connection;

    if (http_connection != NULL) {
        /* If we're at the max request count, set us up to get a new connection.  Also close the original connection so
         * that the connection manager doesn't reuse it.  TODO maybe find a more visible way of preventing the
         * connection from going back into the pool. */
        if (pipeline->connection_request_count == s_s3_max_request_count_per_connection) {
            aws_http_connection_close(http_connection);
            aws_http_connection_manager_release_connection(pipeline->http_connection_manager, http_connection);

            http_connection = NULL;
            pipeline->connection_request_count = 0;
        } else if (!aws_http_connection_is_open(http_connection)) {
            /* If our connection is closed for some reason, also get rid of it.*/
            aws_http_connection_manager_release_connection(pipeline->http_connection_manager, http_connection);

            http_connection = NULL;
            pipeline->connection_request_count = 0;
        }
    }

    pipeline->http_connection = http_connection;

    if (http_connection != NULL) {
        s_s3_request_pipeline_on_acquire_connection(http_connection, AWS_ERROR_SUCCESS, pipeline);
    } else {
        aws_http_connection_manager_acquire_connection(
            pipeline->http_connection_manager, s_s3_request_pipeline_on_acquire_connection, pipeline);
    }
}

static void s_s3_request_pipeline_on_acquire_connection(
    struct aws_http_connection *http_connection,
    int error_code,
    void *user_data) {
    struct aws_s3_request_pipeline *pipeline = user_data;
    AWS_PRECONDITION(pipeline);

    struct aws_s3_request *request = pipeline->request;
    AWS_PRECONDITION(request);

    if (error_code != AWS_ERROR_SUCCESS || http_connection == NULL) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_REQUEST_PIPELINE,
            "id=%p: Could not acquire connection due to error code %d (%s)",
            (void *)pipeline,
            error_code,
            aws_error_str(error_code));
        goto error_finish;
    }

    /* If our cached connection is not equal to the one we just received, switch to the received one. */
    if (pipeline->http_connection != http_connection) {
        if (pipeline->http_connection != NULL) {
            aws_http_connection_manager_release_connection(
                pipeline->http_connection_manager, pipeline->http_connection);
            pipeline->http_connection = NULL;
        }

        pipeline->http_connection = http_connection;
        pipeline->connection_request_count = 0;
    }

    /* Now that we have a signed request and a connection, go ahead and issue the request. */
    struct aws_http_make_request_options options;
    AWS_ZERO_STRUCT(options);
    options.self_size = sizeof(struct aws_http_make_request_options);
    options.request = request->message;
    options.user_data = pipeline;
    options.on_response_headers = s_s3_request_pipeline_incoming_headers;
    options.on_response_header_block_done = s_s3_request_pipeline_incoming_header_block_done;
    options.on_response_body = s_s3_request_pipeline_incoming_body;
    options.on_complete = s_s3_request_pipeline_stream_complete;

    pipeline->stream = aws_http_connection_make_request(pipeline->http_connection, &options);

    if (pipeline->stream == NULL) {
        error_code = aws_last_error();
        AWS_LOGF_ERROR(AWS_LS_S3_REQUEST_PIPELINE, "id=%p: Could not make HTTP request", (void *)pipeline);
        goto error_finish;
    }

    if (aws_http_stream_activate(pipeline->stream) != AWS_OP_SUCCESS) {
        error_code = aws_last_error();
        AWS_LOGF_ERROR(AWS_LS_S3_REQUEST_PIPELINE, "id=%p: Could not activate HTTP stream", (void *)pipeline);
        goto error_finish;
    }

    return;

error_finish:
    s_s3_request_pipeline_finish_request(pipeline, error_code);
}

static int s_s3_request_pipeline_incoming_headers(
    struct aws_http_stream *stream,
    enum aws_http_header_block header_block,
    const struct aws_http_header *headers,
    size_t headers_count,
    void *user_data) {
    AWS_PRECONDITION(user_data);
    (void)stream;

    struct aws_s3_request_pipeline *pipeline = user_data;
    struct aws_s3_request *request = pipeline->request;
    AWS_FATAL_ASSERT(request != NULL);

    return aws_s3_request_incoming_headers(request, stream, header_block, headers, headers_count);
}

static int s_s3_request_pipeline_incoming_header_block_done(
    struct aws_http_stream *stream,
    enum aws_http_header_block header_block,
    void *user_data) {
    (void)stream;
    AWS_PRECONDITION(user_data);

    struct aws_s3_request_pipeline *pipeline = user_data;
    struct aws_s3_request *request = pipeline->request;
    AWS_FATAL_ASSERT(request != NULL);

    return aws_s3_request_incoming_header_block_done(request, stream, header_block);
}

static int s_s3_request_pipeline_incoming_body(
    struct aws_http_stream *stream,
    const struct aws_byte_cursor *data,
    void *user_data) {
    (void)stream;
    AWS_PRECONDITION(user_data);

    struct aws_s3_request_pipeline *pipeline = user_data;

    struct aws_s3_meta_request *meta_request = pipeline->meta_request;
    AWS_FATAL_ASSERT(meta_request != NULL);

    struct aws_s3_request *request = pipeline->request;
    AWS_FATAL_ASSERT(request != NULL);

    struct aws_s3_body_meta_data body_meta_data;
    AWS_ZERO_STRUCT(body_meta_data);

    /* Tell the request about the incoming data first, so that it can generate useful meta data about it. */
    if (aws_s3_request_incoming_body(request, stream, data, &body_meta_data)) {
        return AWS_OP_ERR;
    }

    /* Tell our listeners about the data, including the meta data generated by the request. */
    for (size_t listener_index = 0; listener_index < PIPELINE_LISTENER_SLOT_MAX; ++listener_index) {
        struct aws_s3_request_pipeline_listener *listener = &pipeline->listeners[listener_index];

        if (listener->body_callback != NULL) {
            listener->body_callback(pipeline, meta_request, request, data, &body_meta_data, listener->user_data);
        }
    }

    return AWS_OP_SUCCESS;
}

static void s_s3_request_pipeline_stream_complete(struct aws_http_stream *stream, int error_code, void *user_data) {
    (void)stream;
    AWS_PRECONDITION(user_data);

    struct aws_s3_request_pipeline *pipeline = user_data;

    struct aws_s3_meta_request *meta_request = pipeline->meta_request;
    AWS_FATAL_ASSERT(meta_request != NULL);

    struct aws_s3_request *request = pipeline->request;
    AWS_FATAL_ASSERT(request != NULL);

    /* TODO may one day need support for this being able to throw an error. */
    aws_s3_request_stream_complete(request, stream, error_code);

    s_s3_request_pipeline_finish_request(pipeline, error_code);
}

void s_s3_request_pipeline_finish_request(struct aws_s3_request_pipeline *pipeline, int error_code) {
    AWS_PRECONDITION(pipeline);

    int response_status = 0;

    if (error_code != AWS_ERROR_SUCCESS) {
        AWS_LOGF_DEBUG(
            AWS_LS_S3_REQUEST_PIPELINE,
            "id=%p Pipeline finished processing request with error code %d (%s)",
            (void *)pipeline,
            error_code,
            aws_error_str(error_code));
    } else {

        if (aws_http_stream_get_incoming_response_status(pipeline->stream, &response_status)) {
            error_code = aws_last_error();
        } else if (
            response_status == AWS_S3_RESPONSE_STATUS_SUCCESS ||
            response_status == AWS_S3_RESPONSE_STATUS_RANGE_SUCCESS) {
            /* Nothing to do for success respone status*/
        } else if (response_status == AWS_S3_RESPONSE_STATUS_INTERNAL_ERROR) {
            error_code = AWS_ERROR_S3_INTERNAL_ERROR;
            aws_raise_error(AWS_ERROR_S3_INTERNAL_ERROR);
        } else {
            error_code = AWS_ERROR_S3_INVALID_RESPONSE_STATUS;
            aws_raise_error(AWS_ERROR_S3_INVALID_RESPONSE_STATUS);
        }
    }

    struct aws_s3_meta_request *meta_request = pipeline->meta_request;
    AWS_FATAL_ASSERT(meta_request != NULL);

    struct aws_s3_request *request = pipeline->request;
    AWS_FATAL_ASSERT(request != NULL);

    ++pipeline->connection_request_count;

    /* TODO may one day need support for this being able to throw an error. */
    aws_s3_request_finish(request, error_code);

    /* Tell our listeners that everything has fnished. */
    for (size_t listener_index = 0; listener_index < PIPELINE_LISTENER_SLOT_MAX; ++listener_index) {
        struct aws_s3_request_pipeline_listener *listener = &pipeline->listeners[listener_index];

        if (listener->exec_finished_callback != NULL) {
            listener->exec_finished_callback(pipeline, meta_request, request, error_code, listener->user_data);
        }
    }

    aws_s3_meta_request_release(meta_request);

    pipeline->meta_request = NULL;
    pipeline->request = NULL;

    if (pipeline->stream != NULL) {
        aws_http_stream_release(pipeline->stream);
        pipeline->stream = NULL;
    }

    /* Tell our listeners that we have now cleaned up this execution of the pipeline. */
    /* TODO we may want to copy these, just in case someone is setting up a destroy in an early registered listener.*/
    for (size_t listener_index = 0; listener_index < PIPELINE_LISTENER_SLOT_MAX; ++listener_index) {
        struct aws_s3_request_pipeline_listener *listener = &pipeline->listeners[listener_index];

        if (listener->exec_cleaned_up_callback != NULL) {
            listener->exec_cleaned_up_callback(pipeline, error_code, listener->user_data);
        }
    }
}
