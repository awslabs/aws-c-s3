/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_client_impl.h"
#include "aws/s3/private/s3_get_object_request.h"
#include "aws/s3/private/s3_meta_request_impl.h"
#include "aws/s3/private/s3_put_object_request.h"
#include "aws/s3/private/s3_request.h"
#include "aws/s3/private/s3_request_pipeline.h"
#include "aws/s3/private/s3_util.h"
#include <aws/common/clock.h>
#include <inttypes.h>

enum s3_push_request_flag { S3_PUSH_REQUEST_FLAG_LAST_REQUEST = 0x00000001 };

enum s3_gen_ranged_gets_flag {
    S3_GEN_RANGED_GETS_FLAG_SKIP_FIRST = 0x00000001,
    S3_GEN_RANGED_GETS_FLAG_LAST_REQUEST = 0x00000002
};

static void s_s3_meta_request_lock_synced_data(struct aws_s3_meta_request *meta_request);
static void s_s3_meta_request_unlock_synced_data(struct aws_s3_meta_request *meta_request);

static void s_s3_meta_request_check_for_completion_synced(struct aws_s3_meta_request *meta_request);
static void s_s3_meta_request_finish_synced(struct aws_s3_meta_request *meta_request, int error_code);

static int s_s3_meta_request_push_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *reques,
    uint32_t flags);

/* Auto Ranged Get Functions */
static int s_s3_meta_request_setup_parallel_get(struct aws_s3_meta_request *meta_request);

static int s_s3_meta_request_initial_get_object_headers_finished_callback(
    struct aws_s3_get_object_request *request,
    struct aws_http_stream *stream,
    void *user_data);

int s_s3_meta_request_generate_ranged_gets(
    struct aws_s3_meta_request *meta_request,
    uint64_t range_start,
    uint64_t range_end,
    uint32_t flags);
/* */

/* Auto Ranged Put Functions */
static int s_s3_meta_request_setup_parallel_put(struct aws_s3_meta_request *meta_request);
/* */

static int s_s3_meta_request_pipeline_body_callback(
    struct aws_s3_request_pipeline *pipeline,
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    const struct aws_byte_cursor *data,
    const struct aws_s3_body_meta_data *meta_data,
    void *user_data);

static void s_s3_meta_request_pipeline_finished(
    struct aws_s3_request_pipeline *pipeline,
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    int error_code,
    void *user_data);

static void s_s3_meta_request_lock_synced_data(struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);

    aws_mutex_lock(&meta_request->synced_data.lock);
}

static void s_s3_meta_request_unlock_synced_data(struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);

    aws_mutex_unlock(&meta_request->synced_data.lock);
}

struct aws_s3_meta_request *aws_s3_meta_request_new(
    struct aws_allocator *allocator,
    struct aws_s3_client *client,
    const struct aws_s3_meta_request_internal_options *internal_options) {

    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(internal_options);
    AWS_PRECONDITION(internal_options->options);

    const struct aws_s3_meta_request_options *options = internal_options->options;
    AWS_PRECONDITION(options->message);

    struct aws_s3_meta_request *meta_request = aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_meta_request));

    if (meta_request == NULL) {
        AWS_LOGF_ERROR(AWS_LS_S3_CLIENT, "Could not create s3 meta request for client %p", (void *)client);
        return NULL;
    }

    meta_request->allocator = allocator;

    /* Set up reference count. */
    aws_atomic_init_int(&meta_request->ref_count, 0);
    aws_s3_meta_request_acquire(meta_request);

    meta_request->part_size = client->part_size;

    aws_sys_clock_get_ticks(&meta_request->start_time);

    meta_request->initial_request_message = options->message;
    aws_http_message_acquire(options->message);

    aws_mutex_init(&meta_request->synced_data.lock);

    aws_linked_list_init(&meta_request->synced_data.request_queue);

    meta_request->user_data = options->user_data;
    meta_request->body_callback = options->body_callback;
    meta_request->finish_callback = options->finish_callback;

    meta_request->internal_user_data = internal_options->user_data;
    meta_request->internal_finish_callback = internal_options->finish_callback;

    if (options->type == AWS_S3_META_REQUEST_TYPE_GET_OBJECT) {
        s_s3_meta_request_setup_parallel_get(meta_request);
    } else if (options->type == AWS_S3_META_REQUEST_TYPE_PUT_OBJECT) {
        s_s3_meta_request_setup_parallel_put(meta_request);
    } else {
        AWS_FATAL_ASSERT(false);
    }

    return meta_request;
}

void aws_s3_meta_request_acquire(struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);

    aws_atomic_fetch_add(&meta_request->ref_count, 1);
}

void aws_s3_meta_request_release(struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);

    size_t prev_ref_count = aws_atomic_fetch_sub(&meta_request->ref_count, 1);

    if (prev_ref_count > 1) {
        return;
    }

    /* Dump anthing still in our queue*/
    while (!aws_linked_list_empty(&meta_request->synced_data.request_queue)) {
        struct aws_linked_list_node *next = aws_linked_list_pop_front(&meta_request->synced_data.request_queue);
        struct aws_s3_request *request = AWS_CONTAINER_OF(next, struct aws_s3_request, node);
        aws_s3_request_destroy(request);
    }

    /* Clean up our initial http message */
    if (meta_request->initial_request_message != NULL) {
        aws_http_message_release(meta_request->initial_request_message);
        meta_request->initial_request_message = NULL;
    }

    aws_mutex_clean_up(&meta_request->synced_data.lock);
    aws_mem_release(meta_request->allocator, meta_request);
}

/* Try to pop a request into the give pipeline. */
int aws_s3_meta_request_pop_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request_pipeline *pipeline,
    int *out_found_request) {
    AWS_PRECONDITION(meta_request);

    struct aws_s3_request *request = NULL;
    s_s3_meta_request_lock_synced_data(meta_request);

    /* Pop something from the front of the queue if there is stuff in the queue. */
    if (!aws_linked_list_empty(&meta_request->synced_data.request_queue)) {
        struct aws_linked_list_node *next = aws_linked_list_pop_front(&meta_request->synced_data.request_queue);
        request = AWS_CONTAINER_OF(next, struct aws_s3_request, node);
    }

    if (request != NULL) {
        AWS_LOGF_INFO(
            AWS_LS_S3_META_REQUEST,
            "id=%p, Popping request from meta request into pipeline %p",
            (void *)meta_request,
            (void *)pipeline);

        ++meta_request->synced_data.in_flight_requests;
    }

    s_s3_meta_request_unlock_synced_data(meta_request);

    struct aws_s3_request_pipeline_exec_options options = {
        .meta_request = meta_request,
        .request = request,
        .listener = {.body_callback = s_s3_meta_request_pipeline_body_callback,
                     .exec_finished_callback = s_s3_meta_request_pipeline_finished,
                     .exec_cleaned_up_callback = NULL,
                     .user_data = NULL}};

    if (request != NULL) {
        aws_s3_request_pipeline_setup_execute(pipeline, &options);
    }

    if (out_found_request) {
        *out_found_request = request != NULL;
    }

    return AWS_OP_SUCCESS;
}

/* Callback for receiving incoming body data from the pipeline.  This happens after a request has had time to look at
 * the data and designate if this is actually file contents or an error message. */
static int s_s3_meta_request_pipeline_body_callback(
    struct aws_s3_request_pipeline *pipeline,
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    const struct aws_byte_cursor *data,
    const struct aws_s3_body_meta_data *meta_data,
    void *user_data) {

    AWS_PRECONDITION(pipeline);
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(request);
    (void)pipeline;
    (void)request;
    (void)meta_data;
    (void)user_data;

    if ((meta_data->data_flags & AWS_S3_INCOMING_BODY_FLAG_OBJECT_DATA) == 0) {
        return AWS_OP_SUCCESS;
    }

    if (meta_request->body_callback != NULL) {
        return meta_request->body_callback(
            meta_request, data, meta_data->range_start, meta_data->range_end, meta_request->user_data);
    }

    return AWS_OP_SUCCESS;
}

/* Callback that is issued when one of this meta request's requests has finished going through a pipeline. */
static void s_s3_meta_request_pipeline_finished(
    struct aws_s3_request_pipeline *pipeline,
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    int error_code,
    void *user_data) {
    AWS_PRECONDITION(pipeline);
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(request);
    (void)pipeline;
    (void)user_data;

    s_s3_meta_request_lock_synced_data(meta_request);

    /* According to S3 Error Best Practices, internal errors do not represent a mistake on the part of the client and
     * shoul be retried. https://docs.aws.amazon.com/AmazonS3/latest/dev/ErrorBestPractices.html  */
    if (error_code == AWS_ERROR_S3_INTERNAL_ERROR) {
        aws_linked_list_push_front(&meta_request->synced_data.request_queue, &request->node);
    } else {
        aws_s3_request_destroy(request);
    }

    --meta_request->synced_data.in_flight_requests;

    /* If we received an error that aws not the fault of the service, then finish this request immediately with an
     * error. */
    if (error_code != AWS_ERROR_S3_INTERNAL_ERROR) {
        s_s3_meta_request_finish_synced(meta_request, error_code);
    } else {
        s_s3_meta_request_check_for_completion_synced(meta_request);
    }

    s_s3_meta_request_unlock_synced_data(meta_request);
}

/* Get the timestamp for when this meta request was initiated. */
uint64_t aws_s3_meta_request_get_start_time(const struct aws_s3_meta_request *meta_request) {
    /* Value is immutable after creation, so we can just read it. */
    return meta_request->start_time;
}

/* Finishes the request if there is no work remaining. */
static void s_s3_meta_request_check_for_completion_synced(struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);

    ASSERT_SYNCED_DATA_LOCK_HELD(meta_request);

    if (meta_request->synced_data.queue_finished_populating &&
        aws_linked_list_empty(&meta_request->synced_data.request_queue) &&
        meta_request->synced_data.in_flight_requests == 0) {

        AWS_LOGF_INFO(AWS_LS_S3_META_REQUEST, "id=%p Request queue is empty, calling finish.", (void *)meta_request);
        s_s3_meta_request_finish_synced(meta_request, AWS_ERROR_SUCCESS);
    }
}

/* Finish up the meta request, regardless of how much work is left. */
static void s_s3_meta_request_finish_synced(struct aws_s3_meta_request *meta_request, int error_code) {
    AWS_PRECONDITION(meta_request);

    ASSERT_SYNCED_DATA_LOCK_HELD(meta_request);

    AWS_LOGF_INFO(
        AWS_LS_S3_META_REQUEST, "id=%p Meta request finished with error code %d", (void *)meta_request, error_code);

    if (error_code != AWS_ERROR_SUCCESS) {
        AWS_LOGF_INFO(
            AWS_LS_S3_META_REQUEST, "id=%p Meta request failed with error code %d", (void *)meta_request, error_code);
    }

    meta_request->synced_data.queue_finished_populating = true;

    while (!aws_linked_list_empty(&meta_request->synced_data.request_queue)) {
        struct aws_linked_list_node *next = aws_linked_list_pop_front(&meta_request->synced_data.request_queue);
        struct aws_s3_request *request = AWS_CONTAINER_OF(next, struct aws_s3_request, node);
        aws_s3_request_destroy(request);
    }

    // TODO we should trigger these callbacks outside of the mutex to be safe.
    if (meta_request->finish_callback != NULL) {
        meta_request->finish_callback(meta_request, error_code, meta_request->user_data);
    }

    if (meta_request->internal_finish_callback != NULL) {
        meta_request->internal_finish_callback(meta_request, error_code, meta_request->internal_user_data);
    }
}

/* Push a single request into the meta request. Should not be used for many request adds. */
static int s_s3_meta_request_push_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    uint32_t flags) {
    AWS_PRECONDITION(meta_request);

    AWS_LOGF_INFO(AWS_LS_S3_META_REQUEST, "id=%p Pushing request into meta request.", (void *)meta_request);

    s_s3_meta_request_lock_synced_data(meta_request);

    if (request == NULL) {
        AWS_LOGF_ERROR(AWS_LS_S3_META_REQUEST, "id=%p: Could not push null request.", (void *)meta_request);
        s_s3_meta_request_finish_synced(meta_request, aws_last_error());
        s_s3_meta_request_unlock_synced_data(meta_request);
        goto error_result;
    }

    if ((flags & S3_PUSH_REQUEST_FLAG_LAST_REQUEST) != 0) {
        meta_request->synced_data.queue_finished_populating = true;
    }

    aws_linked_list_push_back(&meta_request->synced_data.request_queue, &request->node);
    s_s3_meta_request_unlock_synced_data(meta_request);

    return AWS_OP_SUCCESS;

error_result:

    return AWS_OP_ERR;
}

/* If possible, try to parallelize a given get operation into a series of ranged get operations. */
static int s_s3_meta_request_setup_parallel_get(struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(meta_request->initial_request_message);

    struct aws_http_message *message = meta_request->initial_request_message;

    bool already_has_range_header = false;

    for (size_t header_index = 0; header_index < aws_http_message_get_header_count(message); ++header_index) {
        struct aws_http_header header;

        if (aws_http_message_get_header(message, &header, header_index)) {
            AWS_LOGF_ERROR(
                AWS_LS_S3_META_REQUEST,
                "id=%p Could not find header for HTTP message while trying to set parallel get.",
                (void *)meta_request);
            goto error_result;
        }

        if (aws_http_header_name_eq(header.name, g_range_header_name)) {
            already_has_range_header = true;
            break;
        }
    }

    struct aws_s3_request_options request_options = {.message = meta_request->initial_request_message};

    /* TODO If we already have a ranged header, eventually we can break the range up into parts too.  However, this
     * requires additional parsing of this header value, so for now, we just send the message. */
    if (already_has_range_header) {

        struct aws_s3_get_object_request_options get_object_request_options = {
            .range_start = 0, .range_end = 0, .headers_finished_callback = NULL, .user_data = meta_request};

        struct aws_s3_request *request =
            aws_s3_get_object_request_new(meta_request->allocator, &request_options, &get_object_request_options);

        if (s_s3_meta_request_push_request(meta_request, request, S3_PUSH_REQUEST_FLAG_LAST_REQUEST)) {
            goto error_result;
        }

    } else {
        /* We initially queue just one ranged get that is the size of a single part.  The headers from this first get
         * will tell us the size of the object, and we can spin up additional gets if necessary. */
        struct aws_s3_get_object_request_options get_object_request_options = {
            .range_start = 0,
            .range_end = meta_request->part_size - 1,
            .headers_finished_callback = s_s3_meta_request_initial_get_object_headers_finished_callback,
            .user_data = meta_request};

        struct aws_s3_request *request =
            aws_s3_get_object_request_new(meta_request->allocator, &request_options, &get_object_request_options);

        if (s_s3_meta_request_push_request(meta_request, request, 0)) {
            goto error_result;
        }
    }

    return AWS_OP_SUCCESS;

error_result:
    return AWS_OP_ERR;
}

/* Callback for when the initial get-object (which is used to discove the total size of an object) is processed.  We
 * discover the total size of the object based off of the header in that request, and queue more requests if needed. */
static int s_s3_meta_request_initial_get_object_headers_finished_callback(
    struct aws_s3_get_object_request *request,
    struct aws_http_stream *stream,
    void *user_data) {
    AWS_PRECONDITION(request);

    bool is_success = false;

    if (aws_s3_is_stream_response_status_success(stream, &is_success)) {
        return AWS_OP_ERR;
    } else if (!is_success) {
        return AWS_OP_SUCCESS;
    }

    uint64_t content_range_object_size = request->result.content_range.total_object_size;

    /* We did a ranged get so we should have this data. */
    if (content_range_object_size == 0) {
        AWS_LOGF_ERROR(AWS_LS_S3_REQUEST, "id=%p: S3 Get Object has invalid content range.", (void *)request);

        aws_raise_error(AWS_ERROR_S3_MISSING_CONTENT_RANGE_HEADER);
        return AWS_OP_ERR;
    }

    struct aws_s3_meta_request *meta_request = user_data;

    AWS_LOGF_INFO(
        AWS_LS_S3_META_REQUEST,
        "id=%p Detected size for object is %" PRIu64,
        (void *)meta_request,
        content_range_object_size);

    /* Try to queue additional requests based off the retrieved object size. */
    return s_s3_meta_request_generate_ranged_gets(
        meta_request,
        0,
        content_range_object_size - 1,
        S3_GEN_RANGED_GETS_FLAG_SKIP_FIRST | S3_GEN_RANGED_GETS_FLAG_LAST_REQUEST);
}

/* Allocate/queue ranged gets given the range and our part size. */
int s_s3_meta_request_generate_ranged_gets(
    struct aws_s3_meta_request *meta_request,
    uint64_t range_start,
    uint64_t range_end,
    uint32_t flags) {

    bool skip_first_part = (flags & S3_GEN_RANGED_GETS_FLAG_SKIP_FIRST) != 0;
    bool set_finish_populating = (flags & S3_GEN_RANGED_GETS_FLAG_LAST_REQUEST) != 0;

    struct aws_linked_list stack_request_list;
    aws_linked_list_init(&stack_request_list);

    struct aws_s3_request_options request_options;
    AWS_ZERO_STRUCT(request_options);
    request_options.message = meta_request->initial_request_message;

    uint64_t total_size = range_end - range_start + 1;
    uint64_t num_parts = total_size / meta_request->part_size;

    if ((total_size % meta_request->part_size) > 0) {
        ++num_parts;
    }

    uint64_t part_range_start = 0;
    uint64_t num_parts_added = 0;

    /* Allocate requests for each individual ranged get or "part". */
    /* TODO may be able to allocate these in bulk instead of n amount of allocations. */
    for (uint64_t part_index = 0; part_index < num_parts; ++part_index) {

        if (part_index == 0 && skip_first_part) {
            part_range_start += meta_request->part_size;
            continue;
        }

        uint64_t part_range_end = part_range_start + meta_request->part_size - 1;

        if (part_range_end > range_end) {
            part_range_end = range_end;
        }

        struct aws_s3_get_object_request_options get_object_request_options = {.range_start = part_range_start,
                                                                               .range_end = part_range_end};

        struct aws_s3_request *request =
            aws_s3_get_object_request_new(meta_request->allocator, &request_options, &get_object_request_options);

        if (request == NULL) {
            AWS_LOGF_ERROR(
                AWS_LS_S3_META_REQUEST,
                "id=%p: Could not allocate new ranged get request for meta request.",
                (void *)meta_request);

            goto error_clean_up;
        }

        aws_linked_list_push_back(&stack_request_list, &request->node);
        part_range_start += meta_request->part_size;

        ++num_parts_added;
    }

    s_s3_meta_request_lock_synced_data(meta_request);

    /* Queue any requests that we just created */
    while (!aws_linked_list_empty(&stack_request_list)) {
        struct aws_linked_list_node *request_node = aws_linked_list_pop_front(&stack_request_list);
        struct aws_s3_request *request = AWS_CONTAINER_OF(request_node, struct aws_s3_request, node);
        aws_linked_list_push_back(&meta_request->synced_data.request_queue, &request->node);
    }

    if (set_finish_populating) {
        /* No additional requests will need to be allocated. */
        meta_request->synced_data.queue_finished_populating = true;
        s_s3_meta_request_check_for_completion_synced(meta_request);
    }

    s_s3_meta_request_unlock_synced_data(meta_request);

    return AWS_OP_SUCCESS;

error_clean_up:

    while (num_parts_added > 0) {
        struct aws_linked_list_node *node = aws_linked_list_pop_back(&stack_request_list);
        struct aws_s3_request *request = AWS_CONTAINER_OF(node, struct aws_s3_request, node);
        aws_s3_request_destroy(request);
        --num_parts_added;
    }

    return AWS_OP_ERR;
}

/* TODO.  For now, we just queue a single put, but soon this will be a multipart upload. */
static int s_s3_meta_request_setup_parallel_put(struct aws_s3_meta_request *meta_request) {

    struct aws_s3_request_options request_options = {.message = meta_request->initial_request_message};

    struct aws_s3_request *request = aws_s3_put_object_request_new(meta_request->allocator, &request_options);

    return s_s3_meta_request_push_request(meta_request, request, S3_PUSH_REQUEST_FLAG_LAST_REQUEST);
}
