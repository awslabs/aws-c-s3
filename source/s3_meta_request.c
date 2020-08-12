/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_client_impl.h"
#include "aws/s3/private/s3_get_object_request.h"
#include "aws/s3/private/s3_meta_request_impl.h"
#include "aws/s3/private/s3_put_object_request.h"
#include "aws/s3/private/s3_request.h"
#include <aws/common/clock.h>

enum s3_push_request_flag { S3_PUSH_REQUEST_FLAG_LAST_REQUEST = 0x00000001 };

enum s3_gen_ranged_gets_flag {
    S3_GEN_RANGED_GETS_FLAG_SKIP_FIRST = 0x00000001,
    S3_GEN_RANGED_GETS_FLAG_LAST_REQUEST = 0x00000002
};

static void s_s3_meta_request_check_for_completion(struct aws_s3_meta_request *meta_request, struct aws_mutex *lock);

static void s_s3_meta_request_finish(struct aws_s3_meta_request *meta_request, int error_code, struct aws_mutex *lock);

static int s_s3_meta_request_push_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *reques,
    uint32_t flags);

/* Auto Ranged Get Functions */
static int s_s3_meta_request_setup_parallel_get(struct aws_s3_meta_request *meta_request);

static int s_s3_meta_request_initial_get_object_headers_finished_callback(
    struct aws_s3_get_object_request *request,
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

static int s_s3_meta_request_object_body_callback(
    struct aws_s3_request *request,
    struct aws_http_stream *stream,
    const struct aws_byte_cursor *body,
    void *user_data);

static void s_s3_meta_request_queued_request_finished_callback(
    struct aws_s3_request *request,
    int error_code,
    void *user_data);

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

    aws_sys_clock_get_ticks(&meta_request->initiated_timestamp);

    // TODO okay to keep this around?  Maybe we should make a copy instead of grabbing a new ref?
    meta_request->initial_request_message = options->message;
    aws_http_message_acquire(options->message);

    aws_mutex_init(&meta_request->lock);

    aws_linked_list_init(&meta_request->request_queue);

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
    while (!aws_linked_list_empty(&meta_request->request_queue)) {
        struct aws_linked_list_node *next = aws_linked_list_pop_front(&meta_request->request_queue);
        struct aws_s3_request *request = AWS_CONTAINER_OF(next, struct aws_s3_request, node);
        aws_s3_request_release(request);
    }

    /* Clean up our initial http message */
    if (meta_request->initial_request_message != NULL) {
        aws_http_message_release(meta_request->initial_request_message);
        meta_request->initial_request_message = NULL;
    }

    aws_mutex_clean_up(&meta_request->lock);
    aws_mem_release(meta_request->allocator, meta_request);
}

struct aws_s3_request *aws_s3_meta_request_pop_request(struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);

    struct aws_s3_request *request = NULL;
    aws_mutex_lock(&meta_request->lock);

    /* Pop something from the front of the queue if there is stuff in the queue. */
    if (!aws_linked_list_empty(&meta_request->request_queue)) {
        struct aws_linked_list_node *next = aws_linked_list_pop_front(&meta_request->request_queue);
        request = AWS_CONTAINER_OF(next, struct aws_s3_request, node);
    }

    if (request != NULL) {
        ++meta_request->outstanding_requests;
    }

    aws_mutex_unlock(&meta_request->lock);

    /* Once it leaves the queue, we store a reference to the meta request on the request.  This is so that we know the
     * meta request will stay around while the request is being processed, until it either comes back for a retry or
     * gets released. */
    if (request != NULL) {
        aws_s3_request_set_meta_request(request, meta_request);
    }

    /* We don't decrement ref count here.  At this point we surrender the existing ref count to the person using the
     * request. */
    return request;
}

/* Place a request that failed back into the queue so that we can retry it. */
int aws_s3_meta_request_retry_request(struct aws_s3_meta_request *meta_request, struct aws_s3_request *request) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(request);

    if (meta_request != request->meta_request) {
        return AWS_OP_ERR;
    }

    /* Grab a new reference for it's place in the list */
    aws_s3_request_acquire(request);

    /* Erase any reference to a meta request.  We now own the data, and we don't want these references prevent clean up.
     */
    aws_s3_request_set_meta_request(request, NULL);

    aws_mutex_lock(&meta_request->lock);
    aws_linked_list_push_back(&meta_request->request_queue, &request->node);

    if (request != NULL) {
        --meta_request->outstanding_requests;
    }
    aws_mutex_unlock(&meta_request->lock);

    return AWS_OP_SUCCESS;
}

/* Get the timestamp for when this meta request was initiated. */
uint64_t aws_s3_meta_request_get_initiated_timestamp(const struct aws_s3_meta_request *meta_request) {
    /* Value is immutable after creation, so we can just read it. */
    return meta_request->initiated_timestamp;
}

/* Finishes the request if there is no work remaining. */
static void s_s3_meta_request_check_for_completion(struct aws_s3_meta_request *meta_request, struct aws_mutex *lock) {
    AWS_PRECONDITION(meta_request);

    if (lock != NULL) {
        aws_mutex_lock(lock);
    }

    if (meta_request->queue_finished_populating && aws_linked_list_empty(&meta_request->request_queue) &&
        meta_request->outstanding_requests == 0) {
        AWS_LOGF_INFO(AWS_LS_S3_META_REQUEST, "id=%p Request queue is empty, calling finish.", (void *)meta_request);
        s_s3_meta_request_finish(meta_request, AWS_ERROR_SUCCESS, NULL);
    }

    if (lock != NULL) {
        aws_mutex_unlock(lock);
    }
}

/* Finish up the meta request. */
static void s_s3_meta_request_finish(struct aws_s3_meta_request *meta_request, int error_code, struct aws_mutex *lock) {
    AWS_PRECONDITION(meta_request);

    if (lock != NULL) {
        aws_mutex_lock(lock);
    }

    if (error_code != AWS_ERROR_SUCCESS) {
        AWS_LOGF_INFO(
            AWS_LS_S3_META_REQUEST, "id=%p Meta request failed with error code %d", (void *)meta_request, error_code);
    }

    meta_request->queue_finished_populating = true;

    while (!aws_linked_list_empty(&meta_request->request_queue)) {
        struct aws_linked_list_node *next = aws_linked_list_pop_front(&meta_request->request_queue);
        struct aws_s3_request *request = AWS_CONTAINER_OF(next, struct aws_s3_request, node);
        aws_s3_request_release(request);
    }

    // TODO trigger these callbacks outside of the mutex.
    if (meta_request->finish_callback != NULL) {
        meta_request->finish_callback(meta_request, error_code, meta_request->user_data);
    }

    if (meta_request->internal_finish_callback != NULL) {
        meta_request->internal_finish_callback(meta_request, error_code, meta_request->internal_user_data);
    }

    if (lock != NULL) {
        aws_mutex_unlock(lock);
    }
}

/* Push a single request into the meta request. Should not be used for many request adds. */
static int s_s3_meta_request_push_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    uint32_t flags) {
    AWS_PRECONDITION(meta_request);

    if (request == NULL) {
        AWS_LOGF_ERROR(AWS_LS_S3_META_REQUEST, "id=%p: Could not push null request.", (void *)meta_request);
        s_s3_meta_request_finish(meta_request, aws_last_error(), &meta_request->lock);
        goto error_result;
    }

    aws_mutex_lock(&meta_request->lock);

    if ((flags & S3_PUSH_REQUEST_FLAG_LAST_REQUEST) != 0) {
        meta_request->queue_finished_populating = true;
    }

    aws_linked_list_push_back(&meta_request->request_queue, &request->node);
    aws_mutex_unlock(&meta_request->lock);

    return AWS_OP_SUCCESS;

error_result:

    return AWS_OP_ERR;
}

/* Parallelize a get operation. */
static int s_s3_meta_request_setup_parallel_get(struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(meta_request->initial_request_message);

    struct aws_http_message *message = meta_request->initial_request_message;
    struct aws_byte_cursor range_header_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Range");

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

        if (aws_http_header_name_eq(header.name, range_header_name)) {
            already_has_range_header = true;
            break;
        }
    }

    struct aws_s3_request_options request_options = {.message = meta_request->initial_request_message,
                                                     .user_data = meta_request,
                                                     .body_callback = s_s3_meta_request_object_body_callback,
                                                     .finish_callback =
                                                         s_s3_meta_request_queued_request_finished_callback};

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

static int s_s3_meta_request_initial_get_object_headers_finished_callback(
    struct aws_s3_get_object_request *request,
    void *user_data) {
    AWS_PRECONDITION(request);

    uint64_t content_range_object_size = request->result.content_range.object_size;

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

    /* Try to queue additinoal requests based off the retrieved object size. */
    return s_s3_meta_request_generate_ranged_gets(
        meta_request,
        0,
        content_range_object_size - 1,
        S3_GEN_RANGED_GETS_FLAG_SKIP_FIRST | S3_GEN_RANGED_GETS_FLAG_LAST_REQUEST);
}

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
    request_options.user_data = meta_request;
    request_options.body_callback = s_s3_meta_request_object_body_callback;
    request_options.finish_callback = s_s3_meta_request_queued_request_finished_callback;

    uint64_t total_size = range_end - range_start + 1;
    uint64_t num_parts = total_size / meta_request->part_size;

    if ((total_size % meta_request->part_size) > 0) {
        ++num_parts;
    }

    uint64_t part_range_start = 0;
    uint64_t num_parts_added = 0;

    /* Allocate requests for each individual ranged get or "part" */
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

    aws_mutex_lock(&meta_request->lock);

    /* Queue any requests that we just created */
    while (!aws_linked_list_empty(&stack_request_list)) {
        struct aws_linked_list_node *request_node = aws_linked_list_pop_front(&stack_request_list);
        struct aws_s3_request *request = AWS_CONTAINER_OF(request_node, struct aws_s3_request, node);
        aws_linked_list_push_back(&meta_request->request_queue, &request->node);
    }

    if (set_finish_populating) {
        /* No additional requests will need to be allocated. */
        meta_request->queue_finished_populating = true;
        s_s3_meta_request_check_for_completion(meta_request, NULL);
    }

    aws_mutex_unlock(&meta_request->lock);

    return AWS_OP_SUCCESS;

error_clean_up:

    while (num_parts_added > 0) {
        struct aws_linked_list_node *node = aws_linked_list_pop_back(&stack_request_list);
        struct aws_s3_request *request = AWS_CONTAINER_OF(node, struct aws_s3_request, node);
        aws_s3_request_release(request);
        --num_parts_added;
    }

    return AWS_OP_ERR;
}

/* TODO.  For now, we just queue a single put, but soon this will be a multipart upload. */
static int s_s3_meta_request_setup_parallel_put(struct aws_s3_meta_request *meta_request) {

    struct aws_s3_request_options request_options = {.message = meta_request->initial_request_message,
                                                     .user_data = meta_request,
                                                     .body_callback = s_s3_meta_request_object_body_callback,
                                                     .finish_callback =
                                                         s_s3_meta_request_queued_request_finished_callback};

    struct aws_s3_request *request = aws_s3_put_object_request_new(meta_request->allocator, &request_options);

    return s_s3_meta_request_push_request(meta_request, request, S3_PUSH_REQUEST_FLAG_LAST_REQUEST);
}

static int s_s3_meta_request_object_body_callback(
    struct aws_s3_request *request,
    struct aws_http_stream *stream,
    const struct aws_byte_cursor *body,
    void *user_data) {

    (void)request;

    struct aws_s3_meta_request *meta_request = user_data;

    /* Pass back the body.  TODO we need to pass back the range here as well.*/
    if (meta_request->body_callback != NULL) {
        return meta_request->body_callback(meta_request, stream, body, user_data);
    }

    return AWS_OP_SUCCESS;
}

static void s_s3_meta_request_queued_request_finished_callback(
    struct aws_s3_request *request,
    int error_code,
    void *user_data) {

    struct aws_s3_meta_request *meta_request = user_data;

    if (error_code != AWS_ERROR_SUCCESS) {
        /* TODO right now we just blindly retry the request, but we'll add more elegant logic here soon. */
        aws_s3_meta_request_retry_request(meta_request, request);
    } else {
        aws_mutex_lock(&meta_request->lock);
        --meta_request->outstanding_requests;
        aws_mutex_unlock(&meta_request->lock);

        s_s3_meta_request_check_for_completion(meta_request, &meta_request->lock);
    }
}
