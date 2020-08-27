/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_client_impl.h"
#include "aws/s3/private/s3_meta_request_impl.h"
#include "aws/s3/private/s3_request.h"
#include "aws/s3/private/s3_util.h"
#include <aws/common/string.h>
#include <aws/io/event_loop.h>
#include <aws/io/stream.h>
#include <inttypes.h>

static void s_s3_meta_request_lock_synced_data(struct aws_s3_meta_request *meta_request);
static void s_s3_meta_request_unlock_synced_data(struct aws_s3_meta_request *meta_request);

static void s_s3_meta_request_check_for_completion_synced(struct aws_s3_meta_request *meta_request);
static void s_s3_meta_request_finish_synced(struct aws_s3_meta_request *meta_request, int error_code);

static void s_s3_meta_request_write_to_caller_task(struct aws_task *task, void *arg, enum aws_task_status task_status);

static void s_s3_meta_request_try_to_clean_up(struct aws_s3_meta_request *meta_request);

/* Auto Ranged Get Functions */
static int s_s3_meta_request_setup_parallel_get(struct aws_s3_meta_request *meta_request);

int s_s3_meta_request_generate_ranged_gets(
    struct aws_s3_meta_request *meta_request,
    uint64_t range_start,
    uint64_t range_end,
    uint32_t flags);
/* */

/* Auto Ranged Put Functions */
static int s_s3_meta_request_setup_parallel_put(struct aws_s3_meta_request *meta_request);
/* */

static void s_s3_meta_request_clean_up_request_list(
    struct aws_s3_meta_request *meta_request,
    struct aws_linked_list *request_list);

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

    meta_request->client = client;
    meta_request->part_size = client->part_size;

    meta_request->initial_request_message = options->message;
    meta_request->synced_data.initial_body_stream = aws_http_message_get_body_stream(options->message);

    aws_http_message_acquire(options->message);

    aws_mutex_init(&meta_request->synced_data.lock);

    aws_linked_list_init(&meta_request->synced_data.pending_request_queue);
    aws_linked_list_init(&meta_request->synced_data.finished_requests);
    aws_linked_list_init(&meta_request->synced_data.write_queue);

    meta_request->user_data = options->user_data;
    meta_request->body_callback = options->body_callback;
    meta_request->finish_callback = options->finish_callback;
    meta_request->shutdown_callback = options->shutdown_callback;

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

int aws_s3_meta_requests_get_total_object_size(
    struct aws_s3_meta_request *meta_request,
    int64_t *out_total_object_size) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(meta_request->initial_request_message);
    AWS_PRECONDITION(out_total_object_size);

    struct aws_input_stream *request_body = aws_http_message_get_body_stream(meta_request->initial_request_message);

    if (request_body == NULL) {
        return 0;
    }

    return aws_input_stream_get_length(request_body, out_total_object_size);
}

int aws_s3_meta_request_set_upload_id(
    struct aws_s3_meta_request *meta_request,
    struct aws_byte_cursor *upload_id_cursor) {
    AWS_PRECONDITION(meta_request);

    s_s3_meta_request_lock_synced_data(meta_request);
    if (meta_request->upload_id == NULL) {
        aws_string_destroy(meta_request->upload_id);
        meta_request->upload_id = NULL;
    }

    if (upload_id_cursor != NULL) {
        meta_request->upload_id = aws_string_new_from_cursor(meta_request->allocator, upload_id_cursor);

        if (meta_request->upload_id == NULL) {
            s_s3_meta_request_unlock_synced_data(meta_request);
            return AWS_OP_ERR;
        }
    }
    s_s3_meta_request_unlock_synced_data(meta_request);

    return AWS_OP_SUCCESS;
}

struct aws_string *aws_s3_meta_request_get_upload_id(struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);

    struct aws_string *string_copy = NULL;
    s_s3_meta_request_lock_synced_data(meta_request);
    string_copy = aws_string_new_from_string(meta_request->allocator, meta_request->upload_id);
    s_s3_meta_request_unlock_synced_data(meta_request);
    return string_copy;
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

    s_s3_meta_request_lock_synced_data(meta_request);
    meta_request->synced_data.cleaning_up = true;
    s_s3_meta_request_unlock_synced_data(meta_request);

    s_s3_meta_request_try_to_clean_up(meta_request);
}

static void s_s3_meta_request_try_to_clean_up(struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);

    s_s3_meta_request_lock_synced_data(meta_request);

    if (!meta_request->synced_data.cleaning_up || meta_request->synced_data.in_flight_requests > 0 ||
        meta_request->synced_data.is_writing_to_caller) {
        s_s3_meta_request_unlock_synced_data(meta_request);
        return;
    }

    s_s3_meta_request_unlock_synced_data(meta_request);

    s_s3_meta_request_clean_up_request_list(meta_request, &meta_request->synced_data.pending_request_queue);
    s_s3_meta_request_clean_up_request_list(meta_request, &meta_request->synced_data.finished_requests);

    /* Clean up our initial http message */
    if (meta_request->initial_request_message != NULL) {
        aws_http_message_release(meta_request->initial_request_message);
        meta_request->initial_request_message = NULL;
    }

    void *user_data = meta_request->user_data;
    aws_s3_meta_request_shutdown_fn *shutdown_callback = meta_request->shutdown_callback;

    aws_mutex_clean_up(&meta_request->synced_data.lock);
    aws_mem_release(meta_request->allocator, meta_request);

    if (shutdown_callback != NULL) {
        shutdown_callback(user_data);
    }
}

/* Try to pop a request into the give pipeline. */
struct aws_s3_request *aws_s3_meta_request_pop_request(struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);

    struct aws_s3_request *request = NULL;
    s_s3_meta_request_lock_synced_data(meta_request);

    if (meta_request->synced_data.cleaning_up) {
        s_s3_meta_request_unlock_synced_data(meta_request);
        return NULL;
    }

    /* Pop something from the front of the queue if there is stuff in the queue. */
    if (!aws_linked_list_empty(&meta_request->synced_data.pending_request_queue)) {
        struct aws_linked_list_node *next = aws_linked_list_pop_front(&meta_request->synced_data.pending_request_queue);
        request = AWS_CONTAINER_OF(next, struct aws_s3_request, node);
    }

    if (request == NULL) {
        s_s3_meta_request_unlock_synced_data(meta_request);
        return NULL;
    }

    bool request_must_be_last = (aws_s3_request_get_type_flags(request) & AWS_S3_REQUEST_TYPE_FLAG_MUST_BE_LAST) != 0;

    if (request_must_be_last && (!aws_linked_list_empty(&meta_request->synced_data.pending_request_queue) ||
                                 meta_request->synced_data.in_flight_requests > 0)) {

        aws_linked_list_push_back(&meta_request->synced_data.pending_request_queue, &request->node);
        s_s3_meta_request_unlock_synced_data(meta_request);
        return NULL;
    }

    request->meta_request = meta_request;
    aws_s3_meta_request_acquire(meta_request);

    ++meta_request->synced_data.in_flight_requests;

    s_s3_meta_request_unlock_synced_data(meta_request);

    return request;
}

void aws_s3_meta_request_finish_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    int error_code) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(request);

    s_s3_meta_request_lock_synced_data(meta_request);

    --meta_request->synced_data.in_flight_requests;

    if (request->meta_request != NULL) {
        aws_s3_meta_request_release(request->meta_request);
        request->meta_request = NULL;
    }

    if (meta_request->synced_data.cleaning_up) {
        aws_linked_list_push_back(&meta_request->synced_data.finished_requests, &request->node);
        s_s3_meta_request_unlock_synced_data(meta_request);

        s_s3_meta_request_try_to_clean_up(meta_request);
        return;
    }

    if (error_code == AWS_ERROR_SUCCESS) {
        aws_linked_list_push_back(&meta_request->synced_data.finished_requests, &request->node);
        s_s3_meta_request_check_for_completion_synced(meta_request);
    } else {

        aws_linked_list_push_front(&meta_request->synced_data.pending_request_queue, &request->node);

        if (error_code != AWS_ERROR_S3_INTERNAL_ERROR) {
            /* According to S3 Error Best Practices, internal errors do not represent a mistake on the part of the
             * client and should be retried. https://docs.aws.amazon.com/AmazonS3/latest/dev/ErrorBestPractices.html  */
            s_s3_meta_request_finish_synced(meta_request, error_code);
        }
    }

    s_s3_meta_request_unlock_synced_data(meta_request);
}

void aws_s3_meta_request_write_part_buffer_to_caller(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_part_buffer *part_buffer) {
    AWS_PRECONDITION(meta_request);

    s_s3_meta_request_lock_synced_data(meta_request);
    aws_linked_list_push_back(&meta_request->synced_data.write_queue, &part_buffer->node);

    if (meta_request->synced_data.is_writing_to_caller) {
        s_s3_meta_request_unlock_synced_data(meta_request);
        return;
    }

    meta_request->synced_data.is_writing_to_caller = true;
    s_s3_meta_request_unlock_synced_data(meta_request);

    aws_task_init(
        &meta_request->synced_data.write_to_caller_task,
        s_s3_meta_request_write_to_caller_task,
        meta_request,
        "s3_meta_request_write_to_stream");

    aws_event_loop_schedule_task_now(meta_request->client->event_loop, &meta_request->synced_data.write_to_caller_task);
}

static void s_s3_meta_request_write_to_caller_task(struct aws_task *task, void *arg, enum aws_task_status task_status) {
    AWS_PRECONDITION(task);
    AWS_PRECONDITION(arg);
    (void)task;

    if (task_status != AWS_TASK_STATUS_RUN_READY) {
        return;
    }

    struct aws_s3_meta_request *meta_request = arg;
    struct aws_s3_part_buffer *part_buffer = NULL;

    s_s3_meta_request_lock_synced_data(meta_request);

    if (aws_linked_list_empty(&meta_request->synced_data.write_queue) || meta_request->synced_data.cleaning_up) {
        meta_request->synced_data.is_writing_to_caller = false;
        s_s3_meta_request_unlock_synced_data(meta_request);

        s_s3_meta_request_try_to_clean_up(meta_request);
        return;
    } else {
        struct aws_linked_list_node *part_buffer_node =
            aws_linked_list_pop_front(&meta_request->synced_data.write_queue);

        part_buffer = AWS_CONTAINER_OF(part_buffer_node, struct aws_s3_part_buffer, node);

        AWS_FATAL_ASSERT(part_buffer != NULL);
    }

    s_s3_meta_request_unlock_synced_data(meta_request);

    if (meta_request->body_callback != NULL) {
        struct aws_byte_cursor buf_byte_cursor = aws_byte_cursor_from_buf(&part_buffer->buffer);

        meta_request->body_callback(
            meta_request, &buf_byte_cursor, part_buffer->range_start, part_buffer->range_end, meta_request->user_data);
    }

    aws_s3_client_release_part_buffer(meta_request->client, part_buffer);
    part_buffer = NULL;

    s_s3_meta_request_lock_synced_data(meta_request);

    aws_event_loop_schedule_task_now(meta_request->client->event_loop, &meta_request->synced_data.write_to_caller_task);

    s_s3_meta_request_unlock_synced_data(meta_request);
}

/* Finishes the meta request if there is no work remaining. */
static void s_s3_meta_request_check_for_completion_synced(struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);

    ASSERT_SYNCED_DATA_LOCK_HELD(meta_request);

    if (meta_request->synced_data.queue_finished_populating &&
        aws_linked_list_empty(&meta_request->synced_data.pending_request_queue) &&
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
        AWS_LS_S3_META_REQUEST,
        "id=%p Meta request finished with error code %d (%s)",
        (void *)meta_request,
        error_code,
        aws_error_str(error_code));

    meta_request->synced_data.queue_finished_populating = true;

    // TODO we should trigger these callbacks outside of the mutex to be safe.
    if (meta_request->finish_callback != NULL) {
        meta_request->finish_callback(meta_request, error_code, meta_request->user_data);
    }

    if (meta_request->internal_finish_callback != NULL) {
        meta_request->internal_finish_callback(meta_request, error_code, meta_request->internal_user_data);
    }
}

/* Push a single request into the meta request. Should not be used for many request adds. */
int aws_s3_meta_request_push_new_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request_options *request_options,
    uint32_t flags) {
    AWS_PRECONDITION(meta_request);

    struct aws_s3_request *request = aws_s3_request_new(meta_request, request_options);

    if (request == NULL) {
        goto error_result;
    }

    s_s3_meta_request_lock_synced_data(meta_request);

    if (meta_request->synced_data.queue_finished_populating) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST,
            "id=%p Queue is already finished populating, can't push another request.",
            (void *)meta_request);
        s_s3_meta_request_unlock_synced_data(meta_request);
        goto error_result;
    }

    if ((flags & S3_PUSH_NEW_REQUEST_FLAG_QUEUING_DONE) != 0) {
        meta_request->synced_data.queue_finished_populating = true;
    }

    aws_linked_list_push_back(&meta_request->synced_data.pending_request_queue, &request->node);
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

    /* TODO If we already have a ranged header, eventually we can break the range up into parts too.  However, this
     * requires additional parsing of this header value, so for now, we just send the message. */
    if (already_has_range_header) {

        struct aws_s3_request_options request_options = {.request_type = AWS_S3_REQUEST_TYPE_GET_OBJECT};

        if (aws_s3_meta_request_push_new_request(
                meta_request, &request_options, S3_PUSH_NEW_REQUEST_FLAG_QUEUING_DONE)) {
            goto error_result;
        }

    } else {
        /* We initially queue just one ranged get that is the size of a single part.  The headers from this first get
         * will tell us the size of the object, and we can spin up additional gets if necessary. */
        struct aws_s3_request_options request_options = {.request_type = AWS_S3_REQUEST_TYPE_SEED_GET_OBJECT,
                                                         .part_number = 1};

        if (aws_s3_meta_request_push_new_request(meta_request, &request_options, 0)) {
            goto error_result;
        }
    }

    return AWS_OP_SUCCESS;

error_result:
    return AWS_OP_ERR;
}

static int s_s3_meta_request_setup_parallel_put(struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);

    int64_t request_body_length = 0;

    if (aws_s3_meta_requests_get_total_object_size(meta_request, &request_body_length)) {
        return AWS_OP_ERR;
    }

    if (request_body_length < (int64_t)meta_request->part_size) {
        struct aws_s3_request_options request_options = {.request_type = AWS_S3_REQUEST_TYPE_PUT_OBJECT};

        return aws_s3_meta_request_push_new_request(
            meta_request, &request_options, S3_PUSH_NEW_REQUEST_FLAG_QUEUING_DONE);
    } else {
        struct aws_s3_request_options request_options = {.request_type = AWS_S3_REQUEST_TYPE_CREATE_MULTIPART_UPLOAD};

        return aws_s3_meta_request_push_new_request(meta_request, &request_options, 0);
    }
}

/* Allocate/queue ranged gets given the range and our part size. */
int aws_s3_meta_request_generate_ranged_requests(
    struct aws_s3_meta_request *meta_request,
    enum aws_s3_request_type request_type,
    uint64_t range_start,
    uint64_t range_end,
    uint32_t flags) {

    bool skip_first_part = (flags & AWS_S3_META_REQUEST_GEN_RANGED_FLAG_SKIP_FIRST) != 0;
    bool set_finish_populating = (flags & AWS_S3_META_REQUEST_GEN_RANGED_FLAG_QUEUING_DONE) != 0;

    struct aws_linked_list stack_request_list;
    aws_linked_list_init(&stack_request_list);

    uint64_t total_size = range_end - range_start + 1;
    uint64_t num_parts = total_size / meta_request->part_size;

    if ((total_size % meta_request->part_size) > 0) {
        ++num_parts;
    }

    /* Allocate requests for each individual ranged get or "part". */
    /* TODO may be able to allocate these in bulk instead of n amount of allocations. */
    for (uint64_t part_index = 0; part_index < num_parts; ++part_index) {

        if (part_index == 0 && skip_first_part) {
            continue;
        }

        struct aws_s3_request_options request_options = {.request_type = request_type, .part_number = part_index + 1};

        struct aws_s3_request *request = aws_s3_request_new(meta_request, &request_options);

        if (request == NULL) {
            AWS_LOGF_ERROR(
                AWS_LS_S3_META_REQUEST,
                "id=%p: Could not allocate new ranged get request for meta request.",
                (void *)meta_request);

            goto error_clean_up;
        }

        aws_linked_list_push_back(&stack_request_list, &request->node);
    }

    s_s3_meta_request_lock_synced_data(meta_request);

    while (!aws_linked_list_empty(&stack_request_list)) {
        struct aws_linked_list_node *front_node = aws_linked_list_pop_front(&stack_request_list);
        struct aws_s3_request *request = AWS_CONTAINER_OF(front_node, struct aws_s3_request, node);
        aws_linked_list_push_back(&meta_request->synced_data.pending_request_queue, &request->node);
    }

    s_s3_meta_request_clean_up_request_list(meta_request, &stack_request_list);

    if (set_finish_populating) {
        /* No additional requests will need to be allocated. */
        meta_request->synced_data.queue_finished_populating = true;
        s_s3_meta_request_check_for_completion_synced(meta_request);
    }

    s_s3_meta_request_unlock_synced_data(meta_request);

    return AWS_OP_SUCCESS;

error_clean_up:

    s_s3_meta_request_clean_up_request_list(meta_request, &stack_request_list);

    return AWS_OP_ERR;
}

static void s_s3_meta_request_clean_up_request_list(
    struct aws_s3_meta_request *meta_request,
    struct aws_linked_list *request_list) {
    /* Dump anthing still in our queue*/
    while (!aws_linked_list_empty(request_list)) {
        struct aws_linked_list_node *next = aws_linked_list_pop_front(request_list);
        struct aws_s3_request *request = AWS_CONTAINER_OF(next, struct aws_s3_request, node);
        aws_s3_request_destroy(meta_request->allocator, request);
    }
}

void aws_s3_meta_request_iterate_finished_requests(
    struct aws_s3_meta_request *meta_request,
    aws_s3_iterate_finished_requests_callback_fn *callback,
    void *user_data) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(callback);

    s_s3_meta_request_lock_synced_data(meta_request);

    if (aws_linked_list_empty(&meta_request->synced_data.finished_requests)) {
        s_s3_meta_request_unlock_synced_data(meta_request);
        return;
    }

    struct aws_linked_list_node *current_node = aws_linked_list_begin(&meta_request->synced_data.finished_requests);

    while (current_node != aws_linked_list_end(&meta_request->synced_data.finished_requests)) {
        struct aws_s3_request *request = AWS_CONTAINER_OF(current_node, struct aws_s3_request, node);
        callback(request, user_data);
        current_node = aws_linked_list_next(current_node);
    }

    s_s3_meta_request_unlock_synced_data(meta_request);
}

int aws_s3_meta_request_copy_part_to_part_buffer(
    struct aws_s3_meta_request *meta_request,
    uint32_t part_number,
    struct aws_s3_part_buffer *dest_part_buffer) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(dest_part_buffer);

    s_s3_meta_request_lock_synced_data(meta_request);

    uint64_t range_start = 0;

    if (part_number > 0) {
        range_start = (part_number - 1) * meta_request->part_size;
    }

    struct aws_input_stream *initial_body_stream = meta_request->synced_data.initial_body_stream;

    if (initial_body_stream == NULL) {
        s_s3_meta_request_unlock_synced_data(meta_request);
        return AWS_OP_ERR;
    }

    struct aws_byte_cursor buffer_byte_cursor = aws_byte_cursor_from_buf(&dest_part_buffer->buffer);

    AWS_FATAL_ASSERT(buffer_byte_cursor.len <= meta_request->part_size);

    if (aws_input_stream_seek(initial_body_stream, range_start, AWS_SSB_BEGIN)) {
        s_s3_meta_request_unlock_synced_data(meta_request);
        return AWS_OP_ERR;
    }

    if (aws_input_stream_read(initial_body_stream, &dest_part_buffer->buffer)) {
        s_s3_meta_request_unlock_synced_data(meta_request);
        return AWS_OP_ERR;
    }

    s_s3_meta_request_unlock_synced_data(meta_request);

    return AWS_OP_SUCCESS;
}
