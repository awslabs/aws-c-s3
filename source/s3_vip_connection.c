/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_vip_connection.h"
#include "aws/s3/private/s3_client_impl.h"
#include "aws/s3/private/s3_meta_request_impl.h"
#include "aws/s3/private/s3_request.h"
#include "aws/s3/private/s3_request_pipeline.h"
#include "aws/s3/private/s3_util.h"
#include "aws/s3/private/s3_work_util.h"

#include <aws/common/clock.h>
#include <aws/common/mutex.h>
#include <aws/common/string.h>

#include <aws/http/connection.h>
#include <aws/http/connection_manager.h>

/* State machine states for the vip connection. */
enum aws_s3_vip_connection_state {
    AWS_S3_VIP_CONNECTION_STATE_ALIVE,

    AWS_S3_VIP_CONNECTION_STATE_CLEAN_UP,
    AWS_S3_VIP_CONNECTION_STATE_CLEAN_UP_WAIT_FOR_IDLE,
    AWS_S3_VIP_CONNECTION_STATE_CLEAN_UP_IDLE_FINISHED,
    AWS_S3_VIP_CONNECTION_STATE_CLEAN_UP_TASK_UTIL,
    AWS_S3_VIP_CONNECTION_STATE_CLEAN_UP_TASK_UTIL_FINISHED,
    AWS_S3_VIP_CONNECTION_STATE_CLEAN_UP_FINISH
};

/* Processing state of a VIP connection, ie, if it is currently not processing meta requests, or it is processing meta
 * requests.*/
enum aws_s3_vip_connection_processing_state {
    AWS_S3_VIP_CONNECTION_PROCESSING_STATE_IDLE,
    AWS_S3_VIP_CONNECTION_PROCESSING_STATE_ACTIVE
};

struct aws_s3_task_util;

struct aws_s3_vip_connection {
    struct aws_allocator *allocator;
    struct aws_atomic_var ref_count;

    /* Used to group this VIP connection with other VIP connections belonging to the same VIP. */
    void *vip_identifier;

    /* Used to simplify task set up and shutdown. */
    struct aws_s3_task_util *task_util;

    /* The request pipeline that this VIP connection will use to process requests. */
    struct aws_s3_request_pipeline *request_pipeline;

    /* Called when this connection is done cleaning up. */
    aws_s3_vip_connection_shutdown_complete_callback_fn *shutdown_callback;
    void *user_data;

    struct {
        struct aws_mutex lock;

        /* Current place in the VIP connection's state machine. */
        enum aws_s3_vip_connection_state state;

        /* Current processing state (doing work or not doing work) for the VIP connection. */
        enum aws_s3_vip_connection_processing_state processing_state;

        /* Local list of meta requests.  Changes to this list are all pushed by the owning client. */
        struct aws_array_list meta_requests;

        /* List of meta request removals that arrived before their meta-request-push happened due to task ordering.
         * Requires Lock */
        struct aws_array_list meta_request_ooo_removals;

        /* Next meta request to be used.  We try to keep this up always pointing to the next meta request, even when
         * meta requests are removed/added, so that mutations of the meta request list do not cause any unintentional
         * favoring of certain files.  (Might be overkill.)*/
        size_t next_meta_request_index;

    } synced_data;
};

static const size_t s_meta_request_list_initial_capacity = 16;
static const size_t s_out_of_order_removal_list_initial_capacity = 16;
static const uint64_t s_vip_connection_processing_retry_offset_ms = 50;

static void s_s3_vip_connection_lock_synced_data(struct aws_s3_vip_connection *vip_connection);
static void s_s3_vip_connection_unlock_synced_data(struct aws_s3_vip_connection *vip_connection);

/* Initiates a task for starting VIP connection destruction. */
static void s_s3_vip_connection_destroy(struct aws_s3_vip_connection *vip_connection);

/* Task for actually starting the VIP connection destruction. */
static void s_s3_vip_connection_destroy_task(uint32_t num_args, void **args);

/* Finds the index of a meta request in an array list of meta requests. */
static size_t s_find_meta_request_index(
    const struct aws_array_list *meta_requests,
    const struct aws_s3_meta_request *meta_request);

/* Checks to see if the VIP connection is in a clean up state. */
static bool s_s3_vip_connection_is_cleaning_up_synced(struct aws_s3_vip_connection *vip_connection);

/* Task function for pushing a meta request. */
static void s_s3_vip_connection_push_meta_request_task(uint32_t num_args, void **args);

/* Task funciton for removing a meta request. */
static void s_s3_vip_connection_remove_meta_request_task(uint32_t num_args, void **args);

/* Updates the actual processing state. Calling any necessary callbacks. */
static void s_s3_vip_connection_set_processing_state_synced(
    struct aws_s3_vip_connection *vip_connection,
    enum aws_s3_vip_connection_processing_state processing_state);

/* Asynchronously try find a request that can be processed and passes it thorugh the pipeline. */
static int s_s3_vip_connection_process_meta_requests(struct aws_s3_vip_connection *vip_connection);

/* Task function for trying find a request that can be processed and passes it thorugh the pipeline. */
static void s_s3_vip_connection_process_meta_requests_task(uint32_t num_args, void **args);

/* Callback for when the request pipeline has finished executing, which is when we can process an additional request. */
static void s_s3_vip_connection_pipeline_finished(
    struct aws_s3_request_pipeline *pipeline,
    int error_code,
    void *user_data);

/* State machine for the VIP connection.  Mainly used to manage clean up states.*/
static void s_s3_vip_connection_set_state_synced(
    struct aws_s3_vip_connection *vip_connection,
    enum aws_s3_vip_connection_state state);

/* Triggered when the VIP connection's work controller shuts down. */
static void s_s3_task_util_shutdown_callback(void *user_data);

static void s_s3_vip_connection_lock_synced_data(struct aws_s3_vip_connection *vip_connection) {
    aws_mutex_lock(&vip_connection->synced_data.lock);
}

static void s_s3_vip_connection_unlock_synced_data(struct aws_s3_vip_connection *vip_connection) {
    aws_mutex_unlock(&vip_connection->synced_data.lock);
}

struct aws_s3_vip_connection *aws_s3_vip_connection_new(
    struct aws_allocator *allocator,
    const struct aws_s3_vip_connection_options *options) {
    AWS_PRECONDITION(options);
    AWS_PRECONDITION(options->allocator);
    AWS_PRECONDITION(options->event_loop);
    AWS_PRECONDITION(options->credentials_provider);
    AWS_PRECONDITION(options->http_connection_manager);

    struct aws_s3_vip_connection *vip_connection = aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_vip_connection));

    if (vip_connection == NULL) {
        AWS_LOGF_ERROR(AWS_LS_S3_VIP_CONNECTION, "Could not allocate new aws_s3_vip_connection.");
        return NULL;
    }

    /* Initialize ref count */
    aws_atomic_store_int(&vip_connection->ref_count, 0);
    aws_s3_vip_connection_acquire(vip_connection);

    /* Copy over any options relevant for copy */
    vip_connection->allocator = options->allocator;

    struct aws_s3_task_util_options task_util_options = {.allocator = vip_connection->allocator,
                                                         .event_loop = options->event_loop,
                                                         .shutdown_callback = s_s3_task_util_shutdown_callback,
                                                         .shutdown_user_data = vip_connection};

    /* Initialize our work controller so that we can do async actions */
    vip_connection->task_util = aws_s3_task_util_new(allocator, &task_util_options);

    struct aws_s3_request_pipeline_options request_pipeline_options = {
        .credentials_provider = options->credentials_provider,
        .http_connection_manager = options->http_connection_manager,
        .region = options->region,
        .listener = {.exec_cleaned_up_callback = s_s3_vip_connection_pipeline_finished, .user_data = vip_connection}};

    vip_connection->request_pipeline = aws_s3_request_pipeline_new(allocator, &request_pipeline_options);

    aws_mutex_init(&vip_connection->synced_data.lock);

    /* Set up our meta request list. */
    if (aws_array_list_init_dynamic(
            &vip_connection->synced_data.meta_requests,
            options->allocator,
            s_meta_request_list_initial_capacity,
            sizeof(struct aws_s3_meta_request *))) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_VIP_CONNECTION,
            "id=%p Could not allocate meta requests list for aws_s3_vip_connection.",
            (void *)vip_connection);
        goto failed_alloc_meta_request_list;
    }

    /* Set up our meta request out-of-order removal list. */
    if (aws_array_list_init_dynamic(
            &vip_connection->synced_data.meta_request_ooo_removals,
            options->allocator,
            s_out_of_order_removal_list_initial_capacity,
            sizeof(struct aws_s3_meta_request *))) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_VIP_CONNECTION,
            "id=%p Could not allocate out-of-order removals list for aws_s3_vip_connection.",
            (void *)vip_connection);
        goto failed_alloc_removals_list;
    }

    vip_connection->vip_identifier = options->vip_identifier;
    vip_connection->shutdown_callback = options->shutdown_callback;
    vip_connection->user_data = options->user_data;

    return vip_connection;

failed_alloc_removals_list:
    aws_array_list_clean_up(&vip_connection->synced_data.meta_requests);

failed_alloc_meta_request_list:

    aws_mem_release(allocator, vip_connection);

    return NULL;
}

void aws_s3_vip_connection_acquire(struct aws_s3_vip_connection *vip_connection) {
    AWS_PRECONDITION(vip_connection);
    aws_atomic_fetch_add(&vip_connection->ref_count, 1);
}

void aws_s3_vip_connection_release(struct aws_s3_vip_connection *vip_connection) {
    AWS_PRECONDITION(vip_connection);

    size_t prev_ref_count = aws_atomic_fetch_sub(&vip_connection->ref_count, 1);

    if (prev_ref_count > 1) {
        return;
    }

    s_s3_vip_connection_destroy(vip_connection);
}

/* Initiates a task for starting VIP connection destruction. */
static void s_s3_vip_connection_destroy(struct aws_s3_vip_connection *vip_connection) {
    AWS_PRECONDITION(vip_connection);

    if (aws_s3_task_util_create_task(
            vip_connection->task_util, s_s3_vip_connection_destroy_task, 0, 1, vip_connection)) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_VIP_CONNECTION,
            "id=%p Could not initiate clean up for s3 vip connection.",
            (void *)vip_connection);
    }
}

/* Task for actually starting the VIP connection destruction. */
static void s_s3_vip_connection_destroy_task(uint32_t num_args, void **args) {
    AWS_PRECONDITION(num_args == 1);
    (void)num_args;

    struct aws_s3_vip_connection *vip_connection = args[0];
    AWS_PRECONDITION(vip_connection);

    s_s3_vip_connection_lock_synced_data(vip_connection);
    s_s3_vip_connection_set_state_synced(vip_connection, AWS_S3_VIP_CONNECTION_STATE_CLEAN_UP);
    s_s3_vip_connection_unlock_synced_data(vip_connection);
}

/* Access VIP identifier for this VIP connection. */
void *aws_s3_vip_connection_get_vip_identifier(struct aws_s3_vip_connection *vip_connection) {
    return vip_connection->vip_identifier;
}

/* Finds the index of a meta request in an array list of meta requests. */
static size_t s_find_meta_request_index(
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

/* Checks to see if the VIP connection is in a clean up state. */
static bool s_s3_vip_connection_is_cleaning_up_synced(struct aws_s3_vip_connection *vip_connection) {
    ASSERT_SYNCED_DATA_LOCK_HELD(vip_connection);

    return vip_connection->synced_data.state >= AWS_S3_VIP_CONNECTION_STATE_CLEAN_UP &&
           vip_connection->synced_data.state <= AWS_S3_VIP_CONNECTION_STATE_CLEAN_UP_FINISH;
}

/* Asynchronously push a meta request onto this VIP connection. */
int aws_s3_vip_connection_push_meta_request(
    struct aws_s3_vip_connection *vip_connection,
    struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(vip_connection);
    AWS_PRECONDITION(meta_request);

    AWS_LOGF_INFO(
        AWS_LS_S3_VIP_CONNECTION,
        "id=%p Pushing meta request %p into VIP connection.",
        (void *)vip_connection,
        (void *)meta_request);

    aws_s3_meta_request_acquire(meta_request);

    return aws_s3_task_util_create_task(
        vip_connection->task_util, s_s3_vip_connection_push_meta_request_task, 0, 2, vip_connection, meta_request);
}

/* Task function for pushing a meta request. */
static void s_s3_vip_connection_push_meta_request_task(uint32_t num_args, void **args) {
    AWS_PRECONDITION(num_args == 2);
    (void)num_args;

    struct aws_s3_vip_connection *vip_connection = args[0];
    struct aws_s3_meta_request *meta_request = args[1];

    AWS_PRECONDITION(vip_connection);
    AWS_PRECONDITION(meta_request);

    s_s3_vip_connection_lock_synced_data(vip_connection);

    size_t meta_request_index =
        s_find_meta_request_index(&vip_connection->synced_data.meta_request_ooo_removals, meta_request);

    /* If we found this in our out of order removal list, we got a "remove" for this already (before it was even
     * added) due to out of order task evaluation.*/
    /* TODO (size_t)-1 should be a constant. */
    if (meta_request_index != (size_t)-1) {
        aws_array_list_erase(&vip_connection->synced_data.meta_request_ooo_removals, meta_request_index);
        aws_s3_meta_request_release(meta_request);
    } else {
        /* Grab a reference to the meta request and push it into our list. */
        aws_s3_meta_request_acquire(meta_request);
        aws_array_list_push_back(&vip_connection->synced_data.meta_requests, &meta_request);

        /* Make sure that we're processing meta requests. */
        s_s3_vip_connection_set_processing_state_synced(vip_connection, AWS_S3_VIP_CONNECTION_PROCESSING_STATE_ACTIVE);
    }

    s_s3_vip_connection_unlock_synced_data(vip_connection);

    if (meta_request != NULL) {
        aws_s3_meta_request_release(meta_request);
    }
}

/* Asynchronously push a meta request onto this VIP connection. */
int aws_s3_vip_connection_remove_meta_request(
    struct aws_s3_vip_connection *vip_connection,
    struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(vip_connection);
    AWS_PRECONDITION(meta_request);

    AWS_LOGF_INFO(
        AWS_LS_S3_VIP_CONNECTION,
        "id=%p Removing meta request %p from VIP connection.",
        (void *)vip_connection,
        (void *)meta_request);

    aws_s3_meta_request_acquire(meta_request);

    return aws_s3_task_util_create_task(
        vip_connection->task_util, s_s3_vip_connection_remove_meta_request_task, 0, 2, vip_connection, meta_request);
}

/* Task funciton for removing a meta request. */
static void s_s3_vip_connection_remove_meta_request_task(uint32_t num_args, void **args) {
    AWS_PRECONDITION(num_args == 2);
    (void)num_args;

    struct aws_s3_vip_connection *vip_connection = args[0];
    struct aws_s3_meta_request *meta_request = args[1];

    AWS_PRECONDITION(vip_connection);
    AWS_PRECONDITION(meta_request);

    s_s3_vip_connection_lock_synced_data(vip_connection);

    size_t meta_request_index = s_find_meta_request_index(&vip_connection->synced_data.meta_requests, meta_request);

    /* If it wasn't found, then this should mean that we received a remove before an add.  In that case, put it
    into our out-of-order list for the future. Otherwise, we can go
    ahead and remove it from our normal list. */
    /* TODO try to find a better way of verifying this is out-of-order or of preventing this. */
    if (meta_request_index == (size_t)-1) {
        aws_s3_meta_request_acquire(meta_request);
        aws_array_list_push_back(
            &vip_connection->synced_data.meta_request_ooo_removals, meta_request); // TODO handle error
    } else {

        /* Update our next meta request index if needed. This is just to help keep processing order in tact, but
         * might be unnecessary/overkill. */
        size_t *next_meta_request_index = &vip_connection->synced_data.next_meta_request_index;

        if (meta_request_index < *next_meta_request_index) {
            --(*next_meta_request_index);
        } else if (meta_request_index > *next_meta_request_index) {
            *next_meta_request_index =
                *next_meta_request_index % aws_array_list_length(&vip_connection->synced_data.meta_requests);
        }

        aws_array_list_erase(&vip_connection->synced_data.meta_requests, meta_request_index);
        aws_s3_meta_request_release(meta_request);
    }

    s_s3_vip_connection_unlock_synced_data(vip_connection);

    aws_s3_meta_request_release(meta_request);
}

/* Updates the actual processing state. Calling any necessary callbacks. */
static void s_s3_vip_connection_set_processing_state_synced(
    struct aws_s3_vip_connection *vip_connection,
    enum aws_s3_vip_connection_processing_state processing_state) {
    AWS_PRECONDITION(vip_connection);

    ASSERT_SYNCED_DATA_LOCK_HELD(vip_connection);

    if (vip_connection->synced_data.processing_state == processing_state) {
        return;
    }

    vip_connection->synced_data.processing_state = processing_state;

    switch (processing_state) {
        case AWS_S3_VIP_CONNECTION_PROCESSING_STATE_IDLE: {
            /* We're not doing anything right now, so we can reset the next_meta_request_index to just be 0. */
            vip_connection->synced_data.next_meta_request_index = 0;

            /* If clean up is waiting for us to be idle, tell them we're idle. */
            if (vip_connection->synced_data.state == AWS_S3_VIP_CONNECTION_STATE_CLEAN_UP_WAIT_FOR_IDLE) {
                s_s3_vip_connection_set_state_synced(
                    vip_connection, AWS_S3_VIP_CONNECTION_STATE_CLEAN_UP_IDLE_FINISHED);
            }
            break;
        }
        case AWS_S3_VIP_CONNECTION_PROCESSING_STATE_ACTIVE: {
            /* Issue an async action to process requests. */
            if (s_s3_vip_connection_process_meta_requests(vip_connection)) {
                AWS_LOGF_ERROR(
                    AWS_LS_S3_VIP_CONNECTION,
                    "id=%p Could not initiate processing of meta requests.",
                    (void *)vip_connection);
            }
            break;
        }
        default:
            AWS_FATAL_ASSERT(false);
            break;
    }
}

/* Asynchronously try find a request that can be processed and passes it thorugh the pipeline. */
static int s_s3_vip_connection_process_meta_requests(struct aws_s3_vip_connection *vip_connection) {
    return aws_s3_task_util_create_task(
        vip_connection->task_util, s_s3_vip_connection_process_meta_requests_task, 0, 1, vip_connection);
}

/* Task function for trying find a request that can be processed and passes it thorugh the pipeline. */
static void s_s3_vip_connection_process_meta_requests_task(uint32_t num_args, void **args) {
    AWS_PRECONDITION(num_args == 1);
    (void)num_args;

    struct aws_s3_vip_connection *vip_connection = args[0];
    AWS_PRECONDITION(vip_connection);

    s_s3_vip_connection_lock_synced_data(vip_connection);

    size_t num_meta_requests = aws_array_list_length(&vip_connection->synced_data.meta_requests);

    /* If we're trying to clean up, or we don't have anything to do, go back to idle state. */
    if (s_s3_vip_connection_is_cleaning_up_synced(vip_connection) || num_meta_requests == 0) {
        s_s3_vip_connection_set_processing_state_synced(vip_connection, AWS_S3_VIP_CONNECTION_PROCESSING_STATE_IDLE);
        s_s3_vip_connection_unlock_synced_data(vip_connection);
        return;
    }

    size_t next_meta_request_index = vip_connection->synced_data.next_meta_request_index;

    /* Index that is relative to the value of next_meta_request_index.*/
    size_t relative_meta_request_index = 0;

    int found_request = 0;

    for (; relative_meta_request_index < num_meta_requests; ++relative_meta_request_index) {

        /* From our relative index, grab an actual index. */
        size_t meta_request_index = (relative_meta_request_index + next_meta_request_index) % num_meta_requests;

        struct aws_s3_meta_request *meta_request = NULL;
        aws_array_list_get_at(&vip_connection->synced_data.meta_requests, &meta_request, meta_request_index);

        /* Try popping a request from this meta request. */
        aws_s3_meta_request_pop_request(meta_request, vip_connection->request_pipeline, &found_request);

        /* If we successfully got one, then go ahead and calculate a new next_meta_request_index value. */
        if (found_request) {
            next_meta_request_index = (meta_request_index + 1) % num_meta_requests;
            break;
        }
    }

    /* Store our new next_meta_reqest_index value if we have a request, or reset it if we couldn't find anything. */
    if (found_request) {
        vip_connection->synced_data.next_meta_request_index = next_meta_request_index;
    } else {
        vip_connection->synced_data.next_meta_request_index = 0;
    }

    s_s3_vip_connection_unlock_synced_data(vip_connection);

    if (found_request) {

        aws_s3_request_pipeline_execute(vip_connection->request_pipeline);

    } else {
        /* If there isn't an s3 request right now, don't completely shutdown--check back in a little bit to see if there
         * is additional work.*/

        uint64_t time_offset_ns = aws_timestamp_convert(
            s_vip_connection_processing_retry_offset_ms, AWS_TIMESTAMP_MILLIS, AWS_TIMESTAMP_NANOS, NULL);

        aws_s3_task_util_create_task(
            vip_connection->task_util,
            s_s3_vip_connection_process_meta_requests_task,
            time_offset_ns,
            1,
            vip_connection);
    }
}

/* Callback for when the request pipeline has finished executing, which is when we can process an additional request. */
static void s_s3_vip_connection_pipeline_finished(
    struct aws_s3_request_pipeline *pipeline,
    int error_code,
    void *user_data) {
    AWS_PRECONDITION(user_data);
    AWS_PRECONDITION(pipeline);
    (void)pipeline;
    (void)error_code;

    struct aws_s3_vip_connection *vip_connection = user_data;

    s_s3_vip_connection_process_meta_requests_task(1, (void **)&vip_connection);
}

static void s_s3_task_util_shutdown_callback(void *user_data) {
    AWS_PRECONDITION(user_data);

    struct aws_s3_vip_connection *vip_connection = user_data;

    s_s3_vip_connection_lock_synced_data(vip_connection);

    AWS_FATAL_ASSERT(vip_connection->synced_data.state == AWS_S3_VIP_CONNECTION_STATE_CLEAN_UP_TASK_UTIL);
    s_s3_vip_connection_set_state_synced(vip_connection, AWS_S3_VIP_CONNECTION_STATE_CLEAN_UP_TASK_UTIL_FINISHED);

    s_s3_vip_connection_unlock_synced_data(vip_connection);
}

/* State machine for the VIP connection.  Mainly used to manage clean ups tates.*/
static void s_s3_vip_connection_set_state_synced(
    struct aws_s3_vip_connection *vip_connection,
    enum aws_s3_vip_connection_state state) {
    AWS_PRECONDITION(vip_connection);

    ASSERT_SYNCED_DATA_LOCK_HELD(vip_connection);

    if (vip_connection->synced_data.state == state) {
        return;
    }

    vip_connection->synced_data.state = state;

    switch (state) {
        case AWS_S3_VIP_CONNECTION_STATE_ALIVE: {
            break;
        }
        case AWS_S3_VIP_CONNECTION_STATE_CLEAN_UP: {
            s_s3_vip_connection_set_state_synced(vip_connection, AWS_S3_VIP_CONNECTION_STATE_CLEAN_UP_WAIT_FOR_IDLE);
            break;
        }
        case AWS_S3_VIP_CONNECTION_STATE_CLEAN_UP_WAIT_FOR_IDLE: {
            if (vip_connection->synced_data.processing_state == AWS_S3_VIP_CONNECTION_PROCESSING_STATE_IDLE) {
                s_s3_vip_connection_set_state_synced(
                    vip_connection, AWS_S3_VIP_CONNECTION_STATE_CLEAN_UP_IDLE_FINISHED);
            }
            break;
        }
        case AWS_S3_VIP_CONNECTION_STATE_CLEAN_UP_IDLE_FINISHED: {
            s_s3_vip_connection_set_state_synced(vip_connection, AWS_S3_VIP_CONNECTION_STATE_CLEAN_UP_TASK_UTIL);
            break;
        }
        case AWS_S3_VIP_CONNECTION_STATE_CLEAN_UP_TASK_UTIL: {

            if (vip_connection->task_util != NULL) {
                aws_s3_task_util_destroy(vip_connection->task_util);
                vip_connection->task_util = NULL;
            }
            break;
        }
        case AWS_S3_VIP_CONNECTION_STATE_CLEAN_UP_TASK_UTIL_FINISHED: {
            s_s3_vip_connection_set_state_synced(vip_connection, AWS_S3_VIP_CONNECTION_STATE_CLEAN_UP_FINISH);
            break;
        }
        case AWS_S3_VIP_CONNECTION_STATE_CLEAN_UP_FINISH: {

            if (vip_connection->request_pipeline != NULL) {
                aws_s3_request_pipeline_destroy(vip_connection->request_pipeline);
                vip_connection->request_pipeline = NULL;
            }

            aws_mutex_clean_up(&vip_connection->synced_data.lock);

            size_t num_meta_requests = aws_array_list_length(&vip_connection->synced_data.meta_requests);

            /* Dump any meta requests we still have */
            for (size_t meta_request_index = 0; meta_request_index < num_meta_requests; ++meta_request_index) {
                struct aws_s3_meta_request *meta_request = NULL;
                aws_array_list_get_at(&vip_connection->synced_data.meta_requests, &meta_request, meta_request_index);
                aws_s3_meta_request_release(meta_request);
            }

            aws_array_list_clean_up(&vip_connection->synced_data.meta_requests);

            size_t num_ooo_removals = aws_array_list_length(&vip_connection->synced_data.meta_request_ooo_removals);

            /* Dump any out of order meta requests we still have. */
            for (size_t meta_request_index = 0; meta_request_index < num_ooo_removals; ++meta_request_index) {
                struct aws_s3_meta_request *meta_request = NULL;
                aws_array_list_get_at(
                    &vip_connection->synced_data.meta_request_ooo_removals, &meta_request, meta_request_index);
                aws_s3_meta_request_release(meta_request);
            }

            aws_array_list_clean_up(&vip_connection->synced_data.meta_request_ooo_removals);

            aws_s3_vip_connection_shutdown_complete_callback_fn *shutdown_callback = vip_connection->shutdown_callback;
            void *user_data = vip_connection->user_data;

            aws_mem_release(vip_connection->allocator, vip_connection);

            if (shutdown_callback != NULL) {
                shutdown_callback(user_data);
            }

            break;
        }
        default:
            AWS_FATAL_ASSERT(false);
            break;
    }
}
