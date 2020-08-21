/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_work_util.h"
#include "aws/s3/private/s3_client_impl.h"
#include "aws/s3/private/s3_meta_request_impl.h"
#include "aws/s3/private/s3_vip_connection.h"
#include "aws/s3/s3.h"

#include <aws/io/event_loop.h>
#include <stdarg.h>

#define AWS_S3_WORK_UTIL_MAX_ARGS 3

/* Allocated work which essentially wraps a task. */
struct aws_s3_task_util_work {

    /* Number of arguments actualy used. */
    uint32_t num_args;

    /* Arguments for this operation. */
    void *args[AWS_S3_WORK_UTIL_MAX_ARGS];

    /* Function that will process the work. */
    aws_s3_task_util_task_fn *task_fn;

    /* Task util that will be processing this operation. */
    struct aws_s3_task_util *task_util;

    /* The actual task itself. */
    struct aws_task task;
};

/* The task util structure, which handles scheduling tasks and facilitates shutdown. */
struct aws_s3_task_util {
    struct aws_allocator *allocator;
    struct aws_event_loop *event_loop;

    struct {
        struct aws_mutex lock;
        size_t num_tasks_in_flight;
        uint32_t shutting_down : 1;
    } synced_data;

    /* Callback and it's associated user data for when the work controller has finished shutting down. */
    aws_s3_task_util_shutdown_fn *shutdown_callback;
    void *shutdown_user_data;
};

/* Manage the mutex lock for our synced data.*/
static void s_s3_task_util_lock_synced_data(struct aws_s3_task_util *task_util);
static void s_s3_task_util_unlock_synced_data(struct aws_s3_task_util *task_util);

/* Allocate a new task work structure */
static struct aws_s3_task_util_work *s_s3_task_util_work_new(
    struct aws_s3_task_util *task_util,
    uint32_t num_args,
    aws_s3_task_util_task_fn *task_fn);

/* Destroys a task-util-work structure. */
static void s_s3_task_util_work_destroy(struct aws_s3_task_util_work *task_util_work);

/* Initiate asynchrounous clean up of the task utility. */
static void s_s3_task_util_clean_up(struct aws_s3_task_util *task_util);

/* Task that actually cleans up a task utility. */
static void s_s3_task_util_clean_up_task(struct aws_task *task, void *arg, enum aws_task_status task_status);

/* Task that processes an individual task-util-work object. */
static void s_s3_task_util_process_work_task(struct aws_task *task, void *arg, enum aws_task_status task_status);

static void s_s3_task_util_lock_synced_data(struct aws_s3_task_util *task_util) {
    aws_mutex_lock(&task_util->synced_data.lock);
}

static void s_s3_task_util_unlock_synced_data(struct aws_s3_task_util *task_util) {
    aws_mutex_unlock(&task_util->synced_data.lock);
}

/* Allocate and setup a new task util. */
struct aws_s3_task_util *aws_s3_task_util_new(
    struct aws_allocator *allocator,
    struct aws_s3_task_util_options *options) {
    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(options);
    AWS_PRECONDITION(options->allocator);
    AWS_PRECONDITION(options->event_loop);

    struct aws_s3_task_util *task_util = aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_task_util));

    task_util->allocator = options->allocator;
    task_util->event_loop = options->event_loop;

    aws_mutex_init(&task_util->synced_data.lock);

    task_util->shutdown_callback = options->shutdown_callback;
    task_util->shutdown_user_data = options->shutdown_user_data;

    return task_util;
}

/* Initiate destruction of the task util structure. Once there are no more tasks in flight, it will clean up its state
 * and trigger the shutdown callback.  Any additional requests after shutdown will be ignored. */
void aws_s3_task_util_destroy(struct aws_s3_task_util *task_util) {
    bool clean_up = false;

    s_s3_task_util_lock_synced_data(task_util);

    task_util->synced_data.shutting_down = true;

    /* If we have nothing in flight, we can initiate clean up immediately. */
    if (task_util->synced_data.num_tasks_in_flight == 0) {
        clean_up = true;
    }

    s_s3_task_util_unlock_synced_data(task_util);

    if (clean_up) {
        s_s3_task_util_clean_up(task_util);
    }
}

/* Allocate a new task-util-work. */
static struct aws_s3_task_util_work *s_s3_task_util_work_new(
    struct aws_s3_task_util *task_util,
    uint32_t num_args,
    aws_s3_task_util_task_fn task_fn) {
    struct aws_s3_task_util_work *task_util_work =
        aws_mem_acquire(task_util->allocator, sizeof(struct aws_s3_task_util_work));

    if (task_util_work == NULL) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_TASK_UTIL, "id=%p Could not allocate aws_s3_task_util_work structure.", (void *)task_util);
        return NULL;
    }

    task_util_work->num_args = num_args;
    task_util_work->task_fn = task_fn;
    task_util_work->task_util = task_util;

    aws_task_init(
        &task_util_work->task, s_s3_task_util_process_work_task, task_util_work, "s3_task_util_process_work_task");

    return task_util_work;
}

/* Destroys a task-util-work structure. */
static void s_s3_task_util_work_destroy(struct aws_s3_task_util_work *task_util_work) {
    AWS_PRECONDITION(task_util_work);
    AWS_PRECONDITION(task_util_work->task_util);
    AWS_PRECONDITION(task_util_work->task_util->allocator);

    aws_mem_release(task_util_work->task_util->allocator, task_util_work);
}

/* Initiate asynchrounous clean up of the task utility. */
static void s_s3_task_util_clean_up(struct aws_s3_task_util *task_util) {
    AWS_PRECONDITION(task_util);

    struct aws_task *clean_up_task = aws_mem_acquire(task_util->allocator, sizeof(struct aws_task));

    if (clean_up_task == NULL) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_TASK_UTIL,
            "id=%p s_s3_task_util_clean_up could not allocate aws_task for scheduling clean up.",
            (void *)task_util);
        return;
    }

    aws_task_init(clean_up_task, s_s3_task_util_clean_up_task, task_util, "s3_task_util_clean_up_task");

    aws_event_loop_schedule_task_now(task_util->event_loop, clean_up_task);
}

/* Task that actually cleans up a task utility. */
static void s_s3_task_util_clean_up_task(struct aws_task *task, void *arg, enum aws_task_status task_status) {
    AWS_PRECONDITION(task);
    AWS_PRECONDITION(arg);

    struct aws_s3_task_util *task_util = arg;

    aws_mem_release(task_util->allocator, task);
    task = NULL;

    if (task_status != AWS_TASK_STATUS_RUN_READY) {
        return;
    }

    aws_mutex_clean_up(&task_util->synced_data.lock);

    aws_s3_task_util_shutdown_fn *shutdown_callback = task_util->shutdown_callback;
    void *shutdown_user_data = task_util->shutdown_user_data;

    aws_mem_release(task_util->allocator, task_util);
    task_util = NULL;

    if (shutdown_callback != NULL) {
        shutdown_callback(shutdown_user_data);
    }
}

/* Trigegr an async operation, wraping it in a task. */
int aws_s3_task_util_create_task(
    struct aws_s3_task_util *task_util,
    aws_s3_task_util_task_fn *task_fn,
    uint64_t delay_ns,
    uint32_t num_args,
    ...) {

    AWS_PRECONDITION(task_util);
    AWS_PRECONDITION(task_util->allocator);
    AWS_PRECONDITION(task_util->event_loop);
    AWS_PRECONDITION(task_fn);
    AWS_PRECONDITION(num_args <= AWS_S3_WORK_UTIL_MAX_ARGS);

    s_s3_task_util_lock_synced_data(task_util);

    /* If we're shutting down, nothing else is allowed to be done. */
    if (task_util->synced_data.shutting_down) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_TASK_UTIL,
            "id=%p aws_s3_task_util_create_task called on a task util that is shutting down.",
            (void *)task_util);

        s_s3_task_util_unlock_synced_data(task_util);

        goto error_result;
    }

    ++task_util->synced_data.num_tasks_in_flight;

    s_s3_task_util_unlock_synced_data(task_util);

    struct aws_s3_task_util_work *task_util_work = s_s3_task_util_work_new(task_util, num_args, task_fn);

    if (task_util_work == NULL) {
        goto error_result;
    }

    va_list option_args;
    va_start(option_args, num_args);

    /* Copy our args out of the VA List. */
    for (uint32_t arg_index = 0; arg_index < num_args; ++arg_index) {
        task_util_work->args[arg_index] = va_arg(option_args, void *);
    }

    /* Null out anything unused. */
    for (uint32_t arg_index = num_args; arg_index < AWS_S3_WORK_UTIL_MAX_ARGS; ++arg_index) {
        task_util_work->args[arg_index] = NULL;
    }

    va_end(option_args);

    if (delay_ns == 0) {
        aws_event_loop_schedule_task_now(task_util->event_loop, &task_util_work->task);
    } else {
        uint64_t now = 0;

        if (aws_event_loop_current_clock_time(task_util->event_loop, &now)) {
            AWS_LOGF_ERROR(
                AWS_LS_S3_TASK_UTIL,
                "id=%p aws_s3_task_util_create_task could not get current time for delayed task, not scheduling task.",
                (void *)task_util);

            goto error_clean_up_work;
        }

        aws_event_loop_schedule_task_future(task_util->event_loop, &task_util_work->task, now + delay_ns);
    }

    return AWS_OP_SUCCESS;

error_clean_up_work:

    if (task_util_work != NULL) {
        s_s3_task_util_work_destroy(task_util_work);
        task_util_work = NULL;
    }

error_result:

    return AWS_OP_ERR;
}

/* Task that processes an individual task-util-work object. */
static void s_s3_task_util_process_work_task(struct aws_task *task, void *arg, enum aws_task_status task_status) {
    (void)task;
    AWS_PRECONDITION(arg);

    struct aws_s3_task_util_work *task_util_work = arg;
    struct aws_s3_task_util *task_util = task_util_work->task_util;

    AWS_PRECONDITION(task_util);

    if (task_status != AWS_TASK_STATUS_RUN_READY) {
        return;
    }

    /* Call the user's passed in task function. */
    task_util_work->task_fn(task_util_work->num_args, task_util_work->args);

    /* Clean up the task work. */
    s_s3_task_util_work_destroy(task_util_work);
    task_util_work = NULL;

    bool clean_up_task_util = false;

    s_s3_task_util_lock_synced_data(task_util);

    --task_util->synced_data.num_tasks_in_flight;

    /* If the task utility is shutting down, and this was the last thing in flight, then we can initiate clean up. */
    if (task_util->synced_data.shutting_down && task_util->synced_data.num_tasks_in_flight == 0) {
        clean_up_task_util = true;
    }

    s_s3_task_util_unlock_synced_data(task_util);

    if (clean_up_task_util) {
        s_s3_task_util_clean_up(task_util);
    }
}
