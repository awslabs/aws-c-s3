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
struct aws_s3_task_manager_work {

    /* Number of arguments actualy used. */
    uint32_t num_args;

    /* Arguments for this operation. */
    void *args[AWS_S3_WORK_UTIL_MAX_ARGS];

    /* Function that will process the work. */
    aws_s3_task_manager_task_fn *task_fn;

    /* Task manager that will be processing this operation. */
    struct aws_s3_task_manager *task_manager;

    /* The actual task itself. */
    struct aws_task task;
};

/* The task manager structure, which tracks the number of tasks in flight, shutdown state, and holds onto state
 * necessary for creating additional tasks. */
struct aws_s3_task_manager {
    struct aws_allocator *allocator;
    struct aws_event_loop *event_loop;

    struct {
        struct aws_mutex lock;
        size_t num_tasks_in_flight;
        uint32_t shutting_down : 1;
    } synced_data;

    /* Callback and it's associated user data for when the work controller has finished shutting down. */
    aws_s3_task_manager_shutdown_fn *shutdown_callback;
    void *shutdown_user_data;
};

/* Manage the mutex lock for our synced data.*/
static void s_s3_task_manager_lock_synced_data(struct aws_s3_task_manager *task_manager);
static void s_s3_task_manager_unlock_synced_data(struct aws_s3_task_manager *task_manager);

/* Allocate a new task work structure */
static struct aws_s3_task_manager_work *s_s3_task_manager_work_new(
    struct aws_s3_task_manager *task_manager,
    uint32_t num_args,
    aws_s3_task_manager_task_fn *task_fn);

/* Destroys a task-util-work structure. */
static void s_s3_task_manager_work_destroy(struct aws_s3_task_manager_work *task_manager_work);

/* Initiate asynchrounous clean up of the task manager. */
static void s_s3_task_manager_clean_up(struct aws_s3_task_manager *task_manager);

/* Task that actually cleans up a task manager. */
static void s_s3_task_manager_clean_up_task(struct aws_task *task, void *arg, enum aws_task_status task_status);

/* Task that processes an individual task-util-work object. */
static void s_s3_task_manager_process_work_task(struct aws_task *task, void *arg, enum aws_task_status task_status);

static void s_s3_task_manager_lock_synced_data(struct aws_s3_task_manager *task_manager) {
    aws_mutex_lock(&task_manager->synced_data.lock);
}

static void s_s3_task_manager_unlock_synced_data(struct aws_s3_task_manager *task_manager) {
    aws_mutex_unlock(&task_manager->synced_data.lock);
}

/* Allocate and setup a new task manager. */
struct aws_s3_task_manager *aws_s3_task_manager_new(
    struct aws_allocator *allocator,
    struct aws_s3_task_manager_options *options) {
    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(options);
    AWS_PRECONDITION(options->allocator);
    AWS_PRECONDITION(options->event_loop);

    struct aws_s3_task_manager *task_manager = aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_task_manager));

    task_manager->allocator = options->allocator;
    task_manager->event_loop = options->event_loop;

    aws_mutex_init(&task_manager->synced_data.lock);

    task_manager->shutdown_callback = options->shutdown_callback;
    task_manager->shutdown_user_data = options->shutdown_user_data;

    return task_manager;
}

/* Initiate destruction of the task manager structure. Once there are no more tasks in flight, it will clean up its
 * state and trigger the shutdown callback.  Any additional requests after shutdown will be ignored. */
void aws_s3_task_manager_destroy(struct aws_s3_task_manager *task_manager) {
    bool clean_up = false;

    s_s3_task_manager_lock_synced_data(task_manager);

    task_manager->synced_data.shutting_down = true;

    /* If we have nothing in flight, we can initiate clean up immediately. */
    if (task_manager->synced_data.num_tasks_in_flight == 0) {
        clean_up = true;
    }

    s_s3_task_manager_unlock_synced_data(task_manager);

    if (clean_up) {
        s_s3_task_manager_clean_up(task_manager);
    }
}

/* Allocate a new task-util-work. */
static struct aws_s3_task_manager_work *s_s3_task_manager_work_new(
    struct aws_s3_task_manager *task_manager,
    uint32_t num_args,
    aws_s3_task_manager_task_fn task_fn) {
    struct aws_s3_task_manager_work *task_manager_work =
        aws_mem_acquire(task_manager->allocator, sizeof(struct aws_s3_task_manager_work));

    if (task_manager_work == NULL) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_TASK_MANAGER,
            "id=%p Could not allocate aws_s3_task_manager_work structure.",
            (void *)task_manager);
        return NULL;
    }

    task_manager_work->num_args = num_args;
    task_manager_work->task_fn = task_fn;
    task_manager_work->task_manager = task_manager;

    aws_task_init(
        &task_manager_work->task,
        s_s3_task_manager_process_work_task,
        task_manager_work,
        "s3_task_manager_process_work_task");

    return task_manager_work;
}

/* Destroys a task-util-work structure. */
static void s_s3_task_manager_work_destroy(struct aws_s3_task_manager_work *task_manager_work) {
    AWS_PRECONDITION(task_manager_work);
    AWS_PRECONDITION(task_manager_work->task_manager);
    AWS_PRECONDITION(task_manager_work->task_manager->allocator);

    aws_mem_release(task_manager_work->task_manager->allocator, task_manager_work);
}

/* Initiate asynchrounous clean up of the task manager. */
static void s_s3_task_manager_clean_up(struct aws_s3_task_manager *task_manager) {
    AWS_PRECONDITION(task_manager);

    struct aws_task *clean_up_task = aws_mem_acquire(task_manager->allocator, sizeof(struct aws_task));

    if (clean_up_task == NULL) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_TASK_MANAGER,
            "id=%p s_s3_task_manager_clean_up could not allocate aws_task for scheduling clean up.",
            (void *)task_manager);
        return;
    }

    aws_task_init(clean_up_task, s_s3_task_manager_clean_up_task, task_manager, "s3_task_manager_clean_up_task");

    aws_event_loop_schedule_task_now(task_manager->event_loop, clean_up_task);
}

/* Task that actually cleans up a task manager. */
static void s_s3_task_manager_clean_up_task(struct aws_task *task, void *arg, enum aws_task_status task_status) {
    AWS_PRECONDITION(task);
    AWS_PRECONDITION(arg);

    struct aws_s3_task_manager *task_manager = arg;

    aws_mem_release(task_manager->allocator, task);
    task = NULL;

    if (task_status != AWS_TASK_STATUS_RUN_READY) {
        return;
    }

    aws_mutex_clean_up(&task_manager->synced_data.lock);

    aws_s3_task_manager_shutdown_fn *shutdown_callback = task_manager->shutdown_callback;
    void *shutdown_user_data = task_manager->shutdown_user_data;

    aws_mem_release(task_manager->allocator, task_manager);
    task_manager = NULL;

    if (shutdown_callback != NULL) {
        shutdown_callback(shutdown_user_data);
    }
}

/* Trigegr an async operation, wraping it in a task. */
int aws_s3_task_manager_create_task(
    struct aws_s3_task_manager *task_manager,
    aws_s3_task_manager_task_fn *task_fn,
    uint64_t delay_ns,
    uint32_t num_args,
    ...) {

    AWS_PRECONDITION(task_manager);
    AWS_PRECONDITION(task_manager->allocator);
    AWS_PRECONDITION(task_manager->event_loop);
    AWS_PRECONDITION(task_fn);
    AWS_PRECONDITION(num_args <= AWS_S3_WORK_UTIL_MAX_ARGS);

    s_s3_task_manager_lock_synced_data(task_manager);

    /* If we're shutting down, nothing else is allowed to be done. */
    if (task_manager->synced_data.shutting_down) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_TASK_MANAGER,
            "id=%p aws_s3_task_manager_create_task called on a task manager that is shutting down.",
            (void *)task_manager);

        s_s3_task_manager_unlock_synced_data(task_manager);

        goto error_result;
    }

    ++task_manager->synced_data.num_tasks_in_flight;

    s_s3_task_manager_unlock_synced_data(task_manager);

    struct aws_s3_task_manager_work *task_manager_work = s_s3_task_manager_work_new(task_manager, num_args, task_fn);

    if (task_manager_work == NULL) {
        goto error_result;
    }

    va_list option_args;
    va_start(option_args, num_args);

    /* Copy our args out of the VA List. */
    for (uint32_t arg_index = 0; arg_index < num_args; ++arg_index) {
        task_manager_work->args[arg_index] = va_arg(option_args, void *);
    }

    /* Null out anything unused. */
    for (uint32_t arg_index = num_args; arg_index < AWS_S3_WORK_UTIL_MAX_ARGS; ++arg_index) {
        task_manager_work->args[arg_index] = NULL;
    }

    va_end(option_args);

    if (delay_ns == 0) {
        aws_event_loop_schedule_task_now(task_manager->event_loop, &task_manager_work->task);
    } else {
        uint64_t now = 0;

        if (aws_event_loop_current_clock_time(task_manager->event_loop, &now)) {
            AWS_LOGF_ERROR(
                AWS_LS_S3_TASK_MANAGER,
                "id=%p aws_s3_task_manager_create_task could not get current time for delayed task, not scheduling "
                "task.",
                (void *)task_manager);

            goto error_clean_up_work;
        }

        aws_event_loop_schedule_task_future(task_manager->event_loop, &task_manager_work->task, now + delay_ns);
    }

    return AWS_OP_SUCCESS;

error_clean_up_work:

    if (task_manager_work != NULL) {
        s_s3_task_manager_work_destroy(task_manager_work);
        task_manager_work = NULL;
    }

error_result:

    return AWS_OP_ERR;
}

/* Task that processes an individual task-util-work object. */
static void s_s3_task_manager_process_work_task(struct aws_task *task, void *arg, enum aws_task_status task_status) {
    (void)task;
    AWS_PRECONDITION(arg);

    struct aws_s3_task_manager_work *task_manager_work = arg;
    struct aws_s3_task_manager *task_manager = task_manager_work->task_manager;

    AWS_PRECONDITION(task_manager);

    if (task_status != AWS_TASK_STATUS_RUN_READY) {
        return;
    }

    /* Call the user's passed in task function. */
    task_manager_work->task_fn(task_manager_work->num_args, task_manager_work->args);

    /* Clean up the task work. */
    s_s3_task_manager_work_destroy(task_manager_work);
    task_manager_work = NULL;

    bool clean_up_task_manager = false;

    s_s3_task_manager_lock_synced_data(task_manager);

    --task_manager->synced_data.num_tasks_in_flight;

    /* If the task manager is shutting down, and this was the last thing in flight, then we can initiate clean up. */
    if (task_manager->synced_data.shutting_down && task_manager->synced_data.num_tasks_in_flight == 0) {
        clean_up_task_manager = true;
    }

    s_s3_task_manager_unlock_synced_data(task_manager);

    if (clean_up_task_manager) {
        s_s3_task_manager_clean_up(task_manager);
    }
}
