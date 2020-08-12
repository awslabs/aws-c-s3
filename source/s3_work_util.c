/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_work_util.h"
#include "aws/s3/private/s3_client_impl.h"
#include "aws/s3/private/s3_meta_request_impl.h"
#include "aws/s3/private/s3_vip.h"
#include "aws/s3/private/s3_vip_connection.h"
#include "aws/s3/s3.h"

#include <aws/io/event_loop.h>

static void s_s3_async_work_destroy(struct aws_s3_async_work *async_work);
static void s_s3_async_work_process(struct aws_task *task, void *arg, enum aws_task_status task_status);
static void s_s3_async_work_controller_clean_up(struct aws_s3_async_work_controller *work_controller);
static void s_s3_async_work_controller_clean_up_task(
    struct aws_task *task,
    void *arg,
    enum aws_task_status task_status);

void aws_s3_async_work_controller_init(
    struct aws_s3_async_work_controller *work_controller,
    struct aws_s3_async_work_controller_options *options) {
    AWS_PRECONDITION(work_controller);
    AWS_PRECONDITION(options);
    AWS_PRECONDITION(options->allocator);
    AWS_PRECONDITION(options->event_loop);
    AWS_PRECONDITION(options->work_fn);

    AWS_ZERO_STRUCT(*work_controller);

    work_controller->allocator = options->allocator;
    work_controller->event_loop = options->event_loop;
    work_controller->work_fn = options->work_fn;
    aws_mutex_init(&work_controller->lock);
    work_controller->shutdown_callback = options->shutdown_callback;
    work_controller->shutdown_user_data = options->shutdown_user_data;
}

void aws_s3_async_work_controller_shutdown(struct aws_s3_async_work_controller *work_controller) {
    (void)work_controller;

    bool clean_up = false;

    aws_mutex_lock(&work_controller->lock);
    work_controller->shutting_down = true;

    if (work_controller->num_work_in_flight == 0) {
        clean_up = true;
    }

    aws_mutex_unlock(&work_controller->lock);

    if (clean_up) {
        s_s3_async_work_controller_clean_up(work_controller);
    }
}

static void s_s3_async_work_controller_clean_up(struct aws_s3_async_work_controller *work_controller) {
    AWS_PRECONDITION(work_controller);

    struct aws_task *clean_up_task = aws_mem_acquire(work_controller->allocator, sizeof(struct aws_task));

    if (clean_up_task == NULL) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_ASYNC_WORK,
            "id=%p s_s3_async_work_controller_clean_up could not allocate aws_task for scheduling clean up.",
            (void *)work_controller);
        return;
    }

    aws_task_init(
        clean_up_task,
        s_s3_async_work_controller_clean_up_task,
        work_controller,
        "s3_async_work_controller_clean_up_task");
    aws_event_loop_schedule_task_now(work_controller->event_loop, clean_up_task);
}

static void s_s3_async_work_controller_clean_up_task(
    struct aws_task *task,
    void *arg,
    enum aws_task_status task_status) {
    AWS_PRECONDITION(task);
    AWS_PRECONDITION(arg);

    struct aws_s3_async_work_controller *work_controller = arg;

    if (task_status != AWS_TASK_STATUS_RUN_READY) {
        goto task_clean_up;
    }

    aws_mutex_clean_up(&work_controller->lock);

    if (work_controller->shutdown_callback != NULL) {
        work_controller->shutdown_callback(work_controller->shutdown_user_data);
    }

task_clean_up:

    aws_mem_release(work_controller->allocator, task);
    task = NULL;
}

int aws_s3_async_work_dispatch(struct aws_s3_async_work_options *options) {
    AWS_PRECONDITION(options);

    struct aws_s3_async_work_controller *work_controller = options->work_controller;
    AWS_PRECONDITION(work_controller);
    AWS_PRECONDITION(work_controller->allocator);
    AWS_PRECONDITION(work_controller->event_loop);
    AWS_PRECONDITION(work_controller->work_fn);

    aws_mutex_lock(&work_controller->lock);

    if (work_controller->shutting_down) {
        // TODO would be nice to have a action-to-string function to make log output like this more helpful.
        AWS_LOGF_WARN(
            AWS_LS_S3_ASYNC_WORK,
            "id=%p aws_s3_async_work_dispatch called on a work controller that is shutting down.",
            (void *)work_controller);

        aws_mutex_unlock(&work_controller->lock);

        return AWS_OP_SUCCESS;
    }

    ++work_controller->num_work_in_flight;

    aws_mutex_unlock(&work_controller->lock);

    struct aws_s3_async_work *async_work =
        aws_mem_calloc(work_controller->allocator, 1, sizeof(struct aws_s3_async_work));

    if (async_work == NULL) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_ASYNC_WORK, "id=%p Could not allocate aws_s3_async_work structure.", (void *)work_controller);
        return AWS_OP_ERR;
    }

    async_work->work_controller = options->work_controller;
    async_work->action = options->action;

    memcpy(async_work->params, options->params, sizeof(void *) * AWS_S3_WORK_UTIL_MAX_PARAMS);

    aws_task_init(&async_work->task, s_s3_async_work_process, async_work, "s3_async_work");

    if (options->schedule_time_offset_ns == 0) {
        aws_event_loop_schedule_task_now(work_controller->event_loop, &async_work->task);
    } else {
        uint64_t now;
        aws_event_loop_current_clock_time(work_controller->event_loop, &now);

        aws_event_loop_schedule_task_future(
            work_controller->event_loop, &async_work->task, now + options->schedule_time_offset_ns);
    }

    return AWS_OP_SUCCESS;
}

static void s_s3_async_work_destroy(struct aws_s3_async_work *async_work) {
    AWS_PRECONDITION(async_work);
    AWS_PRECONDITION(async_work->work_controller);
    AWS_PRECONDITION(async_work->work_controller->allocator);

    aws_mem_release(async_work->work_controller->allocator, async_work);
}

static void s_s3_async_work_process(struct aws_task *task, void *arg, enum aws_task_status task_status) {
    (void)task;
    AWS_PRECONDITION(arg);

    struct aws_s3_async_work *async_work = arg;
    bool clean_up_work_controller = false;
    struct aws_s3_async_work_controller *async_work_controller = async_work->work_controller;

    AWS_PRECONDITION(async_work_controller);

    if (task_status == AWS_TASK_STATUS_RUN_READY) {
        async_work_controller->work_fn(async_work);
    }

    aws_mutex_lock(&async_work_controller->lock);
    --async_work_controller->num_work_in_flight;

    if (async_work_controller->shutting_down && async_work_controller->num_work_in_flight == 0) {
        clean_up_work_controller = true;
    }
    aws_mutex_unlock(&async_work_controller->lock);

    if (async_work != NULL) {
        s_s3_async_work_destroy(async_work);
        async_work = NULL;
    }

    if (clean_up_work_controller) {
        s_s3_async_work_controller_clean_up(async_work_controller);
    }
}
