#ifndef AWS_S3_WORK_UTIL_H
#define AWS_S3_WORK_UTIL_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/mutex.h>
#include <aws/common/task_scheduler.h>
#include <inttypes.h>

/* This is meant as a simple utility to enable quickly setting up asynchronous operations without having to go through
 * all of the task set up each time, particularly for operations that are only a few lines long. The usage of this is
 * such that:
 *
 *    * The user sets up a function that receives async work, having the same format as aws_s3_async_work_fn.
 *    * Async work is identified by an "action" integer which is intended to be an enum defined by the user.
 *    * The user stores a "aws_s3_work_controller" on their system that needs async actions, which holds onto the async
 *      work function, among several other common staples.
 *    * When one wants to trigger an asynchronous operation, they call aws_s3_async_work_dispatch with their desired
 *      options, which includes things such as an action id, parameters, and the associated work controller.  Work is
 *      allocated, and a task is created that calls the work function.
 *    * Dispatch operations are guaranteed to be thread safe, ie, you do not have to provide an additional lock for the
 *      work controller state.
 * */

#define AWS_S3_WORK_UTIL_MAX_PARAMS 4

struct aws_s3_async_work;
struct aws_s3_async_work_controller;

typedef void(aws_s3_async_work_fn)(struct aws_s3_async_work *work);

typedef void(aws_s3_async_work_controller_shutdown_fn)(void *user_data);

/* Options used when dispatching async work.*/
struct aws_s3_async_work_options {
    /* Id of the action.  Intended to an enum specified by the user. */
    int32_t action;

    /* Work controller that will process this operation. */
    struct aws_s3_async_work_controller *work_controller;

    /* Parameters that will be sent with this operation. */
    void *params[AWS_S3_WORK_UTIL_MAX_PARAMS];

    /* How far into the future to schedule the operation. 0 means it will happen 'now'.*/
    uint64_t schedule_time_offset_ns;
};

/* Allocated work which is passed into the work function. */
struct aws_s3_async_work {

    /* Id of the action.  Intended to an enum specified by the user. */
    int32_t action;

    /* Work controller that will be processing this operation. */
    struct aws_s3_async_work_controller *work_controller;

    /* Parameters for this operation. */
    void *params[AWS_S3_WORK_UTIL_MAX_PARAMS];

    /* Task structure used for facilitating the asynchronous functionality. */
    struct aws_task task;
};

/* Options for initializing a work controller. */
struct aws_s3_async_work_controller_options {

    struct aws_allocator *allocator;
    struct aws_event_loop *event_loop;

    /* Function that will process the work. */
    aws_s3_async_work_fn *work_fn;

    /* Callback and it's associated user data for when the work controller has finished shutting down. */
    aws_s3_async_work_controller_shutdown_fn *shutdown_callback;
    void *shutdown_user_data;
};

/* The actual work controller structure, which holds the work function and tracks work currently in progress. */
struct aws_s3_async_work_controller {
    struct aws_allocator *allocator;
    struct aws_event_loop *event_loop;

    /* User specified function that will process the work. */
    aws_s3_async_work_fn *work_fn;

    /* Mutex used for any state accessible between threads. */
    struct aws_mutex lock;

    /* How much work hasn't finished yet.  Requires lock. */
    size_t num_work_in_flight;

    /* If the controller is currently shutting down or not.  Requires lock. */
    bool shutting_down;

    /* Callback and it's associated user data for when the work controller has finished shutting down. */
    aws_s3_async_work_controller_shutdown_fn *shutdown_callback;
    void *shutdown_user_data;
};

/* Initialize a work controller object. */
void aws_s3_async_work_controller_init(
    struct aws_s3_async_work_controller *work_controller,
    struct aws_s3_async_work_controller_options *options);

/* Shutdown the async work controller.  Once there is no more work in flight, it will clean up it's state and trigger
 * the shutdown callback.  Any additional requests after shutdown is initiated will be ignored.. */
void aws_s3_async_work_controller_shutdown(struct aws_s3_async_work_controller *work_controller);

/* Trigegr an async operation. */
int aws_s3_async_work_dispatch(struct aws_s3_async_work_options *options);

#endif /* AWS_S3_WORK_UTIL_H */
