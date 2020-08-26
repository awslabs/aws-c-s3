#ifndef AWS_S3_WORK_UTIL_H
#define AWS_S3_WORK_UTIL_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/linked_list.h>
#include <aws/common/mutex.h>
#include <aws/common/task_scheduler.h>
#include <inttypes.h>

/* Task manager is meant as a simple utility for wrapping task creation and tracking in flight requests.  When shutdown
 * is triggered it will block additional requests, and wait for all in flight requests to finish before cleaning up and
 * calling its own shutdown callback.  */

struct aws_s3_task_manager;

typedef void(aws_s3_task_manager_task_fn)(uint32_t num_args, void **args);
typedef void(aws_s3_task_manager_shutdown_fn)(void *user_data);

/* Options for initializing a task manager */
struct aws_s3_task_manager_options {

    struct aws_allocator *allocator;

    struct aws_event_loop *event_loop;

    aws_s3_task_manager_shutdown_fn *shutdown_callback;
    void *shutdown_user_data;
};

struct aws_s3_task_manager *aws_s3_task_manager_new(
    struct aws_allocator *allocator,
    struct aws_s3_task_manager_options *options);

/* Initiate destruction of the task manager structure. Once there are no more tasks in flight, it will clean up its
 * state and trigger the shutdown callback.  Any additional requests after shutdown will be ignored. */
void aws_s3_task_manager_destroy(struct aws_s3_task_manager *task_manager);

/* Trigger an async operation, wrapping it in a task. */
int aws_s3_task_manager_create_task(
    struct aws_s3_task_manager *task_manager,
    aws_s3_task_manager_task_fn *task_fn,
    uint64_t delay,
    uint32_t num_args,
    ...);

#endif /* AWS_S3_WORK_UTIL_H */
