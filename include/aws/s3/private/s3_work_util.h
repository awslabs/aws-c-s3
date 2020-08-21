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

/* This is meant as simple utility for wrapping common task operations and for providing an off-switch for additional
 * tasks and shutdown behavior.*/

struct aws_s3_task_util;

typedef void(aws_s3_task_util_task_fn)(uint32_t num_args, void **args);
typedef void(aws_s3_task_util_shutdown_fn)(void *user_data);

/* Options for initializing a task util */
struct aws_s3_task_util_options {

    struct aws_allocator *allocator;

    struct aws_event_loop *event_loop;

    aws_s3_task_util_shutdown_fn *shutdown_callback;
    void *shutdown_user_data;
};

struct aws_s3_task_util *aws_s3_task_util_new(
    struct aws_allocator *allocator,
    struct aws_s3_task_util_options *options);

/* Initiate destruction of the task util structure. Once there are no more tasks in flight, it will clean up its state
 * and trigger the shutdown callback.  Any additional requests after shutdown will be ignored. */
void aws_s3_task_util_destroy(struct aws_s3_task_util *task_util);

/* Trigegr an async operation, wraping it in a task. */
int aws_s3_task_util_create_task(
    struct aws_s3_task_util *task_util,
    aws_s3_task_util_task_fn *task_fn,
    uint64_t delay,
    uint32_t num_args,
    ...);

#endif /* AWS_S3_WORK_UTIL_H */
