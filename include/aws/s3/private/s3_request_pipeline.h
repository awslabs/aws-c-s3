#ifndef AWS_S3_REQUEST_PIPELINE_H
#define AWS_S3_REQUEST_PIPELINE_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

/* This structure represents the HTTP processing (called here a "pipeline") of an S3 request, from signing, to acquiring
 * a connection, to handling HTTP callbacks, etc..  We setup a pipeline execution through
 * aws_s3_request_pipeline_setup, and when we are ready (which should be soon thereafter), call
 * aws_s3_request_pipeline_execute.
 */

#include <aws/common/byte_buf.h>

struct aws_s3_request;
struct aws_s3_request_pipeline;
struct aws_s3_meta_request;
struct aws_s3_body_meta_data;

struct aws_http_stream;
struct aws_credentials_provider;
struct aws_http_connection_manager;

/* Incoming body callback listener. */
typedef int(aws_s3_request_pipeline_body_callback_fn)(
    struct aws_s3_request_pipeline *pipeline,
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    const struct aws_byte_cursor *data,
    const struct aws_s3_body_meta_data *meta_data,
    void *user_data);

/* Pipeline execution-has-finished callback. */
typedef void(aws_s3_request_pipeline_exec_finished_callback_fn)(
    struct aws_s3_request_pipeline *pipeline,
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    int error_code,
    void *user_data);

/* Pipeline execution-has-cleaned-up calblack*/
typedef void(aws_s3_request_pipeline_exec_cleaned_up_callback_fn)(
    struct aws_s3_request_pipeline *pipeline,
    int error_code,
    void *user_data);

/* Possible settings/callbacks for listening to events on the pipeline. */
struct aws_s3_request_pipeline_listener {
    aws_s3_request_pipeline_body_callback_fn *body_callback;
    aws_s3_request_pipeline_exec_finished_callback_fn *exec_finished_callback;
    aws_s3_request_pipeline_exec_cleaned_up_callback_fn *exec_cleaned_up_callback;
    void *user_data;
};

/* Options/necessary structures for creating a pipeline. */
struct aws_s3_request_pipeline_options {
    struct aws_credentials_provider *credentials_provider;
    struct aws_http_connection_manager *http_connection_manager;
    struct aws_byte_cursor region;
    struct aws_s3_request_pipeline_listener listener;
};

struct aws_s3_request_pipeline *aws_s3_request_pipeline_new(
    struct aws_allocator *allocator,
    struct aws_s3_request_pipeline_options *options);

void aws_s3_request_pipeline_destroy(struct aws_s3_request_pipeline *pipeline);

/* Options for setting up a pipeline execution.*/
struct aws_s3_request_pipeline_exec_options {

    /* The meta request whose request the pipeline will be processing. */
    struct aws_s3_meta_request *meta_request;

    /* The actual request that will be being processed. */
    struct aws_s3_request *request;

    /* Optional listener data that the caller setting up pipeline execution can use to listen to the pipeline. */
    struct aws_s3_request_pipeline_listener listener;
};

void aws_s3_request_pipeline_setup(
    struct aws_s3_request_pipeline *pipeline,
    struct aws_s3_request_pipeline_exec_options *options);

void aws_s3_request_pipeline_execute(struct aws_s3_request_pipeline *pipeline);

#endif /* AWS_S3_REQUEST_PIPELINE_H */
