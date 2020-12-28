#ifndef AWS_S3_TESTER_H
#define AWS_S3_TESTER_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/s3/private/s3_client_impl.h>
#include <aws/s3/private/s3_meta_request_impl.h>
#include <aws/s3/s3.h>
#include <aws/s3/s3_client.h>

#include <aws/common/common.h>
#include <aws/common/condition_variable.h>
#include <aws/common/logging.h>
#include <aws/common/mutex.h>
#include <aws/common/string.h>

struct aws_client_bootstrap;
struct aws_credentials_provider;
struct aws_event_loop_group;
struct aws_host_resolver;
struct aws_input_stream;

struct aws_s3_client_vtable_patch {
    struct aws_s3_client_vtable *original_vtable;
    struct aws_s3_client_vtable patched_vtable;
};

struct aws_s3_meta_request_vtable_patch {
    struct aws_s3_meta_request_vtable *original_vtable;
    struct aws_s3_meta_request_vtable patched_vtable;
};

/* Utility for setting up commonly needed resources for tests. */
struct aws_s3_tester {
    struct aws_allocator *allocator;
    struct aws_event_loop_group *el_group;
    struct aws_host_resolver *host_resolver;
    struct aws_client_bootstrap *client_bootstrap;
    struct aws_credentials_provider *credentials_provider;
    struct aws_signing_config_aws default_signing_config;

    struct aws_condition_variable signal;
    bool bound_to_client;

    struct aws_array_list client_vtable_patches;
    struct aws_array_list meta_request_vtable_patches;
    void *user_data;

    struct {
        struct aws_mutex lock;

        size_t desired_meta_request_finish_count;
        size_t meta_request_finish_count;

        size_t desired_meta_request_shutdown_count;
        size_t meta_request_shutdown_count;

        int finish_error_code;

        uint32_t meta_requests_finished : 1;
        uint32_t meta_requests_shutdown : 1;
        uint32_t client_shutdown : 1;
    } synced_data;
};

struct aws_s3_meta_request_test_results {
    struct aws_s3_tester *tester;

    struct aws_http_headers *error_response_headers;
    struct aws_byte_buf error_response_body;

    int headers_response_status;
    struct aws_http_headers *response_headers;
    uint64_t expected_range_start;
    uint64_t received_body_size;
    int finished_response_status;
    int finished_error_code;
};

struct aws_s3_client_config;

int aws_s3_tester_init(struct aws_allocator *allocator, struct aws_s3_tester *tester);

enum AWS_S3_TESTER_BIND_CLIENT_FLAGS {
    AWS_S3_TESTER_BIND_CLIENT_REGION = 0x00000001,
    AWS_S3_TESTER_BIND_CLIENT_SIGNING = 0x00000002,
};

enum AWS_S3_TESTER_SSE_TYPE {
    AWS_S3_TESTER_SSE_NONE,
    AWS_S3_TESTER_SSE_KMS,
    AWS_S3_TESTER_SSE_AES256,
};

/* Set up the aws_s3_client's shutdown callbacks to be used by the tester.  This allows the tester to wait for the
 * client to clean up. */
int aws_s3_tester_bind_client(struct aws_s3_tester *tester, struct aws_s3_client_config *config, uint32_t flags);

int aws_s3_tester_bind_meta_request(
    struct aws_s3_tester *tester,
    struct aws_s3_meta_request_options *options,
    struct aws_s3_meta_request_test_results *test_meta_request);

void aws_s3_meta_request_test_results_clean_up(struct aws_s3_meta_request_test_results *test_meta_request);

/* Wait for the correct number of aws_s3_tester_notify_meta_request_finished to be called */
void aws_s3_tester_wait_for_meta_request_finish(struct aws_s3_tester *tester);

/* Wait forthe correct number of aws_s3_tester_notify_meta_request_shutdown to be called. */
void aws_s3_tester_wait_for_meta_request_shutdown(struct aws_s3_tester *tester);

/* Notify the tester that a meta request has finished. */
void aws_s3_tester_notify_meta_request_finished(
    struct aws_s3_tester *tester,
    const struct aws_s3_meta_request_result *result);

/* Notify the tester that a meta request has finished. */
void aws_s3_tester_notify_meta_request_shutdown(struct aws_s3_tester *tester);

/* Handle cleaning up the tester.  If aws_s3_tester_bind_client_shutdown was used, then it will wait for the client to
 * finish shutting down before releasing any resources. */
void aws_s3_tester_clean_up(struct aws_s3_tester *tester);

/* Creates a client with any vtable members replaced with empty functions. */
struct aws_s3_client *aws_s3_tester_mock_client_new(struct aws_s3_tester *tester);

struct aws_http_message *aws_s3_tester_dummy_http_request_new(struct aws_s3_tester *tester);

/* Create a new meta request for testing meta request functionality in isolation. test_results and client are optional.
 * If client is not specified, a new mock client will be created for the meta request. */
struct aws_s3_meta_request *aws_s3_tester_meta_request_new(
    struct aws_s3_tester *tester,
    struct aws_s3_meta_request_test_results *test_results,
    struct aws_s3_client *client);

void aws_s3_create_test_buffer(struct aws_allocator *allocator, size_t buffer_size, struct aws_byte_buf *out_buf);

void aws_s3_tester_lock_synced_data(struct aws_s3_tester *tester);

void aws_s3_tester_unlock_synced_data(struct aws_s3_tester *tester);

struct aws_string *aws_s3_tester_build_endpoint_string(
    struct aws_allocator *allocator,
    const struct aws_byte_cursor *bucket_name,
    const struct aws_byte_cursor *region);

struct aws_http_message *aws_s3_test_get_object_request_new(
    struct aws_allocator *allocator,
    struct aws_byte_cursor host,
    struct aws_byte_cursor key);

struct aws_http_message *aws_s3_test_put_object_request_new(
    struct aws_allocator *allocator,
    struct aws_byte_cursor host,
    struct aws_byte_cursor content_type,
    struct aws_byte_cursor key,
    struct aws_input_stream *body_stream,
    enum AWS_S3_TESTER_SSE_TYPE sse_type);

/* Will copy the client's vtable into a new vtable that can be mutated. Returns the vtable that can be mutated. */
struct aws_s3_client_vtable *aws_s3_tester_patch_client_vtable(
    struct aws_s3_tester *tester,
    struct aws_s3_client *client,
    size_t *out_index);

/* Gets the vtable patch structure that was created as a result of aws_s3_tester_patch_client_vtable.  This allows
 * access to the original vtable.*/
struct aws_s3_client_vtable_patch *aws_s3_tester_get_client_vtable_patch(struct aws_s3_tester *tester, size_t index);

/* Will copy the meta-request's vtable into a new vtable that can be mutated. Returns the vtable that can be mutated. */
struct aws_s3_meta_request_vtable *aws_s3_tester_patch_meta_request_vtable(
    struct aws_s3_tester *tester,
    struct aws_s3_meta_request *meta_request,
    size_t *out_index);

/* Gets the vtable patch structure that was created as a result of aws_s3_tester_patch_meta_request_vtable.  This allows
 * access to the original vtable.*/
struct aws_s3_meta_request_vtable_patch *aws_s3_tester_get_meta_request_vtable_patch(
    struct aws_s3_tester *tester,
    size_t index);

enum AWS_S3_TESTER_SEND_META_REQUEST_FLAGS {
    AWS_S3_TESTER_SEND_META_REQUEST_EXPECT_SUCCESS = 0x00000001,
    AWS_S3_TESTER_SEND_META_REQUEST_DONT_WAIT_FOR_SHUTDOWN = 0x00000002,
};

int aws_s3_tester_send_meta_request(
    struct aws_s3_tester *tester,
    struct aws_s3_client *client,
    struct aws_s3_meta_request_options *options,
    struct aws_s3_meta_request_test_results *test_results,
    uint32_t flags);

int aws_s3_tester_send_get_object_meta_request(
    struct aws_s3_tester *tester,
    struct aws_s3_client *client,
    struct aws_byte_cursor s3_path,
    uint32_t flags,
    enum AWS_S3_TESTER_SSE_TYPE sse_type);

int aws_s3_tester_send_put_object_meta_request(
    struct aws_s3_tester *tester,
    struct aws_s3_client *client,
    uint32_t object_size_mb,
    uint32_t flags,
    enum AWS_S3_TESTER_SSE_TYPE sse_type);

int aws_s3_tester_validate_get_object_results(
    struct aws_s3_meta_request_test_results *meta_request_test_results,
    enum AWS_S3_TESTER_SSE_TYPE sse_type);

int aws_s3_tester_validate_put_object_results(
    struct aws_s3_meta_request_test_results *meta_request_test_results,
    enum AWS_S3_TESTER_SSE_TYPE sse_type);

/*****************************************/
/* Used for mocking functions in vtables */
void aws_s3_client_push_meta_request_empty(struct aws_s3_client *client, struct aws_s3_meta_request *meta_request);

void aws_s3_client_remove_meta_request_empty(struct aws_s3_client *client, struct aws_s3_meta_request *meta_request);

void aws_s3_client_get_http_connection_empty(
    struct aws_s3_client *client,
    struct aws_s3_vip_connection *vip_connection,
    aws_http_connection_manager_on_connection_setup_fn *callback);

int aws_s3_meta_request_next_request_empty(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request **out_request);

int aws_s3_meta_request_prepare_request_empty(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_client *client,
    struct aws_s3_vip_connection *vip_connection,
    bool is_initial_prepare);
/****************************************/

extern struct aws_s3_client_vtable g_aws_s3_client_mock_vtable;

extern const struct aws_byte_cursor g_test_body_content_type;
extern const struct aws_byte_cursor g_test_s3_region;
extern const struct aws_byte_cursor g_test_bucket_name;
extern const struct aws_byte_cursor g_test_public_bucket_name;
extern const struct aws_byte_cursor g_s3_path_get_object_test_1MB;

#endif /* AWS_S3_TESTER_H */
