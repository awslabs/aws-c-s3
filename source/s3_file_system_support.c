/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/s3/private/s3_file_system_support.h>

#include <aws/common/condition_variable.h>
#include <aws/common/linked_list.h>
#include <aws/common/macros.h>
#include <aws/common/mutex.h>
#include <aws/common/ref_count.h>
#include <aws/common/xml_parser.h>

#include <aws/io/uri.h>

#include <aws/http/http.h>
#include <aws/http/request_response.h>

#include <inttypes.h>

enum operation_state {
    OS_NOT_STARTED,
    OS_INITIATED,
    OS_COMPLETED,
    OS_ERROR,
};

struct aws_s3_paginator {
    struct aws_allocator *allocator;
    struct aws_s3_client *client;
    struct aws_s3_meta_request *current_request;
    struct aws_string *bucket_name;
    struct aws_string *prefix;
    struct aws_string *delimiter;
    struct aws_string *endpoint;
    aws_s3_on_object_fn *on_object;
    aws_s3_on_object_list_finished *on_list_finished;
    void *user_data;

    struct aws_ref_count ref_count;
    struct {
        struct aws_string *continuation_token;
        enum operation_state operation_state;
        struct aws_mutex lock;
        bool has_more_results;
    } shared_mt_state;

    struct aws_byte_buf result_body;
};

static const size_t s_dynamic_body_initial_buf_size = 1024;

static void s_ref_count_zero_callback(void *arg) {
    struct aws_s3_paginator *paginator = arg;

    aws_s3_client_release(paginator->client);

    if (paginator->current_request) {
        aws_s3_meta_request_release(paginator->current_request);
    }

    if (paginator->bucket_name) {
        aws_string_destroy(paginator->bucket_name);
    }

    if (paginator->delimiter) {
        aws_string_destroy(paginator->delimiter);
    }

    if (paginator->prefix) {
        aws_string_destroy(paginator->prefix);
    }

    if (paginator->endpoint) {
        aws_string_destroy(paginator->endpoint);
    }

    aws_mem_release(paginator->allocator, paginator);
}

struct aws_s3_paginator *aws_s3_initiate_list_bucket(
    struct aws_allocator *allocator,
    const struct aws_s3_list_bucket_v2_params *params) {
    AWS_FATAL_PRECONDITION(params);
    AWS_FATAL_PRECONDITION(params->client);
    AWS_FATAL_PRECONDITION(params->bucket_name.len);
    AWS_FATAL_PRECONDITION(params->endpoint.len);

    struct aws_s3_paginator *paginator = aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_paginator));
    paginator->allocator = allocator;
    paginator->client = params->client;
    aws_s3_client_acquire(params->client);
    paginator->bucket_name = aws_string_new_from_cursor(allocator, &params->bucket_name);
    paginator->endpoint = aws_string_new_from_cursor(allocator, &params->endpoint);
    paginator->delimiter = params->delimiter.len > 0 ? aws_string_new_from_cursor(allocator, &params->delimiter) : NULL;
    paginator->prefix = params->prefix.len > 0 ? aws_string_new_from_cursor(allocator, &params->prefix) : NULL;
    paginator->on_object = params->on_object;
    paginator->on_list_finished = params->on_list_finished;
    paginator->user_data = params->user_data;
    aws_byte_buf_init(&paginator->result_body, allocator, s_dynamic_body_initial_buf_size);
    aws_ref_count_init(&paginator->ref_count, paginator, s_ref_count_zero_callback);
    aws_mutex_init(&paginator->shared_mt_state.lock);
    paginator->shared_mt_state.operation_state = OS_NOT_STARTED;

    return paginator;
}

void aws_s3_paginator_release(struct aws_s3_paginator *paginator) {
    if (paginator) {
        aws_ref_count_release(&paginator->ref_count);
    }
}

void aws_s3_paginator_acquire(struct aws_s3_paginator *paginator) {
    AWS_FATAL_PRECONDITION(paginator);
    aws_ref_count_acquire(&paginator->ref_count);
}

static inline int s_set_paginator_state_if_legal(
    struct aws_s3_paginator *paginator,
    enum operation_state expected,
    enum operation_state state) {
    aws_mutex_lock(&paginator->shared_mt_state.lock);
    if (paginator->shared_mt_state.operation_state != expected) {
        aws_mutex_unlock(&paginator->shared_mt_state.lock);
        return aws_raise_error(AWS_ERROR_INVALID_STATE);
    }
    paginator->shared_mt_state.operation_state = state;
    aws_mutex_unlock(&paginator->shared_mt_state.lock);
    return AWS_OP_SUCCESS;
}

static void s_list_bucket_shutdown(void *user_data) {
    (void) user_data;

    // TODO: check if required, already released on s_list_bucket_request_finished callback
    //struct aws_s3_paginator *paginator = user_data;
    //aws_ref_count_release(&paginator->ref_count);
}

struct fs_parser_wrapper {
    struct aws_allocator *allocator;
    struct aws_s3_object_file_system_info fs_info;
};

static bool s_on_contents_node(struct aws_xml_parser *parser, struct aws_xml_node *node, void *user_data) {
    struct fs_parser_wrapper *fs_wrapper = user_data;
    struct aws_s3_object_file_system_info *fs_info = &fs_wrapper->fs_info;

    struct aws_byte_cursor node_name;
    aws_xml_node_get_name(node, &node_name);

    if (aws_byte_cursor_eq_c_str_ignore_case(&node_name, "ETag")) {
        return aws_xml_node_as_body(parser, node, &fs_info->e_tag) == AWS_OP_SUCCESS;
    }

    if (aws_byte_cursor_eq_c_str_ignore_case(&node_name, "Key")) {
        return aws_xml_node_as_body(parser, node, &fs_info->key) == AWS_OP_SUCCESS;
    }

    if (aws_byte_cursor_eq_c_str_ignore_case(&node_name, "LastModified")) {
        struct aws_byte_cursor date_cur;
        if (aws_xml_node_as_body(parser, node, &date_cur) == AWS_OP_SUCCESS) {
            aws_date_time_init_from_str_cursor(&fs_info->last_modified, &date_cur, AWS_DATE_FORMAT_ISO_8601);
            return true;
        }

        return false;
    }

    if (aws_byte_cursor_eq_c_str_ignore_case(&node_name, "Size")) {
        struct aws_byte_cursor size_cur;

        if (aws_xml_node_as_body(parser, node, &size_cur) == AWS_OP_SUCCESS) {
            struct aws_string *size_str = aws_string_new_from_cursor(fs_wrapper->allocator, &size_cur);

            bool result = false;

            if (sscanf((const char *)size_str->bytes, "%" PRIu64, &fs_info->size) == 1) {
                result = true;
            }

            aws_string_destroy(size_str);
            return result;
        }
    }

    if (aws_byte_cursor_eq_c_str_ignore_case(&node_name, "ETag")) {
        return aws_xml_node_as_body(parser, node, &fs_info->e_tag) == AWS_OP_SUCCESS;
    }

    return true;
}

static bool s_on_common_prefixes_node(struct aws_xml_parser *parser, struct aws_xml_node *node, void *user_data) {
    struct fs_parser_wrapper *fs_wrapper = user_data;

    struct aws_byte_cursor node_name;
    aws_xml_node_get_name(node, &node_name);

    if (aws_byte_cursor_eq_c_str_ignore_case(&node_name, "Prefix")) {
        return aws_xml_node_as_body(parser, node, &fs_wrapper->fs_info.prefix) == AWS_OP_SUCCESS;
    }

    return true;
}

static bool s_on_list_bucket_result_node_encountered(
    struct aws_xml_parser *parser,
    struct aws_xml_node *node,
    void *user_data) {
    struct aws_s3_paginator *paginator = user_data;

    struct aws_byte_cursor node_name;
    aws_xml_node_get_name(node, &node_name);

    struct fs_parser_wrapper fs_wrapper;
    AWS_ZERO_STRUCT(fs_wrapper);

    if (aws_byte_cursor_eq_c_str_ignore_case(&node_name, "Contents")) {
        fs_wrapper.allocator = paginator->allocator;
        bool ret_val = aws_xml_node_traverse(parser, node, s_on_contents_node, &fs_wrapper) == AWS_OP_SUCCESS;

        if (paginator->prefix) {
            fs_wrapper.fs_info.prefix = aws_byte_cursor_from_string(paginator->prefix);
        }

        if (ret_val && paginator->on_object) {
            ret_val |= paginator->on_object(&fs_wrapper.fs_info, paginator->user_data);
        }

        return ret_val;
    }

    if (aws_byte_cursor_eq_c_str_ignore_case(&node_name, "CommonPrefixes")) {
        bool ret_val = aws_xml_node_traverse(parser, node, s_on_common_prefixes_node, paginator) == AWS_OP_SUCCESS;

        if (ret_val && paginator->on_object) {
            ret_val |= paginator->on_object(&fs_wrapper.fs_info, paginator->user_data);
        }

        return ret_val;
    }

    if (aws_byte_cursor_eq_c_str_ignore_case(&node_name, "NextContinuationToken")) {
        struct aws_byte_cursor continuation_token_cur;
        bool ret_val = aws_xml_node_as_body(parser, node, &continuation_token_cur) == AWS_OP_SUCCESS;

        if (ret_val) {
            aws_mutex_lock(&paginator->shared_mt_state.lock);

            if (paginator->shared_mt_state.continuation_token) {
                aws_string_destroy(paginator->shared_mt_state.continuation_token);
            }
            paginator->shared_mt_state.continuation_token =
                aws_string_new_from_cursor(paginator->allocator, &continuation_token_cur);
            aws_mutex_unlock(&paginator->shared_mt_state.lock);
        }

        return ret_val;
    }

    if (aws_byte_cursor_eq_c_str_ignore_case(&node_name, "IsTruncated")) {
        struct aws_byte_cursor truncated_cur;
        bool ret_val = aws_xml_node_as_body(parser, node, &truncated_cur) == AWS_OP_SUCCESS;

        if (ret_val) {
            if (aws_byte_cursor_eq_c_str_ignore_case(&truncated_cur, "true")) {
                aws_mutex_lock(&paginator->shared_mt_state.lock);
                paginator->shared_mt_state.has_more_results = true;
                aws_mutex_unlock(&paginator->shared_mt_state.lock);
            }
        }

        return ret_val;
    }

    return true;
}

static bool s_on_root_node_encountered(struct aws_xml_parser *parser, struct aws_xml_node *node, void *user_data) {
    struct aws_s3_paginator *paginator = user_data;

    struct aws_byte_cursor node_name;
    aws_xml_node_get_name(node, &node_name);
    if (aws_byte_cursor_eq_c_str_ignore_case(&node_name, "ListBucketResult")) {
        return aws_xml_node_traverse(parser, node, s_on_list_bucket_result_node_encountered, paginator);
    }

    return false;
}

static int s_list_bucket_receive_body_callback(
    struct aws_s3_meta_request *meta_request,
    const struct aws_byte_cursor *body,
    uint64_t range_start,
    void *user_data) {
    (void)range_start;
    (void)meta_request;

    struct aws_s3_paginator *paginator = user_data;

    if (body && body->len) {
        aws_byte_buf_append_dynamic(&paginator->result_body, body);
    }
    return AWS_OP_SUCCESS;
}

static void s_list_bucket_request_finished(
    struct aws_s3_meta_request *meta_request,
    const struct aws_s3_meta_request_result *meta_request_result,
    void *user_data) {
    (void)meta_request;
    struct aws_s3_paginator *paginator = user_data;

    if (meta_request_result->response_status == 200) {

        struct aws_xml_parser_options parser_options = {
            .doc = aws_byte_cursor_from_buf(&paginator->result_body),
            .max_depth = 16U,
        };

        // clears previous continuation token
        aws_mutex_lock(&paginator->shared_mt_state.lock);
        if (paginator->shared_mt_state.continuation_token) {
            aws_string_destroy(paginator->shared_mt_state.continuation_token);
            paginator->shared_mt_state.continuation_token = NULL;
            paginator->shared_mt_state.has_more_results = false;
        }
        aws_mutex_unlock(&paginator->shared_mt_state.lock);

        struct aws_xml_parser *parser = aws_xml_parser_new(paginator->allocator, &parser_options);
        aws_xml_parser_parse(parser, s_on_root_node_encountered, paginator);
        aws_xml_parser_destroy(parser);

        bool has_more_results = false;
        aws_mutex_lock(&paginator->shared_mt_state.lock);
        has_more_results = paginator->shared_mt_state.has_more_results;
        aws_mutex_unlock(&paginator->shared_mt_state.lock);

        if (has_more_results) {
            s_set_paginator_state_if_legal(paginator, OS_INITIATED, OS_NOT_STARTED);
        } else {
            s_set_paginator_state_if_legal(paginator, OS_INITIATED, OS_COMPLETED);
        }

    } else {
        s_set_paginator_state_if_legal(paginator, OS_INITIATED, OS_ERROR);
    }

    if (paginator->on_list_finished) {
        paginator->on_list_finished(paginator, meta_request_result->error_code, paginator->user_data);
    }

    aws_s3_paginator_release(paginator);
}

int aws_s3_paginator_continue(struct aws_s3_paginator *paginator,
                              const struct aws_signing_config_aws *signing_config) {
    AWS_PRECONDITION(paginator);

    if (s_set_paginator_state_if_legal(paginator, OS_NOT_STARTED, OS_INITIATED)) {
        return AWS_OP_ERR;
    }

    struct aws_byte_cursor s_path_start = aws_byte_cursor_from_c_str("/?list-type=2");
    struct aws_byte_buf request_path;
    aws_byte_buf_init_copy_from_cursor(&request_path, paginator->allocator, s_path_start);

    if (paginator->prefix) {
        struct aws_byte_cursor s_prefix = aws_byte_cursor_from_c_str("&prefix=");
        aws_byte_buf_append_dynamic(&request_path, &s_prefix);
        struct aws_byte_cursor s_prefix_val = aws_byte_cursor_from_string(paginator->prefix);
        aws_byte_buf_append_encoding_uri_param(&request_path, &s_prefix_val);
    }

    if (paginator->delimiter) {
        struct aws_byte_cursor s_delimiter = aws_byte_cursor_from_c_str("&delimiter=");
        aws_byte_buf_append_dynamic(&request_path, &s_delimiter);
        struct aws_byte_cursor s_delimiter_val = aws_byte_cursor_from_string(paginator->delimiter);
        aws_byte_buf_append_encoding_uri_param(&request_path, &s_delimiter_val);
    }

    aws_mutex_lock(&paginator->shared_mt_state.lock);
    if (paginator->shared_mt_state.continuation_token) {
        struct aws_byte_cursor s_continuation = aws_byte_cursor_from_c_str("&continuation-token=");
        aws_byte_buf_append_dynamic(&request_path, &s_continuation);
        struct aws_byte_cursor s_continuation_val =
            aws_byte_cursor_from_string(paginator->shared_mt_state.continuation_token);
        aws_byte_buf_append_encoding_uri_param(&request_path, &s_continuation_val);
    }
    aws_mutex_unlock(&paginator->shared_mt_state.lock);

    struct aws_http_message *list_bucket_v2_request = aws_http_message_new_request(paginator->allocator);
    aws_http_message_set_request_path(list_bucket_v2_request, aws_byte_cursor_from_buf(&request_path));

    aws_byte_buf_clean_up(&request_path);

    struct aws_byte_cursor host_cur = aws_byte_cursor_from_string(paginator->bucket_name);
    struct aws_byte_buf host_buf;
    aws_byte_buf_init_copy_from_cursor(&host_buf, paginator->allocator, host_cur);
    struct aws_byte_cursor period_cur = aws_byte_cursor_from_c_str(".");
    struct aws_byte_cursor endpoint_val = aws_byte_cursor_from_string(paginator->endpoint);
    aws_byte_buf_append_dynamic(&host_buf, &period_cur);
    aws_byte_buf_append_dynamic(&host_buf, &endpoint_val);

    struct aws_http_header host_header = {
        .name = aws_byte_cursor_from_c_str("host"),
        .value = aws_byte_cursor_from_buf(&host_buf),
    };

    aws_http_message_add_header(list_bucket_v2_request, host_header);

    struct aws_http_header accept_header = {
        .name = aws_byte_cursor_from_c_str("accept"),
        .value = aws_byte_cursor_from_c_str("application/xml"),
    };

    aws_http_message_add_header(list_bucket_v2_request, host_header);
    aws_http_message_add_header(list_bucket_v2_request, accept_header);

    aws_http_message_set_request_method(list_bucket_v2_request, aws_http_method_get);

    struct aws_s3_meta_request_options request_options = {
        .user_data = paginator,
        .signing_config = signing_config,
        .type = AWS_S3_META_REQUEST_TYPE_DEFAULT,
        .shutdown_callback = s_list_bucket_shutdown,
        .body_callback = s_list_bucket_receive_body_callback,
        .finish_callback = s_list_bucket_request_finished,
        .message = list_bucket_v2_request,
    };

    aws_s3_paginator_acquire(paginator);
    aws_byte_buf_reset(&paginator->result_body, false); // clear body of previous request
    paginator->current_request = aws_s3_client_make_meta_request(paginator->client, &request_options);
    aws_http_message_release(list_bucket_v2_request);

    if (!paginator->current_request) {
        s_set_paginator_state_if_legal(paginator, OS_INITIATED, OS_ERROR);
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}

struct list_bucket_wrapper {
    struct aws_s3_paginator *paginator;
    aws_s3_on_object_fn *on_object;
    aws_s3_on_object_list_finished *on_list_finished;
    struct aws_condition_variable c_var;
    void *user_data;
};

static bool s_run_all_on_object(const struct aws_s3_object_file_system_info *info, void *user_data) {
    struct list_bucket_wrapper *wrapper = user_data;

    if (wrapper->on_object) {
        return wrapper->on_object(info, wrapper->user_data);
    }

    return false;
}

static void s_run_all_on_object_list_finished(
    struct aws_s3_paginator *paginator,
    int error_code,
    void *user_data) {
    struct list_bucket_wrapper *wrapper = user_data;

    if (wrapper->on_list_finished) {
        wrapper->on_list_finished(paginator, error_code, wrapper->user_data);
    }

    aws_condition_variable_notify_one(&wrapper->c_var);
}

static bool s_run_all_completion_check(void *arg) {
    struct list_bucket_wrapper *wrapper = arg;
    return wrapper->paginator->shared_mt_state.operation_state >= OS_COMPLETED
        //&& wrapper->paginator->shared_mt_state.has_more_results == false
        ;
}

int aws_s3_list_bucket_and_run_all_pages(
    struct aws_allocator *allocator,
    const struct aws_s3_list_bucket_v2_params *params,
    struct aws_signing_config_aws *signing_config) {
    struct list_bucket_wrapper wrapper = {
        .user_data = params->user_data,
        .on_object = params->on_object,
        .on_list_finished = params->on_list_finished,
        .c_var = AWS_CONDITION_VARIABLE_INIT,
    };

    struct aws_s3_list_bucket_v2_params params_cpy = *params;
    params_cpy.on_object = s_run_all_on_object;
    params_cpy.on_list_finished = s_run_all_on_object_list_finished;
    params_cpy.user_data = &wrapper;

    wrapper.paginator = aws_s3_initiate_list_bucket(allocator, &params_cpy);

    if (!wrapper.paginator) {
        return AWS_OP_ERR;
    }

    bool continue_paging = true;
    int ret_val = AWS_OP_SUCCESS;

    while (continue_paging && ret_val == AWS_OP_SUCCESS) {

        if (aws_s3_paginator_continue(wrapper.paginator, signing_config)) {
            continue_paging = false;
            ret_val = AWS_OP_ERR;
        } else {
            aws_mutex_lock(&wrapper.paginator->shared_mt_state.lock);
            aws_condition_variable_wait_pred(
                &wrapper.c_var, &wrapper.paginator->shared_mt_state.lock, s_run_all_completion_check, &wrapper);
            continue_paging = wrapper.paginator->shared_mt_state.has_more_results;
            aws_mutex_unlock(&wrapper.paginator->shared_mt_state.lock);
        }
    }

    aws_s3_paginator_release(wrapper.paginator);
    return ret_val;
}

int aws_s3_paginator_has_more_results(const struct aws_s3_paginator *paginator, bool* has_more_results) {
    AWS_PRECONDITION(paginator);
    AWS_PRECONDITION(has_more_results);
    *has_more_results = paginator->shared_mt_state.has_more_results;
    return AWS_OP_SUCCESS;
}