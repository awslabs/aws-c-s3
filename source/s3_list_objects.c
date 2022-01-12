/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/s3/private/s3_list_objects.h>
#include <aws/s3/private/s3_util.h>

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
    /** The current, in-flight paginated request to s3. */
    struct aws_atomic_var current_request;
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

    struct aws_s3_meta_request *previous_request = aws_atomic_exchange_ptr(&paginator->current_request, NULL);
    if (previous_request != NULL) {
        aws_s3_meta_request_release(previous_request);
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

    aws_byte_buf_clean_up(&paginator->result_body);

    aws_mem_release(paginator->allocator, paginator);
}

struct aws_s3_paginator *aws_s3_initiate_list_objects(
    struct aws_allocator *allocator,
    const struct aws_s3_list_objects_params *params) {
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
    aws_atomic_init_ptr(&paginator->current_request, NULL);
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

struct fs_parser_wrapper {
    struct aws_allocator *allocator;
    struct aws_s3_object_info fs_info;
};

/* invoked when the ListBucketResult/Contents node is iterated. */
static bool s_on_contents_node(struct aws_xml_parser *parser, struct aws_xml_node *node, void *user_data) {
    struct fs_parser_wrapper *fs_wrapper = user_data;
    struct aws_s3_object_info *fs_info = &fs_wrapper->fs_info;

    /* for each Contents node, get the info from it and send it off as an object we've encountered */
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
            fs_info->size = strtoull((const char *)size_str->bytes, NULL, 10);
            aws_string_destroy(size_str);
            return true;
        }
    }

    return true;
}

/* invoked when the ListBucketResult/CommonPrefixes node is iterated. */
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
        /* this will traverse the current Contents node, get the metadata necessary to construct
         * an instance of fs_info so we can invoke the callback on it. This happens once per object. */
        bool ret_val = aws_xml_node_traverse(parser, node, s_on_contents_node, &fs_wrapper) == AWS_OP_SUCCESS;

        if (paginator->prefix && !fs_wrapper.fs_info.prefix.len) {
            fs_wrapper.fs_info.prefix = aws_byte_cursor_from_string(paginator->prefix);
        }

        struct aws_byte_buf trimmed_etag;
        AWS_ZERO_STRUCT(trimmed_etag);

        if (fs_wrapper.fs_info.e_tag.len) {
            struct aws_string *quoted_etag_str =
                aws_string_new_from_cursor(fs_wrapper.allocator, &fs_wrapper.fs_info.e_tag);
            replace_quote_entities(fs_wrapper.allocator, quoted_etag_str, &trimmed_etag);
            fs_wrapper.fs_info.e_tag = aws_byte_cursor_from_buf(&trimmed_etag);
            aws_string_destroy(quoted_etag_str);
        }

        if (ret_val && paginator->on_object) {
            ret_val |= paginator->on_object(&fs_wrapper.fs_info, paginator->user_data);
        }

        if (trimmed_etag.len) {
            aws_byte_buf_clean_up(&trimmed_etag);
        }

        return ret_val;
    }

    if (aws_byte_cursor_eq_c_str_ignore_case(&node_name, "CommonPrefixes")) {
        /* this will traverse the current CommonPrefixes node, get the metadata necessary to construct
         * an instance of fs_info so we can invoke the callback on it. This happens once per prefix. */
        bool ret_val = aws_xml_node_traverse(parser, node, s_on_common_prefixes_node, &fs_wrapper) == AWS_OP_SUCCESS;

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

/**
 * On a successful operation, this is an xml document. Just copy the buffers over until we're ready to parse (upon
 * completion) of the response body.
 */
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

        /* clears previous continuation token */
        aws_mutex_lock(&paginator->shared_mt_state.lock);
        if (paginator->shared_mt_state.continuation_token) {
            aws_string_destroy(paginator->shared_mt_state.continuation_token);
            paginator->shared_mt_state.continuation_token = NULL;
            paginator->shared_mt_state.has_more_results = false;
        }
        aws_mutex_unlock(&paginator->shared_mt_state.lock);

        /* we've got a full xml document now and the request succeeded, parse the document and fire all the callbacks
         * for each object and prefix. All of that happens in these three lines. */
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

    /* this ref count was done right before we kicked off the request to keep the paginator object alive. Release it now
     * that the operation has completed. */
    aws_s3_paginator_release(paginator);
}

int aws_s3_paginator_continue(struct aws_s3_paginator *paginator, const struct aws_signing_config_aws *signing_config) {
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
        aws_byte_buf_append_dynamic(&request_path, &s_delimiter_val);
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

    struct aws_http_message *list_objects_v2_request = aws_http_message_new_request(paginator->allocator);
    aws_http_message_set_request_path(list_objects_v2_request, aws_byte_cursor_from_buf(&request_path));

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

    aws_http_message_add_header(list_objects_v2_request, host_header);
    aws_byte_buf_clean_up(&host_buf);

    struct aws_http_header accept_header = {
        .name = aws_byte_cursor_from_c_str("accept"),
        .value = aws_byte_cursor_from_c_str("application/xml"),
    };

    aws_http_message_add_header(list_objects_v2_request, accept_header);

    aws_http_message_set_request_method(list_objects_v2_request, aws_http_method_get);

    struct aws_s3_meta_request_options request_options = {
        .user_data = paginator,
        .signing_config = (struct aws_signing_config_aws *)signing_config,
        .type = AWS_S3_META_REQUEST_TYPE_DEFAULT,
        .body_callback = s_list_bucket_receive_body_callback,
        .finish_callback = s_list_bucket_request_finished,
        .message = list_objects_v2_request,
    };

    /* re-use the current buffer. */
    aws_byte_buf_reset(&paginator->result_body, false);

    /* we're kicking off an asynchronous request. ref-count the paginator to keep it alive until we finish. */
    aws_s3_paginator_acquire(paginator);

    struct aws_s3_meta_request *previous_request = aws_atomic_exchange_ptr(&paginator->current_request, NULL);
    if (previous_request != NULL) {
        /* release request from previous page */
        aws_s3_meta_request_release(previous_request);
    }

    struct aws_s3_meta_request *new_request = aws_s3_client_make_meta_request(paginator->client, &request_options);
    aws_atomic_store_ptr(&paginator->current_request, new_request);

    /* make_meta_request() above, ref counted the http request, so go ahead and release */
    aws_http_message_release(list_objects_v2_request);

    if (new_request == NULL) {
        s_set_paginator_state_if_legal(paginator, OS_INITIATED, OS_ERROR);
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}

bool aws_s3_paginator_has_more_results(const struct aws_s3_paginator *paginator) {
    AWS_PRECONDITION(paginator);
    bool has_more_results = false;
    struct aws_s3_paginator *paginator_mut = (struct aws_s3_paginator *)paginator;
    aws_mutex_lock(&paginator_mut->shared_mt_state.lock);
    has_more_results = paginator->shared_mt_state.has_more_results;
    aws_mutex_unlock(&paginator_mut->shared_mt_state.lock);
    return has_more_results;
}
