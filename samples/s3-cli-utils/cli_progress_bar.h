/**
* Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
* SPDX-License-Identifier: Apache-2.0.
*/

#include <aws/common/common.h>
#include <aws/common/string.h>

struct progress_listener_group;
struct progress_listener;

struct progress_listener_group *progress_listener_group_new(struct aws_allocator *allocator);
void progress_listener_group_delete(struct progress_listener_group *group);
void progress_listener_group_render(struct progress_listener_group *group);

struct progress_listener *progress_listener_new(struct progress_listener_group *group, struct aws_string *label, struct aws_string *state_name, uint64_t max_value);
void progress_listener_update_state(struct progress_listener *listener, struct aws_string *state_name);

void progress_listener_update_progress(struct progress_listener *listener, uint64_t progress_update);
void progress_listener_update_label(struct progress_listener *listener, struct aws_string *new_label);
void progress_listener_render(struct progress_listener *listener);

