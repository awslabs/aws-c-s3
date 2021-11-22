/**
* Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
* SPDX-License-Identifier: Apache-2.0.
 */
#include "cli_progress_bar.h"

#include <aws/common/array_list.h>
#include <aws/common/mutex.h>

#include <inttypes.h>

struct progress_listener_group {
    struct aws_allocator *allocator;
    struct aws_array_list listeners;
    struct aws_mutex mutex;
    FILE *render_sink;
};

struct progress_listener {
    struct progress_listener_group *owning_group;
    struct aws_string *label;
    struct aws_mutex mutex;
    uint64_t max;
    uint64_t current;
    bool render_update_pending;
};

static void s_progress_listener_delete(struct progress_listener *listener) {
    aws_string_destroy(listener->label);
    aws_mutex_clean_up(&listener->mutex);
    aws_mem_release(listener->owning_group->allocator, listener);
}

struct progress_listener_group *progress_listener_group_new(struct aws_allocator *allocator) {
    struct progress_listener_group *group = aws_mem_calloc(allocator, 1, sizeof(struct progress_listener_group));

    aws_mutex_init(&group->mutex);
    group->render_sink = stdout;
    aws_array_list_init_dynamic(&group->listeners, allocator, 16, sizeof(struct progress_listener));

    return group;
}

void progress_listener_group_delete(struct progress_listener_group *group) {
    size_t listeners_len = aws_array_list_length(&group->listeners);
    for (size_t i = 0; i < listeners_len; ++i) {
        struct progress_listener *listener;
        aws_array_list_get_at(&group->listeners, (void **)&listener, i);
        s_progress_listener_delete(listener);
    }
    aws_array_list_clean_up(&group->listeners);
    aws_mutex_clean_up(&group->mutex);
    aws_mem_release(group->allocator, group);
}

void progress_listener_group_render(struct progress_listener_group *group) {
    aws_mutex_lock(&group->mutex);
    size_t listeners_len = aws_array_list_length(&group->listeners);

    size_t lines_per_render = 3;
    size_t lines_render_count = 1;

    for (size_t i = listeners_len - 1; i >= 0; --i) {
        size_t line_skip = lines_per_render * lines_render_count++;

        struct progress_listener *listener;
        aws_array_list_get_at(&group->listeners, (void **)&listener, i);

        aws_mutex_lock(&listener->mutex);

        if (listener->render_update_pending) {
            /* move from the bottom up to the row we need. */
            fprintf(group->render_sink, "\033[<%zu>A", line_skip);
            progress_listener_render(listener);
            listener->render_update_pending = false;
            /* now go back so the next tick gets the same offset to work from. */
            fprintf(group->render_sink, "\033[<%zu>B", line_skip - lines_per_render);
        }
        aws_mutex_unlock(&listener->mutex);
    }
    
    aws_mutex_unlock(&group->mutex);
}

struct progress_listener *progress_listener_new(struct progress_listener_group *group, struct aws_string *label, uint64_t max_value) {
    struct progress_listener *listener = aws_mem_calloc(group->allocator, 1, sizeof(struct progress_listener));

    aws_mutex_init(&listener->mutex);
    listener->max = max_value;
    listener->current = 0;
    listener->label = aws_string_clone_or_reuse(group->allocator, label);
    listener->owning_group = group;
    listener->render_update_pending = false;

    aws_mutex_lock(&group->mutex);
    aws_array_list_push_back(&group->listeners, &listener);
    progress_listener_render(listener);
    aws_mutex_unlock(&group->mutex);

    return listener;
}

void progress_listener_update(struct progress_listener *listener, uint64_t progress_update) {
    aws_mutex_lock(&listener->mutex);
    listener->current += progress_update;
    listener->render_update_pending = true;
    aws_mutex_unlock(&listener->mutex);
}

void progress_listener_render(struct progress_listener *listener) {
    struct progress_listener_group *group = listener->owning_group;

    fprintf(group->render_sink, "%s\n", aws_string_c_str(listener->label));

    size_t completion = (size_t)(((double)listener->current / (double)listener->max) * 100);

    size_t ticks = 50;
    size_t completed_ticks = completion / ticks;

    fprintf(group->render_sink, "  [");

    for(size_t i = 0; i < ticks; ++i) {
        if (completed_ticks > i) {
            fprintf(group->render_sink, "=");
        } else {
            fprintf(group->render_sink, "-");
        }
    }

    fprintf(group->render_sink, "]");
    fprintf(group->render_sink, " %zu%%\n\n", completion);
}
