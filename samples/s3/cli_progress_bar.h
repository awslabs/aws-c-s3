#ifndef CLI_PROGRESS_BAR_H
#define CLI_PROGRESS_BAR_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/common.h>
#include <aws/common/string.h>

struct progress_listener_group;
struct progress_listener;

/**
 * Creates parent container for progress bars. It's rendered as a single block, and in order to work correctly
 * must be the last thing currently rendered on the terminal. It will render all progress bars at ~25 FPS
 * if you call progress_listener_group_run_background_render_thread(). Otherwise, you can always call
 * progress_listener_group_render() manually.
 */
struct progress_listener_group *progress_listener_group_new(struct aws_allocator *allocator);

/**
 * Wait on any background thread resources to clean up, then delete the group.
 */
void progress_listener_group_delete(struct progress_listener_group *group);

/**
 * Render the current state of the progress bars in this group. Please keep in mind. This works as long as this is the
 * last block of text currently rendered on the terminal (the cursor position should be immediately after the last line
 * of this group.
 */
void progress_listener_group_render(struct progress_listener_group *group);

/**
 * Initiates a background thread to run progress_listener_group_render at ~25 FPS
 */
void progress_listener_group_run_background_render_thread(struct progress_listener_group *group);

/**
 * Creates a new progress bar and returns a listener back for updating state, labels, and progress.
 * @param group group to render the progress bar into.
 * @param label label (what are you tracking progress for?)
 * @param state_name name of the state (In progress, success, failed etc...).
 * @param max_value The 100% value of the progress you're tracking
 */
struct progress_listener *progress_listener_new(
    struct progress_listener_group *group,
    struct aws_string *label,
    struct aws_string *state_name,
    uint64_t max_value);

/**
 * Update the state of the progress bar.
 */
void progress_listener_update_state(struct progress_listener *listener, struct aws_string *state_name);

/**
 * Update the progress of the progress bar.
 * @param progress_update amount to increment the progress by.
 */
void progress_listener_update_progress(struct progress_listener *listener, uint64_t progress_update);

void progress_listener_reset_progress(struct progress_listener *listener);

void progress_listener_update_max_value(struct progress_listener *listener, uint64_t max_value);

/**
 * Update the label for the progress bar.
 */
void progress_listener_update_label(struct progress_listener *listener, struct aws_string *new_label);

/**
 * Render just the bar. This will not render in place and you probably should rely on the group render
 * to handle this for you.
 */
void progress_listener_render(struct progress_listener *listener);

#endif /* CLI_PROGRESS_BAR_H */
