

#include "cli_progress_bar.h"

#include <aws/common/thread_scheduler.h>
#include <aws/common/thread.h>
#include <aws/common/task_scheduler.h>
#include <aws/common/mutex.h>
#include <aws/common/condition_variable.h>
#include <aws/common/clock.h>

struct update_data {
    struct progress_listener_group *group;
    struct aws_thread_scheduler *scheduler;
    size_t expected;
    size_t completed;
    struct aws_condition_variable c_var;
};


struct listener_update_data {
    struct progress_listener *listener;
    struct update_data *update_data;
    size_t current_progress;
    size_t final_progress;
};

void s_update_task(struct aws_task * task, void *arg, enum aws_task_status status) {
    (void)task;
    (void)status;

    struct listener_update_data *update_data = arg;

    ++update_data->current_progress;

    if (update_data->current_progress <= update_data->final_progress) {
        progress_listener_update_progress(update_data->listener, 1);
        progress_listener_group_render(update_data->update_data->group);

        if (update_data->update_data->completed < update_data->update_data->expected) {
            struct aws_task *re_update_task = aws_mem_calloc(aws_default_allocator(), 1, sizeof(struct aws_task));
            re_update_task->fn = s_update_task;
            re_update_task->arg = update_data;
            uint64_t current_time = 0;
            aws_high_res_clock_get_ticks(&current_time);
            /* make them not all update at the same time. */
            int rando = rand();
            int random = rando % 100 == 0 ? 99: rando;
            random = random > 0 ? random % 100 : -random %100;
            current_time += AWS_TIMESTAMP_NANOS /  random;
            aws_thread_scheduler_schedule_future(update_data->update_data->scheduler, re_update_task, current_time);
        }
    } else {
        struct aws_string *state = aws_string_new_from_c_str(aws_default_allocator(), "(Success!)");
        progress_listener_update_state(update_data->listener, state);
        progress_listener_group_render(update_data->update_data->group);
        update_data->update_data->completed+= 1;
        aws_string_destroy(state);

        if (update_data->update_data->completed >= update_data->update_data->expected) {
            aws_condition_variable_notify_one(&update_data->update_data->c_var);
        }

        aws_mem_release(aws_default_allocator(), update_data);
    }

    aws_mem_release(aws_default_allocator(), task);
}

static bool s_is_done(void *arg) {
    struct update_data *update_data = arg;
    return update_data->completed >= update_data->expected;
}

int main(void) {
    struct aws_allocator *allocator = aws_default_allocator();

    struct progress_listener_group *listener_group = progress_listener_group_new(allocator);

    struct aws_thread_options options;
    AWS_ZERO_STRUCT(options);

    struct aws_thread_scheduler *scheduler = aws_thread_scheduler_new(allocator, &options);

    struct update_data *update_data = aws_mem_calloc(allocator, 1, sizeof(struct update_data));
    update_data->c_var = (struct aws_condition_variable)AWS_CONDITION_VARIABLE_INIT;
    update_data->expected = 10;

    for (size_t i = 0; i < update_data->expected; ++i) {
        struct listener_update_data *listener_update_data =
            aws_mem_calloc(allocator, 1, sizeof(struct listener_update_data));
        listener_update_data->update_data = update_data;

        char label_buf[1024] = {0};
        sprintf(label_buf, "label %d", (int)i);
        struct aws_string *label = aws_string_new_from_c_str(allocator, label_buf);
        struct aws_string *state = aws_string_new_from_c_str(allocator, "(In Progress)");
        listener_update_data->listener = progress_listener_new(listener_group, label, state, 300);
        aws_string_destroy(state);
        aws_string_destroy(label);
        listener_update_data->final_progress = 300;

        update_data->scheduler = scheduler;
        update_data->group = listener_group;

        struct aws_task *task = aws_mem_calloc(allocator, 1, sizeof(struct aws_task));
        task->fn = s_update_task;
        task->arg = listener_update_data;

        aws_thread_scheduler_schedule_now(scheduler, task);
    }

    struct aws_mutex mutex = AWS_MUTEX_INIT;
    aws_mutex_lock(&mutex);
    aws_condition_variable_wait_pred(&update_data->c_var, &mutex, s_is_done, update_data);
    aws_mutex_unlock(&mutex);

    return 0;
}
