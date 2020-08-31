#include "aws/s3/private/s3_util.h"
#include <aws/http/request_response.h>
#include <aws/s3/s3.h>

const struct aws_byte_cursor g_host_header_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Host");
const struct aws_byte_cursor g_range_header_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Range");
const struct aws_byte_cursor g_etag_header_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("ETag");
const struct aws_byte_cursor g_content_length_header_name_name =
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Content-Length");
const struct aws_byte_cursor g_content_range_header_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Content-Range");
const struct aws_byte_cursor g_content_type_header_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Content-Type");
const struct aws_byte_cursor g_content_length_header_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Content-Length");

#include <aws/common/task_scheduler.h>
#include <aws/io/event_loop.h>
#include <stdarg.h>

/* Allocated work which essentially wraps a task. */
struct aws_s3_task_util_payload {

    struct aws_allocator *allocator;

    /* Function that will process the work. */
    aws_s3_task_util_task_fn *task_fn;

    /* The actual task itself. */
    struct aws_task task;
};

static struct aws_s3_task_util_payload *s_s3_task_util_payload_new(
    struct aws_allocator *allocator,
    aws_s3_task_util_task_fn task_fn,
    uint32_t num_args);

static void s_s3_task_util_payload_destroy(struct aws_s3_task_util_payload *payload);

static void s_s3_task_util_process_task(struct aws_task *task, void *arg, enum aws_task_status task_status);

/* Allocate a new task-util-work. */
static struct aws_s3_task_util_payload *s_s3_task_util_payload_new(
    struct aws_allocator *allocator,
    aws_s3_task_util_task_fn task_fn,
    uint32_t num_args) {

    size_t payload_size = sizeof(struct aws_s3_task_util_payload) + num_args * sizeof(void *);
    struct aws_s3_task_util_payload *payload = aws_mem_acquire(allocator, payload_size);

    if (payload == NULL) {
        AWS_LOGF_ERROR(AWS_LS_S3_GENERAL, "Could not allocate payload for task.");
        return NULL;
    }

    payload->allocator = allocator;
    payload->task_fn = task_fn;

    aws_task_init(&payload->task, s_s3_task_util_process_task, payload, "s3_task_util_process_task");

    return payload;
}

/* Destroys a task-util-work structure. */
static void s_s3_task_util_payload_destroy(struct aws_s3_task_util_payload *payload) {
    AWS_PRECONDITION(payload);
    AWS_PRECONDITION(payload->allocator);

    aws_mem_release(payload->allocator, payload);
}

/* Trigger an async operation, wrapping it in a task. */
int aws_s3_task_util_new_task(
    struct aws_allocator *allocator,
    struct aws_event_loop *event_loop,
    aws_s3_task_util_task_fn *task_fn,
    uint64_t delay_ns,
    uint32_t num_args,
    ...) {

    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(task_fn);

    struct aws_s3_task_util_payload *payload = s_s3_task_util_payload_new(allocator, task_fn, num_args);

    if (payload == NULL) {
        goto error_result;
    }

    void **payload_args = (void **)((uint8_t *)payload + sizeof(struct aws_s3_task_util_payload));

    va_list option_args;
    va_start(option_args, num_args);

    /* Copy our args out of the VA List. */
    for (uint32_t arg_index = 0; arg_index < num_args; ++arg_index) {
        payload_args[arg_index] = va_arg(option_args, void *);
    }

    va_end(option_args);

    if (delay_ns == 0) {
        aws_event_loop_schedule_task_now(event_loop, &payload->task);
    } else {
        uint64_t now = 0;

        if (aws_event_loop_current_clock_time(event_loop, &now)) {
            AWS_LOGF_ERROR(
                AWS_LS_S3_GENERAL,
                "aws_s3_task_util_new_task could not get current time for delayed task, not scheduling "
                "task.");

            goto error_clean_up_work;
        }

        aws_event_loop_schedule_task_future(event_loop, &payload->task, now + delay_ns);
    }

    return AWS_OP_SUCCESS;

error_clean_up_work:

    if (payload != NULL) {
        s_s3_task_util_payload_destroy(payload);
        payload = NULL;
    }

error_result:

    return AWS_OP_ERR;
}

/* Task that processes an individual task-util-work object. */
static void s_s3_task_util_process_task(struct aws_task *task, void *arg, enum aws_task_status task_status) {
    (void)task;

    struct aws_s3_task_util_payload *payload = arg;
    AWS_PRECONDITION(payload);

    if (task_status != AWS_TASK_STATUS_RUN_READY) {
        goto clean_up;
    }

    void **payload_args = (void **)((uint8_t *)payload + sizeof(struct aws_s3_task_util_payload));

    /* Call the user's passed in task function. */
    payload->task_fn(payload_args);

clean_up:
    /* Clean up the task work. */
    s_s3_task_util_payload_destroy(payload);
    payload = NULL;
}
