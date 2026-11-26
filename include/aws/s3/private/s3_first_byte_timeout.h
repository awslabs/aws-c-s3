#ifndef AWS_S3_FIRST_BYTE_TIMEOUT_H
#define AWS_S3_FIRST_BYTE_TIMEOUT_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/s3/s3.h>

#include <aws/common/priority_queue.h>

AWS_PUSH_SANE_WARNING_LEVEL

/* The minimum number of successful requests that the controller collects before it sets a timeout. */
#define AWS_S3_FIRST_BYTE_TIMEOUT_MIN_WARMUP_SAMPLES 10

/**
 * The adaptive first-byte timeout for one type of request.
 *
 * Sometimes, S3 is extremely slow responding to an upload.
 * In these cases, it's much faster to cancel and resend the upload,
 * vs waiting 5sec for the slow response.
 *
 * Typically, S3 responds to an upload in 0.2sec after the request is fully received.
 * But occasionally (about 0.1%) it takes 5sec to respond.
 * In a large 30GiB file upload, you can expect about 4 parts to suffer from
 * a slow response. If one of these parts is near the end of the file,
 * then we end up sitting around doing nothing for up to 5sec, waiting
 * for this final slow upload to complete.
 *
 * We use the response_first_byte_timeout HTTP option to cancel uploads
 * suffering from a slow response. But how should we set it? A fast 100Gbps
 * machine definitely wants it! But a slow computer does not. A slow computer
 * would be better off waiting 5sec for the response, vs re-uploading the whole request.
 *
 * The current algorithm:
 * 1. Start without a timeout value. After max(10, # of ideal connections) requests completed, we know the average of
 *      how long the request takes. We decide if it's worth to set a timeout value or not. (If the average of request
 *      takes more than 5 secs or not).
 * 2. If it is worth to retry, start with a timeout value from max(1 sec, P90 of the initial samples).
 * 3. If a request finishes successfully, use the average response_to_first_byte_time + g_expect_timeout_offset_ms as
 *      our expected timeout value. (TODO: The real expected timeout value should be a P99 of all the requests.)
 *  3.1 Adjust the current timeout value against the expected timeout value, via 0.99 * <current timeout> + 0.01 *
 *      <expected timeout> to get closer to the expected timeout value.
 * 4. If request had timed out. We check the timeout rate.
 *  4.1 If timeout rate is larger than 0.1%, we increase the timeout value by 100ms (Check the timeout value when the
 *      request was made, if the updated timeout value is larger than the expected, skip update).
 *  4.2 If timeout rate is larger than 1%, we increase the timeout value by 1 secs (If needed). And clear the rate
 *      to get the exact rate with new timeout value.
 *  4.3 Once the timeout value is larger than 5 secs, we stop the process.
 *
 * The controller does not use the client. The caller gives it the results of the requests, and sends the next
 * requests with `timeout_ms`. The controller is not thread safe. The caller must hold a lock.
 */
struct aws_s3_first_byte_timeout_controller {
    struct aws_allocator *allocator;

    /* The number of successful requests that the controller collects before it decides to set a timeout or not. */
    uint32_t num_warmup_samples;

    bool stop_timeout;

    /* The first-byte timeout in ms for the next request. Zero if the next request has no timeout. */
    size_t timeout_ms;

    /* Total number of successful requests */
    uint64_t num_successful_requests;

    /* Stats for the request time of the warmup requests */
    struct {
        uint64_t sum_ns;
        uint64_t num_samples;
        bool collecting_p90;
        struct aws_priority_queue p90_samples;
    } initial_request_time;

    /* Track the timeout rate. */
    struct {
        uint64_t num_completed;
        uint64_t num_failed;
    } timeout_rate_tracking;

    /* Stats for the response to first byte time of tracked succeed requests */
    struct {
        uint64_t sum_ns;
        uint64_t num_samples;
    } response_to_first_byte_time;
};

AWS_EXTERN_C_BEGIN

AWS_S3_API
extern const size_t g_expect_timeout_offset_ms;

/**
 * Initializes the controller without a timeout. The controller collects `num_warmup_samples` successful requests
 * before it sets a timeout. `num_warmup_samples` must be AWS_S3_FIRST_BYTE_TIMEOUT_MIN_WARMUP_SAMPLES or more.
 */
AWS_S3_API
void aws_s3_first_byte_timeout_controller_init(
    struct aws_s3_first_byte_timeout_controller *controller,
    struct aws_allocator *allocator,
    uint32_t num_warmup_samples);

AWS_S3_API
void aws_s3_first_byte_timeout_controller_clean_up(struct aws_s3_first_byte_timeout_controller *controller);

/**
 * Records a successful request. The warmup uses `request_time_ns` to decide if a timeout is necessary, and to set the
 * first timeout. After the warmup, the controller moves the timeout toward the mean of `response_to_first_byte_time_ns`
 * plus g_expect_timeout_offset_ms.
 */
AWS_S3_API
void aws_s3_first_byte_timeout_controller_record_success(
    struct aws_s3_first_byte_timeout_controller *controller,
    uint64_t request_time_ns,
    uint64_t response_to_first_byte_time_ns);

/**
 * Records a request that reached its first-byte timeout. `request_timeout_ms` is the timeout that the request used.
 * Returns true if the timeout rate is more than 1%.
 */
AWS_S3_API
bool aws_s3_first_byte_timeout_controller_record_timeout(
    struct aws_s3_first_byte_timeout_controller *controller,
    size_t request_timeout_ms);

AWS_EXTERN_C_END
AWS_POP_SANE_WARNING_LEVEL

#endif /* AWS_S3_FIRST_BYTE_TIMEOUT_H */
