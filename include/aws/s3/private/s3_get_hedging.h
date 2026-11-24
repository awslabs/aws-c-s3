#ifndef AWS_S3_GET_HEDGING_H
#define AWS_S3_GET_HEDGING_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/s3/s3.h>

AWS_PUSH_SANE_WARNING_LEVEL

/* Number of successful GET requests that the controller collects before it arms a deadline. */
#define AWS_S3_GET_HEDGING_WARMUP_SAMPLES 10

/**
 * The adaptive first-byte deadline and the hedge budget for GET part requests.
 *
 * If a GET part request gets no response before the deadline, the client cancels it and sends it again on a new
 * connection. This is a hedge. The controller sets the deadline and decides if the client can afford a hedge now.
 * The time to first byte (TTFB) is the time from the end of the request send to the first byte of the response.
 *
 * 1. Warmup: the controller collects the TTFB of the first AWS_S3_GET_HEDGING_WARMUP_SAMPLES successes. It arms no
 *    deadline.
 * 2. Seed: if the arithmetic mean of the warmup samples is 5 s or more, the controller stops for its full life. On
 *    such a slow link, a new request is as slow as the first request. Otherwise, the deadline is max(p90, 1 s). The
 *    p90 puts the deadline above the slow tail of a link with high variance.
 * 3. Steady state: each success ages the TTFB mean (EWMA, alpha 1/64) and moves the deadline toward the mean plus
 *    700 ms (EWMA, alpha 1/100). A hedge does not change the mean or the deadline. A cancelled request has no real
 *    TTFB, and a value that replaces it would make the deadline smaller after each hedge.
 *
 * The budget starts full with 10 tokens. A success adds 0.05 tokens, and a hedge removes 1 token. The controller
 * arms the deadline only while 1 token or more is available. Thus, hedges stay below approximately 4.8% of the
 * requests (0.05 / 1.05). Successes, not time, refill the budget. Thus, a broad slowdown empties the budget and stops
 * the hedges, and does not double the load.
 *
 * The controller is not thread safe. The caller must hold a lock.
 */
struct aws_s3_get_hedging_controller {
    uint64_t warmup_samples_ns[AWS_S3_GET_HEDGING_WARMUP_SAMPLES];
    size_t num_warmup_samples;

    /* True after the seed. */
    bool warm;

    /* True if the warmup mean was too slow. Then the controller never arms a deadline. */
    bool stopped;

    uint64_t mean_ttfb_ns;
    uint64_t deadline_ns;

    /* The budget, in 1/100 tokens. Integer units make the budget arithmetic exact. */
    uint32_t hedge_centitokens;

    uint64_t num_successes;
    uint64_t num_hedges;
};

AWS_EXTERN_C_BEGIN

AWS_S3_API
void aws_s3_get_hedging_controller_init(struct aws_s3_get_hedging_controller *controller);

AWS_S3_API
void aws_s3_get_hedging_controller_record_success(struct aws_s3_get_hedging_controller *controller, uint64_t ttfb_ns);

AWS_S3_API
void aws_s3_get_hedging_controller_record_hedge(struct aws_s3_get_hedging_controller *controller);

/**
 * Returns the first-byte deadline in milliseconds for the next GET part request. Returns 0 if the request must have no
 * deadline: during the warmup, after a stop, or while the budget has less than 1 token.
 */
AWS_S3_API
uint64_t aws_s3_get_hedging_controller_deadline_ms(const struct aws_s3_get_hedging_controller *controller);

AWS_EXTERN_C_END
AWS_POP_SANE_WARNING_LEVEL

#endif /* AWS_S3_GET_HEDGING_H */
