# GET Hedging

## Overview

A part request can be slow and not fail. It waits for a slow connection, a congested path, or a slow S3 server. A retry does not help, because a retry waits for an error that does not come. GET hedging cuts this tail latency. If a part request of an `AWS_S3_META_REQUEST_TYPE_GET_OBJECT` meta request gets no response before a deadline, the client cancels the request and sends it again at once on a new connection.

GET hedging is off by default. To turn it on, set `aws_s3_client_config.get_object_hedging` to `AWS_TRIBOOL_TRUE`, or set the `AWS_CRT_S3_GET_OBJECT_HEDGING` environment variable to `true`.

The design follows [the hedging design of the AWS S3 Transfer Manager for Rust](https://github.com/awslabs/aws-s3-transfer-manager-rs/blob/main/docs/design/hedging.md). It uses the same constants as the UploadPart timeout of this library (`aws_s3_client_update_upload_part_timeout`), and the 5% limit of speculative load from Dean and Barroso, [*The Tail at Scale*](https://research.google/pubs/the-tail-at-scale/).

## What the deadline measures

The deadline applies to the time to first byte (TTFB): the time from the end of the request send to the first byte of the response. The client uses the `response_first_byte_timeout_ms` option of aws-c-http. Thus, the time that a request waits in the client queue, for memory, or for a connection does not count. The body transfer after the first byte does not count either. The connection monitoring options (`monitoring_options`) find a body that stops.

When the deadline expires, aws-c-http closes the connection with `AWS_ERROR_HTTP_RESPONSE_FIRST_BYTE_TIMEOUT`. The client then sends the request again on a different connection.

## The adaptive deadline

The deadline is the same for all GET part requests of a client. The controller in `source/s3_get_hedging.c` calculates it in three phases:

1. **Warmup.** The controller collects the TTFB of the first 10 successful requests. It arms no deadline.
2. **Seed.** If the arithmetic mean of the 10 samples is 5 s or more, the controller stops for the life of the client. On such a slow link, a new request is as slow as the first request. Otherwise, the deadline starts at `max(p90, 1 s)`. The p90 puts the deadline above the slow tail of a link with high variance. The arithmetic mean, not the moving average, decides the stop, so that one slow cold start does not stop hedging on a healthy link.
3. **Steady state.** Each success ages the TTFB mean (EWMA, alpha 1/64) and moves the deadline toward the mean plus 700 ms (EWMA, alpha 1/100). The deadline moves slowly, so that a short spike does not cause a wave of hedges.

A hedge does not change the mean or the deadline. A cancelled request has no real TTFB. If the deadline were used as its value, each hedge would make the deadline smaller, and a smaller deadline would cause more hedges.

## The hedge budget

The budget decides if the client can afford a hedge now. The budget starts full with 10 tokens. A success adds 0.05 tokens, and a hedge removes 1 token. The client arms the deadline only while 1 token or more is available.

Thus, hedges stay below approximately 4.8% (0.05 / 1.05) of the requests. When few requests are slow, the budget stays almost full. When many requests are slow at the same time, the hedges remove tokens faster than the successes add them. The budget becomes empty and hedging stops until successes refill it. Successes, not time, refill the budget. Thus, a broad slowdown stops the hedges, and does not double the load.

The client checks the budget when it sends a request, and removes a token only when a hedge occurs. Thus, a wave of slow requests can cause more hedges at the same time than the budget has tokens. After the wave, the budget is empty.

## Hedges and retries

- A hedge does not go through the retry strategy. It does not wait for a backoff, does not use the retry quota, and does not count against the max retries.
- The client hedges each part request one time at most. The later attempts of a part request have no deadline. Errors of those attempts go through the retry strategy as usual.
- The `max` value of the `amz-sdk-request` header includes the hedge.
- Hedging is off if retries are off (`AWS_S3_RETRY_MODE_NONE`).

## Monitoring

The request metrics of the `telemetry_callback` show each attempt:

- `aws_s3_request_metrics_get_first_byte_timeout_ms()` gives the deadline of the attempt. The value is 0 during the warmup, after a stop, while the budget is empty, and for an attempt after a hedge.
- `aws_s3_request_metrics_get_error_code()` gives `AWS_ERROR_HTTP_RESPONSE_FIRST_BYTE_TIMEOUT` for an attempt that reached the deadline. For a GetObject attempt, this error code always identifies a hedge. The next attempt of the same part has an `aws_s3_request_metrics_get_retry_attempt()` value that is 1 more.

Useful values:

- The hedge rate: GetObject attempts with `AWS_ERROR_HTTP_RESPONSE_FIRST_BYTE_TIMEOUT`, divided by all GetObject attempts. A rate that stays near 4.8% shows that the budget is empty, which occurs during a broad slowdown.
- The deadline over time, from the first-byte timeout of the GetObject attempts.
- The time to first byte of the attempts after a hedge, compared with the deadline. This shows if the hedges help.

The client also logs at the INFO level when the warmup is complete, with the start value of the deadline, and when it stops hedging. aws-c-http logs each hedge at the INFO level, with the deadline (`Timed out waiting for first byte of HTTP response`).
