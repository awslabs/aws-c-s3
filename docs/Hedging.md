# First-Byte Timeout and Hedging

## Overview

A part request can be slow and not fail. It waits for a slow connection, a congested path, or a slow S3 server. A retry does not help, because a retry waits for an error that does not come. The first-byte timeout cuts this tail latency. If a part request gets no response before the timeout, the client cancels the request and sends it again at once on another connection. This new attempt is a hedge.

The client uses the first-byte timeout for two request types:

| | UploadPart | GetObject (GET hedging) |
|---|---|---|
| On or off | On. Off for S3 Express and if retries are off. | Off by default. Off if retries are off. |
| Cost of a hedge | The client sends the full part again. | Small. The client cancels the request before the body starts. |
| Warmup sample | The full request time | The time to first byte |

To turn on GET hedging, set `aws_s3_client_config.get_object_hedging` to `AWS_TRIBOOL_TRUE`, or set the `AWS_CRT_S3_GET_OBJECT_HEDGING` environment variable to `true`. GET hedging applies to the part requests of an `AWS_S3_META_REQUEST_TYPE_GET_OBJECT` meta request.

The client does not give S3 Express UploadPart requests a first-byte timeout. S3 Express sends a random ETag, and two uploads of the same part can make CompleteMultipartUpload fail ([#602](https://github.com/awslabs/aws-c-s3/pull/602)). GET hedging includes S3 Express, because a GET is idempotent.

The [S3 performance guidelines](https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance-design-patterns.html#optimizing-performance-timeouts-retries) recommend to track slow requests and to send them again. The budget follows [the hedging design of the AWS S3 Transfer Manager for Rust](https://github.com/awslabs/aws-s3-transfer-manager-rs/blob/main/docs/design/hedging.md), and the 5% limit of speculative load from Dean and Barroso, [*The Tail at Scale*](https://research.google/pubs/the-tail-at-scale/).

## What the timeout measures

The timeout applies to the time to first byte (TTFB): the time from the end of the request send to the first byte of the response. The client uses the `response_first_byte_timeout_ms` option of aws-c-http. Thus, the time that a request waits in the client queue, for memory, or for a connection does not count. The time to send an UploadPart body does not count. The body transfer after the first byte does not count either. The connection monitoring options (`monitoring_options`) find a body that stops.

aws-c-http records the end of the send in a task after the socket write completes. A fast server can answer before this task runs. For such a response, the TTFB sample is 0.

When the timeout expires, aws-c-http closes the connection with `AWS_ERROR_HTTP_RESPONSE_FIRST_BYTE_TIMEOUT`. The client then sends the request again on a different connection.

## The adaptive timeout

The client has one controller for UploadPart and one controller for GetObject (`source/s3_first_byte_timeout.c`). The two controllers do not share samples, because the response times of the two request types are very different. Each controller calculates the timeout for all requests of its type:

1. **Warmup.** The controller collects the first max(10, ideal connection count) successful requests. It sets no timeout.
2. **Seed.** If the arithmetic mean of the warmup samples is 5 s or more, the controller stops for the life of the client. On such a slow link, a new attempt is as slow as the first attempt. Otherwise, the timeout starts at `max(p90, 1 s)`. The p90 is the nearest-rank p90 of all warmup samples. It puts the timeout above the slow tail of a link with high variance. The arithmetic mean, not a moving average, decides the stop, so that one slow cold start does not stop the timeout on a healthy link.
3. **Steady state.** Each success updates the mean TTFB. The mean is the arithmetic mean of the first 64 samples, and after that an EWMA with alpha 1/64. Each success also moves the timeout 1% of the distance toward the mean plus 700 ms. The timeout moves slowly, so that a short spike does not cause a wave of hedges.
4. **Timeout rate.** If more than 0.1% of the requests reach the timeout, the timeout increases by 100 ms. If more than 1% reach it, the timeout increases by 1 s, and the count starts again. A request increases the timeout only if its own timeout was less than 100 ms (or 1 s) below the current timeout. Thus, a burst of requests that used the same timeout increases the timeout one time. If the timeout becomes more than 5 s, the controller stops for the life of the client.

A hedge never makes the timeout smaller. A cancelled request has no real TTFB. If the timeout were used as its value, each hedge would make the timeout smaller, and a smaller timeout would cause more hedges.

The warmup sample shows the cost of a new attempt. For UploadPart, a new attempt sends the full part again, so the warmup uses the full request time. A slow computer that needs more than 5 s for each part does not get a timeout. For GetObject, the client cancels the request before the body starts, so the warmup uses the TTFB.

## The hedge budget

The budget limits the number of hedges. The budget starts full with 10 tokens. A success adds 0.05 tokens, and a hedge removes 1 token. The client sets the timeout only while 1 token or more is available. The budget does not change the value of the timeout.

Thus, hedges stay below approximately 4.8% (0.05 / 1.05) of the requests. When few requests are slow, the budget stays almost full. When many requests are slow at the same time, the hedges remove tokens faster than the successes add them. The budget becomes empty, and the client sets no timeout until successes refill it. Successes, not time, refill the budget. Thus, a broad slowdown stops the hedges, and does not double the load.

The timeout rate and the budget do different things. The timeout rate moves the timeout up until approximately 0.1% to 1% of the requests reach it. The budget stops a burst. Many requests that use the same timeout can reach it at the same time. After 10 hedges in sequence, the budget is empty. The next requests have no timeout until 20 successes refill 1 token.

The client checks the budget when it sends a request, and removes a token only when a hedge occurs. Thus, a wave of slow requests can cause more hedges at the same time than the budget has tokens. After the wave, the budget is empty.

## Hedges and retries

- A hedge does not go through the retry strategy. It does not wait for a backoff, does not use the retry quota, and does not count against the max retries. This is also true for a retry strategy that the caller gives in `aws_s3_client_config.retry_strategy`. Such a strategy cannot refuse a hedge.
- In `AWS_S3_RETRY_MODE_ADAPTIVE`, a hedge also does not wait for the client-side rate limiter. While S3 throttles, the limiter delays the first attempts and the retries, or makes them fail at once if `fast_fail_if_no_token` is set. A hedge goes out at once. The hedge budget limits these hedges, as for all hedges.
- The client hedges each part request one time at most. The later attempts of a part request have no first-byte timeout. Errors of those attempts go through the retry strategy as usual. Thus, a part that is slow on each attempt does not use all of its retries for timeouts.
- The `max` value of the `amz-sdk-request` header includes the hedge.
- The first-byte timeout is off if retries are off (`AWS_S3_RETRY_MODE_NONE`).

## Monitoring

The request metrics of the `telemetry_callback` show each attempt:

- `aws_s3_request_metrics_get_first_byte_timeout_ms()` gives the timeout of the attempt. The value is 0 during the warmup, after a stop, while the budget is empty, and for an attempt after a hedge.
- `aws_s3_request_metrics_get_error_code()` gives `AWS_ERROR_HTTP_RESPONSE_FIRST_BYTE_TIMEOUT` for an attempt that reached the timeout. For UploadPart and GetObject attempts, this error code identifies a hedge, unless the meta request is already finishing, for example after a cancel or a failed part. In that case, the client does not send the request again, but the controller counts the timeout. The next attempt of the same part has an `aws_s3_request_metrics_get_retry_attempt()` value that is 1 more.

Useful values:

- The hedge rate: the attempts of one request type with `AWS_ERROR_HTTP_RESPONSE_FIRST_BYTE_TIMEOUT`, divided by all attempts of that type.
- The timeout over time, from the first-byte timeout of the attempts.
- The time to first byte of the attempts after a hedge, compared with the timeout. This shows if the hedges help.

The client also logs these events:

- INFO: the end of the warmup, with the start value of the timeout, and the stop of a controller.
- DEBUG: an empty budget that pauses the timeout, and the resume of the timeout.
- WARN for UploadPart and INFO for GetObject: more than 1% of the requests reached the timeout, so the timeout increases.
- aws-c-http logs each timeout at the INFO level, with the timeout (`Timed out waiting for first byte of HTTP response`).
