# Changelog

## [Unreleased]

### Features

- Add opt-in GET hedging. If a part request of a GetObject meta request gets no response before an adaptive first-byte timeout, the client cancels it and sends it again at once on another connection. A budget keeps the hedges below approximately 5% of the requests. Turn it on with `get_object_hedging` in the client config or the `AWS_CRT_S3_GET_OBJECT_HEDGING` environment variable. Adds `aws_s3_request_metrics_get_first_byte_timeout_ms`. See [First-Byte Timeout and Hedging](docs/Hedging.md). ([#736](https://github.com/awslabs/aws-c-s3/pull/736))

  Behavior change for existing callers: the UploadPart first-byte timeout, which is on by default, now uses the same controller and the same hedge path. This applies to all uploads, also if GET hedging is off:
  - An UploadPart request that reaches its first-byte timeout does not go through the retry strategy, also if the caller gives a `retry_strategy`. The client sends the part again at once, one time. The new attempt has no backoff, does not use the retry quota, and does not count against the max retries. In `AWS_S3_RETRY_MODE_ADAPTIVE`, it does not wait for the client-side rate limiter.
  - Only the first attempt of a part has a first-byte timeout. Before, each retry had one, and a part that was slow on each attempt could use all of its retries and fail the upload.
  - After approximately 10 timeouts in sequence, the hedge budget pauses the timeout until 20 successes refill it.
  - The mean time to first byte ages (EWMA, alpha 1/64), and the first timeout is the nearest-rank p90 of all warmup samples.
  - A response that starts before aws-c-http records the end of the request send has a time to first byte of 0. Before, the time was a very large unsigned value, and one such response could stop the timeout for the life of the client.
  - The UploadPart requests of an `AWS_S3_META_REQUEST_TYPE_DEFAULT` meta request also give their results to the controller. Before, they had the timeout, but the controller did not count them.
  - The `max` value of the `amz-sdk-request` header includes the hedge.

## [1.2.0]

### Features

- Downloads to a file (`recv_filepath`) now write parts to the file in parallel and out of order by default, each at its own offset as it arrives. Adds `out_of_order_delivery` to the client config and the meta request options, `num_file_io_threads` to size a new event loop group for file writes, and the `AWS_CRT_S3_ORDERED_DELIVERY` and `AWS_CRT_S3_FORCE_SEQUENTIAL_REQUESTS` environment variables. ([#679](https://github.com/awslabs/aws-c-s3/pull/679))

  Behavior change for existing callers: the finished file is unchanged, but a download that fails or pauses without `recv_file_delete_on_failure` can now leave a file with gaps rather than a contiguous prefix. The resume token's `continuous_downloaded_bytes` reports the contiguous prefix and `total_downloaded_bytes` reports everything written. Set `out_of_order_delivery` to `AWS_TRIBOOL_FALSE` on the client or the request, or set `AWS_CRT_S3_ORDERED_DELIVERY`, to keep in-order writes. Downloads to a body callback are unchanged and stay in order.

## [1.1.3]

### Features

- Add `aws_s3_default_memory_limit_for_throughput`, which returns the default memory limit the client would use for a given throughput target, so bindings can size their own memory pools to match. ([#681](https://github.com/awslabs/aws-c-s3/pull/681))
- Add request metrics for whether the request used TLS and a snapshot of the HTTP connection manager's state, plus `aws_s3_client_get_max_active_connections`. ([#673](https://github.com/awslabs/aws-c-s3/pull/673))

### Fixes

- Clarify when a download's response checksum is validated. `did_validate` is now true when every delivered byte was checked, either against the object's own checksum or against a checksum on every part response. Ranged GETs only validate part checksums, since the object's checksum covers bytes that were not requested. ([#678](https://github.com/awslabs/aws-c-s3/pull/678))

## [1.1.2]

### Fixes

- Relax the check that rejects a part size larger than the memory limit allows, so it no longer rejects file-streaming requests, which do not reserve a full part-size buffer. ([#680](https://github.com/awslabs/aws-c-s3/pull/680))

## [1.1.1]

### Features

- Report S3 client feature usage (custom part size, throughput target, memory limit, running on EC2, file path transfers) in the User-Agent header. ([#674](https://github.com/awslabs/aws-c-s3/pull/674))

## [1.1.0]

### Features

- Right-size the default memory limit for throughput targets below 10 Gbps, using either the configured `throughput_target_gbps` or the throughput detected from the EC2 instance type. Adds the `AWS_CRT_S3_MEMORY_LIMIT_IN_MB` environment variable, which takes precedence over `AWS_CRT_S3_MEMORY_LIMIT_IN_GIB`. ([#670](https://github.com/awslabs/aws-c-s3/pull/670))
- Add `retry_config` to `aws_s3_client_config` to tune the built-in retry strategy (max retries, backoff scale factor, max backoff, jitter mode, initial bucket capacity) without constructing one. Ignored when `retry_strategy` is set. ([#672](https://github.com/awslabs/aws-c-s3/pull/672))

### Fixes

- Retry requests that fail with HTTP 502 Bad Gateway or 504 Gateway Timeout. ([#671](https://github.com/awslabs/aws-c-s3/pull/671))

## [1.0.0]

Official release of 1.0.0.
