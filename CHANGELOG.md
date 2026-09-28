# Changelog

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
