# AWS C-S3 Client

## Overview
The AWS-C-S3 library is an asynchronous AWS S3 client focused on maximizing throughput and network utilization.

### Key features:
- **Automatic Request Splitting**: Improves throughput by automatically splitting the request into part-sized chunks and performing parallel uploads/downloads of these chunks over multiple connections.
- **Enhanced Transfer Reliability**: Increases resilience against network failures by retrying individual failed chunks of a file transfer, eliminating the need to restart transfers from scratch.
- **DNS Load Balancing**: DNS continuously harvests Amazon S3 IP addresses to spread connections over the fleet, significantly boosting throughput and enhancing overall transfer efficiency.
- **Advanced Network Interface Management**: The client incorporates automatic request parallelization, effective timeouts and retries, and efficient connection reuse. This approach helps to maximize throughput and network utilization and to avoid network overloads.
- **Thread Pools and Async I/O**: Avoids bottlenecks associated with single-thread processing.
- **Parallel Reads**: Supports parallel file reads from disk when uploading to CRT.

## Documentation

- [GetObject](Diagrams/GetObjectFlow.md): A visual representation of the GetObject request flow.
- [Memory Aware Requests Execution](memory_aware_requests_execution.md): An in-depth guide on optimizing memory usage during request executions.

