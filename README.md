## AWS C S3

The AWS-C-S3 library is an asynchronous AWS S3 client focused on maximizing throughput and network utilization.

### Key features:

* **Automatic Request Splitting**: Improves throughput by automatically splitting the request into part-sized chunks and performing parallel uploads/downloads of these chunks over multiple connections. There's a cap on the throughput of single S3 connection, the only way to go faster is multiple parallel connections.
* **Automatic Retries**: Increases resilience by retrying individual failed chunks of a file transfer, eliminating the need to restart transfers from scratch after an intermittent error.
* **DNS Load Balancing**: DNS resolver continuously harvests Amazon S3 IP addresses. When load is spread across the S3 fleet, overall throughput more reliable than if all connections are going to a single IP.
* **Advanced Network Management**: The client incorporates automatic request parallelization, effective timeouts and retries, and efficient connection reuse. This approach helps to maximize throughput and network utilization, and to avoid network overloads.
* **Thread Pools and Async I/O**: Avoids bottlenecks associated with single-thread processing.
* **Parallel Reads**: When uploading a large file from disk, reads from multiple parts of the file in parallel. This is faster than reading the file sequentially from beginning to end.

### Documentation

* [GetObject](docs/GetObject.md): A visual representation of the GetObject request flow.
* [Memory Aware Requests Execution](docs/memory_aware_request_execution.md): An in-depth guide on optimizing memory usage during request executions.

### Configuration

#### Memory Limit

The S3 client uses a buffer pool to manage memory for concurrent transfers. You can control the memory limit in two ways:

1. **Via Configuration** (Recommended): Set `memory_limit_in_bytes` in `aws_s3_client_config`:

```c
   struct aws_s3_client_config config = {
       .memory_limit_in_bytes = GB_TO_BYTES(4), // 4 GiB limit
       // ... other configuration
   };
   ```

2. **Via Environment Variable**: Set the `AWS_CRT_S3_MEMORY_LIMIT_IN_GIB` environment variable:

```bash
   export AWS_CRT_S3_MEMORY_LIMIT_IN_GIB=4  # 4 GiB limit
   ```

**Priority**: The configuration value takes precedence over the environment variable. If `memory_limit_in_bytes` is set to a non-zero value in the config, the environment variable is ignored.

**Default Behavior**: If neither is set (config is 0 and environment variable is not set), the client sets a default memory limit based on the target throughput.

**Notes**:
* The environment variable value must be a valid positive integer representing gigabytes (GiB).
* The value is converted from GiB to bytes internally (1 GiB = 1024³ bytes).
* Invalid values or overflow conditions will cause client creation to fail with `AWS_ERROR_INVALID_ARGUMENT`.

## License

This library is licensed under the Apache 2.0 License.

## Usage

### Building

CMake 3.9+ is required to build.

`<install-path>` must be an absolute path in the following instructions.

#### Linux-Only Dependencies

If you are building on Linux, you will need to build aws-lc and s2n-tls first.

```
git clone git@github.com:aws/aws-lc.git
cmake -S aws-lc -B aws-lc/build -DCMAKE_INSTALL_PREFIX=<install-path>
cmake --build aws-lc/build --target install

git clone git@github.com:aws/s2n-tls.git
cmake -S s2n-tls -B s2n-tls/build -DCMAKE_INSTALL_PREFIX=<install-path> -DCMAKE_PREFIX_PATH=<install-path>
cmake --build s2n-tls/build --target install
```

#### Building aws-c-s3 and Remaining Dependencies

```
git clone git@github.com:awslabs/aws-c-common.git
cmake -S aws-c-common -B aws-c-common/build -DCMAKE_INSTALL_PREFIX=<install-path>
cmake --build aws-c-common/build --target install

git clone git@github.com:awslabs/aws-checksums.git
cmake -S aws-checksums -B aws-checksums/build -DCMAKE_INSTALL_PREFIX=<install-path> -DCMAKE_PREFIX_PATH=<install-path>
cmake --build aws-checksums/build --target install

git clone git@github.com:awslabs/aws-c-cal.git
cmake -S aws-c-cal -B aws-c-cal/build -DCMAKE_INSTALL_PREFIX=<install-path> -DCMAKE_PREFIX_PATH=<install-path>
cmake --build aws-c-cal/build --target install

git clone git@github.com:awslabs/aws-c-io.git
cmake -S aws-c-io -B aws-c-io/build -DCMAKE_INSTALL_PREFIX=<install-path> -DCMAKE_PREFIX_PATH=<install-path>
cmake --build aws-c-io/build --target install

git clone git@github.com:awslabs/aws-c-compression.git
cmake -S aws-c-compression -B aws-c-compression/build -DCMAKE_INSTALL_PREFIX=<install-path> -DCMAKE_PREFIX_PATH=<install-path>
cmake --build aws-c-compression/build --target install

git clone git@github.com:awslabs/aws-c-http.git
cmake -S aws-c-http -B aws-c-http/build -DCMAKE_INSTALL_PREFIX=<install-path> -DCMAKE_PREFIX_PATH=<install-path>
cmake --build aws-c-http/build --target install

git clone git@github.com:awslabs/aws-c-sdkutils.git
cmake -S aws-c-sdkutils -B aws-c-sdkutils/build -DCMAKE_INSTALL_PREFIX=<install-path> -DCMAKE_PREFIX_PATH=<install-path>
cmake --build aws-c-sdkutils/build --target install

git clone git@github.com:awslabs/aws-c-auth.git
cmake -S aws-c-auth -B aws-c-auth/build -DCMAKE_INSTALL_PREFIX=<install-path> -DCMAKE_PREFIX_PATH=<install-path>
cmake --build aws-c-auth/build --target install

git clone git@github.com:awslabs/aws-c-s3.git
cmake -S aws-c-s3 -B aws-c-s3/build -DCMAKE_INSTALL_PREFIX=<install-path> -DCMAKE_PREFIX_PATH=<install-path>
cmake --build aws-c-s3/build --target install
```

#### Running S3 sample

After installing all the dependencies, and building aws-c-s3, you can run the sample directly from the s3 build directory.

To download:

```
aws-c-s3/build/samples/s3/s3 cp s3://<bucket-name>/<object-name> <download-path> --region <region>
```

To upload:

```
aws-c-s3/build/samples/s3/s3 cp <upload-path> s3://<bucket-name>/<object-name> --region <region>
```

To list objects:

```
aws-c-s3/build/samples/s3/s3 ls s3://<bucket-name> --region <region>
```

## Testing

The unit tests require an AWS account with S3 buckets set up in a particular way.
Use the [test_helper script](./tests/test_helper/) to set this up.
