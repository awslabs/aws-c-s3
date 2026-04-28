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

#### Environment Variables

1. **Memory Limit - `AWS_CRT_S3_MEMORY_LIMIT_IN_GIB`**

   The S3 client uses a buffer pool to manage memory for concurrent transfers. 

   Example Usage:

   ```bash
   export AWS_CRT_S3_MEMORY_LIMIT_IN_GIB=4  # 4 GiB limit
   ```

   > [!TIP]
   > You can also control memory limit *in bytes* using client config. The client config takes precedence over the environment variable (memory_limit_in_bytes needs to be set to a non-zero value).
   > ```c
   >    struct aws_s3_client_config config = {
   >        .memory_limit_in_bytes = GB_TO_BYTES(4), // 4 GiB limit
   >        // ... other configuration
   >    };
   > ```

   **Default Behavior**:
   If neither is set (config is 0 and environment variable is not set), the client sets a default memory limit based on the target throughput.

   **Notes**:
   * The limit applies per client. If multiple clients created, limit will apply to each separately.
   * The environment variable value must be a valid positive integer representing gigabytes (GiB).
   * The value is converted from GiB to bytes internally (1 GiB = 1024³ bytes).
   * Invalid values or overflow conditions will cause client creation to fail with `AWS_ERROR_INVALID_ARGUMENT`.

2. **Maximum Parts Pending Read - `AWS_CRT_S3_MAX_PARTS_PENDING_READ`**

   Controls the maximum number of parts that can be pending read from the input stream during a multipart upload. Higher values may improve upload throughput for large files by allowing more parts to be read in parallel, at the cost of higher memory usage.

   Example Usage:

   ```bash
   export AWS_CRT_S3_MAX_PARTS_PENDING_READ=20
   ```

   **Default Behavior**:
   If not set, the default value is 5.

   **Notes**:
   * Only affects multipart uploads. Small files that fit in a single part are not affected.
   * Setting this too low may introduce delays between reads, as the meta-request waits for the client to schedule more work.
   * Setting this too high may cause a single upload to hog work tokens, starving other concurrent uploads.
   * The value must be a positive integer (1–4294967295). Invalid or zero values are ignored with a warning, and the default is used.
   * The value is read once on first use and cached for the lifetime of the process.

3. **Test Bucket - `CRT_S3_TEST_BUCKET_NAME`**

   The S3 bucket name used for running unit tests. See the [test_helper documentation](./tests/test_helper/) for setup instructions.

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
