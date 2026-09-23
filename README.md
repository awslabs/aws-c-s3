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

1. **Memory Limit - `AWS_CRT_S3_MEMORY_LIMIT_IN_GIB` and `AWS_CRT_S3_MEMORY_LIMIT_IN_MB`** 

   The S3 client uses a buffer pool to manage memory for concurrent transfers. 

   Example Usage:

   ```bash
   export AWS_CRT_S3_MEMORY_LIMIT_IN_GIB=4  # 4 GiB limit
   # or
   export AWS_CRT_S3_MEMORY_LIMIT_IN_MB=256 # 256 MiB limit
   ```

   **Default Behavior**:
   When nothing is set, the client sets a default memory limit based on the target throughput.

   **Notes**:
   * If both are set `AWS_CRT_S3_MEMORY_LIMIT_IN_MB` is used and `AWS_CRT_S3_MEMORY_LIMIT_IN_GIB` is ignored.
   * The limit applies per client. If multiple clients created, limit will apply to each separately.
   * The environment variable value must be a valid positive integer representing gigabytes (GiB) or megabytes (MiB).
   * The value is converted from GiB or MiB to bytes internally.
   * Invalid values or overflow conditions will cause client creation to fail with `AWS_ERROR_INVALID_ARGUMENT`.

> [!TIP]
> You can also control memory limit *in bytes* using client config. The client config takes precedence over the environment variable (memory_limit_in_bytes needs to be set to a non-zero value).
> ```c
>    struct aws_s3_client_config config = {
>        .memory_limit_in_bytes = GB_TO_BYTES(4), // 4 GiB limit
>        // ... other configuration
>    };
> ```

2. **Maximum Parts Pending Read - `AWS_CRT_S3_MAX_PARTS_PENDING_READ`**

   Controls the maximum number of parts that can be pending read from the input stream during an individual multipart upload. Higher values may improve upload throughput for large files by allowing more parts to be read in parallel, only if the disk read speed can benefit from more concurrent reading of parts.

   Example Usage:

   ```bash
   export AWS_CRT_S3_MAX_PARTS_PENDING_READ=20
   ```

   **Default Behavior**:
   If not set, the default value is 5.

   **Notes**:
   * Only affects multipart uploads. Small files that fit in a single part are not affected.
   * If there are multiple parallel multipart upload requests, each upload is limited by the value individually (not cumulatively).
   * Setting this too low may introduce delays between reads, as the meta-request waits for the client to schedule more work.
   * Setting this too high may cause a single upload to hog work tokens, starving other concurrent uploads.
   * The value must be a positive integer (1–4294967295). Invalid or zero values are ignored with a warning, and the default is used.
   * The value is read once on first use and cached for the lifetime of the process.
   * If the network bandwidth of the device is too low, even a higher value of pending read might not be respected due to having maximum allowed requests in flight.

3. **Ordered Delivery - `AWS_CRT_S3_ORDERED_DELIVERY`**

   Makes a download deliver its body in object order when it has not asked for a delivery order of its own.

   Example Usage:

   ```bash
   export AWS_CRT_S3_ORDERED_DELIVERY=1
   ```

   **Default Behavior**:
   When nothing is set, a download to a file delivers out of object order (each part is written at its own offset as it arrives) and a download through `body_callback` delivers in object order.

   **Notes**:
   * Any non-empty value turns it on; the value itself is not read.
   * It changes the default, it does not overrule the caller. A `out_of_order_delivery` set on the meta request wins over one set on the client, and either wins over this variable. Only a download that expressed no preference is affected.
   * It can only ask for ordered delivery. There is deliberately no way to turn *out-of-order* delivery on from the environment: for a `body_callback` sink that would change what the caller's own code sees, since `range_start` stops advancing contiguously.
   * Setting it means an interrupted download to a file leaves a valid prefix rather than a file with gaps, at the cost of parts waiting on the part ahead of them.
   * Read once per client, when the client is created.

4. **Sequential Requests - `AWS_CRT_S3_FORCE_SEQUENTIAL_REQUESTS`**

   Makes every download request its parts in object order instead of spreading them across several far-apart regions of the object at once.

   Example Usage:

   ```bash
   export AWS_CRT_S3_FORCE_SEQUENTIAL_REQUESTS=1
   ```

   **Default Behavior**:
   When nothing is set, a download that delivers out of order also issues its range requests across as many far-apart regions of the object as it has connections, one region per connection. A download that delivers in object order already requests in object order and is unaffected.

   **Notes**:
   * Any non-empty value turns it on; the value itself is not read.
   * It only changes what is requested, not how it is delivered. Parts can still be written to the file at their own offsets as they arrive, depending on the delivery order setting.
   * Setting it makes an interrupted download's file size bound how many of its bytes are valid, and makes resuming cheaper, because there are no gaps for the download to be ahead of.
   * Read once per client, when the client is created.

5. **Test Bucket - `CRT_S3_TEST_BUCKET_NAME`**

   The S3 bucket name used for running unit tests. See the [test_helper documentation](./tests/test_helper/) for setup instructions.

## Versioning

This library uses a three-part `Major.Minor.Patch` version scheme. See
[VERSIONING.md](VERSIONING.md) for what each part means and our API/ABI
stability policy.

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
