# Helper script to setup your S3 structure to run the tests for aws-c-s3

To use this script, you must have AWS credentials with permission to create and delete buckets.

To create the S3 buckets and objects that tests will use:

```sh
pip3 install boto3
export CRT_S3_TEST_BUCKET_NAME=<bucket_name>
python3 test_helper.py init
# change directory to the build/tests
cd aws-c-s3/build/tests && ctest
```

To clean up the S3 buckets created

```sh
export CRT_S3_TEST_BUCKET_NAME=<bucket_name>
python3 test_helper.py clean
```

## Actions

### `init` action

* Create `<BUCKET_NAME>` in us-west-2.
* Add the lifecycle to automatic clean up the `upload/` after one day
* Upload files:
  + `pre-existing-10MB-aes256-c` [SSE-C](https://docs.aws.amazon.com/AmazonS3/latest/userguide/ServerSideEncryptionCustomerKeys.html#sse-c-highlights) encrypted fille
  + `pre-existing-10MB-aes256` [SSE-S3](https://docs.aws.amazon.com/AmazonS3/latest/userguide/specifying-s3-encryption.html) encrypted fille
  + `pre-existing-10MB-kms` [SSE-KMS](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingKMSEncryption.html) encrypted fille
  + `pre-existing-10MB`
  + `pre-existing-1MB`
  + `pre-existing-empty`
* Create `<BUCKET_NAME>-public` in us-west-2
* Upload files:
  + `pre-existing-1MB` 1MB file with public read access.

### `clean` action

* Delete the `<BUCKET_NAME>` and `<BUCKET_NAME>-public` and every object inside them

## BUCKET_NAME

You can specify the bucket name to be created either by passing argument to the script or by setting an environment variable, the `bucket_name` passed in takes precedence. If neither of these options is chosen, the `init` action will create a random bucket name. In this case, you will need to set the `CRT_S3_TEST_BUCKET_NAME` environment variable to the printed-out bucket name before running the test.

## Notes

* The MRAP tests are not included in this script, and it's disabled by default. To run those tests, you will need to create a MRAP access point with the buckets have `pre-existing-1MB` in it. Then update `g_test_mrap_endpoint` to the uri of the MRAP endpoint and build with `-DENABLE_MRAP_TESTS=true`.
* To run tests in tests/s3_mock_server_tests.c, initialize the mock S3 server first from [here](./../mock_s3_server/). And build your cmake project with `-DENABLE_MOCK_SERVER_TESTS=true`
* Note: If you are not at the aws-common-runtime AWS team account, you must set environment variable `CRT_S3_TEST_BUCKET_NAME` to the bucket created before running the test.
* When you see error with "Check your account level S3 settings, public access may be blocked.", Check https://docs.aws.amazon.com/AmazonS3/latest/userguide/configuring-block-public-access-account.html to set `BlockPublicAcls` to false, which enables public read of the object with `public-read` ACL in the bucket.

## TODO

* Automatic the mrap creation
* Instead of hard-coded path and region, make it configurable and pick up from tests.
