# Helper script to setup your S3 structure to run the tests for aws-c-s3

To use this script, you must have AWS credentials with permission to create and delete buckets.

To create the S3 buckets and objects that tests will use:

``` sh
pip3 install boto3
python3 test_helper.py -a INIT
```

To clean up the S3 buckets created

``` sh
python3 test_helper.py -a CLEAN
```

## Actions

### `INIT` action

- Create `aws-c-s3-test-bucket` in us-west-2
- Add the lifecyle to automatic clean up the `upload/` after one day
- Upload files:
  - pre-existing-10MB-aes256-c
  - pre-existing-10MB-aes256
  - pre-existing-10MB-kms
  - pre-existing-10MB
  - pre-existing-1MB
  - pre-existing-empty
- Create `aws-c-s3-test-bucket-public` in us-west-2
- Upload files:
  - pre-existing-1MB with public read access.

### `CLEAN` action

- Delete the `aws-c-s3-test-bucket` and `aws-c-s3-test-bucket-public` and every object inside them

## Notes

The MRAP tests are not included in this script, and it's disabled by default. To run those tests, you will need to create a MRAP access point with the buckets have `pre-existing-1MB` in it. Then update `g_test_mrap_endpoint` to the uri of the MRAP endpoint and build with `-DENABLE_MRAP_TESTS=true`.

## TODO

- Automatic the mrap creation
- Instead of hard-coded path, bucket and region, use the helper to set env-var and pick up from tests.
