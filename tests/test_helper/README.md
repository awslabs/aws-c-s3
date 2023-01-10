# Helper script to setup your S3 structure to run the tests for aws-c-s3

You need to have AWS credentials in your environment that have permission to create and delete bucket to use this script.

To create the S3 structure for tests to use:

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

- Create `aws-c-s3-test-bucket` in us-west-2 if not existed before
- Add the lifecyle to automatic clean up the `upload/` after one day
- Upload pre-exist files:
  - pre-existing-10MB-aes256-c
  - pre-existing-10MB-aes256
  - pre-existing-10MB-kms
  - pre-existing-10MB
  - pre-existing-1MB
  - pre-existing-empty
- Create `aws-c-s3-test-bucket-public` in us-west-2 if not existed before
- Upload pre-exist files:
  - pre-existing-1MB with public read access.


### `CLEAN` action

- Delete every objects in the `aws-c-s3-test-bucket` and `aws-c-s3-test-bucket-public`
- Delete `aws-c-s3-test-bucket` and `aws-c-s3-test-bucket-public` buckets.
