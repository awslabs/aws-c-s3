# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0.
import argparse
import boto3
import botocore
import sys
import os


s3 = boto3.resource('s3')
s3_client = boto3.client('s3')


s3_control_client = boto3.client('s3control')


BUCKET_NAME = 'aws-c-s3-test-bucket'
# Create a public bucket with one object for testing public access
PUBLIC_BUCKET_NAME = 'aws-c-s3-test-bucket-public'

REGION = 'us-west-2'
os.environ['AWS_DEFAULT_REGION'] = REGION


MB = 1024*1024
GB = 1024*1024*1024


def create_bytes(size):
    return bytearray([1] * size)


def put_pre_existing_objects(size, keyname, bucket=BUCKET_NAME, sse=None, public_read=False):
    if size == 0:
        s3_client.put_object(Bucket=bucket, Key=keyname)
        return

    body = create_bytes(size)
    args = {'Bucket': bucket, 'Key': keyname, 'Body': body}
    if sse == 'aes256':
        args['ServerSideEncryption'] = 'AES256'
    elif sse == 'aes256-c':
        random_key = os.urandom(32)
        args['SSECustomerKey'] = random_key
        args['SSECustomerAlgorithm'] = 'AES256'
    elif sse == 'kms':
        args['ServerSideEncryption'] = 'aws:kms'
        args['SSEKMSKeyId'] = 'alias/aws/s3'

    if public_read:
        args['ACL'] = 'public-read'

    s3_client.put_object(**args)
    print(f"Object {keyname} uploaded")


def create_bucket_with_lifecycle():
    try:
        # Create the bucket. This returns an error if the bucket already exists.
        s3_client.create_bucket(
            Bucket=BUCKET_NAME, CreateBucketConfiguration={'LocationConstraint': REGION})
        s3_client.put_bucket_lifecycle_configuration(
            Bucket=BUCKET_NAME,
            LifecycleConfiguration={
                'Rules': [
                    {
                        'ID': 'clean up non-pre-existing objects',
                        'Expiration': {
                            'Days': 1,
                        },
                        'Filter': {
                            'Prefix': 'upload/',
                        },
                        'Status': 'Enabled',
                        'NoncurrentVersionExpiration': {
                            'NoncurrentDays': 1,
                        },
                        'AbortIncompleteMultipartUpload': {
                            'DaysAfterInitiation': 1,
                        },
                    },
                ],
            },
        )
        print(f"Bucket {BUCKET_NAME} created", file=sys.stderr)
        put_pre_existing_objects(
            10*MB, 'pre-existing-10MB-aes256-c', sse='aes256-c')
        put_pre_existing_objects(
            10*MB, 'pre-existing-10MB-aes256', sse='aes256')
        put_pre_existing_objects(
            10*MB, 'pre-existing-10MB-kms', sse='kms')
        put_pre_existing_objects(10*MB, 'pre-existing-10MB')
        put_pre_existing_objects(1*MB, 'pre-existing-1MB')
        put_pre_existing_objects(0, 'pre-existing-empty')

    except botocore.exceptions.ClientError as e:
        # The bucket already exists. That's fine.
        if e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou' or e.response['Error']['Code'] == 'BucketAlreadyExists':
            print(
                f"Bucket {PUBLIC_BUCKET_NAME} not created, skip initializing.", file=sys.stderr)
            return
        raise e


def create_bucket_with_public_object():
    try:
        s3_client.create_bucket(Bucket=PUBLIC_BUCKET_NAME,
                                CreateBucketConfiguration={'LocationConstraint': REGION})
        print(f"Bucket {PUBLIC_BUCKET_NAME} created", file=sys.stderr)
        put_pre_existing_objects(
            1*MB, 'pre-existing-1MB', bucket=PUBLIC_BUCKET_NAME, public_read=True)
    except botocore.exceptions.ClientError as e:
        # The bucket already exists. That's fine.
        if e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou' or e.response['Error']['Code'] == 'BucketAlreadyExists':
            print(
                f"Bucket {PUBLIC_BUCKET_NAME} not created, skip initializing.", file=sys.stderr)
            return
        raise e


def cleanup(bucket_name):
    bucket = s3.Bucket(bucket_name)
    bucket.objects.all().delete()
    s3_client.delete_bucket(Bucket=bucket_name)
    print(f"Bucket {bucket_name} deleted", file=sys.stderr)


parser = argparse.ArgumentParser()
parser.add_argument(
    'action',
    choices=['init', 'clean'],
    help='Initialize or clean up the test buckets')

args = parser.parse_args()

if args.action == 'init':
    create_bucket_with_lifecycle()
    create_bucket_with_public_object()

if args.action == 'clean':
    cleanup(BUCKET_NAME)
    cleanup(PUBLIC_BUCKET_NAME)
