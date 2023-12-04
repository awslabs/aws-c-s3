#!/usr/bin/env python3

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0.
import argparse
import boto3
import botocore
import sys
import os
import random

print(boto3.__version__)

REGION = 'us-west-2'
REGION_EAST_1 = 'us-east-1'
s3 = boto3.resource('s3')
s3_client = boto3.client('s3', region_name=REGION)
s3_client_east1 = boto3.client('s3', region_name=REGION_EAST_1)


s3_control_client = boto3.client('s3control')


MB = 1024*1024
GB = 1024*1024*1024


parser = argparse.ArgumentParser()
parser.add_argument(
    'action',
    choices=['init', 'clean'],
    help='Initialize or clean up the test buckets')
parser.add_argument(
    'bucket_name',
    nargs='?',
    help='The bucket name base to use for the test buckets. If not specified, the $CRT_S3_TEST_BUCKET_NAME will be used, if set. Otherwise, a random name will be generated.')

args = parser.parse_args()

if args.bucket_name is not None:
    BUCKET_NAME_BASE = args.bucket_name
elif "CRT_S3_TEST_BUCKET_NAME" in os.environ:
    BUCKET_NAME_BASE = os.environ['CRT_S3_TEST_BUCKET_NAME']
else:
    # Generate a random bucket name
    BUCKET_NAME_BASE = 'aws-c-s3-test-bucket-' + str(random.random())[2:8]

PUBLIC_BUCKET_NAME = BUCKET_NAME_BASE + "-public"


def create_bytes(size):
    return bytearray([1] * size)


def put_pre_existing_objects(size, keyname, bucket=BUCKET_NAME_BASE, sse=None, public_read=False, client=s3_client):
    if size == 0:
        client.put_object(Bucket=bucket, Key=keyname)
        print(f"Object {keyname} uploaded")
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
    try:
        client.put_object(**args)
    except botocore.exceptions.ClientError as e:
        print(f"Object {keyname} failed to upload, with exception: {e}")
        if public_read and e.response['Error']['Code'] == 'AccessDenied':
            print("Check your account level S3 settings, public access may be blocked.")
        exit(-1)
    print(f"Object {keyname} uploaded")


def create_bucket_with_lifecycle(availability_zone=None, client=s3_client):
    try:
        # Create the bucket. This returns an error if the bucket already exists.

        if availability_zone is not None:
            bucket_config = {
                'Location': {
                    'Type': 'AvailabilityZone',
                    'Name': availability_zone
                },
                'Bucket': {
                    'Type': 'Directory',
                    'DataRedundancy': 'SingleAvailabilityZone'
                }
            }
            bucket_name = BUCKET_NAME_BASE+f"--{availability_zone}--x-s3"
        else:
            bucket_config = {'LocationConstraint': REGION}
            bucket_name = BUCKET_NAME_BASE

        client.create_bucket(
            Bucket=bucket_name, CreateBucketConfiguration=bucket_config)
        if availability_zone is None:
            client.put_bucket_lifecycle_configuration(
                Bucket=bucket_name,
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
        print(f"Bucket {bucket_name} created", file=sys.stderr)

        put_pre_existing_objects(
            10*MB, 'pre-existing-10MB', bucket=bucket_name, client=client)

        if availability_zone is None:
            put_pre_existing_objects(
                10*MB, 'pre-existing-10MB-aes256-c', sse='aes256-c', bucket=bucket_name)
            put_pre_existing_objects(
                10*MB, 'pre-existing-10MB-aes256', sse='aes256', bucket=bucket_name)
            put_pre_existing_objects(
                10*MB, 'pre-existing-10MB-kms', sse='kms', bucket=bucket_name)
            put_pre_existing_objects(
                256*MB, 'pre-existing-256MB', bucket=bucket_name)
            put_pre_existing_objects(
                256*MB, 'pre-existing-256MB-@', bucket=bucket_name)
            put_pre_existing_objects(
                2*GB, 'pre-existing-2GB', bucket=bucket_name)
            put_pre_existing_objects(
                2*GB, 'pre-existing-2GB-@', bucket=bucket_name)
            put_pre_existing_objects(
                1*MB, 'pre-existing-1MB', bucket=bucket_name)
            put_pre_existing_objects(
                1*MB, 'pre-existing-1MB-@', bucket=bucket_name)
            put_pre_existing_objects(
                0, 'pre-existing-empty', bucket=bucket_name)

    except botocore.exceptions.ClientError as e:
        # The bucket already exists. That's fine.
        if e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou' or e.response['Error']['Code'] == 'BucketAlreadyExists':
            print(
                f"Bucket {bucket_name} not created, skip initializing.", file=sys.stderr)
            return
        raise e


def create_bucket_with_public_object():
    try:
        s3_client.create_bucket(Bucket=PUBLIC_BUCKET_NAME,
                                CreateBucketConfiguration={
                                    'LocationConstraint': REGION},
                                ObjectOwnership='ObjectWriter'
                                )
        s3_client.put_public_access_block(
            Bucket=PUBLIC_BUCKET_NAME,
            PublicAccessBlockConfiguration={
                'BlockPublicAcls': False,
            }
        )
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


def cleanup(bucket_name, availability_zone=None, client=s3_client):
    if availability_zone is not None:
        bucket_name = bucket_name+f"--{availability_zone}--x-s3"

    objects = client.list_objects_v2(Bucket=bucket_name)["Contents"]
    objects = list(map(lambda x: {"Key": x["Key"]}, objects))
    client.delete_objects(Bucket=bucket_name, Delete={"Objects": objects})
    client.delete_bucket(Bucket=bucket_name)
    print(f"Bucket {bucket_name} deleted", file=sys.stderr)


if args.action == 'init':
    try:
        print(BUCKET_NAME_BASE + " " + PUBLIC_BUCKET_NAME + " initializing...")
        create_bucket_with_lifecycle("use1-az4", s3_client_east1)
        create_bucket_with_lifecycle("usw2-az1")
        create_bucket_with_lifecycle()
        create_bucket_with_public_object()
        if os.environ.get('CRT_S3_TEST_BUCKET_NAME') != BUCKET_NAME_BASE:
            print(
                f"* Please set the environment variable $CRT_S3_TEST_BUCKET_NAME to {BUCKET_NAME_BASE} before running the tests.")
    except Exception as e:
        print(e)
        try:
            # Try to clean up the bucket created, when initialization failed.
            cleanup(BUCKET_NAME_BASE, "use1-az4", s3_client_east1)
            cleanup(BUCKET_NAME_BASE, "usw2-az1")
            cleanup(BUCKET_NAME_BASE)
            cleanup(PUBLIC_BUCKET_NAME)
        except Exception as e2:
            exit(-1)
        exit(-1)
elif args.action == 'clean':
    if "CRT_S3_TEST_BUCKET_NAME" not in os.environ and args.bucket_name is None:
        print("Set the environment variable CRT_S3_TEST_BUCKET_NAME before clean up, or pass in bucket_name as argument.")
        exit(-1)
    cleanup(BUCKET_NAME_BASE, "use1-az4", s3_client_east1)
    cleanup(BUCKET_NAME_BASE, "usw2-az1")
    cleanup(BUCKET_NAME_BASE)
    cleanup(PUBLIC_BUCKET_NAME)
