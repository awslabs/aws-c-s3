#!/usr/bin/env python3

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0.
import argparse
import boto3
import botocore
import sys
import os
import random


s3 = boto3.resource('s3')
s3_client = boto3.client('s3')


s3_control_client = boto3.client('s3control')


REGION = 'us-west-2'


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
    BUCKET_NAME = args.bucket_name
elif "CRT_S3_TEST_BUCKET_NAME" in os.environ:
    BUCKET_NAME = os.environ['CRT_S3_TEST_BUCKET_NAME']
else:
    # Generate a random bucket name
    BUCKET_NAME = 'aws-c-s3-test-bucket-' + str(random.random())[2:8]

PUBLIC_BUCKET_NAME = BUCKET_NAME + "-public"


def create_bytes(size):
    return bytearray([1] * size)


def put_pre_existing_objects(size, keyname, bucket=BUCKET_NAME, sse=None, public_read=False):
    if size == 0:
        s3_client.put_object(Bucket=bucket, Key=keyname)
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
        s3_client.put_object(**args)
    except botocore.exceptions.ClientError as e:
        print(f"Object {keyname} failed to upload, with exception: {e}")
        if public_read and e.response['Error']['Code'] == 'AccessDenied':
            print("Check your account level S3 settings, public access may be blocked.")
        exit(-1)
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
        put_pre_existing_objects(256*MB, 'pre-existing-256MB')
        put_pre_existing_objects(256*MB, 'pre-existing-256MB-@')
        put_pre_existing_objects(2*GB, 'pre-existing-2GB')
        put_pre_existing_objects(2*GB, 'pre-existing-2GB-@')
        put_pre_existing_objects(10*MB, 'pre-existing-10MB')
        put_pre_existing_objects(1*MB, 'pre-existing-1MB')
        put_pre_existing_objects(1*MB, 'pre-existing-1MB-@')
        put_pre_existing_objects(0, 'pre-existing-empty')

    except botocore.exceptions.ClientError as e:
        # The bucket already exists. That's fine.
        if e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou' or e.response['Error']['Code'] == 'BucketAlreadyExists':
            print(
                f"Bucket {BUCKET_NAME} not created, skip initializing.", file=sys.stderr)
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


def cleanup(bucket_name):
    bucket = s3.Bucket(bucket_name)
    bucket.objects.all().delete()
    s3_client.delete_bucket(Bucket=bucket_name)
    print(f"Bucket {bucket_name} deleted", file=sys.stderr)


if args.action == 'init':
    try:
        print(BUCKET_NAME + " " + PUBLIC_BUCKET_NAME + " initializing...")
        create_bucket_with_lifecycle()
        create_bucket_with_public_object()
        if os.environ.get('CRT_S3_TEST_BUCKET_NAME') != BUCKET_NAME:
            print(
                f"* Please set the environment variable $CRT_S3_TEST_BUCKET_NAME to {BUCKET_NAME} before running the tests.")
    except Exception as e:
        print(e)
        try:
            # Try to clean up the bucket created, when initialization failed.
            cleanup(BUCKET_NAME)
            cleanup(PUBLIC_BUCKET_NAME)
        except Exception as e:
            exit(-1)
        raise e
        exit(-1)
elif args.action == 'clean':
    if "CRT_S3_TEST_BUCKET_NAME" not in os.environ and args.bucket_name is None:
        print("Set the environment variable CRT_S3_TEST_BUCKET_NAME before clean up, or pass in bucket_name as argument.")
        exit(-1)
    cleanup(BUCKET_NAME)
    cleanup(PUBLIC_BUCKET_NAME)
