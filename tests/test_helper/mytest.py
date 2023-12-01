import logging
import boto3
from botocore.exceptions import ClientError


print("boto3 version is: " + boto3.__version__)


def create_bucket(s3_client, bucket_name, availability_zone):
    s3_client.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={
            'Location': {'Type': "AvailabilityZone", 'Name': availability_zone},
            'Bucket': {'Type': "Directory", 'DataRedundancy': "SingleAvailabilityZone"}
        })


if __name__ == '__main__':

    bucket_name = 'test-bbarrn--use1-az4--x-s3'
    region = 'us-east-1'
    availability_zone = 'use1-az4'
    s3_client = boto3.client('s3', region_name=region)
    create_bucket(s3_client, bucket_name, availability_zone)
