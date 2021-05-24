import json
import os
import boto3
import re
from botocore.exceptions import ClientError

BUCKET_NAME = 'aws-crt-test-stuff'
CDK_STACK_PATH = 'benchmarks-stack/'
CDK_APP_PATH = CDK_STACK_PATH + 'bin/benchmarks-stack.ts'
CODE_BUILD_NAME = "S3BenchmarksDeploy"

event = {"configure": {"test": 'asda', "second": "2"}}

# If region is in the event. Update the region in the cdk app file.
s3 = boto3.client('s3')
codebuild = boto3.client('codebuild', region_name='us-east-1')
response = codebuild.start_build(projectName=CODE_BUILD_NAME)

print(response)

# Create a cdk.context.json file for configuration
####
# Upload the file to code stored at s3://aws-crt-test-stuff/benchmarks-stack/

# Invoke code build to deploy the CDK stack
# os.system(
#     "./node_modules/aws-cdk/bin/cdk deploy -v -c ProjectName=aws-c-s3")
# print(event)
# print("## LOG ended")

# kwargs = {'name': 'S3BenchmarksDeploy',
# 'description': 'Deploy the benchmarks CFN for testing and dashboard',
# 'source':
# {'type': 'S3',
# 'location': 'aws-crt-test-stuff/benchmarks-stack/',
# 'insecureSsl': False
# },
# 'secondarySources': [],
# 'secondarySourceVersions': [],
# 'artifacts':
# {'type': 'NO_ARTIFACTS'},
# 'secondaryArtifacts': [],
# 'cache': {'type': 'NO_CACHE'},
# 'environment':
# {'type': 'LINUX_CONTAINER',
# 'image': '123124136734.dkr.ecr.us-east-1.amazonaws.com/aws-crt/nodejs:latest',
# 'computeType': 'BUILD_GENERAL1_SMALL',
# 'environmentVariables': [],
# 'privilegedMode': False,
# 'imagePullCredentialsType': 'CODEBUILD'},
# 'serviceRole': 'arn:aws:iam::123124136734:role/service-role/S3BenchmarksCodeBuildRole',
# 'timeoutInMinutes': 60,
# 'queuedTimeoutInMinutes': 480,
# 'encryptionKey': 'arn:aws:kms:us-east-1:123124136734:alias/aws/s3',
# 'tags': [],
# 'created': datetime.datetime(2021, 5, 20, 17, 46, 20, 219000, tzinfo=tzlocal()),
# 'lastModified': datetime.datetime(2021, 5, 21, 11, 46, 52, 688000, tzinfo=tzlocal()),
# 'badge': {'badgeEnabled': False},
# 'logsConfig': {'cloudWatchLogs': {'status': 'ENABLED'},
# 's3Logs': {'status': 'DISABLED', 'encryptionDisabled': False}},
# 'fileSystemLocations': []}
