import json
import os
import boto3
import re
from botocore.exceptions import ClientError

BUCKET_NAME = 'aws-crt-test-stuff'
CDK_STACK_PATH = 'benchmarks-stack/'
CDK_APP_PATH = CDK_STACK_PATH + 'bin/benchmarks-stack.ts'
CDK_CONTEXT_PATH = CDK_STACK_PATH + 'cdk.context.json'
CODE_BUILD_NAME = "S3BenchmarksDeploy"


def update_cdk_region(s3, region):
    '''
    Helper function to update the region in CDK app. s3 (boto3 s3 client)
    '''
    # download the benchmarks-stack.ts from code base in S3
    cdk_app_file = "cdk_app.ts"
    with open(cdk_app_file, 'wb') as f:
        s3.download_fileobj(BUCKET_NAME, CDK_APP_PATH, f)

    # Read from the file into memory (Maybe we should just download into memory, which is not supported by boto3 now)
    contents = None
    with open(cdk_app_file, 'r+') as f:
        contents = f.read()

    # Update the region in the file and write back to the file
    contents = re.sub(r"region: \".\"",
                      "region: \"{}\"".format(region), contents)
    with open(cdk_app_file, 'w') as f:
        f.write(contents)

    # Upload the local file to s3 and overwrite it
    s3.upload_file(cdk_app_file, BUCKET_NAME, CDK_APP_PATH)


def benchmarkManager(event, context):
    '''
    Lambda handler.
    Action in event determing how manager runs benchmark stack.

    delete: Delete a stack.
        - stack_name (string): the name of stack to delete. Default name is `BenchmarksStack`
    test: Deploy the stack via code build.
        - region (string): The region to deploy BenmarkStack
        - configure (map): Configurations for the Stack. Keys listed as below:
            - UserName (string): default: ec2-user
            - ProjectName (string): The project BenchmarkStack runs on. eg: aws-crt-java
            - CIDRRange (string): The inbound IP range for the ec2 instances created by the stack.
            - InstanceConfigName (string): The ec2 instance type to create, default: c5n.18xlarge
            - ThroughputGbps (string): String of the thought put target in Gbps, default: 100
    '''
    cf_client = boto3.client('cloudformation')
    print("## LOG started")
    print(event)
    print("## event ends")
    if event['action'] == 'delete':
        if 'stack_name' in event:
            stack_name = event['stack_name']
        else:
            stack_name = 'BenchmarksStack'
        print(stack_name)
        print(cf_client.delete_stack(
            StackName=stack_name
        ))
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'text/plain'
            },
            'body': 'Deleting {}, it may fail, check your consol to see it succeed or not'.format(stack_name)
        }
    elif event['action'] == 'test':
        s3 = boto3.client('s3')
        # If region is in the event. Update the region in the cdk app file.
        if 'region' in event:
            region = event['region']
            update_cdk_region(s3, region)

        config_file = "cdk_conf.json"
        configure = event["configure"]
        # Create a cdk.context.json file for configuration
        with open(config_file, "w") as f:
            json.dump(configure, f)
        # Upload the file to code stored at s3://aws-crt-test-stuff/benchmarks-stack/
        s3.upload_file(config_file, BUCKET_NAME, CDK_CONTEXT_PATH)

        # The configured codebuild live in us-east-1
        codebuild = boto3.client('codebuild', region_name='us-east-1')
        response = codebuild.start_build(projectName=CODE_BUILD_NAME)
        print("Code build: Response: {}".format(response))

    print(event)
    print("## LOG ended")
