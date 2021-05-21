import json
import time
import boto3


def benchmarkManger(event, context):
    cf_client = boto3.client('cloudformation')
    if event['action'] == 'delete':
        if 'stack_name' in event:
            stack_name = event['stack_name']
        else:
            stack_name = 'BenchmarksStack-aws-c-s3-c5n'
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
    print(event)
