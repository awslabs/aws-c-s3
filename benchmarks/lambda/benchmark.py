import json
import time


def benchmarkManger(event, context):
    # read through the benchmark configure.
    # The configure can be the event
    # Import json as package.
    # OR the json file can be
    # run cdk deploy for benchmark? How.
    # os.

    # This is a template
    print("Lambda function ARN:", context.invoked_function_arn)
    print("CloudWatch log stream name:", context.log_stream_name)
    print("CloudWatch log group name:",  context.log_group_name)
    print("Lambda Request ID:", context.aws_request_id)
    print("Lambda function memory limits in MB:", context.memory_limit_in_mb)
    # We have added a 1 second delay so you can see the time remaining in get_remaining_time_in_millis.
    time.sleep(1)
    print("Lambda time remaining in MS:",
          context.get_remaining_time_in_millis())
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'text/plain'
        },
        'body': 'Hello, CDK! You have hit {}\n'.format(event['path'])
    }
