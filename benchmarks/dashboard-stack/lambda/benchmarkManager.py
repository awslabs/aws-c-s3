import boto3

CODE_BUILD_NAME = "S3BenchmarksDeploy"


def benchmarkManager(event, context):
    '''
    Lambda handler.
    Action in event determing how manager runs benchmark stack.

    delete: Delete a stack.
        - stack_name (string): the name of stack to delete.
    test: Deploy the stack via code build.
    '''
    cf_client = boto3.client('cloudformation')
    print("## LOG started")
    print(event)
    print("## event ends")
    if 'action' not in event:
        print("\'action\' is required in the event for BenchmarkManager")
        return {
            'statusCode': 400,
            'headers': {
                'Content-Type': 'text/plain'
            },
            'body': 'event {} is invalid, \
                \'action\' is required'.format(event)
        }
    if event['action'] == 'delete':
        if 'stack_name' in event:
            stack_name = event['stack_name']
        else:
            print("\'stack_name\' is required for delete action")
            return {
                'statusCode': 400,
                'headers': {
                    'Content-Type': 'text/plain'
                },
                'body': 'event {} is invalid, \
                    \'stack_name\' is required for delete action'.format(event)
            }
        print("Deleting stack, name is {}".format(stack_name))
        response = cf_client.delete_stack(
            StackName=stack_name)
        print("Delete stack response: {}".format(response))
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'text/plain'
            },
            'body': 'Deleting {}, it may fail, \
                check your consol to see it succeed or not'.format(stack_name)
        }
    elif event['action'] == 'test':
        # trigger codebuild to deploy the benchmarks stack
        codebuild = boto3.client('codebuild')
        response = codebuild.start_build(projectName=CODE_BUILD_NAME)
        print("Code build: Response: {}".format(response))
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'text/plain'
            },
            'body': '{} code build in process, \
                check logs if anything failed'.format(CODE_BUILD_NAME)
        }

    print("## LOG ended")
