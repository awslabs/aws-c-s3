# S3 Benchmark CDK kit

This will deploy an EC2 instance with the S3Canary on it, and will dump a run to CloudWatch
when the instance boots.

## Pre-request

* Node
* npm 7.0+
* aws CDK 1.103.0+ (`npm install -g aws-cdk@1.103.0`)
* AWS account with CDK (`cdk bootstrap aws://<ACCOUNT-ID>/<REGION>`)

## Structures

### Dashboard-stack

The stack deploys dashboard and all the other resources for Benchmarks-stack. It will deploy a cloudwatch event to trigger the Benchmarks-stack running daily and clean it up after each test. Usually, user will need to manually deploy this stack by following steps:

* `cd dashboard-stack/` change directory to `dashboard-stack/`. If the current directory is not `aws-c-s3/benchmarks`, use the correct path instead.
* `npm install`         install the dependencies
* `npm run build`       compile typescript to js
* `cdk deploy`          deploy this stack to your default AWS account/region (Use aws cli to setup the default AWS account via `aws configure`)

Will use following resource:

* Code build: Deploy the Benchmarks-stack.
* Lambda function: Invoke code build and automatically delete Benchmarks-stack after testing.
* Cloud Watch Event: Schedule the Benchmark test to run daily.
* EC2 VPC: Control the VPC of the EC2 instances in Benchmarks-stack.
* Cloud Watch Dashboard: Log and visualize the performance test result.
* IAM roles: permission of the services.
* Key Management Service: Generate the Key-pair for the EC2 instance created by Benchmarks-stack.
* Secret Manger: Store the key-pair generated. To get the key generated via aws CLI `aws secretsmanager get-secret-value --secret-id ec2-ssh-key/S3-EC2-Canary-key-pair/private`.

### Benchmarks-stack

The stack deploy the ec2 instance with the S3Canary on it, and will dump a run to CloudWatch
when the instance boots. Usually controlled by dashboard-stack, user don't need to touch anything in it.

### Configuration

The configuration for `benchmarks-stack` are listed here **(TO BE FINALIZED)**, which is the defined context value for cdk to deploy:

* StackName (string): Name of the stack to be created
* UserName (string): *Optional* default: ec2-user
* ProjectName (string): The project BenchmarkStack runs on.
* CIDRRange (string): *Optional* The inbound IP range for the ec2 instances created by the stack.
* InstanceConfigName (string): The ec2 instance type to create
* ThroughputGbps (string): String of the thought put target in Gbp
* AutoTearDown (1 or 0): Whether to tear down the benchmarks stack after test or not, default: 1

The configuration for `benchmark-config.json` are listed here **(TO BE FINALIZED)**, which is defined :

* `projects`, the name of the project to run, eg `aws-crt-java`, will run the performance test within aws-crt-java, which can be found [here](https://github.com/awslabs/aws-crt-java).
  * Each projects contains configuration for the project:
    * `shell_script`: the script to run the test.
    * `branch`: the branch of the repo that test against.
    * `s3_bucket_name`: the S3 bucket to use for the test
* `instances`: The ec2 instance type to create
  * `throughput_gbps`: the target throughput to test against
* `auto-tear-down`: Tear down the tests automatically after it runs or not.
* `key-pair-name`: Set to the key pair name to an existing EC2 key pair for the EC2 instance to use, if not set, CDK will create one and it can be accessed via aws CLI `aws secretsmanager get-secret-value --secret-id ec2-ssh-key/S3-EC2-Canary-key-pair/private`

The dashboard-stack has been deployed to AWS account associated with aws-sdk-common-runtime@amazon.com at us-west-2. If you have access to that and need to update the config (eg: test performance from another branch), you can update `aws-c-s3/benchmarks/benchmark-config.json` and rerun the deploy steps for dashboard stack as shown above.

Note: Once you deploy the stack, the benchmark stack will be trigger by event that runs once per day. If you want to trigger the benchmark stack to run performance test, you can manually start the codebuild job that associated with the dashboard stack.
