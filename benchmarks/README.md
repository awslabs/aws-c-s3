# S3 Benchmark CDK kit

This will deploy an EC2 instance with the S3Canary on it, and will dump a run to CloudWatch
when the instance boots.

## Parameters:
 * -c Uploads=N Where N is >= 0. 0 disables upload benchmark
 * -c Downloads=N Where N is >= 0. 0 disables download benchmark
      Should be about 1.6x your expected instance bandwidth
 * -c InstanceType=I Where I is a valid ec2 instance type. Deployment will fail if CFN
      can't work out the instance type


## Useful commands

 * `npm run build`   compile typescript to js
 * `npm run watch`   watch for changes and compile
 * `npm run test`    perform the jest unit tests
 * `cdk deploy`      deploy this stack to your default AWS account/region
 * `cdk diff`        compare deployed stack with current state
 * `cdk synth`       emits the synthesized CloudFormation template
