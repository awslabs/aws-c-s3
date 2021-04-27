import { expect as expectCDK, matchTemplate, MatchStyle, haveResource } from '@aws-cdk/assert';
import * as cdk from '@aws-cdk/core';
import * as ec2 from '@aws-cdk/aws-ec2';
import * as Benchmarks from '../lib/benchmarks-stack';

test('CIDRRange default', () => {
  const app = new cdk.App({ context: { "UserName": 'ec2-user', "ProjectName": 'aws-c-s3' } });

  const stack = new Benchmarks.BenchmarksStack(app, 'MyTestStack');

  expectCDK(stack).to(haveResource("AWS::EC2::VPC", { "CidrBlock": ec2.Vpc.DEFAULT_CIDR_RANGE }))

});


test('CIDRRange configured', () => {
  const app = new cdk.App({ context: { "UserName": 'ec2-user', "ProjectName": 'aws-c-s3', "CIDRRange": '172.31.0.0/24' } });

  const stack = new Benchmarks.BenchmarksStack(app, 'MyTestStack');

  expectCDK(stack).to(haveResource("AWS::EC2::VPC", { "CidrBlock": '172.31.0.0/24' }))

});
