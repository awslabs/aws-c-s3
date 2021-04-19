import { expect as expectCDK, matchTemplate, MatchStyle, haveResource } from '@aws-cdk/assert';
import * as cdk from '@aws-cdk/core';
import * as Benchmarks from '../lib/benchmarks-stack';

test('CIDRRange configured', () => {
  const app = new cdk.App({ context: { "UserName": 'ec2-user', "ProjectName": 'aws-c-s3', "CIDRRange": '172.31.0.0/24' } });

  // WHEN
  const stack = new Benchmarks.BenchmarksStack(app, 'MyTestStack');
  // THEN
  expectCDK(stack).to(haveResource("AWS::EC2::VPC", { "CidrBlock": '172.31.0.0/24' }))

});
