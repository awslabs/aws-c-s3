import { expect as expectCDK, haveResourceLike } from '@aws-cdk/assert';
import * as cdk from '@aws-cdk/core';
import * as Benchmarks from '../lib/benchmarks-stack';

test('CIDRRange default', () => {
  const anyCIDR = "0.0.0.0/0";
  const app = new cdk.App({ context: { "UserName": 'ec2-user', "ProjectName": 'aws-c-s3' } });

  const stack = new Benchmarks.BenchmarksStack(app, 'MyTestStack');

  expectCDK(stack).to(haveResourceLike("AWS::EC2::SecurityGroup", {
    "SecurityGroupIngress": [{
      "CidrIp": anyCIDR,
      "Description": "SSH",
      "FromPort": 22,
      "IpProtocol": "tcp",
      "ToPort": 22
    }]
  }))

});


test('CIDRRange configured', () => {
  const CIDR = '172.31.0.0/24';
  const app = new cdk.App({ context: { "UserName": 'ec2-user', "ProjectName": 'aws-c-s3', "CIDRRange": CIDR } });

  const stack = new Benchmarks.BenchmarksStack(app, 'MyTestStack');

  expectCDK(stack).to(haveResourceLike("AWS::EC2::SecurityGroup", {
    "SecurityGroupIngress": [{
      "CidrIp": CIDR,
      "Description": "SSH",
      "FromPort": 22,
      "IpProtocol": "tcp",
      "ToPort": 22
    }]
  }))

});
