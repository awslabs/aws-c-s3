#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from '@aws-cdk/core';
import { BenchmarksStack } from '../lib/benchmarks-stack';

const app = new cdk.App();
const base_stack_name = app.node.tryGetContext('StackName') as string;
let benchmarks_stack_name = 'BenchmarksStack'

if (base_stack_name != null) {
  benchmarks_stack_name = benchmarks_stack_name + '-' + base_stack_name;
}

new BenchmarksStack(app, 'BenchmarksStack', { stackName: benchmarks_stack_name, env: { region: process.env.CDK_DEFAULT_REGION, account: process.env.CDK_DEFAULT_ACCOUNT } });
