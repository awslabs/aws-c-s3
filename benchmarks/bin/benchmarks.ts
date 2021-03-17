#!/usr/bin/env node

import 'source-map-support/register';
import * as cdk from '@aws-cdk/core';
import { BenchmarksStack } from '../lib/benchmarks-stack';
import { DashboardStack } from '../lib/dashboard-stack';

const app = new cdk.App();
let benchmarks_stack_name = app.node.tryGetContext('StackName') as string;
let dashboard_stack_name = app.node.tryGetContext('StackName') as string;

if(benchmarks_stack_name == null) {
    benchmarks_stack_name = 'BenchmarksStack';
}

if(dashboard_stack_name == null) {
    dashboard_stack_name = 'DashboardStack';
}

new BenchmarksStack(app, benchmarks_stack_name, { env: { region: process.env.CDK_DEFAULT_REGION, account: process.env.CDK_DEFAULT_ACCOUNT } });

new DashboardStack(app, dashboard_stack_name, { env: { region: process.env.CDK_DEFAULT_REGION, account: process.env.CDK_DEFAULT_ACCOUNT } });
