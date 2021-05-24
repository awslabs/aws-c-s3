#!/usr/bin/env node

import 'source-map-support/register';
import * as cdk from '@aws-cdk/core';
import { DashboardStack } from '../lib/dashboard-stack';

const app = new cdk.App();
const base_stack_name = app.node.tryGetContext('StackName') as string;
let dashboard_stack_name = 'DashboardStack';

if (base_stack_name != null) {
    dashboard_stack_name = dashboard_stack_name + '-' + base_stack_name;
}

new DashboardStack(app, 'DashboardStack', { stackName: dashboard_stack_name, env: { region: process.env.CDK_DEFAULT_REGION, account: process.env.CDK_DEFAULT_ACCOUNT } });
