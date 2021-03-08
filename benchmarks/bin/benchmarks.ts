#!/usr/bin/env node

import 'source-map-support/register';
import * as cdk from '@aws-cdk/core';
import { BenchmarksStack } from '../lib/benchmarks-stack';
import { DashboardStack } from '../lib/dashboard-stack';

const app = new cdk.App();

new BenchmarksStack(app, 'BenchmarksStack', { env: { region: process.env.CDK_DEFAULT_REGION, account: process.env.CDK_DEFAULT_ACCOUNT } });

new DashboardStack(app, 'DashboardStack', { env: { region: process.env.CDK_DEFAULT_REGION, account: process.env.CDK_DEFAULT_ACCOUNT } });
