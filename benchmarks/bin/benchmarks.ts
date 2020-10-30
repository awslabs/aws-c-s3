#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from '@aws-cdk/core';
import { BenchmarksStack } from '../lib/benchmarks-stack';

const app = new cdk.App();
new BenchmarksStack(app, 'BenchmarksStack');
