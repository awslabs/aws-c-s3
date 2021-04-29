
import * as _lambda from '@aws-cdk/aws-lambda';
import * as cdk from '@aws-cdk/core';

export class LambdaStack extends cdk.Stack {
    constructor(scope: cdk.Construct, id: string, props?: cdk.StackProps) {
        super(scope, id, props);
        const lambda = new _lambda.Function(this, 'BenchmarkManager',
            {
                runtime: _lambda.Runtime.PYTHON_3_8,
                code: _lambda.Code.fromAsset("lambda"),
                handler: "benchmark.benchmarkManger"
            });
    }
}
