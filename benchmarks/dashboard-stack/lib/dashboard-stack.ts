import * as cdk from '@aws-cdk/core';
import * as cloudwatch from '@aws-cdk/aws-cloudwatch';
import * as iam from '@aws-cdk/aws-iam';
import * as _lambda from '@aws-cdk/aws-lambda';
import * as codebuild from '@aws-cdk/aws-codebuild';
import * as events from '@aws-cdk/aws-events';
import * as targets from '@aws-cdk/aws-events-targets';
import * as s3 from '@aws-cdk/aws-s3';
import * as ec2 from '@aws-cdk/aws-ec2';
import * as s3deploy from '@aws-cdk/aws-s3-deployment'
import * as path from 'path';
import * as fs from 'fs';

import { KeyPair } from 'cdk-ec2-key-pair';

function policy_doc_helper(path: string): iam.PolicyDocument {
    const policy_doc_json = fs.readFileSync(path, 'utf8');
    const policy_doc = JSON.parse(policy_doc_json);
    return iam.PolicyDocument.fromJson(policy_doc);
}

export class DashboardStack extends cdk.Stack {
    constructor(scope: cdk.Construct, id: string, props?: cdk.StackProps) {
        super(scope, id, props);

        const benchmark_config_json_path = path.join(
            __dirname,
            "..",
            "..",
            "benchmark-config.json"
        );
        const benchmark_config_json = fs.readFileSync(benchmark_config_json_path, 'utf8');
        const benchmark_config = JSON.parse(benchmark_config_json);

        // Lambda to handle trigger codebuild to deploy benchmark and clean up benchmark after tests
        // use admin role here. TODO: simplify later.
        // Permission to delete CFN stack with ec2,s3, iam and security group in it and invoke codebuild.
        const admin_policy_doc = policy_doc_helper(path.join(
            __dirname,
            "policy-doc",
            "admin-policy-doc.json"
        ));

        const lambda_role = new iam.Role(this, 'LambdaRole', {
            assumedBy: new iam.ServicePrincipal("lambda.amazonaws.com"),
            inlinePolicies: { "AdminPolicy": admin_policy_doc }
        });

        const lambda = new _lambda.Function(this, 'BenchmarkManager',
            {
                runtime: _lambda.Runtime.PYTHON_3_8,
                code: _lambda.Code.fromAsset("lambda"),
                handler: "benchmarkManager.benchmarkManager",
                role: lambda_role,
                functionName: "BenchmarkManager",
                timeout: cdk.Duration.minutes(15)
            });


        // Creat the Cloudwatch Event to schedule the lambda to run daily
        const lambda_target = new targets.LambdaFunction(lambda, {
            event: events.RuleTargetInput.fromObject({ 'action': 'test' })
        });

        new events.Rule(this, 'ScheduleRule', {
            schedule: events.Schedule.rate(cdk.Duration.days(1)),
            targets: [lambda_target],
            ruleName: 'BenchmarksTrigger',
            description: 'Trigger Benchmarks test.'
        });

        let region = 'unknown';

        if (props != undefined && props.env != undefined && props.env.region != undefined) {
            region = props.env.region;
        }

        const vpc = new ec2.Vpc(this, 'VPC', {
            enableDnsSupport: true,
            enableDnsHostnames: true
        })

        cdk.Tags.of(vpc).add('S3BenchmarkResources', 'DashboardVPC');

        const metrics_namespace = "S3Benchmark";

        const metric_widget_width = 6;
        const metric_widget_height = 6;
        const num_widgets_per_row = 4;

        let x = 0;
        let y = 0;

        let dashboard_body = {
            widgets: [] as any
        };

        for (let project_name in benchmark_config.projects) {
            const project_config = benchmark_config.projects[project_name];
            const branch_name = project_config.branch;

            let project_header_widget = {
                type: "text",
                width: 24,
                height: 1,
                properties: {
                    markdown: "## " + project_name + " (" + branch_name + ")"
                }
            };

            dashboard_body.widgets.push(project_header_widget);

            for (let instance_config_name in benchmark_config.instances) {
                const instance_config = benchmark_config.instances[instance_config_name];

                if (x >= num_widgets_per_row) {
                    x = 0;
                    y += metric_widget_height;
                }

                let instance_widget = {
                    type: "metric",
                    width: metric_widget_width,
                    height: metric_widget_height,
                    properties: {
                        metrics: [
                            [
                                metrics_namespace,
                                "BytesIn",
                                "Project",
                                project_name,
                                "Branch",
                                branch_name,
                                "InstanceType",
                                instance_config_name,
                                {
                                    id: "m1",
                                    visible: false,
                                }
                            ],
                            [
                                metrics_namespace,
                                "BytesOut",
                                "Project",
                                project_name,
                                "Branch",
                                branch_name,
                                "InstanceType",
                                instance_config_name,
                                {
                                    id: "m2",
                                    visible: false,
                                }
                            ],
                            [
                                {
                                    expression: "m1*8/1000/1000/1000",
                                    "label": "Gbps Download",
                                    "id": "e1",
                                    color: "#0047ab"
                                }
                            ],
                            [
                                {
                                    expression: "m2*8/1000/1000/1000",
                                    "label": "Gbps Upload",
                                    "id": "e2",
                                    color: "#ffa500"
                                }
                            ]
                        ],
                        yAxis: {
                            showUnits: false
                        },
                        stat: "Average",
                        period: 1,
                        region: region,
                        title: instance_config_name
                    }
                };

                dashboard_body.widgets.push(instance_widget);
            }
        }

        const dashboard = new cloudwatch.CfnDashboard(this, id, {
            dashboardBody: JSON.stringify(dashboard_body),
            dashboardName: id + "_" + region
        });

        // Permission to create CFN stack with ec2,s3, iam and security group in it.
        // TODO: simplify it later. Use admin policy for simplicity now.
        const codebuild_role = new iam.Role(this, 'CodeBuildRole', {
            assumedBy: new iam.ServicePrincipal("codebuild.amazonaws.com"),
            inlinePolicies: { "AdminPolicy": admin_policy_doc }
        });

        const code_bucket = new s3.Bucket(this, 'CodeBucket', {
            removalPolicy: cdk.RemovalPolicy.DESTROY,
            autoDeleteObjects: true,
        });

        //Write the config for deploy benchmarks-stack
        fs.writeFileSync(path.join(
            __dirname,
            "..",
            "..",
            "benchmarks-stack",
            "benchmarks-stack",
            "lib",
            "benchmark-config.json"
        ), benchmark_config_json);

        new s3deploy.BucketDeployment(this, 'DeployCodeBase', {
            sources: [s3deploy.Source.asset('../benchmarks-stack')],
            destinationBucket: code_bucket
        });

        new codebuild.Project(this, 'S3BenchmarksDeploy', {
            source: codebuild.Source.s3({
                bucket: code_bucket,
                path: 'benchmarks-stack/',
            }),
            environment: {
                buildImage: codebuild.LinuxBuildImage.fromCodeBuildImageId('aws/codebuild/standard:5.0'),
            },
            role: codebuild_role,
            projectName: "S3BenchmarksDeploy"
        });


        if (benchmark_config["key-pair-name"] == undefined) {
            // Create the Key Pair
            const key = new KeyPair(this, 'EC2CanaryKeyPair', {
                name: 'S3-EC2-Canary-key-pair',
                description: 'Key pair for BenchMarks stack to launch Ec2 instance. The private key is stored in \
             Secrets Manager as ec2-ssh-key/S3-EC2-Canary-key-pair/private',
                storePublicKey: true, // by default the public key will not be stored in Secrets Manager
            });
        }
    }
}
