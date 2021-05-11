import * as cdk from '@aws-cdk/core';
import * as cloudwatch from '@aws-cdk/aws-cloudwatch';
import * as iam from '@aws-cdk/aws-iam';
import * as _lambda from '@aws-cdk/aws-lambda';
import * as path from 'path';
import * as fs from 'fs';

export class DashboardStack extends cdk.Stack {
    constructor(scope: cdk.Construct, id: string, props?: cdk.StackProps) {
        super(scope, id, props);

        const benchmark_config_json = fs.readFileSync(path.join(__dirname, 'benchmark-config.json'), 'utf8');
        const benchmark_config = JSON.parse(benchmark_config_json);

        const lambda_role = iam.Role.fromRoleArn(this, 'LambdaRole', 'arn:aws:iam::123124136734:role/lambda-role-CFN');
        const lambda = new _lambda.Function(this, 'BenchmarkManager',
            {
                runtime: _lambda.Runtime.PYTHON_3_8,
                code: _lambda.Code.fromAsset("lambda"),
                handler: "benchmark.benchmarkManger",
                role: lambda_role,
                functionName: "BenchmarkManager"
            });

        let region = 'unknown';

        if (props != undefined && props.env != undefined && props.env.region != undefined) {
            region = props.env.region;
        }

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
            dashboardName: id
        });
    }
}
