import * as cdk from '@aws-cdk/core';
import * as s3 from '@aws-cdk/aws-s3';
import * as ec2 from '@aws-cdk/aws-ec2';
import * as assets from '@aws-cdk/aws-s3-assets';
import * as iam from '@aws-cdk/aws-iam';
import * as path from 'path';
import * as fs from 'fs';

export class BenchmarksStack extends cdk.Stack {
  constructor(scope: cdk.Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // Set by the lambda function running the stack
    const user_name = this.node.tryGetContext('UserName') as string;
    const project_name = this.node.tryGetContext('ProjectName') as string;
    const cidr = this.node.tryGetContext('CIDRRange') as string;
    const instance_config_name = this.node.tryGetContext('InstanceConfigName') as string;
    const throughput_gbps = this.node.tryGetContext('ThroughputGbps') as string;
    const auto_tear_down = this.node.tryGetContext('AutoTearDown') as string;

    const benchmark_config_json = fs.readFileSync(path.join(__dirname, 'benchmark-config.json'), 'utf8')
    const benchmark_config = JSON.parse(benchmark_config_json)
    const project_config = benchmark_config.projects[project_name];
    const branch_name = project_config.branch;

    let region = 'unknown';

    if (props != undefined && props.env != undefined && props.env.region != undefined) {
      region = props.env.region;
    }

    const init_instance_sh = new assets.Asset(this, 'init_instance.sh', {
      path: path.join(__dirname, 'init_instance.sh')
    });

    const show_instance_dashboard_sh = new assets.Asset(this, 'show_instance_dashboard.sh', {
      path: path.join(__dirname, 'show_instance_dashboard.sh')
    });

    const run_project_template_sh = new assets.Asset(this, 'run_project_template.sh', {
      path: path.join(__dirname, 'run_project_template.sh')
    });

    const get_p90_py = new assets.Asset(this, 'get_p90.py', {
      path: path.join(__dirname, 'get_p90.py')
    });

    const project_shell_script_sh = new assets.Asset(this, project_config.shell_script, {
      path: path.join(__dirname, project_config.shell_script)
    });

    const assetBucket = s3.Bucket.fromBucketName(this, 'AssetBucket', init_instance_sh.s3BucketName)

    /**
     * This bucket already exists in the aws crt team account.
     * It has lifetime rules to delete objects older than a day.
     * If you are trying to run this stack in a different account,
     * you will have to create a bucket and change this variable name to the bucket name.
     */
    const canaryBucketName = "crt-s3canary-bucket-123124136734";

    const vpc = ec2.Vpc.fromLookup(this, 'VPC', {
      tags: { 'S3CanaryResources': 'VPC' }
    });

    const subnetSelection: ec2.SubnetSelection = {
      subnets: vpc.publicSubnets
    };

    const security_group = new ec2.SecurityGroup(this, 'SecurityGroup', {
      vpc: vpc,
    });

    security_group.addIngressRule(
      cidr ? ec2.Peer.ipv4(cidr) : ec2.Peer.anyIpv4(),
      ec2.Port.tcp(22),
      'SSH'
    );

    const policy_doc_json_path = path.join(
      __dirname,
      "canary-policy-doc.json"
    );
    const policy_doc_json = fs.readFileSync(policy_doc_json_path, 'utf8');
    const policy_doc_obj = JSON.parse(policy_doc_json);
    const policy_doc = iam.PolicyDocument.fromJson(policy_doc_obj);

    const canary_role = new iam.Role(this, 'EC2CanaryRole', {
      assumedBy: new iam.ServicePrincipal("ec2.amazonaws.com"),
      inlinePolicies: { "CanaryEC2Policy": policy_doc }
    });

    const project_run_commands = [
      "DOWNLOAD_PERFORMANCE",
      "UPLOAD_PERFORMANCE",
    ];

    const key_name = benchmark_config["key-pair-name"];

    for (let run_command_index in project_run_commands) {
      const run_command = project_run_commands[run_command_index];
      const instance_user_data = ec2.UserData.forLinux();

      const init_instance_sh_path = instance_user_data.addS3DownloadCommand({
        bucket: assetBucket,
        bucketKey: init_instance_sh.s3ObjectKey
      });

      const show_instance_dashboard_sh_path = instance_user_data.addS3DownloadCommand({
        bucket: assetBucket,
        bucketKey: show_instance_dashboard_sh.s3ObjectKey
      });

      const run_project_template_sh_path = instance_user_data.addS3DownloadCommand({
        bucket: assetBucket,
        bucketKey: run_project_template_sh.s3ObjectKey
      });

      const project_shell_script_sh_path = instance_user_data.addS3DownloadCommand({
        bucket: assetBucket,
        bucketKey: project_shell_script_sh.s3ObjectKey
      })

      const get_p90_py_path = instance_user_data.addS3DownloadCommand({
        bucket: assetBucket,
        bucketKey: get_p90_py.s3ObjectKey
      })

      const init_instance_arguments = user_name + ' ' +
        show_instance_dashboard_sh_path + ' ' +
        run_project_template_sh_path + ' ' +
        project_name + ' ' +
        branch_name + ' ' +
        throughput_gbps + ' ' +
        project_shell_script_sh_path + ' ' +
        instance_config_name + ' ' +
        region + ' ' +
        run_command + ' ' +
        this.stackName + ' ' +
        canaryBucketName + ' ' +
        get_p90_py_path + ' ' +
        auto_tear_down;

      instance_user_data.addExecuteFileCommand({
        filePath: init_instance_sh_path,
        arguments: init_instance_arguments
      });

      const ec2instance = new ec2.Instance(this, 'S3BenchmarkClient_' + this.stackName + "_" + instance_config_name + "_" + run_command, {
        instanceType: new ec2.InstanceType(instance_config_name),
        vpc: vpc,
        machineImage: ec2.MachineImage.latestAmazonLinux({ generation: ec2.AmazonLinuxGeneration.AMAZON_LINUX_2 }),
        userData: instance_user_data,
        role: canary_role,
        keyName: key_name ? key_name : 'S3-EC2-Canary-key-pair',
        securityGroup: security_group,
        vpcSubnets: subnetSelection,
        requireImdsv2: true
      });
    }

  }
}
