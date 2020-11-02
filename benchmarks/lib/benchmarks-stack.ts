import * as cdk from '@aws-cdk/core';
import * as s3 from '@aws-cdk/aws-s3';
import * as ec2 from '@aws-cdk/aws-ec2';
import * as assets from '@aws-cdk/aws-s3-assets';
import * as iam from '@aws-cdk/aws-iam';
import * as path from 'path';
import * as fs from 'fs';

export class BenchmarksStack extends cdk.Stack {
  constructor(scope: cdk.Construct, id: string, props?: cdk.StackProps) {
    // props = props ? props : {
    //   env: {
    //     account: 'XXXXXXXXXXX',
    //     region: 'us-east-1'
    //   }
    // }

    super(scope, id, props);

    const instanceType = this.node.tryGetContext('InstanceType') as string;
    const downloads = this.node.tryGetContext('Downloads') as number
    const uploads = this.node.tryGetContext('Uploads') as number

    const s3BucketName = "aws-crt-canary-bucket" + ((this.region != 'us-west-2') ? '-' + this.region : '');

    // Write out canary config
    var canary_config = {
      "ToolName": "S3Canary",
      "InstanceType": instanceType,
      "Region": this.region,
      "BucketName": s3BucketName,
      "DownloadObjectName": "crt-canary-obj-single-part-9223372036854775807",
      "NumUpTransfers": uploads,
      "NumUpConcurrentTransfers": uploads,
      "NumDownTransfers": downloads,
      "NumDownConcurrentTransfers": downloads,
      "FileNameSuffixOffset": 0,
      "MetricsPublishingEnabled": true,
      "MeasureSinglePartTransfer": true
    }

    fs.writeFileSync(path.join(__dirname, 'canary.json'), JSON.stringify(canary_config));

    // Make canary script and config available as assets
    const canary_json = new assets.Asset(this, 'canary.json', {
      path: path.join(__dirname, 'canary.json')
    });
    const canary_sh = new assets.Asset(this, 'canary.sh', {
      path: path.join(__dirname, 'canary.sh')
    });

    const assetBucket = s3.Bucket.fromBucketName(this, 'AssetBucket', canary_sh.s3BucketName)

    const s3bucket = s3.Bucket.fromBucketName(this, 'BenchmarkBucket', s3BucketName);

    // Install canary script and config
    const instanceUserData = ec2.UserData.forLinux();
    const canary_sh_path = instanceUserData.addS3DownloadCommand({
      bucket: assetBucket,
      bucketKey: canary_sh.s3ObjectKey
    });
    const canary_json_path = instanceUserData.addS3DownloadCommand({
      bucket: assetBucket,
      bucketKey: canary_json.s3ObjectKey
    })
    instanceUserData.addExecuteFileCommand({
      filePath: canary_sh_path,
      arguments: canary_json_path
    });

    const vpc = ec2.Vpc.fromLookup(this, 'VPC', {
      vpcId: 'vpc-d96a98a3'
    });
    const security_group = new ec2.SecurityGroup(this, 'SecurityGroup', {
      vpc: vpc,
    });
    security_group.addIngressRule(
      ec2.Peer.anyIpv4(),
      ec2.Port.tcp(22),
      'SSH'
    );
    const ec2instance = new ec2.Instance(this, 'S3BenchmarkClient', {
      instanceType: new ec2.InstanceType(instanceType),
      vpc: vpc,
      machineImage: ec2.MachineImage.latestAmazonLinux(),
      userData: instanceUserData,
      role: iam.Role.fromRoleArn(this, 'S3CanaryInstanceRole', 'arn:aws:iam::123124136734:role/S3CanaryEC2Role'),
      keyName: 'aws-common-runtime-keys',
      securityGroup: security_group
    });

  }
}
