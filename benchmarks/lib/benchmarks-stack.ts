import * as cdk from '@aws-cdk/core';
import * as s3 from '@aws-cdk/aws-s3';
import * as ec2 from '@aws-cdk/aws-ec2';
import * as assets from '@aws-cdk/aws-s3-assets';
import * as path from 'path';
import * as fs from 'fs';




export class BenchmarksStack extends cdk.Stack {
  constructor(scope: cdk.Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const instanceType = new cdk.CfnParameter(this, 'InstanceType', {
      type: "String",
      description: "EC2 Instance Type to test",
      default: 'c5n.18xlarge'
    });

    const downloads = new cdk.CfnParameter(this, 'Downloads', {
      type: "Number",
      description: "1.6 x N Gbps expected bandwidth",
      default: '160'
    });

    const uploads = new cdk.CfnParameter(this, 'Uploads', {
      type: "Number",
      description: "Number of uploads to do, 0 disables",
      default: 0
    });

    const s3BucketName = "aws-crt-canary-bucket" + (this.region != 'us-east-1') ? '-' + this.region : '';

    // Write out canary config
    var canary_config = {
      "ToolName": "S3Canary",
      "InstanceType": instanceType.valueAsString,
      "Region": this.region,
      "BucketName": s3BucketName,
      "DownloadObjectName": "crt-canary-obj-single-part-9223372036854775807",
      "NumUpTransfers": uploads.valueAsNumber,
      "MumUpConcurrentTransfers": uploads.valueAsNumber,
      "NumDownTransfers": downloads.valueAsNumber,
      "NumDownConcurrentTransfers": downloads.valueAsNumber,
      "FileNameSuffixOffset": 0,
      "MetricsPublishingEnabled": true,
      "MeasureSinglePartTransfer": true
    }

    fs.writeFileSync(path.join(__dirname, 'canary.json'), canary_config);

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

    const ec2instance = new ec2.Instance(this, 'BenchmarkClientInstance', {
      instanceType: new ec2.InstanceType('c5n.18xlarge'),
      vpc: ec2.Vpc.fromLookup(this, 'VPC', {
        vpcId: 'vpc-d96a98a3'
      }),
      machineImage: ec2.MachineImage.latestAmazonLinux(),
      userData: instanceUserData
    });

  }
}
