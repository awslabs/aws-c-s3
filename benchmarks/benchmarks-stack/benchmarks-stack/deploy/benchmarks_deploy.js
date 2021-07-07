const AWS = require("aws-sdk");
const { execSync } = require("child_process");
const path = require("path");
const fs = require("fs");

const benchmark_config_json = fs.readFileSync(
  path.join(__dirname, "..", "lib", "benchmark-config.json"),
  "utf8"
);
const benchmark_config = JSON.parse(benchmark_config_json);
let auto_tear_down = "1";
if (benchmark_config["auto-tear-down"] != undefined) {
  auto_tear_down = benchmark_config["auto-tear-down"] ? "1" : "0";
}
const auto_tear_down_config = " -c AutoTearDown=" + auto_tear_down;

// Configurations for the Stack. Keys listed as below:
// - StackName (string): Name of the stack to be created
// - UserName (string): default: ec2-user
// - ProjectName (string): The project BenchmarkStack runs on. eg: aws-crt-java
// - CIDRRange (string): The inbound IP range for the ec2 instances created by the stack.
// - InstanceConfigName (string): The ec2 instance type to create, default: c5n.18xlarge
// - ThroughputGbps (string): String of the thought put target in Gbps, default: 100
// - AutoTearDown (1 or 0): Whether to tear down the benchmarks stack after test or not, default: 1
for (let project_name in benchmark_config.projects) {
  const project_name_config = " -c ProjectName=" + project_name;
  let instance_count = 0;
  for (let instance_config_name in benchmark_config.instances) {
    instance_count++;
    const instance_config = benchmark_config.instances[instance_config_name];
    const instance_config_name_config =
      " -c InstanceConfigName=" + instance_config_name;
    const throuphput_gbps_config =
      " -c ThroughputGbps=" + instance_config["throughput_gbps"];

    let cmd =
      "./node_modules/aws-cdk/bin/cdk deploy -v --require-approval never -c UserName=ec2-user";
    const name_config =
      " -c StackName=" + project_name + "-instance-" + instance_count;
    cmd =
      cmd +
      project_name_config +
      instance_config_name_config +
      throuphput_gbps_config +
      name_config +
      auto_tear_down_config;
    result = execSync(cmd).toString();
    console.log(result);
  }
}
