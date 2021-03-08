#!/usr/bin/env bash

export USER_NAME=$1
export SHOW_DASHBOARD_SCRIPT=$2
export PROJECT_NAME=$3
export BRANCH_NAME=$4
export THROUGHPUT_GBPS=$5
export PROJECT_SHELL_SCRIPT=$6
export INSTANCE_TYPE=$7
export REGION=$8

export BENCHMARK_LOG_FN=/tmp/benchmark.log
export PUBLISH_METRICS_LOG_FN=/tmp/publish_metrics.log

set -ex

function publish_bw_metric() {
    set -ex
    aws cloudwatch put-metric-data \
        --no-cli-pager \
        --namespace S3Benchmark \
        --metric-name BytesIn \
        --unit Bytes \
        --dimensions Project=$PROJECT_NAME,Branch=$BRANCH_NAME,InstanceType=$INSTANCE_TYPE \
        --storage-resolution 1 \
        --value $3 >> $PUBLISH_METRICS_LOG_FN
    aws cloudwatch put-metric-data \
        --no-cli-pager \
        --namespace S3Benchmark \
        --metric-name BytesOut \
        --unit Bytes \
        --dimensions Project=$PROJECT_NAME,Branch=$BRANCH_NAME,InstanceType=$INSTANCE_TYPE \
        --storage-resolution 1 \
        --value $2 >> $PUBLISH_METRICS_LOG_FN
}

export -f publish_bw_metric

sudo chmod +x $SHOW_DASHBOARD_SCRIPT
cp $SHOW_DASHBOARD_SCRIPT /home/$USER_NAME/show_dashboard.sh

sudo yum update -y
sudo yum install -y cmake3 git gcc clang htop tmux

sudo alternatives --install /usr/local/bin/cmake cmake /usr/bin/cmake3 100 \
    --slave /usr/local/bin/ctest ctest /usr/bin/ctest3 \
    --slave /usr/local/bin/cpack cpack /usr/bin/cpack3 \
    --slave /usr/local/bin/ccmake ccmake /usr/bin/ccmake3

sudo yum install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm
sudo yum-config-manager --enable epel
sudo yum install -y bwm-ng

curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install
rm -rf aws
rm -rf awscliv2.zip

INSTANCE_ID=`curl http://169.254.169.254/latest/meta-data/instance-id`
aws ec2 monitor-instances --instance-ids $INSTANCE_ID

sudo sysctl kernel.perf_event_paranoid=0

sudo chmod +x $PROJECT_SHELL_SCRIPT

$PROJECT_SHELL_SCRIPT 'SETUP'
$PROJECT_SHELL_SCRIPT 'RUN' > $BENCHMARK_LOG_FN &

stdbuf -i0 -o0 -e0 bwm-ng -I eth0 -o csv -u bits -d -c 0 \
    | stdbuf -o0 grep -v total \
    | stdbuf -o0 cut -f1,3,4 -d\; --output-delimiter=' ' \
    | xargs -n3 -t -P 32 bash -c 'publish_bw_metric "$@"' _ &
