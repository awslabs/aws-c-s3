#!/usr/bin/env bash

export USER_NAME=$1
export SHOW_INSTANCE_DASHBOARD_SCRIPT=$2
export RUN_PROJECT_TEMPLATE=$3
export PROJECT_NAME=$4
export BRANCH_NAME=$5
export THROUGHPUT_GBPS=$6
export PROJECT_SHELL_SCRIPT=$7
export INSTANCE_TYPE=$8
export REGION=$9

export RUN_PROJECT_LOG_FN=/tmp/benchmark.log
export PUBLISH_METRICS_LOG_FN=/tmp/publish_metrics.log
export SHOW_INSTANCE_DASHBOARD_USER_DEST=/home/$USER_NAME/show_instance_dashboard.sh
export RUN_PROJECT_SCRIPT=/home/$USER_NAME/run_project.sh

sudo chmod +x $SHOW_INSTANCE_DASHBOARD_SCRIPT
cp $SHOW_INSTANCE_DASHBOARD_SCRIPT $SHOW_INSTANCE_DASHBOARD_USER_DEST

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

cp $RUN_PROJECT_TEMPLATE $RUN_PROJECT_SCRIPT

sed -i 's/{PROJECT_NAME}/$PROJECT_NAME/g' $RUN_PROJECT_SCRIPT
sed -i 's/{PROJECT_SHELL_SCRIPT}/$PROJECT_SHELL_SCRIPT/g' $RUN_PROJECT_SCRIPT
sed -i 's/{BRANCH_NAME}/$BRANCH_NAME/g' $RUN_PROJECT_SCRIPT
sed -i 's/{INSTANCE_TYPE}/$INSTANCE_TYPE/g' $RUN_PROJECT_SCRIPT
sed -i 's/{PUBLISH_METRICS_LOG_FN}/$PUBLISH_METRICS_LOG_FN/g' $RUN_PROJECT_SCRIPT
sed -i 's/{RUN_PROJECT_LOG_FN}/$RUN_PROJECT_LOG_FN/g' $RUN_PROJECT_SCRIPT

sudo chmod +x $RUN_PROJECT_SCRIPT
$RUN_PROJECT_SCRIPT
