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
export RUN_COMMAND=${10}

export RUN_PROJECT_LOG_FN=/tmp/benchmark.log
export PUBLISH_METRICS_LOG_FN=/tmp/publish_metrics.log
export SHOW_INSTANCE_DASHBOARD_USER_DEST=/home/$USER_NAME/show_instance_dashboard.sh
export PERF_SCRIPT_TEMP=/tmp/perf_script_temp.tmp
export DOWNLOAD_PERF_SCRIPT=/home/$USER_NAME/download_performance.sh
export UPLOAD_PERF_SCRIPT=/home/$USER_NAME/upload_performance.sh

sudo yum update -y
sudo yum install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm
sudo yum-config-manager --enable epel

sudo yum install -y tmux bwm-ng htop

sudo chmod +x $SHOW_INSTANCE_DASHBOARD_SCRIPT
cp $SHOW_INSTANCE_DASHBOARD_SCRIPT $SHOW_INSTANCE_DASHBOARD_USER_DEST

sudo yum install -y cmake3 git gcc clang

sudo alternatives --install /usr/local/bin/cmake cmake /usr/bin/cmake3 100 \
    --slave /usr/local/bin/ctest ctest /usr/bin/ctest3 \
    --slave /usr/local/bin/cpack cpack /usr/bin/cpack3 \
    --slave /usr/local/bin/ccmake ccmake /usr/bin/ccmake3

curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install
rm -rf aws
rm -rf awscliv2.zip

INSTANCE_ID=`curl http://169.254.169.254/latest/meta-data/instance-id`
aws ec2 monitor-instances --instance-ids $INSTANCE_ID

sudo sysctl kernel.perf_event_paranoid=0

sudo chmod +x $PROJECT_SHELL_SCRIPT
${PROJECT_SHELL_SCRIPT} 'SETUP'

AWK_SCRIPT="{"
AWK_SCRIPT="$AWK_SCRIPT sub(\"{PROJECT_NAME}\", \"$PROJECT_NAME\");";
AWK_SCRIPT="$AWK_SCRIPT sub(\"{PROJECT_SHELL_SCRIPT}\", \"$PROJECT_SHELL_SCRIPT\");"
AWK_SCRIPT="$AWK_SCRIPT sub(\"{BRANCH_NAME}\", \"$BRANCH_NAME\");"
AWK_SCRIPT="$AWK_SCRIPT sub(\"{THROUGHPUT_GBPS}\", \"$THROUGHPUT_GBPS\");"
AWK_SCRIPT="$AWK_SCRIPT sub(\"{INSTANCE_TYPE}\", \"$INSTANCE_TYPE\");"
AWK_SCRIPT="$AWK_SCRIPT sub(\"{REGION}\", \"$REGION\");"
AWK_SCRIPT="$AWK_SCRIPT sub(\"{USER_NAME}\", \"$USER_NAME\");"
AWK_SCRIPT="$AWK_SCRIPT sub(\"{RUN_PROJECT_LOG_FN}\", \"$RUN_PROJECT_LOG_FN\");"
AWK_SCRIPT="$AWK_SCRIPT sub(\"{PUBLISH_METRICS_LOG_FN}\", \"$PUBLISH_METRICS_LOG_FN\");"
AWK_SCRIPT="$AWK_SCRIPT print}"

awk "$AWK_SCRIPT" $RUN_PROJECT_TEMPLATE > $PERF_SCRIPT_TEMP
awk "{sub(\"{RUN_COMMAND}\", \"DOWNLOAD_PERFORMANCE\"); print}" $PERF_SCRIPT_TEMP > $DOWNLOAD_PERF_SCRIPT
awk "{sub(\"{RUN_COMMAND}\", \"UPLOAD_PERFORMANCE\"); print}" $PERF_SCRIPT_TEMP > $UPLOAD_PERF_SCRIPT

sudo chmod +x $DOWNLOAD_PERF_SCRIPT
sudo chmod +x $UPLOAD_PERF_SCRIPT

if [ $RUN_COMMAND = "DOWNLOAD_PERFORMANCE" ]; then
    $DOWNLOAD_PERF_SCRIPT
elif [ $RUN_COMMAND = "UPLOAD_PERFORAMNCE" ]; then
    $UPLOAD_PERF_SCRIPT
fi
