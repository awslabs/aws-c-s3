#!/usr/bin/env bash

set -ex

function publish_bw_metric() {
    set -ex
    aws cloudwatch put-metric-data \
        --no-cli-pager \
        --namespace S3Benchmark \
        --metric-name BytesIn \
        --unit Bytes \
        --dimensions Project={PROJECT_NAME},Branch={BRANCH_NAME},InstanceType={INSTANCE_TYPE} \
        --storage-resolution 1 \
        --value $3 >> {PUBLISH_METRICS_LOG_FN}
    aws cloudwatch put-metric-data \
        --no-cli-pager \
        --namespace S3Benchmark \
        --metric-name BytesOut \
        --unit Bytes \
        --dimensions Project={PROJECT_NAME},Branch={BRANCH_NAME},InstanceType={INSTANCE_TYPE} \
        --storage-resolution 1 \
        --value $2 >> {PUBLISH_METRICS_LOG_FN}
}

export -f publish_bw_metric

{PROJECT_SHELL_SCRIPT} 'RUN' > {RUN_PROJECT_LOG_FN} &

stdbuf -i0 -o0 -e0 bwm-ng -I eth0 -o csv -u bits -d -c 0 \
    | stdbuf -o0 grep -v total \
    | stdbuf -o0 cut -f1,3,4 -d\; --output-delimiter=' ' \
    | xargs -n3 -t -P 32 bash -c 'publish_bw_metric "$@"' _ &
