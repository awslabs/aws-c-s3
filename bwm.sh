#!/usr/bin/env bash
set -ex

function publish() {
  set -ex
  aws cloudwatch put-metric-data \
    --no-cli-pager \
    --namespace S3Benchmark \
    --metric-name BytesIn \
    --unit Bytes \
    --dimensions Host=$HOSTNAME \
    --storage-resolution 1 \
    --value $3
  aws cloudwatch put-metric-data \
    --no-cli-pager \
    --namespace S3Benchmark \
    --metric-name BytesOut \
    --unit Bytes \
    --dimensions Host=$HOSTNAME \
    --storage-resolution 1 \
    --value $2
}

export -f publish
bwm-ng -I eth0 -o csv -u bits -d -c 0 \
    | stdbuf -o0 grep -v total \
    | stdbuf -o0 cut -f1,3,4 -d\; --output-delimiter=' ' \
    | xargs -n3 -t -P 32 bash -c 'publish "$@"' _
