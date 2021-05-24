#!/usr/bin/env bash

export BENCHMARK_LOG_FN=/tmp/benchmark.log
export PUBLISH_METRICS_LOG_FN=/tmp/publish_metrics.log
export CLOUD_INIT_LOG_FN=/var/log/cloud-init-output.log

function show_publish_metrics_log() {
    echo -n "" > $PUBLISH_METRICS_LOG_FN
    tail -f $PUBLISH_METRICS_LOG_FN
}

export -f show_publish_metrics_log

function show_cloud_init_log() {
    echo -n "" > $CLOUD_INIT_LOG_FN
    tail -f $CLOUD_INIT_LOG_FN
}

export -f show_cloud_init_log

function show_benchmark_log() {
    echo -n "" > $BENCHMARK_LOG_FN
    tail -f $BENCHMARK_LOG_FN
}

export -f show_benchmark_log

function show_bandwidth() {
    bwm-ng -I eth0 -u bits
}

export -f show_bandwidth

function show_connection_count() {
    while true; do echo $(date --rfc-3339=seconds) Connections: $(sudo lsof -i TCP:https | wc -l); sleep 1; done
}

export -f show_connection_count

tmux attach -t dashboard

ATTACH_DASHBOARD_RESULT=$?

if [ $ATTACH_DASHBOARD_RESULT -ne 0 ]; then
    tmux new -s dashboard \; \
        split-window -h -p 50 \; \
        select-pane -t 0 \; \
        send-keys 'show_publish_metrics_log' C-m \; \
        split-window -v -p 66 \; \
        send-keys 'show_cloud_init_log' C-m \; \
        split-window -v -p 50 \; \
        send-keys 'htop' C-m \; \
        select-pane -R \; \
        send-keys 'show_benchmark_log' C-m \; \
        split-window -v -p 66 \; \
        send-keys 'show_bandwidth' C-m \; \
        split-window -v -p 50 \; \
        send-keys 'show_connection_count' C-m \;
fi
