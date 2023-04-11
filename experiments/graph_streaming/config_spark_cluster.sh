#!/bin/bash

/exoflow/experiments/graph_streaming/config_spark.sh

RAY_WORKER_IPS=$(ray get-worker-ips ~/ray_bootstrap_config.yaml | grep -oE "\b([0-9]{1,3}\.){3}[0-9]{1,3}\b")
for worker_ip in $RAY_WORKER_IPS; do
    ssh $worker_ip /exoflow/experiments/graph_streaming/config_spark.sh
done
