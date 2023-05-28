#!/bin/bash
RAY_WORKER_IPS=$(ray get-worker-ips ~/ray_bootstrap_config.yaml | grep -oE "\b([0-9]{1,3}\.){3}[0-9]{1,3}\b")

# new conda: source /opt/conda/bin/activate
for worker_ip in $RAY_WORKER_IPS; do
    echo $worker_ip
    ssh $worker_ip "ps -eo pid,pcpu,comm | awk '{if (\$2 > 4) print }'"
done
