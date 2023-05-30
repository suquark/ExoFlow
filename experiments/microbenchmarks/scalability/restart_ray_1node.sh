#!/bin/bash
set -x

# ray get-head-ip ~/ray_bootstrap_config.yaml
RAY_HEAD_IP=$(hostname -i)
RAY_WORKER_IPS=$(ray get-worker-ips ~/ray_bootstrap_config.yaml | grep -oE "\b([0-9]{1,3}\.){3}[0-9]{1,3}\b")

# new conda: source /opt/conda/bin/activate
ray stop --force
for worker_ip in $RAY_WORKER_IPS; do
    ssh $worker_ip "source ~/anaconda3/bin/activate exoflow-dev; ray stop --force &> /dev/null" &
done
wait
sleep 10

workdir=/exoflow/experiments/microbenchmarks/scalability
timestamp=$(date +"%Y%m%d-%H%M%S")
sp="/tmp/ray/workflow_data/${timestamp}"
storage="file://$sp"

PYTHONPATH=$workdir ray start --head --disable-usage-stats --port=6379 --object-manager-port=8076 --autoscaling-config=~/ray_bootstrap_config.yaml --resources='{"machine": 1}' --storage=$storage

for worker_ip in $RAY_WORKER_IPS; do
    ssh $worker_ip "source ~/anaconda3/bin/activate exoflow-dev; PYTHONPATH=$workdir ray start --address=$RAY_HEAD_IP:6379 --disable-usage-stats --object-manager-port=8076 --resources='{\"machine\": 1, \"controller\": 16}' --storage=$storage" &
    # we do not need distributed storage. this is just to make Ray storage happy.
    ssh $worker_ip "mkdir -p $sp && touch $sp/_valid" &
done
wait

sleep 10
