#!/bin/bash
set -x

# ray get-head-ip ~/ray_bootstrap_config.yaml
RAY_HEAD_IP=$(hostname -i)
RAY_WORKER_IP=$(ray get-worker-ips ~/ray_bootstrap_config.yaml | grep -oE "\b([0-9]{1,3}\.){3}[0-9]{1,3}\b")

# new conda: source /opt/conda/bin/activate
ray stop --force
ssh $RAY_WORKER_IP "source ~/anaconda3/bin/activate ray_workflow; ray stop --force"
sleep 10

PYTHONPATH=~/efs/exp1 ray start --head --disable-usage-stats --port=6379 --object-manager-port=8076 --autoscaling-config=~/ray_bootstrap_config.yaml --resources='{"machine": 1, "tag:gpu": 1}' --storage=s3://siyuan-workflow/exp1 --num-cpus 32

ssh $RAY_WORKER_IP "source ~/anaconda3/bin/activate ray_workflow; PYTHONPATH=~/efs/exp1 ray start --address=$RAY_HEAD_IP:6379 --disable-usage-stats --object-manager-port=8076 --resources='{\"machine\": 1, \"tag:data\": 1}' --storage=s3://siyuan-workflow/exp1 --num-cpus 32"

sleep 10
