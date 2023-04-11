#!/bin/bash
# This script benchmarks fault tolerance.

rm *.task_crash
rm *.cluster_crash

failure_list="preprocess.task_crash
transform_8.task_crash
transform_subtask_8.task_crash
train_actor_8.task_crash"

for j in `seq 3`; do
    for i in $failure_list; do
      ./restart_ray.sh && python run.py --checkpoint=hybrid --enhance-dataset-multiplier=4 --failure=$i
      ./restart_ray.sh && python run.py --checkpoint=false --enhance-dataset-multiplier=4 --failure=$i
      ./restart_ray.sh && python run.py --checkpoint=true --enhance-dataset-multiplier=4 --failure=$i
    done
done

# We need to make sure async checkpointing of "preprocess" is done before crashing the cluster.
cluster_failure="train_12.cluster_crash"

for j in `seq 3`; do
    workflow_id=$(openssl rand -hex 12)
    ./restart_ray.sh && python run.py --checkpoint=hybrid --enhance-dataset-multiplier=4 --failure=$cluster_failure --workflow-id=$workflow_id
    ./restart_ray.sh && python run.py --checkpoint=hybrid --enhance-dataset-multiplier=4 --failure=$cluster_failure --workflow-id=$workflow_id --resume

    workflow_id=$(openssl rand -hex 12)
    ./restart_ray.sh && python run.py --checkpoint=false --enhance-dataset-multiplier=4 --failure=$cluster_failure --workflow-id=$workflow_id
    ./restart_ray.sh && python run.py --checkpoint=false --enhance-dataset-multiplier=4 --failure=$cluster_failure --workflow-id=$workflow_id --resume

    workflow_id=$(openssl rand -hex 12)
    ./restart_ray.sh && python run.py --checkpoint=true --enhance-dataset-multiplier=4 --failure=$cluster_failure --workflow-id=$workflow_id
    ./restart_ray.sh && python run.py --checkpoint=true --enhance-dataset-multiplier=4 --failure=$cluster_failure --workflow-id=$workflow_id --resume
done
