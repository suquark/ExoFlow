#!/bin/bash

# By default Ray worker nodes cannot access S3.
# This related doc does not work: https://docs.ray.io/en/latest/cluster/vms/user-guides/launching-clusters/aws.html?highlight=s3#accessing-s3
# This script fixes this issue.

instance_id=$(aws ec2 describe-instances --region=us-east-1 \
  --filters 'Name=instance-state-name,Values=running' \
  'Name=tag:Name,Values=ray-workflow-train-distributed-worker' \
  --query 'Reservations[*].Instances[*].[InstanceId]' \
  --output text)

echo "InstanceId of the worker node: $instance_id"

aws ec2 associate-iam-instance-profile --region us-east-1 \
  --iam-instance-profile Name=ray-autoscaler-v1 \
  --instance-id $instance_id
