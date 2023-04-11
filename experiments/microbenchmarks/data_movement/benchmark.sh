#!/bin/bash

instance_name="ray-workflow-microbenchmarks-head"
server_ip=$(aws --region us-east-1 \
  ec2 describe-instances \
  --filters \
  "Name=instance-state-name,Values=running" \
  "Name=tag:Name,Values=$instance_name" \
  --query 'Reservations[*].Instances[*].[PublicIpAddress]' \
  --output text)

echo "Server IP: $server_ip"
python run.py $server_ip --port=8080
