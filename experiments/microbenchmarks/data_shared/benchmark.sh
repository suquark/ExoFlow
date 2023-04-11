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

./restart_spark.sh
ray stop --force &> /dev/null
sleep 5
python plain_spark.py
python workflow_spark.py
python airflow_spark.py $server_ip --port=18080
