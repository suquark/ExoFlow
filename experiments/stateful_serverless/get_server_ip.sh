#!/bin/bash

instance_name="ray-workflow-stateful_serverless-head"
aws --region us-east-1 \
  ec2 describe-instances \
  --filters \
  "Name=instance-state-name,Values=running" \
  "Name=tag:Name,Values=$instance_name" \
  --query 'Reservations[*].Instances[*].[PublicIpAddress]' \
  --output text
