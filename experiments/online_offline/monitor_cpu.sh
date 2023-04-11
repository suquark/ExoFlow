#!/bin/bash
WORKER_NODES=$(ray get-worker-ips ~/ray_bootstrap_config.yaml | grep -oE "\b([0-9]{1,3}\.){3}[0-9]{1,3}\b")

while true; do
for host in $WORKER_NODES; do
  echo -e "\nStatus of $host:"
  ssh $host ps -e -o pcpu | grep -oE '.*[0-9]+.*' | awk '{s+=$1} END {printf "CPU: %.0f\n", s}'
  ssh $host free -h
done
echo
sleep 3
done
