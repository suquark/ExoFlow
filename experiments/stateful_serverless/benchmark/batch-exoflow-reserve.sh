#!/bin/bash

trap 'echo Interrupted; exit' INT
cd ~/beldi

bp="http://$(/stateful_serverless/get_server_ip.sh):8080"

# warmup
echo "warmup..."
ENDPOINT="/gateway" API=reserve_overlapckpt ./tools/wrk -t1 -c5 -d100s -R5 \
  -s /stateful_serverless/hotel/txn_workload.lua --timeout 10s "$bp" > /dev/null
ENDPOINT="/gateway" API=reserve_skipckpt ./tools/wrk -t1 -c5 -d100s -R5 \
  -s /stateful_serverless/hotel/txn_workload.lua --timeout 10s "$bp" > /dev/null

for api in reserve reserve_serial reserve_overlapckpt reserve_nooverlapckpt; do
  /stateful_serverless/benchmark/benchmark-exoflow-reserve.sh $api
done
