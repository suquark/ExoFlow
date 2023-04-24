#!/bin/bash

trap 'echo Interrupted; exit' INT
cd ~/beldi

bp="http://$(/stateful_serverless/get_server_ip.sh):8080"

output_dir=/stateful_serverless/result

# warmup
echo "Benchmarking warmup..."
ENDPOINT="/gateway" API=reserve_overlapckpt ./tools/wrk -t1 -c5 -d100s -R5 \
  -s /stateful_serverless/hotel/txn_workload.lua --timeout 10s "$bp" > /dev/null
ENDPOINT="/gateway" API=reserve_skipckpt ./tools/wrk -t1 -c5 -d100s -R5 \
  -s /stateful_serverless/hotel/txn_workload.lua --timeout 10s "$bp" > /dev/null

# need longer benchmark time to get more stable result
for api in reserve reserve_serial reserve_overlapckpt reserve_nooverlapckpt; do
  rate=5
  echo "Benchmarking [workflow.$api] @rate=$rate"
  rm $output_dir/temp/*.csv &> /dev/null

  ENDPOINT="/gateway" API=$api ./tools/wrk -t1 -c$rate -d420s -R$rate \
    -s /stateful_serverless/hotel/txn_workload.lua --timeout 10s "$bp" > /dev/null

  echo "Collecting metrics"
  sleep 15
  subdir=workflow-$api
  rm -r $output_dir/$subdir &> /dev/null
  mkdir -p $output_dir/$subdir
  mv $output_dir/temp/*.csv $output_dir/$subdir
  chmod -R 777 $output_dir/$subdir
done
