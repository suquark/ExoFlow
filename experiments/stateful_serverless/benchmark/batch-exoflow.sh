#!/bin/bash

trap 'echo Interrupted; exit' INT
cd ~/beldi

echo "warmup..."
ENDPOINT="/gateway" ./tools/wrk -t4 -c100 -d100s -R100 -s ./benchmark/hotel/workload.lua --timeout 10s "$bp" --latency

for rate in `seq 100 100 1000`; do
  /stateful_serverless/benchmark/benchmark-exoflow.sh $rate
done
