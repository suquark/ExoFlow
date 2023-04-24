#!/bin/bash

trap 'echo Interrupted; exit' INT
cd ~/beldi

bp="$(/stateful_serverless/get_beldi_gateway.sh)/default/beldi-dev-gateway"

for rate in `seq 100 100 1000`; do
  echo "Benchmarking beldi @rate=$rate"
  ENDPOINT="$bp" ./tools/wrk -t4 -c$rate -d420s -R$rate \
    -s ./benchmark/hotel/workload.lua --timeout 10s "$bp" > /dev/null
  echo "Collecting metrics"
  output_dir=/stateful_serverless/result/beldi
  mkdir -p $output_dir
  python ./scripts/hotel/hotel.py --command run --config beldi --duration 7 | tee $output_dir/hotel-metrics-$rate.txt
done
