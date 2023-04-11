#!/bin/bash

trap 'echo Interrupted; exit' INT
cd ~/beldi

bp="$(/exp2/get_beldi_gateway.sh)/default/beldi-dev-gateway"

rate=5
echo "Benchmarking beldi-reserve @rate=$rate"
ENDPOINT="$bp" API="reserve" ./tools/wrk -t1 -c$rate -d420s -R$rate \
  -s /exp2/hotel/txn_workload.lua --timeout 10s "$bp" > /dev/null
echo "Collecting metrics"
output_dir=/exp2/result/beldi
mkdir -p $output_dir
python ./scripts/hotel/hotel.py --command run --config beldi --duration 7 | tee $output_dir/hotel-metrics-reserve.txt
