#!/bin/bash

trap 'echo Interrupted; exit' INT
cd ~/beldi

bp="http://$(/exp2/get_server_ip.sh):8080"

output_dir=/exp2/result
mkdir -p $output_dir
rm /exp2/result/*.csv &> /dev/null

echo "warmup"
ENDPOINT="/gateway" ./tools/wrk -t4 -c100 -d100s -R100 \
    -s ./benchmark/hotel/workload.lua --timeout 10s "$bp" --latency

for rate in `seq 100 100 1000`; do
  echo "Benchmarking workflow failure @rate=$rate"
  ENDPOINT="/gateway" ./tools/wrk -t4 -c$rate -d420s -R$rate \
    -s ./benchmark/hotel/workload.lua --timeout 10s "$bp" --latency

  echo "Collecting metrics"
  sleep 15
  subdir=failure-$rate
  rm -r $output_dir/$subdir &> /dev/null
  mkdir -p $output_dir/$subdir
  mv /exp2/result/*.csv $output_dir/$subdir
  chmod -R 777 $output_dir/$subdir
echo
done
