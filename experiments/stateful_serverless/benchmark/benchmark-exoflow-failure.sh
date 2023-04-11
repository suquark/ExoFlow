#!/bin/bash

trap 'echo Interrupted; exit' INT
cd ~/beldi

rate=$1
bp="http://$(/stateful_serverless/get_server_ip.sh):8080"

output_dir=/stateful_serverless/result

echo "Benchmarking workflow failure @rate=$rate"
rm $output_dir/temp/*.csv &> /dev/null

ENDPOINT="/gateway" ./tools/wrk -t4 -c$rate -d420s -R$rate \
  -s ./benchmark/hotel/workload.lua --timeout 10s "$bp" --latency

echo "Collecting metrics"
sleep 15

temp_output_dir=$output_dir/exoflow-failure/temp/$rate
rm -r $temp_output_dir &> /dev/null
mkdir -p $temp_output_dir
mv $output_dir/temp/*.csv $temp_output_dir
chmod -R 777 $temp_output_dir
