#!/bin/bash

trap 'echo Interrupted; exit' INT

for rate in `seq 100 100 1000`; do
  /stateful_serverless/benchmark/benchmark-beldi.sh $rate
done
