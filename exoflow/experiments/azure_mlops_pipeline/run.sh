#!/bin/bash
# This script benchmarks (dataset_size * checkpoint) for workflow + ephemeral tasks.

# warmup
./restart_ray.sh && python run.py --checkpoint=hybrid --enhance-dataset-multiplier=1 --skip-record

for j in `seq 3`; do
  for i in `seq 4`; do
      ./restart_ray.sh && python run.py --checkpoint=hybrid --enhance-dataset-multiplier=$i
      ./restart_ray.sh && python run.py --checkpoint=false --enhance-dataset-multiplier=$i
      ./restart_ray.sh && python run.py --checkpoint=async --enhance-dataset-multiplier=$i
      ./restart_ray.sh && python run.py --checkpoint=true --enhance-dataset-multiplier=$i
      ./restart_ray.sh && python run.py --checkpoint=false --enhance-dataset-multiplier=$i --disable-ephemeral-tasks
  done
done
