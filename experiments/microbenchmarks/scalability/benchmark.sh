#!/bin/bash

for i in 1 2 4 8 16; do
    ./restart_ray.sh
    python run.py --n-schedulers=12 --n-workers=$i
done
