#!/bin/bash
pip install shortuuid

for i in `seq 16`; do
    ./restart_ray_1node.sh
    python run_ray.py --n-controllers=$i --n-executors=2 --prefix=1node
done

for i in `seq 16`; do
    ./restart_ray_1node.sh
    python run.py --n-controllers=$i --n-executors=2 --prefix=1node
done
