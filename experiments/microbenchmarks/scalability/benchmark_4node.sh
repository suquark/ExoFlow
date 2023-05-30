#!/bin/bash
pip install shortuuid

for i in `seq 16`; do
    ./restart_ray_4node.sh
    python run_ray.py --n-controllers=$i --n-executors=2 --prefix=4node
done

for i in `seq 16`; do
    ./restart_ray_4node.sh
    python run.py --n-controllers=$i --n-executors=2 --prefix=4node
done
