#!/bin/bash

for i in `seq 12`; do
    ray stop --force &> /dev/null
    sleep 5
    python run.py --n-controllers=$i --n-executors=2
done

for i in `seq 12`; do
    ray stop --force &> /dev/null
    sleep 5
    python run_ray.py --n-cpus=$i
done

#for i in `seq 12`; do
#    ray stop --force &> /dev/null
#    sleep 5
#    python run_ray_actor.py --n-cpus=$i
#done
#
#for i in `seq 12`; do
#    ray stop --force &> /dev/null
#    sleep 5
#    python run_ray_async_actor.py --n-cpus=$i
#done
