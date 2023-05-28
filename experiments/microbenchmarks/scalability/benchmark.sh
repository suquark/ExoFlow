#!/bin/bash
pip install shortuuid
# num_nodes=$(ray get-worker-ips ~/ray_bootstrap_config.yaml | grep -oE "\b([0-9]{1,3}\.){3}[0-9]{1,3}\b" | wc -l)

for num_nodes in 1 2 4 8 16; do
    ./restart_ray.sh
    python run.py --n-schedulers=$num_nodes --n-workers=2 --n-nodes=$num_nodes

    # ./restart_ray.sh
    # python run_ray.py --n-schedulers=$i --n-workers=2
done
