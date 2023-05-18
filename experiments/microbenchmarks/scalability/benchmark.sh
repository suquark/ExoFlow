#!/bin/bash
pip install shortuuid

num_nodes=$(ray get-worker-ips ~/ray_bootstrap_config.yaml | grep -oE "\b([0-9]{1,3}\.){3}[0-9]{1,3}\b" | wc -l)
./restart_ray.sh
python run.py --n-schedulers=1 --n-workers=1 --n-nodes=$num_nodes
