import argparse
import json
import os
import time
from datetime import datetime

import ray
import exoflow
from exoflow.api import register_service, run_service_async

import shortuuid
import tqdm

from config import N_TASKS, N_PARALLEL_TASKS


@ray.remote(**exoflow.options(checkpoint=False))
def nop():
    pass


def register_dags():
    register_service(nop.bind(), workflow_id="single")
    dag = nop.bind()
    dag << [nop.bind() for _ in range(N_PARALLEL_TASKS)]
    register_service(dag, workflow_id="parallel")


def run_single(n_dags: int, n_repeats: int = 5, n_warmups: int = 1):
    durations = []
    for _ in tqdm.trange(n_warmups + n_repeats, desc="DAG throughput"):
        start = time.time()
        ray.get([run_service_async("single", shortuuid.uuid()) for _ in range(n_dags)])
        durations.append(time.time() - start)
    return durations[n_warmups:]


def run_parallel(n_dags: int, n_repeats: int = 5, n_warmups: int = 1):
    durations = []
    for _ in tqdm.trange(n_warmups + n_repeats, desc="Task throughput"):
        start = time.time()
        ray.get(
            [run_service_async("parallel", shortuuid.uuid()) for _ in range(n_dags)]
        )
        durations.append(time.time() - start)
    return durations[n_warmups:]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="benchmark throughput")
    parser.add_argument(
        "--n-controllers", help="number of controllers", type=int, default=1
    )
    parser.add_argument("--n-executors", help="number of executors", type=int, default=2)
    args = parser.parse_args()

    os.environ["EXOFLOW_N_CONTROLLERS"] = str(args.n_controllers)
    os.environ["EXOFLOW_N_EXECUTORS"] = str(args.n_executors)
    os.environ["RAY_USAGE_STATS_ENABLED"] = "0"
    os.environ["EXOFLOW_CONTROLLER_MAX_CONCURRENCY"] = "10000"
    ray.init(
        "local", storage=f"file:///tmp/ray/workflow_data/{datetime.now().isoformat()}"
    )
    exoflow.init()
    register_dags()

    durations = run_single(N_TASKS)
    with open(f"result/dag_{args.n_controllers}_{args.n_executors}.json", "w") as f:
        json.dump(durations, f)

    durations = run_parallel(N_TASKS // N_PARALLEL_TASKS)
    with open(f"result/task_{args.n_controllers}_{args.n_executors}.json", "w") as f:
        json.dump(durations, f)
