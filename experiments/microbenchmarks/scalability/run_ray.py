import argparse
import json
import time

import ray
import tqdm

from config import N_TASKS
from exoflow import utils


def nop():
    return


@ray.remote(num_cpus=0)
class TaskExecutor:
    @ray.method(num_returns=2)
    async def submit(self, func):
        return None, func()


@ray.remote(num_cpus=0, resources={"controller": 1})
class Controller:
    def __init__(self, n_executors: int) -> None:
        self._n_executors = n_executors
        self._index = 0
        self._executors = [TaskExecutor.options(
            num_cpus=0,
            max_restarts=-1,
            scheduling_strategy=utils.local_binding_scheduling_strategy(),
        ).remote() for _ in range(n_executors)]

    async def execute(self):
        executor = self._executors[self._index % self._n_executors]
        self._index += 1
        metadata, data = executor.submit.remote(nop)
        await metadata
        return data


def run_ray_tasks(controllers: list, n_tasks: int, n_repeats: int = 5, n_warmups: int = 1):
    durations = []
    for _ in tqdm.trange(n_warmups + n_repeats, desc="Ray Task throughput"):
        start = time.time()
        outputs = []
        for i in range(n_tasks):
            outputs.append(controllers[i % len(controllers)].execute.remote())
        # first get all ObjectRefs
        outputs = ray.get(outputs)
        # then get all data
        ray.get(outputs)
        durations.append(time.time() - start)
    return durations[n_warmups:]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="benchmark scalability")
    parser.add_argument(
        "--n-controllers", help="number of controllers", type=int, default=1
    )
    parser.add_argument("--n-executors", help="number of executors", type=int, default=2)
    args = parser.parse_args()

    ray.init("auto")
    controllers = [Controller.remote(args.n_executors) for _ in range(args.n_controllers)]

    durations = run_ray_tasks(controllers, N_TASKS)
    with open(f"result/ray_{args.n_controllers}.json", "w") as f:
        json.dump(durations, f)
