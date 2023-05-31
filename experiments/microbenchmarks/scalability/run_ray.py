import argparse
import asyncio
import json
import time

import ray
import tqdm

from config import N_TASKS, N_PARALLEL_TASKS
from exoflow import utils


def nop():
    return


@ray.remote(num_cpus=0)
class TaskExecutor:
    @ray.method(num_returns=2)
    async def submit(self, func):
        return None, func()


@ray.remote(num_cpus=0, resources={"controller": 1}, max_concurrency=10000)
class Controller:
    def __init__(self, n_executors: int) -> None:
        utils.increase_recursion_limit()

        self._n_executors = n_executors
        self._index = 0
        self._executors = [TaskExecutor.options(
            num_cpus=0,
            max_restarts=-1,
            scheduling_strategy=utils.local_binding_scheduling_strategy(),
        ).remote() for _ in range(n_executors)]

    def submit_task(self, func, completion_queue, mapping):
        executor = self._executors[self._index % self._n_executors]
        self._index += 1
        metadata_ref, output_ref = executor.submit.remote(func)
        future = asyncio.wrap_future(metadata_ref.future())
        future.add_done_callback(completion_queue.put_nowait)
        mapping[future] = output_ref

    async def execute(self, func, n_parallel_tasks: int):
        completion_queue = asyncio.Queue()
        mapping = {}
        assert n_parallel_tasks >= 0
        for _ in range(n_parallel_tasks):
            self.submit_task(func, completion_queue, mapping)
        for _ in range(n_parallel_tasks):
            future = await completion_queue.get()
            mapping.pop(future)
        self.submit_task(func, completion_queue, mapping)
        future = await completion_queue.get()
        return mapping.pop(future)


def run_ray_tasks(controllers, n_dags: int, n_parallel_tasks: int, n_repeats: int = 5, n_warmups: int = 1):
    durations = []
    for _ in tqdm.trange(n_warmups + n_repeats, desc="Ray Task throughput"):
        start = time.time()
        outputs = []
        for i in range(n_dags):
            outputs.append(controllers[i % len(controllers)].execute.remote(nop, n_parallel_tasks))
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
    parser.add_argument("--prefix", help="prefix of output file", type=str, default="")
    args = parser.parse_args()

    ray.init("auto")
    controllers = [Controller.remote(args.n_executors) for _ in range(args.n_controllers)]

    durations = run_ray_tasks(controllers, N_TASKS, 0)
    if args.prefix:
        output_file = f"result/ray_dag_{args.prefix}_{args.n_controllers}_{args.n_executors}.json"
    else:
        output_file = f"result/ray_dag_{args.n_controllers}_{args.n_executors}.json"
    with open(output_file, "w") as f:
        json.dump(durations, f)

    durations = run_ray_tasks(controllers, N_TASKS // N_PARALLEL_TASKS, N_PARALLEL_TASKS)
    if args.prefix:
        output_file = f"result/ray_task_{args.prefix}_{args.n_controllers}_{args.n_executors}.json"
    else:
        output_file = f"result/ray_task_{args.n_controllers}_{args.n_executors}.json"
    with open(output_file, "w") as f:
        json.dump(durations, f)
