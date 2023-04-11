import argparse
import json
import time

import ray
import tqdm

from config import N_TASKS


@ray.remote(num_cpus=0, max_concurrency=10000)
class Nop:
    def __init__(self):
        import resource
        import sys

        try:
            resource.setrlimit(resource.RLIMIT_STACK, (2 ** 29, -1))
        except ValueError:
            pass
        sys.setrecursionlimit(10 ** 6)

    async def nop(self):
        pass


def run_ray_tasks(actors, n_tasks: int, n_repeats: int = 5, n_warmups: int = 1):
    durations = []
    for _ in tqdm.trange(n_warmups + n_repeats, desc="Ray actor task throughput"):
        start = time.time()
        ray.get([actors[i % (len(actors))].nop.remote() for i in range(n_tasks)])
        durations.append(time.time() - start)
    return durations[n_warmups:]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="benchmark throughput")
    parser.add_argument("--n-cpus", help="number of CPUs", type=int, default=1)
    args = parser.parse_args()
    ray.init(num_cpus=args.n_cpus)
    durations = run_ray_tasks([Nop.remote() for _ in range(args.n_cpus)], N_TASKS)
    with open(f"result/ray_async_actor_{args.n_cpus}.json", "w") as f:
        json.dump(durations, f)
