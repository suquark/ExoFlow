import argparse
import json
import time

import ray
import tqdm

from config import N_TASKS


@ray.remote
def nop():
    pass


def run_ray_tasks(n_tasks: int, n_repeats: int = 5, n_warmups: int = 1):
    durations = []
    for _ in tqdm.trange(n_warmups + n_repeats, desc="Ray Task throughput"):
        start = time.time()
        ray.get([nop.remote() for _ in range(n_tasks)])
        durations.append(time.time() - start)
    return durations[n_warmups:]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="benchmark throughput")
    parser.add_argument("--n-cpus", help="number of CPUs", type=int, default=1)
    args = parser.parse_args()
    ray.init(num_cpus=args.n_cpus)

    durations = run_ray_tasks(N_TASKS)
    with open(f"result/ray_{args.n_cpus}.json", "w") as f:
        json.dump(durations, f)
