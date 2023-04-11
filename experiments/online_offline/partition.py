from typing import List

import numpy as np
import ray

from common import N_PARTITIONS, N_EPOCH_INTERVAL, State


@ray.remote(num_cpus=0.1)
def update_partition(
    new_batches: List[ray.ObjectRef],
    epoch: int,
    n_partitions: int,
    partition_id: int,
):
    new_batches = ray.get(new_batches)
    filtered_batch = []
    for bz in new_batches:
        for a, b in bz:
            if a % n_partitions == partition_id:
                filtered_batch.append((a, b))
    if len(filtered_batch) <= 0:
        return None
    return np.array(filtered_batch, dtype=np.uint32)


@ray.remote
def run_epoch(actors, state: State, epoch: int) -> State:
    epoch_time = state.epoch_time + N_EPOCH_INTERVAL * 10 ** 9

    batches, start_indices = [], []
    for i, a in enumerate(actors):
        a.start_ingest.remote(state.start_indices[i])
        batch, start_index = a.get_next_batch.remote(epoch_time)
        batches.append(batch)
        start_indices.append(start_index)

    partitions = state.partitions
    for i in range(N_PARTITIONS):
        partitions[i].append(
            update_partition.remote(batches, epoch, N_PARTITIONS, partition_id=i)
        )
    ray.wait([p[-1] for p in partitions], num_returns=N_PARTITIONS, fetch_local=False)
    return State(epoch_time, partitions, start_indices)
