from collections import defaultdict
import os
from typing import List, Dict, Tuple

import numpy as np
import pandas as pd
import ray

from common import (
    THRESHOLD,
    P_RETWEET,
    N_PARTITIONS,
    State,
    DUMP_OUTPUT,
    DUMP_OUTPUT_PATH,
    BATCH_UPDATE_INTERVAL,
)


@ray.remote
class PartitionActor:
    def __init__(self, partition_id: int):
        self._epoch = 0
        self._partition_id = partition_id
        self._influence: Dict[int, float] = {}
        self._out_edges: Dict[int, List[int]] = defaultdict(list)
        self._out_edge_influences: Dict[Tuple[int, int], float] = {}

    @ray.method(num_returns=N_PARTITIONS)
    def update_partition(
        self,
        batches: List[ray.ObjectRef],
        epoch: int,
    ):
        n_iter = epoch - self._epoch
        self._epoch = epoch
        assert n_iter > 0

        batches = ray.get(batches[-n_iter:])
        batches = [b for b in batches if b is not None]
        if not batches:
            return (None,) * N_PARTITIONS

        data = np.concatenate(batches)
        unique = np.unique(data[:, 1])

        for e, k in data:
            self._out_edges[int(k)].append(int(e))

        delta_to_push = [[] for _ in range(N_PARTITIONS)]  # target, delta

        for k in unique:
            k = int(k)
            num_out_edges = len(self._out_edges[k])
            tunk_rank = self._influence.get(k, 0)
            new_rank_to_push = (1 + P_RETWEET * tunk_rank) / num_out_edges
            for e in self._out_edges[k]:
                delta = new_rank_to_push - self._out_edge_influences.get((k, e), 0)
                if abs(delta) > THRESHOLD:
                    delta_to_push[e % N_PARTITIONS].append((e, delta))

        dfs = []
        for d in delta_to_push:
            if not d:
                dfs.append(pd.DataFrame({"target": [], "delta": []}))
                continue
            targets, deltas = zip(*d)
            dfs.append(
                pd.DataFrame(
                    {
                        "target": np.array(targets, dtype=np.int32),
                        "delta": np.array(deltas, dtype=np.float),
                    }
                )
            )
        return tuple(dfs)

    def aggregate(self, influences: List[pd.DataFrame], epoch: int):
        dataframes = []
        for df in ray.get(influences):
            if df is not None:
                dataframes.append(df)
        if not dataframes:
            return
        df = pd.concat(dataframes, ignore_index=True)
        df = df.groupby("target", as_index=False).sum()

        for k, v in df.values:
            k, v = int(k), float(v)
            self._influence[k] = self._influence.get(k, 0) + v

        if DUMP_OUTPUT and epoch % BATCH_UPDATE_INTERVAL == 0:
            influence = pd.DataFrame(self._influence.items())
            influence.to_csv(
                os.path.join(
                    DUMP_OUTPUT_PATH, f"online_update_{self._partition_id}_{epoch}.csv"
                ),
                header=False,
                index=False,
            )


class OnlineUpdate:
    def __init__(self):
        self.partition_actors = [PartitionActor.remote(i) for i in range(N_PARTITIONS)]

    def update(self, state: State, epoch: int):
        assert len(state.partitions) == len(self.partition_actors)
        updates = []
        for ps, a in zip(state.partitions, self.partition_actors):
            if ps is not None:
                updates.append(a.update_partition.remote(ps, epoch))

        # shuffle
        outputs = []
        for update, a in zip(zip(*updates), self.partition_actors):
            outputs.append(a.aggregate.remote(list(update), epoch))

        # wait updates
        ray.get(outputs)


@ray.remote
def create_online_actors():
    return OnlineUpdate()


@ray.remote
def online_update(online_updater: OnlineUpdate, state: State, epoch: int):
    return online_updater.update(state, epoch)
