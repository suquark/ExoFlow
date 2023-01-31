import json
import time
from datetime import datetime

import ray
from ray import workflow
from ray.workflow.api import register_service, run_service_async

import shortuuid
import tqdm

import pipeline


@ray.remote(max_concurrency=16)
class SparkActor:
    def __init__(self):
        self.spark = pipeline.connect(pipeline.get_local_addr())
        self.df = None

    def generate_df(self):
        self.df = pipeline.generate_df(self.spark).cache()

    def consume_df(self, seed: int) -> int:
        return pipeline.consume_df(self.df, seed=seed).count()

    def checkpoint(self):
        pipeline.save_df(self.df)

    def restore(self):
        self.df = pipeline.load_df(self.df)

    def stop(self):
        self.spark.stop()


@ray.remote
def create_df():
    actor = SparkActor.remote()
    ray.get(actor.generate_df.remote())
    return actor


@ray.remote
def consume_df(actor, seed: int):
    ray.get(actor.consume_df.remote(seed))


@ray.remote
def sync_checkpoints(actor, dfs):
    ray.get(actor.stop.remote())


@ray.remote
def create_df_no_shared():
    pipeline.generate_df_no_shared(pipeline.get_local_addr())


@ray.remote
def consume_df_no_shared(seed: int):
    pipeline.consume_df_no_shared(pipeline.get_local_addr(), seed)


@ray.remote
def sync_no_shared():
    pass


def register_sharedmem_dags(num_consumers: int, checkpoint: str):
    df = create_df.options(
        **workflow.options(checkpoint=pipeline.CHECKPOINT_MAP[checkpoint])
    ).bind()
    samples = []
    for i in range(num_consumers):
        samples.append(
            consume_df.options(
                **workflow.options(checkpoint=False, name=f"consumer_{i}")
            ).bind(df, seed=i)
        )
    dag = sync_checkpoints.options(
        **workflow.options(
            wait_until_committed=[f"consumer_{i}" for i in range(num_consumers)]
        )
    ).bind(df, samples)
    register_service(dag, workflow_id=f"shared_{checkpoint}_{num_consumers}")


def register_nosharedmem_dags(num_consumers: int):
    df = create_df_no_shared.options(**workflow.options(isolation=True)).bind()
    samples = [
        consume_df_no_shared.options(**workflow.options(isolation=True)).bind(i)
        for i in range(num_consumers)
    ]
    df >> samples
    dag = sync_no_shared.bind()
    dag << samples
    register_service(dag, workflow_id=f"no_shared_{num_consumers}")


def run_shared(num_consumers: int, checkpoint, n_repeats: int, n_warmups: int = 1):
    durations = []
    for _ in range(n_warmups + n_repeats):
        start = time.time()
        ray.get(
            run_service_async(f"shared_{checkpoint}_{num_consumers}", shortuuid.uuid())
        )
        durations.append(time.time() - start)
    return durations[n_warmups:]


def run_no_shared(num_consumers: int, n_repeats: int, n_warmups: int = 1):
    durations = []
    for _ in range(n_warmups + n_repeats):
        start = time.time()
        ray.get(run_service_async(f"no_shared_{num_consumers}", shortuuid.uuid()))
        durations.append(time.time() - start)
    return durations[n_warmups:]


if __name__ == "__main__":

    def _init():
        ray.init(
            "local",
            storage=f"file:///tmp/ray/workflow_data/{datetime.now().isoformat()}",
        )
        workflow.init()

        for n in range(1, N + 1):
            register_sharedmem_dags(n, "skip")
            register_sharedmem_dags(n, "sync")
            register_nosharedmem_dags(n)

    N = 8
    _init()

    for n in tqdm.trange(1, N + 1, desc="workflow skip"):
        durations = run_shared(n, checkpoint="skip", n_repeats=5)
        with open(f"result/workflow_skip_{n}.json", "w") as f:
            json.dump(durations, f)

    for n in tqdm.trange(1, N + 1, desc="workflow sync"):
        durations = run_shared(n, checkpoint="sync", n_repeats=5)
        with open(f"result/workflow_sync_{n}.json", "w") as f:
            json.dump(durations, f)

    for n in tqdm.trange(1, N + 1, desc="workflow no share"):
        ray.shutdown()
        _init()
        durations = run_no_shared(n, n_repeats=5)
        with open(f"result/workflow_no_share_{n}.json", "w") as f:
            json.dump(durations, f)
