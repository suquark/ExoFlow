import json
import time
from multiprocessing import Process
from multiprocessing.pool import ThreadPool

import tqdm

import pipeline


def _process(num_consumers: int):
    pool = ThreadPool(16)
    with pipeline.connect(pipeline.get_local_addr()) as spark:
        df = pipeline.generate_df(spark).cache()
        outputs = []
        for i in range(num_consumers):
            outputs.append(pipeline.consume_df(df, seed=i))
        pool.map(lambda x: x.count(), outputs)


def run_once(num_consumers: int):
    start = time.time()
    p = Process(target=_process, args=(num_consumers,))
    p.start()
    p.join()
    return time.time() - start


if __name__ == "__main__":
    for n in tqdm.trange(1, 9, desc="spark"):
        durations = []
        # 1 warmup
        for _ in range(5 + 1):
            durations.append(run_once(n))
        with open(f"result/spark_{n}.json", "w") as f:
            json.dump(durations[1:], f)
