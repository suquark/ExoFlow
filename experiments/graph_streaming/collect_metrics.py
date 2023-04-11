import json
import multiprocessing
import os
import subprocess

import numpy as np
from smart_open import s3

BUCKET = "exoflow"
N_WORKERS = multiprocessing.cpu_count() * 4
OUTPUT_DIR = "result/analyze_outputs"
# DATE = "2022-11-03"


def _split_key(key: str) -> int:
    name, index = key.split("/")[-2].rsplit("_", 1)
    try:
        return int(index)
    except ValueError:
        return 0


def get_start_time(workflow_id: str, task_name: str):
    data = {}

    def _accept_key(key: str):
        return task_name in key and key.endswith("pre_task_metadata.json")

    prefix = f"graph_streaming/workflows/{workflow_id}/tasks/"
    for key, content in s3.iter_bucket(
        BUCKET, prefix=prefix, accept_key=_accept_key, workers=N_WORKERS
    ):
        data[_split_key(key)] = json.loads(content)["pre_time"]
    data = sorted(data.items())
    indices, data = zip(*data)
    assert indices == tuple(range(len(indices)))
    return np.array(data)


def get_end_time(workflow_id: str, task_name: str):
    data = {}

    def _accept_key(key: str):
        return task_name in key and key.endswith("post_task_metadata.json")

    prefix = f"graph_streaming/workflows/{workflow_id}/tasks/"
    for key, content in s3.iter_bucket(
        BUCKET, prefix=prefix, accept_key=_accept_key, workers=N_WORKERS
    ):
        d = json.loads(content)
        if "checkpoint_time" in d:
            data[_split_key(key)] = d["checkpoint_time"]
        else:
            data[_split_key(key)] = d["end_time"]
    data = sorted(data.items())
    indices, data = zip(*data)
    assert indices == tuple(range(len(indices)))
    return np.array(data)


def get_workflows():
    result = subprocess.check_output(
        "aws s3 ls exoflow/graph_streaming/workflows/ "
        "| awk '{print $2}' | grep -v '__status__'",
        text=True,
        shell=True,
    )
    for workflow_id in result.splitlines():
        # if DATE in workflow_id:
        yield workflow_id.strip("/")


def analyze(workflow_id: str):
    fn = os.path.join(OUTPUT_DIR, workflow_id + ".json")
    if os.path.exists(fn):
        return
    batch_update_durations = get_end_time(
        workflow_id, "batch_update.compute_tunk_rank"
    ) - get_start_time(workflow_id, "batch_update.compute_tunk_rank")
    online_update_durations = get_end_time(
        workflow_id, "online_update.online_update"
    ) - get_start_time(workflow_id, "partition.run_epoch")

    data = {
        "batch_update_durations": batch_update_durations.tolist(),
        "online_update_durations": online_update_durations.tolist(),
    }
    with open(fn, "w") as f:
        json.dump(data, f)


if __name__ == "__main__":
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    for wid in get_workflows():
        print(wid, "*" * 20)
        analyze(wid)
