import json
import multiprocessing
import os
import subprocess

import numpy as np
from smart_open import s3
from tqdm import tqdm

BUCKET = "exoflow"
N_WORKERS = multiprocessing.cpu_count() * 4
OUTPUT_DIR = "result/analyze_outputs"


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
    for key, content in tqdm(s3.iter_bucket(
        BUCKET, prefix=prefix, accept_key=_accept_key, workers=N_WORKERS
    )):
        data[_split_key(key)] = json.loads(content)["pre_time"]
    data = sorted(data.items())
    if not data:
        return None
    indices, data = zip(*data)
    assert indices == tuple(range(len(indices)))
    return np.array(data)


def get_end_time(workflow_id: str, task_name: str):
    data = {}

    def _accept_key(key: str):
        return task_name in key and key.endswith("post_task_metadata.json")

    prefix = f"graph_streaming/workflows/{workflow_id}/tasks/"
    for key, content in tqdm(s3.iter_bucket(
        BUCKET, prefix=prefix, accept_key=_accept_key, workers=N_WORKERS
    )):
        d = json.loads(content)
        if "checkpoint_time" in d:
            data[_split_key(key)] = d["checkpoint_time"]
        else:
            data[_split_key(key)] = d["end_time"]
    data = sorted(data.items())
    if not data:
        return None
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
        print(f"The results of workflow {workflow_id} already exists. "
              "Skipping...\nIf you would like to renew the data, please delete relevant "
              "files under 'result/analyze_outputs'")
        return
    batch_update_end_time = get_end_time(workflow_id, "batch_update.compute_tunk_rank")
    batch_update_start_time = get_start_time(workflow_id, "batch_update.compute_tunk_rank")
    online_update_end_time = get_end_time(workflow_id, "online_update.online_update")
    online_update_start_time = get_start_time(workflow_id, "partition.run_epoch")
    if (batch_update_end_time is None or
        batch_update_start_time is None or
        online_update_end_time is None or
        online_update_start_time is None):
        print(f"It seems that workflow {workflow_id} does not contain valid data. Skipping...")
        return

    try:
        batch_update_durations = batch_update_end_time - batch_update_start_time
        online_update_durations = online_update_end_time - online_update_start_time
    except Exception:
        # This would catch exceptions like:
        #   ValueError: operands could not be broadcast together with shapes (3,) (4,)
        print(f"It seems that workflow {workflow_id} is incompelete. Skipping...")
        return

    data = {
        "batch_update_durations": batch_update_durations.tolist(),
        "online_update_durations": online_update_durations.tolist(),
    }
    with open(fn, "w") as f:
        json.dump(data, f)


if __name__ == "__main__":
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    for wid in get_workflows():
        print("Collecting results of workflow", wid, "." * 20)
        analyze(wid)
        print("\n")
