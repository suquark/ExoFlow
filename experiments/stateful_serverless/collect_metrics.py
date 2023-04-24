import json
import os

import numpy as np


def _get_cloudwatch_latency(path: str):
    with open(path) as f:
        text = f.read()
    p50l, p99l = text.splitlines()[1:3]
    return float(p50l.replace("Median: ", "")), float(
        p99l.replace("99 Percentile: ", "")
    )


def get_cloudwatch_data(dirname: str):
    p50s, p99s = [], []
    for i in range(100, 1001, 100):
        path = f"{dirname}/beldi/hotel-metrics-{i}.txt"
        p50, p99 = _get_cloudwatch_latency(path)
        p50s.append(p50)
        p99s.append(p99)
    return {"p50": p50s, "p99": p99s}


def get_cloudwatch_reserve_data(dirname: str):
    p50, p99 = _get_cloudwatch_latency(f"{dirname}/beldi/hotel-metrics-reserve.txt")
    return {"p50": p50, "p99": p99}


def _get_fastapi_latency(subdir: str, warmup_ratio=0.1):
    latencies = []
    for name in os.listdir(subdir):
        with open(f"{subdir}/{name}") as f:
            text = f.read()
        lines = text.splitlines()
        lines = lines[round(warmup_ratio * len(lines)) :]
        for row in lines:
            if not row:
                continue
            latencies.append(float(row.split(", ")[1]) * 1000)
    latencies = np.array(latencies)
    return np.median(latencies).item(), np.percentile(latencies, 99).item()


def _try_get_fastapi_latency(subdir: str):
    try:
        p50, p99 = _get_fastapi_latency(subdir)
        assert not np.isnan(p50) and not np.isnan(p99)
        return p50, p99
    except Exception:
        print(f"Failed to get latency for {subdir}. Set the result to 0. "
              "It could be that the related experiment is missing. "
              "If this is intended, please ignore this error.")
        return 0, 0


def get_workflow_data(dirname: str):
    p50s, p99s = [], []
    for i in range(100, 1001, 100):
        subdir = f"{dirname}/exoflow/temp/{i}"
        p50, p99 = _try_get_fastapi_latency(subdir)
        p50s.append(p50)
        p99s.append(p99)
    return {"p50": p50s, "p99": p99s}


def get_workflow_failure_data(dirname: str):
    p50s, p99s = [], []
    for i in range(100, 1001, 100):
        subdir = f"{dirname}/failure-{i}"
        p50, p99 = _try_get_fastapi_latency(subdir)
        p50s.append(p50)
        p99s.append(p99)
    return {"p50": p50s, "p99": p99s}


def get_workflow_reserve_data(dirname: str):
    apis = (
        "reserve",
        "reserve_serial",
        "reserve_skipckpt",
        "reserve_overlapckpt",
        "reserve_nooverlapckpt",
    )
    result = {}
    for name in apis:
        subdir = f"{dirname}/workflow-{name}"
        p50, p99 = _get_fastapi_latency(subdir)
        result[name] = {"p50": p50, "p99": p99}
    return result


if __name__ == "__main__":
    result_dir = "result"
    data = {
        "beldi-cloudwatch": get_cloudwatch_data(result_dir),
        "workflow-server": get_workflow_data(result_dir),
        "workflow-server-failure": get_workflow_failure_data(result_dir),
        "beldi-cloudwatch-reserve": get_cloudwatch_reserve_data(result_dir),
        "workflow-server-reserve": get_workflow_reserve_data(result_dir),
    }
    with open("result/result.json", "w") as f:
        json.dump(data, f, indent=2)
