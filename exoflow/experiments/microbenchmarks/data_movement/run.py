from typing import Dict

import argparse
import dataclasses
import json
from datetime import datetime

import numpy as np
import tqdm

import run_airflow
import run_stepfunctions
import run_workflow
from config import (
    POINTS,
    AIRFLOW_SIZE_LIMIT,
    STEPFUNCTION_LIMIT,
    LAMBDAS_LIMIT,
    CHECKPOINT_MAP,
    WORKFLOW_LAMBDA_PREFIX,
    WORKFLOW_RAY_PREFIX,
)


@dataclasses.dataclass
class LatencyRecord:
    trigger_latency_mean: Dict[int, float] = dataclasses.field(default_factory=dict)
    trigger_latency_std: Dict[int, float] = dataclasses.field(default_factory=dict)
    step_latency_mean: Dict[int, float] = dataclasses.field(default_factory=dict)
    step_latency_std: Dict[int, float] = dataclasses.field(default_factory=dict)


def benchmark_airflow(n_repeats: int):
    start_time = datetime.utcnow()
    points = [p for p in POINTS if p < AIRFLOW_SIZE_LIMIT]
    for _ in tqdm.trange(n_repeats, desc="airflow"):
        for size in points:
            run_airflow.trigger_dag_and_wait(
                "MyAirflowEnvironment", "sendrecv", {"size": size}, verbose=False
            )
    # collect data
    data = {"trigger_latency": {}, "step_latency": {}}
    for size in points:
        data["trigger_latency"][size] = run_airflow.get_cloudwatch_data(
            "sendrecv_trigger_latency", start_time, size
        )
        data["step_latency"][size] = run_airflow.get_cloudwatch_data(
            "sendrecv_step_latency", start_time, size
        )
    # process data
    result = LatencyRecord()
    for size, v in data["trigger_latency"].items():
        mean, std = np.mean(v), np.std(v)
        result.trigger_latency_mean[size] = mean.item()
        result.trigger_latency_std[size] = std.item()
    for size, v in data["step_latency"].items():
        mean, std = np.mean(v), np.std(v)
        result.step_latency_mean[size] = mean.item()
        result.step_latency_std[size] = std.item()
    return result


def _process_lambda_data(data: Dict):
    result = LatencyRecord()
    for size, v in data.items():
        trigger_latency = []
        step_latency = []
        for dp in v:
            trigger_latency.append(dp["start_time"] - dp["trigger_time"])
            step_latency.append(dp["end_time"] - dp["start_time"])
        result.trigger_latency_mean[size] = np.mean(trigger_latency).item()
        result.trigger_latency_std[size] = np.std(trigger_latency).item()
        result.step_latency_mean[size] = np.mean(step_latency).item()
        result.step_latency_std[size] = np.std(step_latency).item()
    return result


def benchmark_standard_stepfunctions(n_repeats: int):
    points = [p for p in POINTS if p < STEPFUNCTION_LIMIT]
    data = {size: [] for size in points}
    for _ in tqdm.trange(n_repeats, desc="standard_stepfunctions"):
        for size in points:
            data[size].append(
                run_stepfunctions.invoke_standard_stepfunction(
                    "SendRecvStateMachineStandard", {"size": size}
                )
            )
    return _process_lambda_data(data)


def benchmark_express_stepfunctions(n_repeats: int):
    points = [p for p in POINTS if p < STEPFUNCTION_LIMIT]
    data = {size: [] for size in points}
    for _ in tqdm.trange(n_repeats, desc="express_stepfunctions"):
        for size in points:
            data[size].append(
                run_stepfunctions.invoke_express_stepfunction(
                    "SendRecvStateMachine", {"size": size}
                )
            )
    return _process_lambda_data(data)


def benchmark_workflow_and_ray(url, workflow_id: str, n_repeats: int):
    if workflow_id.startswith(WORKFLOW_LAMBDA_PREFIX):
        points = [p for p in POINTS if p < LAMBDAS_LIMIT]
    else:
        points = POINTS
    data = {}
    for size in points:
        data[size] = run_workflow.send_workflow_request(
            url, workflow_id, size, n_repeats
        )
    return _process_lambda_data(data)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="benchmark data movement")
    parser.add_argument("host", help="The IP of the workflow server")
    parser.add_argument(
        "--port", type=int, help="The port of the workflow server", default=8080
    )
    args = parser.parse_args()

    N_REPEATS = 5

    result = benchmark_airflow(N_REPEATS)
    with open("result/airflow.json", "w") as f:
        json.dump(dataclasses.asdict(result), f)

    result = benchmark_standard_stepfunctions(N_REPEATS)
    with open("result/standard_stepfunctions.json", "w") as f:
        json.dump(dataclasses.asdict(result), f)

    result = benchmark_express_stepfunctions(N_REPEATS)
    with open("result/express_stepfunctions.json", "w") as f:
        json.dump(dataclasses.asdict(result), f)

    url = f"http://{args.host}:{args.port}"

    def _benchmark_workflow_and_ray(workflow_id: str):
        result = benchmark_workflow_and_ray(url, workflow_id, N_REPEATS)
        with open(f"result/{workflow_id}.json", "w") as f:
            json.dump(dataclasses.asdict(result), f)

    for name in CHECKPOINT_MAP.keys():
        _benchmark_workflow_and_ray(WORKFLOW_LAMBDA_PREFIX + name)
    for name in CHECKPOINT_MAP.keys():
        _benchmark_workflow_and_ray(WORKFLOW_RAY_PREFIX + name)
    _benchmark_workflow_and_ray("ray")
