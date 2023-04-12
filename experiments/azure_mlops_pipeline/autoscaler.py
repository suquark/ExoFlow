import math

import ray
from ray import workflow
from exoflow.workflow_context import get_current_workflow_id


def _clip(n, n_min, n_max):
    return min(max(n, n_min), n_max)


def _adjust_n_parallel(n_parallel: int, ratio: float, limit: int) -> int:
    # scale not greater than 2; n_parallel > 1
    return _clip(math.ceil(_clip(ratio, 0.5, 2) * n_parallel), 1, limit)


@ray.remote
def autoscale_load(n_iter: int, n_parallel: int, limit: int, enable_autoscale: bool):
    if n_iter < 2 or not enable_autoscale:
        return n_parallel
    load_metadata = workflow.get_task_execution_metadata(
        get_current_workflow_id(), task_id=f"load_{n_iter - 1}"
    )
    transform_metadata = workflow.get_task_execution_metadata(
        get_current_workflow_id(), task_id=f"transform_{n_iter - 2}"
    )
    ratio = load_metadata.duration / transform_metadata.duration
    print(f"[autoscale_load: {n_iter}] ratio = {ratio}")
    return _adjust_n_parallel(n_parallel, ratio, limit)


@ray.remote
def autoscale_transform(
    n_iter: int, n_parallel: int, limit: int, enable_autoscale: bool
):
    if n_iter < 2 or not enable_autoscale:
        return n_parallel
    transform_metadata = workflow.get_task_execution_metadata(
        get_current_workflow_id(), task_id=f"transform_{n_iter - 1}"
    )
    train_metadata = workflow.get_task_execution_metadata(
        get_current_workflow_id(), task_id=f"train_{n_iter - 2}"
    )
    ratio = transform_metadata.duration / train_metadata.duration
    print(f"[autoscale_transform: {n_iter}] ratio = {ratio}")
    return _adjust_n_parallel(n_parallel, ratio, limit)
