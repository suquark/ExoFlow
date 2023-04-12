import os
import time

import pytest
import ray
import exoflow
from exoflow.tests import utils

from exoflow.tests.test_complex_workflow import (
    generate_layered_dag,
    generate_random_dag,
)


@ray.remote
def gather_and_hash_flaky(*inputs):
    import hashlib

    from exoflow.workflow_context import get_current_task_id

    task_id = get_current_task_id()
    if utils.check_global_mark(task_id):
        utils.unset_global_mark(task_id)
        os.kill(os.getpid(), 9)
        # raise ValueError(f"Intended - {task_id}")

    output = hashlib.sha256("-".join(inputs).encode()).hexdigest()
    sleep_duration = int(output, 16) / 2 ** 256 / 100
    time.sleep(sleep_duration)
    return output


def test_online_recovery_of_complex_workflow():
    utils.clear_marks()
    template = "example.gather_and_hash_flaky_{}"

    random_dag = generate_random_dag(gather_and_hash_flaky, 100)
    for i in range(1, 34):
        if i % 4 == 0:
            utils.set_global_mark(template.format(i))
    assert (
        workflow.run(random_dag)
        == "fc04fa8950c7d352cf34346d304e5eaf07a0e777a105895f41ac3971c1e74eeb"
    )

    width, layers = 5, 5
    layered_dag = generate_layered_dag(
        gather_and_hash_flaky, width=width, layers=layers
    )
    for i in range(1, width * layers):
        if i % 4 == 0:
            utils.set_global_mark(template.format(i))
    assert (
        workflow.run(layered_dag)
        == "a2776f91cc4da2125c8ac51c31a843185cbced7b24f9d0be5471f759b5710b29"
    )


def test_workflow_failure_with_ref(workflow_start_regular):
    fail_tags = [f"data_pipeline_fail_{i}" for i in range(5)]

    @ray.remote(max_retries=0)
    def data_pipeline():
        import numpy as np
        import os

        for t in fail_tags:
            if utils.check_global_mark(t):
                utils.unset_global_mark(t)
                os.kill(os.getpid(), 9)
        return np.ones(2 ** 20)

    @ray.remote
    def create_dataset():
        return [ray.put(234567), data_pipeline.remote()]

    @ray.remote
    def consumer(x):
        ray.get(x)
        # TODO(suquark): what if ray.get(task_1.remote(task_2.remote(x)))?
        #   In this case we might not know that actually it is 'x' that results
        #   in the failure. It looks like we need the entire lineage from Ray to
        #   handle such issues.

    @ray.remote
    def pass_through_1(x):
        return x

    @ray.remote
    def pass_through_2(x):
        return workflow.continuation(consumer.bind(x))

    dag_1 = consumer.bind(pass_through_1.bind(create_dataset.bind()))
    dag_2 = pass_through_2.bind(create_dataset.bind())

    from exoflow.debug_utils import execute_workflow_local

    utils.clear_marks()
    for t in fail_tags:
        utils.set_global_mark(t)

    workflow.run(dag_1, workflow_id="test_workflow_failure_with_ref")

    utils.clear_marks()
    for t in fail_tags:
        utils.set_global_mark(t)

    workflow.run(dag_2, workflow_id="test_workflow_failure_with_ref_2")


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
