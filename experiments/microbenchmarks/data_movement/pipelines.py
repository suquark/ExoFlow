import time
import numpy as np

import ray
from ray import workflow
from ray.dag import InputNode
from ray.workflow.api import register_service
from ray.workflow.lambda_executor import ray_invoke_lambda

from config import (
    CHECKPOINT_MAP,
    WORKFLOW_LAMBDA_PREFIX,
    WORKFLOW_RAY_PREFIX,
    LAMBDAS_PREFIX,
)


@ray.remote
def idle():
    pass


@ray.remote
def producer(trigger_time: float, size: int):
    return {
        "trigger_time": trigger_time,
        "start_time": time.time(),
        "data": np.ones(size, dtype=np.uint8),
    }


@ray.remote
def consumer(input_dict):
    return {
        "trigger_time": input_dict["trigger_time"],
        "start_time": input_dict["start_time"],
        "end_time": time.time(),
        "size": len(input_dict["data"]),
    }


@ray.remote
def sync_checkpoint(x):
    return x


def _embed_input(embed_input):
    return embed_input


def run_ray(size: int, trigger_time: float):
    p = producer.remote(size=size, trigger_time=trigger_time)
    return consumer.remote(p)


def register_dags():
    register_service(idle.bind(), workflow_id="idle")

    for name, option in CHECKPOINT_MAP.items():
        # Workflow + Lambdas
        with InputNode() as dag_input:
            producer_output = ray_invoke_lambda.options(
                **workflow.options(checkpoint=option)
            ).bind(
                LAMBDAS_PREFIX + "producer",
                trigger_time=dag_input.trigger_time,
                size=dag_input.size,
            )
            consumer_output = ray_invoke_lambda.options(
                **workflow.options(checkpoint=False)
            ).bind(
                LAMBDAS_PREFIX + "consumer",
                decoder=_embed_input,
                embed_input=producer_output,
            )
            dag = sync_checkpoint.options(
                **workflow.options(
                    checkpoint=False,
                    wait_until_committed=[LAMBDAS_PREFIX + "producer"],
                )
            ).bind(consumer_output)

            register_service(dag, workflow_id=WORKFLOW_LAMBDA_PREFIX + name)

        # Workflow + Ray
        with InputNode() as dag_input:
            producer_output = producer.options(
                **workflow.options(checkpoint=option, name="producer")
            ).bind(
                trigger_time=dag_input.trigger_time,
                size=dag_input.size,
            )
            consumer_output = consumer.options(
                **workflow.options(checkpoint=False)
            ).bind(producer_output)
            dag = sync_checkpoint.options(
                **workflow.options(
                    checkpoint=False,
                    wait_until_committed=["producer"],
                )
            ).bind(consumer_output)

            register_service(dag, workflow_id=WORKFLOW_RAY_PREFIX + name)
