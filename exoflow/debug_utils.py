"""Utils for debugging purpose."""
import time
from typing import Optional
import uuid

import ray
from ray.dag import DAGNode, DAGInputData

from ray.workflow.common import asyncio_run
from ray.workflow.workflow_executor import WorkflowExecutor
from ray.workflow.workflow_context import workflow_task_context, WorkflowTaskContext
from ray.workflow.workflow_storage import get_workflow_storage


def run_workflow_local(
    dag: DAGNode, workflow_id: Optional[str] = None, *args, **kwargs
):
    """Execute the workflow locally."""
    from ray.workflow.workflow_state_from_dag import workflow_state_from_dag
    from ray.workflow.task_executor import ActorController

    ray.workflow.init()

    if workflow_id is None:
        # Workflow ID format: {Entry workflow UUID}.{Unix time to nanoseconds}
        workflow_id = f"{str(uuid.uuid4())}.{time.time():.9f}"
    job_id = ray.get_runtime_context().job_id.hex()
    context = WorkflowTaskContext(workflow_id=workflow_id)
    with workflow_task_context(context):
        wf_store = get_workflow_storage()
        state = workflow_state_from_dag(
            dag, DAGInputData(*args, **kwargs), workflow_id=workflow_id
        )
        wf_store.save_workflow_execution_state("", state)
        actor_controller = ActorController()
        executor = WorkflowExecutor(state, actor_controller)
        fut = executor.get_task_output_async(state.output_task_id)
        asyncio_run(executor.run_until_complete(job_id, context, wf_store))
        return asyncio_run(fut)


def resume_workflow_local(workflow_id: str):
    """Resume the workflow locally."""
    from ray.workflow.workflow_state_from_storage import workflow_state_from_storage
    from ray.workflow.task_executor import ActorController

    job_id = ray.get_runtime_context().job_id.hex()
    context = WorkflowTaskContext(workflow_id=workflow_id)
    with workflow_task_context(context):
        wf_store = get_workflow_storage()
        state = workflow_state_from_storage(workflow_id, None)
        actor_controller = ActorController()
        executor = WorkflowExecutor(state, actor_controller)
        fut = executor.get_task_output_async(state.output_task_id)
        asyncio_run(executor.run_until_complete(job_id, context, wf_store))
        return asyncio_run(fut)
