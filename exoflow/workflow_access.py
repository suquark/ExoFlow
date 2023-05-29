import asyncio
import copy
import json
import logging
import os
import queue
import random
from typing import Dict, List, Set, Optional, TYPE_CHECKING

import ray
from ray.experimental.internal_kv import _internal_kv_put

from exoflow import common
from exoflow.common import WorkflowStatus, TaskID, SERVICE_SEP
from exoflow import workflow_state_from_storage
from exoflow import workflow_context
from exoflow import workflow_storage
from exoflow.exceptions import (
    WorkflowCancellationError,
    WorkflowNotFoundError,
    WorkflowNotResumableError,
    WorkflowStillActiveError,
)
from exoflow.task_executor import ActorController
from exoflow.workflow_executor import WorkflowExecutor, TaskExecutionMetadata
from exoflow.workflow_state import WorkflowExecutionState
from exoflow.workflow_context import WorkflowTaskContext
from exoflow import utils

if TYPE_CHECKING:
    from ray.actor import ActorHandle

logger = logging.getLogger(__name__)


class SelfResolvingObject:
    def __init__(self, x):
        self.x = x

    def __reduce__(self):
        return ray.get, (self.x,)


@ray.remote(num_cpus=0)
def load_task_output_from_storage(workflow_id: str, task_id: Optional[TaskID]):
    wf_store = workflow_storage.WorkflowStorage(workflow_id)
    tid = wf_store.inspect_output(task_id)
    if tid is not None:
        return wf_store.load_task_output(tid)
    # TODO(suquark): Unify the error from "exoflow.get_output" & "exoflow.run_async".
    # Currently they could be different, because "exoflow.get_output" could
    # get the output from a stopped workflow, it does not may sense to raise
    # "WorkflowExecutionError" as the workflow is not running.
    if task_id is not None:
        raise ValueError(
            f"Cannot load output from task id '{task_id}' in workflow '{workflow_id}'"
        )
    else:
        raise ValueError(f"Cannot load output from workflow '{workflow_id}'")


@ray.remote(num_cpus=0)
def resume_workflow_task(
    job_id: str,
    workflow_id: str,
    task_id: Optional[TaskID] = None,
) -> WorkflowExecutionState:
    """Resume a task of a workflow.

    Args:
        job_id: The ID of the job that submits the workflow execution. The ID
        is used to identify the submitter of the workflow.
        workflow_id: The ID of the workflow job. The ID is used to identify
            the workflow.
        task_id: The task to resume in the workflow.

    Raises:
        WorkflowNotResumableException: fail to resume the workflow.

    Returns:
        The execution result of the workflow, represented by Ray ObjectRef.
    """
    with workflow_context.workflow_logging_context(job_id):
        try:
            return workflow_state_from_storage.workflow_state_from_storage(
                workflow_id, task_id
            )
        except Exception as e:
            raise WorkflowNotResumableError(workflow_id) from e


# TODO(suquark): we may use an actor pool in the future if too much
# concurrent workflow access blocks the actor.
@ray.remote
class WorkflowManagementActor:
    """Keep the ownership and manage the workflow output."""

    def __init__(self, max_running_workflows: int, max_pending_workflows: int):
        self._workflow_executors: Dict[str, WorkflowExecutor] = {}

        self._max_running_workflows: int = max_running_workflows
        self._max_pending_workflows: int = max_pending_workflows

        # 0 means infinite for queue
        self._workflow_queue = queue.Queue(
            max_pending_workflows if max_pending_workflows != -1 else 0
        )

        self._running_workflows: Set[str] = set()
        self._queued_workflows: Dict[str, asyncio.Future] = {}
        # TODO(suquark): We do not cleanup "_executed_workflows" because we need to
        #  know if users are running the same workflow again long after a workflow
        #  completes. One possible alternative solution is to check the workflow
        #  status in the storage.
        self._executed_workflows: Set[str] = set()
        self._service_workflows: Dict[str, WorkflowExecutionState] = {}
        self._actor_controller = ActorController()
        import resource
        import sys

        try:
            resource.setrlimit(resource.RLIMIT_STACK, (2 ** 29, -1))
        except ValueError:
            pass
        sys.setrecursionlimit(10 ** 6)

    def validate_init_options(
        self, max_running_workflows: Optional[int], max_pending_workflows: Optional[int]
    ):
        if (
            max_running_workflows is not None
            and max_running_workflows != self._max_running_workflows
        ) or (
            max_pending_workflows is not None
            and max_pending_workflows != self._max_pending_workflows
        ):
            raise ValueError(
                "The workflow init is called again but the init options"
                "does not match the original ones. Original options: "
                f"max_running_workflows={self._max_running_workflows} "
                f"max_pending_workflows={self._max_pending_workflows}; "
                f"New options: max_running_workflows={max_running_workflows} "
                f"max_pending_workflows={max_pending_workflows}."
            )

    def gen_task_id(self, workflow_id: str, task_name: str) -> str:
        wf_store = workflow_storage.WorkflowStorage(workflow_id)
        idx = wf_store.gen_task_id(task_name)
        if idx == 0:
            return task_name
        else:
            return f"{task_name}_{idx}"

    async def execute_service(
        self,
        workflow_id: str,
        service_id: str,
        job_id: str,
        persist_input: bool,
        input_dict: Dict,
    ):
        state = self._service_workflows.get(workflow_id)
        assert state is not None
        new_state = copy.deepcopy(state)
        new_state.input_dict = input_dict
        service_workflow_id = common.generate_service_workflow_id(
            workflow_id, service_id
        )
        self.submit_workflow(service_workflow_id, new_state)
        context = WorkflowTaskContext(workflow_id=service_workflow_id)
        output_ref = await self.execute_workflow(
            job_id,
            context,
            auto_resolve_output=False,
            persist_input=persist_input,
        )
        return await output_ref

    def submit_workflow(
        self,
        workflow_id: str,
        state: WorkflowExecutionState,
        ignore_existing: bool = False,
        is_service: bool = False,
    ):
        """Submit workflow. A submitted workflow can be executed later.

        Args:
            workflow_id: ID of the workflow.
            state: The initial state of the workflow.
            ignore_existing: Ignore existing executed workflows.
        """
        if is_service:
            if workflow_id in self._service_workflows:
                raise RuntimeError(
                    f"Workflow[id={workflow_id}] is already been "
                    f"registered as a service."
                )
            self._service_workflows[workflow_id] = state
            return

        if workflow_id in self._workflow_executors:
            raise RuntimeError(f"Workflow[id={workflow_id}] is being executed.")
        if workflow_id in self._executed_workflows and not ignore_existing:
            raise RuntimeError(f"Workflow[id={workflow_id}] has been executed.")

        if state.output_task_id is None:
            raise ValueError(
                "No root DAG specified that generates output for the workflow."
            )

        wf_store = workflow_storage.WorkflowStorage(workflow_id)
        is_service_instance = SERVICE_SEP in workflow_id
        if (
            self._max_running_workflows != -1
            and len(self._running_workflows) >= self._max_running_workflows
        ):
            try:
                self._workflow_queue.put_nowait(workflow_id)
                self._queued_workflows[workflow_id] = asyncio.Future()
                if not is_service_instance:
                    wf_store.update_workflow_status(WorkflowStatus.PENDING)
            except queue.Full:
                # override with our error message
                raise queue.Full("Workflow queue has been full") from None
        else:
            self._running_workflows.add(workflow_id)
            if not is_service_instance:
                wf_store.update_workflow_status(WorkflowStatus.RUNNING)
        # initialize executor
        self._workflow_executors[workflow_id] = WorkflowExecutor(
            state, self._actor_controller
        )

    async def reconstruct_workflow(
        self, job_id: str, context: WorkflowTaskContext
    ) -> None:
        """Reconstruct a (failed) workflow and submit it."""
        state = await resume_workflow_task.remote(job_id, context.workflow_id)
        self.submit_workflow(context.workflow_id, state, ignore_existing=True)

    async def execute_workflow(
        self,
        job_id: str,
        context: WorkflowTaskContext,
        auto_resolve_output: bool = True,
        persist_input: bool = False,
    ) -> ray.ObjectRef:
        """Execute a submitted workflow.

        Args:
            job_id: The ID of the job for logging.
            context: The execution context.
        Returns:
            An object ref that represent the result.
        """
        workflow_id = context.workflow_id
        if workflow_id not in self._workflow_executors:
            raise RuntimeError(f"Workflow '{workflow_id}' has not been submitted.")

        pending_fut = self._queued_workflows.get(workflow_id)
        if pending_fut is not None:
            await pending_fut  # wait until this workflow is ready to go

        wf_store = workflow_storage.WorkflowStorage(workflow_id)
        executor = self._workflow_executors[workflow_id]
        try:
            await executor.run_until_complete(job_id, context, wf_store, persist_input)
            if auto_resolve_output:
                return await self.get_output(workflow_id, executor.output_task_id)
            else:
                return executor.get_state().get_input(
                    executor.output_task_id, indirect=False
                )
        finally:
            self._workflow_executors.pop(workflow_id)
            self._running_workflows.remove(workflow_id)
            self._executed_workflows.add(workflow_id)
            if not self._workflow_queue.empty():
                # schedule another workflow from the pending queue
                next_workflow_id = self._workflow_queue.get_nowait()
                self._running_workflows.add(next_workflow_id)
                fut = self._queued_workflows.pop(next_workflow_id)
                fut.set_result(None)

    async def cancel_workflow(self, workflow_id: str) -> None:
        """Cancel workflow execution."""
        if workflow_id in self._workflow_executors:
            executor = self._workflow_executors[workflow_id]
            fut = executor.get_task_output_async(executor.output_task_id)
            executor.cancel()
            try:
                # Wait until cancelled, otherwise workflow status may not
                # get updated after "workflow.cancel()" is called.
                await fut
            except WorkflowCancellationError:
                pass
        else:
            wf_store = workflow_storage.WorkflowStorage(workflow_id)
            wf_store.update_workflow_status(WorkflowStatus.CANCELED)

    def get_workflow_status(self, workflow_id: str) -> WorkflowStatus:
        """Get the status of the workflow."""
        if workflow_id in self._workflow_executors:
            if workflow_id in self._queued_workflows:
                return WorkflowStatus.PENDING
            return WorkflowStatus.RUNNING
        store = workflow_storage.get_workflow_storage(workflow_id)
        status = store.load_workflow_status()
        if status == WorkflowStatus.NONE:
            raise WorkflowNotFoundError(workflow_id)
        elif status in WorkflowStatus.non_terminating_status():
            return WorkflowStatus.RESUMABLE
        return status

    def is_workflow_non_terminating(self, workflow_id: str) -> bool:
        """True if the workflow is still running or pending."""
        return workflow_id in self._workflow_executors

    def list_non_terminating_workflows(self) -> Dict[WorkflowStatus, List[str]]:
        """List workflows whose status are not of terminated status."""
        result = {WorkflowStatus.RUNNING: [], WorkflowStatus.PENDING: []}
        for wf in self._workflow_executors.keys():
            if wf in self._running_workflows:
                result[WorkflowStatus.RUNNING].append(wf)
            else:
                result[WorkflowStatus.PENDING].append(wf)
        return result

    async def get_output(
        self, workflow_id: str, task_id: Optional[TaskID]
    ) -> ray.ObjectRef:
        """Get the output of a running workflow.

        Args:
            workflow_id: The ID of a workflow job.
            task_id: If set, fetch the specific task output instead of the output
                of the workflow.

        Returns:
            An object reference that can be used to retrieve the workflow result.
        """
        ref = None
        if self.is_workflow_non_terminating(workflow_id):
            executor = self._workflow_executors[workflow_id]
            if task_id is None:
                task_id = executor.output_task_id
            workflow_ref = await executor.get_task_output_async(task_id)
            task_id, ref = workflow_ref.task_id, workflow_ref.ref
        if ref is None:
            wf_store = workflow_storage.WorkflowStorage(workflow_id)
            tid = wf_store.inspect_output(task_id)
            if tid is not None:
                ref = load_task_output_from_storage.remote(workflow_id, task_id)
            elif task_id is not None:
                raise ValueError(
                    f"Cannot load output from task id '{task_id}' in workflow "
                    f"'{workflow_id}'"
                )
            else:
                raise ValueError(f"Cannot load output from workflow '{workflow_id}'")
        return SelfResolvingObject(ref)

    def delete_workflow(self, workflow_id: str) -> None:
        """Delete a workflow, its checkpoints, and other information it may have
           persisted to storage.

        Args:
            workflow_id: The workflow to delete.

        Raises:
            WorkflowStillActiveError: The workflow is still active.
            WorkflowNotFoundError: The workflow does not exist.
        """
        if self.is_workflow_non_terminating(workflow_id):
            raise WorkflowStillActiveError("DELETE", workflow_id)
        wf_storage = workflow_storage.WorkflowStorage(workflow_id)
        wf_storage.delete_workflow()
        self._executed_workflows.discard(workflow_id)

    def create_http_event_provider(self) -> None:
        """Deploy an HTTPEventProvider as a Serve deployment with
        name = common.HTTP_EVENT_PROVIDER_NAME, if one doesn't exist
        """
        ray.serve.start(detached=True)
        try:
            ray.serve.get_deployment(common.HTTP_EVENT_PROVIDER_NAME)
        except KeyError:
            from exoflow.http_event_provider import HTTPEventProvider

            HTTPEventProvider.deploy()

    def ready(self) -> None:
        """A no-op to make sure the actor is ready."""

    def get_task_execution_metadata(
        self, workflow_id: str, task_id: TaskID
    ) -> TaskExecutionMetadata:
        executor = self._workflow_executors[workflow_id]
        return executor.get_state().task_execution_metadata[task_id]

    def _start_profile(self):
        import cProfile

        self._profiler = cProfile.Profile()
        self._profiler.enable()

    def _stop_profile(self, dump_file_path: str):
        import pstats

        self._profiler.disable()
        sortby = pstats.SortKey.CUMULATIVE
        ps = pstats.Stats(self._profiler).sort_stats(sortby)
        ps.dump_stats(dump_file_path)
        del self._profiler


def init_management_actor(
    max_running_workflows: Optional[int], max_pending_workflows: Optional[int]
) -> None:
    """Initialize WorkflowManagementActor.

    Args:
        max_running_workflows: The maximum number of concurrently running workflows.
            Use -1 as infinity. Use 'None' for keeping the original value if the actor
            exists, or it is equivalent to infinity if the actor does not exist.
        max_pending_workflows: The maximum number of queued workflows.
            Use -1 as infinity. Use 'None' for keeping the original value if the actor
            exists, or it is equivalent to infinity if the actor does not exist.
    """
    try:
        actor = get_management_actor()
        # Check if max_running_workflows/max_pending_workflows
        # matches the previous settings.
        ray.get(
            actor.validate_init_options.remote(
                max_running_workflows, max_pending_workflows
            )
        )
    except ValueError:
        logger.info("Initializing workflow manager...")
        if max_running_workflows is None:
            max_running_workflows = -1
        if max_pending_workflows is None:
            max_pending_workflows = -1

        controller_resources = os.getenv("EXOFLOW_CONTROLLER_RESOURCES", default=None)
        if controller_resources is None:
            # If no resources are specified, then the default scheduling
            # strategy is "local": the ExoFlow controller will be scheduled
            # on the same node as the Ray driver.
            from ray._private.worker import global_worker
            from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

            node_id = global_worker.core_worker.get_current_node_id().hex()
            scheduling_strategy = NodeAffinitySchedulingStrategy(node_id, soft=False)
            resources = None
        else:
            scheduling_strategy = None
            resources = json.loads(controller_resources)

        # start only local workers, this ensures the number of workers
        # would only be proportional to the number of workflow shards
        # instead of the number of ExoFlow workers
        local_executors_only = int(os.getenv("EXOFLOW_LOCAL_EXECUTORS_ONLY", default=0))
        _internal_kv_put(
            "local_executors_only", str(local_executors_only), namespace="workflow"
        )

        # Total number of workflow controllers.
        n_workflow_shards = int(os.getenv("N_WORKFLOW_SHARDS", default=1))
        _internal_kv_put("n_shards", str(n_workflow_shards), namespace="workflow")

        # Number of workers per node. -1 means the number is proportional to
        # the number of CPU cores. If "local_executors_only" is 1, then there is only
        # one node.
        n_workflow_workers = int(os.getenv("N_WORKFLOW_WORKERS", default=-1))
        _internal_kv_put("n_workers", str(n_workflow_workers), namespace="workflow")

        n_workflow_worker_threads = int(
            os.getenv("N_WORKFLOW_WORKER_THREADS", default=1)
        )
        _internal_kv_put(
            "n_worker_threads", str(n_workflow_worker_threads), namespace="workflow"
        )
        scheduler_max_concurrency = int(
            os.getenv("WORKFLOW_SCHEDULER_MAX_CONCURRENCY", default=1000)
        )

        actors_ready = []
        for i in range(n_workflow_shards):
            if i == 0:
                name = common.MANAGEMENT_ACTOR_NAME
            else:
                name = common.MANAGEMENT_ACTOR_NAME + f"_{i}"
            # the actor does not exist
            actor = WorkflowManagementActor.options(
                name=name,
                namespace=common.MANAGEMENT_ACTOR_NAMESPACE,
                lifetime="detached",
                num_cpus=0,
                max_concurrency=scheduler_max_concurrency,
                scheduling_strategy=scheduling_strategy,
                resources=resources,
            ).remote(max_running_workflows, max_pending_workflows)
            # No-op to ensure the actor is created before the driver exits.
            actors_ready.append(actor.ready.remote())
        ray.get(actors_ready)


_workflow_manager_actor_index = None
_actor_cache = utils.NamedActorCache()
_kv_cache = utils.KVCache()


def get_management_actor(index: Optional[int] = 0) -> "ActorHandle":
    global _workflow_manager_actor_index
    if index is None:
        n_workflow_shards = int(_kv_cache("n_shards"))
        # round robin scheduling
        if _workflow_manager_actor_index is None:
            _workflow_manager_actor_index = random.randrange(n_workflow_shards)
        else:
            _workflow_manager_actor_index += 1
            _workflow_manager_actor_index %= n_workflow_shards
        index = _workflow_manager_actor_index

    if index == 0:
        name = common.MANAGEMENT_ACTOR_NAME
    else:
        name = common.MANAGEMENT_ACTOR_NAME + f"_{index}"

    return _actor_cache(name)
