import asyncio
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
import inspect
import logging
from typing import List, Tuple, Any, Dict, Callable, Optional, TYPE_CHECKING
import ray
from ray import ObjectRef
from ray import cloudpickle
from ray.actor import ActorHandle
from ray._private import signature
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
from ray.experimental.internal_kv import _internal_kv_get

from ray.dag import DAGNode
from exoflow import workflow_context
from exoflow.workflow_context import get_task_status_info
from exoflow import serialization_context
from exoflow import workflow_storage

from exoflow.common import (
    WorkflowStatus,
    WorkflowExecutionMetadata,
    TaskID,
    WorkflowRef,
    CheckpointMode,
    WorkflowTaskRuntimeOptions,
)
from exoflow.workflow_state import WorkflowExecutionState
from exoflow.workflow_state_from_dag import workflow_state_from_dag

# TODO(suquark): This is a temporary module.
from exoflow import dynamodb

if TYPE_CHECKING:
    from exoflow.workflow_context import WorkflowTaskContext


_USE_DYNAMODB = True
logger = logging.getLogger(__name__)
TaskHandle = Tuple[str, TaskID]


@ray.remote
class WorkflowTaskActor:
    def __init__(self):
        # A thread pool for checkpointing.
        sys.setrecursionlimit(10 ** 6)
        n_worker_threads = int(
            _internal_kv_get("n_worker_threads", namespace="workflow")
        )
        self._io_thread_pool = ThreadPoolExecutor(max_workers=n_worker_threads)
        self._futures: Dict[str, Dict] = {}

    def async_submit_task(
        self, workflow_id: str, task_id: TaskID, fn: Callable, *args, **kwargs
    ):
        fut = self._io_thread_pool.submit(fn, *args, **kwargs)
        self._futures.setdefault(workflow_id, {})[task_id] = fut

    async def join_async_tasks(
        self, workflow_id: str, task_ids: Optional[List[TaskID]] = None
    ):
        futures = []
        if task_ids is None:
            tasks = self._futures.pop(workflow_id, {})
            for f in tasks.values():
                futures.append(asyncio.wrap_future(f))
        else:
            if workflow_id not in self._futures:
                return
            for task_id in task_ids:
                f = self._futures[workflow_id].pop(task_id, None)
                if f is not None:
                    futures.append(asyncio.wrap_future(f))
            if not self._futures[workflow_id]:
                del self._futures[workflow_id]
        await asyncio.gather(*futures)

    @ray.method(num_returns=2)
    async def submit(
        self,
        func: bytes,
        context: "WorkflowTaskContext",
        job_id: str,
        task_id: "TaskID",
        baked_inputs: "_BakedWorkflowInputs",
        runtime_options: "WorkflowTaskRuntimeOptions",
    ) -> Any:
        """The remote version of '_workflow_task_executor'."""
        with workflow_context.workflow_logging_context(job_id):
            return await _workflow_task_executor(
                func,
                context,
                task_id,
                baked_inputs,
                runtime_options,
                self.async_submit_task,
            )

    async def save_input_dict(self, workflow_id: str, input_dict: Dict):
        # TODO(suquark): this is a temporary solution. We should enable mixing storages
        if _USE_DYNAMODB:
            await dynamodb.save_input_dict(workflow_id, input_dict)
        else:
            wf_store = workflow_storage.WorkflowStorage(workflow_id)
            fut = self._io_thread_pool.submit(
                wf_store.save_workflow_input_dict, input_dict
            )
            await asyncio.wrap_future(fut)

    @ray.method(num_returns=2)
    def recover_from_checkpoints(
        self, workflow_id: str, task_id: TaskID, baked_inputs: "_BakedWorkflowInputs"
    ):
        wf_store = workflow_storage.WorkflowStorage(workflow_id)
        tid = wf_store.inspect_output(task_id)
        if tid is not None:
            object_refs = []
            for ref in baked_inputs.object_refs:
                if isinstance(ref, str):
                    object_refs.append(wf_store.load_object_ref(ref))
                else:
                    object_refs.append(ref)
            with serialization_context.workflow_args_keeping_context():
                return (
                    WorkflowExecutionMetadata(
                        object_refs=object_refs,
                        actors=baked_inputs.actors,
                        is_checkpoint_loader=True,
                    ),
                    wf_store.load_task_output(tid),
                )
        # TODO(suquark): Unify the error from "exoflow.get_output" & "exoflow.run_async".
        # Currently they could be different, because "exoflow.get_output" could
        # get the output from a stopped workflow, it does not may sense to raise
        # "WorkflowExecutionError" as the workflow is not running.
        raise ValueError(
            f"Cannot load output from task id '{task_id}' in workflow '{workflow_id}'"
        )


def _create_actor(node_id: str):
    # NOTE: use "soft=False" here instead for debugging.
    scheduling_strategy = NodeAffinitySchedulingStrategy(node_id, soft=True)
    return WorkflowTaskActor.options(
        num_cpus=0,
        max_restarts=-1,
        scheduling_strategy=scheduling_strategy,
    ).remote()


class AsyncInvokeClient:
    async def invoke(self, name: str, payload: bytes):
        raise NotImplementedError


class ActorPool:
    def __init__(self, strategies: List[Tuple]):
        self._idle_pool: List[ActorHandle] = []
        self._running_tasks: Dict[TaskHandle, ActorHandle] = {}
        self._actor_tasks_counter: Dict[ActorHandle, int] = {}
        self._isolated_tasks: Dict[TaskHandle, ActorHandle] = {}

        self._node_ids = set()
        for node_id, n_cpus in strategies:
            self._node_ids.add(node_id)
            self._idle_pool.extend(_create_actor(node_id) for _ in range(n_cpus))

    def _get_actor_with_least_tasks(self) -> ActorHandle:
        chosen_actor, min_count = None, float("inf")
        for actor, counter in self._actor_tasks_counter.items():
            if counter < min_count:
                chosen_actor, min_count = actor, counter
        return chosen_actor

    def _get_actor(
        self,
        workflow_id: str,
        task_id: TaskID,
        runtime_options: "WorkflowTaskRuntimeOptions",
    ):
        handle = (workflow_id, task_id)
        if runtime_options.isolation:
            actor = self._isolated_tasks.get(handle)
            if actor is None:
                # TODO(suquark): recycle isolated tasks after a workflow is done.
                actor = self._isolated_tasks.setdefault(
                    handle, _create_actor(list(self._node_ids)[0])
                )
        else:
            if self._idle_pool:
                actor = self._idle_pool.pop()
            else:
                assert self._actor_tasks_counter
                actor = self._get_actor_with_least_tasks()
            assert actor is not None
            self._actor_tasks_counter.setdefault(actor, 0)
            self._actor_tasks_counter[actor] += 1
            self._running_tasks[handle] = actor
        return actor

    def submit(
        self,
        func: bytes,
        context: "WorkflowTaskContext",
        job_id: str,
        task_id: "TaskID",
        baked_inputs: "_BakedWorkflowInputs",
        runtime_options: "WorkflowTaskRuntimeOptions",
    ):
        actor = self._get_actor(context.workflow_id, task_id, runtime_options)
        return actor.submit.remote(
            func, context, job_id, task_id, baked_inputs, runtime_options
        )

    def recover_from_checkpoints(
        self,
        workflow_id: str,
        task_id: TaskID,
        baked_inputs: "_BakedWorkflowInputs",
        runtime_options: "WorkflowTaskRuntimeOptions",
    ):
        actor = self._get_actor(workflow_id, task_id, runtime_options)
        return actor.recover_from_checkpoints.remote(workflow_id, task_id, baked_inputs)

    def save_input_dict(
        self,
        workflow_id: str,
        task_id: TaskID,
        runtime_options: "WorkflowTaskRuntimeOptions",
        input_dict: Dict,
    ):
        actor = self._get_actor(workflow_id, task_id, runtime_options)
        return actor.save_input_dict.remote(workflow_id, input_dict)

    def recycle(self, workflow_id: str, task_id: TaskID) -> None:
        handle = (workflow_id, task_id)
        if handle in self._isolated_tasks:
            return
        actor = self._running_tasks.pop(handle, None)
        if actor is None:
            # could be tasks not scheduled by the actor pool, e.g.,
            # checkpoint loading tasks
            return
        self._actor_tasks_counter[actor] -= 1
        if self._actor_tasks_counter[actor] <= 0:
            self._actor_tasks_counter.pop(actor)
            self._idle_pool.append(actor)

    def join_async_tasks(self, workflow_id: str, task_ids: Optional[List[TaskID]]):
        actors = set(self._idle_pool)
        actors.update(self._running_tasks.values())
        actors.update(self._isolated_tasks.values())
        return [a.join_async_tasks.remote(workflow_id, task_ids) for a in actors]


class ActorController:
    def __init__(self):
        # from ray.experimental.state.api import (
        #     StateApiClient,
        #     StateResource,
        #     ListApiOptions,
        # )
        # from ray.experimental.state.api import list_nodes
        from ray._private.worker import global_worker

        self._pools: Dict[str, ActorPool] = {}
        self._task_tag: Dict[TaskHandle, str] = {}

        n_executors = int(_internal_kv_get("n_executors", namespace="workflow"))
        local_executors_only = int(_internal_kv_get("local_executors_only", namespace="workflow"))

        # client = StateApiClient(global_worker.node.gcs_address)
        # nodes = client.list(
        #     StateResource("nodes"), ListApiOptions(), raise_on_missing_output=False
        # )
        # head_node = global_worker.node.gcs_address.split(":")[0]
        # nodes = list_nodes(f"http://{head_node}:8265")
        nodes = []
        for n in ray.nodes():
            nodes.append({
                "node_id": n["NodeID"],
                "state": "ALIVE" if n["Alive"] else "FAILED",
                "resources_total": n["Resources"],
            })
        if local_executors_only:
            local_node_id = global_worker.core_worker.get_current_node_id().hex()
            nodes = [node for node in nodes if node["node_id"] == local_node_id]
            if not nodes:
                raise RuntimeError("Local node is not found.")

        node_tags = {}
        for node in nodes:
            if node["state"] != "ALIVE":
                continue
            if n_executors > 0:
                n_cpus = n_executors
            else:
                n_cpus = int(node["resources_total"].get("CPU", 1))
            node_id = node["node_id"]
            tag = "default"
            for k in node["resources_total"].keys():
                if k.startswith("tag:"):
                    tag = k
                    break
            node_tags.setdefault(tag, [])
            node_tags[tag].append((node_id, n_cpus))

        for k, v in node_tags.items():
            self._pools[k] = ActorPool(v)

    def _select_pool(
        self,
        workflow_id: str,
        task_id: TaskID,
        runtime_options: "WorkflowTaskRuntimeOptions",
    ) -> ActorPool:
        handle = (workflow_id, task_id)
        tag = "default"
        for k in runtime_options.ray_options.get("resources", {}).keys():
            if k.startswith("tag:") and k in self._pools:
                tag = k
                break
        if tag not in self._pools:
            tag = list(self._pools.keys())[0]
        self._task_tag[handle] = tag
        return self._pools[tag]

    def submit(
        self,
        func: bytes,
        context: "WorkflowTaskContext",
        job_id: str,
        task_id: "TaskID",
        baked_inputs: "_BakedWorkflowInputs",
        runtime_options: "WorkflowTaskRuntimeOptions",
    ):
        pool = self._select_pool(context.workflow_id, task_id, runtime_options)
        return pool.submit(
            func, context, job_id, task_id, baked_inputs, runtime_options
        )

    def recover_from_checkpoints(
        self,
        workflow_id: str,
        task_id: TaskID,
        baked_inputs: "_BakedWorkflowInputs",
        runtime_options: "WorkflowTaskRuntimeOptions",
    ):
        pool = self._select_pool(workflow_id, task_id, runtime_options)
        return pool.recover_from_checkpoints(
            workflow_id, task_id, baked_inputs, runtime_options
        )

    def save_input_dict(self, workflow_id: str, task_id: str, input_dict: Dict):
        # generate a fake runtime option
        runtime_options = WorkflowTaskRuntimeOptions(
            task_type=None,
            catch_exceptions=False,
            retry_exceptions=False,
            max_retries=0,
            checkpoint=False,
            isolation=False,
            wait_until_committed=[],
            ray_options={},
        )
        pool = self._select_pool(workflow_id, task_id, runtime_options)
        return pool.save_input_dict(workflow_id, task_id, runtime_options, input_dict)

    def recycle(self, workflow_id, task_id: TaskID) -> None:
        handle = (workflow_id, task_id)
        tag = self._task_tag.pop(handle)
        self._pools[tag].recycle(workflow_id, task_id)

    def join_async_tasks(
        self, workflow_id: str, task_ids: Optional[List[TaskID]] = None
    ) -> List:
        refs = []
        for v in self._pools.values():
            refs.extend(v.join_async_tasks(workflow_id, task_ids))
        return refs


async def _workflow_task_executor(
    func: bytes,
    context: "WorkflowTaskContext",
    task_id: "TaskID",
    baked_inputs: "_BakedWorkflowInputs",
    runtime_options: "WorkflowTaskRuntimeOptions",
    async_task_submitter: Callable,
) -> Tuple[Any, Any]:
    """Executor function for workflow task.

    Args:
        task_id: ID of the task.
        func: The workflow task function.
        baked_inputs: The processed inputs for the task.
        context: Workflow task context. Used to access correct storage etc.
        runtime_options: Parameters for workflow task execution.

    Returns:
        Workflow task output.
    """
    verbose_log = not context.is_service_instance
    if verbose_log:
        pre_time = time.time()
    func: Callable = cloudpickle.loads(func)
    with workflow_context.workflow_task_context(context):
        store = workflow_storage.get_workflow_storage()
        # Part 1: resolve inputs
        args, kwargs = baked_inputs.resolve(store)

        # Part 2: execute the task
        try:
            if verbose_log:
                store.save_task_prerun_metadata(
                    task_id, {"pre_time": pre_time, "start_time": time.time()}
                )
            with workflow_context.workflow_execution():
                if verbose_log:
                    logger.info(f"{get_task_status_info(WorkflowStatus.RUNNING)}")
                output = func(*args, **kwargs)
                if inspect.iscoroutine(output):
                    output = await output
                del args, kwargs
            if verbose_log:
                end_time = time.time()
                store.save_task_postrun_metadata(task_id, {"end_time": end_time})
        except Exception as e:
            # Always checkpoint the exception.
            # TODO(suquark): I moved the exception checkpointing to the workflow
            #   executor. It seems reasonable for me if the exception is feasible
            #   for serialization.
            # store.save_task_output(task_id, None, None, None, exception=e)
            raise e

        if isinstance(output, DAGNode):
            output = workflow_state_from_dag(
                output,
                None,
                context.workflow_id,
                ref_to_task_mapping=baked_inputs.ref_to_task_mapping,
            )
            execution_metadata = WorkflowExecutionMetadata(is_output_workflow=True)
        else:
            from ray.cloudpickle import dumps, loads

            workflow_refs = []
            object_refs = []
            actors = []
            buffers = []
            input_placeholders = []
            with serialization_context.workflow_args_serialization_context(
                workflow_refs, object_refs, actors, input_placeholders
            ):
                # Here we skip inband serialization of the buffers for efficiency
                output_serialized = dumps(
                    output, protocol=5, buffer_callback=buffers.append
                )
            assert not input_placeholders
            if workflow_refs:
                # TOOD(suquark): support workflow refs
                raise NotImplementedError("workflow refs in output not supported")

            # associates output object refs with input tasks
            ref_to_task_mapping = {}
            for ref in object_refs:
                t = baked_inputs.ref_to_task_mapping.get(ref)
                if t is not None:
                    ref_to_task_mapping[ref] = t

            with serialization_context.workflow_args_keeping_context():
                output = loads(output_serialized, buffers=buffers)
            execution_metadata = WorkflowExecutionMetadata(
                object_refs=object_refs,
                actors=actors,
                ref_to_task_mapping=ref_to_task_mapping,
            )
            if runtime_options.catch_exceptions:
                output = (output, None)

        # Part 3: save outputs
        checkpointed = False
        if isinstance(output, WorkflowExecutionState):
            store.save_workflow_execution_state(task_id, output)
            checkpointed = True
        else:
            checkpoint_mode = CheckpointMode(runtime_options.checkpoint)
            if buffers and checkpoint_mode != CheckpointMode.SKIP:
                # TODO(suquark): Support checkpointing buffers separately
                #  for efficiency. Here we just fuse it with other data.
                output_serialized = dumps(
                    serialization_context.BufferEncoder(output_serialized, buffers)
                )
            if checkpoint_mode == CheckpointMode.SYNC:
                store.save_task_output(
                    task_id,
                    output_serialized,
                    object_refs,
                    actors,
                    exception=None,
                    refs_hex_in_workflow_refs=context.refs_hex_in_workflow_refs,
                )
                checkpointed = True
            elif checkpoint_mode == CheckpointMode.ASYNC:
                async_task_submitter(
                    context.workflow_id,
                    task_id,
                    store.save_task_output,
                    task_id,
                    output_serialized,
                    object_refs,
                    actors,
                    exception=None,
                    refs_hex_in_workflow_refs=context.refs_hex_in_workflow_refs,
                )
        if verbose_log:
            if checkpointed:
                store.save_task_postrun_metadata(
                    task_id, {"end_time": end_time, "checkpoint_time": time.time()}
                )
        return execution_metadata, output


@dataclass
class _BakedWorkflowInputs:
    """This class stores pre-processed inputs for workflow task execution.
    Especially, all input workflows to the workflow task will be scheduled,
    and their outputs (ObjectRefs) replace the original workflows."""

    args: "ObjectRef"
    workflow_refs: "List[WorkflowRef]"
    object_refs: List[ray.ObjectRef]
    actors: List[ray.actor.ActorHandle]
    input_dict: Dict
    ref_to_task_mapping: Dict[ray.ObjectRef, TaskID] = field(default_factory=dict)

    def resolve(self, store: workflow_storage.WorkflowStorage) -> Tuple[List, Dict]:
        """
        This function resolves the inputs for the code inside
        a workflow task (works on the callee side). For outputs from other
        workflows, we resolve them into object instances inplace.

        For each ObjectRef argument, the function returns both the ObjectRef
        and the object instance. If the ObjectRef is a chain of nested
        ObjectRefs, then we resolve it recursively until we get the
        object instance, and we return the *direct* ObjectRef of the
        instance. This function does not resolve ObjectRef
        inside another object (e.g. list of ObjectRefs) to give users some
        flexibility.

        Returns:
            Instances of arguments.
        """
        workflow_ref_mapping = []
        for r in self.workflow_refs:
            assert r.ref is not None
            workflow_ref_mapping.append(r.ref)

        for wf, m in zip(self.workflow_refs, workflow_ref_mapping):
            self.ref_to_task_mapping[m] = wf.task_id

        with serialization_context.workflow_args_resolving_context(
            workflow_ref_mapping, self.object_refs, self.actors, self.input_dict
        ):
            # reconstruct input arguments under correct serialization context
            if self.args is not None:
                flattened_args: List[Any] = ray.get(self.args)
            else:
                task_id = workflow_context.get_current_task_id()
                flattened_args: List[Any] = store._get(store._key_task_args(task_id))
            del self.args, self.workflow_refs, self.object_refs, self.actors

        # dereference arguments like Ray remote functions
        flattened_args = [
            ray.get(a) if isinstance(a, ObjectRef) else a for a in flattened_args
        ]
        return signature.recover_args(flattened_args)
