from typing import Dict, List, Iterator, Optional, Tuple, TYPE_CHECKING

import asyncio
import logging
import time
from collections import defaultdict

import ray
from ray.exceptions import RayTaskError, RayError, ObjectLostError, RayActorError

from exoflow.common import (
    WorkflowRef,
    WorkflowExecutionMetadata,
    WorkflowStatus,
    TaskID,
    CheckpointMode,
    SERVICE_SEP,
)
from exoflow.exceptions import WorkflowCancellationError, WorkflowExecutionError
from exoflow.task_executor import _BakedWorkflowInputs, ActorController
from exoflow.workflow_state import (
    WorkflowExecutionState,
    TaskExecutionMetadata,
    Task,
)

if TYPE_CHECKING:
    from exoflow.workflow_context import WorkflowTaskContext
    from exoflow.workflow_storage import WorkflowStorage

logger = logging.getLogger(__name__)


class WorkflowExecutor:
    def __init__(
        self,
        state: WorkflowExecutionState,
        actor_controller: ActorController,
    ):
        """The core logic of executing a workflow.

        This class is responsible for:

        - Dependency resolving.
        - Task scheduling.
        - Reference counting.
        - Garbage collection.
        - Continuation handling and scheduling.
        - Error handling.
        - Responding callbacks.

        It borrows some design of event loop in asyncio,
        e.g., 'run_until_complete'.

        Args:
            state: The initial state of the workflow.
        """
        self._state = state
        self._completion_queue = asyncio.Queue()
        self._task_done_callbacks: Dict[TaskID, List[asyncio.Future]] = defaultdict(
            list
        )
        self._async_workflow_task_checkpointing: Dict[TaskID, ray.ObjectRef] = {}
        self._async_ray_task_checkpointing: Dict[TaskID, ray.ObjectRef] = {}
        self._actor_controller = actor_controller
        self._workflow_id = None

    def is_running(self) -> bool:
        """The state is running, if there are tasks to be run or running tasks."""
        return bool(self._state.frontier_to_run or self._state.running_frontier)

    def get_state(self) -> WorkflowExecutionState:
        return self._state

    @property
    def is_service_instance(self) -> bool:
        # TODO(suquark): a better way to determine if this is a service instance.
        if self._workflow_id is not None:
            return SERVICE_SEP in self._workflow_id
        return bool(self._state.input_dict)

    def _log(self, msg: str):
        if not self.is_service_instance:
            logger.info(msg)
        else:
            logger.debug(msg)

    @property
    def output_task_id(self) -> TaskID:
        return self._state.output_task_id

    async def run_until_complete(
        self,
        job_id: str,
        context: "WorkflowTaskContext",
        wf_store: "WorkflowStorage",
        persist_input: bool = False,
    ):
        """Drive the state util it completes.

        Args:
            job_id: The Ray JobID for logging properly.
            context: The context of workflow execution.
            wf_store: The store for the workflow.

        # TODO(suquark): move job_id inside context
        """
        workflow_id = context.workflow_id
        # TODO(suquark): a better way to manage workflow_id
        self._workflow_id = workflow_id
        if not self.is_service_instance:
            wf_store.update_workflow_status(WorkflowStatus.RUNNING)
        if persist_input and self._state.input_dict is not None:
            # TODO(suquark): a more unique task_id
            save_input_dict_task_id = "<save_input_dict>"
            await self._actor_controller.save_input_dict(
                workflow_id, save_input_dict_task_id, self._state.input_dict
            )
            self._actor_controller.recycle(workflow_id, save_input_dict_task_id)
        self._log(f"Workflow job [id={workflow_id}] started.")
        self._state.construct_scheduling_plan(self._state.output_task_id)
        self._state.init_context(context)

        while self.is_running():
            # ------------ poll queued tasks ------------
            queued_tasks = self._poll_queued_tasks()

            # --------------- submit task ---------------
            for task_id in queued_tasks:
                # '_submit_ray_task' submit a Ray task based on the workflow task.
                await self._submit_ray_task(workflow_id, task_id, job_id=job_id)
                # '_post_process_submit_task' updates the state related to task
                # submission.
                self._post_process_submit_task(task_id, wf_store)

            # ------------ poll ready tasks ------------
            ready_futures = await self._poll_ready_tasks()

            # ----------- handle ready tasks -----------
            await asyncio.gather(
                *[
                    self._handle_ready_task(
                        fut, workflow_id=workflow_id, wf_store=wf_store
                    )
                    for fut in ready_futures
                ]
            )

            # prevent leaking ObjectRefs into the next iteration
            del ready_futures

        if not self.is_service_instance:
            self._log(f"Workflow '{workflow_id}' joining async tasks...")
            await asyncio.gather(*self._actor_controller.join_async_tasks(workflow_id))
            wf_store.update_workflow_status(WorkflowStatus.SUCCESSFUL)
        self._log(f"Workflow '{workflow_id}' completes successfully.")

        # set errors for pending workflow outputs
        for task_id, futures in self._task_done_callbacks.items():
            err = ValueError(
                f"The workflow haven't yet produced output of task '{task_id}' "
                f"after workflow execution completes."
            )
            for fut in futures:
                if not fut.done():
                    fut.set_exception(err)

        self._actor_controller = None

    def cancel(self) -> None:
        """Cancel the running workflow."""
        for fut, workflow_ref in self._state.running_frontier.items():
            fut.cancel()
            try:
                ray.cancel(workflow_ref.ref, force=True)
            except Exception:
                pass

    def _poll_queued_tasks(self) -> List[TaskID]:
        tasks = []
        while True:
            task_id = self._state.pop_frontier_to_run()
            if task_id is None:
                break
            tasks.append(task_id)
        return tasks

    async def _submit_ray_task(
        self, workflow_id: str, task_id: TaskID, job_id: str
    ) -> None:
        """Submit a workflow task as a Ray task."""
        state = self._state
        assert task_id not in state.task_output_placeholder_ref

        if state._task_done_and_checkpoint_exists(task_id):
            # This comes from recovery to reconstruct its output.
            baked_inputs = _BakedWorkflowInputs(
                args=None,
                workflow_refs=[],
                object_refs=state.object_ref_tracker.get_task_outputs(task_id),
                actors=state.actor_tracker.get_task_outputs(task_id),
                input_dict={},
            )
            state.object_ref_tracker.task_output_entries.pop(task_id, [])
            state.actor_tracker.task_output_entries.pop(task_id, [])
            metadata_ref, output_ref = self._actor_controller.recover_from_checkpoints(
                workflow_id,
                task_id,
                baked_inputs,
                state.tasks[task_id].options,
            )
        else:
            state.object_ref_tracker.task_output_entries.pop(task_id, [])
            state.actor_tracker.task_output_entries.pop(task_id, [])
            baked_inputs = _BakedWorkflowInputs(
                args=state.task_input_args.get(task_id),  # does not exist -> recovery
                workflow_refs=[
                    state.get_input(d)
                    for d in state.upstream_data_dependencies[task_id]
                ],
                object_refs=state.object_ref_tracker.get_task_inputs(task_id),
                actors=state.actor_tracker.get_task_inputs(task_id),
                input_dict={
                    k: state.input_dict[k] for k in state.task_dag_input_keys[task_id]
                },
            )
            task = state.tasks[task_id]
            if task.options.wait_until_committed:
                # TODO(suquark): this is likely not an efficient implementation
                #  with a greater number of actors.
                await asyncio.gather(
                    *self._actor_controller.join_async_tasks(
                        workflow_id, task.options.wait_until_committed
                    )
                )
            state.task_context[task_id].is_service_instance = self.is_service_instance
            metadata_ref, output_ref = self._actor_controller.submit(
                task.func_body,
                state.task_context[task_id],
                job_id,
                task_id,
                baked_inputs,
                task.options,
            )

        # The input workflow is not a reference to an executed workflow.
        future = asyncio.wrap_future(metadata_ref.future())
        future.add_done_callback(self._completion_queue.put_nowait)

        state.on_schedule_tasks.remove(task_id)
        state.insert_running_frontier(future, WorkflowRef(task_id, ref=output_ref))
        state.task_execution_metadata[task_id] = TaskExecutionMetadata(
            submit_time=time.time()
        )

    def _post_process_submit_task(
        self, task_id: TaskID, store: "WorkflowStorage"
    ) -> None:
        """Update dependencies and reference count etc. after task submission."""
        state = self._state
        if task_id in state.continuation_root:
            if state.tasks[task_id].options.checkpoint:
                store.update_continuation_output_link(
                    state.continuation_root[task_id], task_id
                )
        # update reference counting
        state.remove_reference(task_id)

    async def _poll_ready_tasks(self) -> List[asyncio.Future]:
        cq = self._completion_queue
        ready_futures = []
        rf = await cq.get()
        ready_futures.append(rf)
        # get all remaining futures in the queue
        while not cq.empty():
            ready_futures.append(cq.get_nowait())
        return ready_futures

    def _iter_callstack(self, task_id: TaskID) -> Iterator[Tuple[TaskID, Task]]:
        state = self._state
        while task_id in state.task_context and task_id in state.tasks:
            yield task_id, state.tasks[task_id]
            task_id = state.task_context[task_id].creator_task_id

    async def _handle_ready_task(
        self, fut: asyncio.Future, workflow_id: str, wf_store: "WorkflowStorage"
    ) -> None:
        """Handle ready task, especially about its exception."""
        state = self._state
        output_ref = state.pop_running_frontier(fut)
        task_id = output_ref.task_id
        self._actor_controller.recycle(workflow_id, task_id)
        try:
            metadata: WorkflowExecutionMetadata = fut.result()
            state.task_execution_metadata[task_id].finish_time = time.time()
            self._log(
                f"Task status [{WorkflowStatus.SUCCESSFUL}]\t"
                f"[{workflow_id}@{task_id}]"
            )
            await self._post_process_ready_task(
                task_id, metadata, output_ref, wf_store=wf_store
            )
        except asyncio.CancelledError:
            # NOTE: We must update the workflow status before broadcasting
            # the exception. Otherwise, the workflow status would still be
            # 'RUNNING' if check the status immediately after cancellation.
            wf_store.update_workflow_status(WorkflowStatus.CANCELED)
            logger.warning(f"Workflow '{workflow_id}' is cancelled.")
            # broadcasting cancellation to all outputs
            err = WorkflowCancellationError(workflow_id)
            self._broadcast_exception(err)
            raise err from None
        except Exception as e:
            if isinstance(e, RayTaskError):
                # TODO(suquark): something like "retry_ray_tasks"
                # TODO(suquark): max_retries for ObjectLost/ActorError?
                cause = e.cause
                if isinstance(cause, (ObjectLostError, RayActorError)):
                    if isinstance(cause, ObjectLostError):
                        hex_id = cause.object_ref_hex
                        reason = f"lost of object ref '{hex_id}'. "
                        entry = state.object_ref_tracker.entry_creator.get(hex_id)
                    else:
                        # TODO(suquark): https://github.com/ray-project/ray/issues/28018
                        if hasattr(cause, "actor_id_hex"):
                            hex_id = cause.actor_id_hex
                            reason = f"lost of actor '{hex_id}'. "
                        else:
                            # TODO(suquark): get actor_id for actor crash
                            hex_id = None
                            for line in cause.error_msg.split("\n"):
                                line = line.strip()
                                if line.startswith("actor_id: "):
                                    hex_id = line[len("actor_id: ") :]
                            assert hex_id is not None
                            reason = f"crash of actor '{hex_id}'. "
                        entry = state.actor_tracker.entry_creator.get(hex_id)
                    if entry is not None:
                        reason += (
                            f"The object was created by '{entry.creator_task_id}' "
                            f"(index={entry.index})"
                        )
                    else:
                        reason += "The creator of the object is unknown."
                    logger.error(
                        f"Task status [{WorkflowStatus.FAILED}] due to {reason}.\t"
                        f"[{workflow_id}@{task_id}]"
                    )
                    if hex_id in state.object_ref_tracker.outdated_refs:
                        # This error is based on an outdated ref. We should not
                        # invalidate the current ref creator task for it.
                        state.construct_scheduling_plan(task_id)
                        state.construct_scheduling_plan(entry.creator_task_id)
                        return
                    elif entry is not None:
                        creator_task_id = entry.creator_task_id
                        # We choose to re-execute the creator task, because
                        # 1. We have to do so to create the lost object again.
                        # 2. It is likely that all other refs returned by the
                        #    creator task are lost (e.g., owner failure).
                        state.task_output_placeholder_ref.pop(creator_task_id, None)
                        state.object_ref_tracker.invalidate_task(creator_task_id)
                        state.actor_tracker.invalidate_task(creator_task_id)

                        state.construct_scheduling_plan(task_id)
                        state.construct_scheduling_plan(creator_task_id)

                        # Reconstruction of the ref creator task affects the downstream
                        # tasks. We address this by enforcing new dependencies between
                        # affected downstream tasks and the creator task.
                        affected_tasks = {
                            *state.object_ref_tracker.get_downstream_references(
                                creator_task_id
                            ),
                            *state.actor_tracker.get_downstream_references(
                                creator_task_id
                            ),
                        }
                        # Only tasks that will be scheduled later are affected.
                        # That's why we construct the scheduling plan first.
                        affected_tasks &= state.on_schedule_tasks

                        for f_task in affected_tasks:
                            state.pending_input_set[f_task].add(creator_task_id)
                            state.add_reference(f_task, creator_task_id)

                        # Withdraw affected tasks from the execution frontier.
                        state.frontier_to_run_set -= affected_tasks
                        state.frontier_to_run.clear()
                        state.frontier_to_run.extend(state.frontier_to_run_set)
                        return

                reason = "an exception raised by the task"
            elif isinstance(e, RayError):
                reason = "a system error"
            else:
                reason = "an unknown error"
                raise e
            logger.error(
                f"Task status [{WorkflowStatus.FAILED}] due to {reason}.\t"
                f"[{workflow_id}@{task_id}]"
            )

            is_application_error = isinstance(e, RayTaskError)
            options = state.tasks[task_id].options

            # ---------------------- retry the task ----------------------
            if not is_application_error or options.retry_exceptions:
                if state.task_retries[task_id] < options.max_retries:
                    state.task_retries[task_id] += 1
                    logger.info(
                        f"Retry [{workflow_id}@{task_id}] "
                        f"({state.task_retries[task_id]}/{options.max_retries})"
                    )
                    state.construct_scheduling_plan(task_id)
                    return
            # ----------- retry used up, handle the task error -----------
            # Always checkpoint the exception.
            wf_store.save_task_output(
                task_id,
                None,
                None,
                None,
                exception=e.cause,
                refs_hex_in_workflow_refs=None,
            )
            exception_catcher = None
            if is_application_error:
                for t, task in self._iter_callstack(task_id):
                    if task.options.catch_exceptions:
                        exception_catcher = t
                        break
            if exception_catcher is not None:
                logger.info(
                    f"Exception raised by '{workflow_id}@{task_id}' is caught by "
                    f"'{workflow_id}@{exception_catcher}'"
                )
                # assign output to exception catching task;
                # compose output with caught exception
                # TODO(suquark): create a checkpoint for the error handler?
                await self._post_process_ready_task(
                    exception_catcher,
                    metadata=WorkflowExecutionMetadata(),
                    output_ref=WorkflowRef(task_id, ray.put((None, e))),
                    wf_store=wf_store,
                )
                # TODO(suquark): cancel other running tasks?
                return

            # ------------------- raise the task error -------------------
            # NOTE: We must update the workflow status before broadcasting
            # the exception. Otherwise, the workflow status would still be
            # 'RUNNING' if check the status immediately after the exception.
            wf_store.update_workflow_status(WorkflowStatus.FAILED)
            logger.error(f"Workflow '{workflow_id}' failed due to {e}")
            err = WorkflowExecutionError(workflow_id)
            err.__cause__ = e  # chain exceptions
            self._broadcast_exception(err)
            raise err

    async def _post_process_ready_task(
        self,
        task_id: TaskID,
        metadata: WorkflowExecutionMetadata,
        output_ref: WorkflowRef,
        wf_store: "WorkflowStorage",
    ) -> None:
        state = self._state
        o_trk = state.object_ref_tracker
        a_trk = state.actor_tracker
        state.task_retries.pop(task_id, None)
        if metadata.is_output_workflow:  # The task returns a continuation
            sub_workflow_state: WorkflowExecutionState = await output_ref.ref

            # init the context just for "sub_workflow_state"
            sub_workflow_state.init_context(state.task_context[task_id])

            # merge states
            state.merge_state(sub_workflow_state)
            sub_task_ids = {
                *o_trk.task_input_entries.keys(),
                *a_trk.task_input_entries.keys(),
            }
            o_trk.merge_entries_from_subworkflow(
                task_id, sub_workflow_state.object_ref_tracker
            )
            a_trk.merge_entries_from_subworkflow(
                task_id, sub_workflow_state.actor_tracker
            )
            # save deps
            wf_store.save_task_args_ref_metadata(
                task_id,
                object_refs=o_trk.get_task_arguments_metadata(task_id),
                actors=a_trk.get_task_arguments_metadata(task_id),
            )
            for sub_task_id in sub_task_ids:
                wf_store.save_task_args_ref_metadata(
                    sub_task_id,
                    object_refs=o_trk.get_task_inputs_metadata(sub_task_id),
                    actors=a_trk.get_task_inputs_metadata(sub_task_id),
                )

            # build up runtime dependency
            continuation_task_id = sub_workflow_state.output_task_id
            state.append_continuation(task_id, continuation_task_id)
            # Migrate callbacks - all continuation callbacks are moved
            # under the root of continuation, so when the continuation
            # completes, all callbacks in the continuation can be triggered.
            if continuation_task_id in self._task_done_callbacks:
                self._task_done_callbacks[
                    state.continuation_root[continuation_task_id]
                ].extend(self._task_done_callbacks.pop(continuation_task_id))
            state.construct_scheduling_plan(sub_workflow_state.output_task_id)
        else:  # The task returns a normal object
            target_task_id = state.continuation_root.get(task_id, task_id)

            # TODO(suquark): verify the logic of continuation
            o_trk.set_task_outputs(
                target_task_id,
                metadata.object_refs,
                metadata.ref_to_task_mapping,
            )
            a_trk.set_task_outputs(target_task_id, metadata.actors)
            state.task_output_placeholder_ref[target_task_id] = output_ref
            if not metadata.is_checkpoint_loader:
                checkpoint_mode = CheckpointMode(
                    state.tasks[task_id].options.checkpoint
                )
                if checkpoint_mode == CheckpointMode.SYNC:
                    state.checkpoint_map[target_task_id] = WorkflowRef(task_id)
                if checkpoint_mode != CheckpointMode.SKIP:
                    # This is an optimization, as the metadata should already
                    # been save with "wf_store.save_task_output", if both fields
                    # are empty.
                    if metadata.object_refs or metadata.actors:
                        # store with "task_id", because it is where we are going to
                        # load the task output.
                        wf_store.save_task_output_ref_metadata(
                            task_id,
                            object_refs=o_trk.get_task_outputs_metadata(target_task_id),
                            actors=a_trk.get_task_outputs_metadata(target_task_id),
                        )
                    # TODO(suquark): we should mark outputs checkpointed only when
                    #  async checkpointing finishes
                    o_trk.set_task_outputs_checkpointed(target_task_id)
            state.done_tasks.add(target_task_id)
            # TODO(suquark): cleanup callbacks when a result is set?
            if target_task_id in self._task_done_callbacks:
                for callback in self._task_done_callbacks[target_task_id]:
                    callback.set_result(output_ref)

            # schedule pending downstream tasks
            pending_downstream_tasks = state.reference_set[target_task_id]
            o_trk.propagate_references(target_task_id, pending_downstream_tasks)
            a_trk.propagate_references(target_task_id, pending_downstream_tasks)
            for m in pending_downstream_tasks:
                # we ensure that each reference corresponds to a pending input
                state.pending_input_set[m].remove(target_task_id)
                if not state.pending_input_set[m]:
                    state.append_frontier_to_run(m)

    def _broadcast_exception(self, err: Exception):
        for _, futures in self._task_done_callbacks.items():
            for fut in futures:
                if not fut.done():
                    fut.set_exception(err)

    def get_task_output_async(self, task_id: Optional[TaskID]) -> asyncio.Future:
        """Get the output of a task asynchronously.

        Args:
            task_id: The ID of task the callback associates with.

        Returns:
            A callback in the form of a future that associates with the task.
        """
        state = self._state
        if self._task_done_callbacks[task_id]:
            return self._task_done_callbacks[task_id][0]

        fut = asyncio.Future()
        task_id = state.continuation_root.get(task_id, task_id)
        try:
            output = state.get_input(task_id)
        except KeyError:
            output = state.checkpoint_map.get(task_id)
        if output is not None:
            fut.set_result(output)
        elif task_id in state.done_tasks:
            fut.set_exception(
                ValueError(
                    f"Task '{task_id}' is done but neither in memory or in storage "
                    "could we find its output. It could because its in memory "
                    "output has been garbage collected and the task did not"
                    "checkpoint its output."
                )
            )
        else:
            self._task_done_callbacks[task_id].append(fut)
        return fut
