from typing import Optional
from collections import deque

from ray.workflow import serialization
from ray.workflow.common import TaskID, WorkflowRef
from ray.workflow.exceptions import WorkflowTaskNotRecoverableError
from ray.workflow.workflow_storage import TaskInspectResult, WorkflowStorage
from ray.workflow.workflow_state import WorkflowExecutionState, Task, Entry


def _reconstruct_checkpointed_task(
    result: TaskInspectResult,
    state: WorkflowExecutionState,
    task_id: TaskID,
    reader: WorkflowStorage,
):
    target = state.continuation_root.get(task_id, task_id)
    extra_deps = set()
    _ref_entries = []
    _actor_entries = []
    _checkpointed_refs = {}
    for metadata in result.output_refs["object_refs"]:
        entry = Entry.from_dict(metadata)
        _ref_entries.append(entry)
        if entry.creator_task_id != task_id:
            extra_deps.add(entry.creator_task_id)
        else:
            # The object ref was created by the task itself. We have to re-execute
            # the task to get the object ref again.
            if not reader.object_ref_exists(metadata["hex_id"]):
                return None
            _checkpointed_refs[entry] = metadata["hex_id"]

    for metadata in result.output_refs["actors"]:
        entry = Entry.from_dict(metadata)
        _actor_entries.append(entry)
        if entry.creator_task_id != task_id:
            extra_deps.add(entry.creator_task_id)
        else:
            # The actor was created by the task itself. We have to re-execute
            # the task to get the actor again.
            return None

    # TODO(suquark): assign it to task_id or target?
    if _ref_entries:
        t = state.object_ref_tracker
        t.task_output_entries.setdefault(task_id, []).extend(_ref_entries)
        state.object_ref_tracker.set_task_outputs_checkpointed(
            task_id, _checkpointed_refs
        )
    if _actor_entries:
        t = state.actor_tracker
        t.task_output_entries.setdefault(task_id, []).extend(_actor_entries)

    state.checkpoint_map[target] = WorkflowRef(task_id)
    state.add_dependencies(target, list(extra_deps))
    return extra_deps


def workflow_state_from_storage(
    workflow_id: str, task_id: Optional[TaskID]
) -> WorkflowExecutionState:
    """Try to construct a workflow (task) that recovers the workflow task.
    If the workflow task already has an output checkpointing file, we return
    the workflow task id instead.

    Args:
        workflow_id: The ID of the workflow.
        task_id: The ID of the output task. If None, it will be the entrypoint of
            the workflow.

    Returns:
        A workflow that recovers the task, or the output of the task
            if it has been checkpointed.
    """
    reader = WorkflowStorage(workflow_id)
    if task_id is None:
        task_id = reader.get_entrypoint_task_id()

    # Construct the workflow execution state.
    state = WorkflowExecutionState(output_task_id=task_id)
    state.output_task_id = task_id

    visited_tasks = set()
    dag_visit_queue = deque([task_id])
    with serialization.objectref_cache():
        while dag_visit_queue:
            task_id: TaskID = dag_visit_queue.popleft()
            if task_id in visited_tasks:
                continue
            visited_tasks.add(task_id)
            r = reader.inspect_task(task_id)
            if not r.is_recoverable():
                raise WorkflowTaskNotRecoverableError(task_id)
            # TODO(suquark): although not necessary, but for completeness,
            #  we may also load name and metadata.
            state.tasks[task_id] = Task(
                name="",
                options=r.task_options,
                user_metadata={},
                func_body=b"",
            )
            if r.output_object_valid:
                extra_deps = _reconstruct_checkpointed_task(r, state, task_id, reader)
                if extra_deps is not None:
                    dag_visit_queue.extend(extra_deps - visited_tasks)
                    continue
                assert r.output_task_id is None
            if isinstance(r.output_task_id, str):
                # no input dependencies here because the task has already
                # returned a continuation
                state.upstream_dependencies[task_id] = []
                state.upstream_data_dependencies[task_id] = []
                state.append_continuation(task_id, r.output_task_id)
                dag_visit_queue.append(r.output_task_id)
                continue
            # transfer task info to state
            state.add_dependencies(task_id, r.upstream_dependencies)
            state.add_data_dependencies(task_id, r.upstream_data_dependencies)
            state.task_input_args[task_id] = reader.load_task_args(task_id)
            state.tasks[task_id].func_body = reader.load_task_func_body(task_id)
            # TODO(suquark): provide dag input when resuming
            state.task_dag_input_keys[task_id] = set()
            dag_visit_queue.extend(r.upstream_dependencies)

    return state
