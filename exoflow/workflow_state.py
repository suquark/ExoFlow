import asyncio
import enum

from collections import deque, defaultdict
import dataclasses
from dataclasses import field
import logging
from typing import List, Dict, Optional, Set, Deque, Union

import ray
from ray import ObjectRef
from ray.actor import ActorHandle
from exoflow.common import (
    TaskID,
    WorkflowRef,
    WorkflowTaskRuntimeOptions,
)
from exoflow.workflow_context import WorkflowTaskContext
from exoflow.serialization_context import _TaskOutputEncoder, _wrap_task_output

logger = logging.getLogger(__name__)

RefType = Union[ObjectRef, ActorHandle]


@dataclasses.dataclass
class TaskExecutionMetadata:
    submit_time: Optional[float] = None
    finish_time: Optional[float] = None
    output_size: Optional[int] = None

    @property
    def duration(self):
        return self.finish_time - self.submit_time


@dataclasses.dataclass
class Task:
    """Data class for a workflow task."""

    name: str
    options: WorkflowTaskRuntimeOptions
    user_metadata: Dict
    func_body: bytes

    def to_dict(self) -> Dict:
        return {
            "name": self.name,
            "task_options": self.options.to_dict(),
            "user_metadata": self.user_metadata,
        }


@enum.unique
class EntryType(int, enum.Enum):
    # This is a ref to a workflow task output.
    WORKFLOW_REF = 0
    # This is a ref to an ephemeral object in a workflow task output.
    OUTPUT_REF = 1
    # This is a ref to an ephemeral object in a workflow task input.
    INPUT_REF = 2


@dataclasses.dataclass(eq=True, frozen=True)
class Entry:
    # the task ID that creates this ObjectRef
    creator_task_id: TaskID
    # the index of the actor in the output refs of a task
    index: int
    # type of the entry
    entry_type: EntryType

    def to_dict(self) -> Dict:
        return {
            "creator_task_id": self.creator_task_id,
            "index": self.index,
            "entry_type": self.entry_type.value,
        }

    @classmethod
    def from_dict(cls, metadata):
        return cls(
            metadata["creator_task_id"],
            metadata["index"],
            EntryType(metadata["entry_type"]),
        )


def _get_ref_hex(ref: RefType) -> str:
    if isinstance(ref, ObjectRef):
        return ref.hex()
    elif isinstance(ref, ActorHandle):
        return ref._actor_id.hex()


@dataclasses.dataclass
class EphemeralObjectTracker:
    """Tracker of ephemeral objects in workflow."""

    # map from object hex ID to its creator
    entry_creator: Dict[str, Entry] = field(default_factory=dict)
    entry_ref_map: Dict[Entry, RefType] = field(default_factory=dict)
    entry_ref_counter: Dict[Entry, Set] = field(default_factory=dict)
    # The tasks whose upstream workflows contain these entries.
    # This is used during recovery: if the entry creator failed, it needs
    # to be reconstructed and we need to set dependencies for the tasks
    # who are going to use the entry.
    entry_reference_set: Dict[Entry, Set] = field(default_factory=dict)

    # The objects in the arguments for the task.
    task_input_entries: Dict[TaskID, List[Entry]] = field(default_factory=dict)
    task_output_entries: Dict[TaskID, List[Entry]] = field(default_factory=dict)
    # The entries for the inputs of a DAG, returned by a task.
    task_argument_entries: Dict[TaskID, List[Entry]] = field(default_factory=dict)
    # The map from entry to its checkpoint (hex ref ID)
    entry_checkpoint_map: Dict[Entry, str] = field(default_factory=dict)
    # The hex of refs that has been outdated due to recovery.
    outdated_refs: Set[str] = field(default_factory=set)

    def _add_ref(
        self,
        task_id: TaskID,
        index: int,
        entry_type: EntryType,
        ref: RefType,
        ref_to_task_mapping: Dict[RefType, TaskID],
    ) -> Entry:
        hex_id = _get_ref_hex(ref)
        entry = self.entry_creator.get(hex_id)
        if entry is None:
            output_task_id = ref_to_task_mapping.get(ref)
            if output_task_id is None:
                entry = Entry(task_id, index=index, entry_type=entry_type)
            else:
                # The entry points to the output of a workflow task instead.
                entry = Entry(output_task_id, 0, EntryType.WORKFLOW_REF)
            self.entry_creator[hex_id] = entry
        # the ref could be removed before due to reference count goes down to 0
        self.entry_ref_map[entry] = ref  # always set the latest ref
        self.entry_ref_counter.setdefault(entry, set())
        self.entry_ref_counter[entry].add(task_id)
        return entry

    def _remove_ref(self, task_id: TaskID, entry) -> None:
        if task_id not in self.entry_ref_counter[entry]:
            # TODO(suquark): this is a temporary fix for dynamic workflow.
            #  The issue is somehow the inputs of nested workflow task does
            #  not really takes the reference count of an upstream entry.
            return
        self.entry_ref_counter[entry].remove(task_id)
        if not self.entry_ref_counter[entry]:
            self.entry_ref_map.pop(entry)
            self.entry_ref_counter.pop(entry)

    def _entries_to_metadata(self, entries) -> List[Dict]:
        results = []
        for entry in entries:
            results.append(
                {
                    **entry.to_dict(),
                    "hex_id": _get_ref_hex(self.entry_ref_map[entry]),
                }
            )
        return results

    def get_task_inputs(self, task_id: TaskID) -> List[RefType]:
        results = []
        for entry in self.task_input_entries.get(task_id, []):
            results.append(self.entry_ref_map[entry])
        return results

    def get_task_inputs_metadata(self, task_id: TaskID) -> List[Dict]:
        return self._entries_to_metadata(self.task_input_entries.get(task_id, []))

    def get_task_arguments_metadata(self, task_id: TaskID) -> List[Dict]:
        return self._entries_to_metadata(self.task_argument_entries.get(task_id, []))

    def set_task_inputs(
        self,
        task_id: TaskID,
        creator_task_id: TaskID,
        inputs: List[RefType],
        ref_to_task_mapping: Optional[Dict[RefType, TaskID]] = None,
    ):
        """This method also sets the outputs of the creator task."""
        if not inputs:
            return  # do not set empty inputs to keep the state clean
        if ref_to_task_mapping is None:
            ref_to_task_mapping = {}
        self.task_argument_entries.setdefault(creator_task_id, [])
        self.task_input_entries.setdefault(task_id, [])
        for ref in inputs:
            entry = self._add_ref(
                creator_task_id,
                index=len(self.task_argument_entries[creator_task_id]),
                entry_type=EntryType.INPUT_REF,
                ref=ref,
                ref_to_task_mapping=ref_to_task_mapping,
            )
            self.task_argument_entries[creator_task_id].append(entry)
            self.task_input_entries[task_id].append(entry)

    def del_task_inputs(self, task_id: TaskID):
        for entry in self.task_input_entries.get(task_id, []):
            self._remove_ref(task_id, entry)

    def get_task_outputs(self, task_id: TaskID) -> List[RefType]:
        results = []
        for entry in self.task_output_entries.get(task_id, []):
            ref = self.entry_ref_map.get(entry)
            if ref is None:
                ref = self.entry_checkpoint_map[entry]
            results.append(ref)
        return results

    def get_task_outputs_metadata(self, task_id: TaskID) -> List[Dict]:
        return self._entries_to_metadata(self.task_output_entries.get(task_id, []))

    def set_task_outputs(
        self,
        task_id: TaskID,
        outputs: List[RefType],
        ref_to_task_mapping: Optional[Dict[RefType, TaskID]] = None,
    ):
        """This function is called only once after a task completes."""
        assert task_id not in self.task_output_entries, (
            task_id,
            self.task_output_entries,
        )
        if not outputs:
            return  # do not set empty outputs to keep the state clean
        if ref_to_task_mapping is None:
            ref_to_task_mapping = {}
        self.task_output_entries[task_id] = []
        for i, ref in enumerate(outputs):
            # previous duplicated outputs do not affect the index
            entry = self._add_ref(
                task_id,
                index=i,
                entry_type=EntryType.OUTPUT_REF,
                ref=ref,
                ref_to_task_mapping=ref_to_task_mapping,
            )
            self.task_output_entries[task_id].append(entry)

    def set_task_outputs_checkpointed(
        self, task_id: TaskID, checkpoint_map: Optional[Dict[Entry, str]] = None
    ):
        if checkpoint_map is None:
            checkpoint_map = {}
            for entry in self.get_own_output_entries(task_id):
                checkpoint_map[entry] = _get_ref_hex(self.entry_ref_map[entry])
        self.entry_checkpoint_map.update(checkpoint_map)

    def del_task_outputs(self, task_id: TaskID):
        for entry in self.task_output_entries.get(task_id, []):
            self._remove_ref(task_id, entry)

    def merge_entries_from_subworkflow(
        self, task_id: TaskID, sub_tracker: "EphemeralObjectTracker"
    ):
        """Merge the entries from a subworkflow.

        Args:
            task_id: The task that returns the subworkflow.
        """
        if not sub_tracker.task_argument_entries:
            return
        assert tuple(sub_tracker.task_argument_entries.keys()) == (task_id,), (
            task_id,
            sub_tracker.task_argument_entries,
        )
        for t, input_entries in sub_tracker.task_input_entries.items():
            input_refs = []
            for entry in input_entries:
                input_refs.append(sub_tracker.entry_ref_map[entry])
            self.set_task_inputs(t, task_id, input_refs)

    def propagate_references(self, task_id: TaskID, downstream_tasks: Set[TaskID]):
        # TODO(suquark): we did not handle task input entries here, which is related
        # to the continuation tasks.
        if not downstream_tasks:
            return
        for entry in self.task_output_entries.get(task_id, []):
            self.entry_reference_set.setdefault(entry, set())
            self.entry_reference_set[entry].update(downstream_tasks)

    def get_downstream_references(self, task_id: TaskID) -> Set[TaskID]:
        affected_tasks = set()
        for et in self.task_output_entries.get(task_id, []):
            affected_tasks.update(self.entry_reference_set.get(et, set()))
        return affected_tasks

    def get_own_output_entries(self, task_id: TaskID) -> List[Entry]:
        entries = []
        for entry in self.task_output_entries.get(task_id, []):
            if entry.creator_task_id == task_id:
                entries.append(entry)
        return entries

    def invalidate_task(self, task_id: TaskID):
        for entry in self.get_own_output_entries(task_id):
            if entry in self.entry_ref_map:
                # We are going to use new refs. We mark the old ref as outdated
                # so that later failure with the old ref should be ignored.
                self.outdated_refs.add(_get_ref_hex(self.entry_ref_map.pop(entry)))
                # TODO(suquark): It seems we should just keep the reference count?
                self.entry_ref_counter.pop(entry)


@dataclasses.dataclass
class WorkflowExecutionState:
    """The execution state of a workflow. This dataclass helps with observation
    and debugging."""

    # -------------------------------- dependencies -------------------------------- #

    # The mapping from all tasks to immediately upstream tasks.
    upstream_dependencies: Dict[TaskID, List[TaskID]] = field(default_factory=dict)
    # A reverse mapping of the above. The dependency mapping from tasks to
    # immediately downstream tasks.
    downstream_dependencies: Dict[TaskID, List[TaskID]] = field(
        default_factory=lambda: defaultdict(list)
    )
    # The mapping from a task to its immediate continuation.
    next_continuation: Dict[TaskID, TaskID] = field(default_factory=dict)
    # The reversed mapping from continuation to its immediate task.
    prev_continuation: Dict[TaskID, TaskID] = field(default_factory=dict)
    # The mapping from a task to its latest continuation. The latest continuation is
    # a task that returns a value instead of a continuation.
    latest_continuation: Dict[TaskID, TaskID] = field(default_factory=dict)
    # The mapping from a task to the root of the continuation, i.e. the initial task
    # that generates the lineage of continuation.
    continuation_root: Dict[TaskID, TaskID] = field(default_factory=dict)
    # The mapping from all tasks to immediately upstream task outputs.
    upstream_data_dependencies: Dict[TaskID, List[TaskID]] = field(default_factory=dict)
    # ------------------------------- task properties ------------------------------- #

    # Workflow tasks.
    tasks: Dict[TaskID, Task] = field(default_factory=dict)

    # The arguments for the task.
    task_input_args: Dict[TaskID, ray.ObjectRef] = field(default_factory=dict)
    # The keys of inputs for the task that defined in DAGInput
    task_dag_input_keys: Dict[TaskID, Set[Union[int, str]]] = field(
        default_factory=dict
    )
    # The map from a task to its in-memory outputs. Normally it is the ObjectRef
    # returned by the underlying Ray task. Things are different for continuation:
    # because the true output of a continuation is created by the last task in
    # the continuation lineage, so all other tasks in the continuation points
    # to the output of the last task instead of the output of themselves.

    # The task output without object refs.
    task_output_placeholder_ref: Dict[TaskID, WorkflowRef] = field(default_factory=dict)
    # The context of the task.
    task_context: Dict[TaskID, WorkflowTaskContext] = field(default_factory=dict)
    # The execution metadata of a task.
    task_execution_metadata: Dict[TaskID, TaskExecutionMetadata] = field(
        default_factory=dict
    )
    task_retries: Dict[TaskID, int] = field(default_factory=lambda: defaultdict(int))

    # ------------------------------ object management ------------------------------ #

    # Set of references to upstream outputs [upstream -> set[downstream]].
    reference_set: Dict[TaskID, Set[TaskID]] = field(
        default_factory=lambda: defaultdict(set)
    )
    # reversed reference set, mapped from a task to all references in its inputs
    rev_reference_set: Dict[TaskID, Set[TaskID]] = field(
        default_factory=lambda: defaultdict(set)
    )
    # The set of pending inputs of a task. We are able to run the task
    # when it becomes empty [downstream -> set[upstream]].
    pending_input_set: Dict[TaskID, Set[TaskID]] = field(default_factory=dict)
    # The map from a task to its in-storage checkpoints. Normally it is the checkpoint
    # created by the underlying Ray task. For continuations, the semantics is similar
    # to 'task_output_placeholder_ref'.
    checkpoint_map: Dict[TaskID, WorkflowRef] = field(default_factory=dict)
    # object ref tracker
    object_ref_tracker: EphemeralObjectTracker = field(
        default_factory=EphemeralObjectTracker
    )
    # actor tracker
    actor_tracker: EphemeralObjectTracker = field(
        default_factory=EphemeralObjectTracker
    )
    # -------------------------------- scheduling -------------------------------- #

    # The frontier that is ready to run.
    frontier_to_run: Deque[TaskID] = field(default_factory=deque)
    # The set of frontier tasks to run. This field helps deduplicate tasks or
    # look up task quickly. It contains the same elements as 'frontier_to_run',
    # they act like a 'DequeSet' when combined.
    frontier_to_run_set: Set[TaskID] = field(default_factory=set)
    # The frontier that is running.
    running_frontier: Dict[asyncio.Future, WorkflowRef] = field(default_factory=dict)
    # The set of running frontier. This field helps deduplicate tasks or
    # look up task quickly. It contains the same elements as 'running_frontier',
    # they act like a dict but its values are in a set when combined.
    running_frontier_set: Set[TaskID] = field(default_factory=set)
    # The set of completed tasks. They are tasks are actually executed with the state,
    # so inspected during recovery does not count.
    #
    # Normally, a task will be added in 'done_tasks' immediately after its completion.
    # However, a task that is the root of continuations (i.e. it returns a continuation
    # but itself is not a continuation) is only added to 'done_tasks' when all its
    # continuation completes. We do not add its continuations in 'done_tasks' because
    # we indicate their completion from the continuation structure - if a continuation
    # is appended to a previous continuation, then the previous continuation must
    # already complete; if the task that is the root of all continuation completes,
    # then all its continuations would complete.
    done_tasks: Set[TaskID] = field(default_factory=set)

    # Tasks that are prepared to be submitted later. This includes the done tasks that
    # are scheduled again due to recovery
    on_schedule_tasks: Set[TaskID] = field(default_factory=set)

    # -------------------------------- external -------------------------------- #

    # The input dict of the DAG.
    input_dict: Dict = field(default_factory=dict)
    # The ID of the output task.
    output_task_id: Optional[TaskID] = None

    def get_input(
        self, task_id: TaskID, indirect: bool = True
    ) -> Optional[Union[WorkflowRef, ObjectRef]]:
        """Get the input. It checks memory first and storage later. It returns None if
        the input does not exist.
        """
        wf_ref = self.task_output_placeholder_ref[task_id]
        object_refs = self.object_ref_tracker.get_task_outputs(task_id)
        actors = self.actor_tracker.get_task_outputs(task_id)
        if not indirect:
            return _wrap_task_output(wf_ref.ref, object_refs, actors)

        s = _TaskOutputEncoder(wf_ref.ref, object_refs, actors)
        return WorkflowRef(wf_ref.task_id, ref=s)

    def pop_frontier_to_run(self) -> Optional[TaskID]:
        """Pop one task to run from the frontier queue."""
        try:
            t = self.frontier_to_run.popleft()
            self.frontier_to_run_set.remove(t)
            return t
        except IndexError:
            return None

    def append_frontier_to_run(self, task_id: TaskID) -> None:
        """Insert one task to the frontier queue."""
        if (
            task_id not in self.frontier_to_run_set
            and task_id not in self.running_frontier_set
        ):
            self.frontier_to_run.append(task_id)
            self.frontier_to_run_set.add(task_id)

    def add_dependencies(self, task_id: TaskID, in_dependencies: List[TaskID]) -> None:
        """Add dependencies between a task and it input dependencies."""
        self.upstream_dependencies[task_id] = in_dependencies
        for in_task_id in in_dependencies:
            self.downstream_dependencies[in_task_id].append(task_id)

    def add_data_dependencies(
        self, task_id: TaskID, in_dependencies: List[TaskID]
    ) -> None:
        """Add dependencies between a task and it input data dependencies."""
        self.upstream_data_dependencies[task_id] = in_dependencies

    def pop_running_frontier(self, fut: asyncio.Future) -> WorkflowRef:
        """Pop a task from the running frontier."""
        ref = self.running_frontier.pop(fut)
        self.running_frontier_set.remove(ref.task_id)
        return ref

    def insert_running_frontier(self, fut: asyncio.Future, ref: WorkflowRef) -> None:
        """Insert a task to the running frontier."""
        self.running_frontier[fut] = ref
        self.running_frontier_set.add(ref.task_id)

    def append_continuation(
        self, task_id: TaskID, continuation_task_id: TaskID
    ) -> None:
        """Append continuation to a task."""
        continuation_root = self.continuation_root.get(task_id, task_id)
        self.prev_continuation[continuation_task_id] = task_id
        self.next_continuation[task_id] = continuation_task_id
        self.continuation_root[continuation_task_id] = continuation_root
        self.latest_continuation[continuation_root] = continuation_task_id

    def merge_state(self, state: "WorkflowExecutionState") -> None:
        """Merge with another execution state."""
        self.upstream_dependencies.update(state.upstream_dependencies)
        self.upstream_data_dependencies.update(state.upstream_data_dependencies)
        self.downstream_dependencies.update(state.downstream_dependencies)
        self.task_input_args.update(state.task_input_args)
        self.tasks.update(state.tasks)
        self.task_context.update(state.task_context)
        self.task_dag_input_keys.update(state.task_dag_input_keys)
        # self.output_map.update(state.output_map)
        self.checkpoint_map.update(state.checkpoint_map)

    def add_reference(self, req_task: TaskID, target_task: TaskID):
        self.reference_set[target_task].add(req_task)
        self.rev_reference_set[req_task].add(target_task)

    def remove_reference(self, req_task: TaskID):
        self._evict_task_input(req_task)
        for c in self.rev_reference_set.pop(req_task, []):
            self.reference_set[c].remove(req_task)
            if not self.reference_set[c]:
                del self.reference_set[c]
                self._evict_task_output(c)

    def _evict_task_input(self, task_id: TaskID):
        self.task_input_args.pop(task_id, None)
        self.object_ref_tracker.del_task_inputs(task_id)
        self.actor_tracker.del_task_inputs(task_id)

    def _evict_task_output(self, task_id: TaskID):
        del self.task_output_placeholder_ref[task_id]
        self.object_ref_tracker.del_task_outputs(task_id)
        self.actor_tracker.del_task_outputs(task_id)

    def _task_done_and_checkpoint_exists(self, task_id: TaskID) -> bool:
        # TODO(suquark): this is a temporary hack. we just assume async
        #  checkpointing is done.
        if self.actor_tracker.get_own_output_entries(task_id):
            return False
        return task_id in self.checkpoint_map or (
            task_id in self.done_tasks
            and self.tasks[task_id].options.checkpoint in (True, "async")
        )

    def _reconstruct_checkpointed_task(self, task_id: TaskID):
        """This function is similar to
        workflow_state_from_storage._reconstruct_checkpointed_task

        TODO(suquark): Handle the corner case where failure occurs when recovering
            from a cluster failure. Also we need to check if this works for
            workflow continuation in general.
        """
        # assert task_id == self.continuation_root.get(task_id, task_id)

        if not self._task_done_and_checkpoint_exists(task_id):
            return self.upstream_dependencies[task_id]

        extra_deps = set()
        for entry in self.object_ref_tracker.task_output_entries.get(task_id, []):
            if entry.creator_task_id != task_id:
                extra_deps.add(entry.creator_task_id)
        for entry in self.actor_tracker.task_output_entries.get(task_id, []):
            if entry.creator_task_id != task_id:
                extra_deps.add(entry.creator_task_id)
        return extra_deps

    def construct_scheduling_plan(self, task_id: TaskID) -> None:
        """Analyze upstream dependencies of a task to construct the scheduling plan."""
        # if self.get_input(task_id) is not None:
        #     # This case corresponds to the scenario that the task is a
        #     # checkpoint or ref.
        #     return

        dag_visit_queue = deque([task_id])
        while dag_visit_queue:
            tid = dag_visit_queue.popleft()
            # We should not schedule a task, if the task is already been scheduled
            # or its output is still in memory.
            if tid in self.on_schedule_tasks or tid in self.task_output_placeholder_ref:
                continue
            self.on_schedule_tasks.add(tid)
            self.pending_input_set[tid] = set()
            for in_task_id in self._reconstruct_checkpointed_task(tid):
                # keep the upstream task output alive
                self.add_reference(tid, in_task_id)
                if in_task_id not in self.task_output_placeholder_ref:
                    # All upstream deps should already complete here,
                    # so we just check their checkpoints.
                    self.pending_input_set[tid].add(in_task_id)
                    dag_visit_queue.append(in_task_id)
            if tid in self.latest_continuation:
                if self.pending_input_set[tid]:
                    raise ValueError(
                        "A task that already returns a continuation cannot be pending."
                    )
                # construct continuations, as they are not directly connected to
                # the DAG dependency
                self.construct_scheduling_plan(self.latest_continuation[tid])
            elif not self.pending_input_set[tid]:
                self.append_frontier_to_run(tid)

    def init_context(self, context: WorkflowTaskContext) -> None:
        """Initialize the context of all tasks."""
        for task_id, task in self.tasks.items():
            options = task.options
            self.task_context.setdefault(
                task_id,
                dataclasses.replace(
                    context,
                    task_id=task_id,
                    creator_task_id=context.task_id,
                    checkpoint=options.checkpoint,
                    catch_exceptions=options.catch_exceptions,
                ),
            )
