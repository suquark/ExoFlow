import functools
import logging
from typing import Dict, Set, List, Tuple, Union, Optional, Any
import os
import time
import uuid

import ray
from ray.dag import DAGNode
from ray.dag.input_node import DAGInputData
from ray.remote_function import RemoteFunction

# avoid collision with arguments & APIs

from exoflow.common import (
    WorkflowStatus,
    Event,
    asyncio_run,
    validate_user_metadata,
)
from exoflow import serialization, workflow_access, workflow_context
from exoflow.event_listener import EventListener, EventListenerType, TimerListener
from exoflow.workflow_storage import WorkflowStorage
from exoflow.workflow_state_from_dag import workflow_state_from_dag
from exoflow.common import TaskID
from exoflow.workflow_state import TaskExecutionMetadata

from ray.util.annotations import PublicAPI
from ray._private.usage import usage_lib

logger = logging.getLogger(__name__)


@PublicAPI(stability="alpha")
def init(
    *,
    max_running_workflows: Optional[int] = None,
    max_pending_workflows: Optional[int] = None,
) -> None:
    """Initialize workflow.

    If Ray is not initialized, we will initialize Ray and
    use ``/tmp/ray/workflow_data`` as the default storage.

    Args:
        max_running_workflows: The maximum number of concurrently running workflows.
            Use -1 as infinity. 'None' means preserving previous setting or initialize
            the setting with infinity.
        max_pending_workflows: The maximum number of queued workflows.
            Use -1 as infinity. 'None' means preserving previous setting or initialize
            the setting with infinity.
    """
    usage_lib.record_library_usage("workflow")

    if max_running_workflows is not None:
        if not isinstance(max_running_workflows, int):
            raise TypeError("'max_running_workflows' must be None or an integer.")
        if max_running_workflows < -1 or max_running_workflows == 0:
            raise ValueError(
                "'max_running_workflows' must be a positive integer "
                "or use -1 as infinity."
            )
    if max_pending_workflows is not None:
        if not isinstance(max_pending_workflows, int):
            raise TypeError("'max_pending_workflows' must be None or an integer.")
        if max_pending_workflows < -1:
            raise ValueError(
                "'max_pending_workflows' must be a non-negative integer "
                "or use -1 as infinity."
            )

    if not ray.is_initialized():
        # We should use get_temp_dir_path, but for ray client, we don't
        # have this one. We need a flag to tell whether it's a client
        # or a driver to use the right dir.
        # For now, just use /tmp/ray/workflow_data
        ray.init(storage="file:///tmp/ray/workflow_data")
    workflow_access.init_management_actor(max_running_workflows, max_pending_workflows)
    serialization.init_manager()


def _ensure_workflow_initialized() -> None:
    # NOTE: Trying to get the actor has a side effect: it initializes Ray with
    # default arguments. This is different in "init()": it assigns a temporary
    # storage. This is why we need to check "ray.is_initialized()" first.
    if not ray.is_initialized():
        init()
    else:
        try:
            workflow_access.get_management_actor()
        except ValueError:
            init()


def client_mode_wrap(func):
    """Wraps a function called during client mode for execution as a remote task.

    Adopted from "ray._private.client_mode_hook.client_mode_wrap". Some changes are made
    (e.g., init the workflow instead of init Ray; the latter does not specify a storage
    during Ray init and will result in workflow failures).
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        from ray._private.client_mode_hook import client_mode_should_convert

        if os.environ.get("RAY_ENABLE_AUTO_CONNECT", "") != "0":
            _ensure_workflow_initialized()

        # `is_client_mode_enabled_by_default` is used for testing with
        # `RAY_CLIENT_MODE=1`. This flag means all tests run with client mode.
        if client_mode_should_convert(auto_init=False):
            f = ray.remote(num_cpus=0)(func)
            ref = f.remote(*args, **kwargs)
            return ray.get(ref)
        return func(*args, **kwargs)

    return wrapper


@PublicAPI(stability="alpha")
def run(
    dag: DAGNode,
    *args,
    workflow_id: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
    **kwargs,
) -> Any:
    """Run a workflow.

    If the workflow with the given id already exists, it will be resumed.

    Examples:
        >>> import ray
        >>> import exoflow
        >>> Flight, Reservation, Trip = ... # doctest: +SKIP
        >>> @ray.remote # doctest: +SKIP
        ... def book_flight(origin: str, dest: str) -> Flight: # doctest: +SKIP
        ...    return Flight(...) # doctest: +SKIP
        >>> @ray.remote # doctest: +SKIP
        ... def book_hotel(location: str) -> Reservation: # doctest: +SKIP
        ...    return Reservation(...) # doctest: +SKIP
        >>> @ray.remote # doctest: +SKIP
        ... def finalize_trip(bookings: List[Any]) -> Trip: # doctest: +SKIP
        ...    return Trip(...) # doctest: +SKIP

        >>> flight1 = book_flight.bind("OAK", "SAN") # doctest: +SKIP
        >>> flight2 = book_flight.bind("SAN", "OAK") # doctest: +SKIP
        >>> hotel = book_hotel.bind("SAN") # doctest: +SKIP
        >>> trip = finalize_trip.bind([flight1, flight2, hotel]) # doctest: +SKIP
        >>> result = exoflow.run(trip) # doctest: +SKIP

    Args:
        workflow_id: A unique identifier that can be used to resume the
            workflow. If not specified, a random id will be generated.
        metadata: The metadata to add to the workflow. It has to be able
            to serialize to json.

    Returns:
        The running result.
    """
    return ray.get(
        run_async(dag, *args, workflow_id=workflow_id, metadata=metadata, **kwargs)
    )


def _submit_workflow(
    dag: DAGNode,
    *args,
    workflow_id: str,
    metadata: Optional[Dict[str, Any]] = None,
    is_service: bool = False,
    **kwargs,
) -> bool:
    """Submit a workflow DAG.

    Args:
        workflow_id: A unique identifier that can be used to resume the
            workflow. If not specified, a random id will be generated.
        metadata: The metadata to add to the workflow. It has to be able
            to serialize to json.

    Returns:
       True if the workflow with the given id already exists.

    """
    if not isinstance(dag, DAGNode):
        raise TypeError("Input should be a DAG.")
    input_data = DAGInputData(*args, **kwargs)
    validate_user_metadata(metadata)
    metadata = metadata or {}

    controller = workflow_access.get_management_actor()
    if ray.get(controller.is_workflow_non_terminating.remote(workflow_id)):
        raise RuntimeError(f"Workflow '{workflow_id}' is already running or pending.")

    state = workflow_state_from_dag(dag, input_data, workflow_id)
    logger.info(f'Workflow job created. [id="{workflow_id}"].')
    context = workflow_context.WorkflowTaskContext(workflow_id=workflow_id)
    with workflow_context.workflow_task_context(context):
        # checkpoint the workflow
        @client_mode_wrap
        def _try_checkpoint_workflow(workflow_state) -> bool:
            ws = WorkflowStorage(workflow_id)
            ws.save_workflow_user_metadata(metadata)
            try:
                ws.get_entrypoint_task_id()
                return True
            except Exception:
                # The workflow does not exist. We must checkpoint entry workflow.
                ws.save_workflow_execution_state("", workflow_state)
                return False

        wf_exists = _try_checkpoint_workflow(state)
        if not wf_exists:
            if not is_service:
                ray.get(
                    controller.submit_workflow.remote(
                        workflow_id,
                        state,
                        ignore_existing=False,
                        is_service=is_service,
                    )
                )
            else:
                from ray.experimental.internal_kv import _internal_kv_get

                n_controllers = int(
                    _internal_kv_get("n_controllers", namespace="workflow")
                )
                outputs = []
                for i in range(n_controllers):
                    controller = workflow_access.get_management_actor(i)
                    outputs.append(
                        controller.submit_workflow.remote(
                            workflow_id,
                            state,
                            ignore_existing=False,
                            is_service=is_service,
                        )
                    )
                ray.get(outputs)
        return wf_exists


@PublicAPI(stability="alpha")
def run_async(
    dag: DAGNode,
    *args,
    workflow_id: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
    **kwargs,
) -> ray.ObjectRef:
    """Run a workflow asynchronously.

    If the workflow with the given id already exists, it will be resumed.

    Args:
        workflow_id: A unique identifier that can be used to resume the
            workflow. If not specified, a random id will be generated.
        metadata: The metadata to add to the workflow. It has to be able
            to serialize to json.

    Returns:
       The running result as ray.ObjectRef.

    """
    _ensure_workflow_initialized()
    if workflow_id is None:
        # Workflow ID format: {Entry workflow UUID}.{Unix time to nanoseconds}
        workflow_id = f"{str(uuid.uuid4())}.{time.time():.9f}"
    wf_exists = _submit_workflow(
        dag,
        *args,
        workflow_id=workflow_id,
        metadata=metadata,
        is_service=False,
        **kwargs,
    )
    if wf_exists:
        return resume_async(workflow_id)
    controller = workflow_access.get_management_actor()
    job_id = ray.get_runtime_context().job_id.hex()
    context = workflow_context.WorkflowTaskContext(workflow_id=workflow_id)
    return controller.execute_workflow.remote(job_id, context)


def register_service(
    dag: DAGNode,
    *args,
    workflow_id,
    metadata: Optional[Dict[str, Any]] = None,
    **kwargs,
) -> None:
    """Register a workflow as a service.

    Args:
        workflow_id: A unique identifier that can be used to resume the
            workflow.
        metadata: The metadata to add to the workflow. It has to be able
            to serialize to json.

    Returns:
       The running result as ray.ObjectRef.

    """
    _ensure_workflow_initialized()
    wf_exists = _submit_workflow(
        dag,
        *args,
        workflow_id=workflow_id,
        metadata=metadata,
        is_service=True,
        **kwargs,
    )
    assert not wf_exists


def run_service_async(
    workflow_id: str, service_id: str, persist_input: bool = False, *args, **kwargs
) -> ray.ObjectRef:
    _ensure_workflow_initialized()
    job_id = ray.get_runtime_context().job_id.hex()
    controller = workflow_access.get_management_actor(index=None)
    input_dict = dict(enumerate(args))
    input_dict.update(kwargs)
    # TODO(suquark): skip checkpointing if not persisted
    return controller.execute_service.remote(
        workflow_id, service_id, job_id, persist_input, input_dict
    )


@PublicAPI(stability="alpha")
def resume(workflow_id: str) -> Any:
    """Resume a workflow.

    Resume a workflow and retrieve its output. If the workflow was incomplete,
    it will be re-executed from its checkpointed outputs. If the workflow was
    complete, returns the result immediately.

    Examples:
        >>> import exoflow
        >>> start_trip = ... # doctest: +SKIP
        >>> trip = start_trip.bind() # doctest: +SKIP
        >>> res1 = exoflow.run_async(trip, workflow_id="trip1") # doctest: +SKIP
        >>> res2 = exoflow.resume_async("trip1") # doctest: +SKIP
        >>> assert ray.get(res1) == ray.get(res2) # doctest: +SKIP

    Args:
        workflow_id: The id of the workflow to resume.

    Returns:
        The output of the workflow.
    """
    return ray.get(resume_async(workflow_id))


@PublicAPI(stability="alpha")
def resume_async(workflow_id: str) -> ray.ObjectRef:
    """Resume a workflow asynchronously.

    Resume a workflow and retrieve its output. If the workflow was incomplete,
    it will be re-executed from its checkpointed outputs. If the workflow was
    complete, returns the result immediately.

    Examples:
        >>> import exoflow
        >>> start_trip = ... # doctest: +SKIP
        >>> trip = start_trip.bind() # doctest: +SKIP
        >>> res1 = exoflow.run_async(trip, workflow_id="trip1") # doctest: +SKIP
        >>> res2 = exoflow.resume_async("trip1") # doctest: +SKIP
        >>> assert ray.get(res1) == ray.get(res2) # doctest: +SKIP

    Args:
        workflow_id: The id of the workflow to resume.

    Returns:
        An object reference that can be used to retrieve the workflow result.
    """
    _ensure_workflow_initialized()
    logger.info(f'Resuming workflow [id="{workflow_id}"].')
    controller = workflow_access.get_management_actor()
    if ray.get(controller.is_workflow_non_terminating.remote(workflow_id)):
        raise RuntimeError(f"Workflow '{workflow_id}' is already running or pending.")
    # NOTE: It is important to 'ray.get' the returned output. This
    # ensures caller of 'run()' holds the reference to the workflow
    # result. Otherwise if the actor removes the reference of the
    # workflow output, the caller may fail to resolve the result.
    job_id = ray.get_runtime_context().job_id.hex()

    context = workflow_context.WorkflowTaskContext(workflow_id=workflow_id)
    ray.get(controller.reconstruct_workflow.remote(job_id, context))
    result = controller.execute_workflow.remote(job_id, context)
    logger.info(f"Workflow job {workflow_id} resumed.")
    return result


@PublicAPI(stability="alpha")
def get_output(workflow_id: str, *, name: Optional[str] = None) -> Any:
    """Get the output of a running workflow.

    Args:
        workflow_id: The workflow to get the output of.
        name: If set, fetch the specific task instead of the output of the
            workflow.

    Examples:
        >>> import exoflow
        >>> start_trip = ... # doctest: +SKIP
        >>> trip = start_trip.options(name="trip").bind() # doctest: +SKIP
        >>> res1 = exoflow.run_async(trip, workflow_id="trip1") # doctest: +SKIP
        >>> # you could "get_output()" in another machine
        >>> res2 = exoflow.get_output_async("trip1") # doctest: +SKIP
        >>> assert ray.get(res1) == ray.get(res2) # doctest: +SKIP
        >>> task_output = exoflow.get_output_async("trip1", "trip") # doctest: +SKIP
        >>> assert ray.get(task_output) == ray.get(res1) # doctest: +SKIP

    Returns:
        The output of the workflow task.
    """
    return ray.get(get_output_async(workflow_id, task_id=name))


@PublicAPI(stability="alpha")
@client_mode_wrap
def get_output_async(
    workflow_id: str, *, task_id: Optional[str] = None
) -> ray.ObjectRef:
    """Get the output of a running workflow asynchronously.

    Args:
        workflow_id: The workflow to get the output of.
        task_id: If set, fetch the specific task output instead of the output
            of the workflow.

    Returns:
        An object reference that can be used to retrieve the workflow task result.
    """
    _ensure_workflow_initialized()
    try:
        controller = workflow_access.get_management_actor()
    except ValueError as e:
        raise ValueError(
            "Failed to connect to the workflow management "
            "actor. The workflow could have already failed. You can use "
            "exoflow.resume() or exoflow.resume_async() to resume the "
            "workflow."
        ) from e
    return controller.get_output.remote(workflow_id, task_id)


@PublicAPI(stability="alpha")
@client_mode_wrap
def list_all(
    status_filter: Optional[
        Union[Union[WorkflowStatus, str], Set[Union[WorkflowStatus, str]]]
    ] = None
) -> List[Tuple[str, WorkflowStatus]]:
    """List all workflows matching a given status filter. When returning "RESUMEABLE"
    workflows, the workflows that was running ranks before the workflow that was pending
    in the result list.

    Args:
        status_filter: If given, only returns workflow with that status. This can
            be a single status or set of statuses. The string form of the
            status is also acceptable, i.e.,
            "RUNNING"/"FAILED"/"SUCCESSFUL"/"CANCELED"/"RESUMABLE"/"PENDING".
    Examples:
        >>> import exoflow
        >>> long_running_job = ... # doctest: +SKIP
        >>> workflow_task = long_running_job.bind() # doctest: +SKIP
        >>> wf = exoflow.run_async(workflow_task, # doctest: +SKIP
        ...     workflow_id="long_running_job")
        >>> jobs = exoflow.list_all() # doctest: +SKIP
        >>> assert jobs == [ ("long_running_job", exoflow.RUNNING) ] # doctest: +SKIP
        >>> ray.get(wf) # doctest: +SKIP
        >>> jobs = exoflow.list_all({exoflow.RUNNING}) # doctest: +SKIP
        >>> assert jobs == [] # doctest: +SKIP
        >>> jobs = exoflow.list_all(exoflow.SUCCESSFUL) # doctest: +SKIP
        >>> assert jobs == [ # doctest: +SKIP
        ...     ("long_running_job", exoflow.SUCCESSFUL)]

    Returns:
        A list of tuple with workflow id and workflow status
    """
    _ensure_workflow_initialized()
    if isinstance(status_filter, str):
        status_filter = set({WorkflowStatus(status_filter)})
    elif isinstance(status_filter, WorkflowStatus):
        status_filter = set({status_filter})
    elif isinstance(status_filter, set):
        if all(isinstance(s, str) for s in status_filter):
            status_filter = {WorkflowStatus(s) for s in status_filter}
        elif not all(isinstance(s, WorkflowStatus) for s in status_filter):
            raise TypeError(
                "status_filter contains element which is not"
                " a type of `WorkflowStatus or str`."
                f" {status_filter}"
            )
    elif status_filter is None:
        status_filter = set(WorkflowStatus)
        status_filter.discard(WorkflowStatus.NONE)
    else:
        raise TypeError(
            "status_filter must be WorkflowStatus or a set of WorkflowStatus."
        )

    try:
        controller = workflow_access.get_management_actor()
    except ValueError:
        controller = None

    if controller is None:
        non_terminating_workflows = {}
    else:
        non_terminating_workflows = ray.get(
            controller.list_non_terminating_workflows.remote()
        )

    ret = []
    if set(non_terminating_workflows.keys()).issuperset(status_filter):
        for status, workflows in non_terminating_workflows.items():
            if status in status_filter:
                for w in workflows:
                    ret.append((w, status))
        return ret

    ret = []
    # Here we don't have workflow id, so use empty one instead
    store = WorkflowStorage("")
    modified_status_filter = status_filter.copy()
    # Here we have to add non-terminating status to the status filter, because some
    # "RESUMABLE" workflows are converted from non-terminating workflows below.
    # This is the tricky part: the status "RESUMABLE" neither come from
    # the workflow management actor nor the storage. It is the status where
    # the storage says it is non-terminating but the workflow management actor
    # is not running it. This usually happened when there was a sudden crash
    # of the whole Ray runtime or the workflow management actor
    # (due to cluster etc.). So we includes non terminating status in the storage
    # filter to get "RESUMABLE" candidates.
    modified_status_filter.update(WorkflowStatus.non_terminating_status())
    status_from_storage = store.list_workflow(modified_status_filter)
    non_terminating_workflows = {
        k: set(v) for k, v in non_terminating_workflows.items()
    }
    resume_running = []
    resume_pending = []
    for (k, s) in status_from_storage:
        if s in non_terminating_workflows and k not in non_terminating_workflows[s]:
            if s == WorkflowStatus.RUNNING:
                resume_running.append(k)
            elif s == WorkflowStatus.PENDING:
                resume_pending.append(k)
            else:
                assert False, "This line of code should not be reachable."
            continue
        if s in status_filter:
            ret.append((k, s))
    if WorkflowStatus.RESUMABLE in status_filter:
        # The running workflows ranks before the pending workflows.
        for w in resume_running:
            ret.append((w, WorkflowStatus.RESUMABLE))
        for w in resume_pending:
            ret.append((w, WorkflowStatus.RESUMABLE))
    return ret


@PublicAPI(stability="alpha")
@client_mode_wrap
def resume_all(include_failed: bool = False) -> List[Tuple[str, ray.ObjectRef]]:
    """Resume all resumable workflow jobs.

    This can be used after cluster restart to resume all tasks.

    Args:
        include_failed: Whether to resume FAILED workflows.

    Examples:
        >>> import exoflow
        >>> failed_job = ... # doctest: +SKIP
        >>> workflow_task = failed_job.bind() # doctest: +SKIP
        >>> output = exoflow.run_async( # doctest: +SKIP
        ...     workflow_task, workflow_id="failed_job")
        >>> try: # doctest: +SKIP
        >>>     ray.get(output) # doctest: +SKIP
        >>> except Exception: # doctest: +SKIP
        >>>     print("JobFailed") # doctest: +SKIP
        >>> jobs = exoflow.list_all() # doctest: +SKIP
        >>> assert jobs == [("failed_job", exoflow.FAILED)] # doctest: +SKIP
        >>> assert exoflow.resume_all( # doctest: +SKIP
        ...    include_failed=True).get("failed_job") is not None # doctest: +SKIP

    Returns:
        A list of (workflow_id, returned_obj_ref) resumed.
    """
    _ensure_workflow_initialized()
    filter_set = {WorkflowStatus.RESUMABLE}
    if include_failed:
        filter_set.add(WorkflowStatus.FAILED)
    all_failed = list_all(filter_set)

    try:
        controller = workflow_access.get_management_actor()
    except Exception as e:
        raise RuntimeError("Failed to get management actor") from e

    job_id = ray.get_runtime_context().job_id.hex()
    reconstructed_workflows = []
    for wid, _ in all_failed:
        context = workflow_context.WorkflowTaskContext(workflow_id=wid)
        # TODO(suquark): This is not very efficient, but it makes sure
        #  running workflows has higher priority when getting reconstructed.
        try:
            ray.get(controller.reconstruct_workflow.remote(job_id, context))
        except Exception as e:
            # TODO(suquark): Here some workflows got resumed successfully but some
            #  failed and the user has no idea about this, which is very wired.
            # Maybe we should raise an exception here instead?
            logger.error(f"Failed to resume workflow {context.workflow_id}", exc_info=e)
            raise
        reconstructed_workflows.append(context)

    results = []
    for context in reconstructed_workflows:
        results.append(
            (
                context.workflow_id,
                controller.execute_workflow.remote(job_id, context),
            )
        )
    return results


@PublicAPI(stability="alpha")
def get_status(workflow_id: str) -> WorkflowStatus:
    """Get the status for a given workflow.

    Args:
        workflow_id: The workflow to query.

    Examples:
        >>> import exoflow
        >>> trip = ... # doctest: +SKIP
        >>> workflow_task = trip.bind() # doctest: +SKIP
        >>> output = exoflow.run(workflow_task, workflow_id="trip") # doctest: +SKIP
        >>> assert exoflow.SUCCESSFUL == exoflow.get_status("trip") # doctest: +SKIP

    Returns:
        The status of that workflow
    """
    _ensure_workflow_initialized()
    if not isinstance(workflow_id, str):
        raise TypeError("workflow_id has to be a string type.")
    controller = workflow_access.get_management_actor()
    return ray.get(controller.get_workflow_status.remote(workflow_id))


@PublicAPI(stability="alpha")
def wait_for_event(
    event_listener_type: EventListenerType, *args, **kwargs
) -> "DAGNode[Event]":
    if not issubclass(event_listener_type, EventListener):
        raise TypeError(
            f"Event listener type is {event_listener_type.__name__}"
            ", which is not a subclass of exoflow.EventListener"
        )

    @ray.remote
    def get_message(event_listener_type: EventListenerType, *args, **kwargs) -> Event:
        event_listener = event_listener_type()
        return asyncio_run(event_listener.poll_for_event(*args, **kwargs))

    @ray.remote
    def message_committed(
        event_listener_type: EventListenerType, event: Event
    ) -> Event:
        event_listener = event_listener_type()
        asyncio_run(event_listener.event_checkpointed(event))
        return event

    return message_committed.bind(
        event_listener_type, get_message.bind(event_listener_type, *args, **kwargs)
    )


@PublicAPI(stability="alpha")
def sleep(duration: float) -> "DAGNode[Event]":
    """
    A workfow that resolves after sleeping for a given duration.
    """

    @ray.remote
    def end_time():
        return time.time() + duration

    return wait_for_event(TimerListener, end_time.bind())


@PublicAPI(stability="alpha")
@client_mode_wrap
def get_metadata(workflow_id: str, name: Optional[str] = None) -> Dict[str, Any]:
    """Get the metadata of the workflow.

    This will return a dict of metadata of either the workflow (
    if only workflow_id is given) or a specific workflow task (if
    both workflow_id and task name are given). Exception will be
    raised if the given workflow id or task name does not exist.

    If only workflow id is given, this will return metadata on
    workflow level, which includes running status, workflow-level
    user metadata and workflow-level running stats (e.g. the
    start time and end time of the workflow).

    If both workflow id and task name are given, this will return
    metadata on workflow task level, which includes task inputs,
    task-level user metadata and task-level running stats (e.g.
    the start time and end time of the task).


    Args:
        workflow_id: The workflow to get the metadata of.
        name: If set, fetch the metadata of the specific task instead of
            the metadata of the workflow.

    Examples:
        >>> import exoflow
        >>> trip = ... # doctest: +SKIP
        >>> workflow_task = trip.options( # doctest: +SKIP
        ...     **exoflow.options(name="trip", metadata={"k1": "v1"})).bind()
        >>> exoflow.run(workflow_task, # doctest: +SKIP
        ...     workflow_id="trip1", metadata={"k2": "v2"})
        >>> workflow_metadata = exoflow.get_metadata("trip1") # doctest: +SKIP
        >>> assert workflow_metadata["status"] == "SUCCESSFUL" # doctest: +SKIP
        >>> assert workflow_metadata["user_metadata"] == {"k2": "v2"} # doctest: +SKIP
        >>> assert "start_time" in workflow_metadata["stats"] # doctest: +SKIP
        >>> assert "end_time" in workflow_metadata["stats"] # doctest: +SKIP
        >>> task_metadata = exoflow.get_metadata("trip1", "trip") # doctest: +SKIP
        >>> assert task_metadata["task_type"] == "FUNCTION" # doctest: +SKIP
        >>> assert task_metadata["user_metadata"] == {"k1": "v1"} # doctest: +SKIP
        >>> assert "start_time" in task_metadata["stats"] # doctest: +SKIP
        >>> assert "end_time" in task_metadata["stats"] # doctest: +SKIP

    Returns:
        A dictionary containing the metadata of the workflow.

    Raises:
        ValueError: if given workflow or workflow task does not exist.
    """
    _ensure_workflow_initialized()
    store = WorkflowStorage(workflow_id)
    if name is None:
        return store.load_workflow_metadata()
    else:
        return store.load_task_metadata(name)


@PublicAPI(stability="alpha")
def cancel(workflow_id: str) -> None:
    """Cancel a workflow. Workflow checkpoints will still be saved in storage. To
       clean up saved checkpoints, see `exoflow.delete()`.

    Args:
        workflow_id: The workflow to cancel.

    Examples:
        >>> import exoflow
        >>> some_job = ... # doctest: +SKIP
        >>> workflow_task = some_job.bind() # doctest: +SKIP
        >>> output = exoflow.run_async(workflow_task,  # doctest: +SKIP
        ...     workflow_id="some_job")
        >>> exoflow.cancel(workflow_id="some_job") # doctest: +SKIP
        >>> assert [ # doctest: +SKIP
        ...     ("some_job", exoflow.CANCELED)] == exoflow.list_all()

    Returns:
        None

    """
    _ensure_workflow_initialized()
    if not isinstance(workflow_id, str):
        raise TypeError("workflow_id has to be a string type.")
    controller = workflow_access.get_management_actor()
    ray.get(controller.cancel_workflow.remote(workflow_id))


@PublicAPI(stability="alpha")
def delete(workflow_id: str) -> None:
    """Delete a workflow, its checkpoints, and other information it may have
       persisted to storage. To stop a running workflow, see
       `exoflow.cancel()`.

    Args:
        workflow_id: The workflow to delete.

    Raises:
        WorkflowStillActiveError: The workflow is still active.
        WorkflowNotFoundError: The workflow does not exist.

    Examples:
        >>> import exoflow
        >>> some_job = ... # doctest: +SKIP
        >>> workflow_task = some_job.bind() # doctest: +SKIP
        >>> output = exoflow.run_async(workflow_task, # doctest: +SKIP
        ...     workflow_id="some_job")
        >>> exoflow.delete(workflow_id="some_job") # doctest: +SKIP
        >>> assert [] == exoflow.list_all() # doctest: +SKIP
    """
    _ensure_workflow_initialized()
    controller = workflow_access.get_management_actor()
    ray.get(controller.delete_workflow.remote(workflow_id))


@PublicAPI(stability="alpha")
def continuation(dag_node: "Union[DAGNode, List[DAGNode]]") -> Union["DAGNode", Any]:
    """Converts a DAG into a continuation.

    The result depends on the context. If it is inside a workflow, it
    returns a workflow; otherwise it executes and get the result of
    the DAG.

    Args:
        dag_node: The DAG to be converted.
    """
    from exoflow.workflow_context import in_workflow_execution

    if isinstance(dag_node, DAGNode):
        if in_workflow_execution():
            return dag_node
        return ray.get(dag_node.execute())
    elif isinstance(dag_node, (list, tuple)):
        for d in dag_node:
            if not isinstance(d, DAGNode):
                raise TypeError("All objects should be DAG node in the list or tuple.")
        if in_workflow_execution():
            return _continuation_join.bind(dag_node)
        return ray.get([d.execute() for d in dag_node])
    else:
        raise TypeError("Input should be a DAG or a list or tuple of DAGs.")


@PublicAPI(stability="alpha")
def get_task_execution_metadata(
    workflow_id: str, task_id: TaskID
) -> TaskExecutionMetadata:
    _ensure_workflow_initialized()
    if not isinstance(workflow_id, str):
        raise TypeError("workflow_id has to be a string type.")
    controller = workflow_access.get_management_actor()
    return ray.get(
        controller.get_task_execution_metadata.remote(workflow_id, task_id)
    )


@PublicAPI(stability="alpha")
class options:
    """This class serves both as a decorator and options for workflow.

    Examples:
        >>> import ray
        >>> import exoflow
        >>>
        >>> # specify workflow options with a decorator
        >>> @exoflow.options(catch_exceptions=True):
        >>> @ray.remote
        >>> def foo():
        >>>     return 1
        >>>
        >>> # speficy workflow options in ".options"
        >>> foo_new = foo.options(**exoflow.options(catch_exceptions=False))
    """

    def __init__(self, **workflow_options: Dict[str, Any]):
        # TODO(suquark): More rigid arguments check like @ray.remote arguments. This is
        # fairly complex, but we should enable it later.
        valid_options = {
            "name",
            "metadata",
            "catch_exceptions",
            "checkpoint",
            "isolation",
            "wait_until_committed",
        }
        invalid_keywords = set(workflow_options.keys()) - valid_options
        if invalid_keywords:
            raise ValueError(
                f"Invalid option keywords {invalid_keywords} for workflow tasks. "
                f"Valid ones are {valid_options}."
            )
        from exoflow.common import WORKFLOW_OPTIONS

        validate_user_metadata(workflow_options.get("metadata"))

        self.options = {"_metadata": {WORKFLOW_OPTIONS: workflow_options}}

    def keys(self):
        return ("_metadata",)

    def __getitem__(self, key):
        return self.options[key]

    def __call__(self, f: RemoteFunction) -> RemoteFunction:
        if not isinstance(f, RemoteFunction):
            raise ValueError("Only apply 'exoflow.options' to Ray remote functions.")
        f._default_options.update(self.options)
        return f


@options(name="continuation_join", checkpoint=False)
@ray.remote(num_cpus=0)
def _continuation_join(inputs):
    return inputs


__all__ = (
    "init",
    "run",
    "run_async",
    "resume",
    "resume_async",
    "resume_all",
    "cancel",
    "list_all",
    "delete",
    "get_output",
    "get_output_async",
    "get_status",
    "get_metadata",
    "sleep",
    "wait_for_event",
    "options",
    "continuation",
)
