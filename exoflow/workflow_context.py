import logging
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Optional, Set

import ray
from ray._private.ray_logging import configure_log_file, get_worker_log_file_name
from ray.workflow.common import CheckpointModeType, WorkflowStatus, LocalContext

logger = logging.getLogger(__name__)
_local = LocalContext(context=None, in_workflow_execution=False)


@dataclass
class WorkflowTaskContext:
    """
    The structure for saving workflow task context. The context provides
    critical info (e.g. where to checkpoint, which is its parent task)
    for the task to execute correctly.
    """

    # ID of the workflow.
    workflow_id: Optional[str] = None
    # ID of the current task.
    task_id: str = ""
    # ID of the task that creates the current task.
    creator_task_id: str = ""
    # The checkpoint context of parent workflow tasks.
    checkpoint: CheckpointModeType = True
    # The context of catching exceptions.
    catch_exceptions: bool = False
    # The hex string of object refs extracted from input workflow refs.
    refs_hex_in_workflow_refs: Set[str] = field(default_factory=set)
    # If the workflow is a service instance. This is set before submitting the task.
    is_service_instance: bool = False


@contextmanager
def workflow_task_context(context) -> None:
    """Initialize the workflow task context.

    Args:
        context: The new context.
    """
    global _local
    original_context = _local.context
    try:
        _local.context = context
        yield
    finally:
        _local.context = original_context


def get_workflow_task_context() -> Optional[WorkflowTaskContext]:
    return _local.context


def get_current_task_id() -> str:
    """Get the current workflow task ID. Empty means we are in
    the workflow job driver."""
    return get_workflow_task_context().task_id


def get_current_workflow_id() -> str:
    assert _local.context is not None
    return _local.context.workflow_id


def get_name() -> str:
    return f"{get_current_workflow_id()}@{get_current_task_id()}"


def get_task_status_info(status: WorkflowStatus) -> str:
    assert _local.context is not None
    return f"Task status [{status}]\t[{get_name()}]"


@contextmanager
def workflow_execution() -> None:
    """Scope for workflow task execution."""
    global _local
    try:
        _local.in_workflow_execution = True
        yield
    finally:
        _local.in_workflow_execution = False


def in_workflow_execution() -> bool:
    """Whether we are in workflow task execution."""
    global _local
    return _local.in_workflow_execution


@contextmanager
def workflow_logging_context(job_id) -> None:
    """Initialize the workflow logging context.

    Workflow executions are running as remote functions from
    WorkflowManagementActor. Without logging redirection, workflow
    inner execution logs will be pushed to the driver that initially
    created WorkflowManagementActor rather than the driver that
    actually submits the current workflow execution.
    We use this conext manager to re-configure the log files to send
    the logs to the correct driver, and to restore the log files once
    the execution is done.

    Args:
        job_id: The ID of the job that submits the workflow execution.
    """
    node = ray._private.worker._global_node
    original_out_file, original_err_file = node.get_log_file_handles(
        get_worker_log_file_name("WORKER")
    )
    out_file, err_file = node.get_log_file_handles(
        get_worker_log_file_name("WORKER", job_id)
    )
    try:
        configure_log_file(out_file, err_file)
        yield
    finally:
        configure_log_file(original_out_file, original_err_file)
