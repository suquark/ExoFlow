from typing import Any, List, Optional
import re
import unicodedata

import ray
from exoflow.common import WORKFLOW_OPTIONS

from ray.dag import DAGNode, FunctionNode, InputNode
from ray.dag.input_node import InputAttributeNode, DAGInputData
from ray import cloudpickle
from ray._private import signature
from ray._private.client_mode_hook import client_mode_should_convert
from exoflow import serialization_context
from exoflow.common import (
    TaskType,
    WorkflowTaskRuntimeOptions,
    WorkflowRef,
    validate_user_metadata,
    WorkflowDAGInput,
)
from exoflow import workflow_context
from exoflow.workflow_state import WorkflowExecutionState, Task


def get_module(f):
    return f.__module__ if hasattr(f, "__module__") else "__anonymous_module__"


def get_qualname(f):
    return f.__qualname__ if hasattr(f, "__qualname__") else "__anonymous_func__"


def slugify(value: str, allow_unicode=False) -> str:
    """Adopted from
    https://github.com/django/django/blob/master/django/utils/text.py
    Convert to ASCII if 'allow_unicode' is False. Convert spaces or repeated
    dashes to single dashes. Remove characters that aren't alphanumerics,
    underscores, dots or hyphens. Also strip leading and
    trailing whitespace.
    """
    if allow_unicode:
        value = unicodedata.normalize("NFKC", value)
    else:
        value = (
            unicodedata.normalize("NFKD", value)
            .encode("ascii", "ignore")
            .decode("ascii")
        )
    value = re.sub(r"[^\w.\-]", "", value).strip()
    return re.sub(r"[-\s]+", "-", value)


class _DelayedDeserialization:
    def __init__(self, serialized: bytes):
        self._serialized = serialized

    def __reduce__(self):
        return cloudpickle.loads, (self._serialized,)


class _SerializationContextPreservingWrapper:
    """This class is a workaround for preserving serialization context
    in client mode."""

    def __init__(self, obj: Any):
        self._serialized = cloudpickle.dumps(obj)

    def __reduce__(self):
        # This delays the deserialization to the actual worker
        # instead of the Ray client server.
        return _DelayedDeserialization, (self._serialized,)


def workflow_state_from_dag(
    dag_node: DAGNode,
    input_context: Optional[DAGInputData],
    workflow_id: str,
    ref_to_task_mapping: Optional[dict] = None,
):
    """
    Transform a Ray DAG to a workflow. Map FunctionNode to workflow task with
    the workflow decorator.

    Args:
        dag_node: The DAG to be converted to a workflow.
        input_context: The input data that wraps varibles for the input node of the DAG.
        workflow_id: The ID of the workflow.
        ref_to_task_mapping: The dict associates the output object refs to
            input workflows.
    """
    if not isinstance(dag_node, FunctionNode):
        raise TypeError("Currently workflow does not support classes as DAG inputs.")
    if ref_to_task_mapping is None:
        ref_to_task_mapping = {}
    if input_context is None:
        input_context = DAGInputData()
    state = WorkflowExecutionState()

    # TODO(suquark): remove this cyclic importing later by changing the way of
    # task ID assignment.
    from exoflow.workflow_access import get_management_actor

    mgr = get_management_actor()
    context = workflow_context.get_workflow_task_context()
    owner_task_id = context.task_id if context is not None else ""

    def _node_visitor(node: Any) -> Any:
        if isinstance(node, FunctionNode):
            bound_options = node._bound_options.copy()
            num_returns = bound_options.get("num_returns", 1)
            if num_returns is None:  # ray could use `None` as default value
                num_returns = 1
            if num_returns > 1:
                raise ValueError("Workflow task can only have one return.")

            workflow_options = bound_options.pop("_metadata", {}).get(
                WORKFLOW_OPTIONS, {}
            )

            # If checkpoint option is not specified, inherit checkpoint
            # options from context (i.e. checkpoint options of the outer
            # task). If it is still not specified, it's True by default.
            checkpoint = workflow_options.get("checkpoint", None)
            if checkpoint is None:
                checkpoint = context.checkpoint if context is not None else True
            # When it returns a nested workflow, catch_exception
            # should be passed recursively.
            catch_exceptions = workflow_options.get("catch_exceptions", None)
            if catch_exceptions is None:
                if node.get_stable_uuid() == dag_node.get_stable_uuid():
                    # 'catch_exception' context should be passed down to
                    # its direct continuation task.
                    # In this case, the direct continuation is the output node.
                    catch_exceptions = (
                        context.catch_exceptions if context is not None else False
                    )
                else:
                    catch_exceptions = False

            # We do not need to check the validness of bound options, because
            # Ray option has already checked them for us.
            max_retries = bound_options.get("max_retries", 3)
            retry_exceptions = bound_options.get("retry_exceptions", False)

            task_options = WorkflowTaskRuntimeOptions(
                task_type=TaskType.FUNCTION,
                catch_exceptions=catch_exceptions,
                retry_exceptions=retry_exceptions,
                max_retries=max_retries,
                checkpoint=checkpoint,
                isolation=workflow_options.get("isolation", False),
                wait_until_committed=workflow_options.get("wait_until_committed", []),
                ray_options=bound_options,
            )

            workflow_refs: List[WorkflowRef] = []
            object_refs: List[ray.ObjectRef] = []
            actors: List[ray.actor.ActorHandle] = []
            dag_inputs: List[WorkflowDAGInput] = []
            with serialization_context.workflow_args_serialization_context(
                workflow_refs, object_refs, actors, dag_inputs
            ):
                _func_signature = signature.extract_signature(node._body)
                flattened_args = signature.flatten_args(
                    _func_signature, node._bound_args, node._bound_kwargs
                )
                # NOTE: When calling 'ray.put', we trigger python object
                # serialization. Under our serialization context,
                # Workflows are separated from the arguments,
                # leaving a placeholder object with all other python objects.
                # Then we put the placeholder object to object store,
                # so it won't be mutated later. This guarantees correct
                # semantics. See "tests/test_variable_mutable.py" as
                # an example.
                if client_mode_should_convert(auto_init=False):
                    # Handle client mode. The Ray client would serialize and
                    # then deserialize objects in the Ray client server. When
                    # the object is being deserialized, the serialization context
                    # will be missing, resulting in failures. Here we protect the
                    # object from deserialization in client server, and we make sure
                    # the 'real' deserialization happens under the serialization
                    # context later.
                    flattened_args = _SerializationContextPreservingWrapper(
                        flattened_args
                    )
                    input_placeholder: ray.ObjectRef = ray.put(flattened_args)
                else:
                    input_placeholder: ray.ObjectRef = ray.put(
                        flattened_args, _owner=mgr
                    )

            # assert not actors, "not support actors as an input of workflow task."
            name = workflow_options.get("name")
            if name is None:
                name = f"{get_module(node._body)}.{slugify(get_qualname(node._body))}"
            task_id = ray.get(mgr.gen_task_id.remote(workflow_id, name))

            def _get_task_ids(wf_refs):
                return [s.task_id for s in wf_refs]

            state.add_data_dependencies(task_id, _get_task_ids(workflow_refs))
            state.add_dependencies(
                task_id,
                _get_task_ids(workflow_refs + node._bound_upstream_nodes),
            )

            state.task_input_args[task_id] = input_placeholder
            state.task_dag_input_keys[task_id] = {k.key for k in dag_inputs}
            state.object_ref_tracker.set_task_inputs(
                task_id,
                owner_task_id,
                object_refs,
                ref_to_task_mapping,
            )
            state.actor_tracker.set_task_inputs(task_id, owner_task_id, actors)
            user_metadata = workflow_options.pop("metadata", {})
            validate_user_metadata(user_metadata)
            state.tasks[task_id] = Task(
                name=name,
                options=task_options,
                user_metadata=user_metadata,
                func_body=cloudpickle.dumps(node._body),
            )
            return WorkflowRef(task_id)

        if isinstance(node, InputAttributeNode):
            try:
                return node._execute_impl()  # get data from input node
            except (IndexError, KeyError):
                return WorkflowDAGInput(node._key)
        if isinstance(node, InputNode):
            return input_context  # replace input node with input data
        if not isinstance(node, DAGNode):
            return node  # return normal objects
        raise TypeError(f"Unsupported DAG node: {node}")

    output_workflow_ref = dag_node.apply_recursive(_node_visitor)

    from ray.dag.py_obj_scanner import _instances

    _instances.clear()  # TODO(suquark): this is a temporary fix for memory leak
    state.output_task_id = output_workflow_ref.task_id
    return state
