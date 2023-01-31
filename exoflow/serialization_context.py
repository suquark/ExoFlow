import contextlib
from typing import List, Any, Dict, Union, Tuple

import ray
from ray import ObjectRef
from ray.actor import ActorHandle
from ray.util.serialization import register_serializer, deregister_serializer
from ray.workflow.common import WorkflowRef, WorkflowDAGInput


def _resolve_workflow_refs(index: Tuple[int, int]) -> Any:
    raise ValueError("There is no context for resolving workflow refs.")


@contextlib.contextmanager
def _set_resolver(resolver):
    global _resolve_workflow_refs
    _resolve_workflow_refs_bak = _resolve_workflow_refs

    try:
        _resolve_workflow_refs = resolver
        yield
    finally:
        _resolve_workflow_refs = _resolve_workflow_refs_bak


def _resolve_task_output(output_ref: ObjectRef, object_refs, actors):
    from ray.workflow.workflow_context import get_workflow_task_context

    context = get_workflow_task_context()
    if context is not None:
        # only consider object refs, since actors require checkpointing anyway
        context.refs_hex_in_workflow_refs.update(r.hex() for r in object_refs)

    with workflow_args_resolving_context([], object_refs, actors, {}):
        return ray.get(output_ref)


class _TaskOutputDecoder:
    def __init__(self, output_ref: ObjectRef, object_refs, actors):
        self._output_ref = output_ref
        self._object_refs = object_refs
        self._actors = actors

    def __reduce__(self):
        return _resolve_task_output, (self._output_ref, self._object_refs, self._actors)


def _wrap_task_output(output_ref: ObjectRef, object_refs, actors):
    return ray.put(_TaskOutputDecoder(output_ref, object_refs, actors))


class _TaskOutputEncoder:
    def __init__(self, output_ref: ObjectRef, object_refs, actors):
        self._output_ref = output_ref
        self._object_refs = object_refs
        self._actors = actors

    def __reduce__(self):
        return _wrap_task_output, (self._output_ref, self._object_refs, self._actors)


@contextlib.contextmanager
def workflow_args_serialization_context(
    workflow_refs: List[WorkflowRef],
    object_refs: List[ObjectRef],
    actors: List[ActorHandle],
    input_placeholders: List[WorkflowDAGInput],
) -> None:
    """
    This serialization context reduces workflow input arguments to three
    parts:

    1. A workflow input placeholder. It is an object without 'Workflow' and
       'ObjectRef' object. They are replaced with integer indices. During
       deserialization, we can refill the placeholder with a list of
       'Workflow' and a list of 'ObjectRef'. This provides us great
       flexibility, for example, during recovery we can plug an alternative
       list of 'Workflow' and 'ObjectRef', since we lose the original ones.
    2. A list of 'Workflow'. There is no duplication in it.
    3. A list of 'ObjectRef'. There is no duplication in it.

    We do not allow duplication because in the arguments duplicated workflows
    and object refs are shared by reference. So when deserialized, we also
    want them to be shared by reference. See
    "tests/test_object_deref.py:deref_shared" as an example.

    The deduplication works like this:
        Inputs: [A B A B C C A]
        Output List: [A B C]
        Index in placeholder: [0 1 0 1 2 2 0]

    Args:
        workflow_refs: Output list of workflows or references to workflows.
    """
    deduplicator: Dict[
        Union[WorkflowRef, ObjectRef, ActorHandle, WorkflowDAGInput],
        Tuple[int, int],
    ] = {}

    def serializer(w):
        if w in deduplicator:
            return deduplicator[w]
        if isinstance(w, WorkflowRef):
            # The ref should be resolved by the workflow management actor
            # when treated as the input of a workflow, so we remove the ref here.
            w.ref = None
            tag = (0, len(workflow_refs))
            workflow_refs.append(w)
        elif isinstance(w, ObjectRef):
            tag = (1, len(object_refs))
            object_refs.append(w)
        elif isinstance(w, ActorHandle):
            tag = (2, len(actors))
            actors.append(w)
        elif isinstance(w, WorkflowDAGInput):
            tag = (3, w.key)
            input_placeholders.append(w)
        else:
            assert False
        deduplicator[w] = tag
        return tag

    import ray.cloudpickle

    _origin = {
        ObjectRef: ray.cloudpickle.CloudPickler.dispatch[ObjectRef],
        ActorHandle: ray.cloudpickle.CloudPickler.dispatch[ActorHandle],
    }

    try:
        register_serializer(
            WorkflowRef,
            serializer=serializer,
            deserializer=_resolve_workflow_refs,
        )
        register_serializer(
            ObjectRef,
            serializer=serializer,
            deserializer=_resolve_workflow_refs,
        )
        register_serializer(
            ActorHandle,
            serializer=serializer,
            deserializer=_resolve_workflow_refs,
        )
        register_serializer(
            WorkflowDAGInput,
            serializer=serializer,
            deserializer=_resolve_workflow_refs,
        )
        yield
    finally:
        ray.cloudpickle.CloudPickler.dispatch.update(_origin)
        # we do not want to serialize Workflow objects in other places.
        deregister_serializer(WorkflowRef)
        deregister_serializer(WorkflowDAGInput)


@contextlib.contextmanager
def workflow_args_resolving_context(
    workflow_ref_mapping: List[Any],
    object_refs: List[Any],
    actors: List[ActorHandle],
    input_dict: Dict,
) -> None:
    """
    This context resolves workflows and object refs inside workflow
    arguments into correct values.

    Args:
        workflow_ref_mapping: List of workflow refs.
    """

    def _resolver(x):
        t, i = x
        if t == 0:
            return workflow_ref_mapping[i]
        elif t == 1:
            return object_refs[i]
        elif t == 2:
            return actors[i]
        elif t == 3:
            return input_dict[i]
        else:
            assert False

    with _set_resolver(_resolver):
        yield


class _KeepWorkflowRefs:
    def __init__(self, index: int):
        self._index = index

    def __reduce__(self):
        return _resolve_workflow_refs, (self._index,)


@contextlib.contextmanager
def workflow_args_keeping_context() -> None:
    """
    This context only read workflow arguments. Workflows inside
    are untouched and can be serialized again properly.
    """
    # we must capture the old functions to prevent self-referencing.
    with _set_resolver(_KeepWorkflowRefs):
        yield


def _load_buffers(serialized: bytes, buffers: List):
    from ray.cloudpickle import loads

    return loads(serialized, buffers=buffers)


class BufferEncoder:
    def __init__(self, serialized: bytes, buffers: List):
        self._serialized = serialized
        self._buffers = buffers

    def __reduce__(self):
        return _load_buffers, (self._serialized, self._buffers)
