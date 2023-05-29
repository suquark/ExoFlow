from typing import Dict, Any

import time

import ray
from ray.experimental.internal_kv import _internal_kv_get
from ray._private.worker import global_worker
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from exoflow import common


class CacheWithExpiration:
    def __init__(self, expiration_time: float = 1.0) -> None:
        self._cache: Dict[str, Any] = {}
        self._creation_time: Dict[str, float] = {}
        self._expiration_time: float = expiration_time
    
    def get(self, name: str) -> Any:
        now = time.time()
        if name in self._cache and now - self._creation_time[name] < self._expiration_time:
            return self._cache[name]

        value = self._get_internal(name)
        self._creation_time[name] = now
        self._cache[name] = value
        return value

    def _get_internal(self, name: str) -> Any:
        raise NotImplementedError()


class NamedActorCache(CacheWithExpiration):
    def _get_internal(self, name: str):
        return ray.get_actor(name, namespace=common.MANAGEMENT_ACTOR_NAMESPACE)


class KVCache(CacheWithExpiration):
    def _get_internal(self, name: str):
        return _internal_kv_get(name, namespace="workflow")


def local_binding_scheduling_strategy():
    """Get a scheduling strategy that binds tasks to the current node."""
    node_id = global_worker.core_worker.get_current_node_id().hex()
    scheduling_strategy = NodeAffinitySchedulingStrategy(node_id, soft=False)
    return scheduling_strategy
