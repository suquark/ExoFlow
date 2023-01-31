from typing import Dict, Optional, Callable
import asyncio
import json
import threading
from concurrent.futures import ThreadPoolExecutor

import ray
from ray.workflow.common import WORKFLOW_OPTIONS

_client = None
_boto3_client_lock = threading.Lock()


class AWSLambdaClient:
    def __init__(self):
        import boto3
        from botocore.config import Config

        config = Config(retries={"max_attempts": 10, "mode": "standard"})
        self._client = boto3.client("lambda", region_name="us-east-1", config=config)
        self._thread_pool = ThreadPoolExecutor(32)

    async def invoke(self, name: str, payload: str):
        raw_future = self._thread_pool.submit(
            self._client.invoke, FunctionName=name, Payload=payload
        )
        future = asyncio.wrap_future(raw_future)
        response = await future
        payload_handle = response["Payload"]
        result = asyncio.wrap_future(self._thread_pool.submit(payload_handle.read))
        return await result


async def invoke_lambda_async(
    name: str,
    encoder: Optional[Callable] = None,
    decoder: Optional[Callable] = None,
    **_payloads,
):
    global _client

    # Abort boto3 client thread safety:
    # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/clients.html#caveats
    with _boto3_client_lock:
        if _client is None:
            _client = AWSLambdaClient()

    if decoder is not None:
        _payloads = decoder(**_payloads)
    r = await _client.invoke(name, json.dumps(_payloads))
    result: Dict = json.loads(r)
    if encoder is not None:
        result = encoder(**result)
    return result


@ray.remote
def _ray_invoke_lambda(
    name: str,
    encoder: Optional[Callable] = None,
    decoder: Optional[Callable] = None,
    **_payloads,
):
    raise RuntimeError(
        "This remote function is only a placeholder and should not be called directly."
    )


class RayInvokeLambdaWrapper:
    def __init__(self, remote_func=None):
        self._remote_func = remote_func
        if self._remote_func is None:
            self._remote_func = _ray_invoke_lambda

    def options(self, **kwargs):
        return RayInvokeLambdaWrapper(self._remote_func.options(**kwargs))

    def bind(
        self,
        name: str,
        encoder: Optional[Callable] = None,
        decoder: Optional[Callable] = None,
        **_payloads,
    ):
        node = self._remote_func.bind(
            name, encoder=encoder, decoder=decoder, **_payloads
        )
        node._body = invoke_lambda_async
        node._bound_options.setdefault("_metadata", {}).setdefault(
            WORKFLOW_OPTIONS, {}
        ).setdefault("name", name)
        return node


ray_invoke_lambda = RayInvokeLambdaWrapper()
