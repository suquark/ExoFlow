from typing import Dict
import asyncio
import json
import time

import shortuuid

import ray
from exoflow.lambda_executor import AWSLambdaClient


async def method_1(n):
    responses = []
    client = AWSLambdaClient()

    for _ in range(n):
        input_wrapper = {
            "CallerName": "",
            "CallerId": "",
            "CallerStep": 0,
            "InstanceId": shortuuid.uuid(),
            "Input": {"Username": "user1", "Password": "2222"},
            "TxnId": "",
            "Instruction": "",
            "Async": False,
        }
        response = client.invoke("beldi-wf-dev-user", json.dumps(input_wrapper))
        responses.append(response)
    results = await asyncio.gather(*responses)
    return [json.loads(r) for r in results]


@ray.remote
def ray_invoke_lambda(name: str, payload: Dict):
    import boto3

    client = boto3.client("lambda", region_name="us-east-1")
    response = client.invoke(FunctionName=name, Payload=json.dumps(payload))
    r = response["Payload"].read()
    return json.loads(r)


def method_2():
    responses = []
    for _ in range(1000):
        input_wrapper = {
            "CallerName": "",
            "CallerId": "",
            "CallerStep": 0,
            "InstanceId": shortuuid.uuid(),
            "Input": {"Username": "user1", "Password": "2222"},
            "TxnId": "",
            "Instruction": "",
            "Async": False,
        }
        response = ray_invoke_lambda.remote("beldi-wf-dev-user", input_wrapper)
        responses.append(response)
    return ray.get(responses)


@ray.remote(max_concurrency=32)
class RayInvokeLambda:
    def __init__(self):
        import boto3

        self._client = boto3.client("lambda", region_name="us-east-1")

    def invoke(self, name: str, payload: Dict):
        response = self._client.invoke(FunctionName=name, Payload=json.dumps(payload))
        r = response["Payload"].read()
        return json.loads(r)


def method_3(n):
    actor = RayInvokeLambda.remote()
    responses = []
    for _ in range(n):
        input_wrapper = {
            "CallerName": "",
            "CallerId": "",
            "CallerStep": 0,
            "InstanceId": shortuuid.uuid(),
            "Input": {"Username": "user1", "Password": "2222"},
            "TxnId": "",
            "Instruction": "",
            "Async": False,
        }
        response = actor.invoke.remote("beldi-wf-dev-user", input_wrapper)
        responses.append(response)
    return ray.get(responses)


if __name__ == "__main__":
    # ray.init(num_cpus=32)
    for _ in range(100):
        start = time.time()
        result = asyncio.run(method_1(1))
        # result = method_3(1)
        print(time.time() - start)
        print(result)
    # return await response.read()
