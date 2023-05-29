import time
from typing import Dict

from datetime import datetime
import os
import threading

from fastapi import FastAPI
from pydantic import BaseModel
from starlette.middleware.base import BaseHTTPMiddleware

import ray
from exoflow.api import run_service_async
from exoflow.workflow_access import get_management_actor

import pipelines

app = FastAPI()

DEBUG = False
worker_id = os.getpid()
STORAGE_URL = f"/exoflow/stateful_serverless-workflows/{datetime.now().isoformat()}"
result_dir = "/exoflow/experiments/stateful_serverless/result/temp"
os.makedirs(result_dir, exist_ok=True)


def _dump(s: "LatencyStats"):
    os.makedirs("result", exist_ok=True)
    while True:
        time.sleep(10)
        if s.durations:
            durations = s.durations
            s.durations = []
            ds = [f"{a}, {b}" for a, b in durations]
            ds.append("")
            try:
                with open(f"{result_dir}/hotel-worker-metrics-{worker_id}.csv", "a") as f:
                    f.write("\n".join(ds))
            except Exception:
                pass


class LatencyStats:
    def __init__(self):
        self.durations = []
        self._thread = threading.Thread(target=_dump, args=(self,))
        self._thread.start()

    async def __call__(self, request, call_next):
        # do something with the request object
        start = time.time()
        # process the request and get the response
        response = await call_next(request)
        self.durations.append((start, time.time() - start))
        return response


# https://stackoverflow.com/questions/71525132/how-to-write-a-custom-fastapi-middleware-class
metrics_middleware = LatencyStats()
app.add_middleware(BaseHTTPMiddleware, dispatch=metrics_middleware)


@app.on_event("startup")
async def startup_event():
    os.environ["EXOFLOW_N_CONTROLLERS"] = "32"
    os.environ["EXOFLOW_N_EXECUTORS"] = "1"
    os.environ["N_WORKFLOW_WORKER_THREADS"] = "16"
    os.environ["RAY_USAGE_STATS_ENABLED"] = "0"
    ray.init("local", storage=STORAGE_URL)
    pipelines.register_dags(worker_id)


class RPCInput(BaseModel):
    Function: str
    Input: Dict


class Request(BaseModel):
    InstanceId: str
    CallerName: str
    Async: bool
    Input: RPCInput


@app.get("/gateway")
async def gateway(req: Request):
    instance_id = req.InstanceId
    r = await run_service_async(
        f"{req.Input.Function}_{worker_id}",
        instance_id,
        persist_input=True,
        instance_id=instance_id,
        **req.Input.Input,
    )
    if DEBUG:
        print(f"[{req.Input.Function}] {req.Input.Input} -> {r}")
    return r


@app.get("/overhead/{req}")
async def overhead(req: str):
    if req == "get_single_actor":
        get_management_actor(0)
    elif req == "get_random_actor":
        get_management_actor(None)
    elif req == "actor_call":
        actor = get_management_actor(None)
        actor.ready.remote()
    elif req == "actor_call_block":
        actor = get_management_actor(None)
        await actor.ready.remote()
    elif req == "idle":
        pass
    else:
        assert False
    return "ok"
