from datetime import datetime
import os

from fastapi import FastAPI
import shortuuid

import ray
from exoflow.api import run_service_async

import config
import pipelines

app = FastAPI()

DEBUG = False
STORAGE_URL = (
    "s3://siyuan-workflow/microbenchmarks/latency/workflows/"
    f"{datetime.now().isoformat()}"
)


@app.on_event("startup")
async def startup_event():
    # os.environ["N_WORKFLOW_SHARDS"] = "32"
    # os.environ["N_WORKFLOW_WORKERS"] = "1"
    # os.environ["N_WORKFLOW_WORKER_THREADS"] = "16"
    os.environ["RAY_USAGE_STATS_ENABLED"] = "0"
    ray.init("local", storage=STORAGE_URL)
    pipelines.register_dags()


@app.get("/{workflow_id}")
async def gateway(workflow_id: str, size: int, trigger_time: float):
    if workflow_id == "ray":
        r = await pipelines.run_ray(size, trigger_time)
    else:
        persist_input = config.WORKFLOW_LAMBDA_PREFIX in workflow_id
        r = await run_service_async(
            workflow_id,
            shortuuid.uuid(),
            persist_input=persist_input,
            size=size,
            trigger_time=trigger_time,
        )
    if DEBUG:
        print(f"[{workflow_id}] {size} -> {r}")
    return r


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("server:app", host="0.0.0.0", port=8080)
