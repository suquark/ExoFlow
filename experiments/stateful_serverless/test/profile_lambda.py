import os
import time

import ray
import exoflow
from exoflow.api import register_service, run_service_async
from ray.dag import InputNode
import shortuuid
from exoflow.lambda_executor import ray_invoke_lambda

if __name__ == "__main__":
    import shutil

    os.environ["EXOFLOW_N_CONTROLLERS"] = "4"
    os.environ["EXOFLOW_N_EXECUTORS"] = "8"

    shutil.rmtree("/tmp/ray/workflow", ignore_errors=True)
    ray.init(num_cpus=4, storage="/tmp/ray/workflow")
    exoflow.init()
    workflows = []

    with InputNode() as dag_input:
        c = ray_invoke_lambda.bind("beldi-wf-dev-user", dag_input.input_dict)
        register_service(c, workflow_id="service")

    for i in range(10):
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
        workflows.append(
            run_service_async("service", str(i) + "_pre", input_dict=input_wrapper)
        )
    ray.get(workflows)

    from exoflow.workflow_access import get_management_actor

    mgr = get_management_actor()
    ray.get(mgr._start_profile.remote())
    start = time.time()
    for i in range(1000):
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
        workflows.append(run_service_async("service", str(i), input_dict=input_wrapper))
    ray.get(workflows)
    ray.get(mgr._stop_profile.remote("profile.pstats"))
    end = time.time() - start

    print("\n" * 10)
    time.sleep(5)
    print(end)

    workflows.clear()
    print("\n" * 10)
    for i in range(10):
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
        start = time.time()
        ray.get(run_service_async("service", f"aaa_{i}", input_dict=input_wrapper))
        print(time.time() - start)
