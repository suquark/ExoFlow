# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/stepfunctions.html
import json
import time

import boto3
import shortuuid


# Express Mode, Log Level = OFF
# 256 KB limit for AWS step function
# 6 MB for synchronous Lambda invocations
# https://aws.amazon.com/about-aws/whats-new/2020/09/aws-step-functions-increases-payload-size-to-256kb/


def invoke_express_stepfunction(name: str, input_dict: dict):
    client = boto3.client("stepfunctions", region_name="us-east-1")
    state_machines = client.list_state_machines()["stateMachines"]
    arn = None
    for s in state_machines:
        if s["name"] == name:
            arn = s["stateMachineArn"]
            break
    if arn is None:
        raise NameError(f"There is no step function state machine with name {name}")

    input_dict["trigger_time"] = time.time()
    response = client.start_sync_execution(
        stateMachineArn=arn,
        name=shortuuid.uuid(),
        input=json.dumps(input_dict),
    )
    if response["status"] != "SUCCEEDED":
        raise ValueError(response)
    return json.loads(response["output"])


def invoke_standard_stepfunction(name: str, input_dict: dict):
    client = boto3.client("stepfunctions", region_name="us-east-1")
    state_machines = client.list_state_machines()["stateMachines"]
    arn = None
    for s in state_machines:
        if s["name"] == name:
            arn = s["stateMachineArn"]
            break
    if arn is None:
        raise NameError(f"There is no step function state machine with name {name}")

    input_dict["trigger_time"] = time.time()
    response = client.start_execution(
        stateMachineArn=arn,
        name=shortuuid.uuid(),
        input=json.dumps(input_dict),
    )
    executionArn = response["executionArn"]
    while True:
        response = client.describe_execution(executionArn=executionArn)
        if response["status"] != "RUNNING":
            break
        time.sleep(1)

    if response["status"] != "SUCCEEDED":
        raise ValueError(response)
    return json.loads(response["output"])


if __name__ == "__main__":
    print(invoke_express_stepfunction("SendRecvStateMachine", {"size": 10000}))
    print(invoke_standard_stepfunction("SendRecvStateMachineStandard", {"size": 10000}))
