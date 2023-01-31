# https://docs.aws.amazon.com/mwaa/latest/userguide/samples-lambda.html
# https://airflow.apache.org/docs/apache-airflow/2.0.0/cli-and-env-variables-ref.html
import json
import time
from datetime import datetime, timedelta

import boto3
import http.client
import base64
import ast
import shortuuid

mwaa_cli_command = "dags trigger"


# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/cloudwatch.html#CloudWatch.Client.get_metric_data
# https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/Statistics-definitions.html
def get_cloudwatch_data(name: str, start_time: datetime, size):
    cloudwatch = boto3.resource("cloudwatch", region_name="us-east-1")
    metric = cloudwatch.Metric("MWAA-Custom", name)
    end_time = datetime.utcnow()
    response = metric.get_statistics(
        # NOTE: 'Dimensions' is required to get any data
        Dimensions=[{"Name": "Category", "Value": f"size_{size}"}],
        Statistics=["Average"],
        StartTime=start_time,
        EndTime=end_time + timedelta(minutes=1),
        Period=1,
        Unit="Seconds",
    )
    points = response["Datapoints"]
    points.sort(key=lambda x: x["Timestamp"])
    return [p["Average"] for p in points]


def trigger_dag(mwaa_env_name: str, dag_name: str, config: dict, run_id: str):
    client = boto3.client("mwaa", region_name="us-east-1")
    # get web token
    mwaa_cli_token = client.create_cli_token(Name=mwaa_env_name)

    conn = http.client.HTTPSConnection(mwaa_cli_token["WebServerHostname"])
    config["trigger_time"] = time.time()

    payload = f"{mwaa_cli_command} {dag_name} -r {run_id} --conf '{json.dumps(config)}'"
    headers = {
        "Authorization": "Bearer " + mwaa_cli_token["CliToken"],
        "Content-Type": "text/plain",
    }
    conn.request("POST", "/aws_mwaa/cli", payload, headers)
    res = conn.getresponse()
    data = res.read()
    dict_str = data.decode("UTF-8")
    mydata = ast.literal_eval(dict_str)
    return base64.b64decode(mydata["stdout"])


def _watch_dag_run(mwaa_env_name: str, dag_name: str, run_id: str):
    client = boto3.client("mwaa", region_name="us-east-1")
    # get web token
    mwaa_cli_token = client.create_cli_token(Name=mwaa_env_name)

    conn = http.client.HTTPSConnection(mwaa_cli_token["WebServerHostname"])

    payload = f"tasks states-for-dag-run {dag_name} {run_id}"

    headers = {
        "Authorization": "Bearer " + mwaa_cli_token["CliToken"],
        "Content-Type": "text/plain",
    }
    conn.request("POST", "/aws_mwaa/cli", payload, headers)
    res = conn.getresponse()
    data = res.read()
    dict_str = data.decode("UTF-8")
    mydata = ast.literal_eval(dict_str)
    return base64.b64decode(mydata["stdout"]).decode("latin1")


def watch_dag_run(
    mwaa_env_name: str, dag_name: str, run_id: str, verbose: bool = False
):
    while True:
        output = _watch_dag_run(mwaa_env_name, dag_name, run_id=run_id)
        if (
            "queued" in output
            or "None" in output
            or "running" in output
            or "scheduled" in output
        ):
            if verbose:
                print(output)
            time.sleep(1)
        else:
            if verbose:
                print(output)
            break


def trigger_dag_and_wait(
    mwaa_env_name: str, dag_name: str, config: dict, verbose: bool = False
):
    run_id = shortuuid.uuid()
    trigger_dag(mwaa_env_name, dag_name, config, run_id=run_id)
    watch_dag_run(mwaa_env_name, dag_name, run_id=run_id, verbose=verbose)


if __name__ == "__main__":
    for i in range(10):
        trigger_dag_and_wait(
            "MyAirflowEnvironment", "sendrecv", {"size": 10000}, verbose=True
        )
        trigger_dag_and_wait(
            "MyAirflowEnvironment", "sendrecv_s3", {"size": 10000}, verbose=True
        )
