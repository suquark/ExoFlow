# https://docs.aws.amazon.com/mwaa/latest/userguide/samples-lambda.html
# https://airflow.apache.org/docs/apache-airflow/2.0.0/cli-and-env-variables-ref.html
import argparse
import json
import time

import boto3
import http.client
import base64
import ast
import shortuuid
import tqdm

mwaa_cli_command = "dags trigger"


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
            time.sleep(0.1)
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


def run_airflow(num_consumers: int, config: dict, n_repeats: int, n_warmups: int = 1):
    durations = []
    for _ in range(n_warmups + n_repeats):
        start = time.time()
        trigger_dag_and_wait(
            "MyAirflowEnvironment",
            f"spark_dag_{num_consumers}",
            config,
            verbose=False,
        )
        durations.append(time.time() - start)
    return durations[n_warmups:]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="benchmark data movement")
    parser.add_argument("host", help="The IP of the workflow server")
    parser.add_argument(
        "--port", type=int, help="The port of the workflow server", default=18080
    )
    args = parser.parse_args()

    N = 8
    config = {"host": args.host, "port": args.port}

    for n in tqdm.trange(1, N + 1, desc="airflow"):
        durations = run_airflow(n, config, n_repeats=5)
        with open(f"result/airflow_{n}.json", "w") as f:
            json.dump(durations, f)
