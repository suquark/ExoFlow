# https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html
import logging
import time
import pickle

import pendulum
import smart_open

from airflow.decorators import dag, task

logger = logging.getLogger("airflow.task")


def publish_metric(name: str, value: float, cat: str, unit="None"):
    # https://docs.aws.amazon.com/mwaa/latest/userguide/samples-custom-metrics.html
    from datetime import datetime

    import boto3

    client = boto3.client("cloudwatch")
    response = client.put_metric_data(
        Namespace="MWAA-Custom",
        MetricData=[
            {
                "MetricName": name,
                "Dimensions": [
                    {"Name": "Category", "Value": cat},
                ],
                "Timestamp": datetime.utcnow(),
                "Value": value,
                "Unit": unit,
                "StorageResolution": 1,
            },
        ],
    )
    return response


@dag(
    dag_id="sendrecv_s3",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule_interval=None,
    catchup=False,
    tags=["sendrecv"],
)
def sendrecv_s3():
    @task()
    def producer(**context):
        start_time = time.time()
        conf = context["dag_run"].conf
        payload = "A" * conf["size"]
        path = "s3://siyuan-airflow/payload"
        with smart_open.open(path, "wb") as f:
            pickle.dump(payload, f)
        return {
            "trigger_time": conf["trigger_time"],
            "start_time": start_time,
            "data": path,
        }

    @task()
    def consumer(input_dict):
        end_time = time.time()
        with smart_open.open(input_dict["data"], "rb") as f:
            data = pickle.load(f)
        size = len(data)
        result = {
            "trigger_time": input_dict["trigger_time"],
            "start_time": input_dict["start_time"],
            "end_time": end_time,
            "size": size,
        }
        publish_metric(
            name="sendrecv_s3_trigger_latency",
            value=result["start_time"] - result["trigger_time"],
            cat=f"size_{size}",
            unit="Seconds",
        )
        publish_metric(
            name="sendrecv_s3_step_latency",
            value=result["end_time"] - result["start_time"],
            cat=f"size_{size}",
            unit="Seconds",
        )
        return result

    consumer(producer())


dag = sendrecv_s3()
