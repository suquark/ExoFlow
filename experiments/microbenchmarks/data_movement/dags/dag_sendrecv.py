# https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html
import logging
import time

import pendulum

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
    dag_id="sendrecv",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule_interval=None,
    catchup=False,
    tags=["sendrecv"],
)
def sendrecv():
    @task()
    def producer(**context):
        start_time = time.time()
        conf = context["dag_run"].conf
        return {
            "trigger_time": conf["trigger_time"],
            "start_time": start_time,
            "data": "A" * conf["size"],
        }

    @task()
    def consumer(input_dict):
        end_time = time.time()
        size = len(input_dict["data"])
        result = {
            "trigger_time": input_dict["trigger_time"],
            "start_time": input_dict["start_time"],
            "end_time": end_time,
            "size": size,
        }
        publish_metric(
            name="sendrecv_trigger_latency",
            value=result["start_time"] - result["trigger_time"],
            cat=f"size_{size}",
            unit="Seconds",
        )
        publish_metric(
            name="sendrecv_step_latency",
            value=result["end_time"] - result["start_time"],
            cat=f"size_{size}",
            unit="Seconds",
        )
        return result

    consumer(producer())


# NOTE: need to set "core.enable_xcom_pickling=True" in MWAA env.
#  AWS MWAA uses PostgresSQL, so XCOM limit is 1GB
# https://stackoverflow.com/questions/54545384/maximum-memory-size-for-an-xcom-in-airflow
dag = sendrecv()
