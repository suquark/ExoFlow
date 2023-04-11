import time

import boto3


def main(event, context):
    payload = event
    s3 = boto3.resource("s3")
    response = s3.Object("siyuan-airflow", "lambda_payload").get()
    data = response["Body"].read()
    return {
        "size": len(data),
        "start_time": payload["start_time"],
        "end_time": time.time(),
    }
