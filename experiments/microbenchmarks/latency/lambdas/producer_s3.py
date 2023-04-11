import time
import boto3


def main(event, context):
    s3 = boto3.resource("s3")
    s3.Bucket("exoflow-airflow").put_object(
        Key="lambda_payload", Body=b"A" * event["size"]
    )
    return {"start_time": time.time()}
