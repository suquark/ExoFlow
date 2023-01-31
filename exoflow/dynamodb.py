# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.create_table
import asyncio
import json
import threading

from concurrent.futures import ThreadPoolExecutor


TABLE_NAME = "ExoFlowInputDict"

_client = None
_boto3_client_lock = threading.Lock()


def _get_client():
    global _client
    with _boto3_client_lock:
        if _client is None:
            _client = AWSDynamoDBClient()
    return _client


class AWSDynamoDBClient:
    def __init__(self):
        import boto3

        self._client = boto3.client("dynamodb", region_name="us-east-1")
        self._thread_pool = ThreadPoolExecutor(32)

    def _save_input_dict(self, workflow_id: str, input_dict: dict) -> dict:
        response = self._client.put_item(
            TableName=TABLE_NAME,
            Item={
                "workflow_id": {"S": workflow_id},
                "input_dict": {"S": json.dumps(input_dict)},
            },
        )
        return response

    def _load_input_dict(self, workflow_id: str) -> dict:
        response = self._client.get_item(
            TableName=TABLE_NAME, Key={"workflow_id": {"S": workflow_id}}
        )
        return response["Item"]

    async def save_input_dict(self, workflow_id: str, input_dict: dict) -> dict:
        future = self._thread_pool.submit(
            self._save_input_dict, workflow_id, input_dict
        )
        return await asyncio.wrap_future(future)

    async def load_input_dict(self, workflow_id: str) -> dict:
        future = self._thread_pool.submit(self._load_input_dict, workflow_id)
        return await asyncio.wrap_future(future)


def create_table():
    import boto3

    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    table_names = [table.name for table in dynamodb.tables.all()]
    if TABLE_NAME in table_names:
        return

    table = dynamodb.create_table(
        TableName=TABLE_NAME,
        KeySchema=[
            {"AttributeName": "workflow_id", "KeyType": "HASH"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "workflow_id", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )

    table.wait_until_exists()


def delete_table():
    import boto3

    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    table_names = [table.name for table in dynamodb.tables.all()]
    if TABLE_NAME not in table_names:
        return
    table = dynamodb.Table(TABLE_NAME)
    table.delete()
    table.wait_until_not_exists()


async def save_input_dict(workflow_id: str, input_dict: dict) -> dict:
    return await _get_client().save_input_dict(workflow_id, input_dict)


async def load_input_dict(workflow_id: str) -> dict:
    return await _get_client().load_input_dict(workflow_id)
