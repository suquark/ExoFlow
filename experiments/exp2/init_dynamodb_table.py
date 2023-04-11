# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.create_table
from ray.workflow.dynamodb import delete_table, create_table

if __name__ == "__main__":
    delete_table()
    create_table()
