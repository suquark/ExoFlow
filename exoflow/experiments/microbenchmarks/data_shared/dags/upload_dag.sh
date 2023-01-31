#!/bin/bash

# https://docs.aws.amazon.com/mwaa/latest/userguide/environment-class.html
# https://docs.aws.amazon.com/mwaa/latest/userguide/mwaa-create-role.html#mwaa-create-role-how-adding
# https://docs.aws.amazon.com/mwaa/latest/userguide/configuring-dag-folder.html
upload_dag () {
  aws s3 cp "$1" s3://siyuan-airflow/dags/"$1"
}

python render.py
for dag in dag_spark_*.py; do
  upload_dag $dag
done
aws s3 cp requirements.txt s3://siyuan-airflow/requirements.txt
