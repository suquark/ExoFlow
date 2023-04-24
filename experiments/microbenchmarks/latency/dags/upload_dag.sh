#!/bin/bash

# https://docs.aws.amazon.com/mwaa/latest/userguide/environment-class.html
# https://docs.aws.amazon.com/mwaa/latest/userguide/mwaa-create-role.html#mwaa-create-role-how-adding
# https://docs.aws.amazon.com/mwaa/latest/userguide/configuring-dag-folder.html
upload_dag () {
  aws s3 cp "$1" s3://exoflow-airflow/dags/"$1"
}
upload_dag dag_sendrecv.py
# upload_dag dag_sendrecv_s3.py
aws s3 cp requirements.txt s3://exoflow-airflow/requirements.txt
