#!/bin/bash

aws s3 rm --recursive s3://exoflow/stateful_serverless/result
aws s3 sync result s3://exoflow/stateful_serverless/result
