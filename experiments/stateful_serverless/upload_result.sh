#!/bin/bash

aws s3 rm --recursive s3://siyuan-workflow/stateful_serverless/result
aws s3 sync result s3://siyuan-workflow/stateful_serverless/result
