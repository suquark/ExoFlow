#!/bin/bash

rm -r result
git checkout -- result
aws s3 sync --exact-timestamps s3://exoflow/stateful_serverless/result result
