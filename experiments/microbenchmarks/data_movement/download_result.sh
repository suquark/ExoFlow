#!/bin/bash

aws s3 sync --exact-timestamps s3://siyuan-workflow/microbenchmarks/data_movement/result result
