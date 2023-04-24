#!/bin/bash

aws s3 sync --exact-timestamps s3://exoflow/microbenchmarks/throughput/result result
