#!/bin/bash

rm -r result
git checkout -- result
aws s3 sync --exact-timestamps s3://siyuan-workflow/exp2/result result
