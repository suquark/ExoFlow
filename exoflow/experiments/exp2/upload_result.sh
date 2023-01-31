#!/bin/bash

aws s3 rm --recursive s3://siyuan-workflow/exp2/result
aws s3 sync result s3://siyuan-workflow/exp2/result
