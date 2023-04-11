#!/bin/bash
# This script benchmarks checkpoint for workflow + ephemeral tasks.

# python run.py --checkpoint=hybrid
python run.py --checkpoint=false
python run.py --checkpoint=async
python run.py --checkpoint=true
# python run.py --checkpoint=false --disable-ephemeral-tasks
