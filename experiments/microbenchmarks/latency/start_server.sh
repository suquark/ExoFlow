#!/bin/bash

ray stop --force &> /dev/null
pip install -r requirements.txt
sleep 5
python server.py
