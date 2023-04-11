#!/bin/bash

ray stop --force &> /dev/null
sleep 5
python server.py
