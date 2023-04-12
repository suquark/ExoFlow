#!/bin/bash

ray stop --force
gunicorn "server:app" --worker-class uvicorn.workers.UvicornWorker --workers 8 --bind 0.0.0.0:8080
