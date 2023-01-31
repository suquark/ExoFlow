#!/bin/bash
# https://spark.apache.org/docs/latest/spark-standalone.html
export SPARK_HOME=/spark
export PYSPARK_PYTHON=$(which python)
export PYSPARK_DRIVER_PYTHON=$(which python)

/spark/sbin/stop-worker.sh
/spark/sbin/stop-master.sh

sleep 5

sudo kill -9 $(sudo lsof -t -i:17077) &> /dev/null

/spark/sbin/start-master.sh --port 17077
/spark/sbin/start-worker.sh $(hostname -i):17077

sleep 5
