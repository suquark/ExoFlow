#!/bin/bash

cat <<'EOT' >> /spark/conf/spark-defaults.conf
spark.executor.memory 100g
spark.executor.memoryOverhead 50g
spark.driver.memory 100g
spark.driver.memoryOverhead 50g
spark.python.worker.memory 100g
spark.driver.maxResultSize 100g
spark.network.timeout 2400
spark.executor.heartbeatInterval 1200
spark.rpc.message.maxSize 2000
spark.sql.execution.arrow.enabled True
spark.sql.execution.arrow.pyspark.fallback.enabled False
EOT

cat <<'EOT' >> /spark/conf/spark-defaults.conf
#!/usr/bin/env bash
export SPARK_DAEMON_MEMORY=100g
source ~/anaconda3/bin/activate exoflow-dev
export PYSPARK_PYTHON=/home/ubuntu/anaconda3/envs/exoflow-dev/bin/python
export PYSPARK_DRIVER_PYTHON=/home/ubuntu/anaconda3/envs/exoflow-dev/bin/python
EOT
