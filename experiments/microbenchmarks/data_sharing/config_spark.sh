#!/bin/bash

cat <<'EOT' >> /spark/conf/spark-defaults.conf
spark.executor.memory 4g
spark.executor.memoryOverhead 4g
spark.driver.memory 4g
spark.driver.memoryOverhead 4g
spark.python.worker.memory 4g
spark.driver.maxResultSize 4g
spark.network.timeout 2400
spark.executor.heartbeatInterval 1200
spark.rpc.message.maxSize 2000
EOT

cat <<'EOT' >> /spark/conf/spark-env.sh
#!/usr/bin/env bash
export SPARK_DAEMON_MEMORY=100g
source ~/anaconda3/bin/activate exoflow-dev
export PYSPARK_PYTHON=/home/ubuntu/anaconda3/envs/exoflow-dev/bin/python
export PYSPARK_DRIVER_PYTHON=/home/ubuntu/anaconda3/envs/exoflow-dev/bin/python
EOT

chmod +x /spark/conf/spark-env.sh
