## Spark version

spark-3.3.0-bin-hadoop3

## spark/conf/spark-defaults.conf

```
spark.executor.memory 4g
spark.executor.memoryOverhead 4g
spark.driver.memory 4g
spark.driver.memoryOverhead 4g
spark.python.worker.memory 4g
spark.driver.maxResultSize 4g
spark.network.timeout 2400
spark.executor.heartbeatInterval 1200
spark.rpc.message.maxSize 2000
```

## spark/conf/spark-env.sh

```bash
#!/usr/bin/env bash
export SPARK_DAEMON_MEMORY=100g
```
