## Spark version

spark-3.3.0-bin-hadoop3

## spark/conf/spark-defaults.conf

```
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
```

## spark/conf/spark-env.sh

```bash
#!/usr/bin/env bash
export SPARK_DAEMON_MEMORY=100g
```
