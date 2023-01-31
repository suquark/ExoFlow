#!/bin/bash
# https://spark.apache.org/docs/latest/spark-standalone.html
export SPARK_HOME=/spark

WORKER_NODES=$(ray get-worker-ips ~/ray_bootstrap_config.yaml | grep -oE "\b([0-9]{1,3}\.){3}[0-9]{1,3}\b")

for host in $WORKER_NODES; do
  ssh $host /spark/sbin/stop-worker.sh
done
/spark/sbin/stop-master.sh

sleep 5

sudo kill -9 $(sudo lsof -t -i:17077) &> /dev/null

/spark/sbin/start-master.sh --port 17077
for host in $WORKER_NODES; do
  ssh $host /spark/sbin/start-worker.sh $(hostname -i):17077
done

sleep 5
