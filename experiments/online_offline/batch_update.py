# http://www.fengzhao.com/pubs/eurosys12_kineograph.pdf section5.1
import os
import socket
from typing import Iterable, Tuple
from operator import add as add_op

import pandas as pd
import pyspark
from pyspark import RDD
from pyspark.resultiterable import ResultIterable
from pyspark.sql.functions import (
    sum,
    col,
    count,
    mean,
    lit,
)
from pyspark.sql import DataFrame, SparkSession

import numpy as np
import ray

from common import P_RETWEET, DUMP_OUTPUT, DUMP_OUTPUT_PATH

# https://sparkbyexamples.com/pyspark/pyspark-flatmap-transformation/


def compute_contibutes(
    following: ResultIterable[int], influence: float
) -> Iterable[Tuple[int, float]]:
    """Calculates URL contributions to the rank of other URLs."""
    if influence is None:
        influence = 0.0
    n_following = len(following)
    contrib = (1 + P_RETWEET * influence) / n_following
    for t in following:
        yield t, contrib


# This is a hack for Spark to serialize functions
compute_contibutes.__name__ = "<???>"
compute_contibutes.__qualname__ = "<???>"


def tunk_rank_fast(graph: RDD) -> DataFrame:
    # Reference implementation:
    # https://github.com/apache/spark/blob/master/examples/src/main/python/pagerank.py

    following = graph.map(lambda x: (x[1], x[0])).groupByKey().cache()
    follower = graph.groupByKey()
    influence = follower.map(lambda neighbors: (neighbors[0], 0.0)).cache()
    N = influence.count()

    iteration = 0
    while True:
        iteration += 1
        old_influence = influence
        print(f">>> Tunk rank (iteration={iteration})")
        # Calculates URL contributions to the rank of other URLs.
        contribs = following.leftOuterJoin(influence).flatMap(
            lambda x: compute_contibutes(x[1][0], x[1][1])
        )
        # Re-calculates URL ranks based on neighbor contributions.
        influence = contribs.reduceByKey(add_op).cache()
        if iteration > 1:
            delta = (
                influence.join(old_influence)
                .map(lambda x: (1, abs(x[1][0] - x[1][1])))
                .reduceByKey(add_op)
                .collect()[0][1]
            ) / N
            print(f">>> delta = {delta}")
            if delta < 1e-4:
                break
        old_influence.unpersist()

    influence_sorted = influence.sortBy(lambda x: x[1], ascending=False)
    df = influence_sorted.toDF(["uid", "influence"]).toPandas()
    return df


def tunk_rank_fast_2(graph: DataFrame) -> DataFrame:
    # Reference implementation:
    # https://github.com/apache/spark/blob/master/examples/src/main/python/pagerank.py

    # TunkRank algorithm
    influence = (
        graph.select("a")
        .withColumnRenamed("a", "uid")
        .distinct()
        .withColumn("influence", lit(0.0))
    )
    following = graph.withColumnRenamed("b", "uid")
    n_following = following.groupby("uid").agg(count("a").alias("count"))
    ratio = following.join(n_following, on="uid", how="left").cache()

    iteration = 0
    while True:
        iteration += 1
        print(f">>> Tunk rank (iteration={iteration})")
        old_influence = influence.withColumnRenamed("influence", "old_influence")
        full_map = ratio.join(influence, on="uid", how="left").select(
            col("a").alias("uid"), "count", "influence"
        )
        contribs = full_map.select(
            "uid", ((1 + P_RETWEET * col("influence")) / col("count")).alias("contrib")
        )
        influence = contribs.groupby("uid").agg(sum("contrib").alias("influence"))
        if iteration > 1:
            delta = (
                influence.join(old_influence, on="uid")
                .select(((col("influence") - col("old_influence")) ** 2).alias("squ"))
                .agg(mean("squ").alias("mean_squ"))
            )
            if delta.first()["mean_squ"] < 0.001:
                break

    result = influence.sort(col("influence").desc()).toPandas()
    return result


@ray.remote
def compute_tunk_rank(state: "State", epoch: int):
    refs = []
    for partition in state.partitions:
        refs.extend(partition)
    arrays = ray.get(refs)
    del state, refs

    arrs = [arr for arr in arrays if arr is not None]
    if not arrs:
        return None
    arrs = np.concatenate(arrs)
    return compute_with_spark(arrs, epoch)


def compute_with_spark(arrs: np.ndarray, epoch: int):
    python_path = "/home/ubuntu/anaconda3/envs/ray_workflow/bin/python"
    os.environ["SPARK_HOME"] = "/spark"
    os.environ["PYSPARK_PYTHON"] = python_path
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_path
    os.system("./restart_spark.sh")

    # SparkSession.builder.appName("TunkRank").config().getOrCreate()
    # spark = (
    #     SparkSession.builder.master("local[*]")
    #     .appName("TunkRank")
    #     .config("spark.executor.memory", SPARK_MEMORY)
    #     .config("spark.driver.memory", SPARK_MEMORY)
    #     .config("spark.python.worker.memory", SPARK_MEMORY)
    #     .config("spark.driver.maxResultSize", SPARK_MEMORY)
    #     .config("spark.network.timeout", "2400")
    #     .config("spark.executor.heartbeatInterval", "1200")
    #     .getOrCreate()
    # )

    # cannot use localhost for spark
    addr = socket.gethostbyname(socket.gethostname())
    spark = SparkSession.builder.master(f"spark://{addr}:17077").getOrCreate()
    for item in spark.sparkContext.getConf().getAll():
        print(item)
    # number of visible nodes
    # assert spark._jsc.sc().getExecutorMemoryStatus().size() > 1
    df = pd.DataFrame(arrs, columns=["a", "b"])
    path = os.path.abspath(os.path.join(os.path.dirname(__file__), "table.csv"))
    df.to_csv(path, header=False, index=False)
    sdf = spark.read.csv(path)
    rdd = sdf.rdd.repartition(32).persist(pyspark.StorageLevel.MEMORY_ONLY)
    influence = tunk_rank_fast(rdd)
    spark.stop()
    if DUMP_OUTPUT:
        influence.to_csv(
            os.path.join(DUMP_OUTPUT_PATH, f"batch_update_{epoch}.csv"),
            header=False,
            index=False,
        )
    return influence
