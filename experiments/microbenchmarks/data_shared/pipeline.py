import os
import socket

from pyspark.sql import SparkSession, DataFrame
from pyspark.mllib.random import RandomRDDs

N_ROWS = 2 ** 20
N_COLS = 2 ** 10 // 8
FRACTION = 0.001

CHECKPOINT_DIR = "/exoflow/experiments/microbenchmarks/data_shared"
# CHECKPOINT_DIR = "s3://exoflow/microbenchmarks/checkpoint/"
CHECKPOINT_FILE = os.path.join(CHECKPOINT_DIR, "data.parquet")
CHECKPOINT_MAP = {"sync": True, "async": "async", "skip": False}


def get_local_addr() -> str:
    return socket.gethostbyname(socket.gethostname())


def connect(addr: str) -> SparkSession:
    python_path = "/home/ubuntu/anaconda3/envs/exoflow-dev/bin/python"
    os.environ["SPARK_HOME"] = "/spark"
    os.environ["PYSPARK_PYTHON"] = python_path
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_path
    spark = SparkSession.builder.master(f"spark://{addr}:17077").getOrCreate()
    # for item in spark.sparkContext.getConf().getAll():
    #     print(item)
    return spark


def generate_df(spark: SparkSession, n_rows: int = N_ROWS, n_cols: int = N_COLS):
    return (
        RandomRDDs.uniformVectorRDD(spark.sparkContext, n_rows, n_cols)
        .map(lambda a: a.tolist())
        .toDF()
    )


def consume_df(df: DataFrame, fraction: float = FRACTION, seed: int = 0) -> DataFrame:
    return df.sample(fraction=fraction, seed=seed)


def save_df(df: DataFrame):
    df.write.format("parquet").mode("overwrite").save(CHECKPOINT_FILE)


def load_df(spark: SparkSession):
    return spark.read.format("parquet").load(CHECKPOINT_FILE)


def generate_df_no_shared(addr: str):
    with connect(addr) as spark:
        df = generate_df(spark)
        save_df(df)


def consume_df_no_shared(addr: str, seed: int):
    with connect(addr) as spark:
        df = load_df(spark)
        consume_df(df, seed=seed).count()
