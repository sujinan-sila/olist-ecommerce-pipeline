"""Spark tuning profile for the Olist Dataproc cluster.

2 workers x e2-standard-4 (4 vCPU / 16 GB) -> 8 cores / 32 GB for executors.
"""
from pyspark.sql import SparkSession

SPARK_TUNING = {
    # one executor per worker: 6 GB + ~10% overhead fits YARN's ~12 GB per node
    "spark.executor.memory": "6g",
    "spark.executor.cores": "4",
    "spark.executor.instances": "2",
    # Dataproc enables this by default; it ignores instances and resizes mid-run
    "spark.dynamicAllocation.enabled": "false",
    "spark.driver.memory": "4g",
    "spark.driver.maxResultSize": "2g",
    # 8 cores x 8 for join-heavy stages; AQE coalesces small partitions at runtime
    "spark.sql.shuffle.partitions": "64",
    "spark.default.parallelism": "64",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.sql.autoBroadcastJoinThreshold": str(20 * 1024 * 1024),
    "spark.sql.files.maxPartitionBytes": str(64 * 1024 * 1024),
    "spark.sql.files.openCostInBytes": str(2 * 1024 * 1024),
    "spark.memory.fraction": "0.8",
    "spark.memory.storageFraction": "0.2",
    # default is file:/spark-warehouse on the master's local disk
    "spark.sql.warehouse.dir": "hdfs:///user/hive/warehouse",
}


def build_session(app_name: str) -> SparkSession:
    """Build the session with this cluster's profile.

    Run on a fresh Python 3 kernel. The PySpark kernel creates its own session
    first, and getOrCreate() would return that one with these settings ignored.
    """
    builder = SparkSession.builder.appName(app_name).enableHiveSupport()
    for k, v in SPARK_TUNING.items():
        builder = builder.config(k, v)
    spark = builder.getOrCreate()
    if spark.sparkContext.appName != app_name:
        raise RuntimeError("got an existing session; switch to the Python 3 kernel and restart")
    return spark
