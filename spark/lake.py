"""Shared helpers of the Spark jobs: the Spark session and where each lakehouse layer lives.

By default the layers are the S3 buckets (s3a://bronze/, s3a://silver/, s3a://gold/).
LAKE_URI can point them elsewhere, e.g. a local folder for tests: LAKE_URI=file:///tmp/lake/
"""
from __future__ import annotations

import os

from pyspark.sql import SparkSession

LAKE_URI = os.getenv("LAKE_URI", "s3a://")


def path(layer: str, dataset: str) -> str:
    """path('silver', 'contacts') -> s3a://silver/contacts"""
    return f"{LAKE_URI}{layer}/{dataset}"


def spark_session(app: str) -> SparkSession:
    spark = (SparkSession.builder.appName(app)
             .config("spark.sql.session.timeZone", "UTC")
             # overwrite only the date/market partitions a run writes, keep the others (idempotent reruns)
             .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
             .config("spark.sql.shuffle.partitions", "8")          # small data: few partitions
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    # S3 credentials come from the environment (never written in a file that goes to GitHub)
    hadoop = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop.set("fs.s3a.access.key", os.getenv("S3_ACCESS_KEY", "lakehouse"))
    hadoop.set("fs.s3a.secret.key", os.getenv("S3_SECRET_KEY", "lakehouse123"))
    return spark
