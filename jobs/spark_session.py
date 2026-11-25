"""Fábrica padronizada de SparkSession configurada para S3A (SeaweedFS)."""
import os
from pyspark.sql import SparkSession


def build_spark_session(app_name: str) -> SparkSession:
    endpoint = os.environ.get("S3_ENDPOINT", "http://seaweedfs:8333")
    access_key = os.environ.get("S3_ACCESS_KEY", "laguna_poc_access")
    secret_key = os.environ.get("S3_SECRET_KEY", "laguna_poc_secret")
    master_url = os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077")
    driver_host = os.environ.get("SPARK_DRIVER_HOST", "spark-driver")
    driver_memory = os.environ.get("SPARK_DRIVER_MEMORY", "512m")
    executor_memory = os.environ.get("SPARK_EXECUTOR_MEMORY", "512m")

    return (
        SparkSession.builder
        .appName(app_name)
        .master(master_url)
        .config("spark.driver.host", driver_host)
        .config("spark.driver.memory", driver_memory)
        .config("spark.executor.memory", executor_memory)
        .config("spark.sql.caseSensitive", "true")
        .config("spark.sql.mapKeyDedupPolicy", "LAST_WIN")
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
