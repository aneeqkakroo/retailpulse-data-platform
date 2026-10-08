import os
import sys

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


def get_spark():
    builder = (
        SparkSession.builder
        .appName("RetailPulse")
        .master("local[*]")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "spark.sql.session.timeZone",
            "UTC",
        )
    )

    spark = (
        configure_spark_with_delta_pip(
            builder
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark