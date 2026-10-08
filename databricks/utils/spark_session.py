import os
import sys

from pyspark.sql import SparkSession


os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


def get_spark():
    spark = (
        SparkSession.builder
        .appName("RetailPulse")
        .master("local[*]")
        .config(
            "spark.sql.session.timeZone",
            "UTC",
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark