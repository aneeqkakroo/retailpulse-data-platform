from pathlib import Path
import sys

from pyspark.sql import functions as F
from pyspark.sql.window import Window


PROJECT_ROOT = Path(__file__).resolve().parents[2]

sys.path.append(
    str(PROJECT_ROOT / "databricks" / "utils")
)

from spark_session import get_spark


BRONZE_ROOT = PROJECT_ROOT / "bronze"
SILVER_ROOT = PROJECT_ROOT / "silver"


def read_bronze(spark, table_name):
    table_path = BRONZE_ROOT / table_name

    files = list(
        table_path.rglob("*.parquet")
    )

    if not files:
        raise FileNotFoundError(
            f"No Bronze Parquet files found for {table_name} "
            f"under {table_path}"
        )

    spark_paths = [
        str(file.resolve()).replace("\\", "/")
        for file in files
    ]

    print(
        f"Reading {len(spark_paths)} Bronze "
        f"Parquet file(s) for {table_name}"
    )

    return spark.read.parquet(
        *spark_paths
    )


def deduplicate_latest(df, primary_key):
    window = (
        Window
        .partitionBy(primary_key)
        .orderBy(
            F.col("updated_at").desc(),
            F.col("_ingested_at").desc(),
        )
    )

    return (
        df
        .withColumn(
            "_row_number",
            F.row_number().over(window),
        )
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )


def write_silver(df, table_name):
    path = SILVER_ROOT / table_name

    (
        df.write
        .mode("overwrite")
        .parquet(str(path))
    )


def write_quarantine(df, table_name):
    path = (
        SILVER_ROOT
        / "quarantine"
        / table_name
    )

    (
        df.write
        .mode("overwrite")
        .parquet(str(path))
    )


def transform_customers(spark):
    df = read_bronze(
        spark,
        "customers",
    )

    df = deduplicate_latest(
        df,
        "customer_id",
    )

    valid_condition = (
        F.col("customer_id").isNotNull()
        & F.col("email").isNotNull()
        & F.col("first_name").isNotNull()
        & F.col("last_name").isNotNull()
        & F.col("signup_date").isNotNull()
    )

    valid = (
        df
        .filter(valid_condition)
        .withColumn(
            "email",
            F.lower(
                F.trim(F.col("email"))
            ),
        )
        .withColumn(
            "country",
            F.coalesce(
                F.col("country"),
                F.lit("United Kingdom"),
            ),
        )
    )

    invalid = (
        df
        .filter(~valid_condition)
        .withColumn(
            "_quarantine_reason",
            F.lit(
                "Missing mandatory customer field"
            ),
        )
    )

    write_silver(
        valid,
        "customers",
    )

    write_quarantine(
        invalid,
        "customers",
    )

    return (
        valid.count(),
        invalid.count(),
    )


def transform_products(spark):
    df = read_bronze(
        spark,
        "products",
    )

    df = deduplicate_latest(
        df,
        "product_id",
    )

    valid_condition = (
        F.col("product_id").isNotNull()
        & F.col("sku").isNotNull()
        & F.col("product_name").isNotNull()
        & F.col("category").isNotNull()
        & (F.col("unit_cost") >= 0)
        & (F.col("retail_price") >= 0)
        & (
            F.col("retail_price")
            >= F.col("unit_cost")
        )
    )

    valid = (
        df
        .filter(valid_condition)
        .withColumn(
            "sku",
            F.upper(
                F.trim(F.col("sku"))
            ),
        )
        .withColumn(
            "category",
            F.initcap(
                F.trim(F.col("category"))
            ),
        )
    )

    invalid = (
        df
        .filter(~valid_condition)
        .withColumn(
            "_quarantine_reason",
            F.lit(
                "Invalid product attributes"
            ),
        )
    )

    write_silver(
        valid,
        "products",
    )

    write_quarantine(
        invalid,
        "products",
    )

    return (
        valid.count(),
        invalid.count(),
    )


def transform_orders(spark):
    df = read_bronze(
        spark,
        "orders",
    )

    df = deduplicate_latest(
        df,
        "order_id",
    )

    allowed_statuses = [
        "pending",
        "processing",
        "shipped",
        "delivered",
        "cancelled",
    ]

    valid_condition = (
        F.col("order_id").isNotNull()
        & F.col("customer_id").isNotNull()
        & F.col("order_date").isNotNull()
        & F.col("order_status").isin(
            allowed_statuses
        )
        & (F.col("shipping_cost") >= 0)
    )

    valid = (
        df
        .filter(valid_condition)
        .withColumn(
            "order_status",
            F.lower(
                F.trim(
                    F.col("order_status")
                )
            ),
        )
        .withColumn(
            "order_date",
            F.to_timestamp(
                F.col("order_date")
            ),
        )
    )

    invalid = (
        df
        .filter(~valid_condition)
        .withColumn(
            "_quarantine_reason",
            F.lit(
                "Invalid order record"
            ),
        )
    )

    write_silver(
        valid,
        "orders",
    )

    write_quarantine(
        invalid,
        "orders",
    )

    return (
        valid.count(),
        invalid.count(),
    )


def transform_order_items(spark):
    df = read_bronze(
        spark,
        "order_items",
    )

    df = deduplicate_latest(
        df,
        "order_item_id",
    )

    valid_condition = (
        F.col("order_item_id").isNotNull()
        & F.col("order_id").isNotNull()
        & F.col("product_id").isNotNull()
        & (F.col("quantity") > 0)
        & (F.col("unit_price") >= 0)
        & (F.col("discount_pct") >= 0)
        & (F.col("discount_pct") <= 100)
    )

    valid = (
        df
        .filter(valid_condition)
        .withColumn(
            "gross_amount",
            F.round(
                F.col("quantity")
                * F.col("unit_price"),
                2,
            ),
        )
        .withColumn(
            "discount_amount",
            F.round(
                (
                    F.col("quantity")
                    * F.col("unit_price")
                )
                * (
                    F.col("discount_pct")
                    / 100
                ),
                2,
            ),
        )
        .withColumn(
            "net_amount",
            F.round(
                (
                    F.col("quantity")
                    * F.col("unit_price")
                )
                * (
                    1
                    - (
                        F.col("discount_pct")
                        / 100
                    )
                ),
                2,
            ),
        )
    )

    invalid = (
        df
        .filter(~valid_condition)
        .withColumn(
            "_quarantine_reason",
            F.lit(
                "Invalid order item"
            ),
        )
    )

    write_silver(
        valid,
        "order_items",
    )

    write_quarantine(
        invalid,
        "order_items",
    )

    return (
        valid.count(),
        invalid.count(),
    )


def transform_returns(spark):
    df = read_bronze(
        spark,
        "returns",
    )

    df = deduplicate_latest(
        df,
        "return_id",
    )

    valid_condition = (
        F.col("return_id").isNotNull()
        & F.col("order_item_id").isNotNull()
        & F.col("return_date").isNotNull()
        & (F.col("quantity_returned") > 0)
        & (F.col("refund_amount") >= 0)
    )

    valid = (
        df
        .filter(valid_condition)
        .withColumn(
            "return_reason",
            F.lower(
                F.trim(
                    F.col("return_reason")
                )
            ),
        )
    )

    invalid = (
        df
        .filter(~valid_condition)
        .withColumn(
            "_quarantine_reason",
            F.lit(
                "Invalid return record"
            ),
        )
    )

    write_silver(
        valid,
        "returns",
    )

    write_quarantine(
        invalid,
        "returns",
    )

    return (
        valid.count(),
        invalid.count(),
    )


def main():
    spark = get_spark()

    pipelines = [
        (
            "customers",
            transform_customers,
        ),
        (
            "products",
            transform_products,
        ),
        (
            "orders",
            transform_orders,
        ),
        (
            "order_items",
            transform_order_items,
        ),
        (
            "returns",
            transform_returns,
        ),
    ]

    print(
        "\nStarting Silver transformations\n"
    )

    for table_name, transformation in pipelines:
        valid_rows, invalid_rows = (
            transformation(spark)
        )

        print(
            f"{table_name}: "
            f"{valid_rows:,} valid, "
            f"{invalid_rows:,} quarantined"
        )

    spark.stop()

    print(
        "\nSilver pipeline completed."
    )


if __name__ == "__main__":
    main()