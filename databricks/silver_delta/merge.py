from pathlib import Path
import sys

from delta.tables import DeltaTable

from pyspark.sql import functions as F
from pyspark.sql.window import Window


PROJECT_ROOT = Path(__file__).resolve().parents[2]

sys.path.append(
    str(
        PROJECT_ROOT
        / "databricks"
        / "utils"
    )
)

sys.path.append(
    str(
        PROJECT_ROOT
        / "databricks"
        / "silver_delta"
    )
)


from spark_session import get_spark
from checkpoint import (
    get_checkpoint,
    save_checkpoint,
)


BRONZE_ROOT = (
    PROJECT_ROOT
    / "bronze"
)

SILVER_ROOT = (
    PROJECT_ROOT
    / "lakehouse"
    / "silver"
)

QUARANTINE_ROOT = (
    PROJECT_ROOT
    / "lakehouse"
    / "quarantine"
)


TABLE_CONFIG = {
    "customers": {
        "primary_key": "customer_id",
    },
    "products": {
        "primary_key": "product_id",
    },
    "orders": {
        "primary_key": "order_id",
    },
    "order_items": {
        "primary_key": "order_item_id",
    },
    "returns": {
        "primary_key": "return_id",
    },
}

def read_incremental_bronze(
    spark,
    table_name,
):
    table_path = (
        BRONZE_ROOT
        / table_name
    )

    files = list(
        table_path.rglob(
            "*.parquet"
        )
    )

    if not files:
        raise FileNotFoundError(
            f"No Bronze files found for "
            f"{table_name}"
        )

    paths = [
        str(file.resolve())
        for file in files
    ]

    df = spark.read.parquet(
        *paths
    )

    checkpoint = get_checkpoint(
        table_name
    )

    if checkpoint is None:
        print(
            f"{table_name}: "
            "no checkpoint — initial load"
        )

        return df

    print(
        f"{table_name}: "
        f"checkpoint = {checkpoint}"
    )

    return df.filter(
        F.col("_ingested_at")
        > F.to_timestamp(
            F.lit(checkpoint)
        )
    )

def deduplicate_latest(
    df,
    primary_key,
):
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
            F.row_number().over(
                window
            ),
        )
        .filter(
            F.col("_row_number")
            == 1
        )
        .drop(
            "_row_number"
        )
    )

def clean_customers(df):
    valid_condition = (
        F.col(
            "customer_id"
        ).isNotNull()
        & F.col(
            "email"
        ).isNotNull()
        & F.col(
            "first_name"
        ).isNotNull()
        & F.col(
            "last_name"
        ).isNotNull()
        & F.col(
            "signup_date"
        ).isNotNull()
    )

    valid = (
        df
        .filter(
            valid_condition
        )
        .withColumn(
            "email",
            F.lower(
                F.trim(
                    F.col("email")
                )
            ),
        )
        .withColumn(
            "country",
            F.coalesce(
                F.col("country"),
                F.lit(
                    "United Kingdom"
                ),
            ),
        )
    )

    invalid = (
        df
        .filter(
            ~valid_condition
        )
        .withColumn(
            "_quarantine_reason",
            F.lit(
                "Missing mandatory "
                "customer field"
            ),
        )
    )

    return valid, invalid

def clean_products(df):
    valid_condition = (
        F.col(
            "product_id"
        ).isNotNull()
        & F.col(
            "sku"
        ).isNotNull()
        & F.col(
            "product_name"
        ).isNotNull()
        & F.col(
            "category"
        ).isNotNull()
        & (
            F.col(
                "unit_cost"
            )
            >= 0
        )
        & (
            F.col(
                "retail_price"
            )
            >= F.col(
                "unit_cost"
            )
        )
    )

    valid = (
        df
        .filter(
            valid_condition
        )
        .withColumn(
            "sku",
            F.upper(
                F.trim(
                    F.col("sku")
                )
            ),
        )
        .withColumn(
            "category",
            F.initcap(
                F.trim(
                    F.col("category")
                )
            ),
        )
    )

    invalid = (
        df
        .filter(
            ~valid_condition
        )
        .withColumn(
            "_quarantine_reason",
            F.lit(
                "Invalid product attributes"
            ),
        )
    )

    return valid, invalid

def clean_orders(df):
    statuses = [
        "pending",
        "processing",
        "shipped",
        "delivered",
        "cancelled",
    ]

    valid_condition = (
        F.col(
            "order_id"
        ).isNotNull()
        & F.col(
            "customer_id"
        ).isNotNull()
        & F.col(
            "order_date"
        ).isNotNull()
        & F.col(
            "order_status"
        ).isin(statuses)
        & (
            F.col(
                "shipping_cost"
            )
            >= 0
        )
    )

    valid = (
        df
        .filter(
            valid_condition
        )
        .withColumn(
            "order_status",
            F.lower(
                F.trim(
                    F.col(
                        "order_status"
                    )
                )
            ),
        )
    )

    invalid = (
        df
        .filter(
            ~valid_condition
        )
        .withColumn(
            "_quarantine_reason",
            F.lit(
                "Invalid order record"
            ),
        )
    )

    return valid, invalid

def clean_order_items(df):
    valid_condition = (
        F.col(
            "order_item_id"
        ).isNotNull()
        & F.col(
            "order_id"
        ).isNotNull()
        & F.col(
            "product_id"
        ).isNotNull()
        & (
            F.col(
                "quantity"
            )
            > 0
        )
        & (
            F.col(
                "unit_price"
            )
            >= 0
        )
        & (
            F.col(
                "discount_pct"
            )
            >= 0
        )
        & (
            F.col(
                "discount_pct"
            )
            <= 100
        )
    )

    valid = (
        df
        .filter(
            valid_condition
        )
        .withColumn(
            "gross_amount",
            F.round(
                F.col("quantity")
                * F.col(
                    "unit_price"
                ),
                2,
            ),
        )
        .withColumn(
            "discount_amount",
            F.round(
                (
                    F.col("quantity")
                    * F.col(
                        "unit_price"
                    )
                )
                * (
                    F.col(
                        "discount_pct"
                    )
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
                    * F.col(
                        "unit_price"
                    )
                )
                * (
                    1
                    - (
                        F.col(
                            "discount_pct"
                        )
                        / 100
                    )
                ),
                2,
            ),
        )
    )

    invalid = (
        df
        .filter(
            ~valid_condition
        )
        .withColumn(
            "_quarantine_reason",
            F.lit(
                "Invalid order item"
            ),
        )
    )

    return valid, invalid

def clean_returns(df):
    valid_condition = (
        F.col(
            "return_id"
        ).isNotNull()
        & F.col(
            "order_item_id"
        ).isNotNull()
        & F.col(
            "return_date"
        ).isNotNull()
        & (
            F.col(
                "quantity_returned"
            )
            > 0
        )
        & (
            F.col(
                "refund_amount"
            )
            >= 0
        )
    )

    valid = (
        df
        .filter(
            valid_condition
        )
        .withColumn(
            "return_reason",
            F.lower(
                F.trim(
                    F.col(
                        "return_reason"
                    )
                )
            ),
        )
    )

    invalid = (
        df
        .filter(
            ~valid_condition
        )
        .withColumn(
            "_quarantine_reason",
            F.lit(
                "Invalid return record"
            ),
        )
    )

    return valid, invalid

TRANSFORMATIONS = {
    "customers": clean_customers,
    "products": clean_products,
    "orders": clean_orders,
    "order_items": clean_order_items,
    "returns": clean_returns,
}

def merge_delta(
    spark,
    df,
    table_name,
    primary_key,
):
    target_path = (
        SILVER_ROOT
        / table_name
    )

    target_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    delta_log = (
        target_path
        / "_delta_log"
    )

    if not delta_log.exists():
        print(
            f"{table_name}: "
            "creating initial Delta table"
        )

        (
            df.write
            .format("delta")
            .mode("overwrite")
            .save(
                str(target_path)
            )
        )

        return

    print(
        f"{table_name}: "
        "MERGE into existing Delta table"
    )

    target = (
        DeltaTable.forPath(
            spark,
            str(target_path),
        )
    )

    (
        target
        .alias("target")
        .merge(
            df.alias("source"),
            (
                f"target.{primary_key} "
                f"= source.{primary_key}"
            ),
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def write_quarantine(
    df,
    table_name,
):
    if df.isEmpty():
        return

    path = (
        QUARANTINE_ROOT
        / table_name
    )

    (
        df.write
        .format("delta")
        .mode("append")
        .save(
            str(path)
        )
    )


def process_table(
    spark,
    table_name,
):
    config = TABLE_CONFIG[
        table_name
    ]

    primary_key = config[
        "primary_key"
    ]

    print(
        "\n"
        + "=" * 60
    )

    print(
        f"Processing {table_name}"
    )

    print(
        "=" * 60
    )

    df = read_incremental_bronze(
        spark,
        table_name,
    )

    if df.isEmpty():
        print(
            f"{table_name}: "
            "no new Bronze records"
        )

        return

    incoming_rows = df.count()

    print(
        f"Incoming rows: "
        f"{incoming_rows:,}"
    )

    max_ingested_at = (
        df
        .agg(
            F.max(
                "_ingested_at"
            )
        )
        .collect()[0][0]
    )

    df = deduplicate_latest(
        df,
        primary_key,
    )

    transformation = (
        TRANSFORMATIONS[
            table_name
        ]
    )

    valid, invalid = (
        transformation(df)
    )

    valid_count = (
        valid.count()
    )

    invalid_count = (
        invalid.count()
    )

    print(
        f"Valid: "
        f"{valid_count:,}"
    )

    print(
        f"Quarantine: "
        f"{invalid_count:,}"
    )

    if valid_count > 0:
        merge_delta(
            spark,
            valid,
            table_name,
            primary_key,
        )

    if invalid_count > 0:
        write_quarantine(
            invalid,
            table_name,
        )

    save_checkpoint(
        table_name,
        max_ingested_at,
    )

    print(
        f"Checkpoint updated: "
        f"{max_ingested_at}"
    )

def main():
    spark = get_spark()

    print(
        "\nRetailPulse "
        "Incremental Silver Delta Pipeline"
    )

    for table_name in TABLE_CONFIG:
        process_table(
            spark,
            table_name,
        )

    spark.stop()

    print(
        "\nDelta Silver pipeline completed."
    )


if __name__ == "__main__":
    main()
