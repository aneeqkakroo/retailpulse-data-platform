from datetime import datetime, timezone
from pathlib import Path
import sys

from delta.tables import DeltaTable
from pyspark.sql import functions as F


# ==========================================================
# PROJECT PATHS
# ==========================================================

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
        / "pipeline_utils"
    )
)

from spark_session import get_spark

from audit import (
    start_pipeline_run,
    complete_pipeline_run,
    fail_pipeline_run,
)

from db import get_connection


SILVER_ROOT = (
    PROJECT_ROOT
    / "lakehouse"
    / "silver"
)

GOLD_ROOT = (
    PROJECT_ROOT
    / "lakehouse"
    / "gold"
)


# ==========================================================
# HELPERS
# ==========================================================

def silver_path(table_name):
    return str(
        SILVER_ROOT
        / table_name
    )


def gold_path(table_name):
    return str(
        GOLD_ROOT
        / table_name
    )


def read_silver(
    spark,
    table_name,
):
    return (
        spark.read
        .format("delta")
        .load(
            silver_path(table_name)
        )
    )


# ==========================================================
# GOLD AUDIT WRAPPER
# ==========================================================

def run_audited_gold_step(
    pipeline_name,
    source_name,
    target_name,
    build_function,
    spark,
):
    run_id = None

    rows_read = 0
    rows_written = 0

    # ------------------------------------------------------
    # START AUDIT
    # ------------------------------------------------------

    with get_connection() as connection:
        with connection.cursor() as cursor:

            run_id = start_pipeline_run(
                cursor=cursor,
                pipeline_name=pipeline_name,
                layer="GOLD",
                source_name=source_name,
                target_name=target_name,
            )

            connection.commit()

    print()
    print(
        f"Audit run ID: {run_id}"
    )

    try:

        metrics = build_function(
            spark
        )

        if metrics is not None:

            rows_read = metrics.get(
                "rows_read",
                0,
            )

            rows_written = metrics.get(
                "rows_written",
                0,
            )

        # --------------------------------------------------
        # SUCCESS AUDIT
        # --------------------------------------------------

        with get_connection() as connection:
            with connection.cursor() as cursor:

                complete_pipeline_run(
                    cursor=cursor,
                    run_id=run_id,
                    rows_read=rows_read,
                    rows_written=rows_written,
                    rows_quarantined=0,
                    watermark_from=None,
                    watermark_to=None,
                )

                connection.commit()

        return metrics

    except Exception as error:

        # --------------------------------------------------
        # FAILURE AUDIT
        # --------------------------------------------------

        try:

            with get_connection() as connection:
                with connection.cursor() as cursor:

                    fail_pipeline_run(
                        cursor=cursor,
                        run_id=run_id,
                        error=error,
                        rows_read=rows_read,
                        rows_written=rows_written,
                        rows_quarantined=0,
                        watermark_from=None,
                        watermark_to=None,
                    )

                    connection.commit()

        except Exception as audit_error:

            print(
                "WARNING: Gold failure audit "
                "could not be saved."
            )

            print(
                f"Audit error: {audit_error}"
            )

        raise


# ==========================================================
# DIM CUSTOMER — SCD TYPE 2
# ==========================================================

def build_dim_customer(spark):
    print()
    print("=" * 70)
    print("Building DimCustomer — SCD Type 2")
    print("=" * 70)

    source = read_silver(
        spark,
        "customers",
    )

    source_count = (
        source.count()
    )

    tracked_columns = [
        "first_name",
        "last_name",
        "email",
        "address_line_1",
        "city",
        "region",
        "postcode",
        "country",
        "customer_segment",
    ]

    # ------------------------------------------------------
    # ATTRIBUTE HASH
    # Used to identify customer attribute changes
    # ------------------------------------------------------

    source = source.withColumn(
        "_attribute_hash",
        F.sha2(
            F.concat_ws(
                "||",
                *[
                    F.coalesce(
                        F.col(
                            column
                        ).cast(
                            "string"
                        ),
                        F.lit(
                            "<NULL>"
                        ),
                    )
                    for column
                    in tracked_columns
                ],
            ),
            256,
        ),
    )

    target_path = (
        GOLD_ROOT
        / "dim_customer"
    )

    # Timestamp used for future SCD2 changes
    effective_timestamp = (
        datetime.now(
            timezone.utc
        )
        .replace(
            tzinfo=None
        )
    )

    effective_literal = (
        F.lit(
            effective_timestamp
        )
        .cast(
            "timestamp"
        )
    )

    # ------------------------------------------------------
    # FIRST LOAD
    # ------------------------------------------------------
    #
    # 1900-01-01 allows historical orders to resolve to
    # the initial customer dimension version.
    # ------------------------------------------------------

    if not (
        target_path
        / "_delta_log"
    ).exists():

        initial_valid_from = (
            F.to_timestamp(
                F.lit(
                    "1900-01-01 00:00:00"
                )
            )
        )

        initial = (
            source
            .withColumn(
                "valid_from",
                initial_valid_from,
            )
            .withColumn(
                "valid_to",
                F.lit(
                    None
                ).cast(
                    "timestamp"
                ),
            )
            .withColumn(
                "is_current",
                F.lit(
                    True
                ),
            )
            .withColumn(
                "customer_sk",
                F.sha2(
                    F.concat_ws(
                        "||",
                        F.col(
                            "customer_id"
                        ).cast(
                            "string"
                        ),
                        F.col(
                            "valid_from"
                        ).cast(
                            "string"
                        ),
                    ),
                    256,
                ),
            )
        )

        columns = [
            "customer_sk",
            "customer_id",
            "first_name",
            "last_name",
            "email",
            "date_of_birth",
            "address_line_1",
            "city",
            "region",
            "postcode",
            "country",
            "customer_segment",
            "signup_date",
            "valid_from",
            "valid_to",
            "is_current",
            "_attribute_hash",
        ]

        initial_output = (
            initial
            .select(
                *columns
            )
        )

        output_count = (
            initial_output.count()
        )

        (
            initial_output
            .write
            .format(
                "delta"
            )
            .mode(
                "overwrite"
            )
            .save(
                str(
                    target_path
                )
            )
        )

        print(
            f"Initial DimCustomer rows: "
            f"{output_count:,}"
        )

        return {
            "rows_read":
                source_count,

            "rows_written":
                output_count,
        }

    # ------------------------------------------------------
    # EXISTING DIMENSION
    # ------------------------------------------------------

    existing = (
        spark.read
        .format(
            "delta"
        )
        .load(
            str(
                target_path
            )
        )
    )

    current = (
        existing
        .filter(
            F.col(
                "is_current"
            )
            == True
        )
        .select(
            "customer_id",

            F.col(
                "_attribute_hash"
            ).alias(
                "_existing_hash"
            ),
        )
    )

    comparison = (
        source
        .join(
            current,
            on="customer_id",
            how="left",
        )
    )

    # ------------------------------------------------------
    # NEW CUSTOMERS
    # ------------------------------------------------------

    new_customers = (
        comparison
        .filter(
            F.col(
                "_existing_hash"
            ).isNull()
        )
        .drop(
            "_existing_hash"
        )
    )

    # ------------------------------------------------------
    # CHANGED CUSTOMERS
    # ------------------------------------------------------

    changed_customers = (
        comparison
        .filter(
            F.col(
                "_existing_hash"
            ).isNotNull()
            &
            (
                F.col(
                    "_attribute_hash"
                )
                !=
                F.col(
                    "_existing_hash"
                )
            )
        )
        .drop(
            "_existing_hash"
        )
    )

    new_count = (
        new_customers.count()
    )

    changed_count = (
        changed_customers.count()
    )

    print(
        f"New customers: "
        f"{new_count:,}"
    )

    print(
        f"Changed customers: "
        f"{changed_count:,}"
    )

    # ------------------------------------------------------
    # CLOSE OLD CUSTOMER VERSIONS
    # ------------------------------------------------------

    if changed_count > 0:

        changed_ids = (
            changed_customers
            .select(
                "customer_id"
            )
            .distinct()
        )

        target = (
            DeltaTable.forPath(
                spark,
                str(
                    target_path
                ),
            )
        )

        (
            target
            .alias(
                "target"
            )
            .merge(
                changed_ids.alias(
                    "source"
                ),
                """
                target.customer_id = source.customer_id
                AND target.is_current = true
                """,
            )
            .whenMatchedUpdate(
                set={
                    "valid_to":
                        effective_literal,

                    "is_current":
                        F.lit(
                            False
                        ),
                }
            )
            .execute()
        )

    # ------------------------------------------------------
    # INSERT NEW CUSTOMER VERSIONS
    # ------------------------------------------------------

    records_to_insert = (
        new_customers
        .unionByName(
            changed_customers
        )
    )

    insert_count = (
        records_to_insert.count()
    )

    if insert_count > 0:

        records_to_insert = (
            records_to_insert
            .withColumn(
                "valid_from",
                effective_literal,
            )
            .withColumn(
                "valid_to",
                F.lit(
                    None
                ).cast(
                    "timestamp"
                ),
            )
            .withColumn(
                "is_current",
                F.lit(
                    True
                ),
            )
            .withColumn(
                "customer_sk",
                F.sha2(
                    F.concat_ws(
                        "||",
                        F.col(
                            "customer_id"
                        ).cast(
                            "string"
                        ),
                        F.col(
                            "valid_from"
                        ).cast(
                            "string"
                        ),
                    ),
                    256,
                ),
            )
        )

        columns = [
            "customer_sk",
            "customer_id",
            "first_name",
            "last_name",
            "email",
            "date_of_birth",
            "address_line_1",
            "city",
            "region",
            "postcode",
            "country",
            "customer_segment",
            "signup_date",
            "valid_from",
            "valid_to",
            "is_current",
            "_attribute_hash",
        ]

        (
            records_to_insert
            .select(
                *columns
            )
            .write
            .format(
                "delta"
            )
            .mode(
                "append"
            )
            .save(
                str(
                    target_path
                )
            )
        )

    print(
        f"Inserted customer versions: "
        f"{insert_count:,}"
    )

    return {
        "rows_read":
            source_count,

        "rows_written":
            insert_count,
    }


# ==========================================================
# DIM PRODUCT — TYPE 1
# ==========================================================

def build_dim_product(spark):
    print()
    print("=" * 70)
    print("Building DimProduct")
    print("=" * 70)

    products = read_silver(
        spark,
        "products",
    )

    source_count = (
        products.count()
    )

    dim_product = (
        products
        .withColumn(
            "product_sk",
            F.xxhash64(
                F.col(
                    "product_id"
                )
            ),
        )
        .select(
            "product_sk",
            "product_id",
            "sku",
            "product_name",
            "category",
            "brand",
            "supplier_id",
            "unit_cost",
            "retail_price",
            "active",
        )
    )

    output_count = (
        dim_product.count()
    )

    (
        dim_product
        .write
        .format(
            "delta"
        )
        .mode(
            "overwrite"
        )
        .option(
            "overwriteSchema",
            "true",
        )
        .save(
            gold_path(
                "dim_product"
            )
        )
    )

    print(
        f"DimProduct rows: "
        f"{output_count:,}"
    )

    return {
        "rows_read":
            source_count,

        "rows_written":
            output_count,
    }


# ==========================================================
# DIM DATE
# ==========================================================

def build_dim_date(spark):
    print()
    print("=" * 70)
    print("Building DimDate")
    print("=" * 70)

    orders = read_silver(
        spark,
        "orders",
    )

    source_count = (
        orders.count()
    )

    min_max = (
        orders
        .agg(
            F.min(
                F.to_date(
                    "order_date"
                )
            ).alias(
                "min_date"
            ),

            F.max(
                F.to_date(
                    "order_date"
                )
            ).alias(
                "max_date"
            ),
        )
        .collect()[0]
    )

    min_date = (
        min_max[
            "min_date"
        ]
    )

    max_date = (
        min_max[
            "max_date"
        ]
    )

    if (
        min_date is None
        or max_date is None
    ):
        raise RuntimeError(
            "Cannot build DimDate because "
            "orders contain no valid dates."
        )

    dates = (
        spark.sql(
            f"""
            SELECT explode(
                sequence(
                    to_date('{min_date}'),
                    to_date('{max_date}'),
                    interval 1 day
                )
            ) AS date
            """
        )
    )

    dim_date = (
        dates
        .withColumn(
            "date_sk",
            F.date_format(
                "date",
                "yyyyMMdd",
            ).cast(
                "int"
            ),
        )
        .withColumn(
            "year",
            F.year(
                "date"
            ),
        )
        .withColumn(
            "quarter",
            F.quarter(
                "date"
            ),
        )
        .withColumn(
            "month",
            F.month(
                "date"
            ),
        )
        .withColumn(
            "month_name",
            F.date_format(
                "date",
                "MMMM",
            ),
        )
        .withColumn(
            "week_of_year",
            F.weekofyear(
                "date"
            ),
        )
        .withColumn(
            "day_of_month",
            F.dayofmonth(
                "date"
            ),
        )
        .withColumn(
            "day_name",
            F.date_format(
                "date",
                "EEEE",
            ),
        )
        .withColumn(
            "is_weekend",
            F.dayofweek(
                "date"
            ).isin(
                1,
                7,
            ),
        )
        .select(
            "date_sk",
            "date",
            "year",
            "quarter",
            "month",
            "month_name",
            "week_of_year",
            "day_of_month",
            "day_name",
            "is_weekend",
        )
    )

    output_count = (
        dim_date.count()
    )

    (
        dim_date
        .write
        .format(
            "delta"
        )
        .mode(
            "overwrite"
        )
        .option(
            "overwriteSchema",
            "true",
        )
        .save(
            gold_path(
                "dim_date"
            )
        )
    )

    print(
        f"DimDate rows: "
        f"{output_count:,}"
    )

    return {
        "rows_read":
            source_count,

        "rows_written":
            output_count,
    }


# ==========================================================
# FACT SALES
# Grain: one row per order item
# ==========================================================

def build_fact_sales(spark):
    print()
    print("=" * 70)
    print(
        "Building FactSales "
        "(grain: one row per order item)"
    )
    print("=" * 70)

    orders = read_silver(
        spark,
        "orders",
    )

    order_items = read_silver(
        spark,
        "order_items",
    )

    orders_count = (
        orders.count()
    )

    order_items_count = (
        order_items.count()
    )

    dim_customer = (
        spark.read
        .format(
            "delta"
        )
        .load(
            gold_path(
                "dim_customer"
            )
        )
    )

    dim_product = (
        spark.read
        .format(
            "delta"
        )
        .load(
            gold_path(
                "dim_product"
            )
        )
    )

    # ------------------------------------------------------
    # ITEM COUNT PER ORDER
    # ------------------------------------------------------

    item_counts = (
        order_items
        .groupBy(
            "order_id"
        )
        .agg(
            F.count(
                "*"
            ).alias(
                "items_in_order"
            )
        )
    )

    # ------------------------------------------------------
    # ORDER ITEM + ORDER
    # ------------------------------------------------------

    sales = (
        order_items.alias(
            "item"
        )
        .join(
            orders.alias(
                "ord"
            ),
            F.col(
                "item.order_id"
            )
            ==
            F.col(
                "ord.order_id"
            ),
            "inner",
        )
        .select(
            F.col(
                "item.order_item_id"
            ).alias(
                "order_item_id"
            ),

            F.col(
                "item.order_id"
            ).alias(
                "order_id"
            ),

            F.col(
                "item.product_id"
            ).alias(
                "product_id"
            ),

            F.col(
                "ord.customer_id"
            ).alias(
                "customer_id"
            ),

            F.col(
                "ord.order_date"
            ).alias(
                "order_date"
            ),

            F.col(
                "ord.order_status"
            ).alias(
                "order_status"
            ),

            F.col(
                "ord.payment_method"
            ).alias(
                "payment_method"
            ),

            F.col(
                "ord.shipping_cost"
            ).alias(
                "shipping_cost"
            ),

            F.col(
                "item.quantity"
            ).alias(
                "quantity"
            ),

            F.col(
                "item.unit_price"
            ).alias(
                "unit_price"
            ),

            F.col(
                "item.discount_pct"
            ).alias(
                "discount_pct"
            ),

            F.col(
                "item.gross_amount"
            ).alias(
                "gross_amount"
            ),

            F.col(
                "item.discount_amount"
            ).alias(
                "discount_amount"
            ),

            F.col(
                "item.net_amount"
            ).alias(
                "net_amount"
            ),
        )
    )

    # ------------------------------------------------------
    # SHIPPING ALLOCATION INPUT
    # ------------------------------------------------------

    sales = (
        sales.alias(
            "sale"
        )
        .join(
            item_counts.alias(
                "counts"
            ),
            F.col(
                "sale.order_id"
            )
            ==
            F.col(
                "counts.order_id"
            ),
            "left",
        )
        .select(
            "sale.*",

            F.col(
                "counts.items_in_order"
            ).alias(
                "items_in_order"
            ),
        )
    )

    # ------------------------------------------------------
    # HISTORICAL CUSTOMER LOOKUP
    # ------------------------------------------------------

    sales = (
        sales.alias(
            "sale"
        )
        .join(
            dim_customer.alias(
                "customer"
            ),
            (
                F.col(
                    "sale.customer_id"
                )
                ==
                F.col(
                    "customer.customer_id"
                )
            )
            &
            (
                F.col(
                    "sale.order_date"
                )
                >=
                F.col(
                    "customer.valid_from"
                )
            )
            &
            (
                F.col(
                    "customer.valid_to"
                ).isNull()
                |
                (
                    F.col(
                        "sale.order_date"
                    )
                    <
                    F.col(
                        "customer.valid_to"
                    )
                )
            ),
            "left",
        )
        .select(
            "sale.*",

            F.col(
                "customer.customer_sk"
            ).alias(
                "customer_sk"
            ),
        )
    )

    # ------------------------------------------------------
    # PRODUCT LOOKUP
    # ------------------------------------------------------

    sales = (
        sales.alias(
            "sale"
        )
        .join(
            dim_product.alias(
                "product"
            ),
            F.col(
                "sale.product_id"
            )
            ==
            F.col(
                "product.product_id"
            ),
            "left",
        )
        .select(
            "sale.*",

            F.col(
                "product.product_sk"
            ).alias(
                "product_sk"
            ),

            F.col(
                "product.unit_cost"
            ).alias(
                "product_unit_cost"
            ),
        )
    )

    # ------------------------------------------------------
    # FACT MEASURES
    # ------------------------------------------------------

    fact_sales = (
        sales
        .withColumn(
            "date_sk",
            F.date_format(
                F.col(
                    "order_date"
                ),
                "yyyyMMdd",
            ).cast(
                "int"
            ),
        )
        .withColumn(
            "allocated_shipping_cost",
            F.round(
                F.when(
                    F.col(
                        "items_in_order"
                    ) > 0,
                    F.col(
                        "shipping_cost"
                    )
                    /
                    F.col(
                        "items_in_order"
                    ),
                )
                .otherwise(
                    F.lit(
                        0
                    )
                ),
                2,
            ),
        )
        .withColumn(
            "cost_of_goods",
            F.round(
                F.col(
                    "quantity"
                )
                *
                F.col(
                    "product_unit_cost"
                ),
                2,
            ),
        )
        .withColumn(
            "gross_margin",
            F.round(
                F.col(
                    "net_amount"
                )
                -
                F.col(
                    "cost_of_goods"
                )
                -
                F.col(
                    "allocated_shipping_cost"
                ),
                2,
            ),
        )
        .withColumn(
            "sales_sk",
            F.xxhash64(
                F.col(
                    "order_item_id"
                )
            ),
        )
        .select(
            "sales_sk",
            "order_id",
            "order_item_id",
            "date_sk",
            "customer_sk",
            "product_sk",
            "order_status",
            "payment_method",
            "quantity",
            "unit_price",
            "discount_pct",
            "gross_amount",
            "discount_amount",
            "net_amount",
            "allocated_shipping_cost",
            "cost_of_goods",
            "gross_margin",
            "order_date",
        )
    )

    output_count = (
        fact_sales.count()
    )

    (
        fact_sales
        .write
        .format(
            "delta"
        )
        .mode(
            "overwrite"
        )
        .option(
            "overwriteSchema",
            "true",
        )
        .save(
            gold_path(
                "fact_sales"
            )
        )
    )

    print(
        f"FactSales rows: "
        f"{output_count:,}"
    )

    return {
        "rows_read":
            orders_count
            +
            order_items_count,

        "rows_written":
            output_count,
    }


# ==========================================================
# FACT RETURNS
# Grain: one row per return
# ==========================================================

def build_fact_returns(spark):
    print()
    print("=" * 70)
    print(
        "Building FactReturns "
        "(grain: one row per return)"
    )
    print("=" * 70)

    returns = read_silver(
        spark,
        "returns",
    )

    order_items = read_silver(
        spark,
        "order_items",
    )

    orders = read_silver(
        spark,
        "orders",
    )

    returns_count = (
        returns.count()
    )

    order_items_count = (
        order_items.count()
    )

    orders_count = (
        orders.count()
    )

    dim_product = (
        spark.read
        .format(
            "delta"
        )
        .load(
            gold_path(
                "dim_product"
            )
        )
    )

    dim_customer = (
        spark.read
        .format(
            "delta"
        )
        .load(
            gold_path(
                "dim_customer"
            )
        )
    )

    # ------------------------------------------------------
    # RETURN + ORDER ITEM
    # ------------------------------------------------------

    joined = (
        returns.alias(
            "ret"
        )
        .join(
            order_items.alias(
                "item"
            ),
            F.col(
                "ret.order_item_id"
            )
            ==
            F.col(
                "item.order_item_id"
            ),
            "inner",
        )
        .select(
            F.col(
                "ret.return_id"
            ).alias(
                "return_id"
            ),

            F.col(
                "ret.order_item_id"
            ).alias(
                "order_item_id"
            ),

            F.col(
                "item.order_id"
            ).alias(
                "order_id"
            ),

            F.col(
                "item.product_id"
            ).alias(
                "product_id"
            ),

            F.col(
                "ret.return_date"
            ).alias(
                "return_date"
            ),

            F.col(
                "ret.return_reason"
            ).alias(
                "return_reason"
            ),

            F.col(
                "ret.return_status"
            ).alias(
                "return_status"
            ),

            F.col(
                "ret.quantity_returned"
            ).alias(
                "quantity_returned"
            ),

            F.col(
                "ret.refund_amount"
            ).alias(
                "refund_amount"
            ),
        )
    )

    # ------------------------------------------------------
    # ADD ORDER INFO
    # ------------------------------------------------------

    joined = (
        joined.alias(
            "ret"
        )
        .join(
            orders.alias(
                "ord"
            ),
            F.col(
                "ret.order_id"
            )
            ==
            F.col(
                "ord.order_id"
            ),
            "inner",
        )
        .select(
            "ret.*",

            F.col(
                "ord.customer_id"
            ).alias(
                "customer_id"
            ),

            F.col(
                "ord.order_date"
            ).alias(
                "order_date"
            ),
        )
    )

    # ------------------------------------------------------
    # PRODUCT DIMENSION
    # ------------------------------------------------------

    joined = (
        joined.alias(
            "ret"
        )
        .join(
            dim_product.alias(
                "product"
            ),
            F.col(
                "ret.product_id"
            )
            ==
            F.col(
                "product.product_id"
            ),
            "left",
        )
        .select(
            "ret.*",

            F.col(
                "product.product_sk"
            ).alias(
                "product_sk"
            ),
        )
    )

    # ------------------------------------------------------
    # HISTORICAL CUSTOMER DIMENSION
    # ------------------------------------------------------

    joined = (
        joined.alias(
            "ret"
        )
        .join(
            dim_customer.alias(
                "customer"
            ),
            (
                F.col(
                    "ret.customer_id"
                )
                ==
                F.col(
                    "customer.customer_id"
                )
            )
            &
            (
                F.col(
                    "ret.order_date"
                )
                >=
                F.col(
                    "customer.valid_from"
                )
            )
            &
            (
                F.col(
                    "customer.valid_to"
                ).isNull()
                |
                (
                    F.col(
                        "ret.order_date"
                    )
                    <
                    F.col(
                        "customer.valid_to"
                    )
                )
            ),
            "left",
        )
        .select(
            "ret.*",

            F.col(
                "customer.customer_sk"
            ).alias(
                "customer_sk"
            ),
        )
    )

    # ------------------------------------------------------
    # FACT
    # ------------------------------------------------------

    fact_returns = (
        joined
        .withColumn(
            "return_sk",
            F.xxhash64(
                F.col(
                    "return_id"
                )
            ),
        )
        .withColumn(
            "return_date_sk",
            F.date_format(
                F.col(
                    "return_date"
                ),
                "yyyyMMdd",
            ).cast(
                "int"
            ),
        )
        .select(
            "return_sk",
            "return_id",
            "order_item_id",
            "order_id",
            "return_date_sk",
            "customer_sk",
            "product_sk",
            "return_reason",
            "return_status",
            "quantity_returned",
            "refund_amount",
            "return_date",
        )
    )

    output_count = (
        fact_returns.count()
    )

    (
        fact_returns
        .write
        .format(
            "delta"
        )
        .mode(
            "overwrite"
        )
        .option(
            "overwriteSchema",
            "true",
        )
        .save(
            gold_path(
                "fact_returns"
            )
        )
    )

    print(
        f"FactReturns rows: "
        f"{output_count:,}"
    )

    return {
        "rows_read":
            returns_count
            +
            order_items_count
            +
            orders_count,

        "rows_written":
            output_count,
    }


# ==========================================================
# GOLD DATA QUALITY
# ==========================================================

def run_gold_quality_checks(spark):
    print()
    print("=" * 70)
    print("Gold data-quality checks")
    print("=" * 70)

    fact_sales = (
        spark.read
        .format(
            "delta"
        )
        .load(
            gold_path(
                "fact_sales"
            )
        )
    )

    fact_returns = (
        spark.read
        .format(
            "delta"
        )
        .load(
            gold_path(
                "fact_returns"
            )
        )
    )

    dim_customer = (
        spark.read
        .format(
            "delta"
        )
        .load(
            gold_path(
                "dim_customer"
            )
        )
    )

    # ------------------------------------------------------
    # FACT SALES CHECKS
    # ------------------------------------------------------

    missing_customer = (
        fact_sales
        .filter(
            F.col(
                "customer_sk"
            ).isNull()
        )
        .count()
    )

    missing_product = (
        fact_sales
        .filter(
            F.col(
                "product_sk"
            ).isNull()
        )
        .count()
    )

    duplicate_sales = (
        fact_sales
        .groupBy(
            "order_item_id"
        )
        .count()
        .filter(
            F.col(
                "count"
            ) > 1
        )
        .count()
    )

    negative_sales = (
        fact_sales
        .filter(
            F.col(
                "net_amount"
            ) < 0
        )
        .count()
    )

    # ------------------------------------------------------
    # FACT RETURNS CHECKS
    # ------------------------------------------------------

    missing_return_customer = (
        fact_returns
        .filter(
            F.col(
                "customer_sk"
            ).isNull()
        )
        .count()
    )

    missing_return_product = (
        fact_returns
        .filter(
            F.col(
                "product_sk"
            ).isNull()
        )
        .count()
    )

    duplicate_returns = (
        fact_returns
        .groupBy(
            "return_id"
        )
        .count()
        .filter(
            F.col(
                "count"
            ) > 1
        )
        .count()
    )

    negative_refunds = (
        fact_returns
        .filter(
            F.col(
                "refund_amount"
            ) < 0
        )
        .count()
    )

    # ------------------------------------------------------
    # SCD2 CHECKS
    # ------------------------------------------------------

    multiple_current_customers = (
        dim_customer
        .filter(
            F.col(
                "is_current"
            )
            == True
        )
        .groupBy(
            "customer_id"
        )
        .count()
        .filter(
            F.col(
                "count"
            ) > 1
        )
        .count()
    )

    invalid_scd_dates = (
        dim_customer
        .filter(
            F.col(
                "valid_to"
            ).isNotNull()
            &
            (
                F.col(
                    "valid_to"
                )
                <=
                F.col(
                    "valid_from"
                )
            )
        )
        .count()
    )

    print(
        f"Missing sales customer keys: "
        f"{missing_customer:,}"
    )

    print(
        f"Missing sales product keys: "
        f"{missing_product:,}"
    )

    print(
        f"Duplicate sales rows: "
        f"{duplicate_sales:,}"
    )

    print(
        f"Negative net sales: "
        f"{negative_sales:,}"
    )

    print(
        f"Missing return customer keys: "
        f"{missing_return_customer:,}"
    )

    print(
        f"Missing return product keys: "
        f"{missing_return_product:,}"
    )

    print(
        f"Duplicate return rows: "
        f"{duplicate_returns:,}"
    )

    print(
        f"Negative refunds: "
        f"{negative_refunds:,}"
    )

    print(
        f"Customers with multiple "
        f"current SCD versions: "
        f"{multiple_current_customers:,}"
    )

    print(
        f"Invalid SCD date ranges: "
        f"{invalid_scd_dates:,}"
    )

    total_issues = (
        missing_customer
        + missing_product
        + duplicate_sales
        + negative_sales
        + missing_return_customer
        + missing_return_product
        + duplicate_returns
        + negative_refunds
        + multiple_current_customers
        + invalid_scd_dates
    )

    if total_issues > 0:

        raise RuntimeError(
            f"Gold data-quality checks "
            f"failed with "
            f"{total_issues} issue(s)."
        )

    print()
    print(
        "Gold quality checks: PASS"
    )


# ==========================================================
# MAIN
# ==========================================================

def main():
    spark = get_spark()

    try:

        print()

        print(
            "RetailPulse Gold "
            "Dimensional Pipeline"
        )

        GOLD_ROOT.mkdir(
            parents=True,
            exist_ok=True,
        )

        # --------------------------------------------------
        # DIM CUSTOMER
        # --------------------------------------------------

        run_audited_gold_step(
            pipeline_name=(
                "gold_dim_customer"
            ),
            source_name=(
                "lakehouse/silver/customers"
            ),
            target_name=(
                "lakehouse/gold/dim_customer"
            ),
            build_function=(
                build_dim_customer
            ),
            spark=spark,
        )

        # --------------------------------------------------
        # DIM PRODUCT
        # --------------------------------------------------

        run_audited_gold_step(
            pipeline_name=(
                "gold_dim_product"
            ),
            source_name=(
                "lakehouse/silver/products"
            ),
            target_name=(
                "lakehouse/gold/dim_product"
            ),
            build_function=(
                build_dim_product
            ),
            spark=spark,
        )

        # --------------------------------------------------
        # DIM DATE
        # --------------------------------------------------

        run_audited_gold_step(
            pipeline_name=(
                "gold_dim_date"
            ),
            source_name=(
                "lakehouse/silver/orders"
            ),
            target_name=(
                "lakehouse/gold/dim_date"
            ),
            build_function=(
                build_dim_date
            ),
            spark=spark,
        )

        # --------------------------------------------------
        # FACT SALES
        # --------------------------------------------------

        run_audited_gold_step(
            pipeline_name=(
                "gold_fact_sales"
            ),
            source_name=(
                "silver/orders + "
                "silver/order_items"
            ),
            target_name=(
                "lakehouse/gold/fact_sales"
            ),
            build_function=(
                build_fact_sales
            ),
            spark=spark,
        )

        # --------------------------------------------------
        # FACT RETURNS
        # --------------------------------------------------

        run_audited_gold_step(
            pipeline_name=(
                "gold_fact_returns"
            ),
            source_name=(
                "silver/returns + "
                "silver/order_items + "
                "silver/orders"
            ),
            target_name=(
                "lakehouse/gold/fact_returns"
            ),
            build_function=(
                build_fact_returns
            ),
            spark=spark,
        )

        # --------------------------------------------------
        # DATA QUALITY
        # --------------------------------------------------

        run_gold_quality_checks(
            spark
        )

        print()

        print(
            "Gold pipeline completed."
        )

    finally:

        spark.stop()


if __name__ == "__main__":
    main()