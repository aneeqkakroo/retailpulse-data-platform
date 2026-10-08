from datetime import datetime
from pathlib import Path
import sys


PROJECT_ROOT = (
    Path(__file__)
    .resolve()
    .parents[1]
)

sys.path.append(
    str(
        PROJECT_ROOT
        / "databricks"
        / "silver_delta"
    )
)

from merge import (
    clean_customers,
    clean_order_items,
    deduplicate_latest,
)


def test_customer_email_is_normalised(
    spark,
):
    data = [
        (
            1,
            "John",
            "Smith",
            "  TEST@EMAIL.COM  ",
            "United Kingdom",
            datetime(
                2026,
                1,
                1,
            ),
        )
    ]

    columns = [
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "country",
        "signup_date",
    ]

    df = spark.createDataFrame(
        data,
        columns,
    )

    valid, invalid = (
        clean_customers(df)
    )

    result = (
        valid
        .select("email")
        .collect()[0][0]
    )

    assert result == (
        "test@email.com"
    )

    assert invalid.count() == 0


def test_invalid_customer_is_quarantined(
    spark,
):
    data = [
        (
            1,
            None,
            "Smith",
            "test@email.com",
            "United Kingdom",
            datetime(
                2026,
                1,
                1,
            ),
        )
    ]

    columns = [
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "country",
        "signup_date",
    ]

    df = spark.createDataFrame(
        data,
        columns,
    )

    valid, invalid = (
        clean_customers(df)
    )

    assert valid.count() == 0
    assert invalid.count() == 1


def test_order_item_calculations(
    spark,
):
    data = [
        (
            1,
            100,
            50,
            2,
            20.0,
            10.0,
        )
    ]

    columns = [
        "order_item_id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "discount_pct",
    ]

    df = spark.createDataFrame(
        data,
        columns,
    )

    valid, invalid = (
        clean_order_items(df)
    )

    row = (
        valid
        .select(
            "gross_amount",
            "discount_amount",
            "net_amount",
        )
        .collect()[0]
    )

    assert row["gross_amount"] == 40.0
    assert row["discount_amount"] == 4.0
    assert row["net_amount"] == 36.0

    assert invalid.count() == 0


def test_invalid_customer_is_quarantined(
    spark,
):
    from pyspark.sql.types import (
        StructType,
        StructField,
        LongType,
        StringType,
        TimestampType,
    )

    schema = StructType(
        [
            StructField(
                "customer_id",
                LongType(),
                False,
            ),
            StructField(
                "first_name",
                StringType(),
                True,
            ),
            StructField(
                "last_name",
                StringType(),
                True,
            ),
            StructField(
                "email",
                StringType(),
                True,
            ),
            StructField(
                "country",
                StringType(),
                True,
            ),
            StructField(
                "signup_date",
                TimestampType(),
                True,
            ),
        ]
    )

    data = [
        (
            1,
            None,
            "Smith",
            "test@email.com",
            "United Kingdom",
            datetime(
                2026,
                1,
                1,
            ),
        )
    ]

    df = spark.createDataFrame(
        data,
        schema=schema,
    )

    valid, invalid = (
        clean_customers(df)
    )

    assert valid.count() == 0
    assert invalid.count() == 1


def test_deduplicate_keeps_latest_record(
    spark,
):
    data = [
        (
            1,
            "London",
            datetime(
                2026,
                1,
                1,
                10,
                0,
            ),
            datetime(
                2026,
                1,
                1,
                10,
                5,
            ),
        ),
        (
            1,
            "Manchester",
            datetime(
                2026,
                1,
                2,
                10,
                0,
            ),
            datetime(
                2026,
                1,
                2,
                10,
                5,
            ),
        ),
    ]

    columns = [
        "customer_id",
        "city",
        "updated_at",
        "_ingested_at",
    ]

    df = spark.createDataFrame(
        data,
        columns,
    )

    result = deduplicate_latest(
        df,
        "customer_id",
    )

    row = (
        result
        .select("city")
        .collect()[0][0]
    )

    assert row == "Manchester"
    assert result.count() == 1