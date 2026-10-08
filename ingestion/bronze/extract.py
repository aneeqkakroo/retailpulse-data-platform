from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from config import TABLE_CONFIG
from db import get_connection
from watermark import (
    get_watermark,
    mark_pipeline_failed,
    mark_pipeline_started,
    mark_pipeline_success,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
BRONZE_ROOT = PROJECT_ROOT / "bronze"


def standardise_timestamp_columns(df):
    """
    Enforce consistent timestamp types before writing Parquet.

    PostgreSQL TIMESTAMP columns are treated as timezone-naive
    timestamps in Bronze. This avoids incompatible Parquet
    timestamp representations across different ingestion runs.
    """

    timestamp_columns = [
        "created_at",
        "updated_at",
        "order_date",
        "return_date",
        "signup_date",
    ]

    for column in timestamp_columns:
        if column in df.columns:
            df[column] = pd.to_datetime(
                df[column],
                errors="coerce",
            )

    return df


def extract_table(table_name):
    config = TABLE_CONFIG[table_name]

    source_table = config["source_table"]
    watermark_column = config["watermark_column"]

    pipeline_name = f"bronze_{table_name}"

    with get_connection() as connection:
        with connection.cursor() as cursor:
            try:
                # --------------------------------------------------
                # Mark pipeline as started
                # --------------------------------------------------

                mark_pipeline_started(
                    cursor,
                    pipeline_name,
                    source_table,
                )

                connection.commit()

                # --------------------------------------------------
                # Retrieve previous watermark
                # --------------------------------------------------

                previous_watermark = get_watermark(
                    cursor,
                    pipeline_name,
                )

                print()
                print("=" * 60)
                print(f"Pipeline: {pipeline_name}")
                print(f"Source:   {source_table}")
                print(
                    f"Previous watermark: "
                    f"{previous_watermark}"
                )
                print("=" * 60)

                # --------------------------------------------------
                # Build extraction query
                # --------------------------------------------------

                if previous_watermark is None:
                    query = f"""
                        SELECT *
                        FROM {source_table}
                        ORDER BY {watermark_column}
                    """

                    params = None

                    print(
                        "Mode: INITIAL FULL LOAD"
                    )

                else:
                    query = f"""
                        SELECT *
                        FROM {source_table}
                        WHERE {watermark_column} > %s
                        ORDER BY {watermark_column}
                    """

                    params = (
                        previous_watermark,
                    )

                    print(
                        "Mode: INCREMENTAL LOAD"
                    )

                # --------------------------------------------------
                # Extract from PostgreSQL
                # --------------------------------------------------

                df = pd.read_sql_query(
                    query,
                    connection,
                    params=params,
                )

                rows_extracted = len(df)

                print(
                    f"Rows extracted: "
                    f"{rows_extracted:,}"
                )

                # --------------------------------------------------
                # Nothing new to process
                # --------------------------------------------------

                if df.empty:
                    mark_pipeline_success(
                        cursor,
                        pipeline_name,
                        previous_watermark,
                        0,
                    )

                    connection.commit()

                    print(
                        "No new or changed rows."
                    )

                    return

                # --------------------------------------------------
                # Standardise timestamps
                # --------------------------------------------------

                df = standardise_timestamp_columns(
                    df
                )

                # --------------------------------------------------
                # Determine new source watermark
                # --------------------------------------------------

                max_watermark = (
                    df[watermark_column].max()
                )

                if pd.isna(max_watermark):
                    raise RuntimeError(
                        f"Unable to determine watermark "
                        f"for {source_table}"
                    )

                # --------------------------------------------------
                # Add Bronze technical metadata
                # --------------------------------------------------

                extraction_time = datetime.now(
                    timezone.utc
                )

                # Keep ingestion timestamp timezone-naive
                # for consistent Parquet/Spark handling.
                ingestion_timestamp = pd.Timestamp(
                    extraction_time.replace(
                        tzinfo=None
                    )
                )

                df["_ingested_at"] = (
                    ingestion_timestamp
                )

                df["_source_table"] = (
                    source_table
                )

                # --------------------------------------------------
                # Build partitioned output path
                # --------------------------------------------------

                load_date = (
                    extraction_time
                    .date()
                    .isoformat()
                )

                output_directory = (
                    BRONZE_ROOT
                    / table_name
                    / f"load_date={load_date}"
                )

                output_directory.mkdir(
                    parents=True,
                    exist_ok=True,
                )

                filename = (
                    extraction_time.strftime(
                        "%Y%m%dT%H%M%S%fZ"
                    )
                    + ".parquet"
                )

                output_path = (
                    output_directory
                    / filename
                )

                # --------------------------------------------------
                # Write immutable Bronze Parquet file
                # --------------------------------------------------

                df.to_parquet(
                    output_path,
                    index=False,
                    engine="pyarrow",
                    coerce_timestamps="us",
                    allow_truncated_timestamps=True,
                )

                # --------------------------------------------------
                # Update watermark only AFTER successful write
                # --------------------------------------------------

                mark_pipeline_success(
                    cursor,
                    pipeline_name,
                    max_watermark,
                    rows_extracted,
                )

                connection.commit()

                # --------------------------------------------------
                # Logging
                # --------------------------------------------------

                print(
                    f"Written to: "
                    f"{output_path}"
                )

                print(
                    f"New watermark: "
                    f"{max_watermark}"
                )

                print(
                    "Pipeline status: SUCCESS"
                )

            except Exception as error:
                # --------------------------------------------------
                # Roll back previous transaction state
                # --------------------------------------------------

                connection.rollback()

                try:
                    mark_pipeline_failed(
                        cursor,
                        pipeline_name,
                        error,
                    )

                    connection.commit()

                except Exception:
                    connection.rollback()

                print()
                print(
                    f"Pipeline status: FAILED"
                )

                print(
                    f"Error: {error}"
                )

                raise


def main():
    print()
    print(
        "RetailPulse Bronze "
        "Incremental Ingestion"
    )

    for table_name in TABLE_CONFIG:
        extract_table(
            table_name
        )

    print()
    print(
        "Bronze ingestion completed."
    )


if __name__ == "__main__":
    main()