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


def extract_table(table_name):
    config = TABLE_CONFIG[table_name]

    source_table = config["source_table"]
    watermark_column = config["watermark_column"]

    pipeline_name = f"bronze_{table_name}"

    with get_connection() as connection:
        with connection.cursor() as cursor:
            try:
                mark_pipeline_started(
                    cursor,
                    pipeline_name,
                    source_table,
                )

                connection.commit()

                previous_watermark = get_watermark(
                    cursor,
                    pipeline_name,
                )

                print(
                    f"\nPipeline: {pipeline_name}"
                )

                print(
                    f"Previous watermark: "
                    f"{previous_watermark}"
                )

                if previous_watermark is None:
                    query = f"""
                        SELECT *
                        FROM {source_table}
                        ORDER BY {watermark_column}
                    """

                    params = None

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

                extraction_time = (
                    datetime.now(timezone.utc)
                )

                df["_ingested_at"] = (
                    extraction_time
                )

                df["_source_table"] = (
                    source_table
                )

                max_watermark = (
                    df[watermark_column].max()
                )

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
                    extraction_time
                    .strftime(
                        "%Y%m%dT%H%M%S%fZ"
                    )
                    + ".parquet"
                )

                output_path = (
                    output_directory
                    / filename
                )

                df.to_parquet(
                    output_path,
                    index=False,
                    engine="pyarrow",
                )

                mark_pipeline_success(
                    cursor,
                    pipeline_name,
                    max_watermark,
                    rows_extracted,
                )

                connection.commit()

                print(
                    f"Written to: "
                    f"{output_path}"
                )

                print(
                    f"New watermark: "
                    f"{max_watermark}"
                )

            except Exception as error:
                connection.rollback()

                mark_pipeline_failed(
                    cursor,
                    pipeline_name,
                    error,
                )

                connection.commit()

                raise


if __name__ == "__main__":
    for table in TABLE_CONFIG:
        extract_table(table)