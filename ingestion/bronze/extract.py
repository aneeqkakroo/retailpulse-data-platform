from datetime import datetime, timezone
from pathlib import Path
import sys


import pandas as pd


# ==========================================================
# PROJECT PATH
# ==========================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

BRONZE_ROOT = (
    PROJECT_ROOT
    / "bronze"
)


# Allow shared pipeline utilities to be imported.
sys.path.append(
    str(
        PROJECT_ROOT
        / "pipeline_utils"
    )
)

from adls import upload_file
from audit import (
    start_pipeline_run,
    complete_pipeline_run,
    fail_pipeline_run,
)

from config import TABLE_CONFIG
from db import get_connection

from watermark import (
    get_watermark,
    mark_pipeline_failed,
    mark_pipeline_started,
    mark_pipeline_success,
)


# ==========================================================
# TIMESTAMP STANDARDISATION
# ==========================================================

def standardise_timestamp_columns(df):
    """
    Standardise timestamp columns before writing Parquet.

    This ensures Bronze Parquet files maintain a consistent
    physical timestamp representation across ingestion runs.
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


# ==========================================================
# TABLE EXTRACTION
# ==========================================================

def extract_table(table_name):

    config = TABLE_CONFIG[
        table_name
    ]

    source_table = config[
        "source_table"
    ]

    watermark_column = config[
        "watermark_column"
    ]

    pipeline_name = (
        f"bronze_{table_name}"
    )

    target_name = (
        f"bronze/{table_name}"
    )

    run_id = None

    previous_watermark = None
    max_watermark = None

    rows_extracted = 0
    rows_written = 0

    with get_connection() as connection:

        with connection.cursor() as cursor:

            try:

                # ==================================================
                # START AUDIT RUN
                # ==================================================

                run_id = start_pipeline_run(
                    cursor=cursor,
                    pipeline_name=pipeline_name,
                    layer="BRONZE",
                    source_name=source_table,
                    target_name=target_name,
                )

                # Commit immediately so the RUNNING record survives
                # even if later pipeline processing fails.
                connection.commit()

                print()
                print("=" * 70)

                print(
                    f"Run ID:   {run_id}"
                )

                print(
                    f"Pipeline: {pipeline_name}"
                )

                print(
                    f"Source:   {source_table}"
                )

                print(
                    f"Target:   {target_name}"
                )

                # ==================================================
                # MARK WATERMARK PIPELINE START
                # ==================================================

                mark_pipeline_started(
                    cursor,
                    pipeline_name,
                    source_table,
                )

                connection.commit()

                # ==================================================
                # GET CURRENT WATERMARK
                # ==================================================

                previous_watermark = (
                    get_watermark(
                        cursor,
                        pipeline_name,
                    )
                )

                print(
                    f"Previous watermark: "
                    f"{previous_watermark}"
                )

                print("=" * 70)

                # ==================================================
                # BUILD EXTRACTION QUERY
                # ==================================================

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

                # ==================================================
                # EXTRACT
                # ==================================================

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

                # ==================================================
                # NO CHANGES
                # ==================================================

                if df.empty:

                    mark_pipeline_success(
                        cursor,
                        pipeline_name,
                        previous_watermark,
                        0,
                    )

                    complete_pipeline_run(
                        cursor=cursor,
                        run_id=run_id,
                        rows_read=0,
                        rows_written=0,
                        rows_quarantined=0,
                        watermark_from=previous_watermark,
                        watermark_to=previous_watermark,
                    )

                    connection.commit()

                    print(
                        "No new or changed rows."
                    )

                    print(
                        "Pipeline status: SUCCESS"
                    )

                    return

                # ==================================================
                # STANDARDISE TIMESTAMPS
                # ==================================================

                df = (
                    standardise_timestamp_columns(
                        df
                    )
                )

                # ==================================================
                # CALCULATE NEXT WATERMARK
                # ==================================================

                max_watermark = (
                    df[
                        watermark_column
                    ].max()
                )

                if pd.isna(
                    max_watermark
                ):
                    raise RuntimeError(
                        f"Unable to determine "
                        f"watermark for "
                        f"{source_table}"
                    )

                # ==================================================
                # TECHNICAL BRONZE METADATA
                # ==================================================

                extraction_time = (
                    datetime.now(
                        timezone.utc
                    )
                )

                ingestion_timestamp = (
                    pd.Timestamp(
                        extraction_time.replace(
                            tzinfo=None
                        )
                    )
                )

                df[
                    "_ingested_at"
                ] = ingestion_timestamp

                df[
                    "_source_table"
                ] = source_table

                df[
                    "_pipeline_run_id"
                ] = str(
                    run_id
                )

                # ==================================================
                # OUTPUT PATH
                # ==================================================

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
                    +
                    ".parquet"
                )

                output_path = (
                    output_directory
                    / filename
                )

                # ==================================================
                # WRITE PARQUET
                # ==================================================

                df.to_parquet(
                    output_path,
                    index=False,
                    engine="pyarrow",
                    coerce_timestamps="us",
                    allow_truncated_timestamps=True,
                )
                relative_path = output_path.relative_to(
                    PROJECT_ROOT
                )

                remote_path = relative_path.as_posix()

                upload_file(
                    local_path=output_path,
                    remote_path=remote_path,
                )

                rows_written = (
                    rows_extracted
                )

                # ==================================================
                # UPDATE WATERMARK
                # ==================================================

                mark_pipeline_success(
                    cursor,
                    pipeline_name,
                    max_watermark,
                    rows_extracted,
                )

                # ==================================================
                # COMPLETE AUDIT
                # ==================================================

                complete_pipeline_run(
                    cursor=cursor,
                    run_id=run_id,
                    rows_read=rows_extracted,
                    rows_written=rows_written,
                    rows_quarantined=0,
                    watermark_from=previous_watermark,
                    watermark_to=max_watermark,
                )

                connection.commit()

                # ==================================================
                # LOGGING
                # ==================================================

                print(
                    f"Written to: "
                    f"{output_path}"
                )

                print(
                    f"Rows written: "
                    f"{rows_written:,}"
                )

                print(
                    f"New watermark: "
                    f"{max_watermark}"
                )

                print(
                    "Pipeline status: SUCCESS"
                )

            except Exception as error:

                # ==================================================
                # FAILURE HANDLING
                # ==================================================

                connection.rollback()

                try:

                    mark_pipeline_failed(
                        cursor,
                        pipeline_name,
                        error,
                    )

                    if run_id is not None:

                        fail_pipeline_run(
                            cursor=cursor,
                            run_id=run_id,
                            error=error,
                            rows_read=rows_extracted,
                            rows_written=rows_written,
                            rows_quarantined=0,
                            watermark_from=previous_watermark,
                            watermark_to=max_watermark,
                        )

                    connection.commit()

                except Exception as audit_error:

                    connection.rollback()

                    print(
                        "WARNING: Could not "
                        "persist failure audit."
                    )

                    print(
                        f"Audit error: "
                        f"{audit_error}"
                    )

                print()
                print(
                    "Pipeline status: FAILED"
                )

                print(
                    f"Error: {error}"
                )

                raise


# ==========================================================
# MAIN
# ==========================================================

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