from datetime import datetime


def get_watermark(
    cursor,
    pipeline_name,
):
    cursor.execute(
        """
        SELECT last_watermark
        FROM metadata.pipeline_watermarks
        WHERE pipeline_name = %s
        """,
        (pipeline_name,),
    )

    row = cursor.fetchone()

    if row is None:
        return None

    return row[0]


def mark_pipeline_started(
    cursor,
    pipeline_name,
    source_table,
):
    cursor.execute(
        """
        INSERT INTO metadata.pipeline_watermarks (
            pipeline_name,
            source_table,
            last_run_started_at,
            status,
            updated_at
        )
        VALUES (
            %s,
            %s,
            CURRENT_TIMESTAMP,
            'RUNNING',
            CURRENT_TIMESTAMP
        )
        ON CONFLICT (pipeline_name)
        DO UPDATE SET
            last_run_started_at =
                CURRENT_TIMESTAMP,
            status = 'RUNNING',
            error_message = NULL,
            updated_at = CURRENT_TIMESTAMP
        """,
        (
            pipeline_name,
            source_table,
        ),
    )


def mark_pipeline_success(
    cursor,
    pipeline_name,
    watermark,
    rows_extracted,
):
    cursor.execute(
        """
        UPDATE metadata.pipeline_watermarks
        SET
            last_watermark = %s,
            last_run_completed_at =
                CURRENT_TIMESTAMP,
            last_rows_extracted = %s,
            status = 'SUCCESS',
            error_message = NULL,
            updated_at = CURRENT_TIMESTAMP
        WHERE pipeline_name = %s
        """,
        (
            watermark,
            rows_extracted,
            pipeline_name,
        ),
    )


def mark_pipeline_failed(
    cursor,
    pipeline_name,
    error_message,
):
    cursor.execute(
        """
        UPDATE metadata.pipeline_watermarks
        SET
            last_run_completed_at =
                CURRENT_TIMESTAMP,
            status = 'FAILED',
            error_message = %s,
            updated_at = CURRENT_TIMESTAMP
        WHERE pipeline_name = %s
        """,
        (
            str(error_message),
            pipeline_name,
        ),
    )