from uuid import uuid4


def start_pipeline_run(
    cursor,
    pipeline_name,
    layer,
    source_name=None,
    target_name=None,
):
    """
    Create an audit record representing a running pipeline.
    """

    run_id = uuid4()

    cursor.execute(
        """
        INSERT INTO metadata.pipeline_runs (
            run_id,
            pipeline_name,
            layer,
            source_name,
            target_name,
            status,
            run_started_at
        )
        VALUES (
            %s,
            %s,
            %s,
            %s,
            %s,
            'RUNNING',
            CURRENT_TIMESTAMP
        )
        """,
        (
            run_id,
            pipeline_name,
            layer,
            source_name,
            target_name,
        ),
    )

    return run_id


def complete_pipeline_run(
    cursor,
    run_id,
    rows_read=0,
    rows_written=0,
    rows_quarantined=0,
    watermark_from=None,
    watermark_to=None,
):
    """
    Mark a pipeline run as successfully completed.
    """

    cursor.execute(
        """
        UPDATE metadata.pipeline_runs
        SET
            status = 'SUCCESS',
            run_completed_at = CURRENT_TIMESTAMP,

            duration_seconds =
                EXTRACT(
                    EPOCH FROM (
                        CURRENT_TIMESTAMP
                        - run_started_at
                    )
                ),

            rows_read = %s,
            rows_written = %s,
            rows_quarantined = %s,

            watermark_from = %s,
            watermark_to = %s,

            error_message = NULL

        WHERE run_id = %s
        """,
        (
            rows_read,
            rows_written,
            rows_quarantined,
            watermark_from,
            watermark_to,
            run_id,
        ),
    )


def fail_pipeline_run(
    cursor,
    run_id,
    error,
    rows_read=0,
    rows_written=0,
    rows_quarantined=0,
    watermark_from=None,
    watermark_to=None,
):
    """
    Mark a pipeline run as failed.
    """

    cursor.execute(
        """
        UPDATE metadata.pipeline_runs
        SET
            status = 'FAILED',
            run_completed_at = CURRENT_TIMESTAMP,

            duration_seconds =
                EXTRACT(
                    EPOCH FROM (
                        CURRENT_TIMESTAMP
                        - run_started_at
                    )
                ),

            rows_read = %s,
            rows_written = %s,
            rows_quarantined = %s,

            watermark_from = %s,
            watermark_to = %s,

            error_message = %s

        WHERE run_id = %s
        """,
        (
            rows_read,
            rows_written,
            rows_quarantined,
            watermark_from,
            watermark_to,
            str(error)[:5000],
            run_id,
        ),
    )