WITH latest_runs AS (
    SELECT
        pipeline_name,
        layer,
        status,
        rows_read,
        rows_written,
        rows_quarantined,
        duration_seconds,
        error_message,
        run_started_at,
        run_completed_at,

        ROW_NUMBER() OVER (
            PARTITION BY pipeline_name
            ORDER BY run_started_at DESC
        ) AS row_number

    FROM metadata.pipeline_runs
)

SELECT
    layer,
    pipeline_name,
    status,
    rows_read,
    rows_written,
    rows_quarantined,
    ROUND(
        duration_seconds,
        2
    ) AS duration_seconds,
    run_started_at,
    run_completed_at,
    error_message

FROM latest_runs

WHERE row_number = 1

ORDER BY
    CASE layer
        WHEN 'ORCHESTRATION' THEN 1
        WHEN 'BRONZE' THEN 2
        WHEN 'SILVER' THEN 3
        WHEN 'GOLD' THEN 4
        ELSE 5
    END,
    pipeline_name;