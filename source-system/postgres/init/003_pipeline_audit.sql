CREATE SCHEMA IF NOT EXISTS metadata;


CREATE TABLE IF NOT EXISTS metadata.pipeline_runs (
    run_id UUID PRIMARY KEY,

    pipeline_name VARCHAR(200) NOT NULL,
    layer VARCHAR(50) NOT NULL,

    source_name VARCHAR(300),
    target_name VARCHAR(300),

    status VARCHAR(30) NOT NULL,

    run_started_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    run_completed_at TIMESTAMPTZ,

    duration_seconds NUMERIC(12, 3),

    rows_read BIGINT NOT NULL DEFAULT 0,
    rows_written BIGINT NOT NULL DEFAULT 0,
    rows_quarantined BIGINT NOT NULL DEFAULT 0,

    watermark_from TIMESTAMP,
    watermark_to TIMESTAMP,

    error_message TEXT,

    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pipeline_runs_status_check
        CHECK (
            status IN (
                'RUNNING',
                'SUCCESS',
                'FAILED'
            )
        )
);


CREATE INDEX IF NOT EXISTS idx_pipeline_runs_pipeline_name
    ON metadata.pipeline_runs (
        pipeline_name
    );


CREATE INDEX IF NOT EXISTS idx_pipeline_runs_started_at
    ON metadata.pipeline_runs (
        run_started_at DESC
    );


CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status
    ON metadata.pipeline_runs (
        status
    );