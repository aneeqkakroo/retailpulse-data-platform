CREATE SCHEMA IF NOT EXISTS metadata;


CREATE TABLE IF NOT EXISTS metadata.pipeline_watermarks (
    pipeline_name VARCHAR(200) PRIMARY KEY,
    source_table VARCHAR(200) NOT NULL,

    last_watermark TIMESTAMP,

    last_run_started_at TIMESTAMP,
    last_run_completed_at TIMESTAMP,

    last_rows_extracted BIGINT DEFAULT 0,

    status VARCHAR(30),
    error_message TEXT,

    updated_at TIMESTAMP
        NOT NULL
        DEFAULT CURRENT_TIMESTAMP
);