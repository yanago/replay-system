-- V1: initial schema for replay_jobs

CREATE TABLE IF NOT EXISTS replay_jobs (
    job_id           TEXT             PRIMARY KEY,
    source_table     TEXT             NOT NULL,
    target_topic     TEXT             NOT NULL,
    from_time        TIMESTAMPTZ      NOT NULL,
    to_time          TIMESTAMPTZ      NOT NULL,
    speed_multiplier DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    status           TEXT             NOT NULL,
    created_at       TIMESTAMPTZ      NOT NULL,
    updated_at       TIMESTAMPTZ      NOT NULL,
    events_published BIGINT           NOT NULL DEFAULT 0,
    error_message    TEXT
);

CREATE INDEX IF NOT EXISTS idx_replay_jobs_status
    ON replay_jobs (status);

CREATE INDEX IF NOT EXISTS idx_replay_jobs_created_at
    ON replay_jobs (created_at DESC);
