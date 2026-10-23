-- database/migrations/orchestrator_init.sql
-- Integration Orchestrator database schema (saga state).

CREATE TABLE IF NOT EXISTS saga_instances (
    saga_id         TEXT PRIMARY KEY,
    task_id         TEXT NOT NULL UNIQUE,
    correlation_id  TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'PENDING',
    strategy        TEXT,            -- DAG | REACT
    dag_run_id      TEXT,
    current_step    TEXT,
    context         JSONB NOT NULL DEFAULT '{}',
    timeout_at      TIMESTAMPTZ,
    version         INTEGER NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_saga_task_id ON saga_instances(task_id);
CREATE INDEX IF NOT EXISTS idx_saga_status ON saga_instances(status);

-- Immutable checkpoint history for resume-after-failure
CREATE TABLE IF NOT EXISTS saga_checkpoints (
    id              BIGSERIAL PRIMARY KEY,
    saga_id         TEXT NOT NULL REFERENCES saga_instances(saga_id),
    step_name       TEXT NOT NULL,
    step_status     TEXT NOT NULL,
    payload         JSONB NOT NULL DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_saga_checkpoints_saga_id ON saga_checkpoints(saga_id);
