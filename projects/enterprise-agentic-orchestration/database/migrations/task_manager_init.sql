-- database/migrations/task_manager_init.sql
-- Task Manager database schema.

CREATE TABLE IF NOT EXISTS tasks (
    task_id         TEXT PRIMARY KEY,
    correlation_id  TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'RECEIVED',
    priority        INTEGER NOT NULL DEFAULT 50,
    task_type       TEXT NOT NULL,
    source_event_id TEXT,
    subject         TEXT,
    context         JSONB NOT NULL DEFAULT '{}',
    version         INTEGER NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tasks_correlation_id ON tasks(correlation_id);
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
CREATE INDEX IF NOT EXISTS idx_tasks_priority ON tasks(priority DESC, created_at ASC);

-- Immutable lifecycle history
CREATE TABLE IF NOT EXISTS task_events (
    id              BIGSERIAL PRIMARY KEY,
    task_id         TEXT NOT NULL REFERENCES tasks(task_id),
    from_status     TEXT,
    to_status       TEXT NOT NULL,
    actor           TEXT,
    reason          TEXT,
    metadata        JSONB NOT NULL DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_task_events_task_id ON task_events(task_id);

-- Deduplication: prevents processing the same raw event twice
CREATE TABLE IF NOT EXISTS task_dedup_keys (
    event_id        TEXT PRIMARY KEY,
    task_id         TEXT NOT NULL,
    received_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
