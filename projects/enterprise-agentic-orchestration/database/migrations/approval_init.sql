-- database/migrations/approval_init.sql
-- Human Approval Service database schema.

CREATE TABLE IF NOT EXISTS approval_requests (
    approval_id     TEXT PRIMARY KEY,
    task_id         TEXT NOT NULL,
    saga_id         TEXT,
    correlation_id  TEXT NOT NULL,
    action_type     TEXT NOT NULL,
    action_payload  JSONB NOT NULL DEFAULT '{}',
    requested_by    TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'PENDING',  -- PENDING / APPROVED / REJECTED / EXPIRED
    reviewer_id     TEXT,
    reviewer_note   TEXT,
    expires_at      TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    decided_at      TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_approval_task_id ON approval_requests(task_id);
CREATE INDEX IF NOT EXISTS idx_approval_status ON approval_requests(status);
