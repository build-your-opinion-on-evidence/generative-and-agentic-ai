-- database/migrations/policy_init.sql
-- Policy Engine database schema.

CREATE TABLE IF NOT EXISTS policy_rules (
    rule_id         TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    description     TEXT,
    action_type     TEXT NOT NULL,        -- FREEZE_CARD / BLOCK_TRANSACTION / NOTIFY_CUSTOMER etc.
    condition       JSONB NOT NULL DEFAULT '{}',
    decision        TEXT NOT NULL,        -- AUTO_ALLOW / AUTO_DENY / REQUIRE_APPROVAL
    priority        INTEGER NOT NULL DEFAULT 50,
    enabled         BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Immutable audit of every policy decision
CREATE TABLE IF NOT EXISTS policy_decisions (
    decision_id     TEXT PRIMARY KEY,
    rule_id         TEXT REFERENCES policy_rules(rule_id),
    task_id         TEXT NOT NULL,
    correlation_id  TEXT NOT NULL,
    action_type     TEXT NOT NULL,
    action_payload  JSONB NOT NULL DEFAULT '{}',
    decision        TEXT NOT NULL,        -- AUTO_ALLOW / AUTO_DENY / REQUIRE_APPROVAL
    rationale       TEXT,
    decided_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_policy_decisions_task_id ON policy_decisions(task_id);

-- Seed default rules
INSERT INTO policy_rules (rule_id, name, action_type, decision, priority)
VALUES
    ('rule-freeze-card-high-risk',    'Freeze card — high-risk score',    'FREEZE_CARD',         'REQUIRE_APPROVAL', 90),
    ('rule-freeze-card-stolen',       'Freeze card — reported stolen',     'FREEZE_CARD',         'AUTO_ALLOW',       100),
    ('rule-block-tx-suspicious',      'Block tx — suspicious pattern',     'BLOCK_TRANSACTION',   'REQUIRE_APPROVAL', 80),
    ('rule-notify-customer-always',   'Notify customer — always allowed',  'NOTIFY_CUSTOMER',     'AUTO_ALLOW',       10),
    ('rule-unblock-card-low-risk',    'Unblock card — low risk',           'UNBLOCK_CARD',        'AUTO_ALLOW',       20)
ON CONFLICT (rule_id) DO NOTHING;
