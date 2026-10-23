-- database/migrations/agent_registry_init.sql
-- Agent Discovery registry schema.

CREATE TABLE IF NOT EXISTS agent_registry (
    agent_id            TEXT PRIMARY KEY,
    name                TEXT NOT NULL,
    version             TEXT NOT NULL DEFAULT '1.0.0',
    capabilities        TEXT[] NOT NULL DEFAULT '{}',
    endpoint            TEXT NOT NULL,
    required_permissions TEXT[] NOT NULL DEFAULT '{}',
    timeout_seconds     INTEGER NOT NULL DEFAULT 30,
    enabled             BOOLEAN NOT NULL DEFAULT TRUE,
    prompt_version      TEXT NOT NULL DEFAULT 'v1',
    input_schema        JSONB NOT NULL DEFAULT '{}',
    output_schema       JSONB NOT NULL DEFAULT '{}',
    last_health_check   TIMESTAMPTZ,
    registered_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
