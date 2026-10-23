-- database/migrations/banking_core_init.sql
-- Synthetic banking core schema — no real personal data.

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ── customers ────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS customers (
    customer_id     TEXT PRIMARY KEY,
    full_name       TEXT NOT NULL,
    national_id_hash TEXT NOT NULL,
    risk_segment    TEXT NOT NULL DEFAULT 'STANDARD',  -- LOW / STANDARD / HIGH / CRITICAL
    home_country    TEXT NOT NULL DEFAULT 'EG',
    phone_number    TEXT NOT NULL,
    phone_changed_at TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── accounts ────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS accounts (
    account_id      TEXT PRIMARY KEY,
    customer_id     TEXT NOT NULL REFERENCES customers(customer_id),
    account_type    TEXT NOT NULL DEFAULT 'CURRENT',  -- CURRENT / SAVINGS / BUSINESS
    currency        TEXT NOT NULL DEFAULT 'EGP',
    balance         NUMERIC(15,2) NOT NULL DEFAULT 0.00,
    status          TEXT NOT NULL DEFAULT 'ACTIVE'    -- ACTIVE / FROZEN / CLOSED
);

-- ── cards ────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS cards (
    card_id         TEXT PRIMARY KEY,
    customer_id     TEXT NOT NULL REFERENCES customers(customer_id),
    account_id      TEXT NOT NULL REFERENCES accounts(account_id),
    masked_pan      TEXT NOT NULL,
    card_type       TEXT NOT NULL DEFAULT 'DEBIT',    -- DEBIT / CREDIT / PREPAID
    status          TEXT NOT NULL DEFAULT 'ACTIVE',   -- ACTIVE / FROZEN / CANCELLED / REPORTED_STOLEN
    issued_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── transactions ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id  TEXT PRIMARY KEY,
    card_id         TEXT NOT NULL REFERENCES cards(card_id),
    customer_id     TEXT NOT NULL REFERENCES customers(customer_id),
    merchant_id     TEXT NOT NULL,
    amount          NUMERIC(12,2) NOT NULL,
    currency        TEXT NOT NULL DEFAULT 'EGP',
    country         TEXT NOT NULL DEFAULT 'EG',
    channel         TEXT NOT NULL DEFAULT 'POS',      -- POS / ONLINE / ATM / MOBILE
    transaction_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status          TEXT NOT NULL DEFAULT 'APPROVED', -- APPROVED / DECLINED / REVERSED / PENDING
    fraud_label     BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_transactions_card_id ON transactions(card_id);
CREATE INDEX IF NOT EXISTS idx_transactions_customer_id ON transactions(customer_id);
CREATE INDEX IF NOT EXISTS idx_transactions_time ON transactions(transaction_time DESC);
CREATE INDEX IF NOT EXISTS idx_transactions_fraud ON transactions(fraud_label) WHERE fraud_label = TRUE;

-- ── devices ─────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS devices (
    device_id       TEXT PRIMARY KEY,
    customer_id     TEXT NOT NULL REFERENCES customers(customer_id),
    fingerprint_hash TEXT NOT NULL,
    trusted         BOOLEAN NOT NULL DEFAULT FALSE,
    first_seen_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    country         TEXT NOT NULL DEFAULT 'EG'
);

CREATE INDEX IF NOT EXISTS idx_devices_customer_id ON devices(customer_id);

-- ── login_events ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS login_events (
    login_id        TEXT PRIMARY KEY DEFAULT gen_random_uuid()::TEXT,
    customer_id     TEXT NOT NULL REFERENCES customers(customer_id),
    device_id       TEXT REFERENCES devices(device_id),
    login_time      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    country         TEXT NOT NULL DEFAULT 'EG',
    success         BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE INDEX IF NOT EXISTS idx_login_events_customer_id ON login_events(customer_id);
CREATE INDEX IF NOT EXISTS idx_login_events_time ON login_events(login_time DESC);
