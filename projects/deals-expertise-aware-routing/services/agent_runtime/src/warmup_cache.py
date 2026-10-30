"""Persistent warmup cache (SQLite).

A warmup produces learned state that later test runs can reuse:
- DEALS: every agent's per-type success/failure counts and private memory.
- Agentic RAG: the coordinator's shared retrieval memory.

Entries are keyed by a fingerprint of everything that shapes that state
(dataset, system, exact warmup cases, agent models, roles, routing,
executor settings and grader). Changing any of them yields a new key, so a
stale warmup is never reused silently.
"""

import hashlib
import json
import sqlite3
import time
from contextlib import closing
from pathlib import Path

SCHEMA_VERSION = 1


def _digest(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, default=str).encode()
    ).hexdigest()


def fingerprint(dataset, system, warmup_case_ids, context):
    """Return (key, details) for a warmup configuration."""
    details = {
        "schema": SCHEMA_VERSION,
        "dataset": dataset,
        "system": system,
        "warmup_cases": len(warmup_case_ids),
        "warmup_case_set": _digest(list(warmup_case_ids)),
        "context": context,
    }
    return _digest(details), details


class WarmupCache:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as db, db:
            db.execute(
                """
                CREATE TABLE IF NOT EXISTS warmup_cache (
                    key TEXT PRIMARY KEY,
                    dataset TEXT NOT NULL,
                    system TEXT NOT NULL,
                    warmup_cases INTEGER NOT NULL,
                    details TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    source_job TEXT,
                    created_at REAL NOT NULL
                )
                """
            )

    def _connect(self):
        db = sqlite3.connect(self.path, timeout=30)
        db.row_factory = sqlite3.Row
        return db

    def get(self, key):
        with closing(self._connect()) as db:
            row = db.execute(
                "SELECT * FROM warmup_cache WHERE key = ?", (key,)
            ).fetchone()
        if row is None:
            return None
        entry = dict(row)
        entry["details"] = json.loads(entry["details"])
        entry["payload"] = json.loads(entry["payload"])
        return entry

    def put(self, key, details, payload, source_job):
        with closing(self._connect()) as db, db:
            db.execute(
                """
                INSERT OR REPLACE INTO warmup_cache
                (key, dataset, system, warmup_cases, details, payload,
                 source_job, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    key, details["dataset"], details["system"],
                    details["warmup_cases"], json.dumps(details),
                    json.dumps(payload), source_job, time.time(),
                ),
            )

    def entries(self):
        with closing(self._connect()) as db:
            rows = db.execute(
                """
                SELECT key, dataset, system, warmup_cases, source_job,
                       created_at
                FROM warmup_cache ORDER BY created_at DESC
                """
            ).fetchall()
        return [dict(row) for row in rows]

    def clear(self, dataset=None):
        with closing(self._connect()) as db, db:
            if dataset:
                cursor = db.execute(
                    "DELETE FROM warmup_cache WHERE dataset = ?", (dataset,)
                )
            else:
                cursor = db.execute("DELETE FROM warmup_cache")
        return cursor.rowcount
