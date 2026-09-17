"""SQLite storage layer.

A single connection guarded by an RLock is used. Every mutating operation runs
inside a `BEGIN IMMEDIATE` transaction, so claim/ack/fail sequences are
serialized and a task can never be handed out twice concurrently.
"""

from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    seq              INTEGER PRIMARY KEY AUTOINCREMENT,
    id               TEXT NOT NULL UNIQUE,
    group_key        TEXT NOT NULL,
    idempotency_key  TEXT NOT NULL UNIQUE,
    payload          TEXT NOT NULL,
    payload_hash     TEXT NOT NULL,
    status           TEXT NOT NULL DEFAULT 'pending'
                     CHECK (status IN ('pending', 'leased', 'succeeded', 'dead')),
    attempts         INTEGER NOT NULL DEFAULT 0,
    max_attempts     INTEGER NOT NULL,
    lease_token      TEXT,
    lease_expires_at REAL,
    last_error       TEXT,
    created_at       REAL NOT NULL,
    updated_at       REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_tasks_group_seq ON tasks (group_key, seq);
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks (status);
"""


class Database:
    def __init__(self, path: str):
        if path != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.executescript(SCHEMA)

    @contextmanager
    def transaction(self):
        """Exclusive write transaction (BEGIN IMMEDIATE ... COMMIT/ROLLBACK)."""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                yield self._conn
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
            else:
                self._conn.execute("COMMIT")

    @contextmanager
    def read(self):
        with self._lock:
            yield self._conn

    def close(self):
        with self._lock:
            self._conn.close()
