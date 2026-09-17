"""Core queue semantics: idempotent submit, FIFO lease claim, ack/fail, dead letter."""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from typing import Any, Iterable, Optional

from .db import Database

TERMINAL = ("succeeded", "dead")
UNFINISHED = ("pending", "leased")


class QueueError(Exception):
    """Base class carrying an HTTP status code."""

    status_code = 500

    def __init__(self, detail: str):
        super().__init__(detail)
        self.detail = detail


class PayloadConflict(QueueError):
    status_code = 409


class TaskNotFound(QueueError):
    status_code = 404


class LeaseConflict(QueueError):
    status_code = 409


class InvalidState(QueueError):
    status_code = 409


def _canonical(payload: Any) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _hash(payload: Any) -> str:
    return hashlib.sha256(_canonical(payload).encode("utf-8")).hexdigest()


def _row_to_task(row) -> dict:
    return {
        "id": row["id"],
        "group_key": row["group_key"],
        "idempotency_key": row["idempotency_key"],
        "payload": json.loads(row["payload"]),
        "status": row["status"],
        "attempts": row["attempts"],
        "max_attempts": row["max_attempts"],
        "lease_expires_at": row["lease_expires_at"],
        "last_error": row["last_error"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def submit_task(
    db: Database,
    group_key: str,
    idempotency_key: str,
    payload: Any,
    max_attempts: Optional[int],
    default_max_attempts: int,
) -> tuple[dict, bool]:
    """Insert a task, or return the existing one for a replayed idempotency key.

    Same key + identical payload -> (existing task, False).
    Same key + different payload -> PayloadConflict (409).
    """
    now = time.time()
    task_id = uuid.uuid4().hex
    payload_hash = _hash(payload)
    with db.transaction() as conn:
        conn.execute(
            """
            INSERT INTO tasks (id, group_key, idempotency_key, payload, payload_hash,
                               max_attempts, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (idempotency_key) DO NOTHING
            """,
            (
                task_id,
                group_key,
                idempotency_key,
                _canonical(payload),
                payload_hash,
                max_attempts if max_attempts is not None else default_max_attempts,
                now,
                now,
            ),
        )
        row = conn.execute(
            "SELECT * FROM tasks WHERE idempotency_key = ?", (idempotency_key,)
        ).fetchone()
    if row["id"] == task_id:
        return _row_to_task(row), True
    if row["payload_hash"] != payload_hash:
        raise PayloadConflict(
            "idempotency_key already exists with a different payload"
        )
    return _row_to_task(row), False


def _claimable_head(conn, now: float, group_keys: Optional[Iterable[str]]):
    """Oldest unfinished task per group that is free to lease, or None.

    A task is the group head when no earlier-seq task in the same group is
    still unfinished (pending/leased); the head is claimable when it is
    pending or its lease has expired.
    """
    sql = """
        SELECT t.* FROM tasks t
        WHERE (t.status = 'pending' OR (t.status = 'leased' AND t.lease_expires_at <= ?))
          AND NOT EXISTS (
              SELECT 1 FROM tasks p
              WHERE p.group_key = t.group_key
                AND p.status IN ('pending', 'leased')
                AND p.seq < t.seq
          )
    """
    params: list = [now]
    if group_keys:
        placeholders = ",".join("?" for _ in group_keys)
        sql += f" AND t.group_key IN ({placeholders})"
        params.extend(group_keys)
    sql += " ORDER BY t.seq LIMIT 1"
    return conn.execute(sql, params).fetchone()


def claim_tasks(
    db: Database,
    limit: int,
    lease_ttl_seconds: float,
    group_keys: Optional[list[str]] = None,
) -> list[dict]:
    """Lease up to `limit` group-head tasks. At most one task per group_key.

    Runs in a single BEGIN IMMEDIATE transaction: concurrent claimers cannot
    receive the same task. Heads whose attempt budget is already exhausted
    (lease expired without ack) are moved to the dead letter instead.
    """
    now = time.time()
    leased: list[dict] = []
    with db.transaction() as conn:
        while len(leased) < limit:
            row = _claimable_head(conn, now, group_keys)
            if row is None:
                break
            if row["attempts"] >= row["max_attempts"]:
                conn.execute(
                    """
                    UPDATE tasks
                    SET status = 'dead', lease_token = NULL, lease_expires_at = NULL,
                        last_error = COALESCE(last_error, 'attempt limit reached'),
                        updated_at = ?
                    WHERE seq = ?
                    """,
                    (now, row["seq"]),
                )
                continue
            token = uuid.uuid4().hex
            expires = now + lease_ttl_seconds
            conn.execute(
                """
                UPDATE tasks
                SET status = 'leased', attempts = attempts + 1,
                    lease_token = ?, lease_expires_at = ?, updated_at = ?
                WHERE seq = ?
                """,
                (token, expires, now, row["seq"]),
            )
            task = _row_to_task(
                conn.execute("SELECT * FROM tasks WHERE seq = ?", (row["seq"],)).fetchone()
            )
            task["lease_token"] = token
            leased.append(task)
    return leased


def _load_leased(conn, task_id: str, lease_token: str):
    row = conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
    if row is None:
        raise TaskNotFound("task not found")
    if row["status"] != "leased" or row["lease_token"] != lease_token:
        raise LeaseConflict("lease token is not current for this task")
    if row["lease_expires_at"] is None or row["lease_expires_at"] <= time.time():
        raise LeaseConflict("lease has expired")
    return row


def ack_task(db: Database, task_id: str, lease_token: str) -> dict:
    now = time.time()
    with db.transaction() as conn:
        row = _load_leased(conn, task_id, lease_token)
        conn.execute(
            """
            UPDATE tasks
            SET status = 'succeeded', lease_token = NULL, lease_expires_at = NULL,
                updated_at = ?
            WHERE seq = ?
            """,
            (now, row["seq"]),
        )
        return _row_to_task(
            conn.execute("SELECT * FROM tasks WHERE seq = ?", (row["seq"],)).fetchone()
        )


def fail_task(db: Database, task_id: str, lease_token: str, error: Optional[str]) -> dict:
    """Mark the attempt failed. Retries remaining -> pending, else dead letter."""
    now = time.time()
    with db.transaction() as conn:
        row = _load_leased(conn, task_id, lease_token)
        exhausted = row["attempts"] >= row["max_attempts"]
        conn.execute(
            """
            UPDATE tasks
            SET status = ?, lease_token = NULL, lease_expires_at = NULL,
                last_error = ?, updated_at = ?
            WHERE seq = ?
            """,
            ("dead" if exhausted else "pending", error, now, row["seq"]),
        )
        return _row_to_task(
            conn.execute("SELECT * FROM tasks WHERE seq = ?", (row["seq"],)).fetchone()
        )


def get_task(db: Database, task_id: str) -> dict:
    with db.read() as conn:
        row = conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
    if row is None:
        raise TaskNotFound("task not found")
    return _row_to_task(row)


def list_tasks(
    db: Database,
    status: Optional[str] = None,
    group_key: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
) -> list[dict]:
    sql = "SELECT * FROM tasks WHERE 1=1"
    params: list = []
    if status:
        sql += " AND status = ?"
        params.append(status)
    if group_key:
        sql += " AND group_key = ?"
        params.append(group_key)
    sql += " ORDER BY seq LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    with db.read() as conn:
        rows = conn.execute(sql, params).fetchall()
    return [_row_to_task(r) for r in rows]


def list_dead_letters(db: Database, limit: int = 100, offset: int = 0) -> list[dict]:
    return list_tasks(db, status="dead", limit=limit, offset=offset)


def requeue_dead_letter(db: Database, task_id: str) -> dict:
    now = time.time()
    with db.transaction() as conn:
        row = conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
        if row is None:
            raise TaskNotFound("task not found")
        if row["status"] != "dead":
            raise InvalidState("only dead-letter tasks can be requeued")
        conn.execute(
            """
            UPDATE tasks
            SET status = 'pending', attempts = 0, lease_token = NULL,
                lease_expires_at = NULL, last_error = NULL, updated_at = ?
            WHERE seq = ?
            """,
            (now, row["seq"]),
        )
        return _row_to_task(
            conn.execute("SELECT * FROM tasks WHERE seq = ?", (row["seq"],)).fetchone()
        )
