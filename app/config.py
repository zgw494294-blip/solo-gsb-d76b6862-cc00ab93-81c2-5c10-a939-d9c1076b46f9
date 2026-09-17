"""Runtime configuration, sourced from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    db_path: str
    lease_ttl_seconds: float
    max_attempts: int
    max_batch_size: int


def load_settings() -> Settings:
    return Settings(
        db_path=os.environ.get("TQ_DB_PATH", "./queue.db"),
        lease_ttl_seconds=float(os.environ.get("TQ_LEASE_TTL_SECONDS", "30")),
        max_attempts=int(os.environ.get("TQ_MAX_ATTEMPTS", "3")),
        max_batch_size=int(os.environ.get("TQ_MAX_BATCH_SIZE", "100")),
    )


settings = load_settings()
