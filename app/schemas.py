"""Pydantic request/response schemas."""

from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, Field


class TaskSubmit(BaseModel):
    group_key: str = Field(min_length=1, max_length=256)
    idempotency_key: str = Field(min_length=1, max_length=256)
    payload: Any
    max_attempts: Optional[int] = Field(default=None, ge=1, le=1000)


class ClaimRequest(BaseModel):
    limit: int = Field(default=1, ge=1)
    lease_ttl_seconds: Optional[float] = Field(default=None, gt=0, le=86400)
    group_keys: Optional[list[str]] = Field(default=None, max_length=100)


class AckRequest(BaseModel):
    lease_token: str = Field(min_length=1)


class FailRequest(BaseModel):
    lease_token: str = Field(min_length=1)
    error: Optional[str] = Field(default=None, max_length=4096)


class TaskOut(BaseModel):
    id: str
    group_key: str
    idempotency_key: str
    payload: Any
    status: str
    attempts: int
    max_attempts: int
    lease_expires_at: Optional[float]
    last_error: Optional[str]
    created_at: float
    updated_at: float


class LeasedTask(TaskOut):
    lease_token: str


class ClaimResponse(BaseModel):
    tasks: list[LeasedTask]


class TaskListResponse(BaseModel):
    tasks: list[TaskOut]
