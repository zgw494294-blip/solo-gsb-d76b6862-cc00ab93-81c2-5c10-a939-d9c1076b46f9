"""FastAPI application exposing the task queue HTTP API."""

from __future__ import annotations

from typing import Optional

from fastapi import Depends, FastAPI, Query, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from .config import settings
from .db import Database
from .schemas import (
    AckRequest,
    ClaimRequest,
    ClaimResponse,
    FailRequest,
    TaskListResponse,
    TaskOut,
    TaskSubmit,
)
from . import service

app = FastAPI(title="Task Queue API", version="1.0.0")

_db: Optional[Database] = None


def get_db() -> Database:
    global _db
    if _db is None:
        _db = Database(settings.db_path)
    return _db


@app.exception_handler(service.QueueError)
async def queue_error_handler(_: Request, exc: service.QueueError):
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})


@app.exception_handler(RequestValidationError)
async def validation_error_handler(_: Request, exc: RequestValidationError):
    return JSONResponse(status_code=422, content={"detail": exc.errors()})


@app.get("/healthz")
def healthz():
    return {"status": "ok"}


@app.post("/tasks", response_model=TaskOut, status_code=201)
def submit_task(body: TaskSubmit, response: Response, db: Database = Depends(get_db)):
    task, created = service.submit_task(
        db,
        group_key=body.group_key,
        idempotency_key=body.idempotency_key,
        payload=body.payload,
        max_attempts=body.max_attempts,
        default_max_attempts=settings.max_attempts,
    )
    if not created:
        response.status_code = 200
    return task


@app.post("/claims", response_model=ClaimResponse)
def claim_tasks(body: ClaimRequest, db: Database = Depends(get_db)):
    limit = min(body.limit, settings.max_batch_size)
    ttl = body.lease_ttl_seconds or settings.lease_ttl_seconds
    tasks = service.claim_tasks(
        db, limit=limit, lease_ttl_seconds=ttl, group_keys=body.group_keys
    )
    return {"tasks": tasks}


@app.post("/tasks/{task_id}/ack", response_model=TaskOut)
def ack_task(task_id: str, body: AckRequest, db: Database = Depends(get_db)):
    return service.ack_task(db, task_id, body.lease_token)


@app.post("/tasks/{task_id}/fail", response_model=TaskOut)
def fail_task(task_id: str, body: FailRequest, db: Database = Depends(get_db)):
    return service.fail_task(db, task_id, body.lease_token, body.error)


@app.get("/tasks/{task_id}", response_model=TaskOut)
def get_task(task_id: str, db: Database = Depends(get_db)):
    return service.get_task(db, task_id)


@app.get("/tasks", response_model=TaskListResponse)
def list_tasks(
    status: Optional[str] = Query(default=None, pattern="^(pending|leased|succeeded|dead)$"),
    group_key: Optional[str] = None,
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    db: Database = Depends(get_db),
):
    return {"tasks": service.list_tasks(db, status=status, group_key=group_key, limit=limit, offset=offset)}


@app.get("/dead-letter", response_model=TaskListResponse)
def list_dead_letter(
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    db: Database = Depends(get_db),
):
    return {"tasks": service.list_dead_letters(db, limit=limit, offset=offset)}


@app.post("/dead-letter/{task_id}/requeue", response_model=TaskOut)
def requeue_dead_letter(task_id: str, db: Database = Depends(get_db)):
    return service.requeue_dead_letter(db, task_id)
