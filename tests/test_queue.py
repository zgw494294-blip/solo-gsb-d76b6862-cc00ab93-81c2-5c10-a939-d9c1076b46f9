import threading
import time

import pytest
from fastapi.testclient import TestClient

from app.db import Database
from app.main import app, get_db
from app import service


@pytest.fixture()
def db(tmp_path):
    database = Database(str(tmp_path / "test.db"))
    yield database
    database.close()


@pytest.fixture()
def client(db):
    app.dependency_overrides[get_db] = lambda: db
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


def submit(client, group="g1", key="k1", payload=None, max_attempts=None):
    body = {"group_key": group, "idempotency_key": key, "payload": payload or {"n": 1}}
    if max_attempts is not None:
        body["max_attempts"] = max_attempts
    return client.post("/tasks", json=body)


def test_submit_and_idempotent_replay(client):
    r1 = submit(client, key="k1", payload={"a": 1})
    assert r1.status_code == 201
    task = r1.json()
    assert task["status"] == "pending"
    assert task["attempts"] == 0

    # Same key + same payload -> original task, HTTP 200
    r2 = submit(client, key="k1", payload={"a": 1})
    assert r2.status_code == 200
    assert r2.json()["id"] == task["id"]

    # Key order inside payload does not matter
    r3 = client.post(
        "/tasks",
        content=b'{"group_key":"g1","idempotency_key":"k1","payload":{"a":1}}',
        headers={"content-type": "application/json"},
    )
    assert r3.status_code == 200

    # Same key + different payload -> 409
    r4 = submit(client, key="k1", payload={"a": 2})
    assert r4.status_code == 409


def test_claim_ack_happy_path(client):
    submit(client, key="k1")
    r = client.post("/claims", json={"limit": 5})
    assert r.status_code == 200
    tasks = r.json()["tasks"]
    assert len(tasks) == 1
    leased = tasks[0]
    assert leased["status"] == "leased"
    assert leased["attempts"] == 1
    assert leased["lease_token"]

    r = client.post(f"/tasks/{leased['id']}/ack", json={"lease_token": leased["lease_token"]})
    assert r.status_code == 200
    assert r.json()["status"] == "succeeded"

    # Nothing left to claim
    assert client.post("/claims", json={"limit": 5}).json()["tasks"] == []


def test_ack_with_wrong_or_replayed_token(client):
    submit(client, key="k1")
    leased = client.post("/claims", json={}).json()["tasks"][0]

    assert client.post(
        f"/tasks/{leased['id']}/ack", json={"lease_token": "bogus"}
    ).status_code == 409

    assert client.post(
        f"/tasks/{leased['id']}/ack", json={"lease_token": leased["lease_token"]}
    ).status_code == 200

    # Token is consumed: replaying it must fail
    assert client.post(
        f"/tasks/{leased['id']}/ack", json={"lease_token": leased["lease_token"]}
    ).status_code == 409


def test_fifo_per_group(client):
    submit(client, group="g", key="k1")
    submit(client, group="g", key="k2")
    submit(client, group="other", key="k3")

    tasks = client.post("/claims", json={"limit": 10}).json()["tasks"]
    assert len(tasks) == 2  # one head per group
    by_group = {t["group_key"]: t for t in tasks}
    assert by_group["g"]["idempotency_key"] == "k1"

    # Second task in group g is blocked while k1 is leased
    tasks = client.post("/claims", json={"limit": 10}).json()["tasks"]
    assert tasks == []

    # Ack k1 -> k2 becomes claimable
    client.post(f"/tasks/{by_group['g']['id']}/ack", json={"lease_token": by_group["g"]["lease_token"]})
    tasks = client.post("/claims", json={"limit": 10}).json()["tasks"]
    assert [t["idempotency_key"] for t in tasks] == ["k2"]


def test_fail_retries_then_dead_letter(client):
    submit(client, key="k1", max_attempts=2)

    for attempt in (1, 2):
        leased = client.post("/claims", json={}).json()["tasks"][0]
        assert leased["attempts"] == attempt
        r = client.post(
            f"/tasks/{leased['id']}/fail",
            json={"lease_token": leased["lease_token"], "error": f"boom {attempt}"},
        )
        assert r.status_code == 200

    assert r.json()["status"] == "dead"
    assert r.json()["last_error"] == "boom 2"

    # Dead tasks are never delivered again
    assert client.post("/claims", json={}).json()["tasks"] == []

    dead = client.get("/dead-letter").json()["tasks"]
    assert [t["idempotency_key"] for t in dead] == ["k1"]


def test_lease_expiry_reclaim_and_old_token_invalid(client):
    submit(client, key="k1")
    first = client.post("/claims", json={"lease_ttl_seconds": 0.05}).json()["tasks"][0]
    time.sleep(0.1)

    second = client.post("/claims", json={}).json()["tasks"][0]
    assert second["id"] == first["id"]
    assert second["lease_token"] != first["lease_token"]

    # Old token must be rejected
    assert client.post(
        f"/tasks/{first['id']}/ack", json={"lease_token": first["lease_token"]}
    ).status_code == 409
    # New token works
    assert client.post(
        f"/tasks/{second['id']}/ack", json={"lease_token": second["lease_token"]}
    ).status_code == 200


def test_ack_after_lease_expiry_rejected(client):
    submit(client, key="k1")
    leased = client.post("/claims", json={"lease_ttl_seconds": 0.05}).json()["tasks"][0]
    time.sleep(0.1)
    # Even without a reclaim, an expired lease cannot ack
    assert client.post(
        f"/tasks/{leased['id']}/ack", json={"lease_token": leased["lease_token"]}
    ).status_code == 409


def test_expired_lease_exhaustion_goes_dead(client):
    submit(client, key="k1", max_attempts=1)
    client.post("/claims", json={"lease_ttl_seconds": 0.05})
    time.sleep(0.1)
    # Attempt budget spent and lease expired -> dead letter, not re-leased
    assert client.post("/claims", json={}).json()["tasks"] == []
    dead = client.get("/dead-letter").json()["tasks"]
    assert len(dead) == 1 and dead[0]["idempotency_key"] == "k1"


def test_dead_letter_requeue(client):
    submit(client, key="k1", max_attempts=1)
    leased = client.post("/claims", json={}).json()["tasks"][0]
    client.post(f"/tasks/{leased['id']}/fail", json={"lease_token": leased["lease_token"]})

    r = client.post(f"/dead-letter/{leased['id']}/requeue")
    assert r.status_code == 200
    assert r.json()["status"] == "pending"
    assert r.json()["attempts"] == 0

    leased2 = client.post("/claims", json={}).json()["tasks"][0]
    assert leased2["id"] == leased["id"]

    # Requeue of a non-dead task -> 409
    assert client.post(f"/dead-letter/{leased['id']}/requeue").status_code == 409


def test_dead_letter_unblocks_group_fifo(client):
    submit(client, group="g", key="k1", max_attempts=1)
    submit(client, group="g", key="k2")
    leased = client.post("/claims", json={}).json()["tasks"][0]
    client.post(f"/tasks/{leased['id']}/fail", json={"lease_token": leased["lease_token"]})

    # k1 is dead -> k2 becomes the group head and is claimable
    tasks = client.post("/claims", json={}).json()["tasks"]
    assert [t["idempotency_key"] for t in tasks] == ["k2"]


def test_requeued_task_takes_head_position(client):
    submit(client, group="g", key="k1", max_attempts=1)
    submit(client, group="g", key="k2")
    leased = client.post("/claims", json={}).json()["tasks"][0]
    client.post(f"/tasks/{leased['id']}/fail", json={"lease_token": leased["lease_token"]})
    client.post(f"/dead-letter/{leased['id']}/requeue")

    # Requeued k1 keeps its original sequence, so it is served before k2 again
    tasks = client.post("/claims", json={}).json()["tasks"]
    assert [t["idempotency_key"] for t in tasks] == ["k1"]


def test_status_and_listing(client):
    submit(client, group="g1", key="k1")
    r = submit(client, group="g2", key="k2")
    task_id = r.json()["id"]

    got = client.get(f"/tasks/{task_id}")
    assert got.status_code == 200
    assert got.json()["group_key"] == "g2"

    assert client.get("/tasks/unknown-id").status_code == 404
    assert len(client.get("/tasks", params={"group_key": "g1"}).json()["tasks"]) == 1
    assert len(client.get("/tasks", params={"status": "pending"}).json()["tasks"]) == 2


def test_group_key_filter_on_claim(client):
    submit(client, group="a", key="k1")
    submit(client, group="b", key="k2")
    tasks = client.post("/claims", json={"limit": 10, "group_keys": ["b"]}).json()["tasks"]
    assert [t["group_key"] for t in tasks] == ["b"]


def test_concurrent_claims_no_duplicate_delivery(db):
    for i in range(20):
        service.submit_task(db, f"g{i}", f"k{i}", {"i": i}, None, 3)

    claimed = []
    barrier = threading.Barrier(8)

    def worker():
        barrier.wait()
        claimed.extend(service.claim_tasks(db, limit=1, lease_ttl_seconds=30))

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    ids = [t["id"] for t in claimed]
    assert len(ids) == len(set(ids)) == 8
