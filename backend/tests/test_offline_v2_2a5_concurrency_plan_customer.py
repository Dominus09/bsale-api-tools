"""2A.5+ — locks de negocio plan/cliente + concurrencia (suite PG en runner)."""

from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi import HTTPException

from backend.services.distribuidora import delivery_offline_sync_service as offline_svc
from backend.tests.test_offline_v2_2a4_lab_e2e import (
    CLIENTS,
    _complete_body,
    _connect,
    _user,
)

# Expone fixture lab_db del módulo 2A.4 a esta suite.
pytest_plugins = ["backend.tests.test_offline_v2_2a4_lab_e2e"]

DSN = (os.getenv("SIMPLE_PLANNING_TEST_DSN") or "").strip()

pytestmark = pytest.mark.skipif(
    not DSN,
    reason="Define SIMPLE_PLANNING_TEST_DSN (runner pgserver aislado)",
)


def test_two_keys_same_client_concurrent_one_visit(lab_db):
    """Dos claves distintas, mismo plan/cliente, en paralelo → 1 finalización."""
    client = CLIENTS[0]
    body_a = _complete_body("conc-key-a", client)
    body_b = _complete_body("conc-key-b", client)

    results: list[dict] = []
    errors: list[BaseException] = []

    def run(body):
        try:
            return offline_svc.sync_offline_operation(
                user=_user(),
                plan_id=920,
                body=body,
                r2_put=lambda *a, **k: "",
            )
        except BaseException as exc:  # noqa: BLE001
            return exc

    with ThreadPoolExecutor(max_workers=2) as pool:
        futs = [pool.submit(run, body_a), pool.submit(run, body_b)]
        for f in futs:
            out = f.result()
            if isinstance(out, BaseException):
                errors.append(out)
            else:
                results.append(out)

    ok = [r for r in results if isinstance(r, dict) and r.get("visit")]
    conflicts = [
        e
        for e in errors
        if isinstance(e, HTTPException)
        and e.status_code == 409
        and isinstance(e.detail, dict)
        and e.detail.get("code") == offline_svc.ERR_DUPLICATE_VISIT_OP
    ]
    assert len(ok) == 1, f"esperaba 1 OK, got ok={len(ok)} errors={errors!r}"
    assert len(conflicts) >= 1, f"esperaba ≥1 conflicto 409, errors={errors!r}"

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) AS n FROM distribuidora.delivery_visits WHERE dispatch_plan_id=920"
    )
    assert int(cur.fetchone()["n"]) == 1
    cur.execute(
        """
        SELECT COUNT(*) AS n FROM distribuidora.delivery_offline_ops
        WHERE dispatch_plan_id=920 AND status='completed'
        """
    )
    assert int(cur.fetchone()["n"]) == 1
    cur.execute(
        """
        SELECT COUNT(*) AS n FROM distribuidora.delivery_visit_events e
        JOIN distribuidora.delivery_visits v ON v.id = e.visit_id
        WHERE v.dispatch_plan_id=920 AND e.action='visit_finalized'
        """
    )
    assert int(cur.fetchone()["n"]) == 1
    cur.close()
    conn.close()


def test_advisory_lock_keys_differ_for_business_vs_idem():
    a = offline_svc._advisory_lock_key(42, "k1")
    b = offline_svc._advisory_lock_key(42, "k2")
    pc = offline_svc._advisory_lock_key_plan_customer(42, 920, "id:9201")
    pc2 = offline_svc._advisory_lock_key_plan_customer(42, 920, "id:9202")
    assert a != b
    assert pc != a
    assert pc != pc2


def test_different_customers_not_serialized_by_same_pc_key():
    k1 = offline_svc._advisory_lock_key_plan_customer(42, 920, "id:9201")
    k2 = offline_svc._advisory_lock_key_plan_customer(42, 920, "id:9202")
    assert k1 != k2
