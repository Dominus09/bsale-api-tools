"""FastAPI laboratorio 2A.5 — sync offline aislado + R2 mock + PG temporal.

Escucha 127.0.0.1 (nunca api.quillotana.cl). Auth lab: Bearer cualquiera → user 42.

Nota: no usar ``from __future__ import annotations`` — FastAPI necesita
resolver ``Request`` en tiempo de definición de rutas.
"""

import json
import os
import tempfile
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
SQL_DIR = ROOT / "backend" / "sql" / "distribuidora"
SQL_FILES = [
    SQL_DIR / "056_delivery_visits_evidence.sql",
    SQL_DIR / "057_delivery_visit_closure_guards.sql",
    SQL_DIR / "058_delivery_visit_admin_void.sql",
    SQL_DIR / "059_delivery_payment_declaration_AWAITING_APPROVAL.sql",
    SQL_DIR / "060_delivery_check_declared_AWAITING_APPROVAL.sql",
    SQL_DIR / "061_delivery_offline_ops_AWAITING_APPROVAL.sql",
]

CLIENTS = [
    {"key": "id:9201", "cid": 9201, "doc": 91055, "d1": 91001, "d2": 91002},
    {"key": "id:9202", "cid": 9202, "doc": 91056, "d1": 91003, "d2": 91004},
    {"key": "id:9203", "cid": 9203, "doc": 91057, "d1": 91005, "d2": 91006},
    {"key": "id:9204", "cid": 9204, "doc": 91058, "d1": 91007, "d2": 91008},
    {"key": "id:9205", "cid": 9205, "doc": 91059, "d1": 91009, "d2": 91010},
]

R2_STORE: dict[str, bytes] = {}
LAB_STATE: dict[str, Any] = {}


def _assert_isolated(uri: str) -> None:
    low = uri.lower()
    for b in ("analytics", "quillotana", "prod", "coolify", "neon.tech", "supabase"):
        if b in low:
            raise RuntimeError(f"DSN sospechoso: {b}")


def _strip_tx(sql: str) -> str:
    return "\n".join(
        ln
        for ln in sql.splitlines()
        if ln.strip().upper() not in ("BEGIN;", "COMMIT;", "BEGIN", "COMMIT")
    )


def _split_sql(raw: str) -> list[str]:
    parts: list[str] = []
    buf: list[str] = []
    for line in raw.splitlines():
        if line.strip() == "-- +go":
            chunk = "\n".join(buf).strip()
            if chunk:
                parts.append(chunk)
            buf = []
        else:
            buf.append(line)
    chunk = "\n".join(buf).strip()
    if chunk:
        parts.append(chunk)
    return parts


def bootstrap(dsn: str) -> None:
    import psycopg2
    from psycopg2.extras import RealDictCursor

    _assert_isolated(dsn)
    conn = psycopg2.connect(dsn, cursor_factory=RealDictCursor)
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS distribuidora")
    for t in (
        "delivery_offline_ops",
        "delivery_report_snapshots",
        "delivery_conformity",
        "delivery_evidence",
        "delivery_collections",
        "delivery_line_results",
        "delivery_document_results",
        "delivery_visit_events",
        "delivery_visits",
        "document_details",
        "dispatch_plan",
    ):
        cur.execute(f"DROP TABLE IF EXISTS distribuidora.{t} CASCADE")
    cur.execute(
        """
        CREATE TABLE distribuidora.dispatch_plan (
            id BIGSERIAL PRIMARY KEY,
            status TEXT NOT NULL DEFAULT 'confirmed',
            planning_code TEXT,
            driver_user_id INTEGER
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE distribuidora.document_details (
            detail_id BIGINT PRIMARY KEY,
            document_id BIGINT NOT NULL,
            line_number INT,
            variant_id BIGINT,
            variant_description TEXT,
            variant_code TEXT,
            quantity NUMERIC(18, 4)
        )
        """
    )
    conn.commit()
    for path in SQL_FILES:
        raw = _strip_tx(path.read_text(encoding="utf-8"))
        for stmt in _split_sql(raw):
            if stmt.strip():
                cur.execute(stmt)
    conn.commit()
    cur.execute(
        """
        INSERT INTO distribuidora.dispatch_plan (id, planning_code, driver_user_id)
        VALUES (920, 'PLAN-OFFLINE-2A5-LAB', 42)
        """
    )
    detail_rows = []
    for i, c in enumerate(CLIENTS):
        detail_rows.append((c["d1"], c["doc"], 1, 1, f"Prod {i}A", 4))
        detail_rows.append((c["d2"], c["doc"], 2, 1, f"Prod {i}B", 2))
    cur.executemany(
        """
        INSERT INTO distribuidora.document_details
            (detail_id, document_id, line_number, variant_id, variant_description, quantity)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        detail_rows,
    )
    conn.commit()
    cur.close()
    conn.close()
    LAB_STATE["dsn"] = dsn
    R2_STORE.clear()


def connect():
    import psycopg2
    from psycopg2.extras import RealDictCursor

    dsn = LAB_STATE["dsn"]
    _assert_isolated(dsn)
    return psycopg2.connect(dsn, cursor_factory=RealDictCursor)


def r2_put(key: str, data: bytes, content_type: str) -> str:
    if LAB_STATE.get("r2_fail_once"):
        LAB_STATE["r2_fail_once"] = False
        raise RuntimeError("R2 lab timeout simulated")
    R2_STORE[key] = data
    return key


def orphan_candidates() -> list[dict[str, Any]]:
    """Dry-run: ops con clave R2 sin confirmación / sin visita."""
    conn = connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, evidence_object_key, evidence_upload_confirmed, status, visit_id
        FROM distribuidora.delivery_offline_ops
        WHERE evidence_object_key IS NOT NULL
          AND (
            evidence_upload_confirmed = FALSE
            OR status IN ('failed', 'awaiting_evidence')
          )
        ORDER BY id
        """
    )
    rows = [dict(r) for r in cur.fetchall()]
    cur.close()
    conn.close()
    return rows


def build_app():
    from typing import Optional

    from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
    from fastapi.middleware.cors import CORSMiddleware

    from backend.services.distribuidora import delivery_offline_sync_service as offline_svc
    from backend.services.distribuidora import delivery_visit_service as visit_svc
    from backend.services.distribuidora import delivery_offline_validate as ov

    # Patches de laboratorio
    offline_svc.get_connection = connect  # type: ignore
    visit_svc.get_connection = connect  # type: ignore
    ov.get_connection = connect  # type: ignore

    by_key = {c["key"]: c for c in CLIENTS}

    def resolve_client(_p, key):
        c = by_key.get(key) or CLIENTS[0]
        return {
            "client_id": c["cid"],
            "customer_key": c["key"],
            "client_name": f"Cliente {c['cid']}",
            "latitude": -33.45,
            "longitude": -70.66,
        }

    visit_svc.read_svc.enforce_driver_plan_read_access = lambda _u, _p: 42  # type: ignore
    offline_svc.read_svc.enforce_driver_plan_read_access = lambda _u, _p: 42  # type: ignore
    ov.read_svc.enforce_driver_plan_read_access = lambda _u, _p: 42  # type: ignore
    for svc in (visit_svc, offline_svc):
        svc.loc_svc._resolve_client = resolve_client  # type: ignore
        svc.loc_svc._parse_client_id = lambda k, c: int(c["client_id"])  # type: ignore
        svc.loc_svc._authorized_company_id_for_plan = lambda _u, _p: 3  # type: ignore
    visit_svc._client_ref_coords = lambda _p, _k: (-33.45, -70.66)  # type: ignore
    visit_svc._expected_related_document_ids = (  # type: ignore
        lambda _p, key: [by_key[key]["doc"]] if key in by_key else []
    )

    def lab_context(_plan_id: int, customer_key: str) -> dict:
        c = by_key[customer_key]
        conn = connect()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT detail_id, quantity
            FROM distribuidora.document_details
            WHERE document_id = %s
            ORDER BY detail_id
            """,
            (c["doc"],),
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return {
            "picking_ready": True,
            "picking_reason": None,
            "picking_id": 501,
            "picking_version": "3",
            "is_current": True,
            "plan_status": "confirmed",
            "customer_key": customer_key,
            "ficha": {
                "client": {
                    "client_id": c["cid"],
                    "customer_key": customer_key,
                    "client_name": f"Cliente {c['cid']}",
                },
                "documents": [
                    {
                        "related_document_id": c["doc"],
                        "products": [
                            {
                                "source_detail_id": int(r["detail_id"]),
                                "unidades": float(r["quantity"]),
                            }
                            for r in rows
                        ],
                    }
                ],
            },
        }

    real_validate = ov.validate_offline_operation

    def validate_lab(*, user, plan_id, body, context_fetcher=None, allow_closed_visit_id=None):
        return real_validate(
            user=user,
            plan_id=plan_id,
            body=body,
            context_fetcher=context_fetcher or lab_context,
            allow_closed_visit_id=allow_closed_visit_id,
        )

    ov.validate_offline_operation = validate_lab  # type: ignore

    app = FastAPI(title="Offline V2 Lab 2A.5")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.get("/health")
    def health():
        return {"ok": True, "lab": "2a5", "r2_objects": len(R2_STORE)}

    @app.get("/auth/me")
    def auth_me():
        """Sesión lab para AuthGate en flujo real (nunca prod)."""
        return {
            "id": 42,
            "email": "chofer@lab.local",
            "role": "driver",
            "apps": ["pwa"],
            "profiles": ["chofer"],
            "has_erp_access": False,
            "has_pwa_access": True,
        }

    @app.get("/lab/pg-stats")
    def pg_stats():
        conn = connect()
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) AS n FROM distribuidora.delivery_visits WHERE dispatch_plan_id=920"
        )
        visits = int(cur.fetchone()["n"])
        cur.execute(
            """
            SELECT COUNT(*) AS n FROM distribuidora.delivery_offline_ops
            WHERE dispatch_plan_id=920 AND status='completed'
            """
        )
        ops = int(cur.fetchone()["n"])
        cur.execute(
            """
            SELECT COUNT(*) AS n FROM distribuidora.delivery_evidence e
            JOIN distribuidora.delivery_visits v ON v.id = e.visit_id
            WHERE v.dispatch_plan_id=920 AND e.upload_confirmed
            """
        )
        evidence = int(cur.fetchone()["n"])
        cur.close()
        conn.close()
        return {
            "visits": visits,
            "ops_completed": ops,
            "evidence_confirmed": evidence,
            "r2_keys": list(R2_STORE.keys()),
            "orphans_dry_run": orphan_candidates(),
        }

    @app.post("/lab/r2-fail-once")
    def r2_fail_once():
        LAB_STATE["r2_fail_once"] = True
        return {"ok": True}

    @app.post("/lab/reset-plan/{plan_id}")
    def reset_plan(plan_id: int):
        """Limpia visitas/ops del plan lab (entre E2E). No toca prod."""
        conn = connect()
        cur = conn.cursor()
        cur.execute(
            """
            DELETE FROM distribuidora.delivery_offline_ops
            WHERE dispatch_plan_id = %s
            """,
            (int(plan_id),),
        )
        cur.execute(
            """
            DELETE FROM distribuidora.delivery_visit_events e
            USING distribuidora.delivery_visits v
            WHERE e.visit_id = v.id AND v.dispatch_plan_id = %s
            """,
            (int(plan_id),),
        )
        cur.execute(
            """
            DELETE FROM distribuidora.delivery_evidence e
            USING distribuidora.delivery_visits v
            WHERE e.visit_id = v.id AND v.dispatch_plan_id = %s
            """,
            (int(plan_id),),
        )
        cur.execute(
            """
            DELETE FROM distribuidora.delivery_collections c
            USING distribuidora.delivery_visits v
            WHERE c.visit_id = v.id AND v.dispatch_plan_id = %s
            """,
            (int(plan_id),),
        )
        cur.execute(
            """
            DELETE FROM distribuidora.delivery_line_results lr
            USING distribuidora.delivery_visits v
            WHERE lr.visit_id = v.id AND v.dispatch_plan_id = %s
            """,
            (int(plan_id),),
        )
        cur.execute(
            """
            DELETE FROM distribuidora.delivery_document_results dr
            USING distribuidora.delivery_visits v
            WHERE dr.visit_id = v.id AND v.dispatch_plan_id = %s
            """,
            (int(plan_id),),
        )
        cur.execute(
            "DELETE FROM distribuidora.delivery_visits WHERE dispatch_plan_id = %s",
            (int(plan_id),),
        )
        conn.commit()
        cur.close()
        conn.close()
        R2_STORE.clear()
        return {"ok": True, "plan_id": int(plan_id)}

    @app.post("/distribuidora/pwa/driver/despachos/{plan_id}/offline-ops/sync")
    async def sync_op(plan_id: int, request: Request):
        form = await request.form()
        payload = form.get("payload")
        if payload is None:
            raise HTTPException(status_code=400, detail="payload requerido")
        if hasattr(payload, "read"):
            payload = (await payload.read()).decode("utf-8")  # type: ignore[union-attr]
        payload = str(payload)
        try:
            body = json.loads(payload)
        except Exception as exc:
            raise HTTPException(status_code=400, detail="payload JSON inválido") from exc
        evidence_bytes = None
        ctype = "image/jpeg"
        evidence = form.get("evidence")
        if evidence is not None and hasattr(evidence, "read"):
            evidence_bytes = await evidence.read()  # type: ignore[misc]
            ctype = (getattr(evidence, "content_type", None) or "").strip() or "image/jpeg"
        user = {"id": 42, "email": "chofer@lab.local"}
        if not offline_svc.offline_pilot_user_allowed(42):
            raise HTTPException(status_code=403, detail="PILOT_USER_NOT_ALLOWED")
        return offline_svc.sync_offline_operation(
            user=user,
            plan_id=int(plan_id),
            body=body,
            evidence_bytes=evidence_bytes,
            evidence_content_type=ctype,
            r2_put=r2_put,
            require_writes_flag=True,
            context_fetcher=lab_context,
        )

    return app


def main() -> None:
    import pgserver
    import uvicorn

    os.environ["OFFLINE_SYNC_WRITES_ENABLED"] = "1"
    base = Path(tempfile.mkdtemp(prefix="p4_2a5_lab_"))
    srv = pgserver.get_server(base / "pgdata", cleanup_mode="stop")
    uri = srv.get_uri() if callable(getattr(srv, "get_uri", None)) else str(srv)
    if callable(getattr(srv, "get_uri", None)):
        uri = srv.get_uri()
    print(f"[2a5-api] dsn={uri}", flush=True)
    bootstrap(uri)
    app = build_app()
    port = int(os.getenv("LAB_2A5_PORT", "18765"))
    print(f"[2a5-api] http://127.0.0.1:{port}", flush=True)
    uvicorn.run(app, host="127.0.0.1", port=port, log_level="warning")
    _ = srv


if __name__ == "__main__":
    main()
