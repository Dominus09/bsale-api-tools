"""E2E warehouse contra backend/main.py + JWT real + PG aislado.

No stub de require_management_access: usa create_access_token + role gerencia.
Apunta PG_* al pgserver temporal (nunca analytics/prod).
Sin sync Bsale / cron.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[2]
SQL_DIR = ROOT / "backend" / "sql" / "warehouse"


def _assert_isolated(uri: str) -> None:
    low = uri.lower()
    for b in ("analytics", "quillotana", "prod", "production", "coolify", "neon.tech"):
        if b in low:
            raise SystemExit(f"DSN sospechoso: {b}")


def main() -> int:
    import pgserver
    import psycopg2
    from fastapi.testclient import TestClient

    base = Path(tempfile.mkdtemp(prefix="warehouse_1_4_main_"))
    srv = pgserver.get_server(base / "pgdata", cleanup_mode="stop")
    uri = srv.get_uri() if callable(getattr(srv, "get_uri", None)) else str(srv)
    if callable(getattr(srv, "get_uri", None)):
        uri = srv.get_uri()
    _assert_isolated(uri)
    parsed = urlparse(uri)

    os.environ["PG_HOST"] = parsed.hostname or "127.0.0.1"
    os.environ["PG_PORT"] = str(parsed.port or 5432)
    os.environ["PG_DB"] = (parsed.path or "/postgres").lstrip("/") or "postgres"
    os.environ["PG_USER"] = parsed.username or "postgres"
    os.environ["PG_PASSWORD"] = parsed.password or ""
    os.environ["ENVIRONMENT"] = "development"
    os.environ["JWT_SECRET_KEY"] = "warehouse-slice-1-4-e2e-main-auth-secret-key!!"
    # Arranque main.py exige schema distribuidora 048; en lab aislado solo warehouse.
    os.environ["DISTRIBUIDORA_SCHEMA_CHECK"] = "warn"
    # Evitar cutover multiapp que exigiría filas user_apps
    os.environ.pop("AUTH_REQUIRE_USER_APPS", None)

    conn = psycopg2.connect(uri)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("SHOW data_directory")
    datadir = str(cur.fetchone()[0]).lower()
    if "analytics" in datadir or "quillotana" in datadir:
        raise SystemExit("data_directory productivo")
    cur.execute("DROP SCHEMA IF EXISTS warehouse CASCADE")
    for name in (
        "001_schema_layout_AWAITING_APPROVAL.sql",
        "002_seed_bodega_central_draft_AWAITING_APPROVAL.sql",
        "003_seed_bc_special_zones_AWAITING_APPROVAL.sql",
        "004_publish_layout_AWAITING_APPROVAL.sql",
    ):
        cur.execute((SQL_DIR / name).read_text(encoding="utf-8"))
    cur.close()
    conn.close()

    sys.path.insert(0, str(ROOT))
    from backend.auth.jwt import create_staff_token
    from backend.main import app

    mgmt = create_staff_token(
        email="warehouse-main-e2e@test.local",
        role="gerencia",
        user_id=91001,
    )
    denied = create_staff_token(
        email="warehouse-ops@test.local",
        role="operaciones",
        user_id=91002,
    )

    steps: list[dict] = []
    ok = True
    with TestClient(app) as client:
        # 403 sin management
        r = client.get(
            "/warehouse/facilities",
            headers={"Authorization": f"Bearer {denied}"},
        )
        steps.append({"deny_non_mgmt": r.status_code})
        if r.status_code != 403:
            ok = False

        # 401 sin token
        r0 = client.get("/warehouse/facilities")
        steps.append({"deny_anon": r0.status_code})
        if r0.status_code not in (401, 403):
            ok = False

        hdr = {"Authorization": f"Bearer {mgmt}"}
        r1 = client.get("/warehouse/facilities", headers=hdr)
        steps.append({"facilities": r1.status_code})
        if r1.status_code != 200:
            ok = False
            print(json.dumps({"ok": False, "steps": steps, "body": r1.text[:500]}, indent=2))
            return 1

        fac = next(f for f in r1.json()["items"] if f["code"] == "BC")
        r2 = client.get(f"/warehouse/facilities/{fac['id']}/floors", headers=hdr)
        floor = r2.json()["items"][0]
        r3 = client.post(f"/warehouse/floors/{floor['id']}/draft", headers=hdr, json={})
        draft = r3.json()["revision"]
        steps.append({"draft": draft["id"], "status": draft["status"]})

        # Seed no publicable
        rev_id = int(draft["id"])
        r4 = client.post(f"/warehouse/revisions/{rev_id}/evaluate-publish", headers=hdr, json={})
        ev = r4.json()
        steps.append({"seed_can_publish": ev.get("can_publish"), "blockers": len(ev.get("blockers") or [])})
        if ev.get("can_publish") is not False:
            ok = False

        r5 = client.post(
            f"/warehouse/revisions/{rev_id}/publish",
            headers=hdr,
            json={"expected_row_version": draft["row_version"]},
        )
        steps.append({"seed_publish_rejected": r5.status_code})
        if r5.status_code != 400:
            ok = False

        # Layout publicable mínimo en otro piso vía SQL+service sería más largo;
        # aquí validamos auth real + evaluate/publish gate sobre BC seed.
        r6 = client.get(f"/warehouse/revisions/{rev_id}", headers=hdr)
        steps.append({"get_revision": r6.status_code, "elements": r6.json().get("element_count")})
        if r6.status_code != 200:
            ok = False

        r7 = client.get(f"/warehouse/revisions/{rev_id}/audit?limit=5", headers=hdr)
        steps.append({"audit": r7.status_code, "items": len(r7.json().get("items") or [])})
        if r7.status_code != 200:
            ok = False

        # Concurrency 409 on batch with wrong version
        r8 = client.post(
            f"/warehouse/revisions/{rev_id}/elements/batch",
            headers=hdr,
            json={"expected_row_version": 999999, "upserts": []},
        )
        steps.append({"batch_409": r8.status_code})
        if r8.status_code != 409:
            ok = False

    report = {
        "ok": ok,
        "auth": "jwt_create_staff_token+require_management_access+require_erp_app",
        "steps": steps,
    }
    out = ROOT / "_audit_artifacts" / "inteligencia_bodega_1_4_e2e_main_auth.json"
    out.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
