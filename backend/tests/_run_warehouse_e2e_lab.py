"""Lab E2E Inteligencia de Bodega (slice 1.3).

- Postgres temporal (pgserver)
- Schema warehouse + seeds
- FastAPI lab (solo warehouse + /auth/me stub)
- Flujo API integral (generar, guardar, recargar, auditoría, 409, 403)
- Escribe DSN/puerto/token para el frontend Next real

Nunca analytics/prod.
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SQL_DIR = ROOT / "backend" / "sql" / "warehouse"
ART = ROOT / "_audit_artifacts"


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _assert_isolated(uri: str) -> None:
    low = uri.lower()
    for b in ("analytics", "quillotana", "prod", "production", "coolify", "neon.tech", "supabase", "amazonaws.com"):
        if b in low:
            raise SystemExit(f"DSN sospechoso ({b!r})")


def main() -> int:
    import pgserver
    import psycopg2
    import uvicorn
    from fastapi.testclient import TestClient

    from backend.auth.jwt import create_staff_token
    from backend.tests._warehouse_e2e_lab_app import build_lab_app

    base = Path(tempfile.mkdtemp(prefix="warehouse_e2e_"))
    pgdata = base / "pgdata"
    print(f"[e2e-lab] pgdata={pgdata}", flush=True)
    srv = pgserver.get_server(pgdata, cleanup_mode="stop")
    uri = srv.get_uri() if callable(getattr(srv, "get_uri", None)) else str(srv)
    if callable(getattr(srv, "get_uri", None)):
        uri = srv.get_uri()
    print(f"[e2e-lab] uri={uri}", flush=True)
    _assert_isolated(uri)

    conn = psycopg2.connect(uri)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("SHOW data_directory")
    datadir = str(cur.fetchone()[0])
    if "analytics" in datadir.lower() or "quillotana" in datadir.lower():
        raise SystemExit("data_directory productivo — abort")
    for name in (
        "001_schema_layout_AWAITING_APPROVAL.sql",
        "002_seed_bodega_central_draft_AWAITING_APPROVAL.sql",
        "003_seed_bc_special_zones_AWAITING_APPROVAL.sql",
        "004_publish_layout_AWAITING_APPROVAL.sql",

    ):
        cur.execute((SQL_DIR / name).read_text(encoding="utf-8"))
    cur.close()
    conn.close()

    app = build_lab_app(uri)
    port = _free_port()
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    base_url = f"http://127.0.0.1:{port}"
    # Esperar readiness
    import httpx as _hx

    for _ in range(80):
        try:
            if _hx.get(f"{base_url}/health", timeout=0.5).status_code == 200:
                break
        except Exception:
            time.sleep(0.1)
    else:
        raise SystemExit("API lab no arrancó a tiempo")
    print(f"[e2e-lab] api={base_url}", flush=True)

    token = create_staff_token(email="warehouse-lab@test.local", role="gerencia", user_id=9001)
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    import httpx

    report: dict = {"steps": [], "ok": True}
    try:
        with httpx.Client(base_url=base_url, timeout=30.0) as client:
            # health
            r = client.get("/health")
            assert r.status_code == 200 and r.json().get("dsn_isolated") is True
            report["steps"].append({"health": r.status_code})

            # 403 sin override path: use a client without going through override? 
            # Lab always overrides management — test deny via separate TestClient
            deny_app = build_lab_app(uri)

            def _deny():
                from fastapi import HTTPException

                raise HTTPException(status_code=403, detail="forbidden")

            from backend.auth.permissions import require_management_access as rma

            deny_app.dependency_overrides[rma] = _deny
            tc = TestClient(deny_app)
            assert tc.get("/warehouse/facilities").status_code == 403
            report["steps"].append({"deny_403": True})

            me = client.get("/auth/me", headers=headers)
            assert me.status_code == 200
            assert me.json()["role"] == "gerencia"

            facs = client.get("/warehouse/facilities", headers=headers).json()["items"]
            assert facs and facs[0]["code"] == "BC"
            fid = facs[0]["id"]
            floors = client.get(f"/warehouse/facilities/{fid}/floors", headers=headers).json()["items"]
            floor = floors[0]
            assert floor["code"] == "PISO1"
            draft = client.post(f"/warehouse/floors/{floor['id']}/draft", headers=headers, json={}).json()
            rev_id = draft["revision"]["id"]
            version = draft["revision"]["row_version"]
            bundle = client.get(f"/warehouse/revisions/{rev_id}", headers=headers).json()
            assert bundle["element_count"] >= 14
            codes = {e["physical"]["code"] for e in bundle["elements"]}
            assert "IZ-SU" in codes and "P2" in codes
            p2 = next(e for e in bundle["elements"] if e["physical"]["code"] == "P2")
            report["steps"].append({"loaded_bc": True, "rev": rev_id, "p2": p2["physical"]["id"]})

            # Preview generate 4+5
            preview = client.post(
                "/warehouse/meta/generate-positions/preview",
                headers=headers,
                json={
                    "parent": {
                        "x_m": p2["geometry"]["x_m"],
                        "y_m": p2["geometry"]["y_m"],
                        "width_m": p2["geometry"]["width_m"],
                        "depth_m": p2["geometry"]["depth_m"],
                        "code": "P2",
                        "element_type": "aisle",
                    },
                    "parent_physical_id": p2["physical"]["id"],
                    "preset": "4+5",
                    "default_orientation": "standard",
                    "anchor": "end",
                    "code_prefix": "P2",
                    "floor_length_m": float(floor["length_m"]),
                    "floor_width_m": float(floor["width_m"]),
                    "existing_elements": bundle["elements"],
                },
            ).json()
            assert preview["persists"] is False
            assert preview["position_count"] == 9
            assert abs(preview["extension_outside_parent_m"] - 0.30) < 1e-6
            report["steps"].append({"preview_4_5": preview["position_count"]})

            # Confirm: batch upsert first position only + modify
            pos0 = preview["positions"][0]
            g = pos0["geometry"]
            g["x_m"] = float(g["x_m"]) + 0.05  # modificar
            save1 = client.post(
                f"/warehouse/revisions/{rev_id}/elements/batch",
                headers=headers,
                json={
                    "expected_row_version": version,
                    "upserts": [
                        {
                            "physical_element_id": None,
                            "parent_id": p2["physical"]["id"],
                            "element_type": "position",
                            "code": pos0["code"],
                            "name": pos0["name"],
                            "meta_json": {"storage_kind": "pallet_standard"},
                            "geometry": g,
                        },
                        {
                            "physical_element_id": None,
                            "parent_id": next(
                                e["physical"]["id"]
                                for e in bundle["elements"]
                                if e["physical"]["code"] == "IZ"
                            ),
                            "element_type": "loose_storage",
                            "code": "IZ-SU-LAB",
                            "name": "Sueltos lab",
                            "meta_json": {"storage_kind": "loose_no_pallet"},
                            "geometry": {
                                "x_m": 0.1,
                                "y_m": 0.1,
                                "width_m": 2.52,
                                "depth_m": 1.06,
                                "validation_status": "pending_validation",
                                "constraints_json": {"storage_kind": "loose_no_pallet"},
                                "observations": "creado en e2e lab",
                                "meta_json": {"provisional": True},
                            },
                        },
                    ],
                },
            )
            assert save1.status_code == 200, save1.text
            version2 = save1.json()["revision"]["row_version"]
            report["steps"].append({"saved": True, "version": version2})

            # Reload persistence
            bundle2 = client.get(f"/warehouse/revisions/{rev_id}", headers=headers).json()
            codes2 = {e["physical"]["code"] for e in bundle2["elements"]}
            assert pos0["code"] in codes2
            assert "IZ-SU-LAB" in codes2
            saved_pos = next(e for e in bundle2["elements"] if e["physical"]["code"] == pos0["code"])
            assert abs(float(saved_pos["geometry"]["x_m"]) - float(g["x_m"])) < 1e-6
            report["steps"].append({"persisted_reload": True})

            # Audit
            audit = client.get(f"/warehouse/revisions/{rev_id}/audit?limit=20", headers=headers).json()["items"]
            assert any(a["action"] == "batch_saved" for a in audit)
            report["steps"].append({"audit": True})

            # Validate endpoint
            val = client.post(f"/warehouse/revisions/{rev_id}/validate", headers=headers, json={}).json()
            assert "counts" in val
            report["steps"].append({"validate": val["counts"]})

            # Concurrency 409
            bad = client.post(
                f"/warehouse/revisions/{rev_id}/elements/batch",
                headers=headers,
                json={"expected_row_version": version2 - 1, "upserts": []},
            )
            assert bad.status_code == 409
            report["steps"].append({"conflict_409": True})

        # Persist lab info for Next browser session
        lab_info = {
            "api_base": base_url,
            "dsn": uri,
            "token": token,
            "email": "warehouse-lab@test.local",
            "role": "gerencia",
            "company_id": 3,
            "revision_id": rev_id,
            "floor_id": floor["id"],
            "facility_id": fid,
        }
        (ROOT / "_tmp_warehouse_e2e_lab.json").write_text(
            json.dumps(lab_info, indent=2), encoding="utf-8"
        )
        (ART / "inteligencia_bodega_1_3_e2e_api.json").write_text(
            json.dumps(report, indent=2), encoding="utf-8"
        )
        print(json.dumps({"e2e_api_ok": True, **lab_info, "report_steps": len(report["steps"])}, indent=2), flush=True)

        # Keep server alive briefly if WAREHOUSE_E2E_KEEP=1 for browser
        keep = os.getenv("WAREHOUSE_E2E_KEEP", "").strip() in ("1", "true", "yes")
        if keep:
            print("[e2e-lab] keeping API up (WAREHOUSE_E2E_KEEP=1). Ctrl+C to stop.", flush=True)
            while True:
                time.sleep(1)
        return 0
    except Exception as exc:
        report["ok"] = False
        report["error"] = repr(exc)
        (ART / "inteligencia_bodega_1_3_e2e_api.json").write_text(
            json.dumps(report, indent=2), encoding="utf-8"
        )
        print(f"[e2e-lab] FAIL {exc!r}", flush=True)
        return 1
    finally:
        server.should_exit = True
        try:
            closer = getattr(srv, "cleanup", None) or getattr(srv, "close", None)
            if callable(closer):
                closer()
        except Exception as e:  # noqa: BLE001
            print(f"[e2e-lab] pg_cleanup_warn={e!r}", flush=True)


if __name__ == "__main__":
    raise SystemExit(main())
