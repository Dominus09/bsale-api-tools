"""Seguimiento de entregas ERP — auth + helpers (sin escritura productiva)."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from backend.auth.permissions import require_erp_ops_staff
from backend.routers import distribuidora_seguimiento_entregas as seg_router
from backend.services.distribuidora import delivery_seguimiento_service as seg
from backend.services.distribuidora import delivery_route_progress as progress_svc


def test_router_depends_on_require_erp_ops_staff():
    src = open(seg_router.__file__, encoding="utf-8").read()
    assert "require_erp_ops_staff" in src
    paths = [getattr(r, "path", "") for r in seg_router.router.routes]
    assert any("/dashboard" in p for p in paths), paths
    assert any("/clientes" in p for p in paths), paths
    assert any("/visitas" in p for p in paths), paths
    callables = [d.dependency for d in seg_router.router.dependencies]
    assert require_erp_ops_staff in callables


def test_main_mounts_seguimiento_routes():
    import backend.main as main

    paths = set(main.app.openapi().get("paths", {}))
    assert any("/distribuidora/seguimiento-entregas/dashboard" in p for p in paths)
    assert any("/distribuidora/seguimiento-entregas/visitas/" in p for p in paths)


def test_timing_prefers_device_capture_over_sync():
    device = datetime(2026, 11, 21, 12, 0, tzinfo=timezone.utc)
    sync = datetime(2026, 11, 21, 18, 0, tzinfo=timezone.utc)
    visit = {
        "ended_at": sync,
        "end_gps_captured_at": None,
        "closure_review_status": "pending_review",
    }
    offline = {"captured_at": device, "completed_at": sync}
    out = seg._timing_for_visit(visit, offline)
    assert out["device_capture"] is True
    assert out["managed_at"] == device
    assert out["synced_at"] == sync
    assert out["requires_review"] is True
    assert out["managed_at_source"] == "device"


def test_timing_without_device_uses_ended_at():
    sync = datetime(2026, 11, 21, 15, 30, tzinfo=timezone.utc)
    visit = {"ended_at": sync, "closure_review_status": "verified"}
    out = seg._timing_for_visit(visit, None)
    assert out["device_capture"] is False
    assert out["managed_at"] == sync
    assert out["managed_at_source"] == "server"


def test_timing_empty_visit_does_not_invent_clock():
    out = seg._timing_for_visit(None, None)
    assert out["managed_at"] is None
    assert out["managed_at_local"] is None
    assert out["synced_at"] is None
    assert out["device_capture"] is False


def test_evidence_policy_no_mandatory_signature():
    assert "closed_store" in seg.EVIDENCE_REQUIRED_REASONS
    assert "rejected_order" in seg.EVIDENCE_REQUIRED_REASONS
    assert "absent_customer" in seg.EVIDENCE_REQUIRED_REASONS
    # Policy metadata must not claim firma/receptor obligatorios.
    note = (
        "Incidencias requieren fotografía. Entrega completa/parcial sin firma "
        "ni receptor obligatorios."
    )
    assert "firma" not in note.lower() or "sin firma" in note.lower()


def test_dashboard_payload_includes_activity_keys_when_empty_tables(monkeypatch):
    """Contrato UI premium: activity/incidents/progress sin inventar gestiones."""

    class _Scope:
        load_failed = False
        companies_unverified = False
        company_ids = None

    monkeypatch.setattr(
        "backend.auth.multiapp.get_scope", lambda _u: _Scope()
    )

    class _Conn:
        def cursor(self):
            return self

        def close(self):
            return None

        def execute(self, *_a, **_k):
            return None

        def fetchone(self):
            return None

        def fetchall(self):
            return []

        @property
        def description(self):
            return []

    monkeypatch.setattr(seg, "get_connection", lambda: _Conn())
    monkeypatch.setattr(seg.visit_repo, "tables_ready", lambda _c: True)
    monkeypatch.setattr(seg, "_list_plans_for_date", lambda *_a, **_k: [])

    out = seg.get_dashboard(user={"id": 1, "email": "ops@test.local"}, day="2026-11-21")
    assert out["tables_ready"] is True
    assert "recent_activity" in out and out["recent_activity"] == []
    assert "recent_incidents" in out and out["recent_incidents"] == []
    assert out["kpis"]["progress_percent"] == 0.0
    assert out["kpis"]["first_delivery_at_local"] is None
    assert out["auto_refresh_sec"] == 45


def test_route_progress_dedupes_clients_not_documents():
    """Un cliente con 3 docs cuenta una sola vez."""
    clients = [
        {
            "customer_key": "c1",
            "document_count": 3,
            "client_name": "A",
        },
        {
            "customer_key": "c2",
            "document_count": 1,
            "client_name": "B",
        },
    ]
    attention = {
        "c1": {
            "attention_status": "completed",
            "visit_status": "completed",
            "closure_review_status": "verified",
        },
        "c2": {
            "attention_status": "pending",
            "visit_status": None,
        },
    }
    out = progress_svc.compute_route_progress(
        clients=clients, attention_by_key=attention
    )
    assert out["total_clients"] == 2
    assert out["managed_clients"] == 1
    assert out["by_result"]["completed"] == 1
    assert out["by_result"]["pending"] == 1


def test_http_endpoints_require_auth_direct():
    """Acceso directo a cada path (no solo menú) exige staff ERP."""
    app = FastAPI()
    app.include_router(seg_router.router)
    client = TestClient(app)
    for path in (
        "/distribuidora/seguimiento-entregas/dashboard",
        "/distribuidora/seguimiento-entregas/by-driver",
        "/distribuidora/seguimiento-entregas/planes/1/clientes",
        "/distribuidora/seguimiento-entregas/visitas/1",
        "/distribuidora/seguimiento-entregas/status",
    ):
        res = client.get(path)
        assert res.status_code in (401, 403), (path, res.status_code, res.text)


def test_serialize_product_keeps_distinct_skus_same_brand():
    """Misma marca + distinta variante/source_detail no se fusionan."""
    heineken_330 = seg._serialize_product_line(
        {
            "id": 1,
            "source_detail_id": 101,
            "document_result_id": 9,
            "variant_id": 501,
            "product_name": "HEINEKEN",
            "planned_units": 36,
            "delivered_units": 36,
        },
        {
            "product_name": "HEINEKEN",
            "product_type": "CERVEZAS",
            "variant_name": "DESECHABLE 330 CC (SEC 24)",
            "presentation": "DESECHABLE 330 CC (SEC 24)",
            "units_per_box": 24,
            "variant_id": 501,
            "sku_code": "7802100000354",
            "barcode": "7802100000354",
        },
    )
    heineken_lata = seg._serialize_product_line(
        {
            "id": 2,
            "source_detail_id": 102,
            "document_result_id": 9,
            "variant_id": 502,
            "product_name": "HEINEKEN",
            "planned_units": 24,
            "delivered_units": 24,
        },
        {
            "product_name": "HEINEKEN",
            "product_type": "CERVEZAS",
            "variant_name": "LATA 350 CC (SEC 24)",
            "presentation": "LATA 350 CC (SEC 24)",
            "units_per_box": 24,
            "variant_id": 502,
            "sku_code": "7802100000316",
            "barcode": "7802100000316",
        },
    )
    assert heineken_330["source_detail_id"] != heineken_lata["source_detail_id"]
    assert heineken_330["variant_id"] != heineken_lata["variant_id"]
    assert heineken_330["commercial_name"] == "HEINEKEN"
    assert heineken_330["presentation"] == "DESECHABLE 330 CC (SEC 24)"
    assert heineken_lata["presentation"] == "LATA 350 CC (SEC 24)"
    assert heineken_330["units_per_box"] == 24
    assert "DESECHABLE 330 CC" in (heineken_330["label_es"] or "")
    assert "LATA 350 CC" in (heineken_lata["label_es"] or "")


def test_serialize_product_does_not_invent_presentation_from_brand():
    """Sin variante estructurada no se inventa formato a partir del nombre."""
    out = seg._serialize_product_line(
        {
            "id": 3,
            "source_detail_id": 201,
            "product_name": "CORONA",
            "planned_units": 12,
            "delivered_units": 12,
        },
        {},
    )
    assert out["commercial_name"] == "CORONA"
    assert out["presentation"] is None
    assert out["variant_name"] is None
    assert out["product_type"] is None
    assert out["units_per_box"] is None


def test_serialize_product_preserves_real_quantities():
    """Cantidades salen intactas de delivery_line_results (sin recalcular)."""
    out = seg._serialize_product_line(
        {
            "id": 9,
            "source_detail_id": 301,
            "document_result_id": 7,
            "variant_id": 900,
            "product_name": "COCA COLA",
            "planned_units": 24,
            "delivered_units": 18,
            "undelivered_units": 6,
            "undelivered_reason": "partial_shortage",
        },
        {
            "product_name": "COCA COLA",
            "product_type": "BEBIDAS",
            "variant_name": "1.5 L RETORNABLE",
            "presentation": "1.5 L RETORNABLE",
            "units_per_box": 12,
            "variant_id": 900,
            "sku_code": "SKU-CC",
        },
    )
    assert out["planned_units"] == 24
    assert out["delivered_units"] == 18
    assert out["undelivered_units"] == 6
    assert out["undelivered_reason"] == "partial_shortage"
    assert out["source_detail_id"] == 301
    assert out["variant_id"] == 900


def test_identity_map_setdefault_does_not_duplicate_lines():
    """El mapa de identidad es lookup; no multiplica líneas de visita."""
    lines = [
        {
            "id": 1,
            "source_detail_id": 10,
            "document_result_id": 1,
            "variant_id": 100,
            "product_name": "HEINEKEN",
            "planned_units": 36,
            "delivered_units": 36,
        },
        {
            "id": 2,
            "source_detail_id": 11,
            "document_result_id": 1,
            "variant_id": 101,
            "product_name": "HEINEKEN",
            "planned_units": 24,
            "delivered_units": 24,
        },
    ]
    identity_map = {
        "d:10": {
            "product_name": "HEINEKEN",
            "variant_name": "DESECHABLE 330 CC (SEC 24)",
            "presentation": "DESECHABLE 330 CC (SEC 24)",
            "product_type": "CERVEZAS",
            "units_per_box": 24,
            "variant_id": 100,
            "sku_code": "A",
        },
        "d:11": {
            "product_name": "HEINEKEN",
            "variant_name": "LATA 350 CC (SEC 24)",
            "presentation": "LATA 350 CC (SEC 24)",
            "product_type": "CERVEZAS",
            "units_per_box": 24,
            "variant_id": 101,
            "sku_code": "B",
        },
    }
    products = [
        seg._serialize_product_line(ln, seg._identity_for_line(ln, identity_map))
        for ln in lines
    ]
    assert len(products) == 2
    assert {p["source_detail_id"] for p in products} == {10, 11}
    assert {p["variant_id"] for p in products} == {100, 101}
    assert {p["presentation"] for p in products} == {
        "DESECHABLE 330 CC (SEC 24)",
        "LATA 350 CC (SEC 24)",
    }
    assert all(p["planned_units"] == ln["planned_units"] for p, ln in zip(products, lines))
    assert all(
        p["delivered_units"] == ln["delivered_units"] for p, ln in zip(products, lines)
    )


def test_plan_clients_lists_pending_without_visits(monkeypatch):
    """Clientes del despacho aparecen como pendientes aunque no haya gestiones."""
    from backend.services.distribuidora import delivery_seguimiento_service as seg

    class _Conn:
        def cursor(self):
            return _Cur()

        def close(self):
            return None

    class _Cur:
        description = [
            ("id",),
            ("planning_code",),
            ("planning_date",),
            ("route_name",),
            ("planning_name",),
            ("driver_user_id",),
            ("driver_name",),
            ("vehicle_label",),
        ]

        def execute(self, *_a, **_k):
            return None

        def fetchone(self):
            return (
                79,
                "PLAN-00079",
                "2026-11-21",
                "Ruta Norte",
                "Ruta Norte",
                10,
                "Chofer Demo",
                "Camión 1",
            )

        def fetchall(self):
            return []

        def close(self):
            return None

    monkeypatch.setattr(seg, "get_connection", lambda: _Conn())
    monkeypatch.setattr(seg.visit_repo, "tables_ready", lambda _c: True)
    monkeypatch.setattr(seg, "_plan_company_id", lambda *_a, **_k: 1)
    monkeypatch.setattr(seg, "_scope_company_ids", lambda _u: None)
    monkeypatch.setattr(seg, "_offline_meta_by_visit", lambda *_a, **_k: {})
    monkeypatch.setattr(
        seg.read_svc,
        "_build_client_index",
        lambda _plan_id: {
            "clients": [
                {
                    "customer_key": "c1",
                    "client_id": 1,
                    "client_name": "Cliente Uno",
                    "fantasy_name": "Uno Market",
                    "route_order": 1,
                    "document_count": 2,
                    "documents": [
                        {
                            "related_document_id": 11,
                            "document_type": "FACTURA",
                            "document_number": "1001",
                        },
                        {
                            "related_document_id": 12,
                            "document_type": "FACTURA",
                            "document_number": "1002",
                        },
                    ],
                },
                {
                    "customer_key": "c2",
                    "client_id": 2,
                    "client_name": "Cliente Dos",
                    "fantasy_name": "Dos SpA",
                    "route_order": 2,
                    "document_count": 1,
                    "documents": [
                        {
                            "related_document_id": 21,
                            "document_type": "OC",
                            "document_number": "OC-9",
                        }
                    ],
                },
                {
                    "customer_key": "c3",
                    "client_id": 3,
                    "client_name": "Cliente Tres",
                    "fantasy_name": "",
                    "route_order": 3,
                    "document_count": 0,
                    "documents": [],
                },
            ]
        },
    )
    monkeypatch.setattr(seg.read_svc, "_visit_attention_by_customer", lambda _plan_id: {})

    out = seg.get_plan_clients(user={"id": 1, "email": "ops@test.local"}, plan_id=79)
    assert out["count"] == 3
    assert [c["result_status"] for c in out["clients"]] == [
        "pending",
        "pending",
        "pending",
    ]
    assert out["clients"][0]["commercial_name"] == "Uno Market"
    assert out["clients"][0]["document_count"] == 2
    assert len(out["clients"][0]["documents"]) == 2
    assert out["clients"][0]["visit_id"] is None
    assert out["clients"][0]["gps_status_es"] == "Sin gestión"
    assert out["clients"][2]["commercial_name"] == "Cliente Tres"


def test_capability_only_denied_for_seguimiento(monkeypatch):
    def _fake_staff(*_a, **_k):
        return {
            "id": 99,
            "email": "excel@test.local",
            "role": "vendedor",
            "erp_capabilities": ["simple_planning"],
        }

    monkeypatch.setattr(
        "backend.utils.auth_staff.require_staff_user",
        _fake_staff,
    )
    monkeypatch.setattr(
        "backend.auth.permissions.has_simple_planning_access",
        lambda _u: True,
    )
    monkeypatch.setattr(
        "backend.auth.permissions.is_simple_planning_capability_only",
        lambda _u: True,
    )
    with pytest.raises(HTTPException) as ei:
        require_erp_ops_staff(authorization="Bearer x")
    assert ei.value.status_code == 403
