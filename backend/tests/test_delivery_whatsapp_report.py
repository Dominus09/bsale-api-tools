"""P4.3: formatos WhatsApp compactos (máx. 6 líneas) + demos sin escritura."""

from __future__ import annotations

from backend.services.distribuidora import delivery_whatsapp_report as wa
from backend.services.distribuidora import delivery_visit_service as svc


def test_templates_a_to_f_compact():
    cases = [
        ("completed", None, "completed"),
        ("partial", None, "partial"),
        ("not_delivered", "closed_store", "closed_store"),
        ("not_delivered", "not_found", "not_found"),
        ("not_delivered", "rejected_order", "rejected_order"),
        ("not_delivered", "absent_customer", "absent_customer"),
        ("not_delivered", "other", "other_incident"),
    ]
    for status, reason, key in cases:
        assert wa.resolve_template_key(status=status, result_reason=reason) == key
        text = wa.build_report_text(
            {
                "status": status,
                "result_reason": reason,
                "result_note": "Obs incidencia de prueba" if status == "not_delivered" else "",
                "planning_code": "PLAN-X",
                "client_name": "Cliente",
                "driver_name": "Chofer",
                "visit_date": "09/11/2026",
                "started_at_local": "10:00",
                "ended_at_local": "10:20",
                "duration_label": "20 min",
                "documents": [
                    {
                        "document_type": "Boleta",
                        "document_number": 1,
                        "document_total": 1000,
                    }
                ],
                "products": [
                    {
                        "planned_units": 2,
                        "delivered_units": 2 if status != "not_delivered" else 0,
                        "undelivered_units": 0 if status != "not_delivered" else 2,
                    }
                ],
                "collections": [
                    {
                        "method": "cash",
                        "declared_amount": 500,
                    },
                    {
                        "method": "pending",
                        "declared_amount": 500,
                    },
                ],
                "gps_registered": True,
                "evidence_status": "sí",
                # Firma histórica opcional: no debe exigir ni afirmar envío.
                "signature_status": None,
            }
        )
        visible = [ln for ln in text.splitlines() if ln.strip()]
        assert len(visible) <= 6
        assert visible[0].startswith(("🟢", "🟡", "🔴"))
        assert "QUILLOTANA |" in visible[0]
        assert "-42." not in text
        assert "chat.whatsapp.com" not in text.lower()
        assert "cash" not in text.lower() or "Efectivo" in text
        assert "r2.dev" not in text.lower()
        assert "cloudflare" not in text.lower()
        assert "object_key" not in text.lower()
        assert "Firma:" not in text
        assert "receptor" not in text.lower()
        if status in ("completed", "partial"):
            assert "$" not in text or "💵" in text  # sin montos CLP en forma de pago nueva
            assert (
                "Gestión confirmada" in text
                or "Gestión registrada" in text
                or "Validación pendiente" in text
                or "Obs:" in text
            )
            assert "Chofer" in text and "PLAN-X" in text and "10:20" in text
        else:
            assert "Chofer" in text and "PLAN-X" in text
            assert "10:20" in text
            assert "Obs incidencia" in text or "📝" in text


def test_pending_review_keeps_red_semaphore_header():
    text = wa.build_report_text(
        {
            "status": "not_delivered",
            "result_reason": "closed_store",
            "closure_review_status": "pending_review",
            "client_name": "Cliente",
            "driver_name": "Chofer A",
            "planning_code": "PLAN-1",
            "ended_at_local": "09:15",
            "result_note": "Cerrado con candado",
            "documents": [{"document_type": "Boleta", "document_number": 1}],
            "products": [],
            "collections": [],
            "gps_registered": True,
            "gps_within_radius": False,
            "evidence_status": "sí",
        }
    )
    lines = [ln for ln in text.splitlines() if ln.strip()]
    assert lines[0] == "🔴 QUILLOTANA | LOCAL CERRADO"
    assert "GPS pendiente" in text
    assert "GPS validado" not in text
    assert "Chofer A" in text and "PLAN-1" in text and "09:15" in text
    assert "Cerrado con candado" in text
    assert "r2.dev" not in text.lower()
    assert len(lines) <= 6


def test_declarative_payment_no_amounts_in_whatsapp():
    text = wa.build_report_text(
        {
            "status": "completed",
            "client_name": "Cliente",
            "documents": [{"document_type": "Factura", "document_number": 99}],
            "products": [
                {"planned_units": 2, "delivered_units": 2, "undelivered_units": 0}
            ],
            "collections": [
                {"method": "cash_and_transfer", "declared_amount": None},
            ],
            "signature_status": "sí",
            "gps_registered": True,
        }
    )
    assert "Efectivo + transf" in text
    assert "$" not in text
    assert "no verificada" in text.lower()


def test_demo_scenarios_marked_compact_and_safe():
    demos = wa.demo_scenarios()
    assert len(demos) == 6
    for d in demos:
        assert d["demo"] is True
        assert d["writes_production"] is False
        text = d["report_text"]
        visible = [ln for ln in text.splitlines() if ln.strip()]
        assert len(visible) <= 6
        assert "PRUEBA" in text or "ficticios" in text.lower() or "🧪" in text
        assert d["share"]["delivery_claimed"] is False
        assert "chat.whatsapp.com" not in text.lower()
        assert "invite" not in text.lower()
        # No afirmar verificación falsa de transferencia
        if "Transf" in text:
            assert "no verificada" in text.lower() or "informada" in text.lower() or "inf." in text.lower()


def test_list_demo_service_no_db(monkeypatch):
    out = svc.list_demo_whatsapp_reports(user={"id": 1, "email": "a@b.c"})
    assert out["writes_production"] is False
    assert out["count"] == 6
    assert out["adapter"]["official_api_ready"] is False


def test_partial_highlights_pending_compact():
    text = wa.build_report_text(
        {
            "status": "partial",
            "planning_code": "P",
            "client_name": "C",
            "driver_name": "D",
            "visit_date": "09/11/2026",
            "started_at_local": "11:00",
            "ended_at_local": "11:30",
            "duration_label": "30 min",
            "documents": [{"document_type": "Fact.", "document_number": "TEST-001"}],
            "products": [
                {
                    "product_name": "Prod",
                    "planned_units": 3,
                    "delivered_units": 1,
                    "undelivered_units": 2,
                    "undelivered_reason": "physical_shortage",
                }
            ],
            "collections": [
                {
                    "method": "transfer_declared",
                    "document_amount": 10000,
                    "declared_amount": 10000,
                    "verified_amount": None,
                    "pending_balance": 0,
                }
            ],
            "evidence_status": "no",
            "signature_status": "sí",
        }
    )
    visible = [ln for ln in text.splitlines() if ln.strip()]
    assert len(visible) <= 6
    assert "ENTREGA PARCIAL" in text
    assert "Transf" in text
    assert "verificada" not in text.lower() or "no verificada" in text.lower()


def test_share_url_has_no_group_id():
    bundle = wa.build_report_bundle(
        {
            "status": "completed",
            "client_name": "X",
            "documents": [],
            "products": [],
            "collections": [],
            "signature_status": "sí",
        }
    )
    url = bundle["share"]["whatsapp_url"]
    assert url.startswith("https://wa.me/?text=")
    assert "chat.whatsapp.com" not in url
    assert bundle["share"]["delivery_claimed"] is False
    assert bundle["share"]["auto_send"] is False


def test_incident_and_delivery_include_ops_context():
    """Local cerrado / no entregado: chofer, despacho, hora, motivo, obs, GPS."""
    text = wa.build_report_text(
        {
            "status": "not_delivered",
            "result_reason": "rejected_order",
            "client_name": "Minimarket Sur",
            "driver_name": "Pedro",
            "planning_code": "PLAN-00099",
            "ended_at_local": "14:02",
            "observations": "Cliente rechazó por precio",
            "documents": [{"document_type": "Factura", "document_number": 55}],
            "products": [],
            "collections": [],
            "gps_registered": True,
            "gps_within_radius": True,
            "evidence_status": "sí",
        }
    )
    assert "RECHAZ" in text.upper() or "rechaz" in text.lower() or "🔴" in text
    assert "Pedro" in text and "PLAN-00099" in text and "14:02" in text
    assert "precio" in text.lower()
    assert len([ln for ln in text.splitlines() if ln.strip()]) <= 6
