"""P4 complemento: formatos WhatsApp + demos sin escritura."""

from __future__ import annotations

from backend.services.distribuidora import delivery_whatsapp_report as wa
from backend.services.distribuidora import delivery_visit_service as svc


def test_templates_a_to_f():
    cases = [
        ("completed", None, "completed", "✅ ENTREGA COMPLETADA"),
        ("partial", None, "partial", "⚠️ ENTREGA PARCIAL"),
        ("not_delivered", "closed_store", "closed_store", "🔒 LOCAL CERRADO"),
        ("not_delivered", "not_found", "not_found", "📍 LOCAL NO ENCONTRADO"),
        ("not_delivered", "rejected_order", "rejected_order", "❌ PEDIDO RECHAZADO"),
        ("not_delivered", "other", "other_incident", "⚠️ INCIDENCIA DE ENTREGA"),
    ]
    for status, reason, key, header in cases:
        assert wa.resolve_template_key(status=status, result_reason=reason) == key
        text = wa.build_report_text(
            {
                "status": status,
                "result_reason": reason,
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
                "products": [],
                "collections": [],
                "gps_registered": True,
                "evidence_status": "sí",
                "signature_status": "pendiente",
            }
        )
        assert header in text
        assert "Grupo La Quillotana" in text
        assert "-42." not in text
        assert "chat.whatsapp.com" not in text.lower()


def test_demo_scenarios_marked_and_safe():
    demos = wa.demo_scenarios()
    assert len(demos) == 6
    for d in demos:
        assert d["demo"] is True
        assert d["writes_production"] is False
        assert "PRUEBA — QUILLOTANA APP" in d["report_text"]
        assert "DATOS FICTICIOS" in d["report_text"]
        assert d["share"]["delivery_claimed"] is False
        assert "chat.whatsapp.com" not in d["report_text"].lower()
        assert "invite" not in d["report_text"].lower()


def test_list_demo_service_no_db(monkeypatch):
    out = svc.list_demo_whatsapp_reports(user={"id": 1, "email": "a@b.c"})
    assert out["writes_production"] is False
    assert out["count"] == 6
    assert out["adapter"]["official_api_ready"] is False


def test_partial_highlights_pending_reasons():
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
            "documents": [],
            "products": [
                {
                    "product_name": "Prod",
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
    assert "ENTREGA PARCIAL" in text
    assert "Faltante físico" in text
    assert "Transferencia informada" in text
    assert "Pago verificado: no" in text


def test_share_url_has_no_group_id():
    url = wa.whatsapp_share_url("hola")
    assert url.startswith("https://wa.me/?text=")
    assert "chat.whatsapp.com" not in url
