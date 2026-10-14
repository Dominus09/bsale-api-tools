"""Analítica V2 — clasificación de movimientos y señales de costo (impuestos: test_analytics_v2_tax_engine.py)."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from backend.services.analytics_v2.contracts import (
    COMPARABLE_MOVEMENTS,
    AnomalyCode,
    CostReliability,
    MovementClass,
    Severity,
)
from backend.services.analytics_v2.cost_signals import (
    LineFacts,
    average_divergence,
    change_pct,
    is_comparable_line,
    line_anomalies,
    sku_reliability,
)
from backend.services.analytics_v2.movement_classification import (
    DOCUMENT_RULES,
    classify_movement,
    movement_class_sql,
)
D = Decimal

# ---------------------------------------------------------------- clasificación

@pytest.mark.parametrize(
    ("document", "dispatch", "expected"),
    [
        ("Factura", 0, MovementClass.FACTURA),
        ("GUÍA DE DESPACHO ELECTRÓNICA", 0, MovementClass.GUIA_DESPACHO),
        ("Guía", None, MovementClass.GUIA_DESPACHO),
        ("NOTA DE CRÉDITO ELECTRÓNICA T", 0, MovementClass.NOTA_CREDITO),
        ("Sin Documento", 0, MovementClass.SIN_DOCUMENTO),
        ("GUÍA DE DESPACHO ELECTRÓNICA 26354 Ingreso por despacho anulado", 0, MovementClass.AJUSTE),
        ("Ajuste de inventario", 0, MovementClass.AJUSTE),
        (None, 0, MovementClass.NO_DETERMINADO),
        ("   ", None, MovementClass.NO_DETERMINADO),
        ("Boleta de honorarios", 0, MovementClass.NO_DETERMINADO),
        ("GUÍA DE DESPACHO ELECTRÓNICA", 812, MovementClass.TRANSFERENCIA_INTERNA),
        ("Factura", 5, MovementClass.TRANSFERENCIA_INTERNA),
    ],
)
def test_classify_movement_real_labels(document, dispatch, expected):
    assert classify_movement(document, dispatch).movement_class == expected


def test_only_invoices_and_guides_are_comparable():
    assert COMPARABLE_MOVEMENTS == {MovementClass.FACTURA, MovementClass.GUIA_DESPACHO}
    assert classify_movement("Factura", 0).comparable
    assert not classify_movement("NOTA DE CRÉDITO ELECTRÓNICA T", 0).comparable
    assert not classify_movement("Sin Documento", 0).comparable
    assert not classify_movement("Factura", 9).comparable


def test_movement_sql_mirrors_python_rules_in_order():
    sql = movement_class_sql("r.document", "r.internal_dispatch_id")
    assert "%s" not in sql
    positions = [sql.index(f"'{token}'") for token, _cls, _reason in DOCUMENT_RULES]
    assert positions == sorted(positions)
    assert sql.index("coalesce(r.internal_dispatch_id, 0) <> 0") < positions[0]
    for _token, cls, _reason in DOCUMENT_RULES:
        assert f"THEN '{cls.value}'" in sql
    assert f"ELSE '{MovementClass.NO_DETERMINADO.value}'" in sql


def test_annulled_dispatch_is_not_a_purchase_even_with_guide_label():
    result = classify_movement("GUÍA DE DESPACHO ELECTRÓNICA 26661 Ingreso por despacho anulado", 0)
    assert result.movement_class == MovementClass.AJUSTE
    assert not result.comparable


# ---------------------------------------------------------------- señales

def _line(**kw) -> LineFacts:
    base = dict(
        movement_class=MovementClass.FACTURA,
        cost=D("1000"),
        quantity=D("12"),
        details_complete=True,
    )
    base.update(kw)
    return LineFacts(**base)


def _codes(line: LineFacts) -> set[AnomalyCode]:
    return {a.code for a in line_anomalies(line)}


def test_clean_purchase_has_no_anomalies():
    assert _codes(_line(previous_comparable_cost=D("950"), median_comparable_quantity=D("12"))) == set()


def test_zero_and_null_costs():
    zero = line_anomalies(_line(cost=D("0")))
    assert [(a.code, a.severity) for a in zero] == [(AnomalyCode.COSTO_CERO, Severity.ALTA)]
    assert _codes(_line(cost=None)) == {AnomalyCode.COSTO_NULO}
    nc_zero = line_anomalies(_line(movement_class=MovementClass.NOTA_CREDITO, cost=D("0")))
    assert nc_zero[0].severity == Severity.MEDIA
    assert _codes(_line(movement_class=MovementClass.TRANSFERENCIA_INTERNA, cost=D("0"))) == set()


def test_cost_variations_thresholds():
    assert change_pct(D("1300"), D("1000")) == D("30.00")
    assert _codes(_line(cost=D("1299"), previous_comparable_cost=D("1000"))) == set()
    rise = line_anomalies(_line(cost=D("1300"), previous_comparable_cost=D("1000")))
    assert [(a.code, a.severity) for a in rise] == [(AnomalyCode.ALZA_ANORMAL, Severity.MEDIA)]
    severe = line_anomalies(_line(cost=D("2000"), previous_comparable_cost=D("1000")))
    assert severe[0].severity == Severity.ALTA
    drop = line_anomalies(_line(cost=D("700"), previous_comparable_cost=D("1000")))
    assert [(a.code, a.severity) for a in drop] == [(AnomalyCode.BAJA_SOSPECHOSA, Severity.MEDIA)]
    severe_drop = line_anomalies(_line(cost=D("400"), previous_comparable_cost=D("1000")))
    assert severe_drop[0].severity == Severity.ALTA
    assert change_pct(D("10"), D("0")) is None
    assert _codes(_line(movement_class=MovementClass.NOTA_CREDITO, cost=D("5000"), previous_comparable_cost=D("1000"))) == set()


def test_quantities_and_modifications():
    assert AnomalyCode.CANTIDAD_EXTREMA in _codes(_line(quantity=D("121"), median_comparable_quantity=D("12")))
    assert AnomalyCode.CANTIDAD_EXTREMA not in _codes(_line(quantity=D("120"), median_comparable_quantity=D("12")))
    assert _codes(_line(quantity=D("0"))) == {AnomalyCode.CANTIDAD_NO_POSITIVA}
    modified = line_anomalies(_line(cost_modified=True))
    assert [(a.code, a.severity) for a in modified] == [(AnomalyCode.RECEPCION_MODIFICADA, Severity.ALTA)]
    assert line_anomalies(_line(quantity_modified=True))[0].severity == Severity.MEDIA
    assert _codes(_line(variant_modified=True)) == {AnomalyCode.CAMBIO_VARIANTE}
    assert _codes(_line(details_complete=False)) == {AnomalyCode.DETALLE_INCOMPLETO}
    assert _codes(_line(movement_class=MovementClass.NO_DETERMINADO)) == {
        AnomalyCode.MOVIMIENTO_NO_DETERMINADO
    }


def test_comparable_line_requires_purchase_positive_values_and_complete_detail():
    assert is_comparable_line(MovementClass.GUIA_DESPACHO, D("10"), D("1"), True)
    assert not is_comparable_line(MovementClass.GUIA_DESPACHO, D("0"), D("1"), True)
    assert not is_comparable_line(MovementClass.FACTURA, D("10"), D("0"), True)
    assert not is_comparable_line(MovementClass.FACTURA, D("10"), D("1"), False)
    assert not is_comparable_line(MovementClass.AJUSTE, D("10"), D("1"), True)


TODAY = date(2026, 10, 13)


def test_reliability_states():
    def rel(**kw):
        base = dict(
            average_cost=D("1000"),
            last_comparable_cost=D("1010"),
            last_comparable_date=date(2026, 10, 1),
            last_comparable_anomalies=[],
            today=TODAY,
        )
        base.update(kw)
        return sku_reliability(**base)[0]

    assert rel() == CostReliability.CONSISTENTE
    assert rel(last_comparable_date=date(2026, 3, 1)) == CostReliability.DESACTUALIZADO
    assert rel(last_comparable_cost=None) == CostReliability.SOLO_PROMEDIO_BSALE
    assert rel(last_comparable_cost=None, average_cost=D("0")) == CostReliability.SIN_COSTO
    assert rel(last_comparable_cost=None, average_cost=None) == CostReliability.SIN_COSTO
    rise = line_anomalies(_line(cost=D("2000"), previous_comparable_cost=D("1000")))
    assert rel(last_comparable_anomalies=rise) == CostReliability.REQUIERE_REVISION
    undetermined = line_anomalies(_line(movement_class=MovementClass.NO_DETERMINADO))
    assert rel(last_comparable_anomalies=undetermined) == CostReliability.CONSISTENTE


def test_average_divergence_is_flagged_but_average_is_never_replaced():
    assert average_divergence(D("1400"), D("1000")) is None
    anomaly = average_divergence(D("1600"), D("1000"))
    assert anomaly is not None and anomaly.code == AnomalyCode.DIFERENCIA_PROMEDIO_BSALE
    assert average_divergence(D("1600"), None) is None
