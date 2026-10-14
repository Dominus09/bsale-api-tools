"""Promociones V2 — dominio puro: vigencia, transiciones, verificación Bsale, excepciones y riesgos."""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

from backend.services.analytics_v2.commercial_scope import resolve_price_list_scope
from backend.services.analytics_v2.contracts import (
    CampaignApproval as A,
    CampaignTimeline as T,
    CostReliability,
    MarginComplianceStatus as S,
    MarginRisk,
    PriceVerification as V,
    PromotionCheck as C,
)
from backend.services.analytics_v2.promotion_campaigns import (
    BsalePrice,
    CampaignConflict,
    CampaignValidationError,
    approval_after_change,
    assert_editable,
    campaign_item_exception,
    campaign_timeline,
    effective_end,
    legacy_exception,
    legacy_mapping_status,
    measurement_windows,
    month_bounds,
    next_approval,
    price_verification,
    validate_dates,
    validate_promo_price,
)
from backend.services.analytics_v2.promotion_exceptions import (
    ApprovalStatus,
    ExceptionKind,
    ExceptionOrigin,
    PromotionException,
    resolve_exception,
)
from backend.tests.test_analytics_v2_margins import cost, price, rule, run

D = Decimal
TODAY = date(2026, 10, 13)
FRESH = datetime(2026, 10, 13, 3, 0, tzinfo=timezone.utc)
STALE = datetime(2026, 10, 1, 3, 0, tzinfo=timezone.utc)
SCOPE = resolve_price_list_scope(
    3,
    [
        {"price_list_id": 12, "name": "SUPERMERCADO", "state": 0, "missing_since": None},
        {"price_list_id": 13, "name": "RUTA Y WEB", "state": 0, "missing_since": None},
        {"price_list_id": 14, "name": "COMODITI ALCOHOLES", "state": 1, "missing_since": None},
        {"price_list_id": 16, "name": "MELINKA", "state": 0, "missing_since": None},
    ],
)


# ---------------------------------------------------------------- vigencia y transiciones


def test_timeline_is_derived_from_dates_and_expires_without_cron():
    start, end = date(2026, 10, 1), date(2026, 10, 31)
    assert campaign_timeline(A.APROBADA, start, end, date(2026, 9, 30)) is T.PROGRAMADA
    assert campaign_timeline(A.APROBADA, start, end, start) is T.VIGENTE
    assert campaign_timeline(A.APROBADA, start, end, end) is T.VIGENTE
    assert campaign_timeline(A.APROBADA, start, end, date(2026, 11, 1)) is T.FINALIZADA
    assert campaign_timeline(A.CANCELADA, start, end, date(2026, 10, 15)) is T.CANCELADA


def test_cancellation_cuts_the_real_end_date():
    end = date(2026, 10, 31)
    assert effective_end(end, None) == end
    assert effective_end(end, datetime(2026, 10, 20, 15, tzinfo=timezone.utc)) == date(2026, 10, 19)
    assert effective_end(end, datetime(2026, 12, 1, tzinfo=timezone.utc)) == end


def test_transitions_allowed_and_forbidden():
    assert next_approval("submit", A.BORRADOR, T.PROGRAMADA) is A.PENDIENTE_APROBACION
    assert next_approval("submit", A.RECHAZADA, T.VIGENTE) is A.PENDIENTE_APROBACION
    assert next_approval("approve", A.PENDIENTE_APROBACION, T.VIGENTE) is A.APROBADA
    assert next_approval("reject", A.PENDIENTE_APROBACION, T.VIGENTE) is A.RECHAZADA
    assert next_approval("cancel", A.APROBADA, T.VIGENTE) is A.CANCELADA
    with pytest.raises(CampaignConflict):
        next_approval("approve", A.BORRADOR, T.VIGENTE)
    with pytest.raises(CampaignConflict):
        next_approval("cancel", A.CANCELADA, T.CANCELADA)
    with pytest.raises(CampaignConflict):
        next_approval("approve", A.PENDIENTE_APROBACION, T.FINALIZADA)
    with pytest.raises(CampaignValidationError):
        next_approval("delete", A.BORRADOR, T.VIGENTE)


def test_material_changes_on_approved_campaign_require_new_approval():
    assert approval_after_change(A.APROBADA) is A.PENDIENTE_APROBACION
    assert approval_after_change(A.RECHAZADA) is A.BORRADOR
    assert approval_after_change(A.BORRADOR) is A.BORRADOR
    with pytest.raises(CampaignConflict):
        assert_editable(A.CANCELADA, T.CANCELADA)
    with pytest.raises(CampaignConflict):
        assert_editable(A.APROBADA, T.FINALIZADA)


def test_input_validation():
    with pytest.raises(CampaignValidationError):
        validate_dates(date(2026, 10, 10), date(2026, 10, 1))
    with pytest.raises(CampaignValidationError):
        validate_dates(date(2026, 1, 1), date(2027, 6, 1))
    assert validate_promo_price(D("1290")) == D("1290.00")
    for bad in (D("0"), D("-5"), D("9.99"), D("12.345"), D("NaN")):
        with pytest.raises(CampaignValidationError):
            validate_promo_price(bad)
    assert month_bounds("2026-02", TODAY)[:2] == (date(2026, 2, 1), date(2026, 2, 28))
    assert month_bounds(None, TODAY)[2] == "2026-10"
    with pytest.raises(CampaignValidationError):
        month_bounds("2026-13", TODAY)


# ---------------------------------------------------------------- verificación contra Bsale


def verify(timeline=T.VIGENTE, promo="1290", gross="1290", fetched=FRESH, missing=False):
    bsale = None if gross is None else BsalePrice(D(gross), missing, fetched)
    return price_verification(timeline=timeline, promo_gross_price=D(promo), bsale=bsale, today=TODAY)


def test_registering_a_price_is_not_evidence_it_was_applied():
    assert verify() is V.COINCIDE_BSALE
    assert verify(gross="1490") is V.NO_COINCIDE_BSALE
    assert verify(gross=None) is V.SIN_PRECIO_BSALE
    assert verify(missing=True) is V.SIN_PRECIO_BSALE
    assert verify(fetched=STALE) is V.PRECIO_BSALE_DESACTUALIZADO
    assert verify(timeline=T.PROGRAMADA, gross="1490") is V.NO_COINCIDE_BSALE


def test_finished_campaign_still_at_promo_price_is_flagged():
    assert verify(timeline=T.FINALIZADA) is V.VENCIDA_SIN_REVERTIR
    assert verify(timeline=T.FINALIZADA, gross="1490") is V.NO_APLICA
    assert verify(timeline=T.CANCELADA) is V.NO_APLICA


# ---------------------------------------------------------------- excepciones (7 condiciones)


def exc(**kw) -> PromotionException:
    base = dict(
        company_id=3,
        variant_id=10,
        price_list_id=13,
        kind=ExceptionKind.PROMOCION,
        starts_on=date(2026, 10, 1),
        ends_on=date(2026, 10, 31),
        applicable_gross_price=D("1300"),
        approval=ApprovalStatus.APROBADA,
        campaign_id=1,
        item_id=1,
    )
    base.update(kw)
    return PromotionException(**base)


def resolve(candidates, gross="1300", fetched=FRESH, today=TODAY, **kw):
    args = dict(
        source_available=True,
        candidates=candidates,
        company_id=3,
        variant_id=10,
        price_list_id=13,
        bsale_gross_price=D(gross) if gross else None,
        today=today,
        bsale_price_fetched_at=fetched,
        max_price_age_days=3,
    )
    args.update(kw)
    return resolve_exception(**args)


def test_valid_exception_is_verified():
    res = resolve([exc()])
    assert res.check is C.EXCEPCION_VERIFICADA and res.verified_status is S.PROMOCION_VIGENTE
    assert res.to_json()["exception"]["campaign_id"] == 1


@pytest.mark.parametrize(
    "candidate",
    [exc(company_id=1), exc(variant_id=11), exc(price_list_id=12), exc(price_list_id=14)],
    ids=["empresa_equivocada", "variante_equivocada", "lista_equivocada", "lista_retirada"],
)
def test_exception_for_other_company_variant_or_list_does_not_apply(candidate):
    res = resolve([candidate])
    assert res.check is C.SIN_EXCEPCION and res.verified_status is None


def test_unapproved_promotion_is_reported_but_not_granted():
    res = resolve([exc(approval=ApprovalStatus.PENDIENTE)])
    assert res.check is C.EXCEPCION_PENDIENTE_APROBACION and res.verified_status is None and res.pending_verification


def test_future_and_expired_campaigns_do_not_apply():
    assert resolve([exc(starts_on=date(2026, 10, 20))]).check is C.SIN_EXCEPCION
    assert resolve([exc(ends_on=date(2026, 10, 12))]).check is C.SIN_EXCEPCION
    assert resolve([exc()], today=date(2026, 11, 1), fetched=datetime(2026, 11, 1, tzinfo=timezone.utc)).check is C.SIN_EXCEPCION


def test_price_must_match_and_be_verifiable_in_bsale():
    assert resolve([exc()], gross="1350").check is C.EXCEPCION_NO_VERIFICADA
    assert resolve([exc()], gross=None).check is C.PRECIO_NO_VERIFICABLE
    stale = resolve([exc()], fetched=STALE)
    assert stale.check is C.PRECIO_NO_VERIFICABLE and stale.reason == "precio_bsale_desactualizado"
    assert resolve([exc()], fetched=None).check is C.PRECIO_NO_VERIFICABLE


def test_overlapping_promotions_are_a_conflict_not_an_exception():
    two_approved = resolve([exc(), exc(campaign_id=2, item_id=2, applicable_gross_price=D("1250"))])
    assert two_approved.check is C.CONFLICTO_PROMOCIONES and two_approved.verified_status is None
    approved_and_pending = resolve([exc(), exc(campaign_id=2, item_id=2, approval=ApprovalStatus.PENDIENTE)])
    assert approved_and_pending.check is C.CONFLICTO_PROMOCIONES
    assert len(approved_and_pending.conflicts) == 1


def test_legacy_promotions_never_grant_exceptions():
    legacy = exc(origin=ExceptionOrigin.LEGACY, approval=ApprovalStatus.PENDIENTE, campaign_id=None, item_id=None)
    only_legacy = resolve([legacy])
    assert only_legacy.check is C.PROMOCION_LEGACY_NO_VERIFICABLE and only_legacy.verified_status is None
    assert resolve([exc(), legacy]).check is C.EXCEPCION_VERIFICADA
    different = exc(origin=ExceptionOrigin.LEGACY, approval=ApprovalStatus.PENDIENTE, applicable_gross_price=D("990"))
    assert resolve([exc(), different]).check is C.CONFLICTO_PROMOCIONES


# ---------------------------------------------------------------- motor: la excepción no oculta riesgos


def test_verified_exception_excludes_normal_low_margin_alert():
    r = run(price=price("1300"), promotion=resolve([exc()]))
    assert r["status"] == S.PROMOCION_VIGENTE
    assert S.PROMOCION_PENDIENTE_VERIFICACION not in r["qualifiers"]
    assert r["promotion"]["check"] == C.EXCEPCION_VERIFICADA


@pytest.mark.parametrize(
    "res",
    [
        resolve([exc()], gross="1350"),
        resolve([exc(approval=ApprovalStatus.PENDIENTE)]),
        resolve([exc(), exc(campaign_id=2, item_id=2)]),
        resolve([exc()], fetched=STALE),
    ],
    ids=["precio_distinto", "sin_aprobar", "conflicto", "precio_desactualizado"],
)
def test_failed_exception_keeps_low_margin_alert_visible(res):
    r = run(price=price("1300"), promotion=res)
    assert r["status"] == S.BAJO_POLITICA
    assert S.PROMOCION_PENDIENTE_VERIFICACION in r["qualifiers"]


def test_exception_never_hides_sale_below_cost_or_suspicious_cost():
    remate = exc(kind=ExceptionKind.REMATE, applicable_gross_price=D("900"))
    r = run(
        price=price("900"),
        promotion=resolve([remate], gross="900"),
        cost=cost(reliability=CostReliability.REQUIERE_REVISION),
    )
    assert r["status"] == S.REMATE_VIGENTE
    assert MarginRisk.VENTA_BAJO_COSTO in r["risks"]
    assert MarginRisk.COSTO_SOSPECHOSO in r["risks"]
    assert MarginRisk.COSTO_NO_VERIFICADO in r["risks"]
    assert S.COSTO_NO_VERIFICADO in r["qualifiers"]


@pytest.mark.parametrize(
    "kwargs,status,risk",
    [
        ({"price": price(None)}, S.SIN_PRECIO, MarginRisk.PRECIO_NO_VERIFICADO),
        ({"cost": cost(net=None)}, S.SIN_COSTO, MarginRisk.SIN_COSTO_VALIDO),
        ({"cost": cost(net="0")}, S.SIN_COSTO, MarginRisk.SIN_COSTO_VALIDO),
        ({"rule": None}, S.SIN_REGLA, None),
    ],
)
def test_exception_never_overrides_missing_price_cost_or_rule(kwargs, status, risk):
    r = run(promotion=resolve([exc()]), **kwargs)
    assert r["status"] == status
    if risk:
        assert risk in r["risks"]


def test_exception_only_replaces_below_policy():
    r = run(price=price("1500"), promotion=resolve([exc(applicable_gross_price=D("1500"))], gross="1500"), rule=rule("10", "40"))
    assert r["status"] == S.DENTRO_POLITICA


# ---------------------------------------------------------------- contrato campaña → excepción y legacy


def item_row(**kw):
    base = dict(
        item_id=7,
        campaign_id=3,
        company_id=3,
        campaign_name="Ofertas octubre",
        kind="oferta",
        approval_status="aprobada",
        starts_on=date(2026, 10, 1),
        ends_on=date(2026, 10, 31),
        approved_by="gerencia@test.local",
        approved_at=FRESH,
        variant_id=10,
        price_list_id=13,
        promo_gross_price=D("1300"),
        item_status="activo",
    )
    base.update(kw)
    return base


def test_only_approved_or_pending_active_items_become_candidates():
    e = campaign_item_exception(item_row())
    assert e.approval is ApprovalStatus.APROBADA and e.origin is ExceptionOrigin.V2 and e.reference == "campania:3/item:7"
    assert campaign_item_exception(item_row(approval_status="pendiente_aprobacion")).approval is ApprovalStatus.PENDIENTE
    assert campaign_item_exception(item_row(kind="remate")).kind is ExceptionKind.REMATE
    for status in ("borrador", "rechazada", "cancelada"):
        assert campaign_item_exception(item_row(approval_status=status)) is None
    assert campaign_item_exception(item_row(item_status="retirado")) is None


@pytest.mark.parametrize(
    "variants,lists,expected",
    [
        ([10], [13], "mapeable"),
        (None, [13], "variante_no_encontrada"),
        ([10, 11], [13], "variante_ambigua"),
        ([10], None, "lista_no_encontrada"),
        ([10], [12, 13], "lista_ambigua"),
        ([10], [14], "lista_fuera_de_alcance"),
    ],
)
def test_legacy_mapping_never_guesses(variants, lists, expected):
    status, *_ = legacy_mapping_status({"variant_ids": variants, "price_list_ids": lists}, SCOPE)
    assert status == expected


def test_legacy_exception_requires_active_flag_and_price():
    row = {"promotion_id": 1, "snapshot_id": 2, "tipo": "remate", "activa": True, "sale_price": D("990"),
           "fecha_inicio": date(2026, 10, 1), "fecha_fin": date(2026, 10, 31)}
    e = legacy_exception(row, 3, 10, 13)
    assert e.origin is ExceptionOrigin.LEGACY and e.kind is ExceptionKind.REMATE
    assert legacy_exception({**row, "activa": False}, 3, 10, 13) is None
    assert legacy_exception({**row, "sale_price": None}, 3, 10, 13) is None


# ---------------------------------------------------------------- medición futura


def test_measurement_windows_prepare_before_during_after_without_numbers():
    m = measurement_windows(date(2026, 10, 1), date(2026, 10, 10), None, date(2026, 10, 20))
    assert m["status"] == "no_implementado" and m["metrics"] is None
    assert m["planned_days"] == 10 and m["real_days"] == 10
    assert m["before"] == {"from": "2026-09-21", "to": "2026-09-30"}
    assert m["during"] == {"from": "2026-10-01", "to": "2026-10-10"}
    assert m["after"] == {"from": "2026-10-11", "to": "2026-10-20"}


def test_measurement_uses_real_duration_when_cancelled_early():
    m = measurement_windows(date(2026, 10, 1), date(2026, 10, 31), datetime(2026, 10, 6, 15, tzinfo=timezone.utc), date(2026, 10, 20))
    assert m["real_days"] == 5 and m["real_end_on"] == "2026-10-05"
    never_ran = measurement_windows(date(2026, 10, 10), date(2026, 10, 31), datetime(2026, 10, 5, tzinfo=timezone.utc), TODAY)
    assert never_ran["real_days"] == 0 and never_ran["during"] is None
