"""Tests del motor puro de costos V2 (sin PostgreSQL / sin float)."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from backend.services.analytics.cost_audit_models import TaxCatalogEntry
from backend.services.analytics.cost_v2_calculator import (
    CALCULATION_VERSION,
    build_tax_context_from_ids,
    calculate_cost_reception,
    source_history_fingerprint,
    tax_context_fingerprint,
)
from backend.services.analytics.cost_v2_models import (
    CostReceptionInput,
    TaxContextInput,
    TaxRateEntry,
)
from backend.services.analytics.money import D


def _row(**kwargs):
    base = dict(
        history_id=1,
        company_id=3,
        office_id=3,
        variant_id=100,
        admission_date=date(2026, 6, 1),
        stored_cost_net=D("1000"),
        stored_quantity=D("1"),
        stored_iva_amount=D("0"),
        stored_other_taxes=D("0"),
        stored_gross_cost=D("1000"),
        reception_tax_ids=(1,),
        catalog_tax_ids=(1,),
        source_history_created_at=None,
    )
    base.update(kwargs)
    return CostReceptionInput(**base)


def _ctx_iva19():
    return build_tax_context_from_ids([1])


def test_1_mankeke_669():
    row = _row(
        stored_cost_net=D("669"),
        stored_gross_cost=D("669"),
        stored_iva_amount=D("0"),
        stored_other_taxes=D("0"),
        reception_tax_ids=(1,),
    )
    res = calculate_cost_reception(row, _ctx_iva19())
    assert res.corrected_gross_cost == D("796.11")
    assert res.gross_difference_amount == D("127.11")
    assert res.tax_rate_on_net_pct == D("19.00")
    assert res.gross_understatement_vs_corrected_pct == D("15.97")
    assert res.effective_quality_status == "missing_taxes_in_gross"
    assert res.calculation_version == CALCULATION_VERSION


def test_2_neto_632():
    row = _row(stored_cost_net=D("632"), stored_gross_cost=D("632"))
    res = calculate_cost_reception(row, _ctx_iva19())
    assert res.corrected_gross_cost == D("752.08")


def test_3_vino():
    ctx = build_tax_context_from_ids([2, 1])
    row = _row(reception_tax_ids=(2, 1), catalog_tax_ids=(1, 2))
    res = calculate_cost_reception(row, ctx)
    assert res.iva_rate == D("19")
    assert res.additional_tax_rate_total == D("20.50")
    assert res.total_tax_rate == D("39.50")
    assert res.corrected_gross_cost == D("1395.00")


def test_4_cerveza():
    ctx = build_tax_context_from_ids([1, 3])
    res = calculate_cost_reception(_row(reception_tax_ids=(1, 3)), ctx)
    assert res.total_tax_rate == D("39.50")
    assert res.corrected_gross_cost == D("1395.00")


def test_5_destilado():
    ctx = build_tax_context_from_ids([8, 1])
    res = calculate_cost_reception(_row(reception_tax_ids=(8, 1)), ctx)
    assert res.total_tax_rate == D("50.50")
    assert res.corrected_gross_cost == D("1505.00")


def test_6_order_independent():
    a = calculate_cost_reception(_row(), build_tax_context_from_ids([8, 1]))
    b = calculate_cost_reception(_row(), build_tax_context_from_ids([1, 8]))
    assert a.corrected_gross_cost == b.corrected_gross_cost
    assert a.total_tax_rate == b.total_tax_rate
    assert a.iva_tax_id == b.iva_tax_id == 1
    assert tax_context_fingerprint(
        build_tax_context_from_ids([8, 1])
    ) == tax_context_fingerprint(build_tax_context_from_ids([1, 8]))


def test_7_tax_ids_4_7_with_explicit_rates():
    catalog = {
        4: TaxCatalogEntry(4, "ILA 4", D("10")),
        5: TaxCatalogEntry(5, "ILA 5", D("12")),
        1: TaxCatalogEntry(1, "IVA", D("19")),
    }
    ctx = build_tax_context_from_ids([1, 4, 5], tax_catalog=catalog)
    res = calculate_cost_reception(_row(reception_tax_ids=(1, 4, 5)), ctx)
    assert res.effective_quality_status == "missing_taxes_in_gross"
    assert res.total_tax_rate == D("41.00")
    assert res.corrected_gross_cost == D("1410.00")


def test_8_unknown_tax_incomplete():
    ctx = build_tax_context_from_ids([999])
    res = calculate_cost_reception(_row(reception_tax_ids=(999,)), ctx)
    assert res.effective_quality_status == "incomplete_tax_context"
    assert res.corrected_gross_cost is None
    assert res.tax_resolution_quality == "unresolved"


def test_9_cost_null():
    res = calculate_cost_reception(
        _row(stored_cost_net=None, stored_gross_cost=None), _ctx_iva19()
    )
    assert res.effective_quality_status == "missing_cost"
    assert res.corrected_gross_cost is None


def test_10_cost_zero():
    res = calculate_cost_reception(
        _row(stored_cost_net=D("0"), stored_gross_cost=D("0")), _ctx_iva19()
    )
    assert res.effective_quality_status == "missing_cost"


def test_11_valid_gross():
    row = _row(
        stored_cost_net=D("669"),
        stored_iva_amount=D("127.11"),
        stored_other_taxes=D("0"),
        stored_gross_cost=D("796.11"),
    )
    res = calculate_cost_reception(row, _ctx_iva19())
    assert res.effective_quality_status == "valid_gross"
    assert res.corrected_gross_cost == D("796.11")
    assert res.gross_difference_amount == D("0.00")


def test_12_iva_duplicated():
    # 10000 * 1.19 * 1.19 = 14161
    row = _row(
        stored_cost_net=D("10000"),
        stored_iva_amount=D("1900"),
        stored_other_taxes=D("2261"),
        stored_gross_cost=D("14161"),
    )
    res = calculate_cost_reception(row, _ctx_iva19())
    assert res.effective_quality_status == "duplicated_taxes_in_gross"


def test_13_component_mismatch():
    row = _row(
        stored_cost_net=D("1000"),
        stored_iva_amount=D("100"),
        stored_other_taxes=D("0"),
        stored_gross_cost=D("1500"),
    )
    res = calculate_cost_reception(row, _ctx_iva19())
    assert res.effective_quality_status == "gross_component_mismatch"


def test_14_component_rounding_warning():
    row = _row(
        stored_cost_net=D("1000"),
        stored_iva_amount=D("190"),
        stored_other_taxes=D("0"),
        stored_gross_cost=D("1190.005"),
    )
    res = calculate_cost_reception(row, _ctx_iva19())
    assert "stored_components_rounding" in res.warnings
    assert res.effective_quality_status != "gross_component_mismatch"


def test_15_outlier_warning_preserves_tax_status():
    row = _row(stored_cost_net=D("669"), stored_gross_cost=D("669"))
    res = calculate_cost_reception(
        row, _ctx_iva19(), external_warnings=("suspicious_outlier",)
    )
    assert res.effective_quality_status == "missing_taxes_in_gross"
    assert "suspicious_outlier" in res.warnings


def test_16_fingerprint_stable():
    row = _row()
    a = source_history_fingerprint(row)
    b = source_history_fingerprint(row)
    assert a == b
    assert len(a) == 64


def test_17_tax_fingerprint_order_independent():
    a = tax_context_fingerprint(build_tax_context_from_ids([8, 1]))
    b = tax_context_fingerprint(build_tax_context_from_ids([1, 8]))
    assert a == b


def test_18_rate_change_changes_tax_fingerprint():
    t1 = TaxContextInput(
        tax_ids=(1,),
        taxes=(TaxRateEntry(1, "IVA", D("19"), "iva", "bsale_taxes"),),
        context_source="bsale_taxes",
        context_as_of=None,
        context_is_historical=False,
        resolution_quality="current_catalog",
        tax_ids_source="current_product_tax",
        tax_rates_source="bsale_taxes",
    )
    t2 = TaxContextInput(
        tax_ids=(1,),
        taxes=(TaxRateEntry(1, "IVA", D("20"), "iva", "bsale_taxes"),),
        context_source="bsale_taxes",
        context_as_of=None,
        context_is_historical=False,
        resolution_quality="current_catalog",
        tax_ids_source="current_product_tax",
        tax_rates_source="bsale_taxes",
    )
    assert tax_context_fingerprint(t1) != tax_context_fingerprint(t2)


def test_19_cost_change_changes_history_fingerprint():
    a = source_history_fingerprint(_row(stored_cost_net=D("669")))
    b = source_history_fingerprint(_row(stored_cost_net=D("670")))
    assert a != b


def test_20_no_float_in_output_amounts():
    res = calculate_cost_reception(
        _row(stored_cost_net=D("669"), stored_gross_cost=D("669")), _ctx_iva19()
    )
    for v in (
        res.corrected_gross_cost,
        res.calculated_iva_amount,
        res.gross_difference_amount,
        res.tax_rate_on_net_pct,
    ):
        assert v is None or isinstance(v, Decimal)
        assert not isinstance(v, float)


def test_21_tax_ids_source_current_product_tax():
    catalog = {1: TaxCatalogEntry(1, "IVA", D("19"))}
    ctx = build_tax_context_from_ids(
        [1], tax_catalog=catalog, tax_ids_source="current_product_tax"
    )
    assert ctx.tax_ids_source == "current_product_tax"
    res = calculate_cost_reception(_row(reception_tax_ids=()), ctx)
    assert res.tax_ids_source == "current_product_tax"


def test_22_tax_rates_source_bsale_taxes():
    catalog = {1: TaxCatalogEntry(1, "IVA", D("19"))}
    ctx = build_tax_context_from_ids(
        [1], tax_catalog=catalog, tax_ids_source="current_product_tax"
    )
    assert ctx.tax_rates_source == "bsale_taxes"
    assert ctx.resolution_quality == "current_catalog"
    assert ctx.context_is_historical is False
    res = calculate_cost_reception(_row(reception_tax_ids=()), ctx)
    assert res.tax_rates_source == "bsale_taxes"
    assert res.tax_context_source == "bsale_taxes"


def test_23_canonical_fallback_separate_from_ids_source():
    ctx = build_tax_context_from_ids(
        [1], tax_catalog={}, tax_ids_source="current_product_tax"
    )
    assert ctx.tax_ids_source == "current_product_tax"
    assert ctx.tax_rates_source == "canonical_fallback"
    assert ctx.resolution_quality == "canonical_fallback"
    res = calculate_cost_reception(
        _row(stored_cost_net=D("669"), stored_gross_cost=D("669"), reception_tax_ids=()),
        ctx,
    )
    assert res.tax_ids_source == "current_product_tax"
    assert res.tax_rates_source == "canonical_fallback"
    assert res.corrected_gross_cost == D("796.11")


def test_24_fingerprint_includes_tax_sources():
    catalog = {1: TaxCatalogEntry(1, "IVA", D("19"))}
    a = build_tax_context_from_ids(
        [1], tax_catalog=catalog, tax_ids_source="current_product_tax"
    )
    b = build_tax_context_from_ids(
        [1], tax_catalog=catalog, tax_ids_source="historical_product_tax"
    )
    assert tax_context_fingerprint(a) != tax_context_fingerprint(b)
    c = TaxContextInput(
        tax_ids=a.tax_ids,
        taxes=a.taxes,
        context_source=a.context_source,
        context_as_of=a.context_as_of,
        context_is_historical=a.context_is_historical,
        resolution_quality=a.resolution_quality,
        tax_ids_source=a.tax_ids_source,
        tax_rates_source="canonical_fallback",
    )
    assert tax_context_fingerprint(a) != tax_context_fingerprint(c)


def _catalog_iva_advance() -> dict[int, TaxCatalogEntry]:
    return {
        1: TaxCatalogEntry(1, "IVA", D("19")),
        6: TaxCatalogEntry(6, "IVA HARI", D("12")),
        7: TaxCatalogEntry(7, "IVA CARN.", D("5")),
        8: TaxCatalogEntry(8, "Destilados", D("31.5")),
    }


def test_25_harina_1_6_rates():
    ctx = build_tax_context_from_ids(
        [1, 6],
        tax_catalog=_catalog_iva_advance(),
        tax_ids_source="current_product_tax",
    )
    res = calculate_cost_reception(
        _row(
            stored_cost_net=D("664"),
            stored_gross_cost=D("664"),
            reception_tax_ids=(),
            catalog_tax_ids=(1, 6),
        ),
        ctx,
    )
    assert res.iva_tax_id == 1
    assert res.iva_rate == D("19")
    assert res.calculated_iva_amount == D("126.16")
    assert res.additional_tax_rate_total == D("12.00")
    assert res.additional_tax_amount_total == D("79.68")
    assert res.total_tax_rate == D("31.00")
    assert {t.tax_id for t in res.additional_taxes} == {6}
    assert all(t.category == "iva_advance" for t in res.additional_taxes)


def test_26_harina_664():
    ctx = build_tax_context_from_ids([1, 6], tax_catalog=_catalog_iva_advance())
    res = calculate_cost_reception(
        _row(stored_cost_net=D("664"), stored_gross_cost=D("664"), reception_tax_ids=()),
        ctx,
    )
    assert res.corrected_gross_cost == D("869.84")
    assert res.effective_quality_status == "missing_taxes_in_gross"


def test_27_harina_13560():
    ctx = build_tax_context_from_ids([1, 6], tax_catalog=_catalog_iva_advance())
    res = calculate_cost_reception(
        _row(stored_cost_net=D("13560"), stored_gross_cost=D("13560"), reception_tax_ids=()),
        ctx,
    )
    assert res.corrected_gross_cost == D("17763.60")


def test_28_carne_1_7_rates():
    ctx = build_tax_context_from_ids(
        [1, 7],
        tax_catalog=_catalog_iva_advance(),
        tax_ids_source="current_product_tax",
    )
    res = calculate_cost_reception(
        _row(
            stored_cost_net=D("7770"),
            stored_gross_cost=D("7770"),
            reception_tax_ids=(),
            catalog_tax_ids=(1, 7),
        ),
        ctx,
    )
    assert res.iva_tax_id == 1
    assert res.iva_rate == D("19")
    assert res.calculated_iva_amount == D("1476.30")
    assert res.additional_tax_rate_total == D("5.00")
    assert res.additional_tax_amount_total == D("388.50")
    assert res.total_tax_rate == D("24.00")
    assert {t.tax_id for t in res.additional_taxes} == {7}


def test_29_carne_7770():
    ctx = build_tax_context_from_ids([1, 7], tax_catalog=_catalog_iva_advance())
    res = calculate_cost_reception(
        _row(stored_cost_net=D("7770"), stored_gross_cost=D("7770"), reception_tax_ids=()),
        ctx,
    )
    assert res.corrected_gross_cost == D("9634.80")


def test_30_tax_6_not_principal_iva():
    ctx = build_tax_context_from_ids([1, 6], tax_catalog=_catalog_iva_advance())
    res = calculate_cost_reception(
        _row(stored_cost_net=D("664"), stored_gross_cost=D("664"), reception_tax_ids=()),
        ctx,
    )
    assert res.iva_tax_id == 1
    assert any(t.tax_id == 6 and t.category == "iva_advance" for t in res.additional_taxes)
    assert all(t.tax_id != 6 or t.category == "iva_advance" for t in res.additional_taxes)


def test_31_tax_7_not_principal_iva():
    ctx = build_tax_context_from_ids([1, 7], tax_catalog=_catalog_iva_advance())
    res = calculate_cost_reception(
        _row(stored_cost_net=D("7770"), stored_gross_cost=D("7770"), reception_tax_ids=()),
        ctx,
    )
    assert res.iva_tax_id == 1
    assert any(t.tax_id == 7 and t.category == "iva_advance" for t in res.additional_taxes)


def test_32_tax_6_without_bsale_rate_unresolved():
    catalog = {1: TaxCatalogEntry(1, "IVA", D("19"))}  # sin 6
    ctx = build_tax_context_from_ids([1, 6], tax_catalog=catalog)
    res = calculate_cost_reception(
        _row(stored_cost_net=D("664"), stored_gross_cost=D("664"), reception_tax_ids=()),
        ctx,
    )
    assert res.effective_quality_status == "incomplete_tax_context"
    assert res.corrected_gross_cost is None
    assert ctx.resolution_quality == "unresolved"
    assert 6 not in {t.tax_id for t in ctx.taxes}


def test_33_tax_7_without_bsale_rate_unresolved():
    catalog = {1: TaxCatalogEntry(1, "IVA", D("19"))}
    ctx = build_tax_context_from_ids([1, 7], tax_catalog=catalog)
    res = calculate_cost_reception(
        _row(stored_cost_net=D("7770"), stored_gross_cost=D("7770"), reception_tax_ids=()),
        ctx,
    )
    assert res.effective_quality_status == "incomplete_tax_context"
    assert res.corrected_gross_cost is None
    assert ctx.resolution_quality == "unresolved"


def test_34_no_canonical_fallback_for_6_or_7():
    from backend.services.analytics.cost_tax_resolution import TAX_ID_FALLBACK

    assert 6 not in TAX_ID_FALLBACK
    assert 7 not in TAX_ID_FALLBACK
    ctx6 = build_tax_context_from_ids([6], tax_catalog={})
    ctx7 = build_tax_context_from_ids([7], tax_catalog={})
    assert ctx6.resolution_quality == "unresolved"
    assert ctx7.resolution_quality == "unresolved"
    assert ctx6.taxes == ()
    assert ctx7.taxes == ()


def test_35_missing_cost_preserves_resolved_tax_ids():
    ctx = build_tax_context_from_ids(
        [8, 1],
        tax_catalog=_catalog_iva_advance(),
        tax_ids_source="current_product_tax",
    )
    res = calculate_cost_reception(
        _row(
            stored_cost_net=None,
            stored_gross_cost=None,
            catalog_tax_ids=(8, 1),
            reception_tax_ids=(),
        ),
        ctx,
    )
    assert res.effective_quality_status == "missing_cost"
    assert res.resolved_tax_ids == (1, 8)
    assert res.tax_resolution_quality == "current_catalog"
    assert res.corrected_gross_cost is None


def test_36_missing_cost_preserves_tax_context_fingerprint():
    ctx = build_tax_context_from_ids(
        [8, 1],
        tax_catalog=_catalog_iva_advance(),
        tax_ids_source="current_product_tax",
    )
    fp = tax_context_fingerprint(ctx)
    res = calculate_cost_reception(
        _row(stored_cost_net=D("0"), stored_gross_cost=D("0"), reception_tax_ids=()),
        ctx,
    )
    assert res.effective_quality_status == "missing_cost"
    assert res.tax_context_fingerprint == fp
    assert res.tax_ids_source == "current_product_tax"
    assert res.tax_rates_source == "bsale_taxes"


def test_37_missing_cost_no_amounts():
    ctx = build_tax_context_from_ids([1, 6], tax_catalog=_catalog_iva_advance())
    for net in (None, D("0"), D("-10")):
        res = calculate_cost_reception(
            _row(stored_cost_net=net, stored_gross_cost=net, reception_tax_ids=()),
            ctx,
        )
        assert res.effective_quality_status == "missing_cost"
        assert res.calculated_iva_amount is None
        assert res.additional_tax_amount_total is None
        assert res.corrected_gross_cost is None
        assert res.resolved_tax_ids == (1, 6)


def test_38_order_6_1_equals_1_6():
    cat = _catalog_iva_advance()
    a = calculate_cost_reception(
        _row(stored_cost_net=D("664"), stored_gross_cost=D("664"), reception_tax_ids=()),
        build_tax_context_from_ids([6, 1], tax_catalog=cat),
    )
    b = calculate_cost_reception(
        _row(stored_cost_net=D("664"), stored_gross_cost=D("664"), reception_tax_ids=()),
        build_tax_context_from_ids([1, 6], tax_catalog=cat),
    )
    assert a.corrected_gross_cost == b.corrected_gross_cost == D("869.84")
    assert a.total_tax_rate == b.total_tax_rate
    assert a.iva_tax_id == b.iva_tax_id == 1
    assert tax_context_fingerprint(
        build_tax_context_from_ids([6, 1], tax_catalog=cat)
    ) == tax_context_fingerprint(build_tax_context_from_ids([1, 6], tax_catalog=cat))


def test_39_order_7_1_equals_1_7():
    cat = _catalog_iva_advance()
    a = calculate_cost_reception(
        _row(stored_cost_net=D("7770"), stored_gross_cost=D("7770"), reception_tax_ids=()),
        build_tax_context_from_ids([7, 1], tax_catalog=cat),
    )
    b = calculate_cost_reception(
        _row(stored_cost_net=D("7770"), stored_gross_cost=D("7770"), reception_tax_ids=()),
        build_tax_context_from_ids([1, 7], tax_catalog=cat),
    )
    assert a.corrected_gross_cost == b.corrected_gross_cost == D("9634.80")
    assert tax_context_fingerprint(
        build_tax_context_from_ids([7, 1], tax_catalog=cat)
    ) == tax_context_fingerprint(build_tax_context_from_ids([1, 7], tax_catalog=cat))


def test_40_fingerprints_stable_iva_advance():
    cat = _catalog_iva_advance()
    ctx = build_tax_context_from_ids([1, 6], tax_catalog=cat)
    assert tax_context_fingerprint(ctx) == tax_context_fingerprint(ctx)
    row = _row(stored_cost_net=D("664"), stored_gross_cost=D("664"), reception_tax_ids=())
    assert source_history_fingerprint(row) == source_history_fingerprint(row)
