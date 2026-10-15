"""Rentabilidad de packs Bsale: composición desde RAW, costo por componentes e integración V2."""

from __future__ import annotations

import re
import time
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from backend.services.analytics.validate_distribuidora_source import assert_sql_is_read_only
from backend.services.analytics_v2.contracts import (
    CostBasis,
    CostReliability,
    GrossCostStatus,
    MarginComplianceStatus as S,
    MarginRisk,
    PackComponentStatus as C,
    PackCostStatus as P,
)
from backend.services.analytics_v2.cost_control_repo import PACK_DETAILS_SQL, VARIANT_FACTS_SQL, CostControlV2Repository
from backend.services.analytics_v2.margins_service import MarginsNotFound, MarginsValidationError, data_class
from backend.services.analytics_v2.pack_engine import (
    ISSUE_ABSENT,
    ISSUE_EMPTY,
    ISSUE_INACTIVE_DETAIL,
    ISSUE_NOT_EXPANDED,
    WARN_COST_INCREASE,
    WARN_INACTIVE_COMPONENT,
    WARN_NESTED,
    ComponentFacts,
    PackCostEngine,
    parse_pack_details,
)
from backend.services.analytics_v2.packs_service import PackProblem, PacksV2Service, list_price_spread
from backend.tests.test_analytics_v2_cost_control import FakeRepo, fact, service
from backend.tests.test_analytics_v2_margins import make_service, vp
from backend.tests.test_analytics_v2_tax_engine import profile, with_catalog

D = Decimal
FETCHED = datetime(2026, 10, 13, 6, 0, tzinfo=timezone.utc)
CHECKS = Path(__file__).resolve().parents[1] / "sql" / "analytics_v2" / "checks"


def detail(variant_id, quantity=1, *, multiple=0, state=0, detail_id=None):
    """Forma de un detalle de pack Bsale (variant como recurso con href + id)."""
    return {
        "id": detail_id or variant_id,
        "quantity": quantity,
        "variant": {"href": f"https://api.bsale.io/v1/variants/{variant_id}.json", "id": str(variant_id)},
        "multipleVariant": multiple,
        "state": state,
    }


def pack_row(product_id, details, fetched=FETCHED):
    return {"product_id": product_id, "pack_details": details, "composition_fetched_at": fetched}


def pack_fact(variant_id, product_id, **kw):
    """SKU del pack: costo promedio propio cero y dos ILA propios (no deben usarse)."""
    base = dict(product_id=product_id, product_classification=3, tax_ids=[1, 5, 8], average_cost=D("0"))
    base.update(kw)
    return fact(variant_id, **base)


# Pack 5646 (Mistral 35° + Sprite + hielo): componentes con impuestos propios distintos.
HIELO = fact(9800, product_id=4481, tax_ids=[1], average_cost=D("280"))
SPRITE = fact(10963, product_id=4896, tax_ids=[1, 5], average_cost=D("1178.66"))
MISTRAL = fact(11592, product_id=5138, tax_ids=[1, 8], average_cost=D("3289"))
PACK_5646 = pack_fact(56460, 5646)
PACK_5646_DETAILS = [detail(9800), detail(10963), detail(11592)]
EXPECTED_NET = D("280") + D("1178.66") + D("3289")
EXPECTED_GROSS = D("280") * D("1.19") + D("1178.66") * D("1.29") + D("3289") * D("1.505")


def cost_service(facts, packs):
    return service(with_catalog(FakeRepo(facts, packs=packs)))


# ---------------------------------------------------------------- parser


def test_parser_accepts_bsale_shapes_and_ids_only_from_ids():
    for raw in (
        [detail(9800, 2)],
        {"items": [detail(9800, 2)]},
        '[{"variantId": 9800, "quantity": "2", "multipleVariant": 0}]',
        [{"variant_id": 9800, "quantity": 2.0}],
    ):
        comp = parse_pack_details(raw)
        assert comp.status is None and comp.issues == ()
        assert [(c.variant_id, c.quantity) for c in comp.components] == [(9800, D("2"))]


@pytest.mark.parametrize(
    "raw, status, issue",
    [
        (None, P.SIN_COMPOSICION, ISSUE_ABSENT),
        ([], P.SIN_COMPOSICION, ISSUE_EMPTY),
        ({"href": "https://api.bsale.io/v1/products/1/pack_details.json"}, P.SIN_COMPOSICION, ISSUE_NOT_EXPANDED),
        ("no-json", P.COMPOSICION_NO_INTERPRETABLE, "pack_details_no_interpretable"),
        ([{"quantity": 1}], P.COMPOSICION_NO_INTERPRETABLE, "detalle_no_interpretable"),
        ([detail(9800, 0)], P.COMPOSICION_NO_INTERPRETABLE, "cantidad_invalida"),
        ([detail(9800, "x")], P.COMPOSICION_NO_INTERPRETABLE, "cantidad_invalida"),
        ([detail(9800, state=1)], P.SIN_COMPOSICION, ISSUE_INACTIVE_DETAIL),
    ],
)
def test_parser_flags_incomplete_compositions(raw, status, issue):
    comp = parse_pack_details(raw)
    assert comp.status is status
    assert issue in comp.issues


def test_parser_open_pack_by_multiple_variant_or_product_only():
    assert parse_pack_details([detail(9800, multiple=1)]).status is P.PACK_ABIERTO
    assert parse_pack_details([{"product": {"id": 4481}, "quantity": 1}]).status is P.PACK_ABIERTO
    # productId = 0 (detalle cerrado de Bsale) no se interpreta como producto
    comp = parse_pack_details([{"variantId": 9800, "productId": 0, "quantity": 1}])
    assert comp.status is None and comp.components[0].product_id is None


# ---------------------------------------------------------------- motor


def facts_of(*rows, packs=None):
    """Índice de componentes como lo arma Control de Costos V2 (sin SQL)."""
    packs = packs or {}
    out = {}
    for r in rows:
        comp = parse_pack_details(packs[r["product_id"]]) if r.get("product_classification") == 3 else None
        out[r["variant_id"]] = ComponentFacts(
            variant_id=r["variant_id"],
            product_id=r["product_id"],
            code=r["code"],
            name=r["product_name"],
            active=r.get("variant_state", 0) == 0,
            average_net=r.get("average_cost"),
            last_comparable_net=r.get("lc_cost"),
            tax_profile=profile(r.get("tax_ids")),
            reliability=r.get("_reliability", CostReliability.SOLO_PROMEDIO_BSALE),
            cost_change_pct=r.get("_pct"),
            composition=comp,
        )
    return out


def test_three_components_with_their_own_taxes():
    idx = facts_of(HIELO, SPRITE, MISTRAL, PACK_5646, packs={5646: PACK_5646_DETAILS})
    pc = PackCostEngine(idx).evaluate(56460)
    assert pc.status is P.CALCULADO and pc.components_count == 3
    assert pc.net_total == EXPECTED_NET == D("4747.66")
    assert pc.gross_total == EXPECTED_GROSS == D("6803.6164")
    lines = {l.ref.variant_id: l for l in pc.lines}
    assert lines[10963].gross_cost.tax_ids == (1, 5) and lines[11592].gross_cost.factor == D("1.505")
    # el costo propio (0) y los impuestos propios del pack (dos ILA) no participan
    assert pc.to_json()["pack_own_cost_used"] is False and pc.to_json()["pack_own_taxes_used"] is False


def test_quantities_other_than_one_multiply_subtotals():
    can = fact(500, product_id=50, tax_ids=[1, 3], average_cost=D("700"))
    glass = fact(501, product_id=51, tax_ids=[1], average_cost=D("150"))
    pack = pack_fact(900, 90, tax_ids=[1, 3])
    idx = facts_of(can, glass, pack, packs={90: [detail(500, 6), detail(501, "2")]})
    pc = PackCostEngine(idx).evaluate(900)
    assert pc.net_total == D("700") * 6 + D("150") * 2
    assert pc.gross_total == D("700") * 6 * D("1.395") + D("150") * 2 * D("1.19")
    sub = {l.ref.variant_id: l.subtotal_net for l in pc.lines}
    assert sub == {500: D("4200"), 501: D("300")}


def test_component_without_cost_blocks_the_margin():
    no_cost = fact(9800, product_id=4481, tax_ids=[1], average_cost=D("0"))
    idx = facts_of(no_cost, SPRITE, MISTRAL, PACK_5646, packs={5646: PACK_5646_DETAILS})
    pc = PackCostEngine(idx).evaluate(56460)
    assert pc.status is P.COMPONENTE_SIN_COSTO and not pc.calculable
    assert pc.net_total is None and pc.gross_total is None
    assert pc.missing_cost_variant_ids == [9800]


def test_missing_component_and_component_with_unresolved_taxes():
    idx = facts_of(SPRITE, MISTRAL, PACK_5646, packs={5646: PACK_5646_DETAILS})
    pc = PackCostEngine(idx).evaluate(56460)
    assert pc.status is P.COMPONENTE_INEXISTENTE and pc.missing_variant_ids == [9800]

    two_ila = fact(9800, product_id=4481, tax_ids=[1, 2, 8], average_cost=D("280"))
    idx = facts_of(two_ila, SPRITE, MISTRAL, PACK_5646, packs={5646: PACK_5646_DETAILS})
    pc = PackCostEngine(idx).evaluate(56460)
    assert pc.status is P.COMPONENTE_REQUIERE_REVISION_TRIBUTARIA and pc.gross_total is None


def test_open_pack_stays_pending_even_if_other_components_are_closed():
    details = [detail(9800), detail(10963, multiple=1), detail(11592)]
    idx = facts_of(HIELO, SPRITE, MISTRAL, PACK_5646, packs={5646: details})
    pc = PackCostEngine(idx).evaluate(56460)
    assert pc.status is P.PACK_ABIERTO and pc.gross_total is None
    assert {l.ref.variant_id: l.status for l in pc.lines}[10963] is C.VARIANTE_NO_DETERMINADA


def test_nested_closed_pack_is_resolved_from_its_components():
    inner = pack_fact(700, 70)
    outer = pack_fact(800, 80)
    packs = {70: [detail(9800, 2)], 80: [detail(700, 1), detail(11592, 1)]}
    pc = PackCostEngine(facts_of(HIELO, MISTRAL, inner, outer, packs=packs)).evaluate(800)
    assert pc.status is P.CALCULADO and pc.nested and WARN_NESTED in pc.warnings
    assert pc.net_total == D("280") * 2 + D("3289")
    assert pc.gross_total == D("280") * 2 * D("1.19") + D("3289") * D("1.505")


def test_nested_pack_not_calculable_propagates():
    inner = pack_fact(700, 70)
    outer = pack_fact(800, 80)
    packs = {70: [detail(9800, multiple=1)], 80: [detail(700), detail(11592)]}
    pc = PackCostEngine(facts_of(HIELO, MISTRAL, inner, outer, packs=packs)).evaluate(800)
    assert pc.status is P.COMPONENTE_PACK_NO_CALCULABLE and pc.gross_total is None


def test_cycles_are_detected_without_infinite_recursion():
    a, b = pack_fact(700, 70), pack_fact(800, 80)
    idx = facts_of(HIELO, a, b, packs={70: [detail(800), detail(9800)], 80: [detail(700)]})
    engine = PackCostEngine(idx)
    assert engine.evaluate(700).status is P.CICLO_DETECTADO
    assert engine.evaluate(800).status is P.CICLO_DETECTADO
    self_ref = pack_fact(600, 60)
    assert PackCostEngine(facts_of(self_ref, packs={60: [detail(600)]})).evaluate(600).status is P.CICLO_DETECTADO


def test_depth_limit_marks_pending():
    chain = [pack_fact(600 + i, 60 + i) for i in range(4)]
    packs = {60 + i: [detail(600 + i + 1)] for i in range(3)}
    packs[63] = [detail(9800)]
    idx = facts_of(HIELO, *chain, packs=packs)
    assert PackCostEngine(idx, max_depth=4).evaluate(600).status is P.CALCULADO
    assert PackCostEngine(idx, max_depth=2).evaluate(600).status is P.COMPONENTE_PACK_NO_CALCULABLE


def test_cost_basis_is_explicit_for_packs():
    hielo = fact(9800, product_id=4481, tax_ids=[1], average_cost=D("280"), lc_cost=D("300"))
    idx = facts_of(hielo, pack_fact(700, 70), packs={70: [detail(9800, 2)]})
    assert PackCostEngine(idx).evaluate(700).net_total == D("560")
    assert PackCostEngine(idx, basis=CostBasis.ULTIMO_COMPARABLE).evaluate(700).net_total == D("600")
    # sin última recepción comparable no se sustituye por el promedio
    idx = facts_of(HIELO, pack_fact(700, 70), packs={70: [detail(9800)]})
    assert PackCostEngine(idx, basis=CostBasis.ULTIMO_COMPARABLE).evaluate(700).status is P.COMPONENTE_SIN_COSTO


def test_component_warnings_and_worst_reliability():
    hielo = {**HIELO, "_reliability": CostReliability.DESACTUALIZADO, "_pct": D("45"), "variant_state": 1}
    idx = facts_of(hielo, SPRITE, MISTRAL, PACK_5646, packs={5646: PACK_5646_DETAILS})
    pc = PackCostEngine(idx).evaluate(56460)
    assert pc.calculable and pc.reliability is CostReliability.DESACTUALIZADO
    assert {WARN_COST_INCREASE, WARN_INACTIVE_COMPONENT, "componente_costo_desactualizado"} <= set(pc.warnings)


def test_normal_product_is_not_evaluated_as_pack():
    idx = facts_of(HIELO)
    assert PackCostEngine(idx).evaluate(9800) is None
    assert idx[9800].composition is None


# ---------------------------------------------------------------- Control de Costos V2


def test_cost_service_uses_components_and_ignores_pack_own_cost_and_taxes():
    svc = cost_service([HIELO, SPRITE, MISTRAL, PACK_5646], [pack_row(5646, PACK_5646_DETAILS)])
    item = svc.get_product(56460)
    assert item["is_pack"] is True and item["bsale_average_cost"]["value"] == "0"
    c = item["commercial_gross_cost"]
    assert D(c["net"]) == EXPECTED_NET and D(c["gross"]) == EXPECTED_GROSS
    assert c["base"] == "componentes_promedio_bsale" and c["tax_ids"] == []
    assert c["status"] == GrossCostStatus.PRELIMINAR_NO_VERIFICADO.value
    # costo propio cero: no es "sin costo" porque los componentes tienen costo
    assert item["reliability"]["status"] != CostReliability.SIN_COSTO.value
    assert item["reliability"]["reason"] == "pack_por_componentes"
    assert [l["variant_id"] for l in item["pack"]["lines"]] == [9800, 10963, 11592]
    assert item["pack"]["composition_fetched_at"] == FETCHED.isoformat()
    # sus dos ILA propios siguen visibles en su perfil, pero no lo vuelven error tributario
    assert item["tax_profile"]["status"] == "ila_combinado"


def test_normal_product_keeps_individual_rules():
    svc = cost_service([HIELO, SPRITE, MISTRAL, PACK_5646], [pack_row(5646, PACK_5646_DETAILS)])
    item = svc.get_product(10963)
    assert item["is_pack"] is False and item["pack"] is None
    assert item["commercial_gross_cost"]["base"] == "promedio_bsale"
    assert D(item["commercial_gross_cost"]["gross"]) == D("1178.66") * D("1.29")


def test_tax_coverage_excludes_packs_but_keeps_strict_individual_validation():
    bad = fact(77, product_id=770, tax_ids=[1, 2, 8], average_cost=D("1000"))
    svc = cost_service([HIELO, SPRITE, MISTRAL, PACK_5646, bad], [pack_row(5646, PACK_5646_DETAILS)])
    cov = svc.tax_coverage()
    assert cov["skus_active"] == 4 and cov["scope"] == "productos_individuales"
    assert cov["skus_tax_review"] == 1  # solo el producto individual con dos ILA
    assert cov["packs"]["total"] == 1 and cov["packs"]["calculable"] == 1
    summary = svc.summary()
    assert summary["skus_active_by_tax_profile"].get("ila_combinado") == 1
    assert summary["packs_active"]["calculable"] == 1


def test_pending_pack_is_not_marked_as_tax_error_and_list_filters_by_kind():
    svc = cost_service([SPRITE, MISTRAL, PACK_5646], [pack_row(5646, PACK_5646_DETAILS)])
    item = svc.get_product(56460)
    assert item["commercial_gross_cost"]["status"] == GrossCostStatus.PACK_NO_CALCULABLE.value
    assert item["commercial_gross_cost"]["reason"] == P.COMPONENTE_INEXISTENTE.value
    assert item["reliability"]["status"] == CostReliability.REQUIERE_REVISION.value
    packs_only = svc.list_products(kind="pack", page_size=50)
    assert [i["variant_id"] for i in packs_only["items"]] == [56460]
    assert {i["variant_id"] for i in svc.list_products(kind="individual", page_size=50)["items"]} == {10963, 11592}


def test_pack_without_row_in_pack_details_is_pending():
    svc = cost_service([HIELO, PACK_5646], [])
    assert svc.get_product(56460)["pack"]["status"] == P.SIN_COMPOSICION.value


def test_no_pack_query_when_company_has_no_packs():
    repo = with_catalog(FakeRepo([HIELO]))
    service(repo).get_product(9800)
    assert "pack_details" not in [name for name, _ in repo.calls]


# ---------------------------------------------------------------- Márgenes V2


def margins(facts, prices, packs):
    svc, _, cost_repo = make_service(facts, prices)
    with_catalog(cost_repo)
    cost_repo._packs = packs
    return svc


PACK_FACTS = [HIELO, SPRITE, MISTRAL, PACK_5646]
PACK_PRICES = [vp(12, 56460, "9590"), vp(13, 56460, "9590"), vp(16, 56460, "9590")]


def test_margins_use_pack_cost_on_lists_12_13_16():
    svc = margins(PACK_FACTS, PACK_PRICES, [pack_row(5646, PACK_5646_DETAILS)])
    rows = {r["price_list"]["id"]: r for r in svc.product(56460)["rows"]}
    assert set(rows) == {12, 13, 16}
    row = rows[12]
    ref = row["costs"]["reference"]
    assert D(ref["gross"]) == EXPECTED_GROSS and ref["tax_status"] == "por_componente"
    expected = (D("9590") / EXPECTED_GROSS - 1) * 100
    assert D(row["markup_pct"]) == expected.quantize(D("0.01"))
    expected_margin = (D("9590") - EXPECTED_GROSS) / D("9590") * 100
    assert D(row["margin_on_sale_pct"]) == expected_margin.quantize(D("0.01"))
    assert row["status"] == S.SOBRE_POLITICA.value  # 40,95 % > máximo 30 %
    assert row["pack"]["status"] == P.CALCULADO.value and row["is_pack"] is True
    assert S.COSTO_NO_VERIFICADO.value in row["qualifiers"] and row["validity"]["status"] == "provisional"


@pytest.mark.parametrize(
    "raw",
    [
        [{"variant_id": v, "quantity": 1, "multipleVariant": 0} for v in (9800, 10963, 11592)],
        [detail(9800, 1.0), detail(10963, 1.0), detail(11592, 1.0)],
        '{"items": [{"variantId": "9800", "quantity": "1.0", "multipleVariant": "0"},'
        ' {"variantId": "10963", "quantity": "1", "multipleVariant": "0"},'
        ' {"variantId": "11592", "quantity": 1, "multipleVariant": "0"}]}',
    ],
)
def test_pack_5646_real_raw_values_end_to_end(raw):
    sprite = fact(10963, product_id=4896, tax_ids=[1, 5], average_cost=D("1178.67"))
    facts = [HIELO, sprite, MISTRAL, PACK_5646]
    item = cost_service(facts, [pack_row(5646, raw)]).get_product(56460)
    assert item["commercial_gross_cost"]["net"] == "4747.67"
    assert item["commercial_gross_cost"]["gross"] == "6803.6293"
    assert item["commercial_gross_cost"]["status"] == GrossCostStatus.PRELIMINAR_NO_VERIFICADO.value
    rows = margins(facts, PACK_PRICES, [pack_row(5646, raw)]).product(56460)["rows"]
    assert {(r["markup_pct"], r["margin_on_sale_pct"]) for r in rows} == {("40.95", "29.05")}
    assert {r["validity"]["status"] for r in rows} == {"provisional"}


def test_pack_alerts_are_not_removed_for_being_a_pack():
    prices = [vp(12, 56460, "7500"), vp(13, 56460, "765"), vp(16, 56460, "9590")]
    svc = margins(PACK_FACTS, prices, [pack_row(5646, PACK_5646_DETAILS)])
    rows = {r["price_list"]["id"]: r for r in svc.product(56460)["rows"]}
    assert rows[12]["status"] == S.BAJO_POLITICA.value
    assert rows[13]["status"] == S.BAJO_POLITICA.value
    assert MarginRisk.VENTA_BAJO_COSTO.value in rows[13]["risks"]


def test_not_calculable_pack_has_no_margin_and_is_unavailable():
    svc = margins([SPRITE, MISTRAL, PACK_5646], PACK_PRICES, [pack_row(5646, PACK_5646_DETAILS)])
    out = svc.list_rows(price_list_id=12, q="SKU56460")
    row = out["items"][0]
    assert row["status"] == S.PACK_NO_CALCULABLE.value
    assert row["markup_pct"] is None and row["margin_on_sale_pct"] is None
    assert row["target_price"]["min"] is None
    assert MarginRisk.SIN_COSTO_VALIDO.value in row["risks"]
    assert data_class(row).value == "no_disponible"


def test_last_comparable_basis_recomputes_pack_with_same_basis():
    svc = margins(PACK_FACTS, PACK_PRICES, [pack_row(5646, PACK_5646_DETAILS)])
    rows = svc.product(56460, cost_basis="ultimo_comparable")["rows"]
    # los componentes no tienen recepción comparable: no se sustituye por el promedio
    assert {r["status"] for r in rows} == {S.PACK_NO_CALCULABLE.value}
    assert rows[0]["pack"]["basis"] == "ultimo_comparable"


# ---------------------------------------------------------------- Rentabilidad de packs


def packs_service(facts, prices, packs):
    m = margins(facts, prices, packs)
    return PacksV2Service(m, m.costs)


def test_packs_rows_detail_and_list_filters():
    svc = packs_service(PACK_FACTS, PACK_PRICES, [pack_row(5646, PACK_5646_DETAILS)])
    out = svc.list_rows(price_list_id=13)
    assert out["total"] == 1
    row = out["items"][0]
    assert row["pack_state"] == "activo" and row["components_count"] == 3
    assert D(row["cost_net_total"]) == EXPECTED_NET and D(row["price_gross"]) == D("9590")
    assert row["updated_at"]["composition_fetched_at"] == FETCHED.isoformat()
    assert out["meta"]["historical_profitability"] == "no_implementada"
    det = svc.detail(56460)
    assert [l["variant_id"] for l in det["pack"]["lines"]] == [9800, 10963, 11592]
    sprite = det["pack"]["lines"][1]
    assert sprite["tax_ids"] == [1, 5] and D(sprite["subtotal_gross"]) == D("1178.66") * D("1.29")
    assert det["pack_sku_own"]["used_for_cost"] is False and det["pack_sku_own"]["tax_ids"] == [1, 5, 8]
    with pytest.raises(MarginsNotFound):
        svc.detail(9800)
    with pytest.raises(MarginsValidationError):
        svc.list_rows(problem="inventado")


def test_extreme_list_gap_is_flagged_for_review_not_classified():
    prices = [vp(12, 56460, "6490"), vp(13, 56460, "765"), vp(16, 56460, "765")]
    svc = packs_service(PACK_FACTS, prices, [pack_row(5646, PACK_5646_DETAILS)])
    det = svc.detail(56460)
    spread = det["list_price_spread"]
    assert spread["alert"] is True and spread["ratio"] == "8.48"
    assert spread["note"] == "verificar_en_bsale_raw_antes_de_clasificar_como_anomalia"
    assert PackProblem.DIFERENCIA_EXTREMA_ENTRE_LISTAS in det["problems"]
    assert PackProblem.PRECIO_BAJO_COSTO in det["problems"]
    # el precio original no se toca
    assert {r["price_gross"] for r in det["rows"]} == {"6490", "765"}


def test_list_spread_ignores_invalid_prices():
    rows = [
        {"price_list": {"id": 12}, "price": {"gross": "6490", "issue": None}},
        {"price_list": {"id": 13}, "price": {"gross": "5", "issue": "precio_simbolico"}},
    ]
    assert list_price_spread(rows) is None


def test_packs_summary_coverage_and_problems():
    other_pack = pack_fact(57100, 5710)
    facts = [*PACK_FACTS, other_pack]
    packs = [pack_row(5646, PACK_5646_DETAILS), pack_row(5710, [detail(9800), detail(99999)])]
    prices = [*PACK_PRICES, vp(12, 57100, "5000")]
    out = packs_service(facts, prices, packs).summary()
    assert out["packs_active"] == 2 and out["packs_active_calculable"] == 1 and out["packs_active_pending"] == 1
    assert out["coverage_pct"] == "50.0"
    assert out["by_pack_status"][P.COMPONENTE_INEXISTENTE.value] == 1
    assert out["packs_by_problem"][PackProblem.COMPONENTES_FALTANTES] == 1


def test_inactive_packs_are_hidden_by_default_but_queryable():
    inactive = pack_fact(57100, 5710, product_state=1)
    svc = packs_service([*PACK_FACTS, inactive], [*PACK_PRICES, vp(12, 57100, "5000")],
                        [pack_row(5646, PACK_5646_DETAILS), pack_row(5710, [detail(9800)])])
    assert {r["variant_id"] for r in svc.list_rows()["items"]} == {56460}
    assert {r["variant_id"] for r in svc.list_rows(pack_state="inactive")["items"]} == {57100}
    assert {r["variant_id"] for r in svc.list_rows(pack_state="all")["items"]} == {56460, 57100}


def test_active_pack_counts_match_between_costs_and_packs():
    # variante activa de un producto pack inactivo: no cuenta como pack activo en ningún módulo
    inactive = pack_fact(57100, 5710, product_state=1)
    facts = [*PACK_FACTS, inactive]
    packs = [pack_row(5646, PACK_5646_DETAILS), pack_row(5710, [detail(9800)])]
    costs = cost_service(facts, packs)
    packs_summary = packs_service(facts, [*PACK_PRICES, vp(12, 57100, "5000")], packs).summary()
    assert costs.summary()["packs_active"]["total"] == packs_summary["packs_active"] == 1
    assert costs.tax_coverage()["packs"]["total"] == 1


# ---------------------------------------------------------------- aislamiento y SQL


def test_company_isolation_components_only_from_same_company():
    # el componente 9800 existe solo en "otra empresa": aquí no está en los hechos de la empresa 3
    repo = with_catalog(FakeRepo([SPRITE, MISTRAL, PACK_5646], packs=[pack_row(5646, PACK_5646_DETAILS)]))
    item = service(repo).get_product(56460)
    assert item["pack"]["status"] == P.COMPONENTE_INEXISTENTE.value
    assert all(kw["company_id"] == 3 for _, kw in repo.calls)


def test_pack_sql_is_scoped_read_only_and_extracts_only_pack_details():
    assert_sql_is_read_only(PACK_DETAILS_SQL)
    assert "p.company_id = %s" in PACK_DETAILS_SQL and PACK_DETAILS_SQL.count("%s") == 1
    assert "p.classification = 3" in PACK_DETAILS_SQL
    assert "p.classification AS product_classification" in VARIANT_FACTS_SQL

    calls = []
    repo = CostControlV2Repository(lambda sql, params: calls.append((sql, params)) or [])
    repo.pack_details(company_id=3)
    assert calls == [(PACK_DETAILS_SQL, (3,))]


def test_pgadmin_pack_composition_audit_is_read_only_and_scoped():
    sql = (CHECKS / "bsale_pack_composition_audit.sql").read_text(encoding="utf-8")
    body = "\n".join(l for l in sql.splitlines() if not l.lstrip().startswith("--"))
    statements = [s.strip() for s in body.split(";") if s.strip()]
    assert len(statements) == 9
    for s in statements:
        assert_sql_is_read_only(s)
        assert "company_id = 3" in s
    lowered = body.lower()
    for forbidden in ("insert ", "update ", "delete ", "drop ", "alter ", "truncate ", "create "):
        assert forbidden not in lowered
    assert "bsale_token" not in lowered and "bsale.companies" not in lowered
    assert re.search(r"\bnote\b", lowered) is None
    # los componentes salen de ids del payload, nunca del nombre
    assert re.search(r"name\s*(~|ilike|like)", lowered) is None
    assert "classification = 3" in lowered and "pack_details" in lowered


# ---------------------------------------------------------------- Dashboard y rendimiento


def test_dashboard_packs_block():
    from backend.tests.test_analytics_v2_dashboard import scenario

    from backend.services.analytics_v2.dashboard_service import BLOCK_OK

    dash, ctx = scenario(facts=[*PACK_FACTS])
    with_catalog(ctx["cost_repo"])
    ctx["cost_repo"]._packs = [pack_row(5646, PACK_5646_DETAILS)]
    out = dash.build(cost_basis="promedio_bsale")
    assert out["packs"]["status"] == BLOCK_OK
    assert out["packs"]["data"]["packs_active"] == 1 and out["packs"]["data"]["packs_active_calculable"] == 1


def test_performance_many_packs():
    comps = [fact(100_000 + i, product_id=200_000 + i, tax_ids=[1, 5], average_cost=D("1000")) for i in range(3000)]
    packs = [pack_fact(300_000 + i, 400_000 + i) for i in range(1000)]
    details = [
        pack_row(400_000 + i, [detail(100_000 + (3 * i + k) % 3000, k + 1) for k in range(3)]) for i in range(1000)
    ]
    prices = [vp(lid, 300_000 + i, "9990") for i in range(1000) for lid in (12, 13, 16)]
    svc = packs_service([*comps, *packs], prices, details)
    start = time.perf_counter()
    out = svc.summary()
    elapsed = time.perf_counter() - start
    assert out["packs_active_calculable"] == 1000
    assert elapsed < 10, elapsed
