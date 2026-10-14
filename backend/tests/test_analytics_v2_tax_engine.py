"""Motor tributario V2: clasificación de impuestos RAW, costo bruto comercial e integración."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import pytest

from backend.services.analytics_v2 import cost_control_service as cost_service_mod
from backend.services.analytics_v2.contracts import (
    GrossCostStatus,
    MarginComplianceStatus as S,
    TaxProfileStatus as T,
    TaxValidation,
)
from backend.services.analytics_v2.cost_control_repo import TAX_CATALOG_SQL, VARIANT_FACTS_SQL
from backend.services.analytics_v2.dashboard_service import BLOCK_FORBIDDEN, BLOCK_OK
from backend.services.analytics_v2.tax_profile import (
    CLASSIFIED_BY_CODE,
    CLASSIFIED_BY_CONFIRMED_ID,
    TAX_DATA_SOURCE,
    ROLE_ILA,
    ROLE_IVA,
    ROLE_IVA_ANTICIPADO,
    ROLE_UNKNOWN,
    USE_ANTICIPATED,
    USE_COMMERCIAL,
    build_tax_catalog,
    classify_tax,
    commercial_gross_cost,
    resolve_tax_profile,
)
from backend.tests.test_analytics_v2_cost_control import FakeRepo, fact, service
from backend.tests.test_analytics_v2_margins import make_service, sku, vp

D = Decimal


def tax(tax_id: int, name: str, pct: str | None, code: str | None, *, state: int | None = 0, missing=None) -> dict[str, Any]:
    return {
        "tax_id": tax_id,
        "name": name,
        "code": code,
        "percentage": D(pct) if pct is not None else None,
        "state": state,
        "missing_since": missing,
    }


# Impuestos conocidos de La Quillotana (company 3); los códigos SII se validan con el SQL de pgAdmin.
RAW_TAXES = [
    tax(1, "IVA 19%", "19", "14"),
    tax(2, "ILA VINOS 20.5%", "20.5", "25"),
    tax(3, "ILA CERV. 20.5%", "20.5", "26"),
    tax(4, "ILA BEB. 18%", "18", "271"),
    tax(5, "ILA BEB. 10%", "10", "27"),
    tax(6, "IVA HARI, 12%", "12", "19"),
    tax(7, "IVA CARN. 5%", "5", "18"),
    tax(8, "ILA DEST. 31.5%", "31.5", "24"),
]
CATALOG = build_tax_catalog(RAW_TAXES)


def profile(ids, catalog=CATALOG, **kw):
    return resolve_tax_profile(ids, catalog, **kw)


def gross(net, ids, **kw):
    return commercial_gross_cost(D(net) if net is not None else None, profile(ids, **kw))


# ---------------------------------------------------------------- clasificación


def test_catalog_classifies_by_sii_code_and_separates_anticipated_taxes():
    roles = {tid: (e.role, e.use, e.classified_by) for tid, e in CATALOG.items()}
    assert roles[1] == (ROLE_IVA, USE_COMMERCIAL, CLASSIFIED_BY_CODE)
    assert all(roles[t][0] == ROLE_ILA and roles[t][1] == USE_COMMERCIAL for t in (2, 3, 4, 5, 8))
    assert roles[6][:2] == roles[7][:2] == (ROLE_IVA_ANTICIPADO, USE_ANTICIPATED)
    assert all(not e.issues for e in CATALOG.values())


def test_code_wins_over_misleading_name():
    entry = classify_tax(tax(9, "IVA 19%", "31.5", "24"))
    assert entry.role == ROLE_ILA and entry.classified_by == CLASSIFIED_BY_CODE


def test_unknown_sii_code_is_unclassified():
    entry = classify_tax(tax(9, "IVA 19%", "19", "15"))
    assert entry.role == ROLE_UNKNOWN and "codigo_sii_no_reconocido" in entry.issues
    assert profile([1, 9], build_tax_catalog([*RAW_TAXES, tax(9, "IVA 19%", "19", "15")])).status is T.IMPUESTO_NO_RECONOCIDO


@pytest.mark.parametrize("name", ["IVA 19%", "ILA BEB. 10%", "ILA CERVEZA 20.5%", "LICOR", "VINO"])
def test_name_never_classifies_a_tax(name):
    entry = classify_tax(tax(9, name, "19", None))
    assert entry.role == ROLE_UNKNOWN and "codigo_sii_ausente" in entry.issues
    catalog = build_tax_catalog([tax(1, "IVA 19%", "19", "14"), tax(9, name, "10", None)])
    p = profile([1, 9], catalog)
    assert p.status is T.IMPUESTO_NO_RECONOCIDO
    assert commercial_gross_cost(D("10000"), p).gross is None


def test_misleading_name_does_not_change_type_or_rate():
    # el id 5 se llama "IVA" pero su código SII es ILA analcohólicas y su tasa RAW 10
    catalog = build_tax_catalog([tax(1, "ILA", "19", "14"), tax(5, "IVA 19%", "10", "27")])
    g = commercial_gross_cost(D("10000"), profile([1, 5], catalog))
    assert g.gross == D("12900") and g.iva_rate_pct == D("19") and g.ila_rate_pct == D("10")


def test_tax_id_is_never_used_as_percentage():
    # ids 19 y 10 con tasas 19 y 18: el factor sale de la tasa, no del id
    catalog = build_tax_catalog([tax(10, "A", "19", "14"), tax(19, "B", "18", "271")])
    g = commercial_gross_cost(D("10000"), profile([10, 19], catalog))
    assert g.gross == D("13700") and g.factor == D("1.37")


def test_confirmed_role_by_id_without_sii_code():
    rows = [tax(1, "IVA 19%", "19", "14"), tax(9, "IMP. ADICIONAL", "10", None)]
    entry = build_tax_catalog(rows, {9: ROLE_ILA})[9]
    assert entry.role == ROLE_ILA and entry.classified_by == CLASSIFIED_BY_CONFIRMED_ID
    p = profile([1, 9], build_tax_catalog(rows, {9: ROLE_ILA}))
    assert p.status is T.IVA_ILA and "clasificado_por_id_confirmado" in p.issues
    assert commercial_gross_cost(D("10000"), p).gross == D("12900")


def test_confirmed_role_with_non_legal_rate_or_contradicting_code_blocks():
    odd = build_tax_catalog([tax(1, "IVA", "19", "14"), tax(9, "X", "7", None)], {9: ROLE_ILA})
    assert profile([1, 9], odd).status is T.CONFIGURACION_INCONSISTENTE
    clash = build_tax_catalog([tax(1, "IVA", "19", "14"), tax(9, "X", "10", "27")], {9: ROLE_IVA_ANTICIPADO})
    assert "codigo_sii_contradice_id_confirmado" in clash[9].issues
    assert profile([1, 9], clash).status is T.CONFIGURACION_INCONSISTENTE
    with pytest.raises(ValueError):
        build_tax_catalog([tax(9, "X", "10", None)], {9: "categoria_bebidas"})


@pytest.mark.parametrize(
    "row, issue",
    [
        (tax(1, "IVA 19%", "18", "14"), "tasa_distinta_a_codigo_sii"),
        (tax(1, "IVA 19%", None, "14"), "tasa_nula"),
        (tax(1, "IVA 19%", "19", "14", state=1), "impuesto_inactivo"),
        (tax(1, "IVA 19%", "19", "14", missing=datetime(2026, 10, 1, tzinfo=timezone.utc)), "impuesto_retirado_de_bsale"),
    ],
)
def test_inconsistent_tax_rows_block_the_gross_cost(row, issue):
    p = profile([1], build_tax_catalog([row]))
    assert p.status is T.CONFIGURACION_INCONSISTENTE and issue in p.issues
    assert p.validation is TaxValidation.REQUIERE_REVISION_TRIBUTARIA
    g = commercial_gross_cost(D("1000"), p)
    assert g.gross is None and g.status is GrossCostStatus.REQUIERE_REVISION_TRIBUTARIA


# ---------------------------------------------------------------- costo bruto comercial


@pytest.mark.parametrize("ids, expected", [([1, 5], "12900"), ([1, 4], "13700"), ([1], "11900")])
def test_requested_examples(ids, expected):
    assert gross("10000", ids).gross == D(expected)


def test_iva_only():
    g = gross("10000", [1])
    assert g.gross == D("11900") and g.factor == D("1.19")
    assert g.status is GrossCostStatus.PRELIMINAR_NO_VERIFICADO
    assert g.iva_rate_pct == D("19") and g.ila_rate_pct == D("0")


@pytest.mark.parametrize(
    "ids, net, expected, wrong",
    [
        ([1, 2], "13950", D("13950") * D("1.395"), D("13950") * D("1.19") * D("1.205")),
        ([1, 3], "13950", D("13950") * D("1.395"), D("13950") * D("1.19") * D("1.205")),
        ([1, 8], "15050", D("15050") * D("1.505"), D("15050") * D("1.19") * D("1.315")),
        ([1, 4], "1000", D("1370"), None),
        ([1, 5], "1000", D("1290"), None),
    ],
)
def test_iva_plus_one_ila_is_additive_on_the_same_net_base(ids, net, expected, wrong):
    g = gross(net, ids)
    assert g.gross == expected
    if wrong is not None:
        assert g.gross != wrong
    p = profile(ids)
    assert p.status is T.IVA_ILA and g.factor == 1 + p.total_rate_pct / 100


def test_known_examples_exact_values():
    assert gross("13950", [1, 2]).gross == D("19460.25")
    assert gross("15050", [1, 8]).gross == D("22650.25")


def test_rate_comes_from_raw_not_from_constants():
    catalog = build_tax_catalog([tax(1, "IVA", "19", "14"), tax(5, "ILA BEB.", "10", "27")])
    assert commercial_gross_cost(D("100"), profile([1, 5], catalog)).gross == D("129")
    # una tasa RAW distinta a la legal no se "corrige": se marca para revisión
    altered = build_tax_catalog([tax(1, "IVA", "20", "14")])
    assert commercial_gross_cost(D("100"), profile([1], altered)).gross is None


def test_decimals_are_not_rounded():
    g = gross("1234.567", [1, 8])
    assert g.gross == D("1234.567") * D("1.505")
    assert g.gross == D("1858.023335")
    assert isinstance(g.gross, Decimal)


def test_order_and_duplicates_of_tax_ids():
    assert gross("1000", [8, 1]).gross == gross("1000", [1, 8]).gross
    p = profile([1, 1, 8])
    assert "impuesto_repetido_en_bsale" in p.issues and p.status is T.IVA_ILA


def test_verified_exempt_gives_gross_equal_to_net():
    g = gross("1000", [], verified_exempt=True)
    assert g.gross == D("1000") and g.factor == D("1")
    assert profile([], verified_exempt=True).status is T.EXENTO_VERIFICADO


def test_exempt_mark_with_taxes_in_bsale_is_inconsistent():
    p = profile([1], verified_exempt=True)
    assert p.status is T.CONFIGURACION_INCONSISTENTE and "exento_con_impuestos" in p.issues


def test_no_taxes_is_not_assumed_exempt():
    p = profile([])
    assert p.status is T.SIN_IMPUESTOS and p.validation is TaxValidation.SIN_CONFIGURACION
    g = commercial_gross_cost(D("1000"), p)
    assert g.gross is None and g.status is GrossCostStatus.SIN_CONFIGURACION_TRIBUTARIA


def test_not_synced_taxes_have_no_gross():
    g = gross("1000", None)
    assert g.gross is None and g.status is GrossCostStatus.SIN_CONFIGURACION_TRIBUTARIA


def test_two_ila_are_incompatible():
    p = profile([1, 2, 8])
    assert p.status is T.ILA_COMBINADO and "ila_multiple" in p.issues
    assert gross("1000", [1, 2, 8]).status is GrossCostStatus.REQUIERE_REVISION_TRIBUTARIA


@pytest.mark.parametrize("ids", [[1, 6], [1, 7], [6], [7]])
def test_anticipated_vat_requires_review(ids):
    p = profile(ids)
    assert p.status is T.IVA_ANTICIPADO and p.total_rate_pct is None
    assert gross("1000", ids).gross is None


@pytest.mark.parametrize("ids, issue", [([2], "ila_sin_iva"), ([1, 9], "iva_duplicado")])
def test_ila_without_iva_and_duplicated_iva(ids, issue):
    catalog = build_tax_catalog([*RAW_TAXES, tax(9, "IVA 19% (2)", "19", "14")])
    p = profile(ids, catalog)
    assert p.status is T.CONFIGURACION_INCONSISTENTE and issue in p.issues


def test_tax_missing_from_catalog():
    p = profile([1, 99])
    assert p.status is T.IMPUESTO_NO_RECONOCIDO and "impuesto_fuera_de_catalogo" in p.issues


def test_stale_product_configuration():
    p = profile([1], config_stale=True)
    assert p.status is T.CONFIGURACION_INCONSISTENTE and p.total_rate_pct is None


@pytest.mark.parametrize("net, reason", [(None, "costo_nulo"), ("0", "costo_cero"), ("-5", "costo_negativo")])
def test_missing_or_invalid_cost(net, reason):
    g = gross(net, [1])
    assert g.gross is None and g.status is GrossCostStatus.SIN_COSTO_BASE and g.reason == reason


def test_gross_json_keeps_net_rates_and_factor():
    out = gross("1000", [1, 8]).to_json()
    assert out == {
        "net": "1000",
        "iva_rate_pct": "19",
        "ila_rate_pct": "31.5",
        "factor": "1.505",
        "gross": "1505.000",
        "status": "preliminar_no_verificado",
        "reason": None,
        "tax_ids": [1, 8],
        "taxes_applied": [
            {"tax_id": 1, "code": "14", "percentage": "19", "kind": "iva"},
            {"tax_id": 8, "code": "24", "percentage": "31.5", "kind": "ila"},
        ],
        "source": TAX_DATA_SOURCE,
    }


def test_review_keeps_tax_ids_but_applies_nothing():
    out = gross("1000", [1, 2, 8]).to_json()
    assert out["tax_ids"] == [1, 2, 8] and out["taxes_applied"] == [] and out["gross"] is None


# ---------------------------------------------------------------- SQL y aislamiento por empresa


def test_tax_sql_reads_codes_and_is_scoped_by_company():
    sql = " ".join(TAX_CATALOG_SQL.split()).lower()
    assert "from bsale_raw.taxes" in sql and "company_id = %s" in sql
    assert "code" in sql and "missing_since" in sql and "payload" not in sql
    assert "tax_config_stale" in VARIANT_FACTS_SQL


@pytest.mark.parametrize(
    "filename, n_statements",
    [("cost_gross_tax_validation.sql", 11), ("bsale_tax_ids_audit.sql", 4)],
)
def test_pgadmin_tax_scripts_are_read_only_scoped_and_never_infer_by_name(filename, n_statements):
    import re
    from pathlib import Path

    path = Path(__file__).resolve().parents[1] / "sql" / "analytics_v2" / "checks" / filename
    sql = re.sub(r"--[^\n]*", "", path.read_text(encoding="utf-8")).lower()
    for word in ("insert", "update", "delete", "truncate", "drop", "alter", "create", "grant", "copy", "merge"):
        assert re.search(rf"\b{word}\b", sql) is None, word
    assert "bsale_token" not in sql and "bsale.companies" not in sql
    assert re.search(r"\bnote\b", sql) is None
    assert "~*" not in sql and " ilike " not in sql and " like " not in sql and "similar to" not in sql
    statements = [s for s in sql.split(";") if s.strip()]
    assert len(statements) == n_statements
    assert all("company_id = 3" in s for s in statements)


def test_pgadmin_pack_audit_is_read_only_scoped_and_uses_bsale_classification():
    import re
    from pathlib import Path

    path = Path(__file__).resolve().parents[1] / "sql" / "analytics_v2" / "checks" / "bsale_pack_cost_audit.sql"
    sql = re.sub(r"--[^\n]*", "", path.read_text(encoding="utf-8")).lower()
    for word in ("insert", "update", "delete", "truncate", "drop", "alter", "create", "grant", "copy", "merge"):
        assert re.search(rf"\b{word}\b", sql) is None, word
    assert "bsale_token" not in sql and "bsale.companies" not in sql and "clients" not in sql
    assert re.search(r"p\.name\s*~", sql) is None and " ilike " not in sql
    statements = [s for s in sql.split(";") if s.strip()]
    assert len(statements) == 9
    assert all("company_id = 3" in s for s in statements)
    assert "classification = 3" in sql


def with_catalog(repo: FakeRepo, rows=RAW_TAXES) -> FakeRepo:
    repo.tax_catalog = lambda **kw: (repo._rec("tax_catalog", **kw), rows)[1]  # type: ignore[method-assign]
    return repo


def test_cost_service_uses_average_cost_without_overwriting_it():
    repo = with_catalog(FakeRepo([fact(1, tax_ids=[1, 8], average_cost=D("15050"))]))
    item = service(repo).get_product(1)
    c = item["commercial_gross_cost"]
    assert c["net"] == "15050" and c["gross"] == "22650.250" and c["base"] == "promedio_bsale"
    assert c["iva_rate_pct"] == "19" and c["ila_rate_pct"] == "31.5" and c["factor"] == "1.505"
    assert item["tax_profile"]["status"] == "iva_ila"
    assert all(kw["company_id"] == 3 for _, kw in repo.calls)


def test_same_category_different_ila_comes_only_from_product_taxes():
    # mismo tipo de producto (BEBIDAS) y nombres engañosos: solo cuentan los ids de product_taxes
    facts = [
        fact(1, tax_ids=[1, 5], average_cost=D("10000"), product_type_name="BEBIDAS", product_name="Bebida ILA 18"),
        fact(2, tax_ids=[1, 4], average_cost=D("10000"), product_type_name="BEBIDAS", product_name="Bebida ILA 10"),
        fact(3, tax_ids=[1], average_cost=D("10000"), product_type_name="BEBIDAS", product_name="Cerveza"),
    ]
    svc = service(with_catalog(FakeRepo(facts)))
    out = {vid: svc.get_product(vid)["commercial_gross_cost"] for vid in (1, 2, 3)}
    assert D(out[1]["gross"]) == D("12900") and out[1]["tax_ids"] == [1, 5]
    assert D(out[2]["gross"]) == D("13700") and out[2]["tax_ids"] == [1, 4]
    assert D(out[3]["gross"]) == D("11900") and out[3]["tax_ids"] == [1]
    assert [t["tax_id"] for t in out[2]["taxes_applied"]] == [1, 4]


def test_company_catalog_ids_are_not_shared(monkeypatch):
    monkeypatch.setattr(cost_service_mod, "CONFIRMED_TAX_ROLES", {1: {9: ROLE_ILA}})
    rows = [tax(1, "IVA 19%", "19", "14"), tax(9, "IMP. ADICIONAL", "10", None)]
    repo = with_catalog(FakeRepo([fact(1, tax_ids=[1, 9], average_cost=D("1000"))]), rows)
    assert service(repo).get_product(1)["tax_profile"]["status"] == "impuesto_no_reconocido"
    monkeypatch.setattr(cost_service_mod, "CONFIRMED_TAX_ROLES", {3: {9: ROLE_ILA}})
    repo = with_catalog(FakeRepo([fact(1, tax_ids=[1, 9], average_cost=D("1000"))]), rows)
    assert D(service(repo).get_product(1)["commercial_gross_cost"]["gross"]) == D("1290")


def test_verified_exempt_list_is_isolated_by_company(monkeypatch):
    monkeypatch.setattr(cost_service_mod, "VERIFIED_EXEMPT_PRODUCT_IDS", {1: frozenset({10})})
    repo = with_catalog(FakeRepo([fact(1, tax_ids=[], average_cost=D("1000"))]))
    item = service(repo).get_product(1)
    assert item["tax_profile"]["status"] == "sin_impuestos"
    assert item["commercial_gross_cost"]["gross"] is None

    monkeypatch.setattr(cost_service_mod, "VERIFIED_EXEMPT_PRODUCT_IDS", {3: frozenset({10})})
    item = service(with_catalog(FakeRepo([fact(1, tax_ids=[], average_cost=D("1000"))]))).get_product(1)
    assert item["tax_profile"]["status"] == "exento_verificado"
    assert item["commercial_gross_cost"]["gross"] == "1000"


def test_stale_product_tax_row_from_raw():
    repo = with_catalog(FakeRepo([fact(1, tax_ids=[1], average_cost=D("1000"), tax_config_stale=True)]))
    item = service(repo).get_product(1)
    assert item["tax_profile"]["status"] == "configuracion_inconsistente"


def coverage_facts():
    return [
        fact(1, tax_ids=[1], average_cost=D("1000")),
        fact(2, tax_ids=[1, 8], average_cost=D("1000")),
        fact(3, tax_ids=[1, 2, 8], average_cost=D("1000")),
        fact(4, tax_ids=[1, 6], average_cost=D("1000")),
        fact(5, tax_ids=[], average_cost=D("1000")),
        fact(6, tax_ids=[1], average_cost=None),
        fact(7, tax_ids=[1], average_cost=D("1000"), variant_state=1),
    ]


def test_tax_coverage_counts_and_anomalies():
    out = service(with_catalog(FakeRepo(coverage_facts()))).tax_coverage()
    assert out["skus_active"] == 6 and out["products_active"] == 6
    assert out["skus_taxes_identified"] == 5
    assert out["skus_gross_calculable"] == 2
    assert out["skus_without_cost"] == 1
    assert out["skus_tax_review"] == 2
    assert out["skus_without_tax_config"] == 1
    assert out["coverage_pct"] == {"taxes_identified": "83.3", "gross_calculable": "33.3"}
    assert out["by_tax_status"]["ila_combinado"] == 1 and out["by_tax_status"]["iva_anticipado"] == 1
    codes = {a["code"]: a for a in out["anomalies"]}
    assert set(codes) >= {"ila_multiple", "iva_anticipado", "sin_impuestos_configurados"}
    assert codes["ila_multiple"]["examples"][0]["variant_id"] == 3
    combos = {tuple(c["tax_ids"]): c for c in out["combinations"]}
    assert combos[(1,)]["skus"] == 2 and combos[(1, 8)]["total_rate_pct"] == "50.5"
    usage = {c["tax_id"]: c["skus_using"] for c in out["catalog"]}
    assert usage[1] == 5 and usage[6] == 1
    assert out["tax_config_verified"] is False


# ---------------------------------------------------------------- Márgenes V2


def margins_with_taxes(facts, prices):
    svc, repo, cost_repo = make_service(facts, prices)
    with_catalog(cost_repo)
    return svc


def test_margins_use_commercial_gross_from_average_cost():
    svc = margins_with_taxes([sku(10, tax_ids=[1, 8])], [vp(12, 10, "1700"), vp(13, 10, "1500")])
    rows = {r["price_list"]["id"]: r for r in svc.product(10)["rows"]}
    row = rows[12]
    ref = row["costs"]["reference"]
    assert ref["basis"] == "promedio_bsale" and ref["net"] == "900"
    assert D(ref["gross"]) == D("1354.5")
    assert ref["tax_factor"] == "1.505" and ref["ila_rate_pct"] == "31.5"
    # recargo = (precio bruto / costo bruto - 1) × 100
    expected = (D("1700") / D("1354.5") - 1) * 100
    assert D(row["markup_pct"]) == expected.quantize(D("0.01"))
    assert row["status"] == S.DENTRO_POLITICA
    # con 1,19 × 1,315 el costo sería 1408,33 y el recargo de la lista 13 cambiaría de tramo
    assert rows[13]["status"] == S.DENTRO_POLITICA
    assert S.COSTO_NO_VERIFICADO in row["qualifiers"] and row["validity"]["status"] == "provisional"


def test_margins_with_inconsistent_taxes_give_no_recommendation():
    svc = margins_with_taxes([sku(10, tax_ids=[1, 2, 8]), sku(11, tax_ids=[1, 6])], [vp(12, 10, "1500"), vp(12, 11, "1500")])
    rows = {r["variant_id"]: r for r in svc.list_rows(price_list_id=12)["items"]}
    for vid in (10, 11):
        assert rows[vid]["status"] == S.REQUIERE_REVISION_TRIBUTARIA
        assert rows[vid]["markup_pct"] is None and rows[vid]["costs"]["reference"]["gross"] is None
        assert rows[vid]["costs"]["reference"]["net"] == "900"
    full = next(r for r in svc.product(10)["rows"] if r["price_list"]["id"] == 12)
    assert full["target_price"]["min"] is None and full["target_price"]["max"] is None


def test_margins_overview_counts_tax_review():
    svc = margins_with_taxes([sku(10, tax_ids=[1, 2, 8]), sku(11)], [vp(12, 10, "1500"), vp(12, 11, "1500")])
    out = svc.overview()
    lst = next(x for x in out["by_price_list"] if x["price_list_id"] == 12)
    assert lst["tax_review"] == 1
    assert out["meta"]["tax_config_verified"] is False


# ---------------------------------------------------------------- Dashboard V2


def test_dashboard_exposes_tax_block_and_warnings():
    from backend.tests.test_analytics_v2_dashboard import scenario

    facts = [sku(10), sku(11, tax_ids=[1, 2, 8]), sku(12, tax_ids=[1, 6])]
    dash, ctx = scenario(facts=facts)
    with_catalog(ctx["cost_repo"])
    out = dash.build(cost_basis="promedio_bsale")
    assert out["taxes"]["status"] == BLOCK_OK
    data = out["taxes"]["data"]
    assert data["skus_active"] == 3 and data["skus_gross_calculable"] == 1 and data["skus_tax_review"] == 2
    warnings = set(out["reliability"]["warnings"])
    assert {"configuracion_tributaria_no_contrastada", "productos_requieren_revision_tributaria"} <= warnings
    assert out["reliability"]["tax_config_verified"] is False


def test_dashboard_tax_block_requires_margins_or_costs_permission():
    from backend.tests.test_analytics_v2_dashboard import scenario
    from backend.tests.test_analytics_v2_promotions_service import COMERCIAL, FINANZAS

    dash, _ = scenario(user=FINANZAS)
    assert dash.build(cost_basis="promedio_bsale")["taxes"]["status"] == BLOCK_OK
    dash, _ = scenario(user=COMERCIAL)
    assert dash.build(cost_basis="promedio_bsale")["taxes"]["status"] == BLOCK_FORBIDDEN
