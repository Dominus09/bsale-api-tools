"""Regresión del Catálogo Maestro (Inteligencia Comercial 5.0).

El ejecutor de estas pruebas EMULA el formateo cliente de psycopg2
(``sql % params``): si una consulta parametrizada contiene un ``%`` literal
sin escapar (p. ej. ``LIKE 'price_list:%'``), psycopg2 levanta
``TypeError: not enough arguments for format string`` ANTES de llegar a
PostgreSQL. Esa fue la causa raíz del HTTP 500 de ``/catalog/rows`` en
producción (20-oct-2026): ``price_list_freshness`` se ejecuta dentro de
``meta()``, que ``list_rows`` siempre invoca. Los ejecutores simulados que no
formatean la consulta no detectan esta clase de error.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from backend.services.analytics_v2.access import CompanyScope
from backend.services.analytics_v2.catalog_service import CatalogService

FETCHED = datetime(2026, 10, 20, 22, 4, tzinfo=timezone.utc)

TAXES = [
    {"tax_id": 1, "name": "IVA", "code": "14", "percentage": Decimal("19"), "state": 0, "missing_since": None},
    {"tax_id": 2, "name": "ILA Destilados", "code": "24", "percentage": Decimal("31.5"), "state": 0, "missing_since": None},
]

PRICE_LISTS = [
    {"id": 12, "name": "Supermercado", "state": 0},
    {"id": 13, "name": "Ruta/Web", "state": 0},
    {"id": 16, "name": "Melinka", "state": 0},
]


def _variant(variant_id: int, state: int, code: str, name: str, stock, offices) -> dict:
    return {
        "variant_id": variant_id,
        "product_id": 10,
        "code": code,
        "bar_code": "5010327000011",
        "variant_name": name,
        "variant_state": state,
        "product_name": "GLENFIDDICH",
        "product_state": 0,
        "product_type_id": 35,
        "product_type_name": "Whisky",
        "tax_ids": [1, 2],
        "tax_config_stale": False,
        "average_cost": Decimal("52000"),
        "cost_fetched_at": FETCHED,
        "stock_total": stock,
        "stock_fetched_at": FETCHED,
        "stock_offices": offices,
        "price_12": Decimal("89990"),
        "price_net_12": Decimal("70000"),
        "price_fetched_12": FETCHED,
        "price_13": None,
        "price_net_13": None,
        "price_fetched_13": None,
        "price_16": Decimal("91990"),
        "price_net_16": Decimal("72000"),
        "price_fetched_16": FETCHED,
    }


VARIANTS = [
    # Stock en múltiples sucursales (p. ej. 6 + 6): el total ya viene sumado
    # por el subquery agrupado; aquí se verifica el pase fiel del dato.
    _variant(20, 0, "00789", "18 ANOS 750CC", Decimal("12"), 2),
    _variant(21, 1, "00790", "15 ANOS 750CC", Decimal("0"), 6),
]


class PsycopgFormattingExecutor:
    """Ejecutor simulado que reproduce el formateo cliente de psycopg2."""

    def __init__(self) -> None:
        self.formatted_sql: list[str] = []

    def __call__(self, sql: str, params: tuple = ()) -> list[dict]:
        # psycopg2 adapta y formatea la consulta en el cliente: un % literal
        # sin escapar en el SQL rompe aquí, igual que en producción.
        formatted = sql % params if params else sql
        self.formatted_sql.append(formatted)
        if "count(*) AS n FROM (" in formatted:
            return [{"n": len(VARIANTS)}]
        if "count(*) AS n FROM bsale_raw.variants" in formatted:
            return [{"n": len(VARIANTS)}]
        if "FROM bsale_raw.price_lists" in formatted:
            return [dict(r) for r in PRICE_LISTS]
        if "FROM bsale_raw.product_types" in formatted:
            return [{"id": 35, "name": "Whisky"}]
        if "scope LIKE" in formatted:
            assert "scope LIKE 'price_list:%'" in formatted
            return [
                {
                    "scope": "price_list:13",
                    "status": "SUCCESS",
                    "last_success_at": FETCHED,
                    "last_error_at": None,
                    "rows_received": 6140,
                }
            ]
        if "FROM bsale_raw.sync_state" in formatted:
            return [
                {
                    "resource": "variant_prices",
                    "scope": "price_list:13",
                    "status": "SUCCESS",
                    "last_success_at": FETCHED,
                    "last_error_at": None,
                    "rows_received": 6140,
                }
            ]
        if "FROM bsale_raw.taxes" in formatted:
            return [dict(r) for r in TAXES]
        if "AS variant_name" in formatted:
            rows = [dict(r) for r in VARIANTS]
            if "LIMIT" in formatted:
                limit, offset = int(params[-2]), int(params[-1])
                return rows[offset : offset + limit]
            return rows
        raise AssertionError(f"Consulta no esperada: {formatted[:120]}")


def make_service() -> tuple[CatalogService, PsycopgFormattingExecutor]:
    executor = PsycopgFormattingExecutor()
    scope = CompanyScope(company_id=3, company_name="La Quillotana SpA")
    return CatalogService(executor, scope), executor


def test_price_list_freshness_survives_psycopg_formatting():
    svc, _ = make_service()
    rows = svc.price_list_freshness()
    assert rows == [
        {
            "price_list_id": 13,
            "status": "SUCCESS",
            "last_success_at": FETCHED.isoformat(),
            "last_error_at": None,
            "rows_received": 6140,
        }
    ]


def test_meta_includes_per_list_freshness():
    svc, _ = make_service()
    meta = svc.meta()
    assert meta["total_variants"] == 2
    assert [pl["id"] for pl in meta["price_lists"]] == [12, 13, 16]
    assert meta["price_list_freshness"][0]["price_list_id"] == 13
    assert meta["freshness"]["variant_prices_lista_13"] == FETCHED.isoformat()


def test_list_rows_one_row_per_variant_inactive_and_missing_price():
    svc, _ = make_service()
    res = svc.list_rows({"price_list_ids": [12, 13, 16]}, page=1, page_size=50)
    assert res["total"] == 2
    assert [i["variant_id"] for i in res["items"]] == [20, 21]
    active, inactive = res["items"]
    assert active["active"] is True and inactive["active"] is False
    # Precio bruto = variant_value_with_taxes; ausente jamás se convierte en cero.
    assert active["prices"]["12"]["gross"] == "89990"
    assert active["prices"]["13"]["gross"] is None
    assert active["prices"]["16"]["gross"] == "91990"
    # IVA + ILA resueltos sin duplicar la fila.
    assert active["iva_pct"] == "19" and active["ila_pct"] == "31.5"
    assert active["average_cost_net"] == "52000"


def test_list_rows_pagination_does_not_lose_rows():
    svc, _ = make_service()
    p1 = svc.list_rows({"price_list_ids": [12, 13, 16]}, page=1, page_size=1)
    p2 = svc.list_rows({"price_list_ids": [12, 13, 16]}, page=2, page_size=1)
    assert p1["total"] == p2["total"] == 2
    assert [i["variant_id"] for i in p1["items"]] == [20]
    assert [i["variant_id"] for i in p2["items"]] == [21]


def test_iter_rows_exports_everything():
    svc, _ = make_service()
    rows = list(svc.iter_rows({"price_list_ids": [12, 13, 16]}))
    assert [r["variant_id"] for r in rows] == [20, 21]


def test_stock_total_multisucursal_y_cero_real():
    svc, executor = make_service()
    res = svc.list_rows({"price_list_ids": [12, 13, 16]}, page=1, page_size=50)
    con_stock, en_cero = res["items"]
    # Stock acumulado de varias sucursales, numérico y sin duplicar la fila.
    assert con_stock["stock_total"] == "12"
    assert con_stock["stock_state"] == "con_stock"
    assert con_stock["stock_offices"] == 2
    assert con_stock["stock_fetched_at"] == FETCHED.isoformat()
    # Cero real sincronizado: se muestra 0, distinto de "sin sincronizar".
    assert en_cero["stock_total"] == "0"
    assert en_cero["stock_state"] == "cero"
    # La consulta suma el stock físico (quantity) por variante, por empresa,
    # sin tocar disponible ni reservado, y sobrevive al formateo psycopg2.
    main_sql = next(s for s in executor.formatted_sql if "AS variant_name" in s)
    assert "sum(quantity) AS stock_total" in main_sql
    assert "FROM bsale_raw.stocks WHERE company_id = 3 GROUP BY variant_id" in main_sql
    assert "quantity_available" not in main_sql
    assert "quantity_reserved" not in main_sql


def test_stock_sin_sincronizar_y_negativo():
    svc, _ = make_service()
    catalog = svc._tax_catalog()
    base = dict(VARIANTS[0])

    sin_stock = svc._public_row({**base, "stock_total": None, "stock_offices": None}, [12, 13, 16], catalog)
    assert sin_stock["stock_total"] is None
    assert sin_stock["stock_state"] == "sin_sincronizar"

    negativo = svc._public_row({**base, "stock_total": Decimal("-3")}, [12, 13, 16], catalog)
    assert negativo["stock_total"] == "-3"
    assert negativo["stock_state"] == "negativo"
