"""Catálogo Maestro de Inteligencia Comercial (solo lectura, BSALE RAW).

Una fila por variante de la empresa indicada, con:
- costo promedio neto Bsale original (``variant_costs.average_cost``, nunca recalculado
  ni sustituido por costos de recepción);
- precio bruto real sincronizado por lista (``variant_prices.variant_value_with_taxes``);
  si la variante no tiene precio en una lista, el valor es nulo: jamás se infiere de
  otra lista ni se exporta como cero;
- impuestos reales del producto resueltos con el motor tributario V2
  (``tax_profile``): nunca se asume IVA 19 % ni exención por ausencia de datos;
- stock físico total actual (``bsale_raw.stocks.quantity`` sumado por variante
  en todas las sucursales de la empresa; sin filas sincronizadas = no
  disponible, nunca cero inventado; no es stock disponible ni reservado).

Diseñado multiempresa: todo se acota por ``company_id`` y las listas de precios se
resuelven desde la propia empresa (no se asumen las listas 12/13/16 fuera de la
empresa 3). Las variantes retiradas de Bsale (``missing_since``) no forman parte
del catálogo vigente; el estado activo/inactivo proviene de ``variants.state``.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Callable, Iterator

from backend.db import get_connection
from backend.services.analytics.validate_distribuidora_source import (
    make_psycopg_executor,
    open_readonly_connection,
)
from backend.services.analytics_v2.access import CompanyScope, resolve_company_scope
from backend.services.analytics_v2.stock_scope import (
    DEFAULT_STOCK_FILTER,
    STOCK_FILTERS,
    stock_filter_sql_clause,
    stock_join_subquery,
)
from backend.services.analytics_v2.tax_profile import build_tax_catalog, resolve_tax_profile

Executor = Callable[[str, tuple[Any, ...]], list[dict[str, Any]]]

CATALOG_SORTS = frozenset({"product", "code", "type_product", "cost_desc", "cost_asc"})
STATE_FILTERS = frozenset({"all", "active", "inactive"})
COST_FILTERS = frozenset({"all", "with", "without"})
TAX_FILTERS = frozenset({"all", "iva", "ila", "sin_identificar", "sin_configuracion", "revision"})

_ILA_CODES = ("24", "25", "26", "27", "271")
EXPORT_BATCH = 1000


class CatalogValidationError(ValueError):
    pass


def _dec(value: Any) -> Decimal | None:
    return None if value is None else Decimal(str(value))


class CatalogService:
    def __init__(self, executor: Executor, scope: CompanyScope):
        self._ex = executor
        self._scope = scope
        self.company_id = scope.company_id

    # ------------------------------------------------------------- metadatos
    def price_lists(self) -> list[dict[str, Any]]:
        rows = self._ex(
            """
            SELECT bsale_id AS id, name, state
            FROM bsale_raw.price_lists
            WHERE company_id = %s AND missing_since IS NULL
            ORDER BY bsale_id
            """,
            (self.company_id,),
        )
        return [
            {"id": int(r["id"]), "name": r["name"], "active": int(r["state"] or 0) == 0}
            for r in rows
        ]

    def product_types(self) -> list[dict[str, Any]]:
        rows = self._ex(
            """
            SELECT bsale_id AS id, name
            FROM bsale_raw.product_types
            WHERE company_id = %s AND missing_since IS NULL
            ORDER BY name
            """,
            (self.company_id,),
        )
        return [{"id": int(r["id"]), "name": r["name"]} for r in rows]

    def freshness(self) -> dict[str, Any]:
        rows = self._ex(
            """
            SELECT resource, scope, status, last_success_at, last_error_at, rows_received
            FROM bsale_raw.sync_state
            WHERE company_id = %s
            ORDER BY resource, scope
            """,
            (self.company_id,),
        )
        out: dict[str, Any] = {}
        for r in rows:
            key = r["resource"]
            value = r["last_success_at"].isoformat() if r["last_success_at"] else None
            if key not in out or (value or "") > (out.get(key) or ""):
                out[key] = value
            # Frescura por alcance (p. ej. variant_prices de cada lista): permite
            # distinguir una lista recuperada de una que sigue en error.
            if r["resource"] == "variant_prices" and str(r["scope"]).startswith("price_list:"):
                lid = str(r["scope"]).split(":", 1)[1]
                out[f"variant_prices_lista_{lid}"] = value
        return out

    def price_list_freshness(self) -> list[dict[str, Any]]:
        # OJO psycopg2: todo % literal dentro de una consulta parametrizada debe
        # ir escapado como %%; si no, el formateo cliente (sql % params) levanta
        # TypeError antes de llegar a PostgreSQL (regresión del 500, 20-oct-2026).
        rows = self._ex(
            """
            SELECT scope, status, last_success_at, last_error_at, rows_received
            FROM bsale_raw.sync_state
            WHERE company_id = %s AND resource = 'variant_prices'
              AND scope LIKE 'price_list:%%'
            ORDER BY scope
            """,
            (self.company_id,),
        )
        out: list[dict[str, Any]] = []
        for r in rows:
            try:
                lid = int(str(r["scope"]).split(":", 1)[1])
            except (ValueError, IndexError):
                continue
            out.append(
                {
                    "price_list_id": lid,
                    "status": r["status"],
                    "last_success_at": (r["last_success_at"].isoformat() if r["last_success_at"] else None),
                    "last_error_at": (r["last_error_at"].isoformat() if r["last_error_at"] else None),
                    "rows_received": (int(r["rows_received"]) if r["rows_received"] is not None else None),
                }
            )
        return out

    def meta(self) -> dict[str, Any]:
        total = self._ex(
            "SELECT count(*) AS n FROM bsale_raw.variants WHERE company_id = %s AND missing_since IS NULL",
            (self.company_id,),
        )[0]["n"]
        return {
            "company_id": self.company_id,
            "company_name": self._scope.company_name,
            "total_variants": int(total),
            "price_lists": self.price_lists(),
            "product_types": self.product_types(),
            "freshness": self.freshness(),
            "price_list_freshness": self.price_list_freshness(),
            "source": "bsale_raw (solo lectura)",
        }

    # --------------------------------------------------------------- consulta
    def _validated_list_ids(self, requested: list[int] | None) -> list[int]:
        available = [pl["id"] for pl in self.price_lists()]
        if not requested:
            return available
        invalid = [i for i in requested if i not in available]
        if invalid:
            raise CatalogValidationError(f"Listas no vigentes en la empresa: {invalid}")
        return list(dict.fromkeys(requested))

    def _where(self, f: dict[str, Any]) -> tuple[str, list[Any]]:
        clauses = ["v.company_id = %s", "v.missing_since IS NULL"]
        params: list[Any] = [self.company_id]
        q = (f.get("q") or "").strip()
        if q:
            clauses.append("(p.name ILIKE %s OR v.description ILIKE %s OR v.code ILIKE %s OR v.bar_code ILIKE %s)")
            like = f"%{q}%"
            params += [like, like, like, like]
        if f.get("product_type_id") is not None:
            clauses.append("p.product_type_id = %s")
            params.append(int(f["product_type_id"]))
        state = f.get("state", "all")
        if state == "active":
            clauses.append("v.state = 0")
        elif state == "inactive":
            clauses.append("v.state <> 0")
        cost = f.get("cost", "all")
        if cost == "with":
            clauses.append("vc.average_cost > 0")
        elif cost == "without":
            clauses.append("(vc.average_cost IS NULL OR vc.average_cost <= 0)")
        # Existencias globales (SUM quantity). Default operativo: CON STOCK.
        # SIN STOCK incluye sin fila (NULL), cero y negativo.
        stock = f.get("stock", DEFAULT_STOCK_FILTER)
        clause = stock_filter_sql_clause("st", stock_filter=stock)
        if clause:
            clauses.append(clause)
        # Precio presente/ausente en una lista concreta.
        if f.get("price_list_id") is not None and f.get("price_presence") in ("with", "without"):
            alias = "vp_f"
            join = (
                f"LEFT JOIN bsale_raw.variant_prices {alias} ON {alias}.company_id = v.company_id "
                f"AND {alias}.variant_id = v.bsale_id AND {alias}.price_list_id = %s "
                f"AND {alias}.missing_since IS NULL"
            )
            f["_price_join"] = join
            params.append(int(f["price_list_id"]))
            if f["price_presence"] == "with":
                clauses.append(f"{alias}.variant_value_with_taxes IS NOT NULL")
            else:
                clauses.append(f"{alias}.variant_value_with_taxes IS NULL")
        tax = f.get("tax", "all")
        if tax == "iva":
            clauses.append(
                "EXISTS (SELECT 1 FROM bsale_raw.taxes t WHERE t.company_id = v.company_id "
                "AND t.code = '14' AND t.bsale_id = ANY(ptx.tax_ids))"
            )
        elif tax == "ila":
            placeholders = ", ".join(["%s"] * len(_ILA_CODES))
            clauses.append(
                "EXISTS (SELECT 1 FROM bsale_raw.taxes t WHERE t.company_id = v.company_id "
                f"AND t.code IN ({placeholders}) AND t.bsale_id = ANY(ptx.tax_ids))"
            )
            params += list(_ILA_CODES)
        elif tax == "sin_configuracion":
            clauses.append("(ptx.tax_ids IS NULL OR cardinality(ptx.tax_ids) = 0)")
        elif tax == "sin_identificar":
            clauses.append(
                "(ptx.tax_ids IS NOT NULL AND EXISTS (SELECT 1 FROM unnest(ptx.tax_ids) AS tid "
                "WHERE NOT EXISTS (SELECT 1 FROM bsale_raw.taxes t WHERE t.company_id = v.company_id "
                "AND t.bsale_id = tid AND t.code IN ('14','17','18','19','24','25','26','27','271'))))"
            )
        elif tax == "revision":
            clauses.append(
                "(ptx.tax_ids IS NULL OR cardinality(ptx.tax_ids) = 0 OR EXISTS "
                "(SELECT 1 FROM unnest(ptx.tax_ids) AS tid WHERE NOT EXISTS "
                "(SELECT 1 FROM bsale_raw.taxes t WHERE t.company_id = v.company_id AND t.bsale_id = tid "
                "AND t.code IN ('14','17','18','19','24','25','26','27','271'))))"
            )
        return " AND ".join(clauses), params

    _ORDER = {
        "product": "lower(p.name), lower(v.description), v.bsale_id",
        "type_product": "lower(pt.name), lower(p.name), v.bsale_id",
        "code": "v.code, v.bsale_id",
        "cost_desc": "vc.average_cost DESC NULLS LAST, v.bsale_id",
        "cost_asc": "vc.average_cost ASC NULLS LAST, v.bsale_id",
    }

    def _select_sql(self, list_ids: list[int], f: dict[str, Any]) -> tuple[str, list[Any]]:
        where, params = self._where(f)
        price_cols, price_joins = [], []
        for lid in list_ids:
            a = f"vp{lid}"
            price_joins.append(
                f"LEFT JOIN bsale_raw.variant_prices {a} ON {a}.company_id = v.company_id "
                f"AND {a}.variant_id = v.bsale_id AND {a}.price_list_id = {lid} AND {a}.missing_since IS NULL"
            )
            price_cols.append(f"{a}.variant_value_with_taxes AS price_{lid}")
            price_cols.append(f"{a}.variant_value AS price_net_{lid}")
            price_cols.append(f"{a}.api_fetched_at AS price_fetched_{lid}")
        extra_join = f.get("_price_join", "")
        # stock_total = operativo; stock_total_bsale = todas las oficinas.
        # Params del JOIN van ANTES del WHERE (mismo orden textual).
        stock_join, stock_params = stock_join_subquery(self.company_id, alias="st")
        sql = f"""
        SELECT
            v.bsale_id AS variant_id, v.product_id, v.code, v.bar_code,
            v.description AS variant_name, v.state AS variant_state,
            p.name AS product_name, p.state AS product_state, p.product_type_id,
            pt.name AS product_type_name,
            ptx.tax_ids, (ptx.missing_since IS NOT NULL) AS tax_config_stale,
            vc.average_cost, vc.api_fetched_at AS cost_fetched_at,
            st.stock_total, st.stock_total_bsale, st.stock_fetched_at, st.stock_offices
            {("," + ", ".join(price_cols)) if price_cols else ""}
        FROM bsale_raw.variants v
        LEFT JOIN bsale_raw.products p ON p.company_id = v.company_id AND p.bsale_id = v.product_id
        LEFT JOIN bsale_raw.product_types pt ON pt.company_id = p.company_id AND pt.bsale_id = p.product_type_id
        LEFT JOIN bsale_raw.product_taxes ptx ON ptx.company_id = p.company_id AND ptx.product_id = p.bsale_id
        LEFT JOIN bsale_raw.variant_costs vc ON vc.company_id = v.company_id AND vc.variant_id = v.bsale_id
        {stock_join}
        {extra_join}
        {" ".join(price_joins)}
        WHERE {where}
        ORDER BY {self._ORDER[f.get("sort", "product")]}
        """
        return sql, [*stock_params, *params]

    def _tax_catalog(self) -> dict[int, Any]:
        rows = self._ex(
            """
            SELECT bsale_id AS tax_id, name, code, percentage, state, missing_since
            FROM bsale_raw.taxes
            WHERE company_id = %s
            ORDER BY bsale_id
            """,
            (self.company_id,),
        )
        return build_tax_catalog(rows)

    def _public_row(self, r: dict[str, Any], list_ids: list[int], catalog: dict[int, Any]) -> dict[str, Any]:
        profile = resolve_tax_profile(
            list(r["tax_ids"]) if r.get("tax_ids") else ([] if r.get("tax_ids") is not None else None),
            catalog,
            config_stale=bool(r.get("tax_config_stale")),
        )
        cost = _dec(r.get("average_cost"))
        prices: dict[str, Any] = {}
        for lid in list_ids:
            gross = _dec(r.get(f"price_{lid}"))
            prices[str(lid)] = {
                "gross": (str(gross) if gross is not None else None),
                "fetched_at": (r[f"price_fetched_{lid}"].isoformat() if r.get(f"price_fetched_{lid}") else None),
            }
        issues: list[str] = []
        if cost is None:
            issues.append("sin_costo")
        elif cost == 0:
            issues.append("costo_cero")
        elif cost < 0:
            issues.append("costo_negativo")
        missing_prices = [lid for lid in list_ids if r.get(f"price_{lid}") is None]
        if missing_prices:
            issues.append("sin_precio_en_listas")
        if not profile.identified:
            issues.append("impuestos_por_revisar")
        return {
            "company_id": self.company_id,
            "product_id": r["product_id"],
            "variant_id": int(r["variant_id"]),
            "product_type": r.get("product_type_name"),
            "product_type_id": r.get("product_type_id"),
            "product_name": r.get("product_name"),
            "variant_name": r.get("variant_name"),
            "barcode": r.get("bar_code"),
            "code": r.get("code"),
            "active": int(r["variant_state"] or 0) == 0,
            "tax_profile": profile.to_json(),
            "iva_pct": (str(profile.iva_rate_pct) if profile.iva_rate_pct is not None else None),
            "ila_pct": (str(profile.ila_rate_pct) if profile.ila_rate_pct is not None else None),
            "taxes": [
                {
                    "tax_id": t.tax_id,
                    "name": t.name,
                    "code": t.code,
                    "kind": t.role,
                    "percentage": (str(t.percentage) if t.percentage is not None else None),
                }
                for t in profile.taxes
            ],
            "average_cost_net": (str(cost) if cost is not None else None),
            "cost_state": ("ausente" if cost is None else "cero" if cost == 0 else "negativo" if cost < 0 else "con_costo"),
            # stock_total = operativo (filtro); stock_total_bsale = todas las oficinas.
            "stock_total": (str(_dec(r["stock_total"])) if r.get("stock_total") is not None else None),
            "stock_total_bsale": (
                str(_dec(r["stock_total_bsale"])) if r.get("stock_total_bsale") is not None else None
            ),
            "stock_state": (
                "sin_sincronizar"
                if r.get("stock_total") is None
                else "cero"
                if _dec(r["stock_total"]) == 0
                else "negativo"
                if _dec(r["stock_total"]) < 0
                else "con_stock"
            ),
            "stock_offices": (int(r["stock_offices"]) if r.get("stock_offices") is not None else None),
            "stock_fetched_at": (r["stock_fetched_at"].isoformat() if r.get("stock_fetched_at") else None),
            "cost_fetched_at": (r["cost_fetched_at"].isoformat() if r.get("cost_fetched_at") else None),
            "prices": prices,
            "missing_price_lists": missing_prices,
            "integrity_issues": issues,
            "integrity_status": "completo" if not issues else "con_observaciones",
        }

    def validate_filters(self, f: dict[str, Any]) -> None:
        if f.get("state", "all") not in STATE_FILTERS:
            raise CatalogValidationError(f"state inválido: {f.get('state')}")
        if f.get("cost", "all") not in COST_FILTERS:
            raise CatalogValidationError(f"cost inválido: {f.get('cost')}")
        if f.get("stock", DEFAULT_STOCK_FILTER) not in STOCK_FILTERS:
            raise CatalogValidationError(f"stock inválido: {f.get('stock')}")
        if f.get("tax", "all") not in TAX_FILTERS:
            raise CatalogValidationError(f"tax inválido: {f.get('tax')}")
        if f.get("sort", "product") not in CATALOG_SORTS:
            raise CatalogValidationError(f"sort inválido: {f.get('sort')}")

    def list_rows(self, f: dict[str, Any], page: int = 1, page_size: int = 50) -> dict[str, Any]:
        filters = dict(f)
        filters.setdefault("stock", DEFAULT_STOCK_FILTER)
        self.validate_filters(filters)
        list_ids = self._validated_list_ids(filters.get("price_list_ids"))
        sql, params = self._select_sql(list_ids, filters)
        count_rows = self._ex(
            f"SELECT count(*) AS n FROM ({sql}) c", tuple(params)
        )
        total = int(count_rows[0]["n"])
        rows = self._ex(
            sql + " LIMIT %s OFFSET %s", tuple(params) + (page_size, (page - 1) * page_size)
        )
        catalog = self._tax_catalog()
        stock_scope = filters.get("stock", DEFAULT_STOCK_FILTER)
        return {
            "items": [self._public_row(r, list_ids, catalog) for r in rows],
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": max(1, -(-total // page_size)),
            "price_list_ids": list_ids,
            "stock_filter": stock_scope,
            "meta": {**self.meta(), "stock_filter": stock_scope, "stock_filter_default": DEFAULT_STOCK_FILTER},
        }

    def iter_rows(self, f: dict[str, Any]) -> Iterator[dict[str, Any]]:
        """Itera TODAS las filas que cumplen los filtros, por lotes (exportación)."""
        self.validate_filters(f)
        list_ids = self._validated_list_ids(f.get("price_list_ids"))
        sql, params = self._select_sql(list_ids, dict(f))
        catalog = self._tax_catalog()
        offset = 0
        while True:
            rows = self._ex(sql + " LIMIT %s OFFSET %s", tuple(params) + (EXPORT_BATCH, offset))
            if not rows:
                return
            for r in rows:
                yield self._public_row(r, list_ids, catalog)
            if len(rows) < EXPORT_BATCH:
                return
            offset += EXPORT_BATCH


def run_catalog(
    user: dict[str, Any],
    company_id: int,
    fn: Callable[[CatalogService], Any],
    *,
    get_conn: Callable[[], Any] = get_connection,
) -> Any:
    """Sesión read-only con empresa validada contra el usuario (mismo patrón V2)."""
    conn = open_readonly_connection(get_conn)
    try:
        executor = make_psycopg_executor(conn, statement_timeout_seconds=25, lock_timeout="3s")
        scope = resolve_company_scope(user, company_id, executor)
        return fn(CatalogService(executor, scope))
    finally:
        try:
            conn.rollback()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass
