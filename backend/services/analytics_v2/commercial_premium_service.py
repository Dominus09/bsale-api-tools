"""Excel Premium — payload de Auditoría Comercial 8.0 (solo lectura).

Reutiliza las reglas del script ``audit_commercial_cost_census`` (bandas, laterales
comparables, prioridad administrativa) sin limitar anomalías a top_n del JSON CLI.
No escribe Bsale RAW ni dispara sincronizaciones.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

from backend.db import get_connection
from backend.scripts.audit_commercial_cost_census import (
    DIFF_EXTREME_DOWN_PCT,
    DIFF_EXTREME_UP_PCT,
    DIFF_IMPORTANT_PCT,
    DIFF_RELEVANT_PCT,
    _any_reception_evidence_lateral_sql,
    _can_select,
    _fetch_dicts,
    _fetch_one,
    _last_comparable_lateral_sql,
    _open_readonly_tx,
    _row_get,
    census_variants_stocks,
    cost_class_cross_stock,
    cost_coverage,
    orphan_stock_variants,
    score_admin_priority,
    sync_freshness,
)
from backend.services.analytics_v2.commercial_scope import (
    PRICE_LISTS_SQL,
    CommercialScopeNotConfigured,
    evaluated_price_list_names,
    resolve_price_list_scope,
)
from backend.services.analytics_v2.contracts import COMMERCIAL_PRICE_LISTS
from backend.services.analytics_v2.display_labels import office_map_from_rows, price_list_visible_name
from backend.services.analytics_v2.stock_scope import (
    DEFAULT_STOCK_FILTER,
    STOCK_CRITERION,
    STOCK_FILTER_LABELS,
    matches_stock_filter,
    normalize_stock_filter,
    stock_filter_sql_clause,
    stock_state_from_total,
)

_OFFICES_SQL = """
SELECT bsale_id AS office_id, name, state, missing_since
FROM bsale_raw.offices
WHERE company_id = %s
ORDER BY bsale_id
"""

DEFAULT_TIMEOUT_SEC = 180
SHEET_NAMES = (
    "Resumen ejecutivo",
    "Críticos y anomalías",
    "Activos costo cero",
    "Promedio vs recepciones",
    "Integridad y sincronización",
    "Revisión administrativa",
)
REVIEW_SHEET_NAMES = (
    "Por revisar",
    "Resumen",
)


def price_lists_for_company(
    company_id: int,
    raw_rows: list[dict[str, Any]] | None = None,
) -> dict[int, str]:
    """Listas activas autorizadas → nombre comercial visible (sin IDs en la etiqueta).

    Con filas RAW se excluyen inactivas, retiradas y de otras empresas.
    Sin filas (tests / fallback): solo el mapa autorizado configurado.
    """
    if raw_rows is not None:
        try:
            scope = resolve_price_list_scope(int(company_id), raw_rows)
        except CommercialScopeNotConfigured:
            return {}
        return evaluated_price_list_names(scope)
    configured = COMMERCIAL_PRICE_LISTS.get(int(company_id))
    if not configured:
        return {}
    return {
        int(pid): price_list_visible_name(commercial_label=label, price_list_id=int(pid))
        for pid, label in configured.items()
    }


def _to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _to_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return None


def enrich_review_row(row: dict[str, Any], *, cut_at: date) -> dict[str, Any]:
    """Aplica score_admin_priority (mismas reglas que el censo CLI)."""
    item = dict(row)
    diff = _to_decimal(_row_get(item, "diff_pct"))
    avg = _to_decimal(_row_get(item, "average_cost"))
    lc = _to_decimal(_row_get(item, "last_comparable_cost"))
    stock = _to_decimal(_row_get(item, "stock_total"))
    lc_date = _to_date(_row_get(item, "last_comparable_date"))
    maybe_admin = bool(
        diff is not None
        and (diff >= DIFF_EXTREME_UP_PCT or diff <= DIFF_EXTREME_DOWN_PCT)
    )
    name = str(_row_get(item, "variant_name") or "").upper()
    presentation_risk = any(
        token in name
        for token in ("BIDON RETORNABLE", "RECARGA AGUA", "10 UND", "10 UNID")
    )
    has_doc = bool(
        _row_get(item, "lc_document")
        or _row_get(item, "last_comparable_document")
        or _row_get(item, "lc_document_number")
        or _row_get(item, "last_comparable_document_number")
    )
    prio = score_admin_priority(
        diff_pct=diff,
        stock_total=stock,
        average_cost=avg,
        last_comparable_cost=lc,
        last_comparable_date=lc_date,
        cut_at=cut_at,
        has_document=has_doc,
        possible_admin_correction=maybe_admin,
        unit_presentation_verified=False,
        presentation_risk=presentation_risk,
    )
    item["admin_priority"] = prio
    # Exposición: no calculable si no hay comparable o unidad no comprobada
    # (se deja el valor orientativo del score, marcado como no valorizable).
    if diff is None or lc is None or avg is None:
        prio["monetary_exposure_potential"] = None
        prio["exposure_calculable"] = False
    else:
        prio["exposure_calculable"] = False  # unidad no verificada → no valorizar
    return item


def _company_name(cur: Any, company_id: int) -> str:
    try:
        row = _fetch_one(
            cur,
            "SELECT name FROM bsale.companies WHERE company_id = %s",
            (company_id,),
        )
        return str(row.get("name") or f"Empresa {company_id}")
    except Exception:
        return f"Empresa {company_id}"


def _fetch_positive_cost_universe(
    cur: Any,
    company_id: int,
    *,
    can_products: bool,
    can_prices: bool,
    price_list_ids: list[int],
    stock_filter: str = DEFAULT_STOCK_FILTER,
) -> list[dict[str, Any]]:
    """Universo: activas + average_cost>0 + filtro de stock global. Sin LIMIT."""
    rec_join = _last_comparable_lateral_sql()
    ev_join = _any_reception_evidence_lateral_sql()
    stock_scope = normalize_stock_filter(stock_filter)
    stock_clause = stock_filter_sql_clause("st", stock_filter=stock_scope)
    stock_where = f"AND {stock_clause}" if stock_clause else ""

    product_join = ""
    product_cols = "NULL::text AS product_name, NULL::text AS product_type_name,"
    if can_products:
        product_join = """
          LEFT JOIN bsale_raw.products p
            ON p.company_id = v.company_id AND p.bsale_id = v.product_id
          LEFT JOIN bsale_raw.product_types pt
            ON pt.company_id = p.company_id AND pt.bsale_id = p.product_type_id
        """
        product_cols = "p.name AS product_name, pt.name AS product_type_name,"

    price_joins = ""
    price_cols = ""
    for lid in price_list_ids:
        alias = f"vp{lid}"
        price_joins += f"""
          LEFT JOIN bsale_raw.variant_prices {alias}
            ON {alias}.company_id = v.company_id
           AND {alias}.variant_id = v.bsale_id
           AND {alias}.price_list_id = {int(lid)}
           AND {alias}.missing_since IS NULL
        """
        # Solo valor positivo cuenta como vigente; 0/NULL → celda vacía en Excel.
        price_cols += f"""
          CASE WHEN COALESCE({alias}.variant_value_with_taxes, 0) > 0
               THEN {alias}.variant_value_with_taxes END AS price_{lid},
        """
    if not can_prices or not price_list_ids:
        price_joins = ""
        price_cols = ""

    sql = f"""
        WITH st AS (
          SELECT variant_id, sum(quantity) AS stock_total
          FROM bsale_raw.stocks
          WHERE company_id = %s
          GROUP BY variant_id
        )
        SELECT
          v.bsale_id AS variant_id,
          v.product_id,
          v.code AS sku,
          v.bar_code AS barcode,
          v.description AS variant_name,
          {product_cols}
          vc.average_cost,
          st.stock_total,
          {price_cols}
          lc.lc_cost AS last_comparable_cost,
          lc.lc_date AS last_comparable_date,
          lc.lc_reception_id AS last_comparable_reception_id,
          lc.lc_document AS last_comparable_document,
          lc.lc_document_number AS last_comparable_document_number,
          lc.lc_office_id AS last_comparable_office_id,
          lc.lc_movement_class AS last_comparable_movement_class,
          (ev.has_noncomparable_evidence IS TRUE) AS requires_verification,
          CASE
            WHEN lc.lc_cost IS NULL OR lc.lc_cost <= 0 OR vc.average_cost <= 0 THEN NULL
            ELSE round(((lc.lc_cost - vc.average_cost) / vc.average_cost) * 100, 2)
          END AS diff_pct,
          CASE
            WHEN lc.lc_cost IS NULL OR lc.lc_cost <= 0 OR vc.average_cost <= 0 THEN NULL
            ELSE round(lc.lc_cost - vc.average_cost, 4)
          END AS diff_abs
        FROM bsale_raw.variants v
        LEFT JOIN st ON st.variant_id = v.bsale_id
        INNER JOIN bsale_raw.variant_costs vc
          ON vc.company_id = v.company_id AND vc.variant_id = v.bsale_id
         AND vc.average_cost > 0
        {product_join}
        {price_joins}
        {rec_join}
        {ev_join}
        WHERE v.company_id = %s
          AND v.missing_since IS NULL
          AND COALESCE(v.state, 0) = 0
          {stock_where}
        ORDER BY v.bsale_id
    """
    return _fetch_dicts(cur, sql, (company_id, company_id))


def _classify_universe_row(row: dict[str, Any]) -> dict[str, Any]:
    diff = _to_decimal(_row_get(row, "diff_pct"))
    requires = bool(_row_get(row, "requires_verification"))
    if diff is None:
        if requires:
            band = "requiere_verificacion"
            comparability = "evidencia_insuficiente"
        else:
            band = "sin_recepcion_comparable"
            comparability = "sin_comparable"
        extreme = False
    else:
        ap = abs(diff)
        if ap <= DIFF_RELEVANT_PCT:
            band = "hasta_15"
        elif ap <= DIFF_IMPORTANT_PCT:
            band = "entre_15_y_50"
        else:
            band = "sobre_50"
        extreme = bool(diff >= DIFF_EXTREME_UP_PCT or diff <= DIFF_EXTREME_DOWN_PCT)
        comparability = "comparable"
    out = dict(row)
    out["banda_principal"] = band
    out["extrema_indicador"] = extreme
    out["comparability_status"] = comparability
    out["evidence_sufficient"] = comparability == "comparable"
    return out


def _fetch_zero_cost_actives(
    cur: Any,
    company_id: int,
    *,
    can_products: bool,
    can_prices: bool,
    can_receptions: bool,
    price_list_ids: list[int],
) -> list[dict[str, Any]]:
    """Todas las activas con average_cost = 0 (hoja 3)."""
    rec_join = _last_comparable_lateral_sql() if can_receptions else ""
    if can_receptions:
        has_rec = "lc.lc_cost IS NOT NULL"
        rec_cols = """
          (lc.lc_cost IS NOT NULL) AS has_comparable_reception,
          lc.lc_cost AS last_comparable_cost,
          lc.lc_date AS last_comparable_date,
          lc.lc_document AS last_comparable_document,
          lc.lc_document_number AS last_comparable_document_number,
          lc.lc_office_id AS last_comparable_office_id
        """
    else:
        rec_cols = """
          FALSE AS has_comparable_reception,
          NULL::numeric AS last_comparable_cost,
          NULL::date AS last_comparable_date,
          NULL::text AS last_comparable_document,
          NULL::text AS last_comparable_document_number,
          NULL::int AS last_comparable_office_id
        """

    product_join = ""
    product_cols = "NULL::text AS product_name, NULL::text AS product_type_name,"
    if can_products:
        product_join = """
          LEFT JOIN bsale_raw.products p
            ON p.company_id = v.company_id AND p.bsale_id = v.product_id
          LEFT JOIN bsale_raw.product_types pt
            ON pt.company_id = p.company_id AND pt.bsale_id = p.product_type_id
        """
        product_cols = "p.name AS product_name, pt.name AS product_type_name,"

    price_joins = ""
    price_cols = ""
    if can_prices:
        for lid in price_list_ids:
            alias = f"vp{lid}"
            price_joins += f"""
              LEFT JOIN bsale_raw.variant_prices {alias}
                ON {alias}.company_id = v.company_id
               AND {alias}.variant_id = v.bsale_id
               AND {alias}.price_list_id = {int(lid)}
               AND {alias}.missing_since IS NULL
            """
            price_cols += f"""
              CASE WHEN COALESCE({alias}.variant_value_with_taxes, 0) > 0
                   THEN {alias}.variant_value_with_taxes END AS price_{lid},
            """

    sql = f"""
        WITH st AS (
          SELECT variant_id, sum(quantity) AS stock_total
          FROM bsale_raw.stocks
          WHERE company_id = %s
          GROUP BY variant_id
        )
        SELECT
          v.bsale_id AS variant_id,
          v.product_id,
          v.code AS sku,
          v.bar_code AS barcode,
          v.description AS variant_name,
          {product_cols}
          vc.average_cost,
          st.stock_total AS stock_total_raw,
          COALESCE(st.stock_total, 0) AS stock_total,
          (st.variant_id IS NULL) AS sin_fila_stock,
          (st.variant_id IS NOT NULL AND st.stock_total = 0) AS stock_cero_explicito,
          {price_cols}
          {rec_cols}
        FROM bsale_raw.variants v
        INNER JOIN bsale_raw.variant_costs vc
          ON vc.company_id = v.company_id AND vc.variant_id = v.bsale_id
         AND vc.average_cost = 0
        LEFT JOIN st ON st.variant_id = v.bsale_id
        {product_join}
        {price_joins}
        {rec_join}
        WHERE v.company_id = %s
          AND v.missing_since IS NULL
          AND COALESCE(v.state, 0) = 0
        ORDER BY v.bsale_id
    """
    return _fetch_dicts(cur, sql, (company_id, company_id))


def _sync_freshness_view(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """No convertir errores históricos en alerta si hubo SUCCESS posterior."""
    out: list[dict[str, Any]] = []
    for r in rows:
        item = dict(r)
        status = str(item.get("status") or "").upper()
        last_success = item.get("last_success_at")
        last_error = item.get("last_error_at")
        active_alert = False
        note = "ok"
        if status == "SUCCESS" and last_success:
            note = "último SUCCESS vigente"
            if last_error and last_success and last_error > last_success:
                # error posterior al success → alerta
                active_alert = True
                note = "error posterior al último SUCCESS"
            elif last_error and last_success and last_error < last_success:
                note = "hubo errores históricos; último SUCCESS los supersede"
        elif status not in ("SUCCESS",):
            active_alert = True
            note = f"estado actual {status or 'desconocido'}"
        item["active_freshness_alert"] = active_alert
        item["freshness_note"] = note
        out.append(item)
    return out


def build_commercial_premium_payload(
    company_id: int,
    *,
    timeout_sec: int = DEFAULT_TIMEOUT_SEC,
    connection: Any | None = None,
    stock_filter: str | None = None,
) -> dict[str, Any]:
    """
    Fotografía READ ONLY + REPEATABLE READ para Excel Premium.

    Incluye el universo completo de divergencias (>15 %), no el top_n del CLI.
    ``stock_filter`` acota el universo operativo; census/cobertura globales se
    etiquetan aparte para no mezclar denominadores.
    """
    cut = datetime.now(timezone.utc)
    cut_day = cut.date()
    stock_scope = normalize_stock_filter(stock_filter)

    own_conn = connection is None
    conn = connection or get_connection()
    payload: dict[str, Any] = {
        "ok": False,
        "company_id": int(company_id),
        "cut_at_utc": cut.isoformat(),
        "stock_filter": stock_scope,
        "stock_filter_label": STOCK_FILTER_LABELS[stock_scope],
        "stock_criterion": STOCK_CRITERION,
        "price_lists": {},
        "offices": {},
        "methodology": {
            "diff_pct": "(ultima_comparable - average_cost) / average_cost * 100",
            "exposure": "ABS(ultima_comparable - average_cost) * stock — orientativa, no pérdida",
            "average_cost_source": "bsale_raw.variant_costs.average_cost (no se recalcula)",
            "comparable": "FACTURA/GUÍA, details_complete, cost>0, quantity>0",
            "alerts_are_not_confirmed_errors": True,
            "stock_filter": stock_scope,
            "thresholds": {
                "hasta_15": float(DIFF_RELEVANT_PCT),
                "importante_sobre": float(DIFF_IMPORTANT_PCT),
                "extrema_alza": float(DIFF_EXTREME_UP_PCT),
                "extrema_baja": float(DIFF_EXTREME_DOWN_PCT),
            },
        },
        "warnings": [],
    }
    try:
        cur = _open_readonly_tx(conn, timeout_sec=timeout_sec)
        try:
            can_costs = _can_select(cur, "bsale_raw.variant_costs")
            can_prices = _can_select(cur, "bsale_raw.variant_prices")
            can_receptions = _can_select(
                cur, "bsale_raw.stock_receptions"
            ) and _can_select(cur, "bsale_raw.stock_reception_details")
            can_products = _can_select(cur, "bsale_raw.products")

            if not can_costs:
                payload["error"] = "sin SELECT a bsale_raw.variant_costs"
                payload["warnings"].append(payload["error"])
                return payload

            payload["company_name"] = _company_name(cur, company_id)
            payload["census"] = census_variants_stocks(cur, company_id)
            payload["cost_coverage"] = cost_coverage(cur, company_id)
            payload["cost_classes"] = cost_class_cross_stock(cur, company_id)
            payload["orphans"] = orphan_stock_variants(cur, company_id)
            payload["sync_state"] = _sync_freshness_view(sync_freshness(cur, company_id))
            payload["privileges"] = {
                "variant_costs": can_costs,
                "variant_prices": can_prices,
                "receptions": can_receptions,
                "products": can_products,
            }
            # Listas: solo autorizadas + vigentes en RAW de esta empresa.
            try:
                pl_rows = _fetch_dicts(cur, PRICE_LISTS_SQL, (company_id,))
            except Exception:
                pl_rows = []
            price_map = price_lists_for_company(company_id, pl_rows)
            payload["price_lists"] = price_map
            price_list_ids = sorted(price_map.keys())
            try:
                office_rows = _fetch_dicts(cur, _OFFICES_SQL, (company_id,))
            except Exception:
                office_rows = []
            payload["offices"] = office_map_from_rows(office_rows, company_id=company_id)

            if not can_receptions:
                payload["warnings"].append(
                    "Sin acceso a recepciones: divergencias y comparables incompletos."
                )
                universe: list[dict[str, Any]] = []
            else:
                universe = [
                    _classify_universe_row(r)
                    for r in _fetch_positive_cost_universe(
                        cur,
                        company_id,
                        can_products=can_products,
                        can_prices=can_prices,
                        price_list_ids=price_list_ids,
                        stock_filter=stock_scope,
                    )
                ]

            # Buckets (mismas reglas que el censo)
            buckets = {
                "universo": len(universe),
                "sin_recepcion_comparable": 0,
                "requiere_verificacion": 0,
                "hasta_15": 0,
                "entre_15_y_50": 0,
                "sobre_50": 0,
                "extrema_indicador": 0,
            }
            for r in universe:
                band = r["banda_principal"]
                if band in buckets:
                    buckets[band] += 1
                if r.get("extrema_indicador"):
                    buckets["extrema_indicador"] += 1

            primary_sum = (
                buckets["sin_recepcion_comparable"]
                + buckets["requiere_verificacion"]
                + buckets["hasta_15"]
                + buckets["entre_15_y_50"]
                + buckets["sobre_50"]
            )
            payload["divergence_buckets"] = buckets
            payload["divergence_buckets_identity"] = {
                "closes": primary_sum == buckets["universo"],
                "sum_principales": primary_sum,
                "extrema_subset_ok": buckets["extrema_indicador"] <= buckets["sobre_50"],
            }

            # Hoja 2: TODAS las divergencias >15 % (sin LIMIT 30)
            anomalies_raw = [
                r
                for r in universe
                if r.get("diff_pct") is not None
                and abs(_to_decimal(r["diff_pct"]) or Decimal("0")) > DIFF_RELEVANT_PCT
            ]
            anomalies = [enrich_review_row(r, cut_at=cut_day) for r in anomalies_raw]
            anomalies.sort(
                key=lambda r: (
                    -float((r.get("admin_priority") or {}).get("priority_score") or 0),
                    -abs(float(r.get("diff_pct") or 0)),
                    -float(r.get("stock_total") or 0),
                    int(r.get("variant_id") or 0),
                )
            )
            payload["anomalies"] = anomalies
            payload["anomalies_count"] = len(anomalies)
            payload["anomalies_source"] = (
                "universo_completo_abs_diff_gt_15 — no top_n del CLI"
            )

            # Hoja 4: universo enriquecido (prioridad solo donde hay Δ)
            avg_vs_rec = []
            for r in universe:
                if r.get("diff_pct") is not None and abs(
                    _to_decimal(r["diff_pct"]) or Decimal("0")
                ) > DIFF_RELEVANT_PCT:
                    avg_vs_rec.append(enrich_review_row(r, cut_at=cut_day))
                else:
                    avg_vs_rec.append(r)
            payload["average_vs_receptions"] = avg_vs_rec

            zero_rows_all = _fetch_zero_cost_actives(
                cur,
                company_id,
                can_products=can_products,
                can_prices=can_prices,
                can_receptions=can_receptions,
                price_list_ids=price_list_ids,
            )
            for z in zero_rows_all:
                has_price = any(
                    _to_decimal(z.get(f"price_{lid}")) is not None
                    and (_to_decimal(z.get(f"price_{lid}")) or Decimal("0")) > 0
                    for lid in price_list_ids
                )
                z["has_positive_price"] = has_price
                if z.get("sin_fila_stock"):
                    z["stock_status"] = "sin_fila_stock"
                elif z.get("stock_cero_explicito"):
                    z["stock_status"] = "stock_cero_explicito"
                elif (_to_decimal(z.get("stock_total_raw")) or Decimal("0")) > 0:
                    z["stock_status"] = "stock_positivo"
                else:
                    z["stock_status"] = "indeterminado"
                if z.get("has_comparable_reception"):
                    z["reception_status"] = "con_recepcion_comparable"
                    z["recommended_action"] = "revisar_por_que_promedio_sigue_en_cero"
                else:
                    z["reception_status"] = "sin_recepcion_comparable"
                    z["recommended_action"] = (
                        "verificar_alta_catalogo_y_primera_compra"
                        if has_price
                        else "sin_precio_ni_recepcion_revisar_vigencia_comercial"
                    )
                z["commercial_status"] = (
                    "activo_costo_cero_alerta"
                    if z["stock_status"] == "stock_positivo"
                    else "activo_costo_cero_sin_stock_pos"
                )

            def _zero_in_scope(z: dict[str, Any]) -> bool:
                if z.get("sin_fila_stock"):
                    state = "sin_sincronizar"
                else:
                    state = stock_state_from_total(_to_decimal(z.get("stock_total_raw")))
                return matches_stock_filter(state=state, stock_filter=stock_scope)

            zero_rows = [z for z in zero_rows_all if _zero_in_scope(z)]
            payload["zero_cost_actives"] = zero_rows
            payload["zero_cost_summary_global"] = {
                "total": len(zero_rows_all),
                "scope": "global_sin_filtro_stock",
                "stock_positivo": sum(
                    1 for z in zero_rows_all if z.get("stock_status") == "stock_positivo"
                ),
            }
            payload["zero_cost_summary"] = {
                "total": len(zero_rows),
                "scope": f"filtrado_{stock_scope}",
                "stock_cero_explicito": sum(
                    1 for z in zero_rows if z.get("stock_status") == "stock_cero_explicito"
                ),
                "sin_fila_stock": sum(
                    1 for z in zero_rows if z.get("stock_status") == "sin_fila_stock"
                ),
                "stock_positivo": sum(
                    1 for z in zero_rows if z.get("stock_status") == "stock_positivo"
                ),
                "con_recepcion_comparable": sum(
                    1 for z in zero_rows if z.get("has_comparable_reception")
                ),
                "sin_recepcion_comparable": sum(
                    1 for z in zero_rows if not z.get("has_comparable_reception")
                ),
            }

            payload["ok"] = True
            payload["warnings"].extend(
                [
                    "Las alertas de divergencia NO son errores de costo confirmados.",
                    "La exposición monetaria es orientativa; no valorizar si la unidad no está comprobada.",
                    "Editar la hoja de revisión administrativa no actualiza PostgreSQL ni Bsale.",
                ]
            )
            return payload
        finally:
            try:
                conn.rollback()
            except Exception:
                pass
    finally:
        if own_conn:
            try:
                conn.close()
            except Exception:
                pass


def build_payload_from_controlled_data(
    *,
    company_id: int = 3,
    company_name: str = "La Quillotana (prueba)",
    census: dict[str, Any] | None = None,
    anomalies: list[dict[str, Any]] | None = None,
    average_vs_receptions: list[dict[str, Any]] | None = None,
    zero_cost_actives: list[dict[str, Any]] | None = None,
    orphans: list[dict[str, Any]] | None = None,
    sync_state: list[dict[str, Any]] | None = None,
    price_lists: dict[int, str] | None = None,
) -> dict[str, Any]:
    """Payload sintético para pruebas de XLSX (sin PostgreSQL)."""
    cut = datetime.now(timezone.utc)
    price_lists = price_lists or {12: "Supermercado", 13: "Ruta Facturas", 16: "Melinka"}
    anomalies = anomalies or []
    average_vs_receptions = average_vs_receptions or []
    zero_cost_actives = zero_cost_actives or []
    buckets = {
        "universo": len(average_vs_receptions),
        "sin_recepcion_comparable": sum(
            1
            for r in average_vs_receptions
            if r.get("banda_principal") == "sin_recepcion_comparable"
        ),
        "requiere_verificacion": sum(
            1
            for r in average_vs_receptions
            if r.get("banda_principal") == "requiere_verificacion"
        ),
        "hasta_15": sum(
            1 for r in average_vs_receptions if r.get("banda_principal") == "hasta_15"
        ),
        "entre_15_y_50": sum(
            1
            for r in average_vs_receptions
            if r.get("banda_principal") == "entre_15_y_50"
        ),
        "sobre_50": sum(
            1 for r in average_vs_receptions if r.get("banda_principal") == "sobre_50"
        ),
        "extrema_indicador": sum(
            1 for r in average_vs_receptions if r.get("extrema_indicador")
        ),
    }
    return {
        "ok": True,
        "company_id": company_id,
        "company_name": company_name,
        "cut_at_utc": cut.isoformat(),
        "stock_filter": DEFAULT_STOCK_FILTER,
        "stock_filter_label": STOCK_FILTER_LABELS[DEFAULT_STOCK_FILTER],
        "stock_criterion": STOCK_CRITERION,
        "price_lists": price_lists,
        "offices": {
            1: "Bodega Central",
            2: "Bodega 1",
            3: "Supermercado",
            4: "Quillotana I",
            5: "Quillotana II",
            6: "Bajas",
        },
        "census": census
        or {
            "variantes": 100,
            "activas": 80,
            "inactivas": 20,
            "activas_stock_pos": 50,
            "stock_pos_sin_variante": 0,
        },
        "cost_coverage": {
            "variantes": 100,
            "filas_variant_costs": 95,
            "costs_average_pos": 70,
            "costs_average_zero": 25,
            "costs_average_null": 0,
        },
        "cost_classes": [],
        "orphans": orphans or [],
        "sync_state": sync_state
        or [
            {
                "resource": "variant_costs",
                "scope": "full",
                "status": "SUCCESS",
                "last_success_at": cut,
                "last_error_at": None,
                "active_freshness_alert": False,
                "freshness_note": "último SUCCESS vigente",
            }
        ],
        "divergence_buckets": buckets,
        "divergence_buckets_identity": {
            "closes": True,
            "sum_principales": buckets["universo"],
            "extrema_subset_ok": True,
        },
        "anomalies": anomalies,
        "anomalies_count": len(anomalies),
        "anomalies_source": "universo_completo_abs_diff_gt_15 — no top_n del CLI",
        "average_vs_receptions": average_vs_receptions,
        "zero_cost_actives": zero_cost_actives,
        "zero_cost_summary": {
            "total": len(zero_cost_actives),
            "stock_cero_explicito": sum(
                1
                for z in zero_cost_actives
                if z.get("stock_status") == "stock_cero_explicito"
            ),
            "sin_fila_stock": sum(
                1 for z in zero_cost_actives if z.get("stock_status") == "sin_fila_stock"
            ),
            "stock_positivo": sum(
                1 for z in zero_cost_actives if z.get("stock_status") == "stock_positivo"
            ),
            "con_recepcion_comparable": sum(
                1 for z in zero_cost_actives if z.get("has_comparable_reception")
            ),
            "sin_recepcion_comparable": sum(
                1 for z in zero_cost_actives if not z.get("has_comparable_reception")
            ),
        },
        "methodology": {
            "alerts_are_not_confirmed_errors": True,
            "exposure": "orientativa",
        },
        "warnings": [
            "Las alertas de divergencia NO son errores de costo confirmados.",
            "Editar la hoja de revisión administrativa no actualiza PostgreSQL ni Bsale.",
        ],
        "privileges": {
            "variant_costs": True,
            "variant_prices": True,
            "receptions": True,
            "products": True,
        },
    }
