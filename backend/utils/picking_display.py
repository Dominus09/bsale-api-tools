"""Enriquecimiento de lectura para picking (nombres, cajas, KPIs) sin regenerar snapshot."""

from __future__ import annotations

import math
import re
from typing import Any

_SEC_UNITS_RE = re.compile(r"\(SEC\s+(\d+)\)", re.IGNORECASE)


def units_per_box_from_text(*texts: str | None) -> float | None:
    for t in texts:
        if not t:
            continue
        m = _SEC_UNITS_RE.search(t)
        if m:
            try:
                n = int(m.group(1))
                if n > 0:
                    return float(n)
            except (TypeError, ValueError):
                continue
    return None


def resolve_units_per_box(
    row: dict[str, Any],
    *,
    pm: dict[str, Any] | None = None,
    variant: dict[str, Any] | None = None,
) -> float | None:
    for src in (
        row.get("units_per_box"),
        (pm or {}).get("units_per_box"),
        (variant or {}).get("units_per_box"),
    ):
        if src is not None:
            try:
                n = float(src)
                if n > 0:
                    return n
            except (TypeError, ValueError):
                pass
    return units_per_box_from_text(
        (pm or {}).get("variant_name"),
        (pm or {}).get("product_name"),
        row.get("variante"),
        row.get("producto"),
        (variant or {}).get("description"),
    )


def effective_cajas(
    unidades: float,
    units_per_box: float | None,
    stored_cajas: Any,
    *,
    sin_unidad_caja: bool,
) -> float:
    if units_per_box and units_per_box > 0 and unidades > 0:
        return float(math.ceil(unidades / units_per_box))
    if sin_unidad_caja:
        return 0.0
    try:
        return float(stored_cajas or 0)
    except (TypeError, ValueError):
        return 0.0


def format_product_display(
    *,
    pm: dict[str, Any] | None,
    producto: str,
    variante: str,
    producto_variante: str = "",
) -> str:
    pn = ((pm or {}).get("product_name") or "").strip()
    vn = ((pm or {}).get("variant_name") or "").strip()
    p = (producto or "").strip()
    v = (variante or "").strip()
    pv = (producto_variante or "").strip()

    if pn and vn:
        return f"{pn} {vn}".strip()
    if pn:
        if v and v.lower() not in pn.lower():
            return f"{pn} {v}".strip()
        return pn
    if p and v and p != v:
        if p.lower().endswith(v.lower()):
            return p
        return f"{p} {v}".strip()
    if pv and " — " in pv:
        return pv.replace(" — ", " ").strip()
    return p or v or pv or "Sin descripción"


def _client_identity(c: dict[str, Any]) -> str:
    cid = c.get("client_id")
    if cid is not None:
        try:
            n = int(cid)
            if n != 0:
                return f"id:{n}"
        except (TypeError, ValueError):
            pass
    name = (c.get("client_name") or c.get("fantasy_name") or "").strip().lower()
    if name:
        return f"name:{name}"
    return f"doc:{c.get('related_document_id') or c.get('document_number')}"


def count_snapshot_clients(clients: list[dict[str, Any]]) -> int:
    return len({_client_identity(c) for c in clients})


def compute_items_totals(items: list[dict[str, Any]]) -> dict[str, Any]:
    unidades = 0.0
    cajas = 0.0
    monto = 0.0
    barcodes: set[str] = set()
    for it in items:
        u = float(it.get("unidades") or 0)
        unidades += u
        cajas += float(it.get("cajas_efectivas") if "cajas_efectivas" in it else it.get("cajas") or 0)
        monto += float(it.get("total_monto") or 0)
        bc = (it.get("codigo_barras") or "").strip()
        if bc:
            barcodes.add(bc)
    return {
        "lines": len(items),
        "unidades": unidades,
        "cajas": cajas,
        "total_monto_clp": monto,
        "distinct_products": len(barcodes) or len(items),
    }


def enrich_picking_product_rows(
    cur: Any,
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not rows:
        return rows

    barcodes = list(
        {(r.get("codigo_barras") or "").strip() for r in rows if (r.get("codigo_barras") or "").strip()}
    )
    variant_ids = list(
        {int(r["variant_id"]) for r in rows if r.get("variant_id") is not None}
    )

    pm_by_bc: dict[str, dict[str, Any]] = {}
    if barcodes:
        cur.execute(
            """
            SELECT barcode, product_name, variant_name, product_type, units_per_box
            FROM bsale.products_master
            WHERE barcode = ANY(%s)
            """,
            (barcodes,),
        )
        cols = [d[0] for d in cur.description]
        for r in cur.fetchall():
            d = dict(zip(cols, r))
            pm_by_bc[(d.get("barcode") or "").strip()] = d

    variants_by_id: dict[int, dict[str, Any]] = {}
    variants_by_bc: dict[str, dict[str, Any]] = {}
    if variant_ids or barcodes:
        clauses: list[str] = []
        params: list[Any] = []
        if variant_ids:
            clauses.append("bsale_id = ANY(%s)")
            params.append(variant_ids)
        if barcodes:
            clauses.append("NULLIF(BTRIM(bar_code), '') = ANY(%s)")
            params.append(barcodes)
        cur.execute(
            f"""
            SELECT bsale_id, bar_code, units_per_box, description
            FROM bsale.variants
            WHERE company_id = 3
              AND ({' OR '.join(clauses)})
            """,
            tuple(params),
        )
        vcols = [d[0] for d in cur.description]
        for r in cur.fetchall():
            vd = dict(zip(vcols, r))
            try:
                variants_by_id[int(vd["bsale_id"])] = vd
            except (TypeError, ValueError):
                pass
            bc = (vd.get("bar_code") or "").strip()
            if bc:
                variants_by_bc[bc] = vd

    out: list[dict[str, Any]] = []
    for row in rows:
        bc = (row.get("codigo_barras") or "").strip()
        pm = pm_by_bc.get(bc)
        vid = row.get("variant_id")
        variant = None
        if vid is not None:
            try:
                variant = variants_by_id.get(int(vid))
            except (TypeError, ValueError):
                variant = None
        if variant is None and bc:
            variant = variants_by_bc.get(bc)

        upb = resolve_units_per_box(row, pm=pm, variant=variant)
        unidades = float(row.get("unidades") or 0)
        sin_caja = row.get("sin_unidad_caja") is True or not upb
        cajas_eff = effective_cajas(
            unidades,
            upb,
            row.get("cajas"),
            sin_unidad_caja=sin_caja,
        )
        display = format_product_display(
            pm=pm,
            producto=(row.get("producto") or ""),
            variante=(row.get("variante") or ""),
            producto_variante=(row.get("producto_variante") or ""),
        )
        enriched = dict(row)
        enriched["display_name"] = display
        enriched["product_name"] = (pm or {}).get("product_name") or row.get("producto")
        enriched["variant_name"] = (pm or {}).get("variant_name") or row.get("variante")
        enriched["units_per_box_efectivo"] = upb
        enriched["cajas_efectivas"] = cajas_eff
        enriched["cajas"] = cajas_eff
        enriched["sin_unidad_caja"] = sin_caja and not upb
        if pm and pm.get("product_type") and not (row.get("tipo_producto") or "").strip():
            enriched["tipo_producto"] = pm["product_type"]
        out.append(enriched)
    return out


def enrich_product_api_row(row: dict[str, Any]) -> dict[str, Any]:
    """Aplica campos de display sobre dict ya serializado (sin DB)."""
    upb = resolve_units_per_box(row)
    unidades = float(row.get("unidades") or 0)
    sin_caja = row.get("sin_unidad_caja") is True or not upb
    cajas_eff = effective_cajas(
        unidades, upb, row.get("cajas"), sin_unidad_caja=sin_caja
    )
    display = format_product_display(
        pm=None,
        producto=(row.get("producto") or ""),
        variante=(row.get("variante") or ""),
        producto_variante=(row.get("producto_variante") or ""),
    )
    if row.get("product_name") or row.get("variant_name"):
        display = format_product_display(
            pm={"product_name": row.get("product_name"), "variant_name": row.get("variant_name")},
            producto=(row.get("producto") or ""),
            variante=(row.get("variante") or ""),
            producto_variante=(row.get("producto_variante") or ""),
        )
    out = dict(row)
    out["display_name"] = display
    out["units_per_box_efectivo"] = upb
    out["cajas_efectivas"] = cajas_eff
    out["cajas"] = cajas_eff
    return out
