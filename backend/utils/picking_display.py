"""Enriquecimiento de lectura para picking (nombres, cajas, KPIs) sin regenerar snapshot."""

from __future__ import annotations

import re
from typing import Any

_SEC_UNITS_RE = re.compile(r"\(SEC\s+(\d+)\)", re.IGNORECASE)

_VARIANT_BSALE_SELECT = """
    SELECT
        v.bsale_id,
        v.bar_code,
        v.code,
        v.units_per_box,
        v.description AS variant_description,
        p.name AS product_name,
        pt.name AS product_type
    FROM bsale.variants v
    LEFT JOIN bsale.products p
        ON p.company_id = v.company_id
       AND p.bsale_id = v.product_id
    LEFT JOIN bsale.product_types pt
        ON pt.company_id = p.company_id
       AND pt.bsale_id = p.product_type_id
    WHERE v.company_id = 3
      AND ({where_clause})
"""


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
        (variant or {}).get("variant_description"),
        row.get("variante"),
        row.get("producto"),
    )


def effective_cajas(
    unidades: float,
    units_per_box: float | None,
    stored_cajas: Any,
    *,
    sin_unidad_caja: bool,
) -> float:
    """Cajas operativas = unidades / CxC con 2 decimales (sin redondear hacia arriba)."""
    if units_per_box and units_per_box > 0 and unidades > 0:
        return round(float(unidades) / float(units_per_box), 2)
    if sin_unidad_caja:
        return 0.0
    try:
        return round(float(stored_cajas or 0), 2)
    except (TypeError, ValueError):
        return 0.0


def resolve_tipo_producto(
    row: dict[str, Any],
    *,
    pm: dict[str, Any] | None = None,
    variant: dict[str, Any] | None = None,
) -> str:
    snap = (row.get("tipo_producto") or "").strip()
    if snap and snap.lower() not in ("sin tipo", "otros"):
        return snap
    for src in (
        (pm or {}).get("product_type"),
        (variant or {}).get("product_type"),
    ):
        if src and str(src).strip():
            return str(src).strip()
    return "OTROS"


def resolve_ean_barcode(
    row: dict[str, Any],
    *,
    variant: dict[str, Any] | None = None,
) -> str:
    """EAN desde variants.bar_code; document_details.variant_code suele ser SKU."""
    for src in (
        (variant or {}).get("bar_code"),
        row.get("codigo_barras"),
    ):
        bc = (src or "").strip()
        if bc:
            return bc
    return ""


def format_product_display(
    *,
    pm: dict[str, Any] | None,
    variant: dict[str, Any] | None = None,
    producto: str,
    variante: str,
    producto_variante: str = "",
) -> str:
    pn = (
        ((pm or {}).get("product_name") or "")
        or ((variant or {}).get("product_name") or "")
        or (producto or "")
    ).strip()
    vn = (
        ((pm or {}).get("variant_name") or "")
        or ((variant or {}).get("variant_description") or "")
        or (variante or "")
    ).strip()
    pv = (producto_variante or "").strip()

    if pn and vn:
        if pn.lower() == vn.lower() or pn.lower().endswith(vn.lower()):
            return pn
        return f"{pn} {vn}".strip()
    if pn:
        return pn
    if vn:
        return vn
    if pv and " — " in pv:
        return pv.replace(" — ", " ").strip()
    return pv or "Sin descripción"


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
        "cajas": round(cajas, 2),
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
    variants_by_code: dict[str, dict[str, Any]] = {}
    if variant_ids or barcodes:
        clauses: list[str] = []
        params: list[Any] = []
        if variant_ids:
            clauses.append("v.bsale_id = ANY(%s)")
            params.append(variant_ids)
        if barcodes:
            clauses.append("NULLIF(BTRIM(v.bar_code), '') = ANY(%s)")
            params.append(barcodes)
            clauses.append("NULLIF(BTRIM(v.code), '') = ANY(%s)")
            params.append(barcodes)
        cur.execute(
            _VARIANT_BSALE_SELECT.format(where_clause=" OR ".join(clauses)),
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
            code = (vd.get("code") or "").strip()
            if code:
                variants_by_code[code] = vd

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
            variant = variants_by_bc.get(bc) or variants_by_code.get(bc)

        ean = resolve_ean_barcode(row, variant=variant)
        if ean:
            pm = pm_by_bc.get(ean) or pm
            if variant is None:
                variant = variants_by_bc.get(ean)

        upb = resolve_units_per_box(row, pm=pm, variant=variant)
        unidades = float(row.get("unidades") or 0)
        sin_caja = row.get("sin_unidad_caja") is True or not upb
        cajas_eff = effective_cajas(
            unidades,
            upb,
            row.get("cajas"),
            sin_unidad_caja=sin_caja,
        )
        product_name = (
            (pm or {}).get("product_name")
            or (variant or {}).get("product_name")
            or row.get("producto")
        )
        variant_name = (
            (pm or {}).get("variant_name")
            or (variant or {}).get("variant_description")
            or row.get("variante")
        )
        display = format_product_display(
            pm=pm,
            variant=variant,
            producto=(row.get("producto") or ""),
            variante=(row.get("variante") or ""),
            producto_variante=(row.get("producto_variante") or ""),
        )
        enriched = dict(row)
        enriched["display_name"] = display
        enriched["product_name"] = product_name
        enriched["variant_name"] = variant_name
        enriched["tipo_producto"] = resolve_tipo_producto(row, pm=pm, variant=variant)
        if ean:
            enriched["codigo_barras"] = ean
        enriched["units_per_box_efectivo"] = upb
        enriched["cajas_efectivas"] = cajas_eff
        enriched["cajas"] = cajas_eff
        enriched["sin_unidad_caja"] = sin_caja and not upb
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
    out["tipo_producto"] = resolve_tipo_producto(out)
    out["units_per_box_efectivo"] = upb
    out["cajas_efectivas"] = cajas_eff
    out["cajas"] = cajas_eff
    return out
