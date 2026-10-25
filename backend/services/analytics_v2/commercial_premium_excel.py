"""Generador XLSX — Excel Premium Auditoría Comercial 8.0 (6 hojas).

Extiende el estilo de ``excel_export`` (Graphite, filtros, freeze, texto SKU/barras).
No escribe PostgreSQL; la hoja de revisión es solo plantilla editable local.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from io import BytesIO
from typing import Any, Sequence

from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.datavalidation import DataValidation

from backend.services.analytics_v2.commercial_premium_service import SHEET_NAMES

# Estilo alineado a excel_export (sin import circular).
HEADER_FILL = PatternFill("solid", fgColor="17191E")
HEADER_FONT = Font(color="FFFFFF", bold=True, size=10)
DATE_FMT = "yyyy-mm-dd"
DATETIME_FMT = "yyyy-mm-dd hh:mm"
PCT_FMT = "0.00"


def _num(value: Any) -> float | int | None:
    if value is None:
        return None
    try:
        d = Decimal(str(value))
    except Exception:
        return None
    return int(d) if d == d.to_integral_value() else float(d)


def _date(value: Any) -> date | datetime | None:
    if value is None:
        return None
    if isinstance(value, (date, datetime)):
        return value
    text = str(value)
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        try:
            return date.fromisoformat(text[:10])
        except ValueError:
            return None

# Pesos chilenos: enteros habituales; decimales cuando el motor trae fracciones.
CLP_FMT = '#,##0'
CLP_DEC_FMT = '#,##0.00'
PCT_RATIO_FMT = '0.00%'
THIN = Border(
    left=Side(style="thin", color="D0D4DA"),
    right=Side(style="thin", color="D0D4DA"),
    top=Side(style="thin", color="D0D4DA"),
    bottom=Side(style="thin", color="D0D4DA"),
)
FILL_P1 = PatternFill("solid", fgColor="F8D7DA")
FILL_P2 = PatternFill("solid", fgColor="FFE5CC")
FILL_P3 = PatternFill("solid", fgColor="FFF3CD")
FILL_PENDING = PatternFill("solid", fgColor="E8EEF7")
FILL_WARN = PatternFill("solid", fgColor="F5F5F5")
FILL_SECTION = PatternFill("solid", fgColor="17191E")
FONT_SECTION = Font(color="FFFFFF", bold=True, size=11)
FONT_MUTED = Font(color="5C6570", size=9, italic=True)
ADMIN_STATES = (
    "Pendiente",
    "En revisión",
    "Revisado sin error",
    "Corrección necesaria",
    "Corrección aplicada",
)


def _text(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _priority_fill(band: str | None) -> PatternFill | None:
    if not band:
        return None
    b = band.upper()
    if b.startswith("P1"):
        return FILL_P1
    if b.startswith("P2"):
        return FILL_P2
    if b.startswith("P3"):
        return FILL_P3
    return None


def _set_widths(ws: Any, widths: Sequence[int]) -> None:
    for i, w in enumerate(widths, start=1):
        ws.column_dimensions[get_column_letter(i)].width = w


def _header_row(ws: Any, headers: Sequence[str], row: int = 1) -> None:
    for col, title in enumerate(headers, start=1):
        cell = ws.cell(row=row, column=col, value=title)
        cell.fill = HEADER_FILL
        cell.font = HEADER_FONT
        cell.alignment = Alignment(vertical="center", wrap_text=True)
        cell.border = THIN
    ws.freeze_panes = "A2"
    ws.auto_filter.ref = f"A1:{get_column_letter(len(headers))}1"


def _style_data_cell(cell: Any, *, money: bool = False, money_dec: bool = False,
                     pct_ratio: bool = False, pct_points: bool = False,
                     as_date: bool = False, as_datetime: bool = False,
                     as_text: bool = False, fill: PatternFill | None = None) -> None:
    cell.border = THIN
    cell.alignment = Alignment(vertical="center")
    if fill is not None:
        cell.fill = fill
    if as_text:
        cell.number_format = "@"
        if cell.value is not None and not isinstance(cell.value, str):
            cell.value = str(cell.value)
        # Evitar que Excel interprete strings que empiezan con = como fórmula.
        if isinstance(cell.value, str) and cell.value[:1] in ("=", "+", "-", "@"):
            cell.value = "'" + cell.value
    elif money_dec:
        cell.number_format = CLP_DEC_FMT
    elif money:
        cell.number_format = CLP_FMT
    elif pct_ratio:
        cell.number_format = PCT_RATIO_FMT
    elif pct_points:
        cell.number_format = PCT_FMT
    elif as_datetime:
        cell.number_format = DATETIME_FMT
    elif as_date:
        cell.number_format = DATE_FMT


def _write_summary(ws: Any, payload: dict[str, Any]) -> None:
    ws.sheet_view.showGridLines = False
    _set_widths(ws, [42, 55, 55])
    title = ws.cell(1, 1, "Excel Premium — Auditoría Comercial 8.0")
    title.font = Font(bold=True, size=16, color="17191E")
    ws.merge_cells("A1:C1")

    subtitle = ws.cell(
        2, 1,
        "Panel administrativo de control de costos · alertas ≠ errores confirmados",
    )
    subtitle.font = FONT_MUTED
    ws.merge_cells("A2:C2")

    census = payload.get("census") or {}
    costs = payload.get("cost_coverage") or {}
    buckets = payload.get("divergence_buckets") or {}
    zsum = payload.get("zero_cost_summary") or {}
    identity = payload.get("divergence_buckets_identity") or {}

    rows: list[tuple[str, Any, str]] = [
        ("Empresa", f"{payload.get('company_name')} (company_id={payload.get('company_id')})", ""),
        ("Corte UTC", payload.get("cut_at_utc"), "Fotografía REPEATABLE READ"),
        ("Variantes totales", census.get("variantes"), ""),
        ("Activas / Inactivas", f"{census.get('activas')} / {census.get('inactivas')}", ""),
        ("Cobertura costos (filas)", costs.get("filas_variant_costs"), f"de {costs.get('variantes')} variantes"),
        ("Costos promedio > 0", costs.get("costs_average_pos"), "Fuente: variant_costs.average_cost"),
        ("Costos promedio = 0", costs.get("costs_average_zero"), "No es ausencia de fila"),
        ("Activas stock>0", census.get("activas_stock_pos"), ""),
        ("Huérfanas stock>0 sin variante", census.get("stock_pos_sin_variante"), "Integridad catálogo"),
        ("Universo stock+costo positivo", buckets.get("universo"), "Hoja Promedio vs recepciones"),
        ("Divergencias ≤15 %", buckets.get("hasta_15"), "Sin alerta prioritaria"),
        ("Divergencias 15–50 % (moderadas)", buckets.get("entre_15_y_50"), "Alerta investigación"),
        ("Divergencias >50 % (importantes)", buckets.get("sobre_50"), "Incluye extremas"),
        ("Indicador extremas (≥100 % o ≤−60 %)", buckets.get("extrema_indicador"), "Subconjunto de >50 %; no error confirmado"),
        ("Requieren verificación de evidencia", buckets.get("requiere_verificacion"), "Sin Δ% calculable"),
        ("Sin recepción comparable", buckets.get("sin_recepcion_comparable"), "Sin Δ% calculable"),
        ("Anomalías exportadas (Δ>15 %)", payload.get("anomalies_count"), payload.get("anomalies_source")),
        ("Activos costo cero", zsum.get("total"), ""),
        ("  · stock cero explícito", zsum.get("stock_cero_explicito"), "Fila stock con quantity=0"),
        ("  · sin fila de stock", zsum.get("sin_fila_stock"), "Dato ausente ≠ cero"),
        ("Cierre buckets principales", "Sí" if identity.get("closes") else "NO", str(identity)),
    ]

    r = 4
    hdr = ws.cell(r, 1, "Indicador")
    hdr2 = ws.cell(r, 2, "Valor")
    hdr3 = ws.cell(r, 3, "Nota")
    for c in (hdr, hdr2, hdr3):
        c.fill = HEADER_FILL
        c.font = HEADER_FONT
    r = 5
    for label, value, note in rows:
        ws.cell(r, 1, label).font = Font(bold=True, size=10)
        ws.cell(r, 2, value if value is not None else "—")
        ws.cell(r, 3, note).font = FONT_MUTED
        r += 1

    r += 1
    sec = ws.cell(r, 1, "Metodología y advertencias comerciales")
    sec.fill = FILL_SECTION
    sec.font = FONT_SECTION
    ws.merge_cells(start_row=r, start_column=1, end_row=r, end_column=3)
    r += 1
    method_lines = [
        "Costo analizado = average_cost Bsale (no se recalcula desde la última recepción).",
        "Δ% = (última comparable − promedio) / promedio × 100. Comparable = FACTURA/GUÍA completa con costo>0 y cantidad>0.",
        "Exposición potencial = |última comparable − promedio| × stock. Solo orientación de revisión; no es pérdida ni valorización contable.",
        "Si la unidad/presentación no está comprobada, la exposición no debe usarse como valorización definitiva.",
        "Prioridad de revisión (P1–P4) es independiente de la confianza de error.",
        "Una posible corrección administrativa no reduce la prioridad de revisar el caso.",
        "Alertas ≠ errores confirmados. Editar la hoja 6 no actualiza la ERP ni PostgreSQL.",
    ]
    for line in method_lines + list(payload.get("warnings") or []):
        ws.cell(r, 1, "• " + line).font = Font(size=9)
        ws.merge_cells(start_row=r, start_column=1, end_row=r, end_column=3)
        r += 1

    r += 1
    sec2 = ws.cell(r, 1, "Frescura de sincronización (solo alertas activas)")
    sec2.fill = FILL_SECTION
    sec2.font = FONT_SECTION
    ws.merge_cells(start_row=r, start_column=1, end_row=r, end_column=3)
    r += 1
    ws.cell(r, 1, "Recurso").font = HEADER_FONT
    ws.cell(r, 1).fill = HEADER_FILL
    ws.cell(r, 2, "Estado / último SUCCESS").font = HEADER_FONT
    ws.cell(r, 2).fill = HEADER_FILL
    ws.cell(r, 3, "Nota").font = HEADER_FONT
    ws.cell(r, 3).fill = HEADER_FILL
    r += 1
    for s in payload.get("sync_state") or []:
        ws.cell(r, 1, f"{s.get('resource')} / {s.get('scope')}")
        success = s.get("last_success_at")
        ws.cell(r, 2, f"{s.get('status')} · {success}")
        note_cell = ws.cell(r, 3, s.get("freshness_note"))
        if s.get("active_freshness_alert"):
            note_cell.fill = FILL_P2
        r += 1


def _write_anomalies(ws: Any, payload: dict[str, Any]) -> None:
    price_lists: dict[int, str] = payload.get("price_lists") or {}
    list_ids = sorted(price_lists.keys())
    headers = [
        "Prioridad",
        "Score",
        "Producto",
        "Variante",
        "variant_id",
        "SKU",
        "Código de barras",
        "Costo promedio",
        "Último costo comparable",
        "Diferencia %",
        "Stock total",
        "Exposición orientativa",
        "Tipo de alerta",
        "Calidad evidencia",
        "Confianza de error",
        "Documento",
        "N° documento",
        "Fecha recepción",
        "Oficina",
        "Acción recomendada",
        "Advertencia",
    ]
    for lid in list_ids:
        headers.append(f"Precio {price_lists.get(lid, lid)} (#{lid})")

    _header_row(ws, headers)
    _set_widths(
        ws,
        [12, 10, 36, 28, 11, 16, 16, 14, 14, 12, 12, 16, 22, 16, 14, 18, 14, 13, 10, 28, 36]
        + [14] * len(list_ids),
    )

    rows = payload.get("anomalies") or []
    for i, item in enumerate(rows, start=2):
        prio = item.get("admin_priority") or {}
        band = prio.get("review_priority") or prio.get("priority_band")
        fill = _priority_fill(band)
        diff = _num(item.get("diff_pct"))
        # Guardar Δ% como ratio Excel (35.77 → 0.3577) para formato %
        diff_ratio = (float(diff) / 100.0) if diff is not None else None
        doc = item.get("last_comparable_document") or item.get("lc_document")
        doc_n = item.get("last_comparable_document_number") or item.get("lc_document_number")
        values: list[Any] = [
            band,
            prio.get("priority_score"),
            item.get("product_name"),
            item.get("variant_name"),
            item.get("variant_id"),
            _text(item.get("sku")),
            _text(item.get("barcode")),
            _num(item.get("average_cost")),
            _num(item.get("last_comparable_cost")),
            diff_ratio,
            _num(item.get("stock_total")),
            None,  # fórmula exposición
            "divergencia_gt_15_alerta" + ("_extrema" if item.get("extrema_indicador") else ""),
            prio.get("evidence_quality"),
            prio.get("error_confidence"),
            doc,
            doc_n,
            _date(item.get("last_comparable_date") or item.get("lc_date")),
            item.get("last_comparable_office_id") or item.get("lc_office_id"),
            prio.get("suggested_action"),
            prio.get("monetary_exposure_disclaimer"),
        ]
        for lid in list_ids:
            values.append(_num(item.get(f"price_{lid}")))

        for col, val in enumerate(values, start=1):
            cell = ws.cell(i, col, val)
            _style_data_cell(
                cell,
                money=col in (8, 9),
                money_dec=col == 12,
                pct_ratio=col == 10,
                as_date=col == 18,
                as_text=col in (6, 7),
                fill=fill if col == 1 else None,
            )

        # Exposición: fórmula segura; vacía si faltan insumos
        # H=avg, I=last, K=stock
        formula = f'=IF(OR(H{i}="",I{i}="",K{i}=""),"",ABS(I{i}-H{i})*K{i})'
        exp_cell = ws.cell(i, 12, formula)
        _style_data_cell(exp_cell, money_dec=True, fill=FILL_PENDING)

    if rows:
        ws.auto_filter.ref = f"A1:{get_column_letter(len(headers))}{len(rows) + 1}"


def _write_zero_cost(ws: Any, payload: dict[str, Any]) -> None:
    price_lists: dict[int, str] = payload.get("price_lists") or {}
    list_ids = sorted(price_lists.keys())
    headers = [
        "Producto",
        "Variante",
        "variant_id",
        "SKU",
        "Código de barras",
        "Costo promedio",
        "Estado stock",
        "Stock total (vacío=sin fila)",
        "Stock cero explícito",
        "Sin fila stock",
        "Con recepción comparable",
        "Sin recepción comparable",
        "Estado comercial",
        "Acción recomendada",
        "Documento comparable",
        "Fecha comparable",
        "Oficina",
    ]
    for lid in list_ids:
        headers.append(f"Precio vigente {price_lists.get(lid, lid)} (#{lid})")

    _header_row(ws, headers)
    _set_widths(ws, [36, 28, 11, 16, 16, 12, 18, 14, 12, 12, 12, 12, 22, 32, 18, 13, 10] + [14] * len(list_ids))

    rows = payload.get("zero_cost_actives") or []
    for i, z in enumerate(rows, start=2):
        sin_fila = bool(z.get("sin_fila_stock") or z.get("stock_status") == "sin_fila_stock")
        stock_val = None if sin_fila else _num(z.get("stock_total_raw") if z.get("stock_total_raw") is not None else z.get("stock_total"))
        values: list[Any] = [
            z.get("product_name"),
            z.get("variant_name"),
            z.get("variant_id"),
            _text(z.get("sku")),
            _text(z.get("barcode")),
            _num(z.get("average_cost")),  # 0 explícito
            z.get("stock_status"),
            stock_val,
            "Sí" if z.get("stock_status") == "stock_cero_explicito" else "No",
            "Sí" if sin_fila else "No",
            "Sí" if z.get("has_comparable_reception") else "No",
            "No" if z.get("has_comparable_reception") else "Sí",
            z.get("commercial_status"),
            z.get("recommended_action"),
            z.get("last_comparable_document"),
            _date(z.get("last_comparable_date")),
            z.get("last_comparable_office_id"),
        ]
        for lid in list_ids:
            values.append(_num(z.get(f"price_{lid}")))

        pending = sin_fila or not z.get("has_comparable_reception")
        for col, val in enumerate(values, start=1):
            cell = ws.cell(i, col, val)
            _style_data_cell(
                cell,
                money=col == 6 or col > 17,
                as_date=col == 16,
                as_text=col in (4, 5),
                fill=FILL_PENDING if pending and col in (7, 8) else None,
            )
    if rows:
        ws.auto_filter.ref = f"A1:{get_column_letter(len(headers))}{len(rows) + 1}"


def _write_avg_vs_rec(ws: Any, payload: dict[str, Any]) -> None:
    headers = [
        "Producto",
        "Variante",
        "variant_id",
        "SKU",
        "Código de barras",
        "Costo promedio Bsale",
        "Última recepción comparable",
        "Diferencia absoluta",
        "Diferencia %",
        "Banda",
        "Extrema",
        "Fecha",
        "Documento",
        "N° documento",
        "Oficina",
        "Stock total",
        "Estado comparabilidad",
        "Evidencia suficiente",
        "Prioridad revisión",
        "Confianza error",
    ]
    _header_row(ws, headers)
    _set_widths(ws, [36, 28, 11, 16, 16, 14, 14, 12, 12, 18, 10, 12, 18, 14, 10, 12, 18, 12, 12, 12])

    rows = payload.get("average_vs_receptions") or []
    for i, item in enumerate(rows, start=2):
        prio = item.get("admin_priority") or {}
        diff = _num(item.get("diff_pct"))
        diff_ratio = (float(diff) / 100.0) if diff is not None else None
        # Diferencia absoluta: valor motor o fórmula si ambos costos presentes
        diff_abs = _num(item.get("diff_abs"))
        values = [
            item.get("product_name"),
            item.get("variant_name"),
            item.get("variant_id"),
            _text(item.get("sku")),
            _text(item.get("barcode")),
            _num(item.get("average_cost")),
            _num(item.get("last_comparable_cost")),
            diff_abs if diff_abs is not None else None,
            diff_ratio,
            item.get("banda_principal"),
            "Sí" if item.get("extrema_indicador") else "No",
            _date(item.get("last_comparable_date")),
            item.get("last_comparable_document"),
            item.get("last_comparable_document_number"),
            item.get("last_comparable_office_id"),
            _num(item.get("stock_total")),
            item.get("comparability_status"),
            "Sí" if item.get("evidence_sufficient") else "No",
            prio.get("review_priority") or prio.get("priority_band"),
            prio.get("error_confidence"),
        ]
        fill = None if item.get("evidence_sufficient") else FILL_PENDING
        for col, val in enumerate(values, start=1):
            cell = ws.cell(i, col, val)
            _style_data_cell(
                cell,
                money=col in (6, 7, 8),
                pct_ratio=col == 9,
                as_date=col == 12,
                as_text=col in (4, 5),
                fill=fill if col in (8, 9, 17, 18) else _priority_fill(values[18]) if col == 19 else None,
            )
        # Si no hay comparable, forzar diferencias vacías (no calculable)
        if not item.get("evidence_sufficient"):
            ws.cell(i, 8).value = None
            ws.cell(i, 9).value = None
            ws.cell(i, 8).fill = FILL_PENDING
            ws.cell(i, 9).fill = FILL_PENDING
        elif diff_abs is None and _num(item.get("average_cost")) is not None and _num(item.get("last_comparable_cost")) is not None:
            ws.cell(i, 8).value = f'=IF(OR(F{i}="",G{i}=""),"",G{i}-F{i})'
            _style_data_cell(ws.cell(i, 8), money=True)

    if rows:
        ws.auto_filter.ref = f"A1:{get_column_letter(len(headers))}{len(rows) + 1}"


def _write_integrity(ws: Any, payload: dict[str, Any]) -> None:
    _set_widths(ws, [28, 40, 40, 40])
    ws.cell(1, 1, "Integridad de catálogo y sincronización").font = Font(bold=True, size=14)
    ws.merge_cells("A1:D1")

    census = payload.get("census") or {}
    costs = payload.get("cost_coverage") or {}
    r = 3
    for label, value in [
        ("Variantes catálogo", census.get("variantes")),
        ("Filas variant_costs", costs.get("filas_variant_costs")),
        ("Sin fila de costo (aprox.)", (costs.get("variantes") or 0) - (costs.get("filas_variant_costs") or 0)),
        ("Costos NULL", costs.get("costs_average_null")),
        ("Costos = 0", costs.get("costs_average_zero")),
        ("Costos > 0", costs.get("costs_average_pos")),
        ("Activas sin fila stock", census.get("activas_sin_fila_stock")),
        ("Stock>0 sin variante (huérfanas)", census.get("stock_pos_sin_variante")),
        ("Unidades huérfanas", census.get("unidades_stock_pos_sin_variante")),
    ]:
        ws.cell(r, 1, label).font = Font(bold=True)
        ws.cell(r, 2, value)
        r += 1

    r += 1
    ws.cell(r, 1, "Huérfanas (detalle)").font = FONT_SECTION
    ws.cell(r, 1).fill = FILL_SECTION
    ws.merge_cells(start_row=r, start_column=1, end_row=r, end_column=4)
    r += 1
    for col, h in enumerate(("variant_id", "stock_total", "oficinas", "nota"), start=1):
        c = ws.cell(r, col, h)
        c.fill = HEADER_FILL
        c.font = HEADER_FONT
    r += 1
    orphans = payload.get("orphans") or []
    if not orphans:
        ws.cell(r, 1, "(sin huérfanas en esta fotografía)")
        r += 1
    for o in orphans:
        ws.cell(r, 1, o.get("variant_id"))
        ws.cell(r, 2, _num(o.get("stock_total")))
        ws.cell(r, 3, o.get("oficinas"))
        ws.cell(r, 4, "stock>0 sin fila en variants")
        r += 1

    r += 1
    ws.cell(r, 1, "Sincronización").font = FONT_SECTION
    ws.cell(r, 1).fill = FILL_SECTION
    ws.merge_cells(start_row=r, start_column=1, end_row=r, end_column=4)
    r += 1
    for col, h in enumerate(("Recurso", "Estado", "Último SUCCESS", "Advertencia activa"), start=1):
        c = ws.cell(r, col, h)
        c.fill = HEADER_FILL
        c.font = HEADER_FONT
    r += 1
    for s in payload.get("sync_state") or []:
        ws.cell(r, 1, f"{s.get('resource')} / {s.get('scope')}")
        ws.cell(r, 2, s.get("status"))
        ws.cell(r, 3, str(s.get("last_success_at") or ""))
        alert = "Sí" if s.get("active_freshness_alert") else "No"
        cell = ws.cell(r, 4, f"{alert} — {s.get('freshness_note')}")
        if s.get("active_freshness_alert"):
            cell.fill = FILL_P2
        r += 1

    r += 2
    ws.cell(r, 1, "Nota: errores históricos de sync no generan alerta si existe SUCCESS posterior.").font = FONT_MUTED


def _write_admin_review(ws: Any, payload: dict[str, Any]) -> None:
    headers = [
        "variant_id",
        "Producto",
        "Variante",
        "SKU",
        "Prioridad",
        "Motivo de alerta",
        "Documento respaldo",
        "Estado de revisión",
        "Responsable",
        "Fecha de revisión",
        "Observaciones",
        "Resolución",
        "Corrección aplicada",
    ]
    _header_row(ws, headers)
    _set_widths(ws, [11, 36, 28, 16, 12, 28, 22, 20, 18, 14, 36, 28, 22])

    # Prefill from anomalies (casos a revisar); admin columns empty/editable
    rows = payload.get("anomalies") or []
    for i, item in enumerate(rows, start=2):
        prio = item.get("admin_priority") or {}
        band = prio.get("review_priority") or prio.get("priority_band")
        doc = " ".join(
            str(x)
            for x in (
                item.get("last_comparable_document") or item.get("lc_document"),
                item.get("last_comparable_document_number") or item.get("lc_document_number"),
            )
            if x
        ).strip() or None
        values = [
            item.get("variant_id"),
            item.get("product_name"),
            item.get("variant_name"),
            _text(item.get("sku")),
            band,
            "divergencia_gt_15" + ("_extrema" if item.get("extrema_indicador") else ""),
            doc,
            "Pendiente",
            None,
            None,
            None,
            None,
            None,
        ]
        for col, val in enumerate(values, start=1):
            cell = ws.cell(i, col, val)
            _style_data_cell(
                cell,
                as_text=col == 4,
                as_date=col == 10,
                fill=_priority_fill(band) if col == 5 else (FILL_PENDING if col >= 8 else None),
            )

    # Validación desplegable estados (column H)
    if rows:
        dv = DataValidation(
            type="list",
            formula1='"' + ",".join(ADMIN_STATES) + '"',
            allow_blank=True,
            showErrorMessage=True,
            errorTitle="Estado inválido",
            error="Seleccione un estado de la lista.",
        )
        dv.add(f"H2:H{len(rows) + 1}")
        ws.add_data_validation(dv)
        ws.auto_filter.ref = f"A1:{get_column_letter(len(headers))}{len(rows) + 1}"

    # Nota al pie
    note_row = len(rows) + 3
    ws.cell(
        note_row,
        1,
        "Plantilla local: editar aquí NO actualiza PostgreSQL, Bsale ni la ERP. "
        "Use esta hoja para seguimiento administrativo externo.",
    ).font = FONT_MUTED


def commercial_premium_xlsx(payload: dict[str, Any]) -> bytes:
    """Genera el workbook Premium de exactamente 6 hojas funcionales."""
    wb = Workbook()
    # Eliminar hoja default
    default = wb.active
    wb.remove(default)

    ws1 = wb.create_sheet(SHEET_NAMES[0])
    _write_summary(ws1, payload)

    ws2 = wb.create_sheet(SHEET_NAMES[1])
    _write_anomalies(ws2, payload)

    ws3 = wb.create_sheet(SHEET_NAMES[2])
    _write_zero_cost(ws3, payload)

    ws4 = wb.create_sheet(SHEET_NAMES[3])
    _write_avg_vs_rec(ws4, payload)

    ws5 = wb.create_sheet(SHEET_NAMES[4])
    _write_integrity(ws5, payload)

    ws6 = wb.create_sheet(SHEET_NAMES[5])
    _write_admin_review(ws6, payload)

    assert list(wb.sheetnames) == list(SHEET_NAMES)

    out = BytesIO()
    wb.save(out)
    return out.getvalue()
