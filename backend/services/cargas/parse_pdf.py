"""Parser determinístico de PDF de picking (texto digital, sin OCR ni LLM)."""

from __future__ import annotations

import re
from typing import Any

from backend.services.cargas.parse_common import (
    ParsedLoadPreview,
    build_line_from_mapped_row,
    map_headers,
    normalize_header,
    parse_metadata_from_text,
    parse_number,
)
from backend.services.cargas.parse_excel import PickingParseError


REQUIRED_MARKERS = (
    "picking",
    "cantidad",
)


def _require_pdfplumber():
    try:
        import pdfplumber  # type: ignore
    except ImportError as exc:
        raise PickingParseError(
            "pdfplumber no está instalado. Agregue 'pdfplumber' a requirements.txt."
        ) from exc
    return pdfplumber


def _table_score(headers: list[Any]) -> int:
    mapping = map_headers(headers)
    score = len(mapping)
    if "quantity" in mapping:
        score += 3
    if "product_name" in mapping:
        score += 3
    if "barcode" in mapping:
        score += 2
    return score


def _best_table(tables: list[list[list[Any]]]) -> tuple[list[Any], list[list[Any]], dict[str, int]]:
    best: tuple[int, list[Any], list[list[Any]], dict[str, int]] | None = None
    for table in tables:
        if not table or len(table) < 2:
            continue
        # Header may be first non-empty row
        for hi, row in enumerate(table[:3]):
            mapping = map_headers(row)
            if "quantity" not in mapping or "product_name" not in mapping:
                continue
            score = _table_score(row)
            body = table[hi + 1 :]
            if best is None or score > best[0]:
                best = (score, row, body, mapping)
            break
    if best is None:
        raise PickingParseError(
            "El PDF no corresponde al formato de picking esperado "
            "(tabla con CANTIDAD y Producto). No se adivinan columnas."
        )
    return best[1], best[2], best[3]


def _extract_all_tables(pdf) -> list[list[list[Any]]]:
    tables: list[list[list[Any]]] = []
    for page in pdf.pages:
        extracted = page.extract_tables() or []
        for tbl in extracted:
            cleaned = [
                [None if c is None else str(c).replace("\n", " ").strip() for c in row]
                for row in tbl
                if row and any(c is not None and str(c).strip() for c in row)
            ]
            if cleaned:
                tables.append(cleaned)
    return tables


def _full_text(pdf) -> str:
    parts: list[str] = []
    for page in pdf.pages:
        text = page.extract_text() or ""
        if text.strip():
            parts.append(text)
    return "\n".join(parts)


def parse_picking_pdf(*, data: bytes, filename: str) -> ParsedLoadPreview:
    pdfplumber = _require_pdfplumber()
    import io

    try:
        with pdfplumber.open(io.BytesIO(data)) as pdf:
            if not pdf.pages:
                raise PickingParseError("PDF vacío")
            text = _full_text(pdf)
            tables = _extract_all_tables(pdf)
    except PickingParseError:
        raise
    except Exception as exc:
        raise PickingParseError(f"No se pudo abrir el PDF: {exc}") from exc

    if not text.strip():
        raise PickingParseError(
            "El PDF no contiene texto seleccionable. "
            "No se admite OCR ni PDFs escaneados."
        )

    text_norm = normalize_header(text)
    if not all(marker in text_norm for marker in REQUIRED_MARKERS):
        # Allow ERP picking producto variant (Unidades instead of Cantidad)
        if "picking" not in text_norm or (
            "unidades" not in text_norm and "cantidad" not in text_norm
        ):
            raise PickingParseError(
                "Layout de PDF no reconocido como picking Quillotana/Bsale. "
                "Se rechaza sin adivinar datos."
            )

    meta = parse_metadata_from_text(text)
    header, body, mapping = _best_table(tables)

    preview = ParsedLoadPreview(
        source_type="pdf",
        original_filename=filename,
        picking_number=meta.get("picking_number"),
        picking_date=meta.get("picking_date"),
        destination=meta.get("destination"),
        truck=meta.get("truck"),
        seal=meta.get("seal"),
        document_units_total=meta.get("document_units_total"),
        document_value_total=meta.get("document_value_total"),
        format_name="picking_pdf_table_v1",
    )

    # Totales en pie: buscar explícitamente
    for m in re.finditer(
        r"Total\s+general[^\d\$]*\$?\s*([\d\.\,]+)",
        text,
        flags=re.IGNORECASE,
    ):
        val = parse_number(m.group(1))
        if val is None:
            continue
        # Heurística: total monetario suele ser grande; unidades si etiqueta lo dice
        span = m.group(0).lower()
        if "unidad" in span or "cantidad" in span:
            preview.document_units_total = preview.document_units_total or val
        elif "$" in span or "monto" in span or "valor" in span:
            preview.document_value_total = preview.document_value_total or val
        elif preview.document_value_total is None and val >= 1000:
            preview.document_value_total = val
        elif preview.document_units_total is None:
            preview.document_units_total = val

    for row in body:
        joined = " ".join(c for c in row if c).strip()
        if not joined:
            continue
        norm = normalize_header(joined)
        if norm.startswith("total") or "total general" in norm:
            continue
        line = build_line_from_mapped_row(row, mapping)
        if not line.product_name and line.requested_units <= 0:
            continue
        preview.lines.append(line)

    if not preview.picking_number:
        # Fallback: "Picking Producto — BATCH" / planning number in ERP PDFs
        m = re.search(
            r"PLAN-?([A-Za-z0-9\-_/]+)|Picking\s+#?\s*([A-Za-z0-9\-_/]+)",
            text,
            flags=re.IGNORECASE,
        )
        if m:
            preview.picking_number = next(g for g in m.groups() if g)

    if not preview.picking_number:
        preview.errors.append("No se encontró N.º Picking en el PDF")
    if not preview.valid_lines:
        preview.errors.append("No hay líneas válidas en el PDF")

    preview.validate_totals()
    # If document has no printed units total, do not invent — but allow import
    # with warning when only line sum exists.
    if preview.document_units_total is None:
        preview.warnings.append(
            "El PDF no declara Total general de unidades; "
            "se usará la suma de líneas parseadas."
        )
    return preview
