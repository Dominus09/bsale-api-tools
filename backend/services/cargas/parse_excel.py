"""Parser determinístico de picking Excel (.xlsx / .xls)."""

from __future__ import annotations

import io
from typing import Any

from backend.services.cargas.parse_common import (
    ParsedLoadPreview,
    build_line_from_mapped_row,
    map_headers,
    normalize_header,
    parse_metadata_from_text,
    parse_number,
)


class PickingParseError(ValueError):
    """Formato de archivo no reconocido o inválido."""


def _find_header_row(rows: list[list[Any]]) -> tuple[int, dict[str, int]]:
    best: tuple[int, dict[str, int]] | None = None
    for i, row in enumerate(rows[:40]):
        mapping = map_headers(row)
        if "quantity" in mapping and "product_name" in mapping:
            score = len(mapping)
            if best is None or score > len(best[1]):
                best = (i, mapping)
    if best is None:
        raise PickingParseError(
            "No se encontró la tabla de picking esperada "
            "(columnas CANTIDAD y Producto). Verifique el Excel."
        )
    return best


def parse_picking_excel(*, data: bytes, filename: str) -> ParsedLoadPreview:
    try:
        import pandas as pd
    except ImportError as exc:
        raise PickingParseError("pandas/openpyxl no disponible en el backend") from exc

    try:
        df = pd.read_excel(io.BytesIO(data), header=None, dtype=object)
    except Exception as exc:
        raise PickingParseError(f"No se pudo leer el Excel: {exc}") from exc

    if df.empty:
        raise PickingParseError("El Excel está vacío")

    rows = [[None if pd.isna(v) else v for v in row] for row in df.values.tolist()]
    header_idx, mapping = _find_header_row(rows)

    # Metadata: celdas encima del header + texto concatenado
    preamble_parts: list[str] = []
    for row in rows[: header_idx + 1]:
        for cell in row:
            if cell is None:
                continue
            text = str(cell).strip()
            if text:
                preamble_parts.append(text)
    meta = parse_metadata_from_text("\n".join(preamble_parts))

    # Totales a veces vienen en filas inferiores
    footer_text = "\n".join(
        str(c).strip()
        for row in rows[header_idx + 1 :]
        for c in row
        if c is not None and str(c).strip()
    )
    footer_meta = parse_metadata_from_text(footer_text)
    for key in ("document_units_total", "document_value_total"):
        if meta.get(key) is None and footer_meta.get(key) is not None:
            meta[key] = footer_meta[key]

    preview = ParsedLoadPreview(
        source_type="excel",
        original_filename=filename,
        picking_number=meta.get("picking_number"),
        picking_date=meta.get("picking_date"),
        destination=meta.get("destination"),
        truck=meta.get("truck"),
        seal=meta.get("seal"),
        document_units_total=meta.get("document_units_total"),
        document_value_total=meta.get("document_value_total"),
        format_name="picking_excel_v1",
    )

    for row in rows[header_idx + 1 :]:
        # Skip blank / total rows
        joined = " ".join(str(c).strip() for c in row if c is not None).strip()
        if not joined:
            continue
        norm = normalize_header(joined)
        if norm.startswith("total") or "total general" in norm:
            # Try to capture totals from this row if still missing
            nums = [parse_number(c) for c in row]
            nums = [n for n in nums if n is not None]
            if preview.document_units_total is None and nums:
                # Prefer largest integer-ish as units if value total already set
                preview.document_units_total = nums[0]
            if preview.document_value_total is None and len(nums) > 1:
                preview.document_value_total = nums[-1]
            continue
        line = build_line_from_mapped_row(row, mapping)
        if not line.product_name and line.requested_units <= 0:
            continue
        preview.lines.append(line)

    if not preview.picking_number:
        preview.errors.append("No se encontró N.º Picking en el archivo")
    if not preview.valid_lines:
        preview.errors.append("No hay líneas válidas para importar")

    preview.validate_totals()
    return preview
