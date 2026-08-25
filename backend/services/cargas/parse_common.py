"""Modelo común de preview/importación de pickings (Excel o PDF)."""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from typing import Any, Literal

from backend.services.cargas.sec import (
    extract_sec,
    normalize_header,
    normalize_search_text,
    parse_number,
)

SourceType = Literal["excel", "pdf"]
LineSeverity = Literal["ok", "warning", "error"]

# Encabezados canónicos del picking operativo (Excel / PDF tabular).
HEADER_ALIASES: dict[str, tuple[str, ...]] = {
    "branch": ("sucursal", "bodega", "oficina"),
    "quantity": ("cantidad", "cant", "unidades", "qty"),
    "product_type": (
        "tipo de producto servicio",
        "tipo de producto / servicio",
        "tipo de producto",
        "tipo producto",
        "tipo",
        "categoria",
        "categoría",
    ),
    "boxes": (
        "cajas x cargar",
        "cajas a cargar",
        "cajas",
        "caja",
    ),
    "product_name": (
        "producto servicio variante",
        "producto / servicio variante",
        "producto servicio + variante",
        "producto / servicio + variante",
        "producto servicio",
        "producto variante",
        "producto",
        "descripcion",
        "descripción",
    ),
    "barcode": (
        "codigo de barras",
        "código de barras",
        "codigo barra",
        "código barra",
        "barcode",
        "ean",
        "sku barcode",
    ),
    "total_value": ("total", "monto", "valor", "importe"),
}


@dataclass
class ParsedLoadLine:
    branch: str | None = None
    product_type: str | None = None
    product_name: str = ""
    barcode: str | None = None
    requested_units: float = 0.0
    source_boxes_value: float | None = None
    total_value: float | None = None
    sec: int | None = None
    normalized_product_name: str = ""
    severity: LineSeverity = "ok"
    messages: list[str] = field(default_factory=list)
    raw: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ParsedLoadPreview:
    source_type: SourceType
    original_filename: str
    picking_number: str | None = None
    picking_date: date | None = None
    destination: str | None = None
    truck: str | None = None
    seal: str | None = None
    branch_default: str | None = None
    document_units_total: float | None = None
    document_value_total: float | None = None
    lines: list[ParsedLoadLine] = field(default_factory=list)
    format_name: str | None = None
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def valid_lines(self) -> list[ParsedLoadLine]:
        return [ln for ln in self.lines if ln.severity != "error" and ln.requested_units > 0]

    @property
    def warning_lines(self) -> list[ParsedLoadLine]:
        return [ln for ln in self.lines if ln.severity == "warning"]

    @property
    def error_lines(self) -> list[ParsedLoadLine]:
        return [ln for ln in self.lines if ln.severity == "error"]

    @property
    def summed_units(self) -> float:
        return round(sum(ln.requested_units for ln in self.valid_lines), 3)

    @property
    def summed_value(self) -> float | None:
        vals = [ln.total_value for ln in self.valid_lines if ln.total_value is not None]
        if not vals:
            return None
        return round(sum(vals), 2)

    def validate_totals(self, *, units_tol: float = 0.001, money_tol: float = 1.0) -> None:
        if self.document_units_total is not None:
            diff = abs(self.summed_units - float(self.document_units_total))
            if diff > units_tol:
                self.errors.append(
                    "La suma de cantidades parseadas "
                    f"({self.summed_units:g}) no coincide con el Total general "
                    f"de unidades del documento ({float(self.document_units_total):g})."
                )
        if self.document_value_total is not None and self.summed_value is not None:
            diff = abs(self.summed_value - float(self.document_value_total))
            if diff > money_tol:
                self.errors.append(
                    "La suma de totales monetarios parseados "
                    f"({self.summed_value:,.0f}) no coincide con el Total general "
                    f"del documento ({float(self.document_value_total):,.0f})."
                )

    def to_dict(self) -> dict[str, Any]:
        valid = self.valid_lines
        warn = self.warning_lines
        err = self.error_lines
        return {
            "source_type": self.source_type,
            "original_filename": self.original_filename,
            "format_name": self.format_name,
            "picking_number": self.picking_number,
            "picking_date": self.picking_date.isoformat() if self.picking_date else None,
            "destination": self.destination,
            "truck": self.truck,
            "seal": self.seal,
            "branch_default": self.branch_default,
            "document_units_total": self.document_units_total,
            "document_value_total": self.document_value_total,
            "summed_units": self.summed_units,
            "summed_value": self.summed_value,
            "total_items": len(valid),
            "valid_count": len(valid),
            "warning_count": len(warn),
            "error_count": len(err),
            "can_import": not self.errors and len(valid) > 0 and len(err) == 0,
            "errors": self.errors,
            "warnings": self.warnings,
            "lines": [ln.to_dict() for ln in self.lines],
        }


def map_headers(raw_headers: list[Any]) -> dict[str, int]:
    """Mapea índices de columna a claves canónicas. Requiere quantity + product_name."""
    mapping: dict[str, int] = {}
    for idx, header in enumerate(raw_headers):
        norm = normalize_header(header)
        if not norm:
            continue
        for key, aliases in HEADER_ALIASES.items():
            if key in mapping:
                continue
            if norm in aliases or any(alias in norm for alias in aliases if len(alias) >= 5):
                # Prefer exact / longer matches; avoid "total" stealing product columns.
                if key == "total_value" and norm not in {
                    "total",
                    "monto",
                    "valor",
                    "importe",
                }:
                    continue
                if key == "boxes" and norm in {"cantidad"}:
                    continue
                mapping[key] = idx
                break
    return mapping


def cell(row: list[Any] | tuple[Any, ...], idx: int | None) -> Any:
    if idx is None or idx < 0 or idx >= len(row):
        return None
    return row[idx]


def build_line_from_mapped_row(row: list[Any], mapping: dict[str, int]) -> ParsedLoadLine:
    product_name = str(cell(row, mapping.get("product_name")) or "").strip()
    barcode_raw = cell(row, mapping.get("barcode"))
    barcode = None
    if barcode_raw is not None:
        barcode = re.sub(r"\D", "", str(barcode_raw).strip()) or None
        # Keep alphanumeric barcodes if stripping digits emptied it
        if barcode is None:
            cleaned = str(barcode_raw).strip()
            barcode = cleaned or None

    qty = parse_number(cell(row, mapping.get("quantity")))
    boxes = parse_number(cell(row, mapping.get("boxes")))
    total_value = parse_number(cell(row, mapping.get("total_value")))
    branch = str(cell(row, mapping.get("branch")) or "").strip() or None
    product_type = str(cell(row, mapping.get("product_type")) or "").strip() or None
    sec = extract_sec(product_name)

    line = ParsedLoadLine(
        branch=branch,
        product_type=product_type,
        product_name=product_name,
        barcode=barcode,
        requested_units=float(qty or 0),
        source_boxes_value=boxes,
        total_value=total_value,
        sec=sec,
        normalized_product_name=normalize_search_text(product_name),
        raw={
            "branch": branch,
            "product_type": product_type,
            "product_name": product_name,
            "barcode": barcode,
            "quantity": qty,
            "boxes": boxes,
            "total_value": total_value,
        },
    )

    if not product_name:
        line.severity = "error"
        line.messages.append("Producto vacío")
    elif qty is None or qty <= 0:
        line.severity = "error"
        line.messages.append("Cantidad inválida o ausente")
    elif not barcode:
        line.severity = "warning"
        line.messages.append("Sin código de barras")
    if sec is None and product_name:
        line.messages.append("Sin (SEC N) en descripción")
        if line.severity == "ok":
            line.severity = "warning"
    return line


def parse_metadata_from_text(text: str) -> dict[str, Any]:
    """Extrae metadatos típicos del encabezado del picking."""
    meta: dict[str, Any] = {}
    # Normalize "Label\nValue" pairs from Excel cells into "Label: Value"
    lines = [ln.strip() for ln in text.splitlines() if ln and str(ln).strip()]
    paired: list[str] = []
    i = 0
    while i < len(lines):
        cur = lines[i]
        nxt = lines[i + 1] if i + 1 < len(lines) else ""
        if re.search(r"(picking|sello|destino|ruta|cami[oó]n|veh[ií]culo|fecha)", cur, re.I) and nxt and not re.search(
            r"(picking|sello|destino|ruta|cami[oó]n|veh[ií]culo|fecha)", nxt, re.I
        ):
            paired.append(f"{cur}: {nxt}")
            i += 2
            continue
        paired.append(cur)
        i += 1
    text = "\n".join(paired)
    patterns = {
        "picking_number": [
            r"N\.?\s*[ºo°]?\s*Picking\s*[:#]?\s*([A-Za-z0-9\-_/]+)",
            r"Picking\s*(?:N[ºo°]?|#)?\s*[:#]?\s*([A-Za-z0-9\-_/]+)",
            r"N[ºo°]?\s*Picking\s*[:#]?\s*([A-Za-z0-9\-_/]+)",
        ],
        "seal": [r"Sello\s*[:#]?\s*([A-Za-z0-9\-_/]+)"],
        "destination": [
            r"Destino\s*(?:/?\s*ruta)?\s*[:#]?\s*(.+)",
            r"Ruta\s*[:#]?\s*(.+)",
        ],
        "truck": [
            r"Cami[oó]n\s*[:#]?\s*(.+)",
            r"Veh[ií]culo\s*[:#]?\s*(.+)",
        ],
        "date": [
            r"Fecha\s*[:#]?\s*(\d{1,2}[-/]\d{1,2}[-/]\d{2,4})",
            r"Fecha\s*[:#]?\s*(\d{4}[-/]\d{1,2}[-/]\d{1,2})",
        ],
        "units_total": [
            r"Total\s+general\s*(?:de\s+)?(?:unidades|cantidad)?\s*[:#]?\s*([\d\.\,]+)",
            r"Total\s+unidades\s*[:#]?\s*([\d\.\,]+)",
            r"Cantidad\s+total\s*[:#]?\s*([\d\.\,]+)",
        ],
        "value_total": [
            r"Total\s+general\s*(?:\$|CLP)?\s*[:#]?\s*\$?\s*([\d\.\,]+)",
            r"Total\s+(?:documento|monto|valor)\s*[:#]?\s*\$?\s*([\d\.\,]+)",
        ],
    }
    for key, regs in patterns.items():
        for pat in regs:
            m = re.search(pat, text, flags=re.IGNORECASE | re.MULTILINE)
            if not m:
                continue
            raw = m.group(1).strip()
            if key == "date":
                for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d", "%d-%m-%y", "%d/%m/%y"):
                    try:
                        meta["picking_date"] = datetime.strptime(raw, fmt).date()
                        break
                    except ValueError:
                        continue
            elif key == "units_total":
                meta["document_units_total"] = parse_number(raw)
            elif key == "value_total":
                meta["document_value_total"] = parse_number(raw)
            elif key == "picking_number":
                meta["picking_number"] = raw
            elif key == "seal":
                meta["seal"] = raw
            elif key == "destination":
                meta["destination"] = raw.splitlines()[0].strip()[:120]
            elif key == "truck":
                meta["truck"] = raw.splitlines()[0].strip()[:120]
            break
    return meta