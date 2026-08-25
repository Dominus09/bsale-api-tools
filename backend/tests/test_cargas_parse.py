"""Tests parseo Excel/PDF y validación de totales."""

from __future__ import annotations

import io

import pytest

from backend.services.cargas.parse_common import (
    ParsedLoadLine,
    ParsedLoadPreview,
    build_line_from_mapped_row,
    map_headers,
)
from backend.services.cargas.parse_excel import PickingParseError, parse_picking_excel
from backend.services.cargas.parse_pdf import parse_picking_pdf


def test_map_headers_picking_columns():
    headers = [
        "Sucursal",
        "CANTIDAD",
        "Tipo de Producto / Servicio",
        "Cajas x cargar",
        "Producto / Servicio + Variante",
        "Código de Barras",
        "TOTAL",
    ]
    mapping = map_headers(headers)
    assert mapping["quantity"] == 1
    assert mapping["product_name"] == 4
    assert mapping["barcode"] == 5
    assert mapping["boxes"] == 3


def test_build_line_uses_cantidad_not_boxes():
    mapping = {
        "quantity": 0,
        "product_name": 1,
        "barcode": 2,
        "boxes": 3,
    }
    line = build_line_from_mapped_row(
        [480, "CRISTAL LATA 470 CC (SEC 24)", "7802100505323", 20],
        mapping,
    )
    assert line.requested_units == 480
    assert line.source_boxes_value == 20
    assert line.sec == 24
    assert line.barcode == "7802100505323"


def test_validate_totals_blocks_mismatch():
    preview = ParsedLoadPreview(
        source_type="excel",
        original_filename="x.xlsx",
        picking_number="2531",
        document_units_total=100,
        lines=[
            ParsedLoadLine(
                product_name="A (SEC 24)",
                requested_units=90,
                normalized_product_name="a",
            )
        ],
    )
    preview.validate_totals()
    assert preview.errors
    assert preview.to_dict()["can_import"] is False


def test_validate_totals_ok():
    preview = ParsedLoadPreview(
        source_type="pdf",
        original_filename="x.pdf",
        picking_number="2531",
        document_units_total=480,
        document_value_total=14400,
        lines=[
            ParsedLoadLine(
                product_name="CRISTAL (SEC 24)",
                requested_units=480,
                total_value=14400,
                barcode="7802100505323",
                normalized_product_name="cristal",
            )
        ],
    )
    preview.validate_totals()
    assert not preview.errors
    assert preview.to_dict()["can_import"] is True


def test_parse_excel_minimal(tmp_path=None):
    pytest.importorskip("openpyxl")
    import pandas as pd

    buf = io.BytesIO()
    df = pd.DataFrame(
        [
            ["N.º Picking", "2531", None, None, None, None, None],
            ["Destino", "MELINKA", None, None, None, None, None],
            ["Camión", "HINO 4", None, None, None, None, None],
            ["Fecha", "22-08-2026", None, None, None, None, None],
            ["Sello", "ABC123", None, None, None, None, None],
            [
                "Sucursal",
                "CANTIDAD",
                "Tipo de Producto / Servicio",
                "Cajas x cargar",
                "Producto / Servicio + Variante",
                "Código de Barras",
                "TOTAL",
            ],
            [
                "CASTRO",
                480,
                "CERVEZA",
                20,
                "CRISTAL LATA 470 CC (SEC 24)",
                "7802100505323",
                120000,
            ],
            [
                "CASTRO",
                264,
                "BEBIDAS",
                11,
                "COCA COLA LATA 350 CC (SEC 24)",
                "7801610001196",
                80000,
            ],
            [None, None, None, None, "Total general unidades", 744, None],
            [None, None, None, None, "Total general", 200000, None],
        ]
    )
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, header=False)
    preview = parse_picking_excel(data=buf.getvalue(), filename="picking.xlsx")
    assert preview.picking_number == "2531"
    assert preview.destination == "MELINKA"
    assert preview.truck == "HINO 4"
    assert len(preview.valid_lines) == 2
    assert preview.summed_units == 744
    assert preview.valid_lines[0].sec == 24


def test_pdf_without_text_rejected(monkeypatch):
    class FakePage:
        def extract_text(self):
            return ""

        def extract_tables(self):
            return []

    class FakePdf:
        pages = [FakePage()]

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    class FakePlumber:
        @staticmethod
        def open(_buf):
            return FakePdf()

    monkeypatch.setattr(
        "backend.services.cargas.parse_pdf._require_pdfplumber",
        lambda: FakePlumber,
    )
    with pytest.raises(PickingParseError, match="texto seleccionable"):
        parse_picking_pdf(data=b"%PDF-fake", filename="x.pdf")
