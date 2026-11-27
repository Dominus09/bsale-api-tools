"""Pruebas GET /api/catalog: JOIN products_master sin duplicar barcodes."""

from __future__ import annotations

from collections import Counter

from backend.routers.catalog import _CATALOG_QUERY


def _duplicate_barcodes(products: list[dict]) -> dict[str, int]:
    counts = Counter(
        (p.get("barcode") or "").strip()
        for p in products
        if (p.get("barcode") or "").strip()
    )
    return {bc: n for bc, n in counts.items() if n > 1}


def test_catalog_query_uses_single_winner_per_barcode_and_variant():
    sql = _CATALOG_QUERY
    assert "LEFT JOIN LATERAL" not in sql
    assert "DISTINCT ON (BTRIM(barcode))" in sql
    assert "DISTINCT ON (variant_id)" in sql
    assert "ON pm_bar.id IS NULL" in sql
    assert "pm_var.variant_id = cv.variant_id" in sql
    assert "updated_at DESC NULLS LAST, id DESC" in sql


def test_duplicate_barcodes_helper_detects_duplicates():
    products = [
        {"barcode": "7809562401293", "id": 1},
        {"barcode": "7809562401293", "id": 1},
        {"barcode": "111", "id": 2},
    ]
    assert _duplicate_barcodes(products) == {"7809562401293": 2}


def test_duplicate_barcodes_helper_passes_unique_catalog():
    products = [
        {"barcode": "7809562401293", "id": 1},
        {"barcode": "7809562401330", "id": 2},
    ]
    assert _duplicate_barcodes(products) == {}
