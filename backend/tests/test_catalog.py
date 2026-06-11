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


def test_catalog_query_uses_lateral_pm_join_with_limit():
    sql = _CATALOG_QUERY
    assert "LEFT JOIN LATERAL" in sql
    assert "LIMIT 1" in sql
    assert "pm_inner.barcode) = BTRIM(cv.bar_code)" in sql
    assert "THEN 0" in sql
    assert "THEN 1" in sql
    assert ") pm ON TRUE" in sql
    assert "LEFT JOIN bsale.products_master pm\n    ON pm.variant_id" not in sql


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
