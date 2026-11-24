"""Catálogo unificado V2: proyección de lectura RAW + logística PM (preview)."""

from backend.services.catalog_v2.projection import (
    CatalogV2ProjectionError,
    compare_catalog_sample,
    list_catalog_v2,
    raw_sync_integrity_report,
    upb_coverage_report,
)

__all__ = [
    "CatalogV2ProjectionError",
    "compare_catalog_sample",
    "list_catalog_v2",
    "raw_sync_integrity_report",
    "upb_coverage_report",
]
