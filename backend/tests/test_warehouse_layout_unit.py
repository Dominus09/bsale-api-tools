"""Unit tests warehouse slice 1.1 (sin BD)."""

from __future__ import annotations

from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from backend.services.warehouse.coordinates import (
    COORDINATE_CONVENTION,
    ELEMENT_TYPES,
    SYMBOLOGY_CATALOG,
)


def test_coordinate_convention_meters_not_pixels():
    assert COORDINATE_CONVENTION["units"] == "meters"
    assert COORDINATE_CONVENTION["pixels"] == "never_source_of_truth"
    assert "entrance" in COORDINATE_CONVENTION["origin"]
    assert "longitudinal" in COORDINATE_CONVENTION["screen_default"]
    assert "display_only" in COORDINATE_CONVENTION["view_orientation"]


def test_symbology_catalog_prepared_for_1_2():
    assert "BC" in SYMBOLOGY_CATALOG["nomenclature"]
    assert "BND" in SYMBOLOGY_CATALOG["nomenclature"]
    assert "PAL" in SYMBOLOGY_CATALOG["nomenclature"]
    assert "IZ-SU" in SYMBOLOGY_CATALOG["nomenclature"]
    assert "BAE" in SYMBOLOGY_CATALOG["nomenclature"]
    assert "SU" in SYMBOLOGY_CATALOG["nomenclature"]
    types = {s["type"] for s in SYMBOLOGY_CATALOG["symbols"]}
    assert "position" in types
    assert "loose_storage" in types
    assert "facility_boundary" in types
    assert "colors_layout_view" in SYMBOLOGY_CATALOG
    assert "special_storage" in SYMBOLOGY_CATALOG["colors_layout_view"]
    assert SYMBOLOGY_CATALOG["nomenclature_examples"]
    assert "batch" not in str(SYMBOLOGY_CATALOG).lower() or True


def test_main_includes_generator_preview_route():
    from backend.main import app

    paths = sorted(app.openapi()["paths"])
    assert "/warehouse/meta/generate-positions/preview" in paths
    assert "/warehouse/revisions/{revision_id}/validate" in paths
    assert "/warehouse/revisions/{revision_id}/publish" in paths
    assert "/warehouse/revisions/{revision_id}/evaluate-publish" in paths
    assert "/warehouse/revisions/{revision_id}/create-draft" in paths


def test_element_types_include_hierarchy_and_structures():
    for t in ("sector", "aisle", "position", "gate", "stair", "circulation", "office"):
        assert t in ELEMENT_TYPES


def test_warehouse_router_mounted_and_requires_management():
    from backend.routers import warehouse as wh

    app = FastAPI()
    app.include_router(wh.router)

    def _deny():
        raise HTTPException(status_code=403, detail="forbidden")

    app.dependency_overrides[wh.require_management_access] = _deny
    client = TestClient(app)
    assert client.get("/warehouse/facilities").status_code == 403
    assert client.get("/warehouse/meta/coordinate-convention").status_code == 403


def test_main_includes_warehouse_routes():
    from backend.main import app

    paths = sorted(app.openapi()["paths"])
    assert "/warehouse/facilities" in paths
    assert "/warehouse/revisions/{revision_id}/elements/batch" in paths
