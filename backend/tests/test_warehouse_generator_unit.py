"""Unit tests generador + geometría + validación (sin BD)."""

from __future__ import annotations

from backend.services.warehouse.generator import (
    generate_positions,
    resolve_row_counts,
    verify_reference_calcs,
)
from backend.services.warehouse.geometry import rect_contained_in, rects_overlap
from backend.services.warehouse.validation import validate_layout


def test_reference_calcs_match_prompt():
    for r in verify_reference_calcs():
        assert r["ok"], r


def test_preset_4_plus_5_extension_when_anchored_end():
    parent = {"x_m": 0, "y_m": 0, "width_m": 2.80, "depth_m": 6.00, "code": "P2"}
    out = generate_positions(
        {
            "parent": parent,
            "preset": "4+5",
            "default_orientation": "standard",  # 1.26 along Y
            "anchor": "end",
            "gap_longitudinal_m": 0,
            "gap_transverse_m": 0,
        }
    )
    assert out["position_count"] == 9
    assert out["persists"] is False
    assert abs(out["extension_outside_parent_m"] - 0.30) < 1e-6
    # 2,80 − 2×1,06 = 0,68 (holgura transversal nominal del pasillo)
    assert abs(out["remaining_transverse_m"] - 0.68) < 1e-6


def test_rotated_5_plus_5_fits_and_6_plus_6_extends():
    parent = {"x_m": 0, "y_m": 0, "width_m": 2.80, "depth_m": 6.00, "code": "P2"}
    fit = generate_positions(
        {
            "parent": parent,
            "preset": "5+5",
            "default_orientation": "rotated",
            "anchor": "start",
        }
    )
    assert fit["extension_outside_parent_m"] == 0
    assert abs(fit["remaining_transverse_m"] - 0.28) < 1e-6

    ext = generate_positions(
        {
            "parent": parent,
            "preset": "6+6",
            "default_orientation": "rotated",
            "anchor": "end",
        }
    )
    assert abs(ext["extension_outside_parent_m"] - 0.36) < 1e-6


def test_asymmetric_custom_and_independent_row_orientations():
    parent = {"x_m": 1.5, "y_m": 7.35, "width_m": 2.80, "depth_m": 6.00, "code": "P3"}
    out = generate_positions(
        {
            "parent": parent,
            "preset": "custom",
            "row_counts": [3, 5],
            "row_orientations": ["standard", "rotated"],
            "code_prefix": "P3",
        }
    )
    assert out["position_count"] == 8
    assert out["positions"][0]["geometry"]["depth_m"] == 1.26
    assert out["positions"][3]["geometry"]["depth_m"] == 1.06  # first of row1 rotated


def test_iz_da_suffix_mode():
    parent = {"x_m": 0, "y_m": 0, "width_m": 23.1, "depth_m": 2.65, "code": "IZ"}
    out = generate_positions(
        {
            "parent": parent,
            "row_counts": [2, 2],
            "code_prefix": "IZ",
            "code_suffix_mode": "iz_da",
            "default_orientation": "rotated",
        }
    )
    codes = [p["code"] for p in out["positions"]]
    assert "IZ-01-D" in codes
    assert any(c.endswith("-A") for c in codes)


def test_generator_does_not_mutate_existing_implicitly():
    # Solo preview; no hay side effects de identidad
    parent = {"x_m": 0, "y_m": 0, "width_m": 2.8, "depth_m": 6, "code": "P1"}
    a = generate_positions({"parent": parent, "preset": "4+4"})
    b = generate_positions({"parent": parent, "preset": "4+4"})
    assert a["position_count"] == b["position_count"] == 8


def test_rotated_overlap_detection():
    a = {"x_m": 0, "y_m": 0, "width_m": 2, "depth_m": 1, "rotation_deg": 0}
    b = {"x_m": 1.5, "y_m": 0, "width_m": 2, "depth_m": 1, "rotation_deg": 0}
    assert rects_overlap(a, b)
    c = {"x_m": 3, "y_m": 0, "width_m": 1, "depth_m": 1, "rotation_deg": 45}
    # Contained check
    outer = {"x_m": 0, "y_m": 0, "width_m": 10, "depth_m": 10, "rotation_deg": 0}
    assert rect_contained_in(a, outer)


def test_validate_distinguishes_hierarchy_and_conflict():
    elements = [
        {
            "physical": {"id": 1, "code": "P2", "element_type": "aisle", "parent_id": None},
            "geometry": {"x_m": 0, "y_m": 0, "width_m": 2.8, "depth_m": 6, "rotation_deg": 0},
        },
        {
            "physical": {"id": 2, "code": "P2-01", "element_type": "position", "parent_id": 1},
            "geometry": {
                "x_m": 0.2,
                "y_m": 0.2,
                "width_m": 1.06,
                "depth_m": 1.26,
                "rotation_deg": 0,
                "validation_status": "pending_validation",
            },
        },
        {
            "physical": {"id": 3, "code": "P2-02", "element_type": "position", "parent_id": 1},
            "geometry": {
                "x_m": 0.2,
                "y_m": 0.2,
                "width_m": 1.06,
                "depth_m": 1.26,
                "rotation_deg": 0,
            },
        },
    ]
    report = validate_layout(elements, floor_length_m=23.1, floor_width_m=13.7)
    assert report["counts"]["geom_conflict"] >= 1
    assert any(i["code"] == "storage_overlap" for i in report["issues"])
    # Contained position vs aisle should not be flagged as storage_overlap with aisle
    assert not any(
        i["code"] == "storage_overlap" and i.get("other_code") == "P2" for i in report["issues"]
    )


def test_resolve_row_counts_rejects_empty_custom():
    try:
        resolve_row_counts(preset="custom", row_counts=[])
        assert False, "expected ValueError"
    except ValueError:
        pass
