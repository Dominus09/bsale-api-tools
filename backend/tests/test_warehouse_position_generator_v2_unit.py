"""Generador longitudinal y validación de propuestas sin conexión a BD."""
from __future__ import annotations

import pytest

from backend.services.warehouse.generator import generate_positions
from backend.services.warehouse.geometry import rects_overlap
from backend.services.warehouse.service import WarehouseLayoutService


def rect(x, y, w, d):
    return dict(x_m=x, y_m=y, width_m=w, depth_m=d, rotation_deg=0)


def element(pid, code, kind, geometry, parent=None):
    return {"physical": {"id": pid, "code": code, "element_type": kind, "parent_id": parent},
            "geometry": geometry}


IZ = {**rect(0, 0, 23.1, 2.65), "code": "IZ", "element_type": "sector"}


def config(**kwargs):
    return {
        "parent": IZ, "parent_physical_id": 1, "row_axis": "x",
        "row_counts": [10, 10], "code_suffix_mode": "iz_da",
        "grow_rows": "far_to_near", "anchor": "start", "auto_row_gap": True,
        "gap_longitudinal_m": .05, "margin_start_m": .05, "margin_end_m": 1.26,
        "margin_near_m": .05, "margin_far_m": .05,
        "floor_length_m": 23.1, "floor_width_m": 13.7, **kwargs,
    }


def service():
    def no_db():
        raise AssertionError("preview must not connect to DB")
    return WarehouseLayoutService(no_db)


def test_iz_longitudinal_paired_positions_and_reserve():
    out = generate_positions(config())
    by_code = {p["code"]: p["geometry"] for p in out["positions"]}
    assert out["row_axis"] == "x"
    assert len(by_code) == 20
    assert out["persists"] is False
    assert out["extension_outside_parent_m"] == 0
    for i in range(1, 11):
        d, a = by_code[f"IZ-{i:02d}-D"], by_code[f"IZ-{i:02d}-A"]
        assert d["x_m"] == a["x_m"]
        assert d["y_m"] > a["y_m"]
        assert d["width_m"] == pytest.approx(1.26)
        assert d["depth_m"] == pytest.approx(1.06)
        assert d["x_m"] + d["width_m"] <= 21.84
        assert d["constraints_json"]["stacking_authorized"] is False
        assert d["stack_levels_max"] is None
    assert by_code["IZ-10-D"]["x_m"] > by_code["IZ-01-D"]["x_m"]


def test_iz_rotated_orientation_keeps_axis():
    out = generate_positions(config(default_orientation="rotated"))
    d = out["positions"][0]["geometry"]
    assert d["width_m"] == pytest.approx(1.06)
    assert d["depth_m"] == pytest.approx(1.26)
    assert out["remaining_transverse_m"] >= -1e-6


def test_paired_end_anchor_asymmetric_counts_align_codes():
    out = generate_positions(config(row_counts=[3, 2], anchor="end"))
    by_code = {p["code"]: p["geometry"] for p in out["positions"]}
    assert by_code["IZ-02-D"]["x_m"] == by_code["IZ-02-A"]["x_m"]
    assert by_code["IZ-03-D"]["x_m"] == by_code["IZ-03-A"]["x_m"]


def test_aisle_presets_keep_previous_reference_and_auto_gap():
    out = generate_positions({"parent": {**rect(2.8, 7.35, 2.8, 6), "code": "P1"},
                              "row_axis": "y", "preset": "4+4", "anchor": "end", "auto_row_gap": True})
    assert out["position_count"] == 8
    assert out["interior_gap_m"] == pytest.approx(.68)
    assert out["extension_outside_parent_m"] == 0
    assert out["positions"][0]["geometry"]["depth_m"] == pytest.approx(1.26)


def test_preview_ignores_unrelated_conflicts_and_duplicate_obstacles():
    iz = element(1, "IZ", "sector", IZ)
    esc = element(2, "ESC", "stair", rect(21.58, 4.05, 1.52, 3.2))
    bae = element(3, "BAE-01", "position", rect(21.68, 4.2, 1.26, 1.06), 2)
    su = element(4, "IZ-SU", "loose_storage", rect(21.84, .1, 1.26, 2.12), 1)
    result = service().preview_generate_positions(config(existing_elements=[iz,esc,bae,su], obstacles=[esc]))
    assert result["validation"]["counts"]["geom_conflict"] > 0
    assert not result["obstacle_conflicts"]
    assert result["can_incorporate"] is True


def test_iz_su_overlap_blocks_specific_candidate():
    iz = element(1, "IZ", "sector", IZ)
    su = element(4, "IZ-SU", "loose_storage", rect(.05, .05, 1.26, 1.06), 1)
    result = service().preview_generate_positions(config(existing_elements=[iz,su], obstacles=[su]))
    assert result["can_incorporate"] is False
    assert len(result["obstacle_conflicts"]) == 1
    assert result["obstacle_conflicts"][0]["generated_codes"] == ["IZ-01-A"]
    assert result["blocking_issues"]


def test_duplicate_code_is_not_silently_skipped():
    iz = element(1, "IZ", "sector", IZ)
    old = element(4, "IZ-01-D", "position", rect(18, 1.5, 1.26, 1.06), 1)
    result = service().preview_generate_positions(config(existing_elements=[iz,old]))
    assert result["can_incorporate"] is False
    assert any(i["code"] == "duplicate_code" for i in result["blocking_issues"])


def test_extension_requires_explicit_permission_but_cannot_override_structure():
    p = {**rect(2.8, 7.35, 2.8, 6), "code": "P1"}
    base = dict(parent=p, parent_physical_id=11, preset="4+5", row_axis="y", anchor="end",
                floor_length_m=23.1, floor_width_m=13.7, existing_elements=[element(11,"P1","aisle",p)])
    assert service().preview_generate_positions(base)["can_incorporate"] is False
    permitted = service().preview_generate_positions({**base, "allow_outside_parent": True})
    assert permitted["can_incorporate"] is True
    assert permitted["requires_extension_ack"] is True
    obstruction = element(12, "COL", "pillar", permitted["positions"][-1]["geometry"])
    blocked = service().preview_generate_positions({**base, "allow_outside_parent": True,
                                                     "existing_elements": [*base["existing_elements"], obstruction]})
    assert blocked["can_incorporate"] is False


@pytest.mark.parametrize("patch", [
    {"gap_longitudinal_m": -.1},
    {"row_counts": [0, 2]},
    {"row_counts": [501]},
    {"pallet_length_m": 0},
    {"parent": {**IZ, "rotation_deg": 30}},
])
def test_invalid_inputs_rejected(patch):
    with pytest.raises(ValueError):
        generate_positions(config(**patch))


def test_proposal_geometry_does_not_mutate_context():
    source = config()
    from copy import deepcopy
    before = deepcopy(source)
    out = generate_positions(source)
    assert source == before
    for i, a in enumerate(out["positions"]):
        for b in out["positions"][i+1:]:
            assert not rects_overlap(a["geometry"], b["geometry"])


def test_margin_reserved_even_if_extension_is_allowed():
    iz = element(1, "IZ", "sector", IZ)
    result = service().preview_generate_positions(config(
        row_counts=[17, 17], existing_elements=[iz], allow_outside_parent=True,
    ))
    # 17×1.26 + 16×.05 + .05 = 22.27, within the floor, but
    # not within the reserved 1.26 m rear margin.
    assert result["extension_outside_parent_m"] == 0
    assert result["can_incorporate"] is False
    assert any(i["code"] == "reserved_margin" for i in result["blocking_issues"])


def test_extension_into_another_aisle_is_blocked():
    p = {**rect(2.8, 7.35, 2.8, 6), "code": "P1", "element_type": "aisle"}
    adjacent = element(12, "P2", "aisle", rect(5.6, 7.35, 2.8, 6))
    result = service().preview_generate_positions(dict(
        parent=p, parent_physical_id=11, row_axis="y", row_counts=[1, 1, 1],
        anchor="end", existing_elements=[element(11, "P1", "aisle", p), adjacent],
        floor_length_m=23.1, floor_width_m=13.7, allow_outside_parent=True,
    ))
    assert result["can_incorporate"] is False
    assert any(i["code"] == "other_aisle_collision" for i in result["blocking_issues"])
