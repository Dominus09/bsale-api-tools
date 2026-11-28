from uuid import uuid4
import pytest
from pydantic import ValidationError
from backend.services.warehouse.storage_planner import PlanSave, validate_plan, restricted
from backend.services.warehouse.errors import WarehouseValidationError

def payload():
    return {"id":str(uuid4()),"expected_row_version":0,"name":"Escenario",
            "counts":[{"variant_id":10,"source":"BC / IZ","kind":"complete","count":2},
                      {"variant_id":11,"source":"BL / P1","kind":"partial","count":1}],
            "slots":[]}

def slot(variant=10,level=1,kind="complete",method="direct",position=1):
    return {"floor_id":1,"revision_id":1,"revision_version":1,"position_id":position,
            "level":level,"variant_id":variant,"kind":kind,"method":method}

def test_two_complete_same_variant():
    d=payload();d["slots"]=[slot(),slot(level=2)]
    validate_plan(PlanSave(**d).model_dump(mode="json"))

def test_rack_allows_different_variant_and_partial():
    d=payload();d["slots"]=[slot(11,kind="partial",method="rack"),slot(level=2,method="rack")]
    validate_plan(d)

@pytest.mark.parametrize("slots",[
    [slot(),slot()],
    [slot(level=2)],
    [slot(method="floor"),slot(level=2,method="floor")],
    [slot(11,kind="partial")],
    [slot(),slot(11,level=2,kind="partial")],
    [slot(position=1),slot(position=2),slot(position=3)],
])
def test_reject_invalid_slots(slots):
    d=payload();d["slots"]=slots
    with pytest.raises(WarehouseValidationError):validate_plan(d)

def test_duplicate_origin_casefold():
    d=payload();d["counts"].append({**d["counts"][0],"source":" bc / iz "})
    with pytest.raises(WarehouseValidationError):validate_plan(d)

def test_validation_strict():
    d=payload();d["counts"][0]["count"]=1.5
    with pytest.raises(ValidationError):PlanSave(**d)
    d=payload();d["slots"]=[slot(level=3)]
    with pytest.raises(ValidationError):PlanSave(**d)

def test_restricted():
    assert restricted({"code":"BAE-01"})
    assert restricted({"code":"P1","physical_meta_snap":{"under_stair":True}})
    assert not restricted({"code":"IZ-19-D"})
