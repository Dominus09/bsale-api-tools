"""Pruebas puras; no conectan PostgreSQL."""
from datetime import date, datetime, timezone

import pytest
from pydantic import ValidationError

from backend.services.warehouse.pallets import chile_business_date, normalize_line, pallet_code
from backend.services.warehouse.pallet_models import PalletLineIn, PalletRegisterIn
from backend.services.warehouse.errors import WarehouseConflictError, WarehouseValidationError


PRODUCT = {"units_per_box": 24}
BASE = {"variant_id": 123, "quantity_status": "verified", "boxes": 3,
        "loose_units": 2, "expected_units_per_box": 24, "expiry_status": "unknown"}


def test_identity_has_minimum_six_digits_and_never_truncates():
    assert pallet_code(1) == "PAL-000001"
    assert pallet_code(1000001) == "PAL-1000001"


def test_units_are_calculated_from_server_pack():
    result = normalize_line(BASE, PRODUCT)
    assert result["total_units"] == 74
    assert result["units_per_box_snap"] == 24


def test_unknown_quantity_is_not_zero():
    result = normalize_line({**BASE, "quantity_status": "unknown", "boxes": None, "loose_units": None}, PRODUCT)
    assert result["total_units"] is None
    assert result["boxes"] is None
    assert result["loose_units"] is None


def test_units_only_without_pack():
    result = normalize_line({**BASE, "boxes": 0, "loose_units": 31, "expected_units_per_box": None}, {"units_per_box": None})
    assert result["total_units"] == 31
    assert result["units_per_box_snap"] is None


@pytest.mark.parametrize("line,product,error", [
    ({**BASE, "boxes": 0, "loose_units": 0}, PRODUCT, WarehouseValidationError),
    ({**BASE, "boxes": -1}, PRODUCT, WarehouseValidationError),
    ({**BASE, "quantity_status": "unknown"}, PRODUCT, WarehouseValidationError),
    ({**BASE, "expected_units_per_box": 12}, PRODUCT, WarehouseConflictError),
    (BASE, {"units_per_box": None}, WarehouseConflictError),
    ({**BASE, "expiry_status": "dated"}, PRODUCT, WarehouseValidationError),
    ({**BASE, "expires_on": "2027-01-01"}, PRODUCT, WarehouseValidationError),
])
def test_bad_line_rejected(line,product,error):
    with pytest.raises(error):
        normalize_line(line,product)


def test_strict_payload_rejects_user_totals_and_fractional_quantities():
    with pytest.raises(ValidationError):
        PalletLineIn(**{**BASE, "total_units": 10000})
    with pytest.raises(ValidationError):
        PalletLineIn(**{**BASE, "boxes": 1.5})
    with pytest.raises(ValidationError):
        PalletRegisterIn(registration_key="not-a-uuid",floor_id=1,pallet_kind="complete",lines=[BASE])


@pytest.mark.parametrize("instant,expected", [
    (datetime(2026, 1, 15, 2, 30, tzinfo=timezone.utc), date(2026, 1, 14)),
    (datetime(2026, 1, 15, 3, 30, tzinfo=timezone.utc), date(2026, 1, 15)),
    (datetime(2026, 7, 15, 3, 30, tzinfo=timezone.utc), date(2026, 7, 14)),
    (datetime(2026, 7, 15, 4, 30, tzinfo=timezone.utc), date(2026, 7, 15)),
])
def test_chile_business_date_handles_summer_and_winter(instant, expected):
    assert chile_business_date(instant) == expected


def test_chile_business_date_rejects_naive_datetime():
    with pytest.raises(ValueError):
        chile_business_date(datetime(2026, 1, 15, 2, 30))


def test_edit_payload_does_not_accept_location_or_calculated_totals():
    from backend.services.warehouse.pallet_models import PalletDetailsIn
    payload={"expected_row_version":1,"reason":"Conteo físico","pallet_kind":"complete",
             "lines":[{**BASE,"line_id":1}],"remove_line_ids":[]}
    assert PalletDetailsIn(**payload).lines[0].line_id == 1
    with pytest.raises(ValidationError):
        PalletDetailsIn(**{**payload,"position_id":123})
    with pytest.raises(ValidationError):
        PalletDetailsIn(**{**payload,"lines":[{**BASE,"line_id":1,"total_units":500}]})


def _batch_payload(**patch):
    from uuid import uuid4
    return {"batch_key":str(uuid4()),"floor_id":1,"layout_revision_id":1,
            "layout_row_version":6,"observed_level":1,"observed_ack":True,
            "items":[{"position_id":i,"pallet_kind":"complete","lines":[BASE]} for i in (1,2)],**patch}


def test_batch_contract_requires_unique_slots_and_ack():
    from backend.services.warehouse.pallet_models import PalletBatchIn
    payload=_batch_payload()
    assert len(PalletBatchIn(**payload).items)==2
    for change in ({"observed_ack":False},{"observed_ack":"true"},{"observed_level":0},
                   {"observed_level":1.5},{"items":[payload["items"][0]]},
                   {"items":[payload["items"][0]]*2},
                   {"items":[{**payload["items"][0],"position_id":i} for i in range(1,122)]},
                   {"items":[{**payload["items"][0],"position_id":i,"lines":[BASE]*6} for i in range(1,101)]},
                   {"company_id":3},{"items":[{**payload["items"][0],"total_units":100},payload["items"][1]]}):
        with pytest.raises(ValidationError):
            PalletBatchIn(**{**payload,**change})


def test_batch_contract_keeps_per_pallet_exceptions():
    from backend.services.warehouse.pallet_models import PalletBatchIn
    p=_batch_payload()
    p["items"][1]["lines"]=[{**BASE,"quantity_status":"unknown","boxes":None,"loose_units":None}]
    p["items"][1]["received_on"]="2026-11-01"
    p["items"][1]["pallet_kind"]="partial"
    result=PalletBatchIn(**p).model_dump(mode="json")
    assert result["items"][0]["lines"][0]["boxes"]==3
    assert result["items"][1]["lines"][0]["boxes"] is None
    assert result["items"][1]["received_on"]=="2026-11-01"
