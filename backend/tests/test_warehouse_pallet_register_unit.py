"""Pruebas puras; no conectan PostgreSQL."""
import pytest
from pydantic import ValidationError

from backend.services.warehouse.pallets import normalize_line, pallet_code
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
