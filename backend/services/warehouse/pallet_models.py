"""Contrato estricto del registro físico: no acepta stock ni totales del cliente."""
from datetime import date
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, StringConstraints

Positive = Annotated[int, Field(strict=True, gt=0)]
Quantity = Annotated[int, Field(strict=True, ge=0, le=100000000)]
Reason = Annotated[str, StringConstraints(strip_whitespace=True, min_length=3, max_length=1000)]
Note = Annotated[str, StringConstraints(strip_whitespace=True, max_length=2000)]
Lot = Annotated[str, StringConstraints(strip_whitespace=True, max_length=200)]


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ObservedLocation(StrictModel):
    position_id: Positive | None = None
    observed_level: Annotated[int, Field(strict=True, ge=1, le=100)] | None = None
    layout_revision_id: Positive | None = None
    layout_row_version: Positive | None = None
    observed_ack: bool = False


class PalletLineIn(StrictModel):
    variant_id: Positive
    quantity_status: Literal["verified","unknown"]
    boxes: Quantity | None = None
    loose_units: Quantity | None = None
    expected_units_per_box: Positive | None = None
    expiry_status: Literal["dated","unknown","not_applicable"]
    expires_on: date | None = None
    lot: Lot | None = None


class PalletRegisterIn(ObservedLocation):
    registration_key: UUID
    floor_id: Positive
    pallet_kind: Literal["complete","partial","mixed"]
    received_on: date | None = None
    notes: Note | None = None
    lines: list[PalletLineIn] = Field(min_length=1,max_length=100)


class PalletLocationIn(ObservedLocation):
    expected_row_version: Positive
    reason: Reason


class PalletVoidIn(StrictModel):
    expected_row_version: Positive
    reason: Reason
