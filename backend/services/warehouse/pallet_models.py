"""Contrato estricto del registro físico: no acepta stock ni totales del cliente."""
from datetime import date
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, model_validator

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


class PalletEditLineIn(PalletLineIn):
    line_id: Positive | None = None


class PalletDetailsIn(StrictModel):
    expected_row_version: Positive
    reason: Reason
    pallet_kind: Literal["complete","partial","mixed"]
    received_on: date | None = None
    notes: Note | None = None
    lines: list[PalletEditLineIn] = Field(min_length=1,max_length=100)
    remove_line_ids: list[Positive] = Field(default_factory=list,max_length=100)


# Lote de registro (no confundir con lote de fabricación).
# Una solicitud registra varias identidades físicas, todas en un mismo piso/nivel.
MAX_BATCH_PALLETS = 120
MAX_BATCH_LINES = 500


class PalletBatchItemIn(StrictModel):
    position_id: Positive
    pallet_kind: Literal["complete","partial","mixed"]
    received_on: date | None = None
    notes: Note | None = None
    lines: list[PalletLineIn] = Field(min_length=1,max_length=100)


class PalletBatchIn(StrictModel):
    batch_key: UUID
    floor_id: Positive
    layout_revision_id: Positive
    layout_row_version: Positive
    observed_level: Annotated[int, Field(strict=True, ge=1, le=100)]
    observed_ack: Annotated[bool, Field(strict=True)]
    items: list[PalletBatchItemIn] = Field(min_length=2,max_length=MAX_BATCH_PALLETS)

    @model_validator(mode="after")
    def validate_batch(self):
        if not self.observed_ack:
            raise ValueError("Confirme físicamente todas las posiciones y el nivel.")
        if len({p.position_id for p in self.items}) != len(self.items):
            raise ValueError("No se permiten posiciones duplicadas en un mismo nivel.")
        if sum(len(p.lines) for p in self.items) > MAX_BATCH_LINES:
            raise ValueError(f"Máximo {MAX_BATCH_LINES} líneas por registro masivo.")
        return self


MAX_LABEL_PALLETS = 120


class PalletLabelsIn(StrictModel):
    floor_id: Positive
    pallet_ids: list[Positive] = Field(min_length=1,max_length=MAX_LABEL_PALLETS)

    @model_validator(mode="after")
    def unique_ids(self):
        if len(set(self.pallet_ids)) != len(self.pallet_ids):
            raise ValueError("Seleccione identidades de pallet distintas.")
        return self
