"""Planning scenarios, isolated from observed pallets and commercial stock."""
from __future__ import annotations

from collections import Counter
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field
from psycopg2.extras import Json

from backend.services.warehouse.errors import WarehouseConflictError, WarehouseNotFoundError, WarehouseValidationError
from backend.services.warehouse.pallets import COMPANY_ID, PalletService, PalletSchemaUnavailable, _one, _rows


class StrictInput(BaseModel):
    model_config = ConfigDict(extra="forbid")


class PlanCount(StrictInput):
    variant_id: int = Field(gt=0, strict=True)
    source: str = Field(min_length=1, max_length=100)
    kind: Literal["complete", "partial"]
    count: int = Field(ge=1, le=1000, strict=True)


class PlanSlot(StrictInput):
    floor_id: int = Field(gt=0, strict=True)
    revision_id: int = Field(gt=0, strict=True)
    revision_version: int = Field(gt=0, strict=True)
    position_id: int = Field(gt=0, strict=True)
    level: int = Field(ge=1, le=2, strict=True)
    variant_id: int = Field(gt=0, strict=True)
    kind: Literal["complete", "partial"]
    method: Literal["floor", "direct", "rack"]


class PlanSave(StrictInput):
    id: UUID
    expected_row_version: int = Field(ge=0, strict=True)
    name: str = Field(min_length=1, max_length=120)
    counts: list[PlanCount] = Field(default_factory=list, max_length=1000)
    slots: list[PlanSlot] = Field(default_factory=list, max_length=1000)


def restricted(position: dict) -> bool:
    meta = {**(position.get("physical_meta_snap") or {}), **(position.get("constraints_json") or {}), **(position.get("meta_json") or {})}
    return (position["code"].startswith("BAE-") or meta.get("under_stair") is True
            or meta.get("storage_kind") == "special_restricted"
            or meta.get("storage_forbidden_assumed") is True or meta.get("not_storage") is True)


def validate_plan(data: dict) -> None:
    """Check conservation of counts, unique slots and simulated stack rules."""
    counts = Counter()
    seen = set()
    for row in data["counts"]:
        source = row["source"].strip()
        if not source:
            raise WarehouseValidationError("Indique el sector de origen.")
        row["source"] = source
        key = (row["variant_id"], row["kind"], source.casefold())
        if key in seen:
            raise WarehouseValidationError("Conteo duplicado para producto, condición y origen.")
        seen.add(key)
        counts[(row["variant_id"], row["kind"])] += row["count"]
    used = Counter()
    by_slot = {}
    for slot in data["slots"]:
        key = (slot["floor_id"], slot["position_id"], slot["level"])
        if key in by_slot:
            raise WarehouseValidationError("Una posición/nivel no puede contener dos pallets planificados.")
        by_slot[key] = slot
        used[(slot["variant_id"], slot["kind"])] += 1
    if any(n > counts[key] for key, n in used.items()):
        raise WarehouseValidationError("La distribución excede el conteo de una variante/condición.")
    for (floor, pos, level), slot in by_slot.items():
        if slot["method"] == "direct" and slot["kind"] != "complete":
            raise WarehouseValidationError("No apilar directamente pallets molidos.")
        if level == 1:
            continue
        lower = by_slot.get((floor, pos, 1))
        if not lower or lower["method"] != slot["method"] or slot["method"] == "floor":
            raise WarehouseValidationError("El nivel 2 requiere base y método de almacenamiento compatible.")
        if slot["method"] == "direct" and (lower["kind"] != "complete" or lower["variant_id"] != slot["variant_id"]):
            raise WarehouseValidationError("Apilamiento directo simulado: misma variante y pallets completos.")


class StoragePlanner(PalletService):
    def _check_table(self, cur):
        cur.execute("SELECT to_regclass('warehouse.storage_plans')")
        if cur.fetchone()[0] is None:
            raise PalletSchemaUnavailable("Planificador no habilitado: falta migración 010.", code="storage_plan_schema_missing")

    def list_plans(self):
        with self._transaction() as cur:
            self._check_table(cur)
            cur.execute("""SELECT id::text AS id, name, row_version, updated_at, updated_by
                           FROM warehouse.storage_plans WHERE company_id=%s
                           ORDER BY updated_at DESC LIMIT 100""", (COMPANY_ID,))
            return {"items": _rows(cur)}

    def get_plan(self, plan_id: UUID):
        with self._transaction() as cur:
            self._check_table(cur)
            cur.execute("""SELECT id::text AS id, name, row_version, document, updated_at, updated_by
                           FROM warehouse.storage_plans WHERE company_id=%s AND id=%s""", (COMPANY_ID, str(plan_id)))
            row = _one(cur)
            if not row:
                raise WarehouseNotFoundError("Plan no encontrado.")
            return row

    def save_plan(self, body: PlanSave, actor: str):
        data = body.model_dump(mode="json")
        name = data["name"].strip()
        if not name:
            raise WarehouseValidationError("Nombre del escenario requerido.")
        validate_plan(data)
        with self._transaction() as cur:
            self._check_table(cur)
            # Resolve current catalog snapshots; client cannot invent product names.
            products = [self._product(cur, vid) for vid in sorted({r["variant_id"] for r in data["counts"]})]
            floors = {}
            for slot in data["slots"]:
                fid = slot["floor_id"]
                if fid not in floors:
                    self._floor(cur, fid)
                    rev = self._revision(cur, fid, lock=True)
                    if not rev:
                        raise WarehouseConflictError("Piso sin plano vigente.")
                    floors[fid] = (rev, {p["id"]: p for p in self._positions(cur, fid, rev["id"])})
                rev, positions = floors[fid]
                if (slot["revision_id"], slot["revision_version"]) != (rev["id"], rev["row_version"]):
                    raise WarehouseConflictError("El plano cambió. Revise y reasigne el piso antes de guardar.", code="plan_layout_changed")
                position = positions.get(slot["position_id"])
                if not position or restricted(position):
                    raise WarehouseValidationError("Posición no disponible para planificación.")
                # Never override a known lower declared stack limit.
                if position["stack_levels_max"] is not None and slot["level"] > position["stack_levels_max"]:
                    raise WarehouseValidationError("Nivel superior al límite declarado de la posición.")
            document = {"counts": data["counts"], "slots": data["slots"], "products": products}
            values = (name, Json(document), actor)
            expected = body.expected_row_version
            if expected == 0:
                cur.execute("""INSERT INTO warehouse.storage_plans(id,company_id,name,document,updated_by)
                               VALUES (%s,%s,%s,%s,%s) ON CONFLICT(id) DO NOTHING
                               RETURNING id::text AS id,name,row_version,document,updated_at,updated_by""",
                            (str(body.id),COMPANY_ID,*values))
            else:
                cur.execute("""UPDATE warehouse.storage_plans SET name=%s,document=%s,updated_by=%s,
                               updated_at=now(),row_version=row_version+1
                               WHERE id=%s AND company_id=%s AND row_version=%s
                               RETURNING id::text AS id,name,row_version,document,updated_at,updated_by""",
                            (*values,str(body.id),COMPANY_ID,expected))
            row = _one(cur)
            if not row:
                raise WarehouseConflictError("Otro usuario modificó el escenario. Recargue o guarde una copia.", code="plan_version_conflict")
            return row


def default_storage_planner():
    from backend.db import get_connection
    return StoragePlanner(get_connection)
