"""Servicio de layout warehouse — slices 1.1–1.4 (incluye publicación formal)."""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Callable

from backend.services.warehouse.coordinates import (
    COORDINATE_CONVENTION,
    ELEMENT_TYPES,
    STORAGE_KINDS,
    SYMBOLOGY_CATALOG,
    VALIDATION_STATUSES,
)
from backend.services.warehouse.generator import PRESETS, generate_positions, verify_reference_calcs
from backend.services.warehouse.validation import validate_layout
from backend.services.warehouse.errors import (
    WarehouseConflictError,
    WarehouseError,
    WarehouseNotFoundError,
    WarehouseValidationError,
)
from backend.services.warehouse.repository import WarehouseRepository


def _f(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    return float(value)


def _serialize_row(row: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in row.items():
        if isinstance(v, Decimal):
            out[k] = float(v)
        elif hasattr(v, "isoformat"):
            out[k] = v.isoformat()
        else:
            out[k] = v
    return out


class WarehouseLayoutService:
    def __init__(self, conn_factory: Callable[[], Any]):
        self._conn_factory = conn_factory

    def _conn(self):
        return self._conn_factory()

    def coordinate_convention(self) -> dict[str, Any]:
        return {
            "convention": COORDINATE_CONVENTION,
            "symbology_catalog": SYMBOLOGY_CATALOG,
            "element_types": sorted(ELEMENT_TYPES),
            "storage_kinds": sorted(STORAGE_KINDS),
            "generator_presets": sorted(k for k in PRESETS.keys()),
            "reference_calcs_ok": verify_reference_calcs(),
            "note_es": (
                "Catálogo de simbología + generador + publicación formal (slice 1.4). "
                "Publicar no certifica seguridad ni autoriza apilamiento. "
                "Vistas de ocupación/FIFO futuras no inventan inventario aquí."
            ),
            "publication": {
                "statuses": ["draft", "published", "archived"],
                "blocks_publish_severities": ["integrity_error", "geom_conflict"],
                "allows_pending_validation": True,
                "safety_certification": False,
            },
        }

    def preview_generate_positions(self, config: dict[str, Any]) -> dict[str, Any]:
        """Vista previa del generador — no escribe BD."""
        try:
            preview = generate_positions(config)
        except ValueError as e:
            raise WarehouseValidationError(str(e), code="generator_config") from e

        obstacles = config.get("obstacles") or []
        existing = config.get("existing_elements") or []
        floor_length = config.get("floor_length_m")
        floor_width = config.get("floor_width_m")

        # existing_elements ya incluye normalmente las estructuras enviadas en
        # obstacles. Deduplicar sólo la misma identidad (o el mismo registro sin ID).
        context = []
        seen = set()
        for el in [*existing, *obstacles]:
            if el.get("pendingRetire") or el.get("pendingRemoveGeometry"):
                continue
            physical = el.get("physical") or el
            pid = physical.get("id")
            key = ("id", pid) if pid is not None else ("anonymous", repr(el))
            if key in seen:
                continue
            seen.add(key)
            context.append(el)

        synthetic = []
        refs = {}
        for index, p in enumerate(preview["positions"]):
            ref = f"generator-preview:{index}"
            refs[ref] = p["code"]
            synthetic.append(
                {
                    "physical": {
                        "id": None,
                        "code": p["code"],
                        "element_type": p["element_type"],
                        "parent_id": config.get("parent_physical_id"),
                    },
                    "geometry": p["geometry"],
                    "clientKey": ref,
                }
            )
        report = validate_layout(
            context + synthetic,
            floor_length_m=float(floor_length) if floor_length is not None else None,
            floor_width_m=float(floor_width) if floor_width is not None else None,
        )
        new_codes = set(refs.values())
        relevant = []
        for issue in report["issues"]:
            codes = set()
            ref = str(issue.get("element_ref"))
            if ref in refs:
                codes.add(refs[ref])
            if issue.get("other_code") in new_codes:
                codes.add(issue["other_code"])
            if issue.get("code") == "duplicate_code" and issue.get("element_code") in new_codes:
                codes.add(issue["element_code"])
            if not codes:
                continue
            relevant.append({**issue, "generated_codes": sorted(codes)})

        # Conservar el informe completo como diagnóstico, pero no atribuir a la
        # generación los conflictos previos de BAE/ESC u otros sectores.
        preview["validation"] = report
        preview["position_issues"] = relevant
        circ = [i for i in relevant if i.get("code") == "circulation_invasion"]
        obst = [i for i in relevant if i.get("code") in ("structure_collision", "storage_overlap")]
        blockers = [i for i in relevant if i.get("severity") in ("geom_conflict", "integrity_error")]
        extension = preview["extension_outside_parent_m"] > 1e-6
        if (extension or circ) and not config.get("allow_outside_parent"):
            blockers.append({
                "severity": "geom_conflict",
                "code": "outside_not_allowed",
                "message": "La propuesta invade circulación o sale del límite del sector. Ajuste las filas/márgenes.",
                "generated_codes": sorted({c for i in circ for c in i["generated_codes"]}),
            })
        preview["obstacle_conflicts"] = obst
        preview["circulation_warnings"] = circ
        preview["blocking_issues"] = blockers
        preview["can_incorporate"] = bool(preview["positions"]) and not blockers
        preview["requires_extension_ack"] = bool(extension or circ)
        return preview

    def validate_revision_elements(
        self,
        revision_id: int,
        *,
        elements_override: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        conn = self._conn()
        try:
            repo = WarehouseRepository(conn)
            rev = repo.get_revision(revision_id)
            if not rev:
                raise WarehouseNotFoundError("Revisión no encontrada")
            floor = repo.get_floor(int(rev["floor_id"]))
            if elements_override is not None:
                elements = elements_override
            else:
                elements = [self._bundle_element(r) for r in repo.list_revision_bundle(revision_id)]
            report = validate_layout(
                elements,
                floor_length_m=_f(floor.get("length_m")) if floor else None,
                floor_width_m=_f(floor.get("width_m")) if floor else None,
            )
            report["revision_id"] = revision_id
            return report
        finally:
            conn.close()

    def list_facilities(self, *, company_id: int = 3) -> list[dict[str, Any]]:
        conn = self._conn()
        try:
            rows = WarehouseRepository(conn).list_facilities(company_id=company_id)
            return [_serialize_row(r) for r in rows]
        finally:
            conn.close()

    def list_floors(self, facility_id: int) -> list[dict[str, Any]]:
        conn = self._conn()
        try:
            repo = WarehouseRepository(conn)
            if not repo.get_facility(facility_id):
                raise WarehouseNotFoundError("Instalación no encontrada")
            return [_serialize_row(r) for r in repo.list_floors(facility_id)]
        finally:
            conn.close()

    def list_revisions(self, floor_id: int) -> list[dict[str, Any]]:
        conn = self._conn()
        try:
            repo = WarehouseRepository(conn)
            if not repo.get_floor(floor_id):
                raise WarehouseNotFoundError("Piso no encontrado")
            return [_serialize_row(r) for r in repo.list_revisions(floor_id)]
        finally:
            conn.close()

    def get_or_create_draft(
        self,
        floor_id: int,
        *,
        label: str | None = None,
        actor: str | None = None,
        actor_user_id: int | None = None,
    ) -> dict[str, Any]:
        conn = self._conn()
        try:
            repo = WarehouseRepository(conn)
            floor = repo.get_floor(floor_id)
            if not floor:
                raise WarehouseNotFoundError("Piso no encontrado")
            draft = repo.get_draft_for_floor(floor_id)
            created = False
            if not draft:
                draft = repo.create_draft(
                    floor_id=floor_id,
                    label=label or f"Borrador {floor['code']}",
                    actor=actor,
                )
                created = True
                repo.insert_audit(
                    {
                        "revision_id": draft["id"],
                        "facility_id": floor["facility_id"],
                        "floor_id": floor_id,
                        "physical_element_id": None,
                        "action": "draft_created",
                        "actor_user_id": actor_user_id,
                        "actor_label": actor,
                        "detail_json": {"label": draft.get("label")},
                    }
                )
            conn.commit()
            return {
                "created": created,
                "revision": _serialize_row(draft),
            }
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def get_revision(self, revision_id: int) -> dict[str, Any]:
        conn = self._conn()
        try:
            repo = WarehouseRepository(conn)
            rev = repo.get_revision(revision_id)
            if not rev:
                raise WarehouseNotFoundError("Revisión no encontrada")
            floor = repo.get_floor(int(rev["floor_id"]))
            facility = repo.get_facility(int(floor["facility_id"])) if floor else None
            elements = [
                self._bundle_element(r) for r in repo.list_revision_bundle(revision_id)
            ]
            return {
                "revision": _serialize_row(rev),
                "floor": _serialize_row(floor) if floor else None,
                "facility": _serialize_row(facility) if facility else None,
                "coordinate_convention": COORDINATE_CONVENTION,
                "elements": elements,
                "element_count": len(elements),
            }
        finally:
            conn.close()

    def _bundle_element(self, row: dict[str, Any]) -> dict[str, Any]:
        """Representación de un elemento en una revisión.

        Identidad visible (code/name/type/parent/meta) proviene de *_snap
        para que published no se vea alterada por edits posteriores al draft.
        """
        s = _serialize_row(row)
        return {
            "physical": {
                "id": s["physical_element_id"],
                "facility_id": s["facility_id"],
                "floor_id": s["floor_id"],
                "parent_id": s.get("parent_physical_id_snap"),
                "element_type": s["element_type_snap"],
                "code": s["code_snap"],
                "name": s["name_snap"],
                "status": s.get("physical_status_live") or "active",
                "meta_json": s.get("physical_meta_snap") or {},
                "identity_source": "revision_snapshot",
            },
            "geometry": {
                "layout_element_id": s.get("layout_element_id"),
                "revision_id": s.get("revision_id"),
                "x_m": s.get("x_m"),
                "y_m": s.get("y_m"),
                "elevation_m": s.get("elevation_m"),
                "width_m": s.get("width_m"),
                "depth_m": s.get("depth_m"),
                "height_clear_m": s.get("height_clear_m"),
                "rotation_deg": s.get("rotation_deg"),
                "geom_type": s.get("geom_type"),
                "polygon_m": s.get("polygon_m"),
                "stack_levels_max": s.get("stack_levels_max"),
                "validation_status": s.get("validation_status"),
                "constraints_json": s.get("constraints_json") or {},
                "observations": s.get("observations"),
                "meta_json": s.get("layout_meta_json") or {},
            },
        }

    def create_element(
        self,
        revision_id: int,
        *,
        payload: dict[str, Any],
        actor: str | None = None,
        actor_user_id: int | None = None,
        expected_row_version: int | None = None,
    ) -> dict[str, Any]:
        return self.apply_batch(
            revision_id,
            upserts=[payload],
            retire_physical_ids=[],
            remove_geometry_physical_ids=[],
            actor=actor,
            actor_user_id=actor_user_id,
            expected_row_version=expected_row_version,
        )

    def apply_batch(
        self,
        revision_id: int,
        *,
        upserts: list[dict[str, Any]] | None = None,
        retire_physical_ids: list[int] | None = None,
        remove_geometry_physical_ids: list[int] | None = None,
        actor: str | None = None,
        actor_user_id: int | None = None,
        expected_row_version: int | None = None,
    ) -> dict[str, Any]:
        """Batch atómico.

        Semántica:
        - ``upserts``: crea/actualiza identidad + geometría listadas.
        - ``retire_physical_ids``: retira identidades (no reutiliza códigos activos).
        - ``remove_geometry_physical_ids``: quita geometría de la revisión draft
          sin retirar la identidad (salvo que también se retire).
        - Elementos ausentes del payload **NO** se eliminan.
        """
        upserts = upserts or []
        retire_physical_ids = retire_physical_ids or []
        remove_geometry_physical_ids = remove_geometry_physical_ids or []

        conn = self._conn()
        try:
            repo = WarehouseRepository(conn)
            rev = repo.get_revision(revision_id)
            if not rev:
                raise WarehouseNotFoundError("Revisión no encontrada")
            if rev["status"] != "draft":
                raise WarehouseConflictError(
                    "Solo se pueden modificar revisiones en estado draft",
                    code="revision_not_editable",
                )
            if expected_row_version is not None and int(rev["row_version"]) != int(
                expected_row_version
            ):
                raise WarehouseConflictError(
                    "Conflicto de concurrencia: row_version desactualizado",
                    code="row_version_conflict",
                )

            floor = repo.get_floor(int(rev["floor_id"]))
            if not floor:
                raise WarehouseNotFoundError("Piso no encontrado")
            facility_id = int(floor["facility_id"])
            floor_id = int(floor["id"])

            results: list[dict[str, Any]] = []

            for item in upserts:
                results.append(
                    self._upsert_one(
                        repo,
                        revision_id=revision_id,
                        floor_id=floor_id,
                        facility_id=facility_id,
                        item=item,
                        actor=actor,
                    )
                )

            for pid in remove_geometry_physical_ids:
                pid_i = int(pid)
                phys = repo.get_physical(pid_i)
                if not phys or int(phys["floor_id"]) != floor_id:
                    raise WarehouseValidationError(
                        f"physical_element_id={pid_i} inválido para el piso",
                        code="invalid_physical_ref",
                    )
                repo.delete_layout_element(
                    revision_id=revision_id, physical_element_id=pid_i
                )
                repo.insert_audit(
                    {
                        "revision_id": revision_id,
                        "facility_id": facility_id,
                        "floor_id": floor_id,
                        "physical_element_id": pid_i,
                        "action": "geometry_removed_from_draft",
                        "actor_user_id": actor_user_id,
                        "actor_label": actor,
                        "detail_json": {},
                    }
                )

            preserved_in_published: list[int] = []
            published = repo.get_published_for_floor(floor_id)
            for pid in retire_physical_ids:
                pid_i = int(pid)
                phys = repo.get_physical(pid_i)
                if not phys or int(phys["floor_id"]) != floor_id:
                    raise WarehouseValidationError(
                        f"physical_element_id={pid_i} inválido para retiro",
                        code="invalid_physical_ref",
                    )
                # Quitar geometría del draft si existe.
                repo.delete_layout_element(
                    revision_id=revision_id, physical_element_id=pid_i
                )
                # No invalidar prematuramente ubicaciones del layout published vigente:
                # si el physical sigue en published, solo se retira del borrador.
                in_published = bool(
                    published
                    and repo.physical_in_revision(
                        revision_id=int(published["id"]), physical_element_id=pid_i
                    )
                )
                if in_published:
                    preserved_in_published.append(pid_i)
                    repo.insert_audit(
                        {
                            "revision_id": revision_id,
                            "facility_id": facility_id,
                            "floor_id": floor_id,
                            "physical_element_id": pid_i,
                            "action": "geometry_removed_from_draft_published_preserved",
                            "actor_user_id": actor_user_id,
                            "actor_label": actor,
                            "detail_json": {
                                "code": phys.get("code"),
                                "published_revision_id": int(published["id"]),
                            },
                        }
                    )
                    continue
                retired = repo.retire_physical(
                    pid_i, actor=actor, reason="retired_from_draft_batch"
                )
                if not retired:
                    raise WarehouseConflictError(
                        f"No se pudo retirar physical_element_id={pid_i}",
                        code="retire_failed",
                    )
                repo.insert_audit(
                    {
                        "revision_id": revision_id,
                        "facility_id": facility_id,
                        "floor_id": floor_id,
                        "physical_element_id": pid_i,
                        "action": "physical_retired",
                        "actor_user_id": actor_user_id,
                        "actor_label": actor,
                        "detail_json": {"code": phys.get("code")},
                    }
                )

            bumped = repo.bump_revision_version(
                revision_id,
                expected_version=int(rev["row_version"]),
                actor=actor,
            )
            if not bumped:
                raise WarehouseConflictError(
                    "Conflicto de concurrencia al guardar el lote",
                    code="row_version_conflict",
                )

            repo.insert_audit(
                {
                    "revision_id": revision_id,
                    "facility_id": facility_id,
                    "floor_id": floor_id,
                    "physical_element_id": None,
                    "action": "batch_saved",
                    "actor_user_id": actor_user_id,
                    "actor_label": actor,
                    "detail_json": {
                        "upserts": len(upserts),
                        "retire_physical_ids": retire_physical_ids,
                        "remove_geometry_physical_ids": remove_geometry_physical_ids,
                        "row_version": bumped["row_version"],
                    },
                }
            )
            conn.commit()
            return {
                "revision": _serialize_row(bumped),
                "upserted": results,
                "retired_physical_ids": [
                    int(x)
                    for x in retire_physical_ids
                    if int(x) not in preserved_in_published
                ],
                "preserved_in_published_physical_ids": preserved_in_published,
                "removed_geometry_physical_ids": [
                    int(x) for x in remove_geometry_physical_ids
                ],
                "note_es": (
                    "Elementos no listados en el batch no se eliminan. "
                    "Identity snaps actualizan solo esta revisión draft. "
                    "Retiro en draft no invalida el layout published vigente."
                ),
            }
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _upsert_one(
        self,
        repo: WarehouseRepository,
        *,
        revision_id: int,
        floor_id: int,
        facility_id: int,
        item: dict[str, Any],
        actor: str | None,
    ) -> dict[str, Any]:
        element_type = (item.get("element_type") or "").strip()
        if element_type not in ELEMENT_TYPES:
            raise WarehouseValidationError(
                f"element_type inválido: {element_type}",
                code="invalid_element_type",
            )
        code = (item.get("code") or "").strip()
        name = (item.get("name") or "").strip()
        if not code or not name:
            raise WarehouseValidationError("code y name son obligatorios")

        geom = item.get("geometry") or item
        try:
            width_m = float(geom["width_m"])
            depth_m = float(geom["depth_m"])
        except (KeyError, TypeError, ValueError) as e:
            raise WarehouseValidationError(
                "geometry.width_m y geometry.depth_m son obligatorios (>0)"
            ) from e
        if width_m <= 0 or depth_m <= 0:
            raise WarehouseValidationError("width_m y depth_m deben ser > 0")

        validation_status = (
            geom.get("validation_status") or item.get("validation_status") or "pending_validation"
        )
        if validation_status not in VALIDATION_STATUSES:
            raise WarehouseValidationError("validation_status inválido")

        physical_id = item.get("physical_element_id")
        parent_id = item.get("parent_id")

        if parent_id is not None:
            parent = repo.get_physical(int(parent_id))
            if not parent or int(parent["floor_id"]) != floor_id or parent["status"] != "active":
                raise WarehouseValidationError(
                    "parent_id inválido o no pertenece al mismo piso",
                    code="invalid_parent",
                )

        if physical_id is None:
            try:
                phys = repo.insert_physical(
                    {
                        "facility_id": facility_id,
                        "floor_id": floor_id,
                        "parent_id": int(parent_id) if parent_id is not None else None,
                        "element_type": element_type,
                        "code": code,
                        "name": name,
                        "meta_json": item.get("meta_json") or {},
                        "actor": actor,
                    }
                )
            except Exception as e:
                # Unique code violation etc.
                raise WarehouseConflictError(
                    f"No se pudo crear identidad física (¿código duplicado?): {e}",
                    code="physical_create_failed",
                ) from e
            physical_id = int(phys["id"])
        else:
            physical_id = int(physical_id)
            phys = repo.get_physical(physical_id)
            if not phys or int(phys["floor_id"]) != floor_id:
                raise WarehouseValidationError(
                    "physical_element_id inválido",
                    code="invalid_physical_ref",
                )
            if phys["status"] != "active":
                raise WarehouseConflictError(
                    "No se puede editar un elemento retirado",
                    code="physical_retired",
                )
            if parent_id is not None and repo.would_create_cycle(
                floor_id=floor_id, node_id=physical_id, new_parent_id=int(parent_id)
            ):
                raise WarehouseValidationError(
                    "La jerarquía formaría un ciclo",
                    code="hierarchy_cycle",
                )
            updated = repo.update_physical(
                physical_id,
                {
                    "code": code,
                    "name": name,
                    "parent_id": int(parent_id) if parent_id is not None else phys.get("parent_id"),
                    "element_type": element_type,
                    "meta_json": item.get("meta_json"),
                    "actor": actor,
                },
            )
            if not updated:
                raise WarehouseConflictError("Actualización de identidad falló")
            phys = updated

        height_clear = geom.get("height_clear_m")
        stack_max = geom.get("stack_levels_max")
        phys_row = phys if isinstance(phys, dict) else repo.get_physical(physical_id)
        parent_snap = (
            int(parent_id)
            if parent_id is not None
            else (int(phys_row["parent_id"]) if phys_row and phys_row.get("parent_id") is not None else None)
        )
        layout = repo.upsert_layout_element(
            {
                "revision_id": revision_id,
                "physical_element_id": physical_id,
                "code_snap": code,
                "name_snap": name,
                "element_type_snap": element_type,
                "parent_physical_id_snap": parent_snap,
                "physical_meta_snap": item.get("meta_json")
                if item.get("meta_json") is not None
                else ((phys_row or {}).get("meta_json") or {}),
                "x_m": float(geom.get("x_m") or 0),
                "y_m": float(geom.get("y_m") or 0),
                "elevation_m": float(geom.get("elevation_m") or 0),
                "width_m": width_m,
                "depth_m": depth_m,
                "height_clear_m": float(height_clear) if height_clear is not None else None,
                "rotation_deg": float(geom.get("rotation_deg") or 0),
                "geom_type": geom.get("geom_type") or "rect",
                "polygon_m": geom.get("polygon_m"),
                "stack_levels_max": int(stack_max) if stack_max is not None else None,
                "validation_status": validation_status,
                "constraints_json": geom.get("constraints_json") or {},
                "observations": geom.get("observations") or item.get("observations"),
                "meta_json": geom.get("meta_json") or {},
            }
        )
        return {
            "physical": _serialize_row(phys_row if isinstance(phys_row, dict) else repo.get_physical(physical_id)),
            "geometry": _serialize_row(layout),
        }

    def list_audit(self, revision_id: int, *, limit: int = 100) -> list[dict[str, Any]]:
        conn = self._conn()
        try:
            repo = WarehouseRepository(conn)
            if not repo.get_revision(revision_id):
                raise WarehouseNotFoundError("Revisión no encontrada")
            return [_serialize_row(r) for r in repo.list_audit(revision_id, limit=limit)]
        finally:
            conn.close()

    def evaluate_publish(
        self,
        revision_id: int,
        *,
        elements_override: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Valida si una revisión draft es publicable. No escribe."""
        report = self.validate_revision_elements(
            revision_id, elements_override=elements_override
        )
        counts = report.get("counts") or {}
        blockers = [
            i
            for i in report.get("issues") or []
            if i.get("severity") in ("integrity_error", "geom_conflict")
        ]
        pending = [
            i
            for i in report.get("issues") or []
            if i.get("severity") == "pending_validation"
        ]
        warnings = [
            i for i in report.get("issues") or [] if i.get("severity") == "warning"
        ]
        can_publish = len(blockers) == 0
        return {
            "revision_id": revision_id,
            "can_publish": can_publish,
            "blockers": blockers,
            "warnings": warnings,
            "pending_validation": pending,
            "counts": counts,
            "note_es": (
                "Publicación bloqueada ante integrity_error o geom_conflict. "
                "pending_validation y advertencias no certifican seguridad ni "
                "autorizan apilamiento/almacenamiento especial (p. ej. BAE)."
                if can_publish
                else "Corrija errores de integridad y conflictos geométricos antes de publicar."
            ),
            "safety_certification": False,
            "stacking_authorized_by_publish": False,
        }

    def publish_revision(
        self,
        revision_id: int,
        *,
        expected_row_version: int | None,
        actor: str | None = None,
        actor_user_id: int | None = None,
        label: str | None = None,
        force_ignore_pending: bool = False,
    ) -> dict[str, Any]:
        """Publica un draft atómicamente. Archiva published previa del mismo piso."""
        _ = force_ignore_pending  # pending nunca bloquea; parámetro documenta intención UI
        evaluation = self.evaluate_publish(revision_id)
        if not evaluation["can_publish"]:
            raise WarehouseValidationError(
                "La revisión no es publicable: hay errores de integridad o conflictos geométricos",
                code="publish_blocked",
            )

        conn = self._conn()
        try:
            repo = WarehouseRepository(conn)
            rev = repo.get_revision(revision_id)
            if not rev:
                raise WarehouseNotFoundError("Revisión no encontrada")
            if rev["status"] != "draft":
                raise WarehouseConflictError(
                    "Solo se puede publicar una revisión en estado draft",
                    code="revision_not_draft",
                )
            if expected_row_version is not None and int(rev["row_version"]) != int(
                expected_row_version
            ):
                raise WarehouseConflictError(
                    "Conflicto de concurrencia: row_version desactualizado",
                    code="row_version_conflict",
                )

            floor = repo.get_floor(int(rev["floor_id"]))
            if not floor:
                raise WarehouseNotFoundError("Piso no encontrado")
            facility = repo.get_facility(int(floor["facility_id"]))
            floor_id = int(floor["id"])
            facility_id = int(floor["facility_id"])

            previous = repo.get_published_for_floor(floor_id)
            archived = None
            if previous:
                archived = repo.archive_revision(int(previous["id"]), actor=actor)
                if not archived:
                    raise WarehouseConflictError(
                        "No se pudo archivar la revisión published previa",
                        code="archive_failed",
                    )
                repo.insert_audit(
                    {
                        "revision_id": int(previous["id"]),
                        "facility_id": facility_id,
                        "floor_id": floor_id,
                        "physical_element_id": None,
                        "action": "revision_archived",
                        "actor_user_id": actor_user_id,
                        "actor_label": actor,
                        "detail_json": {
                            "reason": "superseded_by_publish",
                            "new_revision_id": revision_id,
                        },
                    }
                )

            publish_meta = {
                "facility": {
                    "id": facility_id,
                    "code": (facility or {}).get("code"),
                    "name": (facility or {}).get("name"),
                },
                "floor": {
                    "id": floor_id,
                    "code": floor.get("code"),
                    "name": floor.get("name"),
                    "length_m": _f(floor.get("length_m")),
                    "width_m": _f(floor.get("width_m")),
                },
                "validation_counts": evaluation.get("counts"),
                "pending_validation_count": len(evaluation.get("pending_validation") or []),
                "warning_count": len(evaluation.get("warnings") or []),
                "safety_certification": False,
                "stacking_authorized_by_publish": False,
                "source_draft_id": revision_id,
                "previous_published_id": int(previous["id"]) if previous else None,
            }

            published = repo.publish_draft_revision(
                revision_id,
                expected_version=int(rev["row_version"]),
                actor=actor,
                publish_meta=publish_meta,
                source_revision_id=int(previous["id"]) if previous else None,
            )
            if not published:
                raise WarehouseConflictError(
                    "Conflicto de concurrencia al publicar",
                    code="row_version_conflict",
                )

            if label:
                cur = conn.cursor()
                cur.execute(
                    """
                    UPDATE warehouse.layout_revisions
                    SET label = %s, updated_at = now()
                    WHERE id = %s
                    """,
                    (label, revision_id),
                )
                cur.close()
                published = repo.get_revision(revision_id)

            repo.insert_audit(
                {
                    "revision_id": revision_id,
                    "facility_id": facility_id,
                    "floor_id": floor_id,
                    "physical_element_id": None,
                    "action": "revision_published",
                    "actor_user_id": actor_user_id,
                    "actor_label": actor,
                    "detail_json": {
                        "from_status": "draft",
                        "to_status": "published",
                        "archived_previous_id": int(previous["id"]) if previous else None,
                        "row_version": published["row_version"],
                        "publish_meta": publish_meta,
                    },
                }
            )
            conn.commit()
            return {
                "revision": _serialize_row(published),
                "archived_previous": _serialize_row(archived) if archived else None,
                "evaluation": evaluation,
                "note_es": (
                    "Layout publicado. No constituye certificación de seguridad "
                    "ni autorización de apilamiento o almacenamiento especial."
                ),
            }
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def create_draft_from_revision(
        self,
        source_revision_id: int,
        *,
        label: str | None = None,
        actor: str | None = None,
        actor_user_id: int | None = None,
    ) -> dict[str, Any]:
        """Nuevo borrador desde published/archived. No modifica la revisión histórica.

        Restaurar layout ≠ rollback de migración SQL: solo copia geometría/snaps
        a un draft nuevo reutilizando physical_element_id existentes.
        """
        conn = self._conn()
        try:
            repo = WarehouseRepository(conn)
            source = repo.get_revision(source_revision_id)
            if not source:
                raise WarehouseNotFoundError("Revisión de origen no encontrada")
            if source["status"] == "draft":
                raise WarehouseValidationError(
                    "El origen debe ser published o archived (no otro draft)",
                    code="invalid_source_status",
                )
            floor = repo.get_floor(int(source["floor_id"]))
            if not floor:
                raise WarehouseNotFoundError("Piso no encontrado")
            floor_id = int(floor["id"])
            existing = repo.get_draft_for_floor(floor_id)
            if existing:
                raise WarehouseConflictError(
                    "Ya existe un borrador activo para este piso; "
                    "trabaje sobre él o publíquelo/archívelo antes de restaurar",
                    code="draft_already_exists",
                )

            draft = repo.create_draft(
                floor_id=floor_id,
                label=label
                or f"Borrador desde {source.get('label') or source_revision_id}",
                actor=actor,
                source_revision_id=source_revision_id,
            )
            copied = repo.copy_layout_elements(
                source_revision_id=source_revision_id,
                target_revision_id=int(draft["id"]),
            )
            repo.insert_audit(
                {
                    "revision_id": int(draft["id"]),
                    "facility_id": int(floor["facility_id"]),
                    "floor_id": floor_id,
                    "physical_element_id": None,
                    "action": "draft_created_from_revision",
                    "actor_user_id": actor_user_id,
                    "actor_label": actor,
                    "detail_json": {
                        "source_revision_id": source_revision_id,
                        "source_status": source["status"],
                        "copied_elements": copied,
                        "note_es": (
                            "Restauración de layout: nuevo draft. "
                            "No es rollback de migración ni borra historial."
                        ),
                    },
                }
            )
            draft_id = int(draft["id"])
            draft_row = _serialize_row(draft)
            copied_n = copied
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

        bundle = self.get_revision(draft_id)
        return {
            "created": True,
            "copied_elements": copied_n,
            "source_revision_id": source_revision_id,
            "revision": draft_row,
            "bundle": bundle,
        }


def default_service() -> WarehouseLayoutService:
    from backend.db import get_connection

    return WarehouseLayoutService(get_connection)
