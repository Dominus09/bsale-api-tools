"""Acceso SQL al schema warehouse. Solo usa conexiones inyectadas (tests aislados)."""

from __future__ import annotations

import json
from typing import Any


def _jsonb(value: Any) -> str:
    return json.dumps(value if value is not None else {}, ensure_ascii=False)


class WarehouseRepository:
    def __init__(self, conn):
        self.conn = conn

    def _cur(self):
        return self.conn.cursor()

    def list_facilities(self, *, company_id: int = 3) -> list[dict[str, Any]]:
        cur = self._cur()
        cur.execute(
            """
            SELECT id, code, name, company_id, bsale_office_id, status, meta_json,
                   created_at, updated_at, created_by, updated_by
            FROM warehouse.facilities
            WHERE company_id = %s AND status = 'active'
            ORDER BY code
            """,
            (company_id,),
        )
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        cur.close()
        return rows

    def get_facility(self, facility_id: int) -> dict[str, Any] | None:
        cur = self._cur()
        cur.execute(
            """
            SELECT id, code, name, company_id, bsale_office_id, status, meta_json,
                   created_at, updated_at, created_by, updated_by
            FROM warehouse.facilities WHERE id = %s
            """,
            (facility_id,),
        )
        row = cur.fetchone()
        cols = [d[0] for d in cur.description]
        cur.close()
        return dict(zip(cols, row)) if row else None

    def list_floors(self, facility_id: int) -> list[dict[str, Any]]:
        cur = self._cur()
        cur.execute(
            """
            SELECT id, facility_id, code, name, level_index, length_m, width_m,
                   status, meta_json, created_at, updated_at, created_by, updated_by
            FROM warehouse.floors
            WHERE facility_id = %s AND status = 'active'
            ORDER BY level_index, code
            """,
            (facility_id,),
        )
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        cur.close()
        return rows

    def get_floor(self, floor_id: int) -> dict[str, Any] | None:
        cur = self._cur()
        cur.execute(
            """
            SELECT id, facility_id, code, name, level_index, length_m, width_m,
                   status, meta_json, created_at, updated_at, created_by, updated_by
            FROM warehouse.floors WHERE id = %s
            """,
            (floor_id,),
        )
        row = cur.fetchone()
        cols = [d[0] for d in cur.description]
        cur.close()
        return dict(zip(cols, row)) if row else None

    _REV_COLS = """
            id, floor_id, status, label, row_version, notes,
            published_at, published_by, archived_at, archived_by,
            source_revision_id, publish_meta_json,
            created_at, updated_at, created_by, updated_by
    """

    def list_revisions(self, floor_id: int) -> list[dict[str, Any]]:
        cur = self._cur()
        cur.execute(
            f"""
            SELECT {self._REV_COLS}
            FROM warehouse.layout_revisions
            WHERE floor_id = %s
            ORDER BY
                CASE status WHEN 'draft' THEN 0 WHEN 'published' THEN 1 ELSE 2 END,
                COALESCE(published_at, updated_at) DESC,
                id DESC
            """,
            (floor_id,),
        )
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        cur.close()
        return rows

    def get_revision(self, revision_id: int) -> dict[str, Any] | None:
        cur = self._cur()
        cur.execute(
            f"""
            SELECT {self._REV_COLS}
            FROM warehouse.layout_revisions WHERE id = %s
            """,
            (revision_id,),
        )
        row = cur.fetchone()
        cols = [d[0] for d in cur.description]
        cur.close()
        return dict(zip(cols, row)) if row else None

    def get_draft_for_floor(self, floor_id: int) -> dict[str, Any] | None:
        cur = self._cur()
        cur.execute(
            f"""
            SELECT {self._REV_COLS}
            FROM warehouse.layout_revisions
            WHERE floor_id = %s AND status = 'draft'
            LIMIT 1
            """,
            (floor_id,),
        )
        row = cur.fetchone()
        cols = [d[0] for d in cur.description]
        cur.close()
        return dict(zip(cols, row)) if row else None

    def get_published_for_floor(self, floor_id: int) -> dict[str, Any] | None:
        cur = self._cur()
        cur.execute(
            f"""
            SELECT {self._REV_COLS}
            FROM warehouse.layout_revisions
            WHERE floor_id = %s AND status = 'published'
            LIMIT 1
            """,
            (floor_id,),
        )
        row = cur.fetchone()
        cols = [d[0] for d in cur.description]
        cur.close()
        return dict(zip(cols, row)) if row else None

    def create_draft(
        self,
        *,
        floor_id: int,
        label: str | None,
        actor: str | None,
        source_revision_id: int | None = None,
    ) -> dict[str, Any]:
        cur = self._cur()
        cur.execute(
            f"""
            INSERT INTO warehouse.layout_revisions (
                floor_id, status, label, row_version, created_by, updated_by,
                source_revision_id
            ) VALUES (%s, 'draft', %s, 1, %s, %s, %s)
            RETURNING {self._REV_COLS}
            """,
            (floor_id, label, actor, actor, source_revision_id),
        )
        row = cur.fetchone()
        cols = [d[0] for d in cur.description]
        cur.close()
        return dict(zip(cols, row))

    def bump_revision_version(
        self, revision_id: int, *, expected_version: int, actor: str | None
    ) -> dict[str, Any] | None:
        cur = self._cur()
        cur.execute(
            f"""
            UPDATE warehouse.layout_revisions
            SET row_version = row_version + 1,
                updated_at = now(),
                updated_by = %s
            WHERE id = %s AND status = 'draft' AND row_version = %s
            RETURNING {self._REV_COLS}
            """,
            (actor, revision_id, expected_version),
        )
        row = cur.fetchone()
        cols = [d[0] for d in cur.description] if row else []
        cur.close()
        return dict(zip(cols, row)) if row else None

    def physical_in_revision(self, *, revision_id: int, physical_element_id: int) -> bool:
        cur = self._cur()
        cur.execute(
            """
            SELECT 1 FROM warehouse.layout_elements
            WHERE revision_id = %s AND physical_element_id = %s
            LIMIT 1
            """,
            (revision_id, physical_element_id),
        )
        hit = cur.fetchone() is not None
        cur.close()
        return hit

    def copy_layout_elements(
        self, *, source_revision_id: int, target_revision_id: int
    ) -> int:
        """Copia geometría + snaps. Reutiliza physical_element_id (sin nuevas identidades)."""
        cur = self._cur()
        cur.execute(
            """
            INSERT INTO warehouse.layout_elements (
                revision_id, physical_element_id,
                code_snap, name_snap, element_type_snap,
                parent_physical_id_snap, physical_meta_snap,
                x_m, y_m, elevation_m, width_m, depth_m, height_clear_m,
                rotation_deg, geom_type, polygon_m, stack_levels_max,
                validation_status, constraints_json, observations, meta_json
            )
            SELECT
                %s, physical_element_id,
                code_snap, name_snap, element_type_snap,
                parent_physical_id_snap, physical_meta_snap,
                x_m, y_m, elevation_m, width_m, depth_m, height_clear_m,
                rotation_deg, geom_type, polygon_m, stack_levels_max,
                validation_status, constraints_json, observations, meta_json
            FROM warehouse.layout_elements
            WHERE revision_id = %s
            ON CONFLICT (revision_id, physical_element_id) DO NOTHING
            """,
            (target_revision_id, source_revision_id),
        )
        n = cur.rowcount
        cur.close()
        return int(n or 0)

    def archive_revision(
        self, revision_id: int, *, actor: str | None
    ) -> dict[str, Any] | None:
        cur = self._cur()
        cur.execute(
            f"""
            UPDATE warehouse.layout_revisions
            SET status = 'archived',
                archived_at = now(),
                archived_by = %s,
                updated_at = now(),
                updated_by = %s
            WHERE id = %s AND status = 'published'
            RETURNING {self._REV_COLS}
            """,
            (actor, actor, revision_id),
        )
        row = cur.fetchone()
        cols = [d[0] for d in cur.description] if row else []
        cur.close()
        return dict(zip(cols, row)) if row else None

    def publish_draft_revision(
        self,
        revision_id: int,
        *,
        expected_version: int,
        actor: str | None,
        publish_meta: dict[str, Any],
        source_revision_id: int | None,
    ) -> dict[str, Any] | None:
        cur = self._cur()
        cur.execute(
            f"""
            UPDATE warehouse.layout_revisions
            SET status = 'published',
                published_at = now(),
                published_by = %s,
                updated_at = now(),
                updated_by = %s,
                row_version = row_version + 1,
                publish_meta_json = %s::jsonb,
                source_revision_id = COALESCE(source_revision_id, %s)
            WHERE id = %s AND status = 'draft' AND row_version = %s
            RETURNING {self._REV_COLS}
            """,
            (
                actor,
                actor,
                _jsonb(publish_meta),
                source_revision_id,
                revision_id,
                expected_version,
            ),
        )
        row = cur.fetchone()
        cols = [d[0] for d in cur.description] if row else []
        cur.close()
        return dict(zip(cols, row)) if row else None

    def list_revision_bundle(self, revision_id: int) -> list[dict[str, Any]]:
        """Lee geometría + identidad congelada (*_snap) de la revisión.

        El JOIN a physical_elements aporta solo id/status live (auditoría);
        code/name/type/parent de la respuesta vienen de los snapshots.
        """
        cur = self._cur()
        cur.execute(
            """
            SELECT
                pe.id AS physical_element_id,
                pe.facility_id,
                pe.floor_id,
                pe.status AS physical_status_live,
                pe.meta_json AS physical_meta_live,
                le.code_snap,
                le.name_snap,
                le.element_type_snap,
                le.parent_physical_id_snap,
                le.physical_meta_snap,
                le.id AS layout_element_id,
                le.revision_id,
                le.x_m, le.y_m, le.elevation_m,
                le.width_m, le.depth_m, le.height_clear_m,
                le.rotation_deg, le.geom_type, le.polygon_m,
                le.stack_levels_max, le.validation_status,
                le.constraints_json, le.observations, le.meta_json AS layout_meta_json
            FROM warehouse.layout_elements le
            INNER JOIN warehouse.physical_elements pe
                ON pe.id = le.physical_element_id
            WHERE le.revision_id = %s
            ORDER BY le.element_type_snap, le.code_snap
            """,
            (revision_id,),
        )
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        cur.close()
        return rows

    def get_physical(self, physical_id: int) -> dict[str, Any] | None:
        cur = self._cur()
        cur.execute(
            """
            SELECT id, facility_id, floor_id, parent_id, element_type, code, name,
                   status, retired_at, retired_by, retire_reason, meta_json,
                   created_at, updated_at, created_by, updated_by
            FROM warehouse.physical_elements WHERE id = %s
            """,
            (physical_id,),
        )
        row = cur.fetchone()
        cols = [d[0] for d in cur.description]
        cur.close()
        return dict(zip(cols, row)) if row else None

    def insert_physical(self, fields: dict[str, Any]) -> dict[str, Any]:
        cur = self._cur()
        cur.execute(
            """
            INSERT INTO warehouse.physical_elements (
                facility_id, floor_id, parent_id, element_type, code, name,
                status, meta_json, created_by, updated_by
            ) VALUES (
                %(facility_id)s, %(floor_id)s, %(parent_id)s, %(element_type)s,
                %(code)s, %(name)s, 'active', %(meta_json)s::jsonb,
                %(actor)s, %(actor)s
            )
            RETURNING id, facility_id, floor_id, parent_id, element_type, code, name,
                      status, meta_json, created_at, updated_at, created_by, updated_by
            """,
            {
                **fields,
                "meta_json": _jsonb(fields.get("meta_json")),
            },
        )
        row = cur.fetchone()
        cols = [d[0] for d in cur.description]
        cur.close()
        return dict(zip(cols, row))

    def update_physical(self, physical_id: int, fields: dict[str, Any]) -> dict[str, Any] | None:
        sets = []
        params: list[Any] = []
        for key in ("code", "name", "parent_id", "element_type"):
            if key in fields and fields[key] is not None:
                sets.append(f"{key} = %s")
                params.append(fields[key])
        if "meta_json" in fields and fields["meta_json"] is not None:
            sets.append("meta_json = %s::jsonb")
            params.append(_jsonb(fields["meta_json"]))
        sets.append("updated_at = now()")
        sets.append("updated_by = %s")
        params.append(fields.get("actor"))
        params.append(physical_id)
        cur = self._cur()
        cur.execute(
            f"""
            UPDATE warehouse.physical_elements
            SET {", ".join(sets)}
            WHERE id = %s AND status = 'active'
            RETURNING id, facility_id, floor_id, parent_id, element_type, code, name,
                      status, meta_json, created_at, updated_at, created_by, updated_by
            """,
            params,
        )
        row = cur.fetchone()
        cols = [d[0] for d in cur.description] if row else []
        cur.close()
        return dict(zip(cols, row)) if row else None

    def retire_physical(
        self, physical_id: int, *, actor: str | None, reason: str | None
    ) -> dict[str, Any] | None:
        cur = self._cur()
        cur.execute(
            """
            UPDATE warehouse.physical_elements
            SET status = 'retired',
                retired_at = now(),
                retired_by = %s,
                retire_reason = %s,
                updated_at = now(),
                updated_by = %s
            WHERE id = %s AND status = 'active'
            RETURNING id, facility_id, floor_id, code, name, status, retired_at
            """,
            (actor, reason, actor, physical_id),
        )
        row = cur.fetchone()
        cols = [d[0] for d in cur.description] if row else []
        cur.close()
        return dict(zip(cols, row)) if row else None

    def upsert_layout_element(self, fields: dict[str, Any]) -> dict[str, Any]:
        cur = self._cur()
        cur.execute(
            """
            INSERT INTO warehouse.layout_elements (
                revision_id, physical_element_id,
                code_snap, name_snap, element_type_snap,
                parent_physical_id_snap, physical_meta_snap,
                x_m, y_m, elevation_m, width_m, depth_m, height_clear_m,
                rotation_deg, geom_type, polygon_m, stack_levels_max,
                validation_status, constraints_json, observations, meta_json
            ) VALUES (
                %(revision_id)s, %(physical_element_id)s,
                %(code_snap)s, %(name_snap)s, %(element_type_snap)s,
                %(parent_physical_id_snap)s, %(physical_meta_snap)s::jsonb,
                %(x_m)s, %(y_m)s, %(elevation_m)s, %(width_m)s, %(depth_m)s,
                %(height_clear_m)s, %(rotation_deg)s, %(geom_type)s,
                CASE WHEN %(polygon_m)s IS NULL THEN NULL ELSE %(polygon_m)s::jsonb END,
                %(stack_levels_max)s,
                %(validation_status)s, %(constraints_json)s::jsonb,
                %(observations)s, %(meta_json)s::jsonb
            )
            ON CONFLICT (revision_id, physical_element_id) DO UPDATE SET
                code_snap = EXCLUDED.code_snap,
                name_snap = EXCLUDED.name_snap,
                element_type_snap = EXCLUDED.element_type_snap,
                parent_physical_id_snap = EXCLUDED.parent_physical_id_snap,
                physical_meta_snap = EXCLUDED.physical_meta_snap,
                x_m = EXCLUDED.x_m,
                y_m = EXCLUDED.y_m,
                elevation_m = EXCLUDED.elevation_m,
                width_m = EXCLUDED.width_m,
                depth_m = EXCLUDED.depth_m,
                height_clear_m = EXCLUDED.height_clear_m,
                rotation_deg = EXCLUDED.rotation_deg,
                geom_type = EXCLUDED.geom_type,
                polygon_m = EXCLUDED.polygon_m,
                stack_levels_max = EXCLUDED.stack_levels_max,
                validation_status = EXCLUDED.validation_status,
                constraints_json = EXCLUDED.constraints_json,
                observations = EXCLUDED.observations,
                meta_json = EXCLUDED.meta_json,
                updated_at = now()
            RETURNING id, revision_id, physical_element_id,
                      code_snap, name_snap, element_type_snap,
                      parent_physical_id_snap, physical_meta_snap,
                      x_m, y_m, elevation_m, width_m, depth_m, height_clear_m,
                      rotation_deg, geom_type, polygon_m, stack_levels_max,
                      validation_status, constraints_json, observations, meta_json
            """,
            {
                **fields,
                "polygon_m": _jsonb(fields.get("polygon_m"))
                if fields.get("polygon_m") is not None
                else None,
                "constraints_json": _jsonb(fields.get("constraints_json")),
                "meta_json": _jsonb(fields.get("meta_json")),
                "physical_meta_snap": _jsonb(fields.get("physical_meta_snap")),
            },
        )
        row = cur.fetchone()
        cols = [d[0] for d in cur.description]
        cur.close()
        return dict(zip(cols, row))

    def delete_layout_element(self, *, revision_id: int, physical_element_id: int) -> bool:
        cur = self._cur()
        cur.execute(
            """
            DELETE FROM warehouse.layout_elements
            WHERE revision_id = %s AND physical_element_id = %s
            """,
            (revision_id, physical_element_id),
        )
        n = cur.rowcount
        cur.close()
        return n > 0

    def would_create_cycle(self, *, floor_id: int, node_id: int, new_parent_id: int) -> bool:
        """True si new_parent_id está en la cadena de ancestros de node_id o es node_id."""
        if node_id == new_parent_id:
            return True
        cur = self._cur()
        cur.execute(
            """
            WITH RECURSIVE chain AS (
                SELECT id, parent_id FROM warehouse.physical_elements
                WHERE id = %s AND floor_id = %s
                UNION ALL
                SELECT p.id, p.parent_id
                FROM warehouse.physical_elements p
                INNER JOIN chain c ON p.id = c.parent_id
                WHERE p.floor_id = %s
            )
            SELECT 1 FROM chain WHERE id = %s LIMIT 1
            """,
            (new_parent_id, floor_id, floor_id, node_id),
        )
        hit = cur.fetchone() is not None
        cur.close()
        return hit

    def insert_audit(self, fields: dict[str, Any]) -> None:
        cur = self._cur()
        cur.execute(
            """
            INSERT INTO warehouse.layout_audit (
                revision_id, facility_id, floor_id, physical_element_id,
                action, actor_user_id, actor_label, detail_json
            ) VALUES (
                %(revision_id)s, %(facility_id)s, %(floor_id)s, %(physical_element_id)s,
                %(action)s, %(actor_user_id)s, %(actor_label)s, %(detail_json)s::jsonb
            )
            """,
            {
                **fields,
                "detail_json": _jsonb(fields.get("detail_json")),
            },
        )
        cur.close()

    def list_audit(self, revision_id: int, *, limit: int = 100) -> list[dict[str, Any]]:
        cur = self._cur()
        cur.execute(
            """
            SELECT id, revision_id, facility_id, floor_id, physical_element_id,
                   action, actor_user_id, actor_label, detail_json, created_at
            FROM warehouse.layout_audit
            WHERE revision_id = %s
            ORDER BY created_at DESC, id DESC
            LIMIT %s
            """,
            (revision_id, limit),
        )
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        cur.close()
        return rows
