"""Consultas de Control de Góndolas.

RAW (`bsale_raw.*`) se lee, nunca se escribe. Las únicas escrituras van a las tablas
propias `app.gondola_*` creadas por `backend/sql/gondolas/001_gondola_control.sql`.

La última entrada a una sucursal sale de `bsale_raw.stock_receptions` filtrada por la
oficina de la cabecera; jamás se usa una fecha global por variante.
"""

from __future__ import annotations

from typing import Any, Callable

from backend.services.analytics_v2.contracts import RAW_READY_COMPANY_IDS
from backend.services.analytics_v2.movement_classification import movement_class_sql

QueryExecutor = Callable[[str, tuple[Any, ...]], list[dict[str, Any]]]

_MOVEMENT_CASE = movement_class_sql("r.document", "r.internal_dispatch_id")

TABLES_READY_SQL = "SELECT to_regclass('app.gondola_sessions') IS NOT NULL AS ready"

COMPANIES_SQL = """
SELECT company_id, name
FROM bsale.companies
WHERE active IS TRUE AND company_id = ANY(%s)
ORDER BY company_id
""".strip()

OFFICES_SQL = """
SELECT bsale_id AS office_id, name
FROM bsale_raw.offices
WHERE company_id = %s AND missing_since IS NULL
ORDER BY name, bsale_id
""".strip()

PRODUCT_TYPES_SQL = """
SELECT pt.bsale_id AS product_type_id, pt.name AS product_type_name,
       count(DISTINCT v.bsale_id) AS variant_count
FROM bsale_raw.products p
JOIN bsale.product_types pt
  ON pt.company_id = p.company_id AND pt.bsale_id = p.product_type_id
LEFT JOIN bsale_raw.variants v
  ON v.company_id = p.company_id AND v.product_id = p.bsale_id
 AND v.missing_since IS NULL AND coalesce(v.state, 0) = 0
WHERE p.company_id = %s AND p.missing_since IS NULL AND p.product_type_id IS NOT NULL
GROUP BY pt.bsale_id, pt.name
ORDER BY pt.name, pt.bsale_id
""".strip()

_LAST_ENTRY_CTE = f"""
last_entry AS (
    SELECT DISTINCT ON (d.variant_id)
        d.variant_id,
        r.admission_date AS last_entry_at,
        {_MOVEMENT_CASE} AS last_entry_movement,
        r.bsale_id AS last_entry_reception_id
    FROM bsale_raw.stock_reception_details d
    JOIN bsale_raw.stock_receptions r
      ON r.company_id = d.company_id AND r.bsale_id = d.reception_id
    WHERE d.company_id = %s AND r.office_id = %s
      AND d.missing_since IS NULL AND r.missing_since IS NULL
    ORDER BY d.variant_id, r.admission_date DESC NULLS LAST, r.bsale_id DESC
)
""".strip()

CATALOG_SQL = f"""
WITH {_LAST_ENTRY_CTE}
SELECT
    v.bsale_id AS variant_id,
    v.product_id,
    v.code AS sku_code,
    v.bar_code,
    v.description AS variant_name,
    p.name AS product_name,
    p.product_type_id,
    pt.name AS product_type_name,
    coalesce(s.quantity, 0) AS stock,
    s.quantity_available AS stock_available,
    s.quantity_reserved AS stock_reserved,
    s.api_fetched_at AS stock_fetched_at,
    le.last_entry_at,
    le.last_entry_movement,
    le.last_entry_reception_id
    {{session_columns}}
FROM bsale_raw.variants v
JOIN bsale_raw.products p
  ON p.company_id = v.company_id AND p.bsale_id = v.product_id
LEFT JOIN bsale.product_types pt
  ON pt.company_id = p.company_id AND pt.bsale_id = p.product_type_id
LEFT JOIN bsale_raw.stocks s
  ON s.company_id = v.company_id AND s.variant_id = v.bsale_id AND s.office_id = %s
LEFT JOIN last_entry le ON le.variant_id = v.bsale_id
{{session_join}}
WHERE v.company_id = %s AND p.product_type_id = %s
  AND coalesce(v.state, 0) = 0 AND v.missing_since IS NULL AND p.missing_since IS NULL
""".strip()

SESSION_COLUMNS = """,
    gi.exhibition_status,
    gi.found_at,
    gi.found_by"""
SESSION_JOIN = """
LEFT JOIN app.gondola_session_items gi
  ON gi.session_id = %s AND gi.variant_id = v.bsale_id"""

CREATE_SESSION_SQL = """
INSERT INTO app.gondola_sessions
    (company_id, office_id, product_type_id, created_by, created_by_role, stock_snapshot_at)
VALUES (%s, %s, %s, %s, %s, now())
RETURNING id, company_id, office_id, product_type_id, status,
          created_by, started_at, stock_snapshot_at
""".strip()

FREEZE_ITEMS_SQL = f"""
WITH {_LAST_ENTRY_CTE}
INSERT INTO app.gondola_session_items
    (session_id, company_id, variant_id, product_id, sku_code, bar_code,
     product_name, variant_name, stock_snapshot, stock_available_snapshot,
     last_entry_at, last_entry_movement)
SELECT %s, s.company_id, v.bsale_id, v.product_id, v.code, v.bar_code,
       p.name, v.description, s.quantity, s.quantity_available,
       le.last_entry_at, le.last_entry_movement
FROM bsale_raw.stocks s
JOIN bsale_raw.variants v
  ON v.company_id = s.company_id AND v.bsale_id = s.variant_id
JOIN bsale_raw.products p
  ON p.company_id = v.company_id AND p.bsale_id = v.product_id
LEFT JOIN last_entry le ON le.variant_id = v.bsale_id
WHERE s.company_id = %s AND s.office_id = %s AND p.product_type_id = %s
  AND s.quantity > 0
  AND coalesce(v.state, 0) = 0 AND v.missing_since IS NULL AND p.missing_since IS NULL
ON CONFLICT (session_id, variant_id) DO NOTHING
RETURNING variant_id
""".strip()

SET_SESSION_TOTAL_SQL = """
UPDATE app.gondola_sessions SET total_sku_snapshot = %s WHERE id = %s AND company_id = %s
RETURNING id, total_sku_snapshot
""".strip()

SESSIONS_SQL = """
SELECT s.id, s.company_id, s.office_id, o.name AS office_name,
       s.product_type_id, pt.name AS product_type_name, s.status,
       s.created_by, s.started_at, s.finished_at, s.finished_by,
       s.cancelled_at, s.cancelled_by, s.stock_snapshot_at, s.total_sku_snapshot,
       count(i.id) AS item_count,
       count(i.id) FILTER (WHERE i.exhibition_status = 'encontrado') AS found_count,
       count(i.id) FILTER (WHERE i.exhibition_status = 'pendiente') AS pending_count
FROM app.gondola_sessions s
LEFT JOIN bsale_raw.offices o
  ON o.company_id = s.company_id AND o.bsale_id = s.office_id
LEFT JOIN bsale.product_types pt
  ON pt.company_id = s.company_id AND pt.bsale_id = s.product_type_id
LEFT JOIN app.gondola_session_items i ON i.session_id = s.id
WHERE s.company_id = %s
GROUP BY s.id, o.name, pt.name
ORDER BY s.started_at DESC, s.id DESC
LIMIT %s
""".strip()

SESSION_SQL = """
SELECT s.id, s.company_id, s.office_id, o.name AS office_name,
       s.product_type_id, pt.name AS product_type_name, s.status,
       s.created_by, s.started_at, s.finished_at, s.finished_by,
       s.cancelled_at, s.cancelled_by, s.stock_snapshot_at, s.total_sku_snapshot,
       count(i.id) AS item_count,
       count(i.id) FILTER (WHERE i.exhibition_status = 'encontrado') AS found_count,
       count(i.id) FILTER (WHERE i.exhibition_status = 'pendiente') AS pending_count,
       count(i.id) FILTER (
           WHERE i.stock_at_close IS NOT NULL AND i.stock_at_close <> i.stock_snapshot
       ) AS stock_changed_count
FROM app.gondola_sessions s
LEFT JOIN bsale_raw.offices o
  ON o.company_id = s.company_id AND o.bsale_id = s.office_id
LEFT JOIN bsale.product_types pt
  ON pt.company_id = s.company_id AND pt.bsale_id = s.product_type_id
LEFT JOIN app.gondola_session_items i ON i.session_id = s.id
WHERE s.company_id = %s AND s.id = %s
GROUP BY s.id, o.name, pt.name
""".strip()

SESSION_ITEMS_SQL = """
SELECT i.variant_id, i.product_id, i.sku_code, i.bar_code,
       i.product_name, i.variant_name, i.stock_snapshot,
       i.stock_available_snapshot, i.stock_at_close,
       i.last_entry_at, i.last_entry_movement,
       i.exhibition_status, i.found_at, i.found_by, i.found_device,
       coalesce(s.quantity, 0) AS stock_current,
       s.quantity_available AS stock_available_current
FROM app.gondola_session_items i
LEFT JOIN bsale_raw.stocks s
  ON s.company_id = i.company_id AND s.variant_id = i.variant_id
 AND s.office_id = %s
WHERE i.session_id = %s AND i.company_id = %s
ORDER BY i.product_name, i.variant_name, i.variant_id
""".strip()

RECENT_SCANS_SQL = """
SELECT id, bar_code_raw, variant_id, result, device, scanned_by, scanned_at
FROM app.gondola_scans
WHERE session_id = %s AND company_id = %s
ORDER BY scanned_at DESC, id DESC
LIMIT %s
""".strip()

SCAN_CANDIDATES_SQL = """
SELECT v.bsale_id AS variant_id, v.product_id, v.code AS sku_code,
       v.bar_code, v.description AS variant_name, p.name AS product_name,
       p.product_type_id, coalesce(s.quantity, 0) AS stock
FROM bsale_raw.variants v
JOIN bsale_raw.products p
  ON p.company_id = v.company_id AND p.bsale_id = v.product_id
LEFT JOIN bsale_raw.stocks s
  ON s.company_id = v.company_id AND s.variant_id = v.bsale_id AND s.office_id = %s
WHERE v.company_id = %s AND coalesce(v.state, 0) = 0
  AND v.missing_since IS NULL AND p.missing_since IS NULL
  AND (v.bar_code = %s OR v.code = %s)
ORDER BY CASE WHEN v.bar_code = %s THEN 0 ELSE 1 END, v.bsale_id
""".strip()

SESSION_ITEM_SQL = """
SELECT variant_id, exhibition_status, found_at, found_by
FROM app.gondola_session_items
WHERE session_id = %s AND variant_id = %s AND company_id = %s
""".strip()

MARK_FOUND_SQL = """
UPDATE app.gondola_session_items i
SET exhibition_status = 'encontrado', found_at = now(),
    found_by = %s, found_device = %s
WHERE i.session_id = %s AND i.variant_id = %s AND i.company_id = %s
  AND i.exhibition_status = 'pendiente'
  AND EXISTS (
      SELECT 1 FROM app.gondola_sessions s
      WHERE s.id = i.session_id AND s.company_id = i.company_id AND s.status = 'en_curso'
  )
RETURNING i.variant_id, i.found_at
""".strip()

# Candado transaccional del recorrido: esta actualización toma el bloqueo de la
# fila de la sesión hasta el fin de la transacción del escaneo. Un cierre o una
# cancelación concurrente espera ese bloqueo y, al avanzar, su condición
# `status = 'en_curso'` ya no se cumple si el escaneo cerró primero (y viceversa:
# si el cierre tomó primero el bloqueo, este retorno viene vacío).
TOUCH_ACTIVE_SESSION_SQL = """
UPDATE app.gondola_sessions
SET stock_snapshot_at = stock_snapshot_at
WHERE id = %s AND company_id = %s AND status = 'en_curso'
RETURNING id
""".strip()

INSERT_SCAN_SQL = """
INSERT INTO app.gondola_scans
    (session_id, company_id, bar_code_raw, variant_id, result, device, scanned_by)
VALUES (%s, %s, %s, %s, %s, %s, %s)
RETURNING id, scanned_at
""".strip()

CLOSE_ITEMS_SQL = """
UPDATE app.gondola_session_items i
SET stock_at_close = coalesce(s.quantity, 0)
FROM app.gondola_sessions ses
LEFT JOIN bsale_raw.stocks s
  ON s.company_id = i.company_id AND s.variant_id = i.variant_id
 AND s.office_id = ses.office_id
WHERE i.session_id = ses.id AND ses.id = %s AND i.company_id = %s
RETURNING i.variant_id
""".strip()

CREATE_REPLENISHMENT_SQL = """
INSERT INTO app.gondola_replenishment
    (session_id, company_id, variant_id, stock_snapshot, stock_at_close)
SELECT session_id, company_id, variant_id, stock_snapshot, stock_at_close
FROM app.gondola_session_items
WHERE session_id = %s AND company_id = %s AND exhibition_status = 'pendiente'
ON CONFLICT (session_id, variant_id) DO NOTHING
RETURNING variant_id
""".strip()

# El cierre se reclama ANTES de congelar stock y generar reposición: al pasar la
# sesión a 'cerrado' en la misma transacción, cualquier escaneo concurrente
# queda bloqueado por la fila de la sesión y luego es rechazado; y la lista de
# reposición se genera viendo ya los hallazgos confirmados antes del cierre.
CLAIM_CLOSE_SQL = """
UPDATE app.gondola_sessions
SET status = 'cerrado', finished_at = now(), finished_by = %s
WHERE id = %s AND company_id = %s AND status = 'en_curso'
RETURNING id, status, finished_at
""".strip()

CANCEL_SESSION_SQL = """
UPDATE app.gondola_sessions
SET status = 'cancelado', cancelled_at = now(), cancelled_by = %s, cancel_reason = %s
WHERE id = %s AND company_id = %s AND status = 'en_curso'
RETURNING id, status, cancelled_at
""".strip()

REPLENISHMENT_SQL = """
SELECT r.variant_id, i.sku_code, i.bar_code, i.product_name, i.variant_name,
       r.status, r.note, r.stock_snapshot, r.stock_at_close,
       coalesce(s.quantity, 0) AS stock_current,
       r.updated_by, r.updated_at
FROM app.gondola_replenishment r
JOIN app.gondola_session_items i
  ON i.session_id = r.session_id AND i.variant_id = r.variant_id
LEFT JOIN bsale_raw.stocks s
  ON s.company_id = r.company_id AND s.variant_id = r.variant_id AND s.office_id = %s
WHERE r.session_id = %s AND r.company_id = %s
ORDER BY i.product_name, i.variant_name, r.variant_id
""".strip()

UPDATE_REPLENISHMENT_SQL = """
UPDATE app.gondola_replenishment
SET status = %s, note = %s, updated_by = %s, updated_at = now()
WHERE session_id = %s AND variant_id = %s AND company_id = %s
RETURNING variant_id, status, updated_at
""".strip()


class GondolasRepository:
    def __init__(self, execute: QueryExecutor) -> None:
        self._execute = execute

    def tables_ready(self) -> bool:
        rows = self._execute(TABLES_READY_SQL, ())
        return bool(rows and rows[0].get("ready"))

    def companies(self) -> list[dict[str, Any]]:
        return self._execute(COMPANIES_SQL, (sorted(RAW_READY_COMPANY_IDS),))

    def offices(self, company_id: int) -> list[dict[str, Any]]:
        return self._execute(OFFICES_SQL, (company_id,))

    def product_types(self, company_id: int) -> list[dict[str, Any]]:
        return self._execute(PRODUCT_TYPES_SQL, (company_id,))

    def catalog(self, company_id: int, office_id: int, product_type_id: int,
                session_id: int | None, session_ready: bool) -> list[dict[str, Any]]:
        sql = CATALOG_SQL.format(
            session_columns=SESSION_COLUMNS if (session_id and session_ready) else "",
            session_join=SESSION_JOIN if (session_id and session_ready) else "",
        )
        params: list[Any] = [company_id, office_id, office_id]
        if session_id and session_ready:
            params.append(session_id)
        params.extend([company_id, product_type_id])
        return self._execute(sql, tuple(params))

    def create_session(self, company_id: int, office_id: int, product_type_id: int,
                       actor: str, role: str | None) -> dict[str, Any]:
        rows = self._execute(CREATE_SESSION_SQL, (company_id, office_id, product_type_id, actor, role))
        return rows[0]

    def freeze_items(self, session_id: int, company_id: int, office_id: int,
                     product_type_id: int) -> int:
        rows = self._execute(
            FREEZE_ITEMS_SQL, (company_id, office_id, session_id, company_id, office_id, product_type_id)
        )
        return len(rows)

    def set_session_total(self, session_id: int, company_id: int, total: int) -> None:
        self._execute(SET_SESSION_TOTAL_SQL, (total, session_id, company_id))

    def sessions(self, company_id: int, limit: int) -> list[dict[str, Any]]:
        return self._execute(SESSIONS_SQL, (company_id, limit))

    def session(self, company_id: int, session_id: int) -> dict[str, Any] | None:
        rows = self._execute(SESSION_SQL, (company_id, session_id))
        return rows[0] if rows else None

    def session_items(self, company_id: int, session_id: int, office_id: int) -> list[dict[str, Any]]:
        return self._execute(SESSION_ITEMS_SQL, (office_id, session_id, company_id))

    def recent_scans(self, company_id: int, session_id: int, limit: int) -> list[dict[str, Any]]:
        return self._execute(RECENT_SCANS_SQL, (session_id, company_id, limit))

    def scan_candidates(self, company_id: int, office_id: int, code: str) -> list[dict[str, Any]]:
        return self._execute(SCAN_CANDIDATES_SQL, (office_id, company_id, code, code, code))

    def session_item(self, company_id: int, session_id: int, variant_id: int) -> dict[str, Any] | None:
        rows = self._execute(SESSION_ITEM_SQL, (session_id, variant_id, company_id))
        return rows[0] if rows else None

    def mark_found(self, company_id: int, session_id: int, variant_id: int,
                   actor: str, device: str | None) -> dict[str, Any] | None:
        rows = self._execute(MARK_FOUND_SQL, (actor, device, session_id, variant_id, company_id))
        return rows[0] if rows else None

    def insert_scan(self, company_id: int, session_id: int, code: str,
                    variant_id: int | None, result: str, device: str | None, actor: str) -> None:
        self._execute(INSERT_SCAN_SQL, (session_id, company_id, code, variant_id, result, device, actor))

    def close_items(self, company_id: int, session_id: int) -> None:
        self._execute(CLOSE_ITEMS_SQL, (session_id, company_id))

    def create_replenishment(self, company_id: int, session_id: int) -> int:
        return len(self._execute(CREATE_REPLENISHMENT_SQL, (session_id, company_id)))

    def touch_active_session(self, company_id: int, session_id: int) -> bool:
        """Bloquea la fila de la sesión si sigue en curso (candado del escaneo)."""
        return bool(self._execute(TOUCH_ACTIVE_SESSION_SQL, (session_id, company_id)))

    def claim_close(self, company_id: int, session_id: int, actor: str) -> dict[str, Any] | None:
        rows = self._execute(CLAIM_CLOSE_SQL, (actor, session_id, company_id))
        return rows[0] if rows else None

    def cancel_session(self, company_id: int, session_id: int, actor: str,
                       reason: str | None) -> dict[str, Any] | None:
        rows = self._execute(CANCEL_SESSION_SQL, (actor, reason, session_id, company_id))
        return rows[0] if rows else None

    def replenishment(self, company_id: int, session_id: int, office_id: int) -> list[dict[str, Any]]:
        return self._execute(REPLENISHMENT_SQL, (office_id, session_id, company_id))

    def update_replenishment(self, company_id: int, session_id: int, variant_id: int,
                             status: str, note: str | None, actor: str) -> dict[str, Any] | None:
        rows = self._execute(
            UPDATE_REPLENISHMENT_SQL, (status, note, actor, session_id, variant_id, company_id)
        )
        return rows[0] if rows else None
