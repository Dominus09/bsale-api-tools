"""Levantamiento observado de pallets. Independiente de stock comercial."""
from __future__ import annotations

import hashlib
import json
from contextlib import contextmanager
from datetime import date
from typing import Any, Callable

from psycopg2 import errors

from backend.services.warehouse.errors import (
    WarehouseConflictError, WarehouseError, WarehouseNotFoundError, WarehouseValidationError,
)
from backend.services.warehouse.service import _serialize_row

# Mismo alcance de empresa que el Editor Maestro existente. No aceptar
# company_id desde el cliente. Ampliar alcance mediante permisos explícitos.
COMPANY_ID = 3


class PalletSchemaUnavailable(WarehouseError):
    pass


def pallet_code(pallet_id: int) -> str:
    return f"PAL-{pallet_id:06d}"


def _rows(cur) -> list[dict[str, Any]]:
    names = [d[0] for d in cur.description]
    return [_serialize_row(dict(zip(names, row))) for row in cur.fetchall()]


def _one(cur) -> dict[str, Any] | None:
    names = [d[0] for d in cur.description]
    row = cur.fetchone()
    return _serialize_row(dict(zip(names, row))) if row else None


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def normalize_line(line: dict, product: dict) -> dict:
    """Datos derivados exclusivamente de una variante resuelta por el servidor."""
    pack = product.get("units_per_box")
    pack = pack if isinstance(pack, int) and pack > 0 else None
    quantity = line["quantity_status"]
    boxes, loose = line.get("boxes"), line.get("loose_units")
    if quantity == "unknown":
        if boxes is not None or loose is not None:
            raise WarehouseValidationError("Cantidad desconocida no debe incluir valores numéricos.")
        total = None
    elif quantity == "verified":
        if type(boxes) is not int or type(loose) is not int or min(boxes, loose) < 0:
            raise WarehouseValidationError("Ingrese cajas y unidades contadas, enteras y no negativas.")
        if boxes and (pack is None or line.get("expected_units_per_box") != pack):
            raise WarehouseConflictError("Unidades por caja no verificadas o cambiaron. Consulte la variante nuevamente.", code="pack_changed")
        total = boxes * (pack or 0) + loose
        if total <= 0 or total > 9007199254740991:
            raise WarehouseValidationError("Cantidad verificada fuera del rango representable.")
    else:
        raise WarehouseValidationError("Estado de cantidad inválido.")
    expires = line.get("expires_on")
    expiry = line["expiry_status"]
    if expiry not in ("dated", "unknown", "not_applicable") or (expiry == "dated") != bool(expires):
        raise WarehouseValidationError("Seleccione fecha de vencimiento o indique explícitamente su estado.")
    return {**line, "boxes": boxes, "loose_units": loose, "total_units": total, "units_per_box_snap": pack}


_PRODUCT_SQL = """
SELECT v.bsale_id AS variant_id, v.product_id, p.name AS product_name,
       v.description AS variant_name, pt.name AS product_type,
       v.bar_code AS barcode, v.code AS sku,
       CASE WHEN v.units_per_box > 0 THEN v.units_per_box ELSE NULL END AS units_per_box
FROM bsale.variants v
JOIN bsale.products p ON p.company_id=v.company_id AND p.bsale_id=v.product_id
LEFT JOIN bsale.product_types pt ON pt.company_id=p.company_id AND pt.bsale_id=p.product_type_id
WHERE v.company_id=%s
"""


class PalletService:
    def __init__(self, conn_factory: Callable[[], Any]):
        self.conn_factory = conn_factory

    @contextmanager
    def _transaction(self):
        conn = self.conn_factory()
        try:
            with conn.cursor() as cur:
                cur.execute("""SELECT to_regclass('warehouse.pallets'),
                    to_regclass('warehouse.pallet_lines'),to_regclass('warehouse.pallet_events')""")
                if any(v is None for v in cur.fetchone()):
                    raise PalletSchemaUnavailable("Registro de pallets aún no habilitado: falta migración 008.", code="pallet_schema_missing")
                yield cur
            conn.commit()
        except errors.UniqueViolation as exc:
            conn.rollback()
            raise WarehouseConflictError("La posición/nivel ya tiene un pallet registrado o existe un registro duplicado.", code="occupied_observed_slot") from exc
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _floor(self, cur, floor_id: int):
        cur.execute("""SELECT fl.id AS floor_id, fl.code AS floor_code, f.id AS facility_id,
                         f.code AS facility_code, f.company_id
                       FROM warehouse.floors fl JOIN warehouse.facilities f ON f.id=fl.facility_id
                       WHERE fl.id=%s AND f.company_id=%s AND fl.status='active' AND f.status='active'""",
                    (floor_id, COMPANY_ID))
        row = _one(cur)
        if not row:
            raise WarehouseNotFoundError("Piso no disponible.")
        return row

    def _revision(self, cur, floor_id: int, *, lock=False):
        # En el levantamiento inicial el borrador guardado es la referencia
        # observacional. No se publica ni se considera certificado.
        suffix = " FOR SHARE" if lock else ""
        cur.execute("""SELECT id, status, row_version FROM warehouse.layout_revisions
                       WHERE floor_id=%s AND status IN ('draft','published')
                       ORDER BY CASE status WHEN 'draft' THEN 0 ELSE 1 END, id DESC LIMIT 1""" + suffix,
                    (floor_id,))
        return _one(cur)

    def _positions(self, cur, floor_id, revision_id):
        cur.execute("""SELECT pe.id, le.code_snap AS code, le.name_snap AS name,
                       le.validation_status, le.stack_levels_max,
                       le.constraints_json, le.physical_meta_snap, le.meta_json
                       FROM warehouse.layout_elements le JOIN warehouse.physical_elements pe
                         ON pe.id=le.physical_element_id
                       WHERE le.revision_id=%s AND pe.floor_id=%s AND pe.status='active'
                         AND pe.element_type='position' AND le.element_type_snap='position'
                       ORDER BY le.code_snap""", (revision_id, floor_id))
        return _rows(cur)

    def locations(self, floor_id: int):
        with self._transaction() as cur:
            floor = self._floor(cur, floor_id)
            rev = self._revision(cur, floor_id)
            items = self._positions(cur, floor_id, rev["id"]) if rev else []
            cur.execute("""SELECT position_id, observed_level FROM warehouse.pallets
                           WHERE floor_id=%s AND company_id=%s AND status='active' AND position_id IS NOT NULL""",
                        (floor_id, COMPANY_ID))
            occupied = _rows(cur)
            return {"floor": floor, "revision": rev, "items": items, "observed_slots": occupied}

    def search_products(self, floor_id: int, query: str):
        query = query.strip()
        if len(query) < 2:
            return {"items": []}
        with self._transaction() as cur:
            self._floor(cur, floor_id)
            like = "%" + query.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"
            cur.execute(_PRODUCT_SQL + """ AND (p.name ILIKE %s OR v.description ILIKE %s
                OR v.bar_code ILIKE %s OR v.code ILIKE %s)
                ORDER BY CASE WHEN v.bar_code=%s OR v.code=%s THEN 0 ELSE 1 END, p.name, v.description, v.bsale_id
                LIMIT 40""", (COMPANY_ID, like, like, like, like, query, query))
            return {"items": _rows(cur)}

    def _product(self, cur, variant_id: int):
        cur.execute(_PRODUCT_SQL + " AND v.bsale_id=%s", (COMPANY_ID, variant_id))
        rows = _rows(cur)
        if len(rows) != 1:
            raise WarehouseValidationError("Variante Bsale inválida o ambigua para la empresa.", code="invalid_variant")
        return rows[0]

    def _location(self, cur, floor_id: int, data: dict):
        position_id = data.get("position_id")
        level = data.get("observed_level")
        if position_id is None:
            if level is not None or data.get("layout_revision_id") is not None or data.get("layout_row_version") is not None:
                raise WarehouseValidationError("Sin ubicación no corresponde indicar nivel ni revisión.")
            return (None, None, None, None, None)
        if not data.get("observed_ack") or type(level) is not int or not 1 <= level <= 100:
            raise WarehouseValidationError("Confirme que posición y nivel son observados, no autorizados.")
        rev = self._revision(cur, floor_id, lock=True)
        if not rev or rev["id"] != data.get("layout_revision_id") or rev["row_version"] != data.get("layout_row_version"):
            raise WarehouseConflictError("Cambió la revisión del plano. Recargue ubicaciones.", code="layout_changed")
        # No usar código libre del cliente. El ID tiene que existir en el piso y revisión.
        pos = next((p for p in self._positions(cur, floor_id, rev["id"]) if p["id"] == position_id), None)
        if not pos:
            raise WarehouseValidationError("Ubicación no existe en el plano guardado.", code="invalid_location")
        return (position_id, pos["code"], level, rev["id"], rev["row_version"])

    def _event(self, cur, pallet_id, action, actor, uid, detail):
        cur.execute("""INSERT INTO warehouse.pallet_events
            (pallet_id,action,actor_label,actor_user_id,detail_json) VALUES (%s,%s,%s,%s,%s::jsonb)""",
                    (pallet_id, action, actor, uid, _json(detail)))

    def _get(self, cur, pallet_id: int, *, lock=False):
        cur.execute("""SELECT * FROM warehouse.pallets WHERE id=%s AND company_id=%s""" +
                    (" FOR UPDATE" if lock else ""), (pallet_id, COMPANY_ID))
        row = _one(cur)
        if not row:
            raise WarehouseNotFoundError("Pallet no encontrado.")
        row.pop("registration_key", None)
        row.pop("request_hash", None)
        row["code"] = pallet_code(row["id"])
        cur.execute("SELECT * FROM warehouse.pallet_lines WHERE pallet_id=%s ORDER BY line_number", (pallet_id,))
        row["lines"] = _rows(cur)
        return row

    def get(self, pallet_id: int):
        with self._transaction() as cur:
            row = self._get(cur, pallet_id)
            cur.execute("SELECT id,action,actor_label,detail_json,created_at FROM warehouse.pallet_events WHERE pallet_id=%s ORDER BY id DESC", (pallet_id,))
            row["events"] = _rows(cur)
            return row

    def list(self, floor_id: int, *, query="", limit=100, offset=0):
        with self._transaction() as cur:
            self._floor(cur, floor_id)
            # Filtrar por código pallet, ubicación, lote, nombre/variante/código de barras.
            q = "%" + query.strip().replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"
            cur.execute("""SELECT p.id FROM warehouse.pallets p
                WHERE p.floor_id=%s AND p.company_id=%s
                AND (%s='' OR ('PAL-' || lpad(p.id::text,greatest(6,length(p.id::text)),'0')) ILIKE %s
                  OR p.position_code_snap ILIKE %s OR EXISTS(
                    SELECT 1 FROM warehouse.pallet_lines l WHERE l.pallet_id=p.id AND
                    (l.product_name_snap ILIKE %s OR l.variant_name_snap ILIKE %s OR l.barcode_snap ILIKE %s OR l.lot ILIKE %s)))
                ORDER BY p.id DESC LIMIT %s OFFSET %s""",
                        (floor_id, COMPANY_ID, query.strip(), q,q,q,q,q,q,limit+1,offset))
            ids = [r[0] for r in cur.fetchall()]
            items = [self._get(cur, i) for i in ids[:limit]]
            rev = self._revision(cur, floor_id)
            pos = {p["id"]: p for p in self._positions(cur, floor_id, rev["id"])} if rev else {}
            for item in items:
                current = pos.get(item["position_id"])
                item["location_in_current_layout"] = current is not None if item["position_id"] else None
                item["current_position_code"] = current["code"] if current else None
            return {"items": items, "has_more": len(ids) > limit, "offset": offset}

    def register(self, data: dict, *, actor: str, uid: int | None):
        fingerprint = hashlib.sha256(_json(data).encode()).hexdigest()
        key = str(data["registration_key"])
        with self._transaction() as cur:
            floor = self._floor(cur, data["floor_id"])
            # Serializa reintentos de una misma solicitud incluso si la conexión
            # anterior perdió respuesta después del COMMIT.
            cur.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", ("warehouse:register:"+key,))
            cur.execute("SELECT id,request_hash FROM warehouse.pallets WHERE registration_key=%s::uuid AND company_id=%s", (key, COMPANY_ID))
            previous = _one(cur)
            if previous:
                if previous["request_hash"] != fingerprint:
                    raise WarehouseConflictError("La solicitud ya se registró con otros datos. Revise el listado.", code="registration_key_reused")
                return self._get(cur, previous["id"])
            kind = data["pallet_kind"]
            lines = data["lines"]
            variants = {line["variant_id"] for line in lines}
            if not 1 <= len(lines) <= 100 or kind not in ("complete","partial","mixed"):
                raise WarehouseValidationError("Tipo o líneas del pallet inválidos.")
            if (kind == "mixed" and len(variants) < 2) or (kind != "mixed" and len(variants) != 1):
                raise WarehouseValidationError("Mixto requiere distintas variantes; completo/molido admite varios lotes de una variante.")
            if data.get("received_on"):
                cur.execute("SELECT (now() AT TIME ZONE 'America/Santiago')::date")
                if date.fromisoformat(str(data["received_on"])) > cur.fetchone()[0]:
                    raise WarehouseValidationError("Fecha de ingreso futura.")
            loc = self._location(cur, floor["floor_id"], data)
            normalized = [(normalize_line(line, product), product) for line in lines
                          for product in [self._product(cur, line["variant_id"])]]
            cur.execute("""INSERT INTO warehouse.pallets
              (registration_key,request_hash,company_id,facility_id,floor_id,pallet_kind,received_on,notes,
               position_id,position_code_snap,observed_level,layout_revision_id,layout_row_version,created_by)
              VALUES (%s::uuid,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
                        (key,fingerprint,COMPANY_ID,floor["facility_id"],floor["floor_id"],kind,
                         data.get("received_on"),data.get("notes"),*loc,actor))
            pid = cur.fetchone()[0]
            for i, (line, product) in enumerate(normalized, 1):
                cur.execute("""INSERT INTO warehouse.pallet_lines
                 (pallet_id,line_number,variant_id,product_id,product_name_snap,variant_name_snap,
                  product_type_snap,barcode_snap,sku_snap,units_per_box_snap,quantity_status,boxes,
                  loose_units,total_units,expiry_status,expires_on,lot)
                 VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                            (pid,i,product["variant_id"],product["product_id"],product["product_name"],
                             product["variant_name"],product["product_type"],product["barcode"],product["sku"],
                             line["units_per_box_snap"],line["quantity_status"],line["boxes"],line["loose_units"],
                             line["total_units"],line["expiry_status"],line.get("expires_on"),line.get("lot")))
            saved = self._get(cur, pid)
            self._event(cur,pid,"registered",actor,uid,saved)
            return saved

    def observe_location(self, pallet_id: int, data: dict, *, actor: str, uid: int | None):
        with self._transaction() as cur:
            before = self._get(cur,pallet_id,lock=True)
            if before["status"] != "active" or before["row_version"] != data["expected_row_version"]:
                raise WarehouseConflictError("El pallet cambió o está anulado. Recargue.")
            self._floor(cur,before["floor_id"])
            loc = self._location(cur,before["floor_id"],data)
            cur.execute("""UPDATE warehouse.pallets SET position_id=%s,position_code_snap=%s,observed_level=%s,
                           layout_revision_id=%s,layout_row_version=%s,row_version=row_version+1,updated_at=now()
                           WHERE id=%s""", (*loc,pallet_id))
            after = self._get(cur,pallet_id)
            fields = ("position_id","position_code_snap","observed_level","layout_revision_id","layout_row_version")
            self._event(cur,pallet_id,"location_observed",actor,uid,{"reason":data["reason"],
                "before":{k:before[k] for k in fields},"after":{k:after[k] for k in fields}})
            return after

    def void(self, pallet_id: int, data: dict, *, actor: str, uid: int | None):
        with self._transaction() as cur:
            before = self._get(cur,pallet_id,lock=True)
            if before["status"] != "active" or before["row_version"] != data["expected_row_version"]:
                raise WarehouseConflictError("El pallet cambió o está anulado. Recargue.")
            cur.execute("UPDATE warehouse.pallets SET status='voided',row_version=row_version+1,updated_at=now() WHERE id=%s",(pallet_id,))
            self._event(cur,pallet_id,"voided",actor,uid,{"reason":data["reason"]})
            return self._get(cur,pallet_id)


def default_pallet_service() -> PalletService:
    from backend.db import get_connection
    return PalletService(get_connection)
