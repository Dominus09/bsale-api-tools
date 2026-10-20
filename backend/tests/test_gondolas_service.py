"""Control de Góndolas: clasificación de escaneos y capacidades por rol (sin base de datos)."""

import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from backend.services.analytics_v2.access import CompanyScope
from backend.services.gondolas.access import gondola_capabilities
from backend.services.gondolas.service import (
    GondolasService,
    GondolasValidationError,
    classify_scan,
)

CANDIDATE = {
    "variant_id": 10,
    "product_id": 5,
    "product_type_id": 35,
    "stock": Decimal("3"),
    "product_name": "Vino Reserva",
    "variant_name": "750 cc",
}


def test_scan_sin_coincidencia():
    result, candidate = classify_scan([], None, product_type_id=35)
    assert result == "sin_coincidencia"
    assert candidate is None


def test_scan_ambiguo_no_elige_variante():
    other = {**CANDIDATE, "variant_id": 11}
    result, candidate = classify_scan([CANDIDATE, other], None, product_type_id=35)
    assert result == "ambiguo"
    assert candidate is None


def test_scan_encontrado_y_duplicado():
    result, _ = classify_scan([CANDIDATE], {"exhibition_status": "pendiente"}, product_type_id=35)
    assert result == "encontrado"
    result, _ = classify_scan([CANDIDATE], {"exhibition_status": "encontrado"}, product_type_id=35)
    assert result == "duplicado"


def test_scan_fuera_de_tipo_y_stock_cero():
    other_type = {**CANDIDATE, "product_type_id": 16}
    result, _ = classify_scan([other_type], None, product_type_id=35)
    assert result == "fuera_de_tipo"

    no_stock = {**CANDIDATE, "stock": Decimal("0")}
    result, _ = classify_scan([no_stock], None, product_type_id=35)
    assert result == "stock_cero"


def test_scan_fuera_de_universo_con_stock():
    result, candidate = classify_scan([CANDIDATE], None, product_type_id=35)
    assert result == "fuera_de_universo"
    assert candidate is CANDIDATE


def test_capacidades_por_rol():
    assert gondola_capabilities({"role": "operativo"}) == {"operate": True, "cancel": False}
    assert gondola_capabilities({"role": "adm"}) == {"operate": True, "cancel": True}
    assert gondola_capabilities({"role": "gerencia"}) == {"operate": True, "cancel": True}
    assert gondola_capabilities({"role": "finanzas"}) == {"operate": False, "cancel": False}
    assert gondola_capabilities(None) == {"operate": False, "cancel": False}


# ---------------------------------------------------------------- concurrencia
#
# Repositorio en memoria que emula la semántica transaccional del SQL real:
# - `touch_active_session` solo prospera si la sesión sigue en curso;
# - `mark_found` es una actualización condicional atómica (pendiente -> encontrado)
#   que además exige sesión en curso, igual que el EXISTS de MARK_FOUND_SQL;
# - `claim_close` reclama el cierre antes de generar la reposición.

SNAPSHOT_AT = datetime(2026, 10, 20, 12, 0, tzinfo=timezone.utc)


def _item(variant_id: int, bar_code: str, name: str) -> dict:
    return {
        "variant_id": variant_id,
        "product_id": 100 + variant_id,
        "sku_code": f"SKU-{variant_id}",
        "bar_code": bar_code,
        "product_name": name,
        "variant_name": "750 cc",
        "stock_snapshot": Decimal("5"),
        "stock_available_snapshot": Decimal("5"),
        "stock_at_close": None,
        "stock_current": Decimal("5"),
        "stock_available_current": Decimal("5"),
        "last_entry_at": None,
        "last_entry_movement": None,
        "exhibition_status": "pendiente",
        "found_at": None,
        "found_by": None,
        "found_device": None,
    }


class FakeGondolaRepo:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._next_id = 1
        self.sessions: dict[int, dict] = {}
        self.items: dict[int, dict[int, dict]] = {}
        self.replenishment_rows: dict[int, dict[int, dict]] = {}
        self.scans: list[dict] = []
        self._new_session_locked()

    def _new_session_locked(self) -> int:
        sid = self._next_id
        self._next_id += 1
        self.sessions[sid] = {
            "id": sid,
            "company_id": 3,
            "office_id": 4,
            "office_name": "QUILLOTANA I",
            "product_type_id": 35,
            "product_type_name": "VINO",
            "status": "en_curso",
            "created_by": "op@lq.cl",
            "started_at": SNAPSHOT_AT,
            "finished_at": None,
            "finished_by": None,
            "cancelled_at": None,
            "cancelled_by": None,
            "stock_snapshot_at": SNAPSHOT_AT,
            "total_sku_snapshot": 2,
        }
        self.items[sid] = {
            10: _item(10, "780001", "Vino Reserva"),
            11: _item(11, "780002", "Vino Blanco"),
        }
        self.replenishment_rows[sid] = {}
        return sid

    # -- infraestructura
    def tables_ready(self) -> bool:
        return True

    def offices(self, company_id: int) -> list[dict]:
        return [{"office_id": 4, "name": "QUILLOTANA I"}]

    def product_types(self, company_id: int) -> list[dict]:
        return [{"product_type_id": 35, "product_type_name": "VINO", "variant_count": 2}]

    # -- sesiones
    def create_session(self, company_id, office_id, product_type_id, actor, role) -> dict:
        with self._lock:
            sid = self._new_session_locked()
            return dict(self.sessions[sid])

    def freeze_items(self, session_id, company_id, office_id, product_type_id) -> int:
        return len(self.items[session_id])

    def set_session_total(self, session_id, company_id, total) -> None:
        self.sessions[session_id]["total_sku_snapshot"] = total

    def session(self, company_id, session_id) -> dict | None:
        row = self.sessions.get(session_id)
        if row is None:
            return None
        items = self.items[session_id].values()
        found = sum(1 for i in items if i["exhibition_status"] == "encontrado")
        out = dict(row)
        out.update({
            "item_count": len(items),
            "found_count": found,
            "pending_count": len(items) - found,
            "stock_changed_count": 0,
        })
        return out

    def session_items(self, company_id, session_id, office_id) -> list[dict]:
        return [dict(i) for i in self.items[session_id].values()]

    def recent_scans(self, company_id, session_id, limit) -> list[dict]:
        return []

    # -- escaneo
    def scan_candidates(self, company_id, office_id, code) -> list[dict]:
        for item in self.items[1].values() if 1 in self.items else []:
            if item["bar_code"] == code:
                return [{
                    "variant_id": item["variant_id"],
                    "product_id": item["product_id"],
                    "sku_code": item["sku_code"],
                    "bar_code": item["bar_code"],
                    "product_name": item["product_name"],
                    "variant_name": item["variant_name"],
                    "product_type_id": 35,
                    "stock": Decimal("5"),
                }]
        # Buscar en cualquier sesión (el candidato es del catálogo, no del recorrido).
        for items in self.items.values():
            for item in items.values():
                if item["bar_code"] == code:
                    return [{
                        "variant_id": item["variant_id"],
                        "product_id": item["product_id"],
                        "sku_code": item["sku_code"],
                        "bar_code": item["bar_code"],
                        "product_name": item["product_name"],
                        "variant_name": item["variant_name"],
                        "product_type_id": 35,
                        "stock": Decimal("5"),
                    }]
        return []

    def session_item(self, company_id, session_id, variant_id) -> dict | None:
        item = self.items[session_id].get(variant_id)
        return dict(item) if item else None

    def touch_active_session(self, company_id, session_id) -> bool:
        with self._lock:
            return self.sessions[session_id]["status"] == "en_curso"

    def mark_found(self, company_id, session_id, variant_id, actor, device) -> dict | None:
        with self._lock:
            if self.sessions[session_id]["status"] != "en_curso":
                return None
            item = self.items[session_id][variant_id]
            if item["exhibition_status"] != "pendiente":
                return None
            item["exhibition_status"] = "encontrado"
            item["found_at"] = SNAPSHOT_AT
            item["found_by"] = actor
            return {"variant_id": variant_id, "found_at": SNAPSHOT_AT}

    def insert_scan(self, company_id, session_id, code, variant_id, result, device, actor) -> None:
        with self._lock:
            self.scans.append({"session_id": session_id, "result": result, "variant_id": variant_id})

    # -- cierre / cancelación
    def claim_close(self, company_id, session_id, actor) -> dict | None:
        with self._lock:
            row = self.sessions[session_id]
            if row["status"] != "en_curso":
                return None
            row["status"] = "cerrado"
            row["finished_at"] = SNAPSHOT_AT
            row["finished_by"] = actor
            return dict(row)

    def close_items(self, company_id, session_id) -> None:
        for item in self.items[session_id].values():
            item["stock_at_close"] = item["stock_snapshot"]

    def create_replenishment(self, company_id, session_id) -> int:
        created = 0
        for item in self.items[session_id].values():
            if item["exhibition_status"] == "pendiente" and item["variant_id"] not in self.replenishment_rows[session_id]:
                self.replenishment_rows[session_id][item["variant_id"]] = {
                    **item,
                    "status": "pendiente_buscar",
                    "note": None,
                    "stock_current": item["stock_snapshot"],
                    "updated_by": None,
                    "updated_at": None,
                }
                created += 1
        return created

    def cancel_session(self, company_id, session_id, actor, reason) -> dict | None:
        with self._lock:
            row = self.sessions[session_id]
            if row["status"] != "en_curso":
                return None
            row["status"] = "cancelado"
            row["cancelled_at"] = SNAPSHOT_AT
            row["cancelled_by"] = actor
            return dict(row)

    def replenishment(self, company_id, session_id, office_id) -> list[dict]:
        return [dict(r) for r in self.replenishment_rows[session_id].values()]


def _service(repo: FakeGondolaRepo, role: str = "operativo") -> GondolasService:
    return GondolasService(
        repo,  # type: ignore[arg-type]
        CompanyScope(company_id=3, company_name="La Quillotana SPA"),
        user={"email": "op@lq.cl", "role": role},
    )


def test_dos_escaneos_simultaneos_un_solo_hallazgo():
    repo = FakeGondolaRepo()
    service = _service(repo)
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = sorted(
            r["result"] for r in pool.map(lambda _: service.register_scan(1, "780001", "usb"), range(2))
        )
    assert results == ["duplicado", "encontrado"]
    assert repo.items[1][10]["exhibition_status"] == "encontrado"
    assert [s["result"] for s in repo.scans].count("encontrado") == 1


def test_escaneo_tras_cierre_es_rechazado():
    repo = FakeGondolaRepo()
    service = _service(repo)
    service.close_session(1)
    with pytest.raises(GondolasValidationError):
        service.register_scan(1, "780001", "usb")
    assert repo.items[1][10]["exhibition_status"] == "pendiente"


def test_escaneo_tras_cancelacion_es_rechazado():
    repo = FakeGondolaRepo()
    service = _service(repo, role="gerencia")
    service.cancel_session(1)
    with pytest.raises(GondolasValidationError):
        service.register_scan(1, "780001", "usb")
    assert repo.items[1][10]["exhibition_status"] == "pendiente"


def test_cierre_reclama_antes_de_generar_reposicion():
    repo = FakeGondolaRepo()
    service = _service(repo)
    service.register_scan(1, "780001", "usb")
    closed = service.close_session(1)
    # El SKU encontrado antes del cierre no entra a reposición; el pendiente sí.
    assert closed["replenishment_created"] == 1
    assert [i["variant_id"] for i in closed["replenishment"]] == [11]


def test_reposicion_historica_sobrevive_a_un_nuevo_recorrido():
    repo = FakeGondolaRepo()
    service = _service(repo)
    service.close_session(1)
    assert [i["variant_id"] for i in service.list_replenishment(1)] == [10, 11]
    created = service.create_session(4, 35)
    assert created["id"] == 2
    # La lista del recorrido cerrado sigue disponible e intacta.
    assert [i["variant_id"] for i in service.list_replenishment(1)] == [10, 11]


def test_crear_sesion_entrega_stock_snapshot_at():
    repo = FakeGondolaRepo()
    service = _service(repo)
    created = service.create_session(4, 35)
    assert created["stock_snapshot_at"] == SNAPSHOT_AT.isoformat()
    assert created["total_sku"] == 2
