"""ERP — Seguimiento de entregas (solo lectura).

Fuente de verdad: delivery_visits + picking/dispatch_plan confirmados en servidor.
No inventa visitas desde IndexedDB ni muta Bsale/stock/cuadratura.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

from fastapi import HTTPException

from backend.db import get_connection
from backend.repositories.distribuidora import delivery_visit_repo as visit_repo
from backend.services.distribuidora import delivery_route_progress as progress_svc
from backend.services.distribuidora import dispatch_plan_pwa_driver_read_service as read_svc
from backend.utils.json_safe import serialize_value

SANTIAGO = ZoneInfo("America/Santiago")

MANAGED = frozenset({"completed", "partial", "not_delivered"})

# Motivos que exigen evidencia fotográfica (o excepción auditable).
EVIDENCE_REQUIRED_REASONS = frozenset(
    {
        "closed_store",
        "not_found",
        "rejected_order",
        "absent_customer",
        "other",
    }
)

REASON_ES = {
    "closed_store": "Local cerrado",
    "not_found": "No encontrado",
    "rejected_order": "Pedido rechazado",
    "absent_customer": "Cliente ausente",
    "other": "Otro motivo",
}


def _parse_date(value: str | date | None) -> date:
    if value is None or value == "":
        return datetime.now(SANTIAGO).date()
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    raw = str(value).strip()[:10]
    try:
        return date.fromisoformat(raw)
    except ValueError as exc:
        raise HTTPException(
            status_code=400, detail="Fecha inválida (use YYYY-MM-DD)"
        ) from exc


def _fmt_local(ts: Any) -> str | None:
    if ts is None:
        return None
    if isinstance(ts, str):
        try:
            ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except ValueError:
            return str(ts)[:19]
    if not isinstance(ts, datetime):
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(SANTIAGO).strftime("%H:%M")


def _fmt_local_full(ts: Any) -> str | None:
    if ts is None:
        return None
    if isinstance(ts, str):
        try:
            ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except ValueError:
            return str(ts)[:19]
    if not isinstance(ts, datetime):
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(SANTIAGO).strftime("%Y-%m-%d %H:%M:%S")


def _scope_company_ids(user: dict[str, Any]) -> set[int] | None:
    from backend.auth.multiapp import get_scope

    scope = get_scope(user)
    if scope.load_failed or getattr(scope, "companies_unverified", False):
        raise HTTPException(
            status_code=503, detail="No se pudo verificar empresas autorizadas"
        )
    return scope.company_ids


def _plan_company_id(cur, plan_id: int) -> int | None:
    """Resuelve empresa del despacho vía visitas o documentos OC (sin inventar)."""
    try:
        cur.execute(
            """
            SELECT company_id
            FROM distribuidora.delivery_visits
            WHERE dispatch_plan_id = %s
            GROUP BY company_id
            ORDER BY COUNT(*) DESC, company_id ASC
            LIMIT 1
            """,
            (int(plan_id),),
        )
        row = cur.fetchone()
        if row is not None:
            val = row[0] if not isinstance(row, dict) else row.get("company_id")
            if val is not None:
                return int(val)
    except Exception:
        pass
    try:
        cur.execute(
            """
            SELECT d.company_id
            FROM distribuidora.dispatch_plan_orders dpo
            JOIN distribuidora.documents d
              ON d.document_id = dpo.oc_document_id
            WHERE dpo.dispatch_plan_id = %s
              AND d.company_id IS NOT NULL
            GROUP BY d.company_id
            ORDER BY COUNT(*) DESC, d.company_id ASC
            LIMIT 1
            """,
            (int(plan_id),),
        )
        row = cur.fetchone()
        if row is not None:
            val = row[0] if not isinstance(row, dict) else row.get("company_id")
            if val is not None:
                return int(val)
    except Exception:
        pass
    return None


def _list_plans_for_date(
    cur, *, day: date, company_ids: set[int] | None
) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT
            dp.id,
            COALESCE(
                NULLIF(BTRIM(dp.planning_code), ''),
                'PLAN-' || LPAD(dp.id::text, 5, '0')
            ) AS planning_code,
            dp.planning_date,
            dp.planning_name,
            dp.route_name,
            dp.status AS plan_status,
            dp.driver_user_id,
            dp.driver_name,
            dp.truck_id,
            COALESCE(
                NULLIF(BTRIM(dp.truck_name), ''),
                NULLIF(BTRIM(t.name), ''),
                NULLIF(BTRIM(t.plate), '')
            ) AS vehicle_label,
            t.plate AS truck_plate
        FROM distribuidora.dispatch_plan dp
        LEFT JOIN distribuidora.trucks t ON t.id = dp.truck_id
        WHERE dp.planning_date = %s
        ORDER BY dp.id ASC
        """,
        (day,),
    )
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in (cur.fetchall() or [])]
    out: list[dict[str, Any]] = []
    for r in rows:
        cid = _plan_company_id(cur, int(r["id"]))
        r["company_id"] = cid
        if company_ids is not None:
            if cid is None or int(cid) not in company_ids:
                continue
        out.append(r)
    return out


def _offline_meta_by_visit(
    cur, visit_ids: list[int]
) -> dict[int, dict[str, Any]]:
    if not visit_ids:
        return {}
    try:
        from backend.repositories.distribuidora import (
            delivery_offline_ops_repo as ops_repo,
        )

        if not ops_repo.table_ready(cur):
            return {}
        cur.execute(
            """
            SELECT DISTINCT ON (visit_id)
                visit_id, captured_at, completed_at, status, op_type
            FROM distribuidora.delivery_offline_ops
            WHERE visit_id = ANY(%s)
              AND status = 'completed'
            ORDER BY visit_id, completed_at DESC NULLS LAST, id DESC
            """,
            (list(visit_ids),),
        )
    except Exception:
        return {}
    cols = [d[0] for d in cur.description]
    out: dict[int, dict[str, Any]] = {}
    for r in cur.fetchall() or []:
        row = dict(zip(cols, r))
        out[int(row["visit_id"])] = row
    return out


def _timing_for_visit(
    visit: dict[str, Any] | None, offline: dict[str, Any] | None
) -> dict[str, Any]:
    if not visit:
        return {
            "managed_at": None,
            "managed_at_local": None,
            "managed_at_source": None,
            "synced_at": None,
            "synced_at_local": None,
            "device_capture": False,
            "requires_review": False,
        }
    offline = offline or {}
    device_ts = offline.get("captured_at") or visit.get("end_gps_captured_at")
    server_ts = offline.get("completed_at") or visit.get("ended_at") or visit.get("updated_at")
    # Hora efectiva de gestión = captura en dispositivo cuando existe; nunca usar solo sync.
    managed = device_ts or visit.get("ended_at")
    source = "device" if device_ts else ("server" if visit.get("ended_at") else None)
    review = str(visit.get("closure_review_status") or "") == "pending_review"
    return {
        "managed_at": managed,
        "managed_at_local": _fmt_local(managed),
        "managed_at_source": source,
        "synced_at": server_ts,
        "synced_at_local": _fmt_local(server_ts),
        "device_capture": bool(device_ts),
        "requires_review": review,
        "closure_review_status": visit.get("closure_review_status"),
    }


def _payment_label(cols: list[dict[str, Any]]) -> str | None:
    if not cols:
        return None
    labels = []
    for c in cols:
        m = str(c.get("method") or "").lower()
        if m in ("cash", "efectivo"):
            labels.append("Efectivo")
        elif m in ("transfer", "transfer_declared", "transferencia"):
            labels.append("Transferencia")
        elif m in ("cash_and_transfer",):
            labels.append("Efectivo + transf")
        elif m in ("pending", "credito", "credit"):
            labels.append("Pendiente")
        elif m:
            labels.append(m)
    return " · ".join(dict.fromkeys(labels)) if labels else None


def get_dashboard(
    *,
    user: dict[str, Any],
    day: str | date | None = None,
    driver_user_id: int | None = None,
    plan_id: int | None = None,
    status: str | None = None,
    client_q: str | None = None,
) -> dict[str, Any]:
    """KPIs del día + filas por chofer/despacho. Solo datos confirmados en PG."""
    company_ids = _scope_company_ids(user)
    day_d = _parse_date(day)
    now_santiago = datetime.now(SANTIAGO)

    conn = get_connection()
    try:
        cur = conn.cursor()
        if not visit_repo.tables_ready(cur):
            cur.close()
            return serialize_value(
                {
                    "tables_ready": False,
                    "date": day_d.isoformat(),
                    "timezone": "America/Santiago",
                    "kpis": {},
                    "by_driver": [],
                    "message_es": "Migración 056 pendiente",
                }
            )
        plans = _list_plans_for_date(cur, day=day_d, company_ids=company_ids)
        cur.close()
    finally:
        conn.close()

    if plan_id is not None:
        plans = [p for p in plans if int(p["id"]) == int(plan_id)]
    if driver_user_id is not None:
        plans = [
            p
            for p in plans
            if p.get("driver_user_id") is not None
            and int(p["driver_user_id"]) == int(driver_user_id)
        ]

    status_f = (status or "").strip().lower() or None
    client_f = (client_q or "").strip().lower() or None

    kpis = {
        "planned_clients": 0,
        "managed_clients": 0,
        "completed": 0,
        "partial": 0,
        "closed_store": 0,
        "other_not_delivered": 0,
        "pending_clients": 0,
        "pending_gps_review": 0,
        "progress_percent": 0.0,
        "first_delivery_at_local": None,
        "last_delivery_at_local": None,
    }
    first_ts: datetime | None = None
    last_ts: datetime | None = None
    by_driver: list[dict[str, Any]] = []
    activity_events: list[dict[str, Any]] = []

    all_visit_ids: list[int] = []
    plan_bundles: list[tuple[dict[str, Any], list[dict[str, Any]], dict[str, dict]]] = []

    for plan in plans:
        try:
            bundle = read_svc._build_client_index(int(plan["id"]))  # noqa: SLF001
            clients = list(bundle.get("clients") or [])
            attention = read_svc._visit_attention_by_customer(int(plan["id"]))  # noqa: SLF001
        except Exception:
            clients, attention = [], {}
        if client_f:
            clients = [
                c
                for c in clients
                if client_f
                in " ".join(
                    [
                        str(c.get("fantasy_name") or ""),
                        str(c.get("client_name") or ""),
                        str(c.get("customer_key") or ""),
                    ]
                ).lower()
            ]
        plan_bundles.append((plan, clients, attention))
        for att in attention.values():
            vid = att.get("operational_visit_id") or att.get("visit_id")
            if vid:
                all_visit_ids.append(int(vid))

    offline_by_visit: dict[int, dict[str, Any]] = {}
    visits_by_id: dict[int, dict[str, Any]] = {}
    if all_visit_ids:
        conn2 = get_connection()
        try:
            cur2 = conn2.cursor()
            offline_by_visit = _offline_meta_by_visit(cur2, list(set(all_visit_ids)))
            for vid in set(all_visit_ids):
                try:
                    v = visit_repo.get_visit(cur2, int(vid))
                    if v:
                        visits_by_id[int(vid)] = v
                except Exception:
                    pass
            cur2.close()
        finally:
            conn2.close()

    for plan, clients, attention in plan_bundles:
        prog = progress_svc.compute_route_progress(
            clients=clients, attention_by_key=attention
        )
        by_result = prog.get("by_result") or {}
        closed_store = 0
        other_nd = 0
        first_m = None
        last_m = None
        last_any = None

        for c in clients:
            key = str(c.get("customer_key") or "")
            att = attention.get(key) or {}
            st = str(att.get("attention_status") or att.get("visit_status") or "pending")
            if status_f and st != status_f and not (
                status_f == "not_delivered" and st == "not_delivered"
            ):
                continue
            reason = str(att.get("result_reason") or "")
            if st == "not_delivered":
                if reason == "closed_store":
                    closed_store += 1
                else:
                    other_nd += 1
            vid = att.get("operational_visit_id") or att.get("visit_id")
            visit = visits_by_id.get(int(vid)) if vid else None
            off = offline_by_visit.get(int(vid)) if vid else None
            timing = _timing_for_visit(visit, off)
            managed_at = timing.get("managed_at")
            if isinstance(managed_at, str):
                try:
                    managed_at = datetime.fromisoformat(
                        managed_at.replace("Z", "+00:00")
                    )
                except ValueError:
                    managed_at = None
            if st in MANAGED and managed_at:
                if first_m is None or managed_at < first_m:
                    first_m = managed_at
                if last_m is None or managed_at > last_m:
                    last_m = managed_at
                if first_ts is None or managed_at < first_ts:
                    first_ts = managed_at
                if last_ts is None or managed_at > last_ts:
                    last_ts = managed_at
            if managed_at and (last_any is None or managed_at > last_any):
                last_any = managed_at

            if st in MANAGED:
                name = (
                    (c.get("fantasy_name") or c.get("client_name") or "—").strip()
                    or "—"
                )
                activity_events.append(
                    {
                        "visit_id": int(vid) if vid else None,
                        "dispatch_plan_id": int(plan["id"]),
                        "planning_code": plan.get("planning_code"),
                        "driver_user_id": plan.get("driver_user_id"),
                        "driver_name": plan.get("driver_name") or "—",
                        "customer_key": key,
                        "commercial_name": name,
                        "result_status": st,
                        "result_reason": reason or None,
                        "result_reason_es": REASON_ES.get(reason, reason or None),
                        "observation": (visit or {}).get("result_note"),
                        "managed_at": managed_at,
                        "managed_at_local": timing.get("managed_at_local"),
                        "synced_at_local": timing.get("synced_at_local"),
                        "device_capture": timing.get("device_capture"),
                        "requires_review": timing.get("requires_review"),
                        "closure_review_status": timing.get("closure_review_status"),
                        "evidence_count": 0,
                    }
                )

        # Si hay filtro de estado, recalcular totales filtrados
        if status_f:
            filtered = [
                c
                for c in clients
                if str(
                    (attention.get(str(c.get("customer_key") or "")) or {}).get(
                        "attention_status"
                    )
                    or (attention.get(str(c.get("customer_key") or "")) or {}).get(
                        "visit_status"
                    )
                    or "pending"
                )
                == status_f
            ]
            total_c = len(filtered)
            managed_c = sum(
                1
                for c in filtered
                if str(
                    (attention.get(str(c.get("customer_key") or "")) or {}).get(
                        "attention_status"
                    )
                    or ""
                )
                in MANAGED
            )
            completed = sum(
                1
                for c in filtered
                if str(
                    (attention.get(str(c.get("customer_key") or "")) or {}).get(
                        "attention_status"
                    )
                    or ""
                )
                == "completed"
            )
            partial = sum(
                1
                for c in filtered
                if str(
                    (attention.get(str(c.get("customer_key") or "")) or {}).get(
                        "attention_status"
                    )
                    or ""
                )
                == "partial"
            )
            pending = total_c - managed_c
            pending_review = sum(
                1
                for c in filtered
                if str(
                    (attention.get(str(c.get("customer_key") or "")) or {}).get(
                        "closure_review_status"
                    )
                    or ""
                )
                == "pending_review"
            )
        else:
            total_c = int(prog.get("total_clients") or 0)
            managed_c = int(prog.get("managed_clients") or 0)
            completed = int(by_result.get("completed") or 0)
            partial = int(by_result.get("partial") or 0)
            pending = int(prog.get("remaining_clients") or 0)
            pending_review = int(by_result.get("pending_review") or 0)

        kpis["planned_clients"] += total_c
        kpis["managed_clients"] += managed_c
        kpis["completed"] += completed
        kpis["partial"] += partial
        kpis["closed_store"] += closed_store
        kpis["other_not_delivered"] += other_nd
        kpis["pending_clients"] += pending
        kpis["pending_gps_review"] += pending_review

        pct = 0.0 if total_c <= 0 else round(100.0 * managed_c / total_c, 1)
        by_driver.append(
            {
                "dispatch_plan_id": int(plan["id"]),
                "planning_code": plan.get("planning_code"),
                "route_name": plan.get("route_name") or plan.get("planning_name"),
                "driver_user_id": plan.get("driver_user_id"),
                "driver_name": plan.get("driver_name") or "—",
                "vehicle_label": plan.get("vehicle_label"),
                "truck_plate": plan.get("truck_plate"),
                "clients_total": total_c,
                "delivered": completed,
                "partial": partial,
                "not_delivered": closed_store + other_nd,
                "closed_store": closed_store,
                "other_not_delivered": other_nd,
                "pending": pending,
                "pending_gps_review": pending_review,
                "progress_percent": pct,
                "first_delivery_at_local": _fmt_local(first_m),
                "last_delivery_at_local": _fmt_local(last_m),
                "last_management_at_local": _fmt_local(last_any),
                "note_es": (
                    "Horas de gestión usan captura del dispositivo cuando existe; "
                    "la hora de sync al servidor se muestra aparte en el detalle."
                ),
            }
        )

    kpis["first_delivery_at_local"] = _fmt_local(first_ts)
    kpis["last_delivery_at_local"] = _fmt_local(last_ts)
    planned = int(kpis["planned_clients"] or 0)
    managed = int(kpis["managed_clients"] or 0)
    kpis["progress_percent"] = (
        0.0 if planned <= 0 else round(100.0 * managed / planned, 1)
    )
    kpis["not_delivered_total"] = int(kpis["closed_store"] or 0) + int(
        kpis["other_not_delivered"] or 0
    )

    # Actividad / incidencias desde el mismo recorrido (sin N+1 adicional de planes).
    def _sort_key(ev: dict[str, Any]):
        ts = ev.get("managed_at")
        if isinstance(ts, datetime):
            return ts
        return datetime.min.replace(tzinfo=timezone.utc)

    activity_events.sort(key=_sort_key, reverse=True)

    # Contar evidencias en batch para eventos recientes con visita.
    recent_for_evidence = [
        ev for ev in activity_events if ev.get("visit_id") and ev.get("result_status")
    ][:40]
    evid_counts: dict[int, int] = {}
    thumb_by_visit: dict[int, dict[str, Any]] = {}
    if recent_for_evidence:
        from backend.services import object_storage_service as storage

        conn3 = get_connection()
        try:
            cur3 = conn3.cursor()
            for ev in recent_for_evidence:
                vid = int(ev["visit_id"])
                if vid in evid_counts:
                    continue
                try:
                    rows = visit_repo.list_evidence(cur3, vid)
                except Exception:
                    rows = []
                ok = [
                    e
                    for e in rows
                    if e.get("upload_confirmed")
                    or e.get("validation_status") == "exception"
                ]
                evid_counts[vid] = len(ok)
                # Miniatura firmada solo para incidencias con foto (tope 8).
                if (
                    ev.get("result_status") == "not_delivered"
                    and ok
                    and len(thumb_by_visit) < 8
                ):
                    photo = next(
                        (
                            e
                            for e in ok
                            if e.get("upload_confirmed") and e.get("object_key")
                        ),
                        None,
                    )
                    if photo:
                        try:
                            url = storage.presign_get(
                                object_key=str(photo["object_key"]), expires_in=300
                            )
                        except Exception:
                            url = None
                        if url:
                            thumb_by_visit[vid] = {
                                "url": url,
                                "expires_in_sec": 300,
                                "evidence_id": int(photo["id"]),
                                "captured_at_local": _fmt_local(
                                    photo.get("captured_at")
                                ),
                                "has_gps": photo.get("capture_lat") is not None
                                and photo.get("capture_lng") is not None,
                            }
            cur3.close()
        finally:
            conn3.close()

    for ev in activity_events:
        vid = ev.get("visit_id")
        if vid:
            ev["evidence_count"] = int(evid_counts.get(int(vid), 0))
            thumb = thumb_by_visit.get(int(vid))
            if thumb:
                ev["evidence_thumbnail"] = thumb
        # No serializar datetime crudo; serialize_value lo maneja, pero limpiamos clave interna.
        ev.pop("managed_at", None)

    recent_activity = activity_events[:20]
    recent_incidents = [
        ev for ev in activity_events if ev.get("result_status") == "not_delivered"
    ][:12]

    return serialize_value(
        {
            "tables_ready": True,
            "date": day_d.isoformat(),
            "timezone": "America/Santiago",
            "panel_updated_at": now_santiago.isoformat(),
            "panel_updated_at_local": now_santiago.strftime("%H:%M:%S"),
            "panel_updated_at_full_local": now_santiago.strftime("%Y-%m-%d %H:%M:%S"),
            "auto_refresh_sec": 45,
            "filters": {
                "driver_user_id": driver_user_id,
                "plan_id": plan_id,
                "status": status_f,
                "client_q": client_q,
            },
            "kpis": kpis,
            "by_driver": by_driver,
            "recent_activity": recent_activity,
            "recent_incidents": recent_incidents,
            "evidence_policy": {
                "required_for_reasons": sorted(EVIDENCE_REQUIRED_REASONS),
                "signature_required_for_delivery": False,
                "receptor_name_required_for_delivery": False,
                "note_es": (
                    "Incidencias requieren fotografía o excepción auditable. "
                    "Entrega completa/parcial sin firma ni receptor obligatorios."
                ),
            },
            "mode": "online_only",
            "read_only": True,
            "note_es": (
                "Solo operaciones confirmadas en servidor. Capturas solo en "
                "IndexedDB no aparecen aquí."
            ),
        }
    )


def get_plan_clients(
    *,
    user: dict[str, Any],
    plan_id: int,
    status: str | None = None,
    client_q: str | None = None,
) -> dict[str, Any]:
    """Listado de clientes únicos del despacho + resultado confirmado."""
    company_ids = _scope_company_ids(user)
    conn = get_connection()
    try:
        cur = conn.cursor()
        if not visit_repo.tables_ready(cur):
            raise HTTPException(status_code=503, detail="Migración 056 pendiente")
        cur.execute(
            """
            SELECT
                dp.id,
                COALESCE(
                    NULLIF(BTRIM(dp.planning_code), ''),
                    'PLAN-' || LPAD(dp.id::text, 5, '0')
                ) AS planning_code,
                dp.planning_date, dp.route_name, dp.planning_name,
                dp.driver_user_id, dp.driver_name,
                COALESCE(
                    NULLIF(BTRIM(dp.truck_name), ''),
                    NULLIF(BTRIM(t.name), ''),
                    NULLIF(BTRIM(t.plate), '')
                ) AS vehicle_label
            FROM distribuidora.dispatch_plan dp
            LEFT JOIN distribuidora.trucks t ON t.id = dp.truck_id
            WHERE dp.id = %s
            """,
            (int(plan_id),),
        )
        prow = cur.fetchone()
        if not prow:
            raise HTTPException(status_code=404, detail="Despacho no encontrado")
        cols = [d[0] for d in cur.description]
        plan = dict(zip(cols, prow))
        cid = _plan_company_id(cur, int(plan_id))
        plan["company_id"] = cid
        if company_ids is not None and (cid is None or int(cid) not in company_ids):
            raise HTTPException(status_code=404, detail="Despacho no encontrado")
        cur.close()
    finally:
        conn.close()

    try:
        bundle = read_svc._build_client_index(int(plan_id))  # noqa: SLF001
        clients = list(bundle.get("clients") or [])
        attention = read_svc._visit_attention_by_customer(int(plan_id))  # noqa: SLF001
    except Exception as exc:
        raise HTTPException(
            status_code=500, detail=f"No se pudo cargar clientes: {exc}"
        ) from exc

    status_f = (status or "").strip().lower() or None
    client_f = (client_q or "").strip().lower() or None

    visit_ids: list[int] = []
    for att in attention.values():
        vid = att.get("operational_visit_id") or att.get("visit_id")
        if vid:
            visit_ids.append(int(vid))

    visits: dict[int, dict[str, Any]] = {}
    docs_by: dict[int, list] = {}
    lines_by: dict[int, list] = {}
    cols_by: dict[int, list] = {}
    evid_by: dict[int, list] = {}
    offline_by: dict[int, dict] = {}

    conn2 = get_connection()
    try:
        cur2 = conn2.cursor()
        offline_by = _offline_meta_by_visit(cur2, list(set(visit_ids)))
        for vid in set(visit_ids):
            v = visit_repo.get_visit(cur2, int(vid))
            if not v:
                continue
            visits[int(vid)] = v
            try:
                docs_by[int(vid)] = visit_repo.list_document_results(cur2, int(vid))
                lines_by[int(vid)] = visit_repo.list_line_results(cur2, int(vid))
                cols_by[int(vid)] = visit_repo.list_active_collections(cur2, int(vid))
                evid_by[int(vid)] = visit_repo.list_evidence(cur2, int(vid))
            except Exception:
                docs_by[int(vid)] = []
                lines_by[int(vid)] = []
                cols_by[int(vid)] = []
                evid_by[int(vid)] = []
        cur2.close()
    finally:
        conn2.close()

    rows: list[dict[str, Any]] = []
    for c in clients:
        key = str(c.get("customer_key") or "")
        name = (c.get("fantasy_name") or c.get("client_name") or "—").strip()
        if client_f and client_f not in f"{name} {key}".lower():
            continue
        att = attention.get(key) or {}
        st = str(att.get("attention_status") or att.get("visit_status") or "pending")
        if status_f and st != status_f:
            continue
        vid = att.get("operational_visit_id") or att.get("visit_id")
        visit = visits.get(int(vid)) if vid else None
        timing = _timing_for_visit(
            visit, offline_by.get(int(vid)) if vid else None
        )
        docs = docs_by.get(int(vid), []) if vid else []
        lines = lines_by.get(int(vid), []) if vid else []
        collections = cols_by.get(int(vid), []) if vid else []
        evidence = evid_by.get(int(vid), []) if vid else []
        reason = att.get("result_reason") or (visit or {}).get("result_reason")
        evid_ok = [
            e
            for e in evidence
            if e.get("upload_confirmed") or e.get("validation_status") == "exception"
        ]
        needs_photo = st == "not_delivered" and str(reason or "") in EVIDENCE_REQUIRED_REASONS
        rows.append(
            {
                "customer_key": key,
                "client_id": c.get("client_id"),
                "route_order": c.get("route_order"),
                "commercial_name": name,
                "client_name": c.get("client_name"),
                "fantasy_name": c.get("fantasy_name") or "",
                "document_count": int(c.get("document_count") or len(docs) or 0),
                "documents": [
                    {
                        "id": d.get("id"),
                        "document_type": d.get("document_type"),
                        "document_number": d.get("document_number"),
                        "result_status": d.get("result_status"),
                        "document_total": d.get("document_total"),
                    }
                    for d in docs
                ],
                "products": [
                    {
                        "id": ln.get("id"),
                        "product_name": ln.get("product_name") or ln.get("variant_name"),
                        "planned_units": ln.get("planned_units"),
                        "delivered_units": ln.get("delivered_units"),
                        "undelivered_units": ln.get("undelivered_units"),
                        "undelivered_reason": ln.get("undelivered_reason"),
                    }
                    for ln in lines
                ],
                "result_status": st,
                "result_reason": reason,
                "result_reason_es": REASON_ES.get(str(reason or ""), reason),
                "result_note": (visit or {}).get("result_note"),
                "observation": (visit or {}).get("result_note"),
                "managed_at_local": timing.get("managed_at_local"),
                "managed_at_source": timing.get("managed_at_source"),
                "synced_at_local": timing.get("synced_at_local"),
                "device_capture": timing.get("device_capture"),
                "payment_declared": _payment_label(collections),
                "closure_review_status": timing.get("closure_review_status"),
                "requires_review": timing.get("requires_review"),
                "sync_confirmed": bool(visit and visit.get("ended_at")),
                "visit_id": int(vid) if vid else None,
                "evidence_count": len(evid_ok),
                "evidence_required": needs_photo,
                "evidence_label_es": (
                    "Sin evidencia fotográfica"
                    if needs_photo and not evid_ok
                    else (
                        f"{len(evid_ok)} evidencia(s)"
                        if evid_ok
                        else ("No aplica" if not needs_photo else "Sin evidencia fotográfica")
                    )
                ),
                # Teléfonos omitidos a propósito (privacidad ERP).
            }
        )

    rows.sort(
        key=lambda r: (
            r.get("route_order") is None,
            int(r.get("route_order") or 0),
            str(r.get("commercial_name") or ""),
        )
    )

    return serialize_value(
        {
            "tables_ready": True,
            "plan": {
                "id": int(plan["id"]),
                "planning_code": plan.get("planning_code"),
                "planning_date": str(plan.get("planning_date")),
                "route_name": plan.get("route_name") or plan.get("planning_name"),
                "driver_user_id": plan.get("driver_user_id"),
                "driver_name": plan.get("driver_name"),
                "vehicle_label": plan.get("vehicle_label"),
            },
            "clients": rows,
            "count": len(rows),
            "read_only": True,
            "mode": "online_only",
            "note_es": (
                "Una fila por cliente (no por factura/OC). Solo visitas confirmadas "
                "en servidor. Hora de sync ≠ hora efectiva de entrega."
            ),
        }
    )


def get_visit_evidence_for_erp(
    *, user: dict[str, Any], visit_id: int
) -> dict[str, Any]:
    """Fotografías de incidencia con URL firmada corta. Sin object_key ni bucket público."""
    from backend.services.distribuidora import delivery_visit_service as visit_svc

    detail = visit_svc.get_visit_detail_for_erp(user=user, visit_id=int(visit_id))
    incident = list(detail.get("incident_evidence") or [])
    if not incident:
        return serialize_value(
            {
                **detail,
                "has_photographic_evidence": False,
                "evidence_empty_label_es": "Sin evidencia fotográfica",
                "read_only": True,
            }
        )
    # Enriquecer con observación de visita y estado pending_review
    visit = detail.get("visit") or {}
    for item in incident:
        item["visit_observation"] = visit.get("result_note")
        item["closure_review_status"] = visit.get("closure_review_status")
        item["captured_at_local"] = _fmt_local_full(item.get("captured_at"))
        if not item.get("url") and item.get("upload_confirmed") is False:
            item["note_es"] = item.get("note_es") or "Sin evidencia fotográfica usable"
    return serialize_value(
        {
            **detail,
            "incident_evidence": incident,
            "has_photographic_evidence": any(bool(i.get("url")) for i in incident),
            "evidence_empty_label_es": "Sin evidencia fotográfica",
            "read_only": True,
        }
    )
