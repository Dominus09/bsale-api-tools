"""CRM predictivo Fase 4.1 — agenda, probabilidades, segmentación, alertas, rutas."""

from __future__ import annotations

from calendar import monthrange
from collections import defaultdict
from datetime import datetime, timezone
from statistics import median
from typing import Any

from backend.services.commercial_analytics_intelligence import PRIORITY_ORDER, _clamp, _float, _int

AGENDA_TYPES = (
    "Recuperación",
    "Cross Selling",
    "Cliente en Riesgo",
    "Cliente VIP",
    "Cliente Nuevo",
    "Frecuencia vencida",
)

MAP_ESTADO = {
    "activo": "saludable",
    "nuevo": "nuevo",
    "recuperado": "recuperado",
    "perdido": "perdido",
    "en_riesgo": "riesgo",
}


def _freq_days(compras_90d: int, dias_sin: int | None) -> float:
    if compras_90d >= 2:
        return max(3.0, 90.0 / compras_90d)
    if dias_sin and dias_sin > 0:
        return float(max(7, min(60, dias_sin)))
    return 30.0


def compute_purchase_probability(
    *,
    dias_sin_comprar: int | None,
    compras_90d: int,
    freq_days: float | None = None,
) -> int:
    """0–100% según frecuencia histórica vs días desde última compra."""
    dias = max(0, _int(dias_sin_comprar))
    freq = freq_days if freq_days and freq_days > 0 else _freq_days(compras_90d, dias)
    if dias <= 0:
        return 85
    ratio = dias / freq
    if ratio <= 0.5:
        prob = 25.0 * (ratio / 0.5)
    elif ratio <= 1.0:
        prob = 25.0 + ((ratio - 0.5) / 0.5) * 45.0
    else:
        prob = 70.0 + min(30.0, (ratio - 1.0) * 50.0)
    return int(_clamp(prob, 0, 100))


def compute_recovery_probability(
    *,
    status: str,
    dias_sin_comprar: int | None,
    ticket_promedio: float,
    client_score: int,
) -> int:
    if status != "perdido":
        return max(10, min(60, client_score // 2))
    dias = _int(dias_sin_comprar)
    base = 70
    base -= min(40, dias // 3)
    if ticket_promedio >= 150_000:
        base += 12
    if ticket_promedio >= 300_000:
        base += 8
    return int(_clamp(base, 5, 95))


def compute_segment(
    *,
    status: str,
    venta_actual: float,
    venta_anterior: float,
    ticket_promedio: float,
    compras_90d: int,
    dias_sin_comprar: int | None,
    freq_days: float,
) -> str:
    if status == "perdido":
        return "Perdido"
    if status == "en_riesgo":
        return "En Riesgo"
    if status == "nuevo":
        return "Nuevo"
    if status == "recuperado":
        return "Crecimiento"
    if venta_actual >= 500_000 or ticket_promedio >= 200_000:
        return "VIP"
    if venta_actual >= 250_000 or (ticket_promedio >= 100_000 and compras_90d >= 3):
        return "Premium"
    if venta_anterior > 0 and venta_actual > venta_anterior * 1.1:
        return "Crecimiento"
    dias = _int(dias_sin_comprar)
    if dias > max(30, int(freq_days * 1.5)) and compras_90d >= 1:
        return "Dormido"
    if compras_90d <= 1:
        return "Ocasional"
    return "Premium" if ticket_promedio >= 80_000 else "Ocasional"


def _peer_medians_by_comuna(clients: list[dict[str, Any]]) -> dict[str, float]:
    buckets: dict[str, list[float]] = defaultdict(list)
    for c in clients:
        comuna = str(c.get("municipality") or c.get("comuna") or "Sin comuna")
        venta = _float(c.get("venta_actual"))
        if venta > 0:
            buckets[comuna].append(venta)
    return {k: float(median(v)) for k, v in buckets.items() if v}


def enrich_client_predictive(
    client: dict[str, Any],
    *,
    peer_medians: dict[str, float],
    cross_by_client: dict[int, dict[str, Any]],
) -> dict[str, Any]:
    compras_90d = _int(client.get("compras_90d"))
    dias_sin = client.get("dias_sin_comprar")
    freq = _freq_days(compras_90d, _int(dias_sin) if dias_sin is not None else None)
    status = str(client.get("status") or "en_riesgo")
    venta = _float(client.get("venta_actual"))
    venta_ant = _float(client.get("venta_anterior"))
    ticket = _float(client.get("ticket_promedio"))
    score = _int(client.get("client_score"))

    purchase_prob = compute_purchase_probability(
        dias_sin_comprar=dias_sin,
        compras_90d=compras_90d,
        freq_days=freq,
    )
    recovery_prob = compute_recovery_probability(
        status=status,
        dias_sin_comprar=dias_sin,
        ticket_promedio=ticket,
        client_score=score,
    )
    segment = compute_segment(
        status=status,
        venta_actual=venta,
        venta_anterior=venta_ant,
        ticket_promedio=ticket,
        compras_90d=compras_90d,
        dias_sin_comprar=dias_sin,
        freq_days=freq,
    )

    comuna = str(client.get("municipality") or "Sin comuna")
    peer = peer_medians.get(comuna, venta)
    potential_monthly = max(venta, peer, ticket * max(1, 30 / freq))
    potential_gap = max(0.0, peer - venta) if peer > venta else max(0.0, potential_monthly - venta)

    cross = cross_by_client.get(_int(client.get("client_id")), {})
    productos_sugeridos = cross.get("productos_sugeridos") or []
    categorias_sugeridas = cross.get("categorias_sugeridas") or []

    return {
        **client,
        "frecuencia_dias": round(freq, 1),
        "purchase_probability": purchase_prob,
        "probabilidad_recuperacion": recovery_prob,
        "segmento": segment,
        "potential_monthly": round(potential_monthly, 0),
        "potential_gap": round(potential_gap, 0),
        "cliente_vip": segment == "VIP",
        "productos_sugeridos": productos_sugeridos,
        "categorias_sugeridas": categorias_sugeridas,
    }


def _cross_lookup(cross: list[dict[str, Any]]) -> dict[int, dict[str, Any]]:
    out: dict[int, dict[str, Any]] = {}
    for x in cross:
        cid = _int(x.get("client_id"))
        if not cid:
            continue
        prod = str(x.get("producto_recomendado") or "")
        cat = str(x.get("producto_comprado") or "")
        entry = out.setdefault(cid, {"productos_sugeridos": [], "categorias_sugeridas": []})
        if prod and prod not in entry["productos_sugeridos"]:
            entry["productos_sugeridos"].append(prod)
        if cat and cat not in entry["categorias_sugeridas"]:
            entry["categorias_sugeridas"].append(cat)
    return out


def _task_sort_key(task: dict[str, Any]) -> tuple[float, float, float]:
    return (
        -_float(task.get("potencial_economico")),
        -_float(task.get("probabilidad_recuperacion")),
        -_float(task.get("score")),
    )


def _base_task(
    *,
    tipo: str,
    client: dict[str, Any],
    motivo: str,
    accion: str,
    prioridad: str,
    potencial: float,
) -> dict[str, Any]:
    return {
        "tipo": tipo,
        "vendedor": client.get("seller_name"),
        "cliente": client.get("client_name"),
        "client_id": client.get("client_id"),
        "comuna": client.get("municipality") or client.get("comuna"),
        "motivo": motivo,
        "prioridad": prioridad,
        "score": _int(client.get("client_score")),
        "potencial_economico": round(potencial, 0),
        "dias_sin_compra": client.get("dias_sin_comprar"),
        "ultima_compra": client.get("ultima_compra"),
        "accion_sugerida": accion,
        "categorias_sugeridas": client.get("categorias_sugeridas") or [],
        "productos_sugeridos": client.get("productos_sugeridos") or [],
        "purchase_probability": client.get("purchase_probability"),
        "probabilidad_recuperacion": client.get("probabilidad_recuperacion"),
        "segmento": client.get("segmento"),
        "potential_monthly": client.get("potential_monthly"),
    }


def build_agenda(
    enriched_unique: list[dict[str, Any]],
    enriched_lost: list[dict[str, Any]],
    cross: list[dict[str, Any]],
) -> dict[str, Any]:
    tasks: list[dict[str, Any]] = []
    cross_by_id = { _int(x.get("client_id")): x for x in cross if x.get("client_id") }

    for c in enriched_lost[:40]:
        pot = _float(c.get("potential_monthly")) or _float(c.get("promedio_compra_mensual")) or _float(c.get("ticket_promedio"))
        tasks.append(_base_task(
            tipo="Recuperación",
            client=c,
            motivo=f"Sin compra hace {_int(c.get('dias_sin_comprar'))} días",
            accion=str(c.get("accion_sugerida") or "Llamar hoy"),
            prioridad=str(c.get("prioridad") or "alta"),
            potencial=pot,
        ))

    for x in cross[:35]:
        cid = _int(x.get("client_id"))
        client = next((u for u in enriched_unique if _int(u.get("client_id")) == cid), None)
        if not client:
            client = {
                "client_id": cid,
                "client_name": x.get("client_name"),
                "seller_name": x.get("seller_name"),
                "municipality": x.get("municipality"),
                "client_score": 55,
                "purchase_probability": 40,
                "probabilidad_recuperacion": 30,
                "segmento": "Ocasional",
                "productos_sugeridos": [x.get("producto_recomendado")],
                "categorias_sugeridas": [x.get("producto_comprado")],
                "potential_monthly": 180_000,
            }
        pot = _float(client.get("potential_gap")) or 180_000
        tasks.append(_base_task(
            tipo="Cross Selling",
            client=client,
            motivo=str(x.get("motivo") or f"Compra {x.get('producto_comprado')}, no {x.get('producto_recomendado')}"),
            accion="Ofrecer producto complementario",
            prioridad=str(x.get("prioridad") or "media"),
            potencial=pot,
        ))

    for c in enriched_unique:
        st = str(c.get("status"))
        if st == "en_riesgo":
            drop = 0.0
            if _float(c.get("venta_anterior")) > 0:
                drop = (1 - _float(c.get("venta_actual")) / _float(c.get("venta_anterior"))) * 100
            tasks.append(_base_task(
                tipo="Cliente en Riesgo",
                client=c,
                motivo=f"Bajó {drop:.0f}% vs período anterior" if drop > 5 else "Actividad irregular",
                accion="Visitar y reforzar relación",
                prioridad="alta",
                potencial=_float(c.get("potential_monthly")),
            ))
        elif st == "nuevo":
            tasks.append(_base_task(
                tipo="Cliente Nuevo",
                client=c,
                motivo="Cliente nuevo — consolidar hábito de compra",
                accion="Visitar y ampliar mix",
                prioridad="media",
                potencial=_float(c.get("potential_monthly")) or _float(c.get("venta_actual")),
            ))
        elif c.get("segmento") == "VIP" and st in ("activo", "en_riesgo"):
            tasks.append(_base_task(
                tipo="Cliente VIP",
                client=c,
                motivo="Cliente de alto valor — seguimiento prioritario",
                accion="Visita prioritaria",
                prioridad="alta",
                potencial=_float(c.get("potential_monthly")),
            ))
        elif st == "activo":
            freq = _float(c.get("frecuencia_dias"))
            dias = _int(c.get("dias_sin_comprar"))
            if freq > 0 and dias > freq * 1.1:
                tasks.append(_base_task(
                    tipo="Frecuencia vencida",
                    client=c,
                    motivo=f"Compra cada ~{freq:.0f} días; lleva {dias} sin comprar",
                    accion="Llamar o visitar hoy",
                    prioridad="alta" if dias > freq * 1.5 else "media",
                    potencial=_float(c.get("potential_monthly")),
                ))

    tasks.sort(key=_task_sort_key)

    by_seller: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for t in tasks:
        sn = str(t.get("vendedor") or "Sin vendedor")
        by_seller[sn].append(t)

    vendedores = []
    for seller_name, seller_tasks in sorted(by_seller.items(), key=lambda kv: -sum(_float(t.get("potencial_economico")) for t in kv[1])):
        seller_tasks.sort(key=_task_sort_key)
        vendedores.append({
            "seller_name": seller_name,
            "tareas": seller_tasks[:25],
            "total_tareas": len(seller_tasks),
            "potencial_total": round(sum(_float(t.get("potencial_economico")) for t in seller_tasks[:25]), 0),
        })

    return {
        "tareas": tasks[:80],
        "vendedores": vendedores,
        "total_tareas": len(tasks),
    }


def build_alerts(
    enriched_unique: list[dict[str, Any]],
    enriched_lost: list[dict[str, Any]],
    sellers: list[dict[str, Any]],
    products: dict[str, Any],
) -> list[dict[str, Any]]:
    alerts: list[dict[str, Any]] = []

    for c in enriched_unique:
        if c.get("segmento") == "VIP" and str(c.get("status")) == "perdido":
            alerts.append({
                "tipo": "vip_perdido",
                "prioridad": "alta",
                "mensaje": f"Cliente VIP {c.get('client_name')} dejó de comprar.",
                "cliente": c.get("client_name"),
                "client_id": c.get("client_id"),
                "vendedor": c.get("seller_name"),
                "accion": "Visitar hoy",
            })
        elif str(c.get("status")) == "recuperado":
            alerts.append({
                "tipo": "cliente_recuperado",
                "prioridad": "media",
                "mensaje": f"{c.get('client_name')} fue recuperado — consolidar relación.",
                "cliente": c.get("client_name"),
                "client_id": c.get("client_id"),
                "vendedor": c.get("seller_name"),
                "accion": "Seguimiento de cortesía",
            })
        venta_ant = _float(c.get("venta_anterior"))
        venta = _float(c.get("venta_actual"))
        ticket = _float(c.get("ticket_promedio"))
        if venta_ant > 0 and venta > 0 and venta < venta_ant * 0.7:
            alerts.append({
                "tipo": "ticket_reducido",
                "prioridad": "media",
                "mensaje": f"{c.get('client_name')} redujo ticket significativamente.",
                "cliente": c.get("client_name"),
                "client_id": c.get("client_id"),
                "vendedor": c.get("seller_name"),
                "accion": "Investigar causa",
            })

    for s in sellers:
        if _float(s.get("variacion_pct")) <= -20:
            alerts.append({
                "tipo": "ruta_caida",
                "prioridad": "alta",
                "mensaje": f"Ruta de {s.get('seller_name')} cayó {_float(s.get('variacion_pct')):.0f}%.",
                "cliente": None,
                "client_id": None,
                "vendedor": s.get("seller_name"),
                "accion": "Revisar cartera y recuperación",
            })

    for p in (products.get("oportunidades") or [])[:5]:
        alerts.append({
            "tipo": "producto_oportunidad",
            "prioridad": "media",
            "mensaje": f"Producto {p.get('producto')} con baja cobertura ({p.get('brecha')} clientes sin vender).",
            "cliente": None,
            "client_id": None,
            "vendedor": None,
            "accion": "Impulsar en ruta",
        })

    for c in enriched_lost[:8]:
        if str(c.get("prioridad")) == "alta":
            alerts.append({
                "tipo": "cliente_perdido",
                "prioridad": "alta",
                "mensaje": f"{c.get('client_name')} dejó de comprar ({_int(c.get('dias_sin_comprar'))} días).",
                "cliente": c.get("client_name"),
                "client_id": c.get("client_id"),
                "vendedor": c.get("seller_name"),
                "accion": str(c.get("accion_sugerida") or "Recuperar"),
            })

    alerts.sort(key=lambda x: PRIORITY_ORDER.get(str(x.get("prioridad")), 9))
    return alerts[:40]


def build_radar_structured(
    classification: dict[str, int],
    enriched_unique: list[dict[str, Any]],
    enriched_lost: list[dict[str, Any]],
    cross: list[dict[str, Any]],
    products: dict[str, Any],
    opportunities: list[dict[str, Any]],
) -> dict[str, Any]:
    def _block(cantidad: int, monto: float, prioridad: str = "media") -> dict[str, Any]:
        return {"cantidad": cantidad, "monto": round(monto, 0), "prioridad": prioridad}

    riesgo = [u for u in enriched_unique if u.get("status") == "en_riesgo"]
    nuevos = [u for u in enriched_unique if u.get("status") == "nuevo"]
    vip = [u for u in enriched_unique if u.get("segmento") == "VIP"]
    cross_ops = [o for o in opportunities if o.get("tipo") == "cross_selling"]

    return {
        "clientes_perdidos": _block(
            classification.get("perdidos", len(enriched_lost)),
            sum(_float(x.get("potential_monthly")) for x in enriched_lost),
            "alta",
        ),
        "clientes_riesgo": _block(
            len(riesgo),
            sum(_float(x.get("potential_monthly")) for x in riesgo),
            "alta",
        ),
        "cross_selling": _block(
            len(cross_ops) or len(cross),
            sum(_float(x.get("monto_estimado")) for x in cross_ops) or len(cross) * 150_000,
            "media",
        ),
        "productos": _block(
            len(products.get("oportunidades") or []),
            len(products.get("oportunidades") or []) * 100_000,
            "media",
        ),
        "nuevos": _block(
            classification.get("nuevos", len(nuevos)),
            sum(_float(x.get("venta_actual")) for x in nuevos),
            "media",
        ),
        "vip": _block(
            len(vip),
            sum(_float(x.get("potential_monthly")) for x in vip),
            "alta",
        ),
        "oportunidades": _block(
            len(opportunities),
            sum(_float(x.get("monto_estimado")) for x in opportunities),
            "alta",
        ),
    }


def build_route_targets(
    sellers: list[dict[str, Any]],
    agenda: dict[str, Any],
    forecast: dict[str, Any],
    filters: Any,
) -> list[dict[str, Any]]:
    today = datetime.now(timezone.utc).date()
    days_in_month = monthrange(filters.date_to.year, filters.date_to.month)[1]
    days_elapsed = max(1, (min(filters.date_to, today) - filters.date_from).days + 1)

    agenda_by_seller = {v["seller_name"]: v for v in agenda.get("vendedores") or []}
    targets: list[dict[str, Any]] = []

    total_venta = sum(_float(s.get("venta_actual")) for s in sellers) or 1
    meta = _float(forecast.get("meta"))
    gap = _float(forecast.get("faltan"))

    for s in sellers:
        name = str(s.get("seller_name"))
        venta = _float(s.get("venta_actual"))
        share = venta / total_venta
        meta_ruta = meta * share
        proyeccion = (venta / days_elapsed) * days_in_month
        ag = agenda_by_seller.get(name, {})
        pendientes = _int(ag.get("total_tareas"))
        potencial = _float(ag.get("potencial_total"))
        cumplimiento = (proyeccion / meta_ruta * 100) if meta_ruta else 0

        targets.append({
            "seller_name": name,
            "meta": round(meta_ruta, 0),
            "venta_actual": round(venta, 0),
            "proyeccion": round(proyeccion, 0),
            "clientes_pendientes": pendientes,
            "potencial": round(potencial, 0),
            "cumplimiento_pct": round(cumplimiento, 1),
            "aporte_necesario": round(gap * share, 0) if gap > 0 else 0,
        })

    targets.sort(key=lambda x: -x["potencial"])
    return targets


def run_commercial_simulator(
    *,
    scenario: str,
    sellers: list[dict[str, Any]],
    enriched_unique: list[dict[str, Any]],
    enriched_lost: list[dict[str, Any]],
    cross: list[dict[str, Any]],
    curr_kpi: dict[str, Any],
    forecast: dict[str, Any],
    seller_filter: str | None = None,
    pct_recuperacion: float = 0.3,
    ticket_uplift_pct: float = 0.1,
    cross_clients: int = 10,
) -> dict[str, Any]:
    venta_base = _float(curr_kpi.get("venta_neta"))
    proyeccion_base = _float(forecast.get("proyeccion"))
    incremento = 0.0
    detalle: list[dict[str, Any]] = []

    scenario_key = (scenario or "").lower().strip()

    if scenario_key in ("recuperar_clientes", "recuperacion", "recuperar"):
        pool = enriched_lost
        if seller_filter:
            pool = [x for x in pool if x.get("seller_name") == seller_filter]
        n = max(1, int(len(pool) * pct_recuperacion))
        selected = sorted(pool, key=lambda x: -_float(x.get("potential_monthly")))[:n]
        incremento = sum(_float(x.get("potential_monthly")) for x in selected)
        by_seller: dict[str, float] = defaultdict(float)
        for x in selected:
            by_seller[str(x.get("seller_name") or "")] += _float(x.get("potential_monthly"))
        detalle = [{"seller_name": k, "incremento": round(v, 0)} for k, v in sorted(by_seller.items(), key=lambda kv: -kv[1])]

    elif scenario_key in ("subir_ticket", "ticket"):
        pool = enriched_unique
        if seller_filter:
            pool = [x for x in pool if x.get("seller_name") == seller_filter]
        incremento = sum(_float(x.get("venta_actual")) * ticket_uplift_pct for x in pool)
        by_seller = defaultdict(float)
        for x in pool:
            by_seller[str(x.get("seller_name") or "")] += _float(x.get("venta_actual")) * ticket_uplift_pct
        detalle = [{"seller_name": k, "incremento": round(v, 0)} for k, v in sorted(by_seller.items(), key=lambda kv: -kv[1])]

    elif scenario_key in ("cross_selling", "cross"):
        pool = cross[:cross_clients]
        if seller_filter:
            pool = [x for x in cross if x.get("seller_name") == seller_filter][:cross_clients]
        incremento = len(pool) * 45_000
        by_seller = defaultdict(float)
        for x in pool:
            by_seller[str(x.get("seller_name") or "")] += 45_000
        detalle = [{"seller_name": k, "incremento": round(v, 0)} for k, v in sorted(by_seller.items(), key=lambda kv: -kv[1])]

    else:
        incremento = 0.0

    venta_proyectada = proyeccion_base + incremento
    return {
        "scenario": scenario_key,
        "venta_periodo_actual": round(venta_base, 0),
        "proyeccion_base": round(proyeccion_base, 0),
        "incremento_esperado": round(incremento, 0),
        "venta_proyectada": round(venta_proyectada, 0),
        "impacto_por_vendedor": detalle,
        "nota": "Simulación — no modifica datos reales.",
    }


def build_predictive_layer(
    *,
    filters: Any,
    classification: dict[str, int],
    unique: list[dict[str, Any]],
    lost: list[dict[str, Any]],
    cross: list[dict[str, Any]],
    sellers: list[dict[str, Any]],
    products: dict[str, Any],
    opportunities: list[dict[str, Any]],
    forecast: dict[str, Any],
) -> dict[str, Any]:
    all_for_peers = list(unique) + [
        {**x, "venta_actual": _float(x.get("promedio_compra_mensual"))}
        for x in lost
    ]
    peer_medians = _peer_medians_by_comuna(all_for_peers)
    cross_lookup = _cross_lookup(cross)

    enriched_unique = [
        enrich_client_predictive(c, peer_medians=peer_medians, cross_by_client=cross_lookup)
        for c in unique
    ]
    enriched_lost = [
        enrich_client_predictive(
            {
                **x,
                "status": "perdido",
                "venta_actual": _float(x.get("promedio_compra_mensual")),
                "venta_anterior": _float(x.get("promedio_compra_mensual")),
                "compras_90d": 0,
                "client_score": compute_recovery_probability(
                    status="perdido",
                    dias_sin_comprar=x.get("dias_sin_comprar"),
                    ticket_promedio=_float(x.get("ticket_promedio")),
                    client_score=40,
                ),
            },
            peer_medians=peer_medians,
            cross_by_client=cross_lookup,
        )
        for x in lost
    ]

    agenda = build_agenda(enriched_unique, enriched_lost, cross)
    alerts = build_alerts(enriched_unique, enriched_lost, sellers, products)
    radar = build_radar_structured(classification, enriched_unique, enriched_lost, cross, products, opportunities)
    route_targets = build_route_targets(sellers, agenda, forecast, filters)

    return {
        "agenda": agenda,
        "alerts": alerts,
        "radar": radar,
        "route_targets": route_targets,
        "clients_enriched": enriched_unique[:120],
    }
