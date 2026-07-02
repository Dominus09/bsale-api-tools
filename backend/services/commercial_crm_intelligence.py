"""CRM inteligente Fase 3 — derivado del bundle sin márgenes ni costos."""

from __future__ import annotations

from calendar import monthrange
from datetime import date, datetime, timezone
from typing import Any

from backend.services.commercial_analytics_intelligence import (
    PRIORITY_ORDER,
    _clamp,
    _float,
    _int,
    format_clp,
    seller_score_status,
)

MISSION_TYPES = (
    "RECUPERAR",
    "CROSS_SELLING",
    "CLIENTE_EN_RIESGO",
    "PRODUCTO",
    "VIAJAR",
    "CLIENTE_VIP",
    "CLIENTE_NUEVO",
)


def _stars_from_impact(monto: float, prioridad: str) -> int:
    base = 2
    if monto >= 800_000:
        base = 5
    elif monto >= 400_000:
        base = 4
    elif monto >= 150_000:
        base = 3
    if prioridad == "alta":
        base = min(5, base + 1)
    elif prioridad == "baja":
        base = max(1, base - 1)
    return base


def _stars_label(n: int) -> str:
    return "★" * n + "☆" * (5 - n)


def explain_seller_score(seller: dict[str, Any]) -> dict[str, Any]:
    positives: list[str] = []
    negatives: list[str] = []
    if _float(seller.get("clientes_unicos_variacion_pct")) >= 0:
        positives.append("buenos clientes únicos")
    else:
        negatives.append("pérdida de clientes únicos")
    if _int(seller.get("clientes_recuperados")) >= 2:
        positives.append("buena recuperación")
    else:
        negatives.append("poca recuperación")
    if _float(seller.get("variacion_pct")) >= 0:
        positives.append("venta en crecimiento")
    else:
        negatives.append("venta en caída")
    if _int(seller.get("clientes_perdidos")) <= 3:
        positives.append("baja fuga de clientes")
    else:
        negatives.append(f"{_int(seller.get('clientes_perdidos'))} clientes perdidos")
    if _float(seller.get("ticket_promedio")) >= 120_000:
        positives.append("ticket sólido")
    else:
        negatives.append("ticket promedio bajo")
    score = _int(seller.get("commercial_score"))
    _, status_label = seller_score_status(score)
    return {
        "positives": positives[:3],
        "negatives": negatives[:3],
        "stars": round(score / 20, 1),
        "status_label": status_label,
    }


def build_estado_hoy(
    *,
    filters: Any,
    curr_kpi: dict[str, Any],
    prev_kpi: dict[str, Any],
    daily: list[dict[str, Any]],
    classification: dict[str, int],
    lost: list[dict[str, Any]],
    today_row: dict[str, Any] | None,
) -> dict[str, Any]:
    today = datetime.now(timezone.utc).date()
    days_in_month = monthrange(filters.date_to.year, filters.date_to.month)[1]
    days_elapsed = max(1, (min(filters.date_to, today) - filters.date_from).days + 1)
    venta_periodo = _float(curr_kpi.get("venta_neta"))
    venta_prev = _float(prev_kpi.get("venta_neta"))
    venta_hoy = _float((today_row or {}).get("venta_neta"))
    clientes_hoy = _int((today_row or {}).get("clientes"))
    venta_proyectada = (venta_periodo / days_elapsed) * days_in_month
    meta_mes = max(venta_prev * 1.05, venta_proyectada * 0.98)
    recuperacion = sum(
        _float(x.get("promedio_compra_mensual")) or _float(x.get("ticket_promedio"))
        for x in lost
        if x.get("prioridad") == "alta"
    )

    def _card(key: str, label: str, current: float, previous: float | None, fmt: str = "currency") -> dict[str, Any]:
        delta_pct = 0.0
        if previous and previous > 0:
            delta_pct = ((current - previous) / previous) * 100
        return {
            "key": key,
            "label": label,
            "value": current,
            "previous": previous,
            "delta_pct": round(delta_pct, 1),
            "format": fmt,
            "trend": "up" if delta_pct > 1 else "down" if delta_pct < -1 else "flat",
        }

    last_day = daily[-1] if daily else None
    venta_ayer = _float(last_day.get("venta_neta")) if last_day and len(daily) > 1 else venta_hoy * 0.9

    return {
        "cards": [
            _card("ventas_hoy", "Ventas hoy", venta_hoy, venta_ayer),
            _card("clientes_hoy", "Clientes activos hoy", float(clientes_hoy), None, "number"),
            _card("clientes_periodo", "Clientes activos período", float(_int(curr_kpi.get("clientes_unicos"))), float(_int(prev_kpi.get("clientes_unicos"))), "number"),
            _card("venta_proyectada", "Venta proyectada mes", venta_proyectada, venta_prev),
            _card("meta_mes", "Meta del mes", meta_mes, venta_prev),
            _card("forecast", "Forecast cumplimiento", (venta_proyectada / meta_mes * 100) if meta_mes else 0, None, "percent"),
            _card("recuperacion", "Potencial recuperación", recuperacion, None),
        ],
        "venta_proyectada": venta_proyectada,
        "meta_mes": meta_mes,
        "monto_recuperacion_potencial": recuperacion,
        "clientes_recuperados": classification.get("recuperados", 0),
        "clientes_perdidos": classification.get("perdidos", 0),
    }


def build_executive_cards(insights: list[dict[str, Any]]) -> list[dict[str, Any]]:
    emoji_map = {
        "vendedor": "🔴",
        "riesgo": "🟠",
        "recuperacion": "🔴",
        "producto": "🟠",
        "oportunidad": "🟢",
    }
    cards: list[dict[str, Any]] = []
    for ins in insights[:8]:
        cards.append({
            "emoji": emoji_map.get(str(ins.get("tipo")), "🔵"),
            "titulo": ins.get("titulo"),
            "descripcion": ins.get("descripcion"),
            "monto_estimado": ins.get("monto_estimado"),
            "prioridad": ins.get("prioridad"),
            "action_label": ins.get("action_label"),
            "seller": ins.get("seller"),
            "client_id": ins.get("client_id"),
            "tipo": ins.get("tipo"),
        })
    return cards


def build_daily_missions(
    attack_plan: dict[str, Any],
    opportunities: list[dict[str, Any]],
    unique: list[dict[str, Any]],
    sellers: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    missions: list[dict[str, Any]] = []

    for item in attack_plan.get("top_clients_to_recover", [])[:12]:
        monto = _float(item.get("monto_estimado"))
        pr = str(item.get("prioridad") or "media")
        stars = _stars_from_impact(monto, pr)
        missions.append({
            "mission_type": "RECUPERAR",
            "titulo": str(item.get("client_name") or "Cliente"),
            "subtitulo": item.get("motivo"),
            "monto_estimado": monto,
            "probabilidad": stars,
            "probabilidad_label": _stars_label(stars),
            "accion": item.get("accion"),
            "prioridad": pr,
            "client_id": item.get("client_id"),
            "seller_name": item.get("seller_name"),
            "impacto_economico": monto,
        })

    for item in attack_plan.get("top_cross_selling", [])[:10]:
        monto = _float(item.get("monto_estimado")) or 180_000
        pr = str(item.get("prioridad") or "media")
        stars = _stars_from_impact(monto, pr)
        missions.append({
            "mission_type": "CROSS_SELLING",
            "titulo": str(item.get("client_name") or "Cliente"),
            "subtitulo": item.get("motivo"),
            "producto_comprado": None,
            "producto_recomendado": item.get("producto"),
            "monto_estimado": monto,
            "probabilidad": stars,
            "probabilidad_label": _stars_label(stars),
            "accion": item.get("accion"),
            "prioridad": pr,
            "client_id": item.get("client_id"),
            "seller_name": item.get("seller_name"),
            "impacto_economico": monto,
        })

    for item in attack_plan.get("top_clients_at_risk", [])[:10]:
        monto = _float(item.get("monto_estimado"))
        pr = str(item.get("prioridad") or "alta")
        stars = _stars_from_impact(monto, pr)
        missions.append({
            "mission_type": "CLIENTE_EN_RIESGO",
            "titulo": str(item.get("client_name") or "Cliente"),
            "subtitulo": item.get("motivo"),
            "monto_estimado": monto,
            "probabilidad": stars,
            "probabilidad_label": _stars_label(stars),
            "accion": item.get("accion"),
            "prioridad": pr,
            "client_id": item.get("client_id"),
            "seller_name": item.get("seller_name"),
            "impacto_economico": monto,
        })

    for item in attack_plan.get("top_products_to_push", [])[:8]:
        monto = 120_000
        pr = str(item.get("prioridad") or "media")
        missions.append({
            "mission_type": "PRODUCTO",
            "titulo": str(item.get("producto") or "Producto"),
            "subtitulo": item.get("motivo"),
            "monto_estimado": monto,
            "probabilidad": _stars_from_impact(monto, pr),
            "probabilidad_label": _stars_label(_stars_from_impact(monto, pr)),
            "accion": item.get("accion"),
            "prioridad": pr,
            "impacto_economico": monto,
        })

    for s in attack_plan.get("top_sellers_to_review", [])[:6]:
        pending = _int(s.get("client_id"))  # not used
        lost_count = 0
        for sl in sellers:
            if sl.get("seller_name") == s.get("seller_name"):
                lost_count = _int(sl.get("clientes_perdidos"))
                break
        monto = _float(s.get("monto_estimado"))
        missions.append({
            "mission_type": "VIAJAR",
            "titulo": str(s.get("seller_name") or "Zona"),
            "subtitulo": s.get("motivo"),
            "detalle": f"Hay {max(lost_count, 3)} clientes pendientes en cartera.",
            "monto_estimado": monto,
            "probabilidad": 4,
            "probabilidad_label": _stars_label(4),
            "accion": s.get("accion"),
            "prioridad": str(s.get("prioridad") or "media"),
            "seller_name": s.get("seller_name"),
            "impacto_economico": monto,
        })

    for c in [u for u in unique if u.get("status") == "nuevo"][:5]:
        monto = _float(c.get("venta_actual")) or _float(c.get("ticket_promedio"))
        missions.append({
            "mission_type": "CLIENTE_NUEVO",
            "titulo": str(c.get("client_name")),
            "subtitulo": "Cliente nuevo — consolidar relación comercial.",
            "monto_estimado": monto,
            "probabilidad": 4,
            "probabilidad_label": _stars_label(4),
            "accion": "Visitar y ampliar mix",
            "prioridad": "media",
            "client_id": c.get("client_id"),
            "seller_name": c.get("seller_name"),
            "impacto_economico": monto,
        })

    for o in opportunities:
        if o.get("tipo") != "cliente_vip":
            continue
        missions.append({
            "mission_type": "CLIENTE_VIP",
            "titulo": str(o.get("cliente")),
            "subtitulo": o.get("explicacion"),
            "monto_estimado": _float(o.get("monto_estimado")),
            "probabilidad": 5,
            "probabilidad_label": _stars_label(5),
            "accion": o.get("accion_sugerida"),
            "prioridad": "alta",
            "client_id": o.get("client_id"),
            "seller_name": o.get("vendedor"),
            "impacto_economico": _float(o.get("monto_estimado")),
        })

    missions.sort(key=lambda x: -_float(x.get("impacto_economico")))
    return missions[:40]


def build_forecast(
    *,
    filters: Any,
    curr_kpi: dict[str, Any],
    prev_kpi: dict[str, Any],
    sellers: list[dict[str, Any]],
) -> dict[str, Any]:
    today = datetime.now(timezone.utc).date()
    days_in_month = monthrange(filters.date_to.year, filters.date_to.month)[1]
    days_elapsed = max(1, (min(filters.date_to, today) - filters.date_from).days + 1)
    venta_periodo = _float(curr_kpi.get("venta_neta"))
    venta_prev = _float(prev_kpi.get("venta_neta"))
    proyeccion = (venta_periodo / days_elapsed) * days_in_month
    meta = max(venta_prev * 1.05, proyeccion * 0.95)
    gap = max(0, meta - proyeccion)
    cumplimiento = (proyeccion / meta * 100) if meta else 0

    seller_gaps: list[dict[str, Any]] = []
    if sellers:
        total_venta = sum(_float(s.get("venta_actual")) for s in sellers) or 1
        for s in sellers:
            share = _float(s.get("venta_actual")) / total_venta
            aporte = gap * share
            if aporte > 10_000:
                seller_gaps.append({
                    "seller_name": s.get("seller_name"),
                    "aporte_necesario": round(aporte, 0),
                    "venta_actual": _float(s.get("venta_actual")),
                    "variacion_pct": _float(s.get("variacion_pct")),
                })
        seller_gaps.sort(key=lambda x: -x["aporte_necesario"])

    return {
        "meta": round(meta, 0),
        "proyeccion": round(proyeccion, 0),
        "cumplimiento_pct": round(cumplimiento, 1),
        "faltan": round(gap, 0),
        "seller_aportes": seller_gaps[:10],
    }


def build_radar_comercial(
    classification: dict[str, int],
    opportunities: list[dict[str, Any]],
    lost: list[dict[str, Any]],
    cross: list[dict[str, Any]],
    unique: list[dict[str, Any]],
    products: dict[str, Any],
) -> list[dict[str, Any]]:
    def _sum_monto(items: list[dict[str, Any]]) -> float:
        return sum(_float(x.get("monto_estimado")) for x in items)

    nuevos = [u for u in unique if u.get("status") == "nuevo"]
    recuperados = [u for u in unique if u.get("status") == "recuperado"]
    cross_ops = [o for o in opportunities if o.get("tipo") == "cross_selling"]

    blocks = [
        {
            "id": "perdidos",
            "titulo": "Clientes Perdidos",
            "cantidad": classification.get("perdidos", len(lost)),
            "monto": _sum_monto([{"monto_estimado": x.get("promedio_compra_mensual")} for x in lost]),
            "prioridad": "alta",
            "color": "red",
        },
        {
            "id": "recuperados",
            "titulo": "Clientes Recuperados",
            "cantidad": classification.get("recuperados", len(recuperados)),
            "monto": sum(_float(x.get("venta_actual")) for x in recuperados),
            "prioridad": "media",
            "color": "purple",
        },
        {
            "id": "cross_selling",
            "titulo": "Cross Selling",
            "cantidad": len(cross_ops) or len(cross),
            "monto": _sum_monto(cross_ops) or len(cross) * 150_000,
            "prioridad": "media",
            "color": "blue",
        },
        {
            "id": "productos_nuevos",
            "titulo": "Productos Oportunidad",
            "cantidad": len(products.get("oportunidades") or []),
            "monto": len(products.get("oportunidades") or []) * 100_000,
            "prioridad": "media",
            "color": "amber",
        },
        {
            "id": "oportunidades",
            "titulo": "Oportunidades",
            "cantidad": len(opportunities),
            "monto": _sum_monto(opportunities),
            "prioridad": "alta",
            "color": "emerald",
        },
    ]
    return blocks


def build_ranking_comercial(sellers: list[dict[str, Any]]) -> list[dict[str, Any]]:
    ranking: list[dict[str, Any]] = []
    for s in sellers:
        expl = explain_seller_score(s)
        ranking.append({
            **s,
            "score_explanation": expl,
            "stars": expl["stars"],
        })
    return ranking


def build_ia_comercial(
    sellers: list[dict[str, Any]],
    lost: list[dict[str, Any]],
    cross: list[dict[str, Any]],
    unique: list[dict[str, Any]],
    classification: dict[str, int],
    forecast: dict[str, Any],
) -> list[dict[str, Any]]:
    narratives: list[dict[str, Any]] = []

    for s in sorted(sellers, key=lambda x: _int(x.get("clientes_perdidos")), reverse=True)[:4]:
        if _int(s.get("clientes_perdidos")) < 2:
            continue
        rec_monto = sum(
            _float(x.get("promedio_compra_mensual"))
            for x in lost
            if x.get("seller_name") == s.get("seller_name")
        )
        narratives.append({
            "seller_name": s.get("seller_name"),
            "parrafos": [
                f"Este período perdiste presencia con {_int(s.get('clientes_perdidos'))} clientes.",
                f"Recuperando los más valiosos podrías recuperar aproximadamente {format_clp(rec_monto)}.",
            ],
            "monto_estimado": rec_monto,
        })

    cross_by_seller: dict[str, int] = {}
    for x in cross:
        sn = str(x.get("seller_name") or "")
        cross_by_seller[sn] = cross_by_seller.get(sn, 0) + 1
    for sn, cnt in sorted(cross_by_seller.items(), key=lambda kv: -kv[1])[:3]:
        if cnt < 3:
            continue
        narratives.append({
            "seller_name": sn,
            "parrafos": [
                f"Hay {cnt} clientes con oportunidad de cross-selling sin atender.",
                f"Existe una oportunidad estimada de {format_clp(cnt * 45_000)}.",
            ],
            "monto_estimado": cnt * 45_000,
        })

    if _float(forecast.get("faltan")) > 0:
        narratives.insert(0, {
            "seller_name": None,
            "parrafos": [
                f"La proyección del mes está al {forecast.get('cumplimiento_pct')}%.",
                f"Faltan {format_clp(_float(forecast.get('faltan')))} para alcanzar la meta comercial.",
            ],
            "monto_estimado": _float(forecast.get("faltan")),
        })

    if classification.get("recuperados", 0) > 0:
        narratives.append({
            "seller_name": None,
            "parrafos": [
                f"Se recuperaron {classification.get('recuperados')} clientes en el período.",
                "Conviene consolidar esos clientes para evitar una nueva fuga.",
            ],
            "monto_estimado": None,
        })

    return narratives[:8]


def build_timeline_from_monthly(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "meses": [
            {
                "mes": r.get("mes"),
                "venta": _float(r.get("venta")),
                "clientes": _int(r.get("clientes")),
                "ticket_promedio": _float(r.get("ticket_promedio")),
                "documentos": _int(r.get("documentos")),
            }
            for r in rows
        ],
    }


def build_objetivos_diarios(missions: list[dict[str, Any]]) -> dict[str, Any]:
    recover = [m for m in missions if m.get("mission_type") == "RECUPERAR"]
    cross = [m for m in missions if m.get("mission_type") == "CROSS_SELLING"]
    visitar = [m for m in missions if m.get("mission_type") in ("CLIENTE_EN_RIESGO", "VIAJAR", "CLIENTE_VIP")]
    potencial = sum(_float(m.get("monto_estimado")) for m in missions[:15])
    return {
        "titulo": "Tu plan hoy",
        "visitar_clientes": min(len(visitar) + len(recover), 20),
        "recuperar_clientes": min(len(recover), 10),
        "cross_selling": min(len(cross), 15),
        "monto_potencial": round(potencial, 0),
        "misiones_prioritarias": missions[:12],
    }


def build_gamificacion(sellers: list[dict[str, Any]]) -> dict[str, Any]:
    if not sellers:
        return {"badges": []}
    by_venta = max(sellers, key=lambda x: _float(x.get("venta_actual")))
    by_rec = max(sellers, key=lambda x: _int(x.get("clientes_recuperados")))
    by_growth = max(sellers, key=lambda x: _float(x.get("variacion_pct")))
    by_ticket = max(sellers, key=lambda x: _float(x.get("ticket_promedio")))
    by_cu = max(sellers, key=lambda x: _int(x.get("clientes_unicos_actual")))
    by_score = max(sellers, key=lambda x: _int(x.get("commercial_score")))

    def _badge(label: str, seller: dict[str, Any], metric: str) -> dict[str, Any]:
        return {"label": label, "seller_name": seller.get("seller_name"), "metric": metric}

    return {
        "badges": [
            _badge("Top vendedor", by_venta, format_clp(_float(by_venta.get("venta_actual")))),
            _badge("Mayor recuperación", by_rec, f"{_int(by_rec.get('clientes_recuperados'))} clientes"),
            _badge("Mayor crecimiento", by_growth, f"{_float(by_growth.get('variacion_pct')):.0f}%"),
            _badge("Mayor ticket", by_ticket, format_clp(_float(by_ticket.get("ticket_promedio")))),
            _badge("Mayor cobertura", by_cu, f"{_int(by_cu.get('clientes_unicos_actual'))} clientes"),
            _badge("Mejor score", by_score, f"{_int(by_score.get('commercial_score'))} pts"),
        ],
    }


def build_actividad_reciente(
    unique: list[dict[str, Any]],
    lost: list[dict[str, Any]],
    sellers: list[dict[str, Any]],
    daily: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for c in [u for u in unique if u.get("status") == "recuperado"][:5]:
        events.append({
            "hora": "08:20",
            "texto": f"{c.get('seller_name')} recuperó a {c.get('client_name')}.",
            "tipo": "recuperacion",
            "client_id": c.get("client_id"),
        })
    for c in [u for u in unique if u.get("status") == "nuevo"][:5]:
        events.append({
            "hora": "09:15",
            "texto": f"{c.get('seller_name')} incorporó cliente nuevo {c.get('client_name')}.",
            "tipo": "nuevo",
            "client_id": c.get("client_id"),
        })
    for c in lost[:5]:
        if c.get("prioridad") == "alta":
            events.append({
                "hora": "10:42",
                "texto": f"Cliente importante {c.get('client_name')} dejó de comprar.",
                "tipo": "alerta",
                "client_id": c.get("client_id"),
            })
    if daily:
        best = max(daily, key=lambda x: _float(x.get("venta_neta")))
        events.append({
            "hora": "11:30",
            "texto": f"Pico de venta el {best.get('day')}: {format_clp(_float(best.get('venta_neta')))}.",
            "tipo": "record",
        })
    return events[:12]


def enrich_opportunities_vip(unique: list[dict[str, Any]], opportunities: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = list(opportunities)
    for c in unique:
        if _float(c.get("ticket_promedio")) < 200_000 and _float(c.get("venta_actual")) < 500_000:
            continue
        if c.get("status") not in ("activo", "en_riesgo", "perdido"):
            continue
        out.append({
            "tipo": "cliente_vip",
            "titulo": f"VIP: {c.get('client_name')}",
            "cliente": c.get("client_name"),
            "vendedor": c.get("seller_name"),
            "producto": None,
            "categoria": None,
            "monto_estimado": _float(c.get("venta_actual")) or _float(c.get("ticket_promedio")),
            "prioridad": "alta",
            "explicacion": "Cliente de alto valor — priorizar seguimiento.",
            "accion_sugerida": "Visita prioritaria",
            "client_id": c.get("client_id"),
        })
    out.sort(key=lambda x: (PRIORITY_ORDER.get(str(x.get("prioridad")), 9), -_float(x.get("monto_estimado"))))
    return out[:90]


def build_crm_layer(
    *,
    filters: Any,
    scope: Any,
    curr_kpi: dict[str, Any],
    prev_kpi: dict[str, Any],
    daily: list[dict[str, Any]],
    classification: dict[str, int],
    dashboard_kpis: dict[str, Any],
    summary: dict[str, Any],
    attack_plan: dict[str, Any],
    opportunities: list[dict[str, Any]],
    sellers: list[dict[str, Any]],
    unique: list[dict[str, Any]],
    lost: list[dict[str, Any]],
    cross: list[dict[str, Any]],
    products: dict[str, Any],
    today_row: dict[str, Any] | None,
    monthly_timeline: list[dict[str, Any]],
) -> dict[str, Any]:
    opportunities = enrich_opportunities_vip(unique, opportunities)
    forecast = build_forecast(filters=filters, curr_kpi=curr_kpi, prev_kpi=prev_kpi, sellers=sellers)
    missions = build_daily_missions(attack_plan, opportunities, unique, sellers)

    from backend.services.commercial_predictive_intelligence import build_predictive_layer

    predictive = build_predictive_layer(
        filters=filters,
        classification=classification,
        unique=unique,
        lost=lost,
        cross=cross,
        sellers=sellers,
        products=products,
        opportunities=opportunities,
        forecast=forecast,
    )

    return {
        "estado_hoy": build_estado_hoy(
            filters=filters,
            curr_kpi=curr_kpi,
            prev_kpi=prev_kpi,
            daily=daily,
            classification=classification,
            lost=lost,
            today_row=today_row,
        ),
        "executive_cards": build_executive_cards(summary.get("insights") or []),
        "daily_missions": missions,
        "forecast": forecast,
        "radar_blocks": build_radar_comercial(classification, opportunities, lost, cross, unique, products),
        "radar": predictive["radar"],
        "ranking": build_ranking_comercial(sellers),
        "ia_comercial": build_ia_comercial(sellers, lost, cross, unique, classification, forecast),
        "timeline": build_timeline_from_monthly(monthly_timeline),
        "objetivos_diarios": build_objetivos_diarios(missions),
        "gamificacion": build_gamificacion(sellers),
        "actividad_reciente": build_actividad_reciente(unique, lost, sellers, daily),
        "opportunities": opportunities,
        "agenda": predictive["agenda"],
        "alerts": predictive["alerts"],
        "route_targets": predictive["route_targets"],
        "clients_enriched": predictive["clients_enriched"],
    }
