"""Scores, insights accionables, plan de ataque y oportunidades comerciales."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

COMPANY_ID = 3
OFFICE_ID = 1

PRIORITY_ORDER = {"alta": 0, "media": 1, "baja": 2}


def _float(v: Any) -> float:
    if v is None:
        return 0.0
    return float(v)


def _int(v: Any) -> int:
    if v is None:
        return 0
    return int(v)


def _clamp(n: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, n))


def _norm(value: float, lo: float, hi: float) -> float:
    if hi <= lo:
        return 0.5 if value > 0 else 0.0
    return _clamp((value - lo) / (hi - lo))


def seller_score_status(score: int) -> tuple[str, str]:
    if score >= 80:
        return "excelente", "🟢 Excelente"
    if score >= 60:
        return "vigilar", "🟡 Vigilar"
    return "revisar", "🔴 Revisar"


def compute_seller_scores(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not items:
        return []

    ventas = [_float(x.get("venta_actual")) for x in items]
    clientes = [_float(x.get("clientes_unicos_actual")) for x in items]
    tickets = [_float(x.get("ticket_promedio")) for x in items]
    recuperados = [_float(x.get("clientes_recuperados")) for x in items]
    perdidos = [_float(x.get("clientes_perdidos")) for x in items]

    v_lo, v_hi = min(ventas), max(ventas)
    c_lo, c_hi = min(clientes), max(clientes)
    t_lo, t_hi = min(tickets), max(tickets)
    r_lo, r_hi = min(recuperados), max(recuperados)
    p_lo, p_hi = min(perdidos), max(perdidos)

    enriched: list[dict[str, Any]] = []
    for item in items:
        growth = _float(item.get("variacion_pct"))
        growth_norm = _clamp((growth + 50.0) / 100.0)
        perdidos_inv = 1.0 - _norm(_float(item.get("clientes_perdidos")), p_lo, p_hi)

        raw = (
            _norm(_float(item.get("venta_actual")), v_lo, v_hi) * 25
            + _norm(_float(item.get("clientes_unicos_actual")), c_lo, c_hi) * 20
            + growth_norm * 15
            + _norm(_float(item.get("clientes_recuperados")), r_lo, r_hi) * 15
            + perdidos_inv * 15
            + _norm(_float(item.get("ticket_promedio")), t_lo, t_hi) * 10
        )
        score = int(round(_clamp(raw, 0, 100)))

        cu_var = 0.0
        prev_cu = _int(item.get("clientes_unicos_anterior"))
        curr_cu = _int(item.get("clientes_unicos_actual"))
        if prev_cu > 0:
            cu_var = ((curr_cu - prev_cu) / prev_cu) * 100.0

        action_parts: list[str] = []
        if _int(item.get("clientes_perdidos")) > 0:
            action_parts.append(f"revisar {_int(item.get('clientes_perdidos'))} clientes perdidos")
        if score < 60:
            action_parts.append("apoyar cobertura comercial")
        if growth < -10:
            action_parts.append("analizar caída de venta")
        if not action_parts:
            action_parts.append("mantener ritmo y buscar cross-selling")

        status_key, status_label = seller_score_status(score)
        enriched.append({
            **item,
            "commercial_score": score,
            "score_status": status_key,
            "score_status_label": status_label,
            "clientes_unicos_variacion_pct": round(cu_var, 1),
            "accion_sugerida": action_parts[0].capitalize() + (
                f" y {action_parts[1]}" if len(action_parts) > 1 else ""
            ) + ".",
        })
    enriched.sort(key=lambda x: x.get("commercial_score", 0), reverse=True)
    return enriched


def client_health_from_status(status: str) -> tuple[str, str]:
    mapping = {
        "activo": ("saludable", "🟢 Saludable"),
        "nuevo": ("nuevo", "🔵 Nuevo"),
        "recuperado": ("recuperado", "🟣 Recuperado"),
        "perdido": ("perdido", "🔴 Perdido"),
        "en_riesgo": ("en_riesgo", "🟡 En riesgo"),
    }
    return mapping.get(status, ("en_riesgo", "🟡 En riesgo"))


def compute_client_score(
    *,
    status: str,
    venta_actual: float,
    venta_anterior: float,
    dias_sin_comprar: int | None,
    compras_90d: int,
    ticket_promedio: float,
) -> int:
    base = {
        "activo": 78,
        "nuevo": 72,
        "recuperado": 68,
        "en_riesgo": 48,
        "perdido": 22,
    }.get(status, 50)

    if venta_anterior > 0:
        drop_pct = ((venta_actual - venta_anterior) / venta_anterior) * 100
        if drop_pct >= 20:
            base -= 12
        elif drop_pct <= -10:
            base += 8

    if compras_90d >= 4:
        base += 8
    elif compras_90d >= 2:
        base += 4

    if dias_sin_comprar is not None and status != "perdido":
        base -= min(25, int(dias_sin_comprar / 2))

    if ticket_promedio >= 150_000:
        base += 5

    return int(_clamp(base, 0, 100))


def enrich_client_row(row: dict[str, Any]) -> dict[str, Any]:
    status = str(row.get("status") or "en_riesgo")
    health_key, health_label = client_health_from_status(status)
    venta_actual = _float(row.get("venta_actual"))
    venta_anterior = _float(row.get("venta_anterior"))
    dias = row.get("dias_sin_comprar")
    dias_i = _int(dias) if dias is not None else None
    compras_90d = _int(row.get("compras_90d"))
    ticket = _float(row.get("ticket_promedio"))

    score = compute_client_score(
        status=status,
        venta_actual=venta_actual,
        venta_anterior=venta_anterior,
        dias_sin_comprar=dias_i,
        compras_90d=compras_90d,
        ticket_promedio=ticket,
    )
    return {
        **row,
        "client_score": score,
        "client_health": health_key,
        "client_health_label": health_label,
        "venta_anterior": venta_anterior,
        "dias_sin_comprar": dias_i,
        "compras_90d": compras_90d,
        "ticket_promedio": ticket,
    }


def _insight(
    *,
    tipo: str,
    prioridad: str,
    titulo: str,
    descripcion: str,
    action_label: str,
    monto_estimado: float | None = None,
    seller: str | None = None,
    client_id: int | None = None,
) -> dict[str, Any]:
    return {
        "tipo": tipo,
        "prioridad": prioridad,
        "titulo": titulo,
        "descripcion": descripcion,
        "monto_estimado": monto_estimado,
        "seller": seller,
        "client_id": client_id,
        "action_label": action_label,
    }


def build_actionable_summary(
    scope_title: str,
    sellers: list[dict[str, Any]],
    lost: list[dict[str, Any]],
    unique: list[dict[str, Any]],
    products: dict[str, Any],
    cross: list[dict[str, Any]],
    classification: dict[str, int],
    dashboard_kpis: dict[str, Any],
) -> dict[str, Any]:
    insights: list[dict[str, Any]] = []

    for s in sorted(sellers, key=lambda x: x.get("commercial_score", 100)):
        cu_var = _float(s.get("clientes_unicos_variacion_pct"))
        if cu_var <= -10 or _int(s.get("clientes_perdidos")) >= 5:
            insights.append(_insight(
                tipo="vendedor",
                prioridad="alta" if cu_var <= -15 or _int(s.get("clientes_perdidos")) >= 8 else "media",
                titulo=f"Revisar a {s.get('seller_name')}",
                descripcion=(
                    f"Bajó {abs(cu_var):.0f}% en clientes únicos"
                    if cu_var < 0
                    else f"Tiene {_int(s.get('clientes_perdidos'))} clientes perdidos"
                ),
                action_label="Revisar cartera",
                seller=str(s.get("seller_name")),
            ))

    high_value_lost = [x for x in lost if x.get("prioridad") == "alta"][:6]
    if high_value_lost:
        insights.append(_insight(
            tipo="recuperacion",
            prioridad="alta",
            titulo=f"Recuperar {len(high_value_lost)} clientes de alto valor",
            descripcion="Clientes con historial fuerte que dejaron de comprar en el período.",
            action_label="Visitar o llamar",
            monto_estimado=sum(_float(x.get("promedio_compra_mensual")) for x in high_value_lost),
        ))

    for gap in (products.get("oportunidades") or [])[:3]:
        insights.append(_insight(
            tipo="producto",
            prioridad="media",
            titulo=f"Impulsar {gap.get('producto')}",
            descripcion=f"Baja cobertura: brecha de {_int(gap.get('brecha'))} clientes vs empresa.",
            action_label="Empujar en ruta",
            monto_estimado=None,
        ))

    at_risk = [u for u in unique if u.get("status") == "en_riesgo"]
    if at_risk:
        insights.append(_insight(
            tipo="riesgo",
            prioridad="alta" if len(at_risk) >= 10 else "media",
            titulo=f"Visitar {len(at_risk)} clientes en riesgo",
            descripcion="Clientes habituales con frecuencia de compra deteriorada.",
            action_label="Visitar esta semana",
        ))

    caidas = products.get("caida_fuerte") or []
    if caidas:
        insights.append(_insight(
            tipo="producto",
            prioridad="media",
            titulo="Revisar productos con caída fuerte",
            descripcion=f"{len(caidas)} productos cayeron más del 50% vs período anterior.",
            action_label="Revisar mix",
        ))

    if cross:
        insights.append(_insight(
            tipo="oportunidad",
            prioridad="media",
            titulo=f"Atacar {len(cross)} oportunidades cross-selling",
            descripcion="Clientes con compras complementarias pendientes.",
            action_label="Ofrecer en visita",
        ))

    venta_pct = _float(dashboard_kpis.get("venta_neta", {}).get("delta_pct"))
    if venta_pct < -5:
        insights.insert(0, _insight(
            tipo="riesgo",
            prioridad="alta",
            titulo="Venta en caída",
            descripcion=f"La venta neta bajó {abs(venta_pct):.1f}% vs período anterior.",
            action_label="Activar plan de recuperación",
        ))

    insights.sort(key=lambda x: (PRIORITY_ORDER.get(str(x.get("prioridad")), 9), x.get("titulo", "")))
    bullets = [f"{i + 1}. {x['titulo']}: {x['descripcion']}." for i, x in enumerate(insights[:8])]

    return {
        "title": scope_title,
        "bullets": bullets,
        "insights": insights[:12],
    }


def build_attack_plan(
    sellers: list[dict[str, Any]],
    lost: list[dict[str, Any]],
    unique: list[dict[str, Any]],
    cross: list[dict[str, Any]],
    products: dict[str, Any],
) -> dict[str, Any]:
    def _plan_item(
        *,
        prioridad: str,
        motivo: str,
        accion: str,
        monto_estimado: float | None = None,
        client_id: int | None = None,
        client_name: str | None = None,
        seller_name: str | None = None,
        producto: str | None = None,
    ) -> dict[str, Any]:
        return {
            "prioridad": prioridad,
            "motivo": motivo,
            "accion": accion,
            "monto_estimado": monto_estimado,
            "client_id": client_id,
            "client_name": client_name,
            "seller_name": seller_name,
            "producto": producto,
        }

    recover = []
    for c in sorted(lost, key=lambda x: _float(x.get("ticket_promedio")), reverse=True)[:15]:
        monto = _float(c.get("promedio_compra_mensual")) or _float(c.get("ticket_promedio"))
        recover.append(_plan_item(
            prioridad=str(c.get("prioridad") or "media"),
            motivo=(
                f"Compraba {format_clp(monto)} mensual y lleva "
                f"{_int(c.get('dias_sin_comprar'))} días sin comprar."
            ),
            accion=str(c.get("accion_sugerida") or "Visitar o llamar hoy"),
            monto_estimado=monto,
            client_id=_int(c.get("client_id")),
            client_name=str(c.get("client_name")),
            seller_name=str(c.get("seller_name")),
        ))

    at_risk = []
    for c in sorted(
        [u for u in unique if u.get("status") == "en_riesgo"],
        key=lambda x: x.get("client_score", 0),
    )[:15]:
        at_risk.append(_plan_item(
            prioridad="alta" if _int(c.get("client_score")) < 45 else "media",
            motivo=f"Cliente en riesgo — {c.get('dias_sin_comprar', '?')} días sin comprar.",
            accion="Visitar antes del viernes",
            monto_estimado=_float(c.get("venta_actual")),
            client_id=_int(c.get("client_id")),
            client_name=str(c.get("client_name")),
            seller_name=str(c.get("seller_name")),
        ))

    cross_plan = []
    for x in cross[:20]:
        cross_plan.append(_plan_item(
            prioridad=str(x.get("prioridad") or "media"),
            motivo=str(x.get("motivo")),
            accion="Ofrecer producto complementario",
            monto_estimado=None,
            client_id=_int(x.get("client_id")),
            client_name=str(x.get("client_name")),
            seller_name=str(x.get("seller_name")),
            producto=str(x.get("producto_recomendado")),
        ))

    products_push = []
    for p in (products.get("oportunidades") or [])[:15]:
        products_push.append(_plan_item(
            prioridad="alta" if _int(p.get("brecha")) >= 20 else "media",
            motivo=f"Solo {_int(p.get('clientes_vendedor'))} clientes del vendedor vs {_int(p.get('clientes_empresa'))} en empresa.",
            accion="Impulsar en ruta y clientes objetivo",
            monto_estimado=None,
            producto=str(p.get("producto")),
        ))

    sellers_review = []
    for s in sorted(sellers, key=lambda x: x.get("commercial_score", 100))[:10]:
        if _int(s.get("commercial_score")) >= 80:
            continue
        sellers_review.append(_plan_item(
            prioridad="alta" if _int(s.get("commercial_score")) < 60 else "media",
            motivo=(
                f"Score {s.get('commercial_score')} — {s.get('score_status_label')}. "
                f"Perdidos: {_int(s.get('clientes_perdidos'))}."
            ),
            accion=str(s.get("accion_sugerida") or "Revisar cartera"),
            monto_estimado=_float(s.get("venta_actual")),
            seller_name=str(s.get("seller_name")),
        ))

    return {
        "top_clients_to_recover": recover,
        "top_clients_at_risk": at_risk,
        "top_cross_selling": cross_plan,
        "top_products_to_push": products_push,
        "top_sellers_to_review": sellers_review,
    }


def build_opportunities(
    lost: list[dict[str, Any]],
    unique: list[dict[str, Any]],
    cross: list[dict[str, Any]],
    products: dict[str, Any],
    sellers: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []

    for x in cross:
        out.append({
            "tipo": "cross_selling",
            "titulo": f"Ofrecer {x.get('producto_recomendado')}",
            "cliente": x.get("client_name"),
            "vendedor": x.get("seller_name"),
            "producto": x.get("producto_recomendado"),
            "categoria": x.get("producto_comprado"),
            "monto_estimado": None,
            "prioridad": x.get("prioridad"),
            "explicacion": x.get("motivo"),
            "accion_sugerida": "Ofrecer en próxima visita",
            "client_id": x.get("client_id"),
        })

    for c in lost[:25]:
        monto = _float(c.get("promedio_compra_mensual")) or _float(c.get("ticket_promedio"))
        out.append({
            "tipo": "cliente_perdido",
            "titulo": f"Recuperar {c.get('client_name')}",
            "cliente": c.get("client_name"),
            "vendedor": c.get("seller_name"),
            "producto": ", ".join((c.get("productos_habituales") or [])[:2]),
            "categoria": None,
            "monto_estimado": monto,
            "prioridad": c.get("prioridad"),
            "explicacion": f"Lleva {_int(c.get('dias_sin_comprar'))} días sin comprar.",
            "accion_sugerida": c.get("accion_sugerida"),
            "client_id": c.get("client_id"),
        })

    for c in [u for u in unique if u.get("status") == "en_riesgo"][:20]:
        out.append({
            "tipo": "cliente_en_riesgo",
            "titulo": f"Cliente en riesgo: {c.get('client_name')}",
            "cliente": c.get("client_name"),
            "vendedor": c.get("seller_name"),
            "producto": None,
            "categoria": None,
            "monto_estimado": _float(c.get("venta_actual")),
            "prioridad": "alta" if _int(c.get("client_score")) < 45 else "media",
            "explicacion": "Frecuencia de compra deteriorada vs patrón habitual.",
            "accion_sugerida": "Visitar esta semana",
            "client_id": c.get("client_id"),
        })

    for p in (products.get("caida_fuerte") or [])[:15]:
        out.append({
            "tipo": "producto_caida",
            "titulo": f"Caída: {p.get('producto') or 'Producto'}",
            "cliente": None,
            "vendedor": products.get("seller"),
            "producto": p.get("producto"),
            "categoria": None,
            "monto_estimado": _float(p.get("venta_anterior")) - _float(p.get("venta_actual")),
            "prioridad": "media",
            "explicacion": f"Caída de {_float(p.get('variacion_pct')):.0f}% vs período anterior.",
            "accion_sugerida": "Revisar precio, stock y exhibición",
            "client_id": None,
        })

    for p in (products.get("oportunidades") or [])[:15]:
        out.append({
            "tipo": "producto_no_vendido_por_vendedor",
            "titulo": f"Oportunidad: {p.get('producto')}",
            "cliente": None,
            "vendedor": products.get("seller"),
            "producto": p.get("producto"),
            "categoria": None,
            "monto_estimado": None,
            "prioridad": "alta" if _int(p.get("brecha")) >= 25 else "media",
            "explicacion": f"Brecha de {_int(p.get('brecha'))} clientes vs cobertura empresa.",
            "accion_sugerida": "Incluir en oferta de ruta",
            "client_id": None,
        })

    for s in sellers:
        if _float(s.get("clientes_unicos_variacion_pct")) >= -5:
            continue
        out.append({
            "tipo": "vendedor",
            "titulo": f"Apoyar a {s.get('seller_name')}",
            "cliente": None,
            "vendedor": s.get("seller_name"),
            "producto": None,
            "categoria": None,
            "monto_estimado": _float(s.get("venta_actual")),
            "prioridad": "alta" if _int(s.get("commercial_score")) < 60 else "media",
            "explicacion": f"Clientes únicos bajaron {_float(s.get('clientes_unicos_variacion_pct')):.0f}%.",
            "accion_sugerida": s.get("accion_sugerida"),
            "client_id": None,
        })

    for c in unique:
        if c.get("status") != "activo":
            continue
        va = _float(c.get("venta_actual"))
        vp = _float(c.get("venta_anterior"))
        if vp > 0 and va < vp * 0.7:
            out.append({
                "tipo": "cliente_baja_ticket",
                "titulo": f"Ticket en caída: {c.get('client_name')}",
                "cliente": c.get("client_name"),
                "vendedor": c.get("seller_name"),
                "producto": None,
                "categoria": None,
                "monto_estimado": vp - va,
                "prioridad": "media",
                "explicacion": "Venta del período cayó vs período anterior.",
                "accion_sugerida": "Revisar mix y frecuencia",
                "client_id": c.get("client_id"),
            })

    out.sort(key=lambda x: (PRIORITY_ORDER.get(str(x.get("prioridad")), 9), -(x.get("monto_estimado") or 0)))
    return out[:80]


def format_clp(n: float) -> str:
    return f"${int(round(n)):,}".replace(",", ".")


def build_meta(
    *,
    filters: Any,
    scope: Any,
    execution_ms: float,
    rows_analyzed: int,
    documents_analyzed: int,
    clients_analyzed: int,
    products_analyzed: int,
) -> dict[str, Any]:
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "company_id": COMPANY_ID,
        "office_id": OFFICE_ID,
        "date_from": filters.date_from.isoformat(),
        "date_to": filters.date_to.isoformat(),
        "previous_date_from": scope.prev_from.isoformat(),
        "previous_date_to": scope.prev_to.isoformat(),
        "rows_analyzed": rows_analyzed,
        "documents_analyzed": documents_analyzed,
        "clients_analyzed": clients_analyzed,
        "products_analyzed": products_analyzed,
        "execution_ms": round(execution_ms, 1),
    }
