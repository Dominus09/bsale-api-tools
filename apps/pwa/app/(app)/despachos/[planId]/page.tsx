"use client"

import Link from "next/link"
import { useParams } from "next/navigation"
import { useEffect, useState } from "react"
import { ApiError, getMyDespacho, type DriverDespacho } from "@/lib/api"
import { formatDate, statusLabel } from "@/lib/format"

export default function DespachoDetailPage() {
  const params = useParams()
  const planId = Number(params.planId)
  const [plan, setPlan] = useState<DriverDespacho | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (!Number.isFinite(planId) || planId < 1) {
      setError("Identificador de despacho inválido")
      return
    }
    let cancelled = false
    async function load() {
      try {
        const data = await getMyDespacho(planId)
        if (cancelled) return
        setPlan(data.plan)
      } catch (err) {
        if (cancelled) return
        if (err instanceof ApiError && (err.status === 404 || err.status === 403)) {
          setError("Despacho no encontrado o no autorizado para tu usuario.")
        } else {
          setError(err instanceof ApiError ? err.message : "Error al cargar")
        }
      }
    }
    void load()
    return () => {
      cancelled = true
    }
  }, [planId])

  if (error) {
    return <div className="banner banner-error">{error}</div>
  }

  if (!plan) {
    return <div className="empty">Cargando ficha…</div>
  }

  return (
    <div className="stack-gap">
      <div className="card despacho-hero">
        <div style={{ display: "flex", justifyContent: "space-between", gap: 8 }}>
          <h2 style={{ margin: 0 }}>{plan.planning_code || `Plan #${plan.id}`}</h2>
          <span className="pill">{statusLabel(plan.status)}</span>
        </div>
        {plan.planning_name ? (
          <p className="muted" style={{ margin: "6px 0 0" }}>
            {plan.planning_name}
          </p>
        ) : null}
        <div className="meta-row">
          <span className="muted">Fecha</span>
          <span>{formatDate(plan.planning_date)}</span>
        </div>
        <div className="meta-row">
          <span className="muted">Ruta</span>
          <span>{plan.route_name || "—"}</span>
        </div>
        <div className="meta-row">
          <span className="muted">Vehículo</span>
          <span>
            {plan.truck_name || "—"}
            {plan.truck_plate ? ` · ${plan.truck_plate}` : ""}
          </span>
        </div>
        <div className="meta-row">
          <span className="muted">Chofer</span>
          <span>{plan.driver_name || "—"}</span>
        </div>

        <div className="kpi-row">
          <div className="kpi">
            <span className="kpi-label">Clientes</span>
            <span className="kpi-value">{plan.client_count ?? "—"}</span>
          </div>
          <div className="kpi">
            <span className="kpi-label">Órdenes</span>
            <span className="kpi-value">{plan.order_count ?? "—"}</span>
          </div>
        </div>

        <div className="meta-row" style={{ marginTop: 12 }}>
          <span className="muted">Total picking</span>
          <span>Monto no disponible</span>
        </div>
        <p className="muted" style={{ margin: "6px 0 0", fontSize: "0.75rem" }}>
          Estado del plan = preparación operativa ({statusLabel(plan.status)}). No
          equivale al avance de entregas en terreno.
        </p>
      </div>

      <Link
        href={`/despachos/${planId}/clientes`}
        className="btn btn-primary"
        style={{ textAlign: "center" }}
      >
        Ver clientes
      </Link>
      <Link
        href={`/despachos/${planId}/mapa`}
        className="btn btn-secondary"
        style={{ textAlign: "center" }}
      >
        Ver mapa
      </Link>
    </div>
  )
}
