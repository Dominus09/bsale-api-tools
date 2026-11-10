"use client"

import Link from "next/link"
import { useParams } from "next/navigation"
import { useEffect, useState } from "react"
import { ApiError, getMyDespacho, type DriverDespacho } from "@/lib/api"
import { moneyCLP } from "@/lib/delivery-flow"
import { formatDate, statusLabel } from "@/lib/format"
import { asAmount } from "@/lib/money-display"

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
    return <div className="empty">Cargando resumen…</div>
  }

  const amount = asAmount(plan.amount_total)
  const docs =
    typeof plan.document_count === "number" ? plan.document_count : null
  const clients =
    typeof plan.client_count === "number" ? plan.client_count : null

  return (
    <div className="stack-gap">
      <div className="card despacho-hero">
        <div className="despacho-hero-head">
          <div>
            <h2 style={{ margin: 0 }}>{plan.planning_code || `Plan #${plan.id}`}</h2>
            <p className="muted" style={{ margin: "4px 0 0", fontSize: "0.8rem" }}>
              {formatDate(plan.planning_date)}
            </p>
          </div>
          <span className={`pill${plan.status === "picking_generated" ? " pill-ok" : ""}`}>
            {statusLabel(plan.status)}
          </span>
        </div>

        <div className="kpi-row">
          <div className="kpi">
            <span className="kpi-label">Clientes</span>
            <span className="kpi-value">{clients ?? "—"}</span>
          </div>
          <div className="kpi">
            <span className="kpi-label">Documentos</span>
            <span className="kpi-value">{docs ?? "—"}</span>
          </div>
          <div className="kpi">
            <span className="kpi-label">Monto total</span>
            <span className="kpi-value" style={{ fontSize: "0.95rem" }}>
              {amount != null ? moneyCLP(amount) : "—"}
            </span>
          </div>
          <div className="kpi">
            <span className="kpi-label">Picking</span>
            <span className="kpi-value" style={{ fontSize: "0.85rem" }}>
              {statusLabel(plan.status)}
            </span>
          </div>
        </div>

        <div className="meta-row" style={{ marginTop: 12 }}>
          <span className="muted">Ruta</span>
          <span>{plan.route_name || "—"}</span>
        </div>
        <div className="meta-row">
          <span className="muted">Vehículo</span>
          <span>{plan.truck_name || "—"}</span>
        </div>
        <div className="meta-row">
          <span className="muted">Patente</span>
          <span>{plan.truck_plate || "—"}</span>
        </div>
      </div>

      <div className="btn-row">
        <Link
          href={`/despachos/${planId}/clientes`}
          className="btn btn-secondary"
          style={{ textAlign: "center" }}
        >
          Ver clientes
        </Link>
        <Link
          href={`/despachos/${planId}/mapa`}
          className="btn btn-primary"
          style={{ textAlign: "center" }}
        >
          Ver mapa
        </Link>
      </div>
    </div>
  )
}
