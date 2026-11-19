"use client"

import { useParams } from "next/navigation"
import { useEffect, useState } from "react"
import { AppLink } from "@/components/app-link"
import { RouteProgressCard } from "@/components/route-progress-card"
import {
  ApiError,
  getMyDespacho,
  type DriverDespacho,
  type RouteProgress,
} from "@/lib/api"
import { setActivePlanId } from "@/lib/active-plan"
import { getSessionUserId } from "@/lib/auth"
import { moneyCLP } from "@/lib/delivery-flow"
import { formatDate, statusLabel } from "@/lib/format"
import { asAmount } from "@/lib/money-display"
import { isDeviceOffline, readOfflinePlanFromList } from "@/lib/offline-load"
import { isLikelyOfflineError } from "@/lib/offline-store"

export default function DespachoDetailPage() {
  const params = useParams()
  const planId = Number(params.planId)
  const [plan, setPlan] = useState<DriverDespacho | null>(null)
  const [progress, setProgress] = useState<RouteProgress | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (!Number.isFinite(planId) || planId < 1) {
      setError("Identificador de despacho inválido")
      return
    }
    const uid = getSessionUserId()
    if (uid) setActivePlanId(uid, planId)
    let cancelled = false
    async function loadFromOffline() {
      const outcome = await readOfflinePlanFromList(planId)
      if (cancelled) return
      if (outcome.status === "success") {
        setPlan(outcome.plan as DriverDespacho)
        setProgress(null)
        setError(null)
        return
      }
      setError(outcome.message)
    }
    async function load() {
      // Pintar snapshot local de inmediato; refrescar en red si hay conectividad.
      const local = await readOfflinePlanFromList(planId)
      let hasLocal = false
      if (!cancelled && local.status === "success") {
        setPlan(local.plan as DriverDespacho)
        setProgress(null)
        setError(null)
        hasLocal = true
      } else if (!cancelled && isDeviceOffline()) {
        setError(
          local.status === "error" || local.status === "empty"
            ? local.message
            : "Este despacho no está descargado.",
        )
        return
      }
      if (cancelled || isDeviceOffline()) return
      try {
        const data = await getMyDespacho(planId)
        if (cancelled) return
        setPlan(data.plan)
        setProgress(data.progress || null)
        setError(null)
      } catch (err) {
        if (cancelled || hasLocal) return
        if (isLikelyOfflineError(err) || isDeviceOffline()) {
          await loadFromOffline()
          return
        }
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
  const nextKey = progress?.next_customer?.customer_key
  const nextHref = nextKey
    ? `/despachos/${planId}/clientes/${encodeURIComponent(nextKey)}`
    : null

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
            <span className="kpi-label">Avance</span>
            <span className="kpi-value" style={{ fontSize: "0.95rem" }}>
              {progress ? `${progress.percent}%` : "—"}
            </span>
          </div>
        </div>

        {progress ? (
          <RouteProgressCard progress={progress} variant="compact" />
        ) : null}

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

      <RouteProgressCard progress={progress} nextHref={nextHref} />

      <div className="btn-row">
        <AppLink
          href={`/despachos/${planId}/clientes`}
          className="btn btn-secondary"
          style={{ textAlign: "center" }}
        >
          Ver clientes
        </AppLink>
        <AppLink
          href={`/despachos/${planId}/mapa`}
          className="btn btn-primary"
          style={{ textAlign: "center" }}
        >
          Ver mapa
        </AppLink>
      </div>
    </div>
  )
}
