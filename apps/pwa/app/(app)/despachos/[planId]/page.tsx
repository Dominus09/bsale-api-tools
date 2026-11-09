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
  const [note, setNote] = useState<string | null>(null)
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
        setNote(data.nav?.note_es || data.nav_placeholders?.note_es || null)
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
      <div className="card">
        <div style={{ display: "flex", justifyContent: "space-between", gap: 8 }}>
          <h2>{plan.planning_code || `Plan #${plan.id}`}</h2>
          <span className="pill">{statusLabel(plan.status)}</span>
        </div>
        {plan.planning_name ? <p className="muted">{plan.planning_name}</p> : null}
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
        <div className="meta-row">
          <span className="muted">Órdenes</span>
          <span>{plan.order_count ?? "—"}</span>
        </div>
        <div className="meta-row">
          <span className="muted">Clientes</span>
          <span>{plan.client_count ?? "—"}</span>
        </div>
      </div>

      {note ? <div className="banner banner-info">{note}</div> : null}

      <Link href={`/despachos/${planId}/clientes`} className="btn btn-primary" style={{ textAlign: "center" }}>
        Ver clientes
      </Link>
      <Link href={`/despachos/${planId}/mapa`} className="btn btn-ghost" style={{ textAlign: "center" }}>
        Abrir mapa
      </Link>
    </div>
  )
}
