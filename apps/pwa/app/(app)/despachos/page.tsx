"use client"

import Link from "next/link"
import { useEffect, useMemo, useState } from "react"
import { ApiError, listMyDespachos, type DriverDespacho } from "@/lib/api"
import { moneyCLP } from "@/lib/delivery-flow"
import { formatDate, statusLabel } from "@/lib/format"
import { asAmount } from "@/lib/money-display"
import { partitionPlansByDate, santiagoTodayYmd } from "@/lib/plan-buckets"

function PlanCard(props: {
  plan: DriverDespacho
  hero?: boolean
  cta?: string
}) {
  const p = props.plan
  const amount = asAmount(p.amount_total)
  const docs =
    typeof p.document_count === "number" ? p.document_count : null
  const clients =
    typeof p.client_count === "number" ? p.client_count : null

  return (
    <div className={`card${props.hero ? " despacho-hero" : ""}`}>
      <div className="despacho-hero-head">
        <h2 style={{ margin: 0, fontSize: props.hero ? "1.15rem" : "1.02rem" }}>
          {p.planning_code || `Plan #${p.id}`}
        </h2>
        <span className={`pill${p.status === "picking_generated" ? " pill-ok" : ""}`}>
          {statusLabel(p.status)}
        </span>
      </div>
      {!props.hero ? (
        <div className="muted" style={{ marginTop: 4, fontSize: "0.8rem" }}>
          {formatDate(p.planning_date)}
        </div>
      ) : null}

      <div className="despacho-hero-grid">
        <div>
          <span className="meta-label">Ruta</span>
          <span className="meta-value">{p.route_name || "—"}</span>
        </div>
        <div>
          <span className="meta-label">Vehículo</span>
          <span className="meta-value">{p.truck_name || "—"}</span>
        </div>
        <div>
          <span className="meta-label">Patente</span>
          <span className="meta-value">{p.truck_plate || "—"}</span>
        </div>
        <div>
          <span className="meta-label">Clientes</span>
          <span className="meta-value">{clients ?? "—"}</span>
        </div>
        <div>
          <span className="meta-label">Documentos</span>
          <span className="meta-value">{docs ?? "—"}</span>
        </div>
        <div>
          <span className="meta-label">Fecha</span>
          <span className="meta-value">{formatDate(p.planning_date)}</span>
        </div>
      </div>

      <div className="despacho-amount">
        <span className="despacho-amount-label">Monto picking</span>
        <div className="despacho-amount-value">
          {amount != null ? moneyCLP(amount) : "Monto no disponible"}
        </div>
      </div>

      <Link
        href={`/despachos/${p.id}`}
        className={`btn ${props.hero ? "btn-primary btn-with-arrow" : "btn-secondary"}`}
        style={{ textAlign: "center", marginTop: 14 }}
      >
        {props.cta || "Abrir despacho"}
        {props.hero ? (
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" aria-hidden>
            <path d="M5 12h14" />
            <path d="m13 6 6 6-6 6" />
          </svg>
        ) : null}
      </Link>
    </div>
  )
}

export default function DespachosPage() {
  const [items, setItems] = useState<DriverDespacho[] | null>(null)
  const [error, setError] = useState<string | null>(null)
  const todayLabel = useMemo(() => {
    const ymd = santiagoTodayYmd()
    const [y, m, d] = ymd.split("-")
    return `${d}/${m}/${y}`
  }, [])

  useEffect(() => {
    let cancelled = false
    async function load() {
      try {
        const data = await listMyDespachos()
        if (!cancelled) setItems(data.items || [])
      } catch (err) {
        if (cancelled) return
        setError(err instanceof ApiError ? err.message : "No se pudieron cargar los despachos")
        setItems([])
      }
    }
    void load()
    return () => {
      cancelled = true
    }
  }, [])

  const buckets = useMemo(
    () => partitionPlansByDate(items || []),
    [items],
  )

  if (items === null && !error) {
    return <div className="empty">Cargando despachos…</div>
  }

  if (error) {
    return <div className="banner banner-error">{error}</div>
  }

  if (!items || items.length === 0) {
    return (
      <div className="card empty">
        <h2 style={{ marginBottom: 8 }}>Sin despachos asignados</h2>
        <p className="muted">
          No hay planificaciones con tu usuario como chofer. Si crees que falta
          uno, pide a operaciones que te asigne con el selector de chofer.
        </p>
      </div>
    )
  }

  return (
    <div className="stack-gap">
      <section>
        <h2 className="section-title">Despacho de hoy</h2>
        <p className="muted" style={{ margin: "0 0 10px", fontSize: "0.78rem" }}>
          Fecha operativa Chile · {todayLabel}
        </p>
        {buckets.today.length === 0 ? (
          <div className="card">
            <p style={{ margin: 0 }}>No tienes un despacho asignado para hoy.</p>
            <p className="muted" style={{ margin: "8px 0 0", fontSize: "0.85rem" }}>
              Consulta próximos o historial más abajo.
            </p>
          </div>
        ) : (
          <div className="stack-gap">
            {buckets.today.map((p) => (
              <PlanCard key={p.id} plan={p} hero cta="Abrir despacho" />
            ))}
          </div>
        )}
      </section>

      {buckets.unknown.length > 0 ? (
        <div className="banner banner-info" style={{ fontSize: "0.82rem" }}>
          Hay {buckets.unknown.length} plan(es) sin fecha operativa legible.
        </div>
      ) : null}

      <details className="details-block">
        <summary>
          Próximos despachos
          <span className="muted">{buckets.upcoming.length}</span>
        </summary>
        <div className="details-body">
          {buckets.upcoming.length === 0 ? (
            <p className="muted" style={{ margin: 0 }}>
              Sin despachos futuros asignados.
            </p>
          ) : (
            buckets.upcoming.map((p) => <PlanCard key={p.id} plan={p} />)
          )}
        </div>
      </details>

      <details className="details-block">
        <summary>
          Historial de despachos
          <span className="muted">{buckets.history.length + buckets.unknown.length}</span>
        </summary>
        <div className="details-body">
          {[...buckets.history, ...buckets.unknown].length === 0 ? (
            <p className="muted" style={{ margin: 0 }}>
              Sin historial visible.
            </p>
          ) : (
            [...buckets.history, ...buckets.unknown].map((p) => (
              <PlanCard key={p.id} plan={p} />
            ))
          )}
        </div>
      </details>
    </div>
  )
}
