"use client"

import Link from "next/link"
import { useEffect, useMemo, useState } from "react"
import { ApiError, listMyDespachos, type DriverDespacho } from "@/lib/api"
import { formatDate, statusLabel } from "@/lib/format"
import { partitionPlansByDate, santiagoTodayYmd } from "@/lib/plan-buckets"

function PlanCard(props: {
  plan: DriverDespacho
  hero?: boolean
  cta?: string
}) {
  const p = props.plan
  return (
    <div className={`card${props.hero ? " despacho-hero" : ""}`}>
      <div style={{ display: "flex", justifyContent: "space-between", gap: 8 }}>
        <h2 style={{ margin: 0, fontSize: props.hero ? "1.2rem" : "1.05rem" }}>
          {p.planning_code || `Plan #${p.id}`}
        </h2>
        <span className="pill">{statusLabel(p.status)}</span>
      </div>
      <div className="muted" style={{ marginTop: 4 }}>
        {formatDate(p.planning_date)}
      </div>
      <div className="meta-row">
        <span className="muted">Ruta</span>
        <span>{p.route_name || "—"}</span>
      </div>
      <div className="meta-row">
        <span className="muted">Vehículo</span>
        <span>
          {p.truck_name || "—"}
          {p.truck_plate ? ` · ${p.truck_plate}` : ""}
        </span>
      </div>
      <div className="kpi-row">
        <div className="kpi">
          <span className="kpi-label">Clientes</span>
          <span className="kpi-value">{p.client_count ?? "—"}</span>
        </div>
        <div className="kpi">
          <span className="kpi-label">Órdenes</span>
          <span className="kpi-value">{p.order_count ?? "—"}</span>
        </div>
      </div>
      <div className="meta-row" style={{ marginTop: 10 }}>
        <span className="muted">Monto picking</span>
        <span>Monto no disponible</span>
      </div>
      <p className="muted" style={{ margin: "6px 0 0", fontSize: "0.72rem" }}>
        El listado aún no expone el total con impuestos del picking; se mostrará
        cuando la API lo confirme.
      </p>
      <Link
        href={`/despachos/${p.id}`}
        className={`btn ${props.hero ? "btn-primary" : "btn-secondary"}`}
        style={{ textAlign: "center", marginTop: 14 }}
      >
        {props.cta || "Abrir despacho"}
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
          uno, pide a operaciones que te asigne con el selector de chofer (no se
          vincula por nombre).
        </p>
      </div>
    )
  }

  return (
    <div className="stack-gap">
      <section>
        <h2 className="section-title">Despacho de hoy</h2>
        <p className="muted" style={{ margin: "0 0 10px", fontSize: "0.82rem" }}>
          Fecha operativa Chile · {todayLabel}
        </p>
        {buckets.today.length === 0 ? (
          <div className="card">
            <p style={{ margin: 0 }}>
              No tienes un despacho asignado para hoy.
            </p>
            <p className="muted" style={{ margin: "8px 0 0", fontSize: "0.85rem" }}>
              Consulta próximos o historial más abajo. No se abre un plan antiguo
              en automático.
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
