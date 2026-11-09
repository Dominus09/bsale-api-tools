"use client"

import Link from "next/link"
import { useEffect, useState } from "react"
import { ApiError, listMyDespachos, type DriverDespacho } from "@/lib/api"
import { formatDate, statusLabel } from "@/lib/format"

export default function DespachosPage() {
  const [items, setItems] = useState<DriverDespacho[] | null>(null)
  const [error, setError] = useState<string | null>(null)

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
      {items.map((p) => (
        <Link key={p.id} href={`/despachos/${p.id}`} className="card tap-card">
          <div style={{ display: "flex", justifyContent: "space-between", gap: 8 }}>
            <h2>{p.planning_code || `Plan #${p.id}`}</h2>
            <span className="pill">{statusLabel(p.status)}</span>
          </div>
          <div className="muted">{formatDate(p.planning_date)}</div>
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
          {p.client_count != null ? (
            <div className="meta-row">
              <span className="muted">Clientes</span>
              <span>{p.client_count}</span>
            </div>
          ) : null}
        </Link>
      ))}
    </div>
  )
}
