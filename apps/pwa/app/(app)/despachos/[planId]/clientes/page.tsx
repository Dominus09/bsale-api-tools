"use client"

import Link from "next/link"
import { useParams } from "next/navigation"
import { useEffect, useMemo, useState } from "react"
import { ApiError, listMyClientes, type DriverClientCard } from "@/lib/api"
import { moneyCLP } from "@/lib/delivery-flow"

function gpsLabel(has: boolean) {
  return has ? "GPS disponible" : "Sin coordenadas"
}

export default function ClientesPage() {
  const params = useParams()
  const planId = Number(params.planId)
  const [items, setItems] = useState<DriverClientCard[]>([])
  const [q, setQ] = useState("")
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)
  const [offline, setOffline] = useState(false)

  useEffect(() => {
    function syncOnline() {
      setOffline(typeof navigator !== "undefined" && !navigator.onLine)
    }
    syncOnline()
    window.addEventListener("online", syncOnline)
    window.addEventListener("offline", syncOnline)
    return () => {
      window.removeEventListener("online", syncOnline)
      window.removeEventListener("offline", syncOnline)
    }
  }, [])

  useEffect(() => {
    if (!Number.isFinite(planId) || planId < 1) {
      setError("Despacho inválido")
      setLoading(false)
      return
    }
    let cancelled = false
    const handle = window.setTimeout(async () => {
      setLoading(true)
      try {
        const data = await listMyClientes(planId, q)
        if (cancelled) return
        setItems(data.items || [])
        setError(null)
      } catch (err) {
        if (cancelled) return
        if (!navigator.onLine) {
          setError("Sin conexión. Reintenta cuando vuelva la red.")
        } else if (err instanceof ApiError && (err.status === 404 || err.status === 403)) {
          setError("Despacho no autorizado o no encontrado.")
        } else {
          setError(err instanceof ApiError ? err.message : "Error al cargar clientes")
        }
      } finally {
        if (!cancelled) setLoading(false)
      }
    }, q ? 220 : 0)
    return () => {
      cancelled = true
      window.clearTimeout(handle)
    }
  }, [planId, q])

  const countLabel = useMemo(
    () => `${items.length} cliente${items.length === 1 ? "" : "s"}`,
    [items.length],
  )

  return (
    <div className="stack-gap">
      <div>
        <h2 style={{ margin: "0 0 4px", fontSize: "1.15rem" }}>Clientes</h2>
        <p className="muted" style={{ margin: 0 }}>
          {countLabel} · documentos de consulta · circuito de entrega habilitado
        </p>
      </div>

      {offline ? <div className="banner banner-error">Sin conexión a internet.</div> : null}

      <div className="field" style={{ marginBottom: 0 }}>
        <label htmlFor="client-search">Buscar local, cliente, folio o dirección</label>
        <input
          id="client-search"
          value={q}
          onChange={(e) => setQ(e.target.value)}
          placeholder="Ej. Galpo, Quellón, 2792373"
          autoComplete="off"
          enterKeyHint="search"
        />
      </div>

      {error ? <div className="banner banner-error">{error}</div> : null}
      {loading ? <div className="empty">Cargando clientes…</div> : null}

      {!loading && !error && items.length === 0 ? (
        <div className="empty">No hay clientes para mostrar.</div>
      ) : null}

      <div className="stack-gap">
        {items.map((c) => {
          const obs = (c.observations || "").trim()
          const amount =
            typeof c.amount_total === "number" ? moneyCLP(c.amount_total) : "Monto no disponible"
          return (
            <Link
              key={c.customer_key}
              href={`/despachos/${planId}/clientes/${encodeURIComponent(c.customer_key)}`}
              className="card tap-card client-card"
            >
              <div className="client-card-head">
                <h3>{c.client_name || "Sin nombre"}</h3>
                <span className={`pill${c.has_coordinates ? " pill-ok" : ""}`}>
                  {gpsLabel(c.has_coordinates)}
                </span>
              </div>
              {c.city ? (
                <p style={{ margin: "4px 0 0", fontWeight: 600, fontSize: "0.92rem" }}>
                  {c.city}
                </p>
              ) : null}
              {c.address ? (
                <p className="muted" style={{ marginTop: 2 }}>
                  {c.address}
                </p>
              ) : null}

              <div className="client-card-meta">
                <span>
                  {c.document_count} doc{c.document_count === 1 ? "" : "s"}
                </span>
                <span>{amount}</span>
              </div>
              {c.attention_label_es ? (
                <div className="group-row" style={{ marginTop: 6 }}>
                  <span
                    className={`pill${
                      c.attention_status && c.attention_status !== "pending"
                        ? " pill-ok"
                        : ""
                    }`}
                  >
                    {c.attention_label_es}
                  </span>
                </div>
              ) : null}

              <div className={obs ? "obs-block" : "obs-block obs-empty"}>
                {obs || "Sin observaciones"}
              </div>

              {(c.load_groups || []).length > 0 ? (
                <div className="group-row" style={{ marginTop: 8 }}>
                  {(c.load_groups || []).map((g) => (
                    <span key={`${g.load_batch_id}-${g.name}`} className="pill">
                      {g.name || `Grupo ${g.load_batch_id}`}
                    </span>
                  ))}
                </div>
              ) : null}
            </Link>
          )
        })}
      </div>
    </div>
  )
}
