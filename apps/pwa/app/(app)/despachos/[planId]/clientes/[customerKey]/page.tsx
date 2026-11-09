"use client"

import Link from "next/link"
import { useParams } from "next/navigation"
import { useEffect, useState } from "react"
import { ApiError, getMyCliente } from "@/lib/api"

function money(n: number | null | undefined) {
  if (n == null || Number.isNaN(n)) return "—"
  return new Intl.NumberFormat("es-CL", {
    style: "currency",
    currency: "CLP",
    maximumFractionDigits: 0,
  }).format(n)
}

function units(n: number | null | undefined) {
  if (n == null || Number.isNaN(n)) return "—"
  return Number.isInteger(n) ? String(n) : n.toFixed(2)
}

export default function ClienteFichaPage() {
  const params = useParams()
  const planId = Number(params.planId)
  const rawKey = params.customerKey
  const customerKey = decodeURIComponent(
    Array.isArray(rawKey) ? rawKey.join("/") : String(rawKey || ""),
  )
  const [data, setData] = useState<Awaited<ReturnType<typeof getMyCliente>> | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (!Number.isFinite(planId) || planId < 1 || !customerKey) {
      setError("Cliente inválido")
      return
    }
    let cancelled = false
    async function load() {
      try {
        const res = await getMyCliente(planId, customerKey)
        if (!cancelled) {
          setData(res)
          setError(null)
        }
      } catch (err) {
        if (cancelled) return
        if (err instanceof ApiError && (err.status === 404 || err.status === 403)) {
          setError("Cliente no pertenece a tu despacho o no estás autorizado.")
        } else if (!navigator.onLine) {
          setError("Sin conexión. Reintenta cuando vuelva la red.")
        } else {
          setError(err instanceof ApiError ? err.message : "Error al cargar ficha")
        }
      }
    }
    void load()
    return () => {
      cancelled = true
    }
  }, [planId, customerKey])

  if (error) {
    return (
      <div className="stack-gap">
        <Link href={`/despachos/${planId}/clientes`} className="muted">
          ← Clientes
        </Link>
        <div className="banner banner-error">{error}</div>
      </div>
    )
  }

  if (!data) {
    return <div className="empty">Cargando ficha…</div>
  }

  const c = data.client

  return (
    <div className="stack-gap">
      <Link href={`/despachos/${planId}/clientes`} className="muted">
        ← Clientes
      </Link>

      <div className="card">
        <h2 style={{ marginBottom: 4 }}>{c.client_name || "Sin nombre"}</h2>
        <p className="muted" style={{ margin: 0 }}>
          Código {c.client_id != null ? c.client_id : "—"}
          {c.fantasy_name ? ` · ${c.fantasy_name}` : ""}
        </p>
        <div className="facade-ph" aria-hidden>
          {c.facade_photo ? (
            // eslint-disable-next-line @next/next/no-img-element
            <img src={c.facade_photo} alt="Fachada" />
          ) : (
            <span>Sin foto de fachada</span>
          )}
        </div>
        <div className="meta-row">
          <span className="muted">Dirección</span>
          <span>{c.address || "—"}</span>
        </div>
        <div className="meta-row">
          <span className="muted">Comuna / sector</span>
          <span>{c.city || "—"}</span>
        </div>
        <div className="meta-row">
          <span className="muted">Teléfono</span>
          <span>
            {c.phone ? (
              <a href={`tel:${c.phone}`} style={{ color: "var(--accent)" }}>
                {c.phone}
              </a>
            ) : (
              "—"
            )}
          </span>
        </div>
        <div className="meta-row">
          <span className="muted">GPS</span>
          <span>
            {c.has_coordinates
              ? `${Number(c.latitude).toFixed(5)}, ${Number(c.longitude).toFixed(5)}`
              : "Sin coordenadas verificables"}
          </span>
        </div>
        {(c.load_groups || []).length > 0 ? (
          <div className="group-row" style={{ marginTop: 10 }}>
            {(c.load_groups || []).map((g) => (
              <span key={`${g.load_batch_id}-${g.name}`} className="pill">
                {g.name || `Grupo ${g.load_batch_id}`}
              </span>
            ))}
          </div>
        ) : null}
        {c.observations ? (
          <p className="muted" style={{ marginTop: 12 }}>
            Obs.: {c.observations}
          </p>
        ) : null}
        {data.navigation?.available && data.navigation.google_maps_url ? (
          <a
            className="btn btn-primary"
            style={{ marginTop: 12, textAlign: "center" }}
            href={data.navigation.google_maps_url}
            target="_blank"
            rel="noopener noreferrer"
          >
            Abrir navegación
          </a>
        ) : (
          <button type="button" className="btn btn-ghost" disabled style={{ marginTop: 12 }}>
            Sin destino GPS válido
          </button>
        )}
      </div>

      <div>
        <h3 style={{ margin: "0 0 8px", fontSize: "1rem" }}>
          Documentos ({data.document_count})
        </h3>
        <div className="stack-gap">
          {data.documents.map((doc) => (
            <div key={String(doc.related_document_id || doc.document_number)} className="card">
              <div className="client-card-head">
                <h3 style={{ fontSize: "1rem" }}>
                  {doc.document_type || "Documento"} {doc.document_number ?? "—"}
                </h3>
                <span className="pill">{money(doc.document_total)}</span>
              </div>
              <div className="meta-row">
                <span className="muted">OC</span>
                <span>{doc.oc_number ?? doc.oc_document_id ?? "—"}</span>
              </div>
              <div className="meta-row">
                <span className="muted">Condición</span>
                <span>{doc.payment_method || "—"}</span>
              </div>
              {doc.load_group?.name ? (
                <div className="meta-row">
                  <span className="muted">Grupo</span>
                  <span>{doc.load_group.name}</span>
                </div>
              ) : null}
              <div style={{ marginTop: 10 }}>
                <div className="muted" style={{ marginBottom: 6 }}>
                  Productos ({doc.product_count})
                </div>
                {(doc.products || []).length === 0 ? (
                  <p className="muted">Sin líneas de producto en picking.</p>
                ) : (
                  <ul className="product-list">
                    {doc.products.map((p, idx) => (
                      <li key={`${p.variant_id}-${idx}`}>
                        <div className="product-name">{p.description || p.producto || "Producto"}</div>
                        <div className="muted">
                          SKU {p.sku_code ?? p.variant_id ?? "—"}
                          {p.codigo_barras ? ` · ${p.codigo_barras}` : ""}
                        </div>
                        <div className="product-qty">
                          {units(p.unidades)} un.
                          {p.cajas_equivalentes != null
                            ? ` · ${units(p.cajas_equivalentes)} cajas`
                            : ""}
                          {p.load_group?.name ? ` · ${p.load_group.name}` : ""}
                        </div>
                      </li>
                    ))}
                  </ul>
                )}
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}
