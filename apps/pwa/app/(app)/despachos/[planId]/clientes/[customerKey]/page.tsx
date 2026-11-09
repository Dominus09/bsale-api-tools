"use client"

import Link from "next/link"
import { useParams } from "next/navigation"
import { useEffect, useState } from "react"
import { ApiError, getMyCliente } from "@/lib/api"
import { formatRutDisplay } from "@/lib/format-rut"
import { ClientLocationCapture } from "@/components/client-location-capture"

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
  const [openDocs, setOpenDocs] = useState<Record<string, boolean>>({})
  const [openSku, setOpenSku] = useState<Record<string, boolean>>({})
  const [reloadToken, setReloadToken] = useState(0)

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
          const first = res.documents?.[0]
          if (first) {
            const id = String(first.related_document_id || first.document_number || 0)
            setOpenDocs({ [id]: true })
          }
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
  }, [planId, customerKey, reloadToken])

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
  const nav = data.navigation

  return (
    <div className="stack-gap">
      <Link href={`/despachos/${planId}/clientes`} className="muted">
        ← Clientes
      </Link>

      <div className="card">
        <h2 style={{ marginBottom: 4 }}>{c.client_name || "Sin nombre"}</h2>
        {c.fantasy_name ? (
          <p className="muted" style={{ margin: "0 0 8px" }}>
            {c.fantasy_name}
          </p>
        ) : null}

        <div className="meta-row">
          <span className="muted">RUT</span>
          <span>{formatRutDisplay(c.rut) || "—"}</span>
        </div>
        <div className="meta-row">
          <span className="muted">Vendedor</span>
          <span>{c.seller_name || "—"}</span>
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
      </div>

      <ClientLocationCapture
        planId={planId}
        customerKey={customerKey}
        address={c.address}
        city={c.city}
        latitude={c.latitude}
        longitude={c.longitude}
        coordinatesSource={c.coordinates_source}
        locationRegistration={c.location_registration}
        facade={c.facade}
        facadePhoto={c.facade_photo}
        navigationUrl={nav?.available ? nav.google_maps_url || null : null}
        warnAccuracyM={data.p3?.gps_warn_accuracy_m}
        onChanged={() => setReloadToken((n) => n + 1)}
      />

      {nav?.warning_es ? (
        <p className="muted" style={{ margin: 0, fontSize: "0.78rem" }}>
          {nav.warning_es}
        </p>
      ) : null}

      <div>
        <h3 style={{ margin: "0 0 8px", fontSize: "1rem" }}>
          Documentos ({data.document_count})
        </h3>
        <div className="stack-gap">
          {data.documents.map((doc) => {
            const id = String(doc.related_document_id || doc.document_number || Math.random())
            const open = Boolean(openDocs[id])
            return (
              <div key={id} className="card doc-card">
                <button
                  type="button"
                  className="doc-toggle"
                  aria-expanded={open}
                  onClick={() => setOpenDocs((s) => ({ ...s, [id]: !s[id] }))}
                >
                  <div className="client-card-head">
                    <h3 style={{ fontSize: "1rem" }}>
                      {doc.document_type || "Documento"} {doc.document_number ?? "—"}
                    </h3>
                    <span className="pill">{money(doc.document_total)}</span>
                  </div>
                  <div className="doc-summary muted">
                    {doc.payment_method || "Sin condición"} ·{" "}
                    {doc.load_group?.name || "Sin grupo"} · {doc.product_count} prod.
                    {doc.seller_name ? ` · ${doc.seller_name}` : ""}
                    {doc.oc_number != null ? ` · OC ${doc.oc_number}` : ""}
                  </div>
                  <span className="muted" style={{ fontSize: "0.8rem" }}>
                    {open ? "Ocultar productos ▲" : "Ver productos ▼"}
                  </span>
                </button>

                {open ? (
                  <div style={{ marginTop: 10 }}>
                    {(doc.products || []).length === 0 ? (
                      <p className="muted">Sin líneas de producto en picking.</p>
                    ) : (
                      <ul className="product-list">
                        {doc.products.map((p, idx) => {
                          const skuKey = `${id}-${p.variant_id}-${idx}`
                          const skuOpen = Boolean(openSku[skuKey])
                          return (
                            <li key={skuKey}>
                              <div className="product-name">
                                {p.name_short || p.description || p.producto || "Producto"}
                              </div>
                              <div className="product-qty">
                                {units(p.unidades)} un.
                                {p.cajas_equivalentes != null
                                  ? ` · ${units(p.cajas_equivalentes)} cajas`
                                  : p.equivalence_label
                                    ? ` · ${p.equivalence_label}`
                                    : ""}
                              </div>
                              <button
                                type="button"
                                className="linkish muted"
                                style={{ marginTop: 4, fontSize: "0.85rem", fontWeight: 500 }}
                                onClick={() =>
                                  setOpenSku((s) => ({ ...s, [skuKey]: !s[skuKey] }))
                                }
                              >
                                {skuOpen ? "Ocultar código" : "SKU / código de barras"}
                              </button>
                              {skuOpen ? (
                                <div className="muted" style={{ marginTop: 4 }}>
                                  SKU {p.sku_code ?? p.variant_id ?? "—"}
                                  {p.codigo_barras ? ` · ${p.codigo_barras}` : ""}
                                  {p.units_per_box != null
                                    ? ` · ${p.units_per_box} un/caja`
                                    : ""}
                                </div>
                              ) : null}
                            </li>
                          )
                        })}
                      </ul>
                    )}
                  </div>
                ) : null}
              </div>
            )
          })}
        </div>
      </div>
    </div>
  )
}
