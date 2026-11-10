"use client"

import Link from "next/link"
import { useParams } from "next/navigation"
import { useEffect, useMemo, useState } from "react"
import { ApiError, getMyCliente } from "@/lib/api"
import { formatRutDisplay } from "@/lib/format-rut"
import { moneyCLP, visitStatusLabel } from "@/lib/delivery-flow"
import { DeliveryLocalToolsHost } from "@/components/delivery-local-tools-host"
import { DeliveryVisitStart } from "@/components/delivery-visit-start"

export default function ClienteFichaPage() {
  const params = useParams()
  const planId = Number(params.planId)
  const rawKey = params.customerKey
  const customerKey = decodeURIComponent(
    Array.isArray(rawKey) ? rawKey.join("/") : String(rawKey || ""),
  )
  const [data, setData] = useState<Awaited<ReturnType<typeof getMyCliente>> | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [reloadToken, setReloadToken] = useState(0)
  const [lightbox, setLightbox] = useState<string | null>(null)

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
  }, [planId, customerKey, reloadToken])

  const totals = useMemo(() => {
    if (!data) return { count: 0, amount: 0 }
    const amount = (data.documents || []).reduce(
      (acc, d) => acc + (typeof d.document_total === "number" ? d.document_total : 0),
      0,
    )
    return { count: data.document_count || data.documents?.length || 0, amount }
  }, [data])

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
  const facadeUrl = c.facade?.url || c.facade_photo || null
  const locLabel = c.location_registration?.label_es || "Ubicación no disponible"
  const visit = data.active_visit
  const backClients = `/despachos/${planId}/clientes`

  return (
    <div className="stack-gap">
      <Link href={backClients} className="muted">
        ← Clientes
      </Link>

      <div className="card">
        <div className="ficha-head">
          <button
            type="button"
            className="facade-thumb"
            onClick={() => facadeUrl && setLightbox(facadeUrl)}
            aria-label={facadeUrl ? "Ampliar fachada" : "Sin fachada"}
            disabled={!facadeUrl}
          >
            {facadeUrl ? (
              // eslint-disable-next-line @next/next/no-img-element
              <img src={facadeUrl} alt="" />
            ) : (
              "Sin foto"
            )}
          </button>
          <div className="ficha-head-main">
            <h2>{c.client_name || "Sin nombre"}</h2>
            {c.fantasy_name ? (
              <p className="muted" style={{ margin: "0 0 6px", fontSize: "0.85rem" }}>
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
            <div className="meta-row">
              <span className="muted">Dirección</span>
              <span style={{ textAlign: "right" }}>
                {[c.address, c.city].filter(Boolean).join(", ") || "—"}
              </span>
            </div>
          </div>
        </div>

        <div className="ficha-summary">
          <span className={`pill${c.location_registration?.verified ? " pill-ok" : ""}`}>
            GPS · {locLabel}
          </span>
          <span className="pill">
            {totals.count} doc. · {moneyCLP(totals.amount)}
          </span>
          <span
            className={`pill${
              visit?.status === "in_progress" || data.latest_visit?.status
                ? " pill-ok"
                : ""
            }`}
          >
            Visita ·{" "}
            {visit?.status === "in_progress"
              ? "En atención"
              : visitStatusLabel(data.latest_visit?.status || visit?.status)}
          </span>
          {(c.load_groups || []).map((g) => (
            <span key={`${g.load_batch_id}-${g.name}`} className="pill">
              {g.name || `Grupo ${g.load_batch_id}`}
            </span>
          ))}
        </div>

        {c.observations ? (
          <p className="muted" style={{ marginTop: 10, marginBottom: 0, fontSize: "0.85rem" }}>
            Obs.: {c.observations}
          </p>
        ) : null}
      </div>

      {nav?.warning_es ? (
        <div className="banner banner-info" style={{ fontSize: "0.82rem" }}>
          {nav.warning_es}
        </div>
      ) : null}

      <DeliveryVisitStart
        planId={planId}
        customerKey={customerKey}
        clientName={c.client_name || "Cliente"}
        documentCount={totals.count}
        documentTotalLabel={moneyCLP(totals.amount)}
        p4Ready={Boolean(data.p4?.tables_ready)}
        p4Note={data.p4?.note_es}
        activeVisit={data.active_visit || null}
        latestVisit={data.latest_visit || data.active_visit || null}
        lastClosedVisit={data.last_closed_visit || null}
        onChanged={() => setReloadToken((n) => n + 1)}
      />

      <p className="muted" style={{ margin: 0, fontSize: "0.8rem" }}>
        GPS, fachada, cómo llegar e historial están en el menú lateral. La entrega se
        registra en pantallas por etapas.
      </p>

      {/* P3: montado en modo menú (sin paneles grandes en el cuerpo). */}
      <DeliveryLocalToolsHost
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
        backHref={backClients}
        onChanged={() => setReloadToken((n) => n + 1)}
      />

      {lightbox ? (
        <div className="lightbox" role="dialog" aria-modal="true">
          <button
            type="button"
            className="drawer-backdrop"
            aria-label="Cerrar imagen"
            onClick={() => setLightbox(null)}
          />
          {/* eslint-disable-next-line @next/next/no-img-element */}
          <img src={lightbox} alt="Fachada ampliada" />
        </div>
      ) : null}
    </div>
  )
}
